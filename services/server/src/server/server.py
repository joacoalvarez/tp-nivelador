import socket
import signal
import os
import threading
import logger
from protocol.protocol import (
    Protocol,
    OPCODE_DATA,
    OPCODE_FIN,
)
from bet_serializer.bet_serializer import deserialize_bets, serialize_bet
from lottery.lottery import Lottery


class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port

        self.lottery_storage_path = os.environ.get(
            "LOTTERY_STORAGE_PATH", "/lottery"
        )
        os.makedirs(self.lottery_storage_path, exist_ok=True)
        self._clear_lottery_storage()
        self.lottery_storage_lock = threading.Lock()

        self.agency_quorum_min = int(os.environ.get("AGENCY_QUORUM_MIN", "1"))
        if self.agency_quorum_min <= 0:
            raise ValueError("AGENCY_QUORUM_MIN must be positive")
        self.quorum_condition = threading.Condition()
        self.completed_agencies = set()

        self.shutdown_event = threading.Event()
        self.threads = set()
        self.threads_lock = threading.Lock()
        self.sockets = set()
        self.sockets_lock = threading.Lock()
        self.server_socket = None

        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)

    def _handle_shutdown_signal(self, signum, frame):
        self._shutdown()

    def _shutdown(self):
        self.shutdown_event.set()

        with self.quorum_condition:
            self.quorum_condition.notify_all()

        sockets = []
        with self.sockets_lock:
            if self.server_socket is not None:
                sockets.append(self.server_socket)
            sockets.extend(self.sockets)

        for client_socket in sockets:
            client_socket.close()

    def _clear_lottery_storage(self) -> None:
        for filename in os.listdir(self.lottery_storage_path):
            os.remove(os.path.join(self.lottery_storage_path, filename))

    def _lottery_file(self) -> Lottery:
        storage_path = os.path.join(
            self.lottery_storage_path, "lottery.csv"
        )
        return Lottery(storage_path)

    def _receive_bets(self, protocol: Protocol) -> tuple[int | None, int]:
        agency_id = None
        lottery = self._lottery_file()
        message_amount = 0

        while not self.shutdown_event.is_set():
            opcode, client_message = protocol.recv_message()
            if opcode == OPCODE_FIN:
                return agency_id, message_amount

            if opcode != OPCODE_DATA:
                try:
                    protocol.send_err()
                except Exception:
                    pass
                raise ValueError(f"unexpected opcode: {opcode}")

            try:
                bets = deserialize_bets(client_message)
                if not bets:
                    raise ValueError("batch cannot be empty")

                batch_agency_id = bets[0].agency_id
                if agency_id is None:
                    agency_id = batch_agency_id
                elif agency_id != batch_agency_id:
                    raise ValueError("a connection cannot contain multiple agencies")

                with self.lottery_storage_lock:
                    lottery.store_bets(bets)
                message_amount += len(bets)
                protocol.send_ack()
            except Exception as e:
                logger.error("receive-bets", logger.LogResult.fail, "err", e)
                try:
                    protocol.send_err()
                except Exception:
                    pass
                continue

    def _send_winners(self, protocol: Protocol, agency_id: int | None) -> None:
        if self.shutdown_event.is_set():
            return
        if agency_id is not None:
            lottery = self._lottery_file()
            winners = []
            with self.lottery_storage_lock:
                for bet in lottery.load_bets():
                    if bet.agency_id == agency_id and lottery.has_won(bet):
                        winners.append(bet)
            for winner in winners:
                if self.shutdown_event.is_set():
                    return
                protocol.send_data(serialize_bet(winner))

        protocol.send_fin()

    def _wait_agency_quorum(self, agency_id: int | None) -> None:
        if agency_id is None:
            return

        with self.quorum_condition:
            if self.shutdown_event.is_set():
                return
            self.completed_agencies.add(agency_id)
            self.quorum_condition.notify_all()
            while len(self.completed_agencies) < self.agency_quorum_min and not self.shutdown_event.is_set():
                self.quorum_condition.wait()

    def _handle_client(self, client_socket):
        action = "handle-client"
        protocol = Protocol(client_socket)

        with self.sockets_lock:
            self.sockets.add(client_socket)

        try:
            logger.info(action, logger.LogResult.in_progress)
            agency_id, message_amount = self._receive_bets(protocol)
            self._wait_agency_quorum(agency_id)
            self._send_winners(protocol, agency_id)
            logger.info(
                action,
                logger.LogResult.success,
                "messages-amount",
                message_amount,
            )
        except Exception as e:
            if not self.shutdown_event.is_set():
                logger.error(action, logger.LogResult.fail, "messages-amount")
                raise e
        finally:
            with self.sockets_lock:
                self.sockets.discard(client_socket)
            try:
                client_socket.close()
            except Exception:
                pass
            
            with self.threads_lock:
                self.threads.discard(threading.current_thread())

    def run(self):
        action = "accept-connection"

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            self.server_socket = server_socket
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while not self.shutdown_event.is_set():
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    if self.shutdown_event.is_set():
                        break
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket,)
                )

                with self.threads_lock:
                    self.threads.add(client_thread)
                    
                client_thread.start()

            logger.info("server-cleanup", logger.LogResult.in_progress)
            
            with self.threads_lock:
                active_threads = list(self.threads)
            
            for t in active_threads:
                if t.is_alive():
                    t.join()
                    
            logger.info("server-stopped-gracefully", logger.LogResult.success)
