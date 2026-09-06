import socket
import os
import logger
from protocol.protocol import Protocol
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

    def _clear_lottery_storage(self) -> None:
        for filename in os.listdir(self.lottery_storage_path):
            os.remove(os.path.join(self.lottery_storage_path, filename))

    def _lottery_for_agency(self, agency_id: int) -> Lottery:
        storage_path = os.path.join(
            self.lottery_storage_path, f"agency-{agency_id}.csv"
        )
        return Lottery(storage_path)

    def _receive_bets(self, protocol: Protocol) -> tuple[int | None, int]:
        agency_id = None
        lottery = None
        message_amount = 0

        while True:
            client_message = protocol.recv_message()
            if protocol.is_fin(client_message):
                return agency_id, message_amount

            bets = deserialize_bets(client_message)
            if not bets:
                raise ValueError("batch cannot be empty")

            batch_agency_id = bets[0].agency_id
            if agency_id is None:
                agency_id = batch_agency_id
                lottery = self._lottery_for_agency(agency_id)
            elif agency_id != batch_agency_id:
                raise ValueError("a connection cannot contain multiple agencies")

            lottery.store_bets(bets)
            message_amount += len(bets)
            
    def _send_winners(self, protocol: Protocol, agency_id: int | None) -> None:
        if agency_id is not None:
            lottery = self._lottery_for_agency(agency_id)
            winners = []
            for bet in lottery.load_bets():
                if lottery.has_won(bet):
                    winners.append(bet)
            for winner in winners:
                protocol.send_message(serialize_bet(winner))

        protocol.send_fin()

    def _handle_client(self, client_socket):
        action = "handle-client"
        protocol = Protocol(client_socket)

        try:
            logger.info(action, logger.LogResult.in_progress)
            agency_id, message_amount = self._receive_bets(protocol)
            self._send_winners(protocol, agency_id)
            logger.info(
                action,
                logger.LogResult.success,
                "messages-amount",
                message_amount,
            )
        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount"
            )
            raise e
        finally:
            client_socket.close()

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                self._handle_client(client_socket)
