import socket
import logger

# TODO: Complete with a short-read/short-write tolerant implementation


def recv_all(socket: socket.socket, size):
    buff = []
    n_recv = 0

    while n_recv < size:
        aux = socket.recv(size)
        n_recv += len(aux)
        buff.append(aux)

    return buff

def send_all(socket: socket.socket, bytes):
    n_sent = 0
    
    while n_sent < len(bytes):
        sent = socket.send(bytes[n_sent:])
        if sent == 0:
            logger.error("socket-close-send", logger.LogResult.fail, "err")
            raise(ConnectionError("socket closed before sending all data"))
        n_sent += sent
