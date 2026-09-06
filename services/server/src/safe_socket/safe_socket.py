import socket


def recv_all(socket: socket.socket, size):
    buffer = bytearray()

    while len(buffer) < size:
        chunk = socket.recv(size - len(buffer))
        if not chunk:
            raise ConnectionError("socket closed before receiving all data")
        buffer.extend(chunk)

    return bytes(buffer)

def send_all(socket: socket.socket, bytes):
    n_sent = 0
    
    while n_sent < len(bytes):
        sent = socket.send(bytes[n_sent:])
        n_sent += sent
