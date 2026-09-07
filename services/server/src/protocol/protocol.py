import socket
import safe_socket

OPCODE_DATA = 0
OPCODE_ACK = 1
OPCODE_ERR = 2
OPCODE_FIN = 3

_OPCODE_SIZE = 1
_LENGTH_SIZE = 4
_HEADER_SIZE = _OPCODE_SIZE + _LENGTH_SIZE
_MAX_PAYLOAD_SIZE = (1 << (_LENGTH_SIZE * 8)) - 1  # 2^32 bits max

class Protocol:
	def __init__(self, socket: socket.socket):
		self._socket = socket
		self._buffer = bytearray()

	def _get_bytes(self, size: int):
		while len(self._buffer) < size:
			missing = size - len(self._buffer)
			chunk = safe_socket.recv_all(self._socket, missing)
			self._buffer.extend(chunk)

		result = self._buffer[:size]
		del self._buffer[:size]
		return bytes(result)

	def recv_message(self) -> tuple[int, bytes]:
		header = self._get_bytes(_HEADER_SIZE)
		opcode = header[0]
		payload_length = int.from_bytes(header[_OPCODE_SIZE:_HEADER_SIZE], byteorder='big')
		return opcode, self._get_bytes(payload_length)

	def _create_message(self, opcode:int, payload: bytes) -> bytes:
		payload_length = len(payload)
		if payload_length > _MAX_PAYLOAD_SIZE:
			raise ValueError(
				f"payload size {payload_length} exceeds maximum {_MAX_PAYLOAD_SIZE}"
			)
		opcode_bytes = opcode.to_bytes(_OPCODE_SIZE, byteorder="big")
		length_bytes = payload_length.to_bytes(_LENGTH_SIZE, byteorder='big')
		return opcode_bytes + length_bytes + payload

	def send_message(self, opcode: int, payload: bytes = b""):
		msg = self._create_message(opcode, payload)
		safe_socket.send_all(self._socket, msg)
	
	def send_data(self, payload: bytes):
		self.send_message(OPCODE_DATA, payload)

	def send_ack(self):
		self.send_message(OPCODE_ACK, b"")

	def send_err(self):
		self.send_message(OPCODE_ERR, b"")

	def send_fin(self):
		self.send_message(OPCODE_FIN, b"")