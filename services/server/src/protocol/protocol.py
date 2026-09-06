import socket
import safe_socket

_HEADER_SIZE = 4
_MAX_PAYLOAD_SIZE = (1 << (_HEADER_SIZE * 8)) - 1 # 2^32 bits max

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

	def recv_message(self):
		header = self._get_bytes(_HEADER_SIZE)
		payload_length = int.from_bytes(header, byteorder='big')
		return self._get_bytes(payload_length)

	@staticmethod
	def is_fin(payload: bytes) -> bool:
		return len(payload) == 0

	def _create_message(self, payload: bytes) -> bytes:
		payload_length = len(payload)
		if payload_length > _MAX_PAYLOAD_SIZE:
			raise ValueError(
				f"payload size {payload_length} exceeds maximum {_MAX_PAYLOAD_SIZE}"
			)
		header = payload_length.to_bytes(_HEADER_SIZE, byteorder='big')
		return header + payload

	def send_message(self, payload: bytes):
		msg = self._create_message(payload)
		safe_socket.send_all(self._socket, msg)

	def send_fin(self):
		self.send_message(b"")