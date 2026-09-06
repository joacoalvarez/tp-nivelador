package safe_socket

import (
	"io"
)



func SendAll(socket io.Writer, bytes []byte) error {
	nSent := 0
	for nSent < len(bytes) {
		n, err := socket.Write(bytes[nSent:])
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		nSent += n
	}
	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buffer := make([]byte, size)
	nRecv := 0

	for nRecv < size {
		n, err := socket.Read(buffer[nRecv:])
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, io.ErrNoProgress
		}
		nRecv += n
	}
	return buffer, nil
}
