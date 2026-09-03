package safe_socket

import (
	"encoding/binary"
	"io"
)



func SendAll(socket io.Writer, bytes []byte) error {
	nSent := 0
	for nSent < len(message) {
		n, err := socket.Write(message[nSent:])
		if err != nil {
			return err
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
		nRecv += n
	}
	return buffer, nil
}
