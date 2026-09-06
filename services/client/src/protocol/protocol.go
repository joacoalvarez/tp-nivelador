package protocol

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const headerSize = 4
const maxPayloadSize = (uint64(1) << (headerSize * 8)) - 1 // 2^32 bits max

type Protocol struct {
	socket io.ReadWriter
	buffer []byte
}

func NewProtocol(socket io.ReadWriter) *Protocol {
	return &Protocol{
		socket: socket,
		buffer: make([]byte, 0),
	}
}

func (protocol *Protocol) getBytes(size int) ([]byte, error) {
	for len(protocol.buffer) < size {
		missing := size - len(protocol.buffer)
		chunk, err := safe_socket.RecvAll(protocol.socket, missing)
		if err != nil {
			return nil, err
		}
		protocol.buffer = append(protocol.buffer, chunk...)
	}

	result := append([]byte(nil), protocol.buffer[:size]...)
	protocol.buffer = protocol.buffer[size:]
	return result, nil
}

func (protocol *Protocol) RecvMessage() ([]byte, error) {
	header, err := protocol.getBytes(headerSize)
	if err != nil {
		return nil, err
	}

	payloadLength := int(binary.BigEndian.Uint32(header))
	return protocol.getBytes(payloadLength)
}

func IsFin(payload []byte) bool {
	return len(payload) == 0
}

func (protocol *Protocol) createMessage(payload []byte) ([]byte, error) {
	if uint64(len(payload)) > maxPayloadSize {
		return nil, fmt.Errorf("payload size %d exceeds maximum %d", len(payload), maxPayloadSize)
	}

	message := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(message[:headerSize], uint32(len(payload)))
	copy(message[headerSize:], payload)
	return message, nil
}

func (protocol *Protocol) SendMessage(payload []byte) error {
	message, err := protocol.createMessage(payload)
	if err != nil {
		return err
	}
	return safe_socket.SendAll(protocol.socket, message)
}

// Empty message indicates end of transmission
func (protocol *Protocol) SendFin() error {
	return protocol.SendMessage([]byte{})
}
