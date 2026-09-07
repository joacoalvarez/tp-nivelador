package protocol

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const (
	OpcodeData byte = 0
	OpcodeAck  byte = 1
	OpcodeErr  byte = 2
	OpcodeFin  byte = 3
)

const opcodeSize = 1
const lengthSize = 4
const headerSize = opcodeSize + lengthSize
const maxPayloadSize = (uint64(1) << (lengthSize * 8)) - 1 // 2^32 bits max

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

func (protocol *Protocol) RecvMessage() (byte, []byte, error) {
	header, err := protocol.getBytes(headerSize)
	if err != nil {
		return 0, nil, err
	}

	opcode := header[0]
	payloadLength := int(binary.BigEndian.Uint32(header[opcodeSize:headerSize]))
	payload, err := protocol.getBytes(payloadLength)
	if err != nil {
		return 0, nil, err
	}

	return opcode, payload, nil
}

func (protocol *Protocol) createMessage(opcode byte, payload []byte) ([]byte, error) {
	if uint64(len(payload)) > maxPayloadSize {
		return nil, fmt.Errorf("payload size %d exceeds maximum %d", len(payload), maxPayloadSize)
	}

	message := make([]byte, headerSize+len(payload))
	message[0] = opcode
	binary.BigEndian.PutUint32(message[opcodeSize:headerSize], uint32(len(payload)))
	copy(message[headerSize:], payload)
	return message, nil
}

func (protocol *Protocol) SendMessage(opcode byte, payload []byte) error {
	message, err := protocol.createMessage(opcode, payload)
	if err != nil {
		return err
	}
	return safe_socket.SendAll(protocol.socket, message)
}

func (protocol *Protocol) SendDataMessage(payload []byte) error {
	return protocol.SendMessage(OpcodeData, payload)
}

func (protocol *Protocol) SendAck() error {
	return protocol.SendMessage(OpcodeAck, []byte{})
}

func (protocol *Protocol) SendErr() error {
	return protocol.SendMessage(OpcodeErr, []byte{})
}

func (protocol *Protocol) SendFin() error {
	return protocol.SendMessage(OpcodeFin, []byte{})
}
