package betserializer

import (
    "encoding/binary"
    "fmt"
)

const (
    agencyByteSize    = 4
	lengthByteSize    = 2
    documentByteSize  = 4
    birthdateByteSize = 10 // YYYY-MM-DD
    numberByteSize    = 4
)

func createPayload(bet bet) []byte {
    payloadSize := agencyByteSize + lengthByteSize + len(bet.firstName) +
        lengthByteSize + len(bet.lastName) + documentByteSize +
        birthdateByteSize + numberByteSize
    payload := make([]byte, payloadSize)
    offset := 0

    binary.BigEndian.PutUint32(payload[offset:], bet.agencyID)
    offset += agencyByteSize
    binary.BigEndian.PutUint16(payload[offset:], uint16(len(bet.firstName)))
    offset += lengthByteSize
    offset += copy(payload[offset:], bet.firstName)
    binary.BigEndian.PutUint16(payload[offset:], uint16(len(bet.lastName)))
    offset += lengthByteSize
    offset += copy(payload[offset:], bet.lastName)
    binary.BigEndian.PutUint32(payload[offset:], bet.document)
    offset += documentByteSize
    offset += copy(payload[offset:], bet.birthdate)
    binary.BigEndian.PutUint32(payload[offset:], bet.number)

    return payload
}

func SerializeBet(bet bet) ([]byte, error) {
    return createPayload(bet), nil
}


func readBytes(payload []byte, offset *int, size int, fieldName string) ([]byte, error) {
    if len(payload)-*offset < size {
        return nil, fmt.Errorf("payload is missing %s", fieldName)
    }

    value := append([]byte(nil), payload[*offset:*offset+size]...)
    *offset += size
    return value, nil
}

func readNumber(payload []byte, offset *int, size int, fieldName string) (uint32, error) {
    value, err := readBytes(payload, offset, size, fieldName)
    if err != nil {
        return 0, err
    }

    switch size {
    case lengthByteSize:
        return uint32(binary.BigEndian.Uint16(value)), nil
    case agencyByteSize:
        return binary.BigEndian.Uint32(value), nil
    default:
        return 0, fmt.Errorf("unsupported numeric field size %d", size)
    }
}

func readText(payload []byte, offset *int, fieldName string) ([]byte, error) {
    size, err := readNumber(payload, offset, lengthByteSize, fieldName+" size")
    if err != nil {
        return nil, err
    }

    return readBytes(payload, offset, int(size), fieldName)
}

func deserializeBet(payload []byte) (bet, error) {
    offset := 0
    agencyID, err := readNumber(payload, &offset, agencyByteSize, "agency ID")
    if err != nil {
        return bet{}, err
    }
    firstName, err := readText(payload, &offset, "first name")
    if err != nil {
        return bet{}, err
    }
    lastName, err := readText(payload, &offset, "last name")
    if err != nil {
        return bet{}, err
    }
    document, err := readNumber(payload, &offset, documentByteSize, "document")
    if err != nil {
        return bet{}, err
    }
    birthdate, err := readBytes(payload, &offset, birthdateByteSize, "birthdate")
    if err != nil {
        return bet{}, err
    }
    number, err := readNumber(payload, &offset, numberByteSize, "number")
    if err != nil {
        return bet{}, err
    }

    if offset != len(payload) {
        return bet{}, fmt.Errorf("payload has unexpected trailing bytes")
    }

    return bet{
        agencyID:  agencyID,
        firstName: firstName,
        lastName:  lastName,
        document:  document,
        birthdate: birthdate,
        number:    number,
    }, nil
}

func DeserializeBet(payload []byte) (bet, error) {
    parsedBet, err := deserializeBet(payload)
    if err != nil {
        return bet{}, err
    }

    return parsedBet, nil
}