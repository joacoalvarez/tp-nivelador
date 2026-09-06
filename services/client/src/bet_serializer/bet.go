package betserializer

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	recordFieldCount = 5

	firstNameFieldIndex = 0
	lastNameFieldIndex  = 1
	documentFieldIndex  = 2
	birthdateFieldIndex = 3
	numberFieldIndex    = 4

	maxTextFieldSize = (1 << (lengthByteSize * 8)) - 1
)

type bet struct {
	agencyID  uint32
	firstName []byte
	lastName  []byte
	document  uint32
	birthdate []byte
	number    uint16
}

func ParseBet(record string, agencyID string) (bet, error) {
	fields := strings.Split(record, ",")
	if len(fields) != recordFieldCount {
		return bet{}, fmt.Errorf("expected %d fields", recordFieldCount)
	}

	firstName := []byte(fields[firstNameFieldIndex])
	lastName := []byte(fields[lastNameFieldIndex])
	birthdate := []byte(fields[birthdateFieldIndex])

	if len(firstName) > maxTextFieldSize {
		return bet{}, fmt.Errorf("first name is too long")
	}
	if len(lastName) > maxTextFieldSize {
		return bet{}, fmt.Errorf("last name is too long")
	}
	if len(birthdate) != birthdateByteSize {
		return bet{}, fmt.Errorf("birthdate must have %d bytes", birthdateByteSize)
	}

	agency, err := strconv.ParseUint(agencyID, 10, 32)
	if err != nil {
		return bet{}, err
	}
	document, err := strconv.ParseUint(fields[documentFieldIndex], 10, 32)
	if err != nil {
		return bet{}, err
	}
	number, err := strconv.ParseUint(fields[numberFieldIndex], 10, 16)
	if err != nil {
		return bet{}, err
	}

	return bet{
		agencyID:  uint32(agency),
		firstName: firstName,
		lastName:  lastName,
		document:  uint32(document),
		birthdate: birthdate,
		number:    uint16(number),
	}, nil
}

func BetToCSV(bet bet) []byte {
	return fmt.Appendf(nil, "%s,%s,%d,%s,%d",
		bet.firstName,
		bet.lastName,
		bet.document,
		bet.birthdate,
		bet.number,
	)
}