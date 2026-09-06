from lottery.bet import Bet

_SERIALIZED_AGENCY_SIZE = 4
_SERIALIZED_LENGTH_SIZE = 2
_SERIALIZED_DOCUMENT_SIZE = 4
_SERIALIZED_BIRTHDATE_SIZE = 10  # YYYY-MM-DD
_SERIALIZED_NUMBER_SIZE = 4
_MAX_SERIALIZED_LENGTH_SIZE = (1 << (_SERIALIZED_LENGTH_SIZE * 8)) - 1  # 2^16 bits max
_BYTE_ORDER = "big"

def _to_bytes_checked(value: int, size: int, field_name: str) -> bytes:
    try:
        return value.to_bytes(size, _BYTE_ORDER)
    except OverflowError:
        raise ValueError(f"{field_name} does not fit in {size} bytes")

def serialize_bet(bet: Bet) -> bytes:
    first_name = bet.first_name.encode()
    last_name = bet.last_name.encode()
    birthdate = bet.birthdate.encode()

    if len(first_name) > _MAX_SERIALIZED_LENGTH_SIZE:
        raise ValueError("first name is too long")
    if len(last_name) > _MAX_SERIALIZED_LENGTH_SIZE:
        raise ValueError("last name is too long")
    if len(birthdate) != _SERIALIZED_BIRTHDATE_SIZE:
        raise ValueError(f"birthdate must have {_SERIALIZED_BIRTHDATE_SIZE} bytes")

    payload = _to_bytes_checked(bet.agency_id, _SERIALIZED_AGENCY_SIZE, "agency_id")
    payload += len(first_name).to_bytes(_SERIALIZED_LENGTH_SIZE, _BYTE_ORDER) + first_name
    payload += len(last_name).to_bytes(_SERIALIZED_LENGTH_SIZE, _BYTE_ORDER) + last_name
    payload += _to_bytes_checked(bet.document, _SERIALIZED_DOCUMENT_SIZE, "document")
    payload += birthdate
    payload += _to_bytes_checked(bet.number, _SERIALIZED_NUMBER_SIZE, "number")
    return payload

def read_bytes(payload: bytes, offset: int, size: int, field_name: str) -> tuple[bytes, int]:
    if len(payload) - offset < size:
        raise ValueError(f"payload is missing {field_name}")
    return payload[offset:offset + size], offset + size

def read_number(payload: bytes, offset: int, size: int, field_name: str) -> tuple[int, int]:
    value, offset = read_bytes(payload, offset, size, field_name)
    return int.from_bytes(value, _BYTE_ORDER), offset

def read_text(payload: bytes, offset: int, field_name: str) -> tuple[str, int]:
    size, offset = read_number(
        payload, offset, _SERIALIZED_LENGTH_SIZE, f"{field_name} size"
    )
    value, offset = read_bytes(payload, offset, size, field_name)
    return value.decode("utf-8"), offset

def deserialize_bet_at(payload: bytes, offset: int) -> tuple[Bet, int]:

    agency_id, offset = read_number(payload, offset, _SERIALIZED_AGENCY_SIZE, "agency ID")
    first_name, offset = read_text(payload, offset, "first name")
    last_name, offset = read_text(payload, offset, "last name")
    document, offset = read_number(payload, offset, _SERIALIZED_DOCUMENT_SIZE, "document")
    birthdate_bytes, offset = read_bytes(payload, offset, _SERIALIZED_BIRTHDATE_SIZE, "birthdate")
    birthdate = birthdate_bytes.decode("utf-8")
    number, offset = read_number(payload, offset, _SERIALIZED_NUMBER_SIZE, "number")

    return Bet(
        agency_id=agency_id,
        first_name=first_name,
        last_name=last_name,
        document=document,
        birthdate=birthdate,
        number=number,
    ), offset

def deserialize_bets(payload: bytes) -> list[Bet]:
    bets = []
    offset = 0
    while offset < len(payload):
        bet, offset = deserialize_bet_at(payload, offset)
        bets.append(bet)
    return bets