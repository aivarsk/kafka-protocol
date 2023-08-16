from io import BytesIO

import pytest

from kafkaprotocol.ktypes import *


def roundtrip(klass, value):
    binary = klass.pack(value)
    bytesio = BytesIO(binary)
    plain = klass.unpack(bytesio)
    assert bytesio.read() == b""
    assert value == plain


def test_varint():
    roundtrip(VARINT, 0)
    roundtrip(VARINT, 1)
    roundtrip(VARINT, 1_000)
    roundtrip(VARINT, 1_000_000)
    roundtrip(VARINT, 1_000_000_000)
    roundtrip(VARINT, -1)
    roundtrip(VARINT, -1_000)
    roundtrip(VARINT, -1_000_000)
    roundtrip(VARINT, -1_000_000_000)


def test_nullable():
    roundtrip(NULLABLE_BYTES, None)
    roundtrip(NULLABLE_BYTES, b"1234567890")
    roundtrip(NULLABLE_STRING, None)
    roundtrip(NULLABLE_STRING, "1234567890")


def test_compact_nullable():
    roundtrip(COMPACT_NULLABLE_BYTES, None)
    roundtrip(COMPACT_NULLABLE_BYTES, b"1234567890")
    roundtrip(COMPACT_NULLABLE_BYTES, b"1234567890" * 26)
    roundtrip(COMPACT_NULLABLE_STRING, None)
    roundtrip(COMPACT_NULLABLE_STRING, "1234567890")
    roundtrip(COMPACT_NULLABLE_STRING, "1234567890" * 26)


def test_nonnull():
    with pytest.raises(ValueError):
        STRING.pack(None)
    with pytest.raises(ValueError):
        STRING.unpack(BytesIO(NULLABLE_STRING.pack(None)))

    with pytest.raises(ValueError):
        BYTES.pack(None)
    with pytest.raises(ValueError):
        BYTES.unpack(BytesIO(NULLABLE_BYTES.pack(None)))
