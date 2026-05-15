"""Binary envelope format (must match core/envelope in Go)."""
from __future__ import annotations

import struct
import time
import uuid
from dataclasses import dataclass

MAGIC = b"BMQ1"
VERSION = 1
HEADER_FIXED = 47


@dataclass
class Envelope:
    idempotency_id: uuid.UUID
    server_msg_id: uuid.UUID
    publish_ts_ns: int
    user_props: bytes
    payload: bytes

    @staticmethod
    def new_publish(payload: bytes, idempotency_id: uuid.UUID | None = None) -> "Envelope":
        if idempotency_id is None:
            idempotency_id = uuid.uuid4()
        return Envelope(
            idempotency_id=idempotency_id,
            server_msg_id=uuid.UUID(int=0),
            publish_ts_ns=time.time_ns(),
            user_props=b"",
            payload=payload,
        )

    def encode(self) -> bytes:
        props = self.user_props or b""
        out = bytearray()
        out += MAGIC
        out += bytes([VERSION])
        out += self.idempotency_id.bytes
        out += self.server_msg_id.bytes
        out += struct.pack(">Q", self.publish_ts_ns & 0xFFFFFFFFFFFFFFFF)
        out += struct.pack(">H", len(props))
        out += props
        out += self.payload
        return bytes(out)

    @staticmethod
    def decode(data: bytes) -> "Envelope":
        if len(data) < HEADER_FIXED or data[:4] != MAGIC:
            raise ValueError("not an envelope")
        if data[4] != VERSION:
            raise ValueError("unsupported version")
        idem = uuid.UUID(bytes=data[5:21])
        srv = uuid.UUID(bytes=data[21:37])
        ts = struct.unpack(">Q", data[37:45])[0]
        plen = struct.unpack(">H", data[45:47])[0]
        props = data[47 : 47 + plen]
        payload = data[47 + plen :]
        return Envelope(idem, srv, ts, props, payload)
