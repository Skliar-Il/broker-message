"""Async MQTT client with envelope + consumer dedup."""
from __future__ import annotations

import asyncio
import struct
import uuid
from collections import OrderedDict
from typing import AsyncIterator, Callable, Optional

from .envelope import Envelope


class SeenCache:
    def __init__(self, capacity: int = 10_000) -> None:
        self.capacity = capacity
        self._order: OrderedDict[uuid.UUID, None] = OrderedDict()

    def seen(self, msg_id: uuid.UUID) -> bool:
        if msg_id.int == 0:
            return False
        if msg_id in self._order:
            return True
        self._order[msg_id] = None
        if len(self._order) > self.capacity:
            self._order.popitem(last=False)
        return False


class Client:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 1883,
        client_id: str = "brokermq-py",
        username: str = "",
        password: str = "",
        qos: int = 1,
    ) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self.username = username
        self.password = password
        self.qos = qos
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._seen = SeenCache()
        self._next_pid = 1

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
        await self._mqtt_connect()
        await self._read_connack()

    async def close(self) -> None:
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()

    async def publish(self, topic: str, payload: bytes, idempotency_key: Optional[uuid.UUID] = None) -> None:
        env = Envelope.new_publish(payload, idempotency_key)
        wire = env.encode()
        pid = 0
        if self.qos > 0:
            pid = self._next_pid
            self._next_pid = (self._next_pid % 65535) + 1
        await self._write_publish(topic, wire, self.qos, pid, False)
        if self.qos > 0:
            await self._read_puback()

    async def subscribe(self, topic_filter: str, handler: Callable[[Envelope, str], None]) -> None:
        await self._write_subscribe(topic_filter, self.qos)
        await self._read_suback()
        asyncio.create_task(self._consume_loop(handler))

    async def messages(self, topic_filter: str) -> AsyncIterator[tuple[str, Envelope]]:
        q: asyncio.Queue[tuple[str, Envelope]] = asyncio.Queue()

        async def _h(env: Envelope, topic: str) -> None:
            await q.put((topic, env))

        await self.subscribe(topic_filter, lambda e, t: asyncio.create_task(_h(e, t)))
        while True:
            yield await q.get()

    async def _consume_loop(self, handler: Callable[[Envelope, str], None]) -> None:
        assert self._reader
        while True:
            pkt_type, flags, body = await self._read_packet()
            if pkt_type != 3:
                continue
            topic, wire, pid = _parse_publish(body, (flags >> 1) & 0x03)
            try:
                env = Envelope.decode(wire)
            except ValueError:
                env = Envelope(uuid.uuid4(), uuid.UUID(int=0), 0, b"", wire)
            if self._seen.seen(env.server_msg_id):
                if pid:
                    await self._write_puback(pid)
                continue
            handler(env, topic)
            if pid:
                await self._write_puback(pid)

    async def _mqtt_connect(self) -> None:
        vh = bytearray()
        vh += struct.pack(">H", 4) + b"MQTT" + bytes([4])
        flags = 0x02
        if self.username:
            flags |= 0x80
            if self.password:
                flags |= 0x40
        vh += bytes([flags]) + struct.pack(">H", 60)
        cid = self.client_id.encode()
        vh += struct.pack(">H", len(cid)) + cid
        if self.username:
            u = self.username.encode()
            vh += struct.pack(">H", len(u)) + u
        if self.password:
            p = self.password.encode()
            vh += struct.pack(">H", len(p)) + p
        pkt = bytes([0x10]) + _encode_remaining(len(vh)) + bytes(vh)
        await self._write(pkt)

    async def _read_connack(self) -> None:
        data = await self._read_exact(4)
        if data[3] != 0:
            raise ConnectionError(f"connack refused {data[3]}")

    async def _write_publish(self, topic: str, payload: bytes, qos: int, pid: int, dup: bool) -> None:
        tb = topic.encode()
        vh = struct.pack(">H", len(tb)) + tb
        if qos > 0:
            vh += struct.pack(">H", pid)
        flags = (qos & 0x03) << 1
        if dup:
            flags |= 0x08
        pkt = bytes([(0x03 << 4) | flags]) + _encode_remaining(len(vh) + len(payload)) + vh + payload
        await self._write(pkt)

    async def _write_subscribe(self, topic: str, qos: int) -> None:
        tb = topic.encode()
        body = struct.pack(">H", 1) + struct.pack(">H", len(tb)) + tb + bytes([qos & 0x03])
        pkt = bytes([0x82]) + _encode_remaining(len(body)) + body
        await self._write(pkt)

    async def _read_suback(self) -> None:
        await self._read_packet()

    async def _read_puback(self) -> None:
        await self._read_packet()

    async def _write_puback(self, pid: int) -> None:
        await self._write(bytes([0x40, 0x02, (pid >> 8) & 0xFF, pid & 0xFF]))

    async def _read_packet(self) -> tuple[int, int, bytes]:
        h = await self._read_exact(1)
        pkt_type = h[0] >> 4
        flags = h[0] & 0x0F
        rem = await self._read_remaining_length()
        body = await self._read_exact(rem) if rem else b""
        return pkt_type, flags, body

    async def _read_remaining_length(self) -> int:
        length = 0
        shift = 0
        while True:
            b = await self._read_exact(1)
            length |= (b[0] & 0x7F) << shift
            if not (b[0] & 0x80):
                break
            shift += 7
        return length

    async def _read_exact(self, n: int) -> bytes:
        assert self._reader
        data = await self._reader.readexactly(n)
        return data

    async def _write(self, data: bytes) -> None:
        assert self._writer
        self._writer.write(data)
        await self._writer.drain()


def _encode_remaining(length: int) -> bytes:
    out = bytearray()
    while True:
        b = length & 0x7F
        length >>= 7
        if length:
            b |= 0x80
        out.append(b)
        if not length:
            break
    return bytes(out)


def _parse_publish(body: bytes, qos: int) -> tuple[str, bytes, int]:
    tl = struct.unpack(">H", body[:2])[0]
    pos = 2 + tl
    topic = body[2:pos].decode()
    pid = 0
    if qos > 0:
        pid = struct.unpack(">H", body[pos : pos + 2])[0]
        pos += 2
    return topic, body[pos:], pid
