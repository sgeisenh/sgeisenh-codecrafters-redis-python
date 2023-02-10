import asyncio
import time
from dataclasses import dataclass
from typing import Union

RespValue = Union["SimpleString", "Error", "Integer", "BulkString", "Array", "Nil"]


@dataclass(frozen=True)
class SimpleString:
    value: bytes

    def encode(self) -> bytearray:
        result = bytearray()
        result.extend(b"+")
        result.extend(self.value)
        result.extend(b"\r\n")
        return result


@dataclass(frozen=True)
class Error:
    message: bytes

    def encode(self) -> bytearray:
        result = bytearray()
        result.extend(b"-")
        result.extend(self.message)
        result.extend(b"\r\n")
        return result


@dataclass(frozen=True)
class Integer:
    value: int

    def encode(self) -> bytearray:
        result = bytearray()
        result.extend(b":")
        result.extend(str(self.value).encode())
        result.extend(b"\r\n")
        return result


@dataclass(frozen=True)
class BulkString:
    value: bytes

    def encode(self) -> bytearray:
        result = bytearray()
        result.extend(b"$")
        result.extend(str(len(self.value)).encode())
        result.extend(b"\r\n")
        result.extend(self.value)
        result.extend(b"\r\n")
        return result


@dataclass(frozen=True)
class Array:
    value: list[RespValue]

    def encode(self) -> bytearray:
        result = bytearray()
        result.extend(b"*")
        result.extend(str(len(self.value)).encode())
        result.extend(b"\r\n")
        for elem in self.value:
            result.extend(elem.encode())
        return result


@dataclass(frozen=True)
class Nil:
    def encode(self) -> bytes:
        return b"$-1\r\n"


async def parse_resp_value(reader: asyncio.StreamReader) -> RespValue:
    byte = await reader.readexactly(1)

    async def read_until_clrf() -> memoryview:
        return memoryview(await reader.readuntil(b"\r\n"))[:-2]

    match byte:
        case b"+":
            return SimpleString(await read_until_clrf())
        case b"-":
            return Error(await read_until_clrf())
        case b":":
            return Integer(int(await read_until_clrf()))
        case b"$":
            length = int(await read_until_clrf())
            if length == -1:
                return Nil()
            value = memoryview(await reader.readexactly(length + 2))
            assert value[-2:] == b"\r\n"
            return BulkString(value[:-2])
        case b"*":
            length = int(await read_until_clrf())
            return Array([await parse_resp_value(reader) for _ in range(length)])
        case _:
            raise ValueError(f"Invalid first byte: {byte!r}")


state: dict[bytes, tuple[bytes, float | None]] = {}


async def handle_connection(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    async def write(value: RespValue) -> None:
        writer.write(value.encode())
        await writer.drain()

    try:
        while True:
            client_message = await parse_resp_value(reader)
            match client_message:
                case Array([BulkString(b"ping")]):
                    await write(SimpleString(b"PONG"))
                case Array([BulkString(b"ping"), BulkString(message)]):
                    await write(BulkString(message))
                case Array([BulkString(b"echo"), BulkString(message)]):
                    await write(BulkString(message))
                case Array(
                    [
                        BulkString(b"set"),
                        BulkString(key),
                        BulkString(value),
                        BulkString(b"px"),
                        BulkString(expiry_ms_bytes),
                    ]
                ):
                    now = time.time()
                    state[key] = value, now + int(expiry_ms_bytes) / 1000
                    await write(SimpleString(b"OK"))
                case Array([BulkString(b"set"), BulkString(key), BulkString(value)]):
                    state[key] = value, None
                    await write(SimpleString(b"OK"))
                case Array([BulkString(b"get"), BulkString(key)]):
                    result = state.get(key)
                    if result is None:
                        await write(Nil())
                        continue
                    value, expiry_ms = result
                    if expiry_ms is not None and time.time() > expiry_ms:
                        del state[key]
                        await write(Nil())
                        continue
                    await write(BulkString(value))
                case _:
                    raise NotImplementedError()
    except Exception:
        return


async def main() -> int:
    server = await asyncio.start_server(handle_connection, "0.0.0.0", 6379)

    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print(f"Serving on {addrs}", flush=True)

    async with server:
        await server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
