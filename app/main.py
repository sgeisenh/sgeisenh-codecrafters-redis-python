import asyncio
from dataclasses import dataclass
from typing import Union

RespValue = Union["SimpleString", "Error", "Integer", "BulkString", "Array", "Nil"]


@dataclass
class SimpleString:
    value: bytes

    def encode(self) -> bytes:
        return b"+" + self.value + b"\r\n"


@dataclass
class Error:
    message: bytes

    def encode(self) -> bytes:
        return b"-" + self.message + b"\r\n"


@dataclass
class Integer:
    value: int

    def encode(self) -> bytes:
        return b":" + str(self.value).encode() + b"\r\n"


@dataclass
class BulkString:
    value: bytes

    def encode(self) -> bytes:
        return b"$" + str(len(self.value)).encode() + b"\r\n" + self.value + b"\r\n"


@dataclass
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


@dataclass
class Nil:
    def encode(self) -> bytes:
        return b"$-1\r\n"


async def parse_resp_value(reader: asyncio.StreamReader) -> RespValue:
    byte = await reader.readexactly(1)
    match byte:
        case b"+":
            return SimpleString((await reader.readuntil(b"\r\n"))[:-2])
        case b"-":
            return Error((await reader.readuntil(b"\r\n"))[:-2])
        case b":":
            return Integer(int((await reader.readuntil(b"\r\n"))[:-2]))
        case b"$":
            length = int((await reader.readuntil(b"\r\n"))[:-2])
            if length == -1:
                return Nil()
            value = await reader.readexactly(length + 2)
            assert value[-2:] == b"\r\n"
            return BulkString(value[:-2])
        case b"*":
            length = int((await reader.readuntil(b"\r\n"))[:-2])
            return Array([await parse_resp_value(reader) for _ in range(length)])
        case _:
            raise ValueError(f"Invalid first byte: {byte!r}")


async def handle_connection(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    try:
        while True:
            value = await parse_resp_value(reader)
            print(f"Received value: {value!r}")
            match value:
                case Array([BulkString(b"ping")]):
                    writer.write(b"+PONG\r\n")
                    await writer.drain()
                case Array([BulkString(b"ping"), BulkString(message)]):
                    writer.write(BulkString(message).encode())
                    await writer.drain()
                case Array([BulkString(b"echo"), BulkString(message)]):
                    writer.write(BulkString(message).encode())
                    await writer.drain()
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
