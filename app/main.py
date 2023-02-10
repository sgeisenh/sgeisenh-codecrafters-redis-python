# Uncomment this to pass the first stage
# import socket
import asyncio


async def handle_connection(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
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
