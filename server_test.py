import asyncio
import time
import logging
from datetime import datetime


LINE_TEMPLATE = """(S:MAWS;D:{};T:{};TAAVG1M:{};PA:{:.1f})"""


def create_data(n):
    now = datetime.utcnow()
    date_str = now.strftime("%y%m%d")
    time_str = now.strftime("%H%M%S")
    temp = 15.0 + n * 0.1
    pressure = 1010.0 + n * 0.1
    return LINE_TEMPLATE.format(date_str, time_str, temp, pressure)


async def handle_echo(reader, writer):

    addr = writer.get_extra_info('peername')
    print(f"Connection from {addr!r}")

    n = 0
    try:
        while True:
            if writer.is_closing():
                break
            print(f"Sending to {addr!r}.")
            n += 1
            writer.write(bytes(create_data(n), "utf-8"))
            await writer.drain()
            await asyncio.sleep(1)
    except ConnectionResetError:
        pass
    except KeyboardInterrupt:
        pass

    print(f"Connection to {addr!r} closed.")
    writer.close()


async def main():
    server = await asyncio.start_server(handle_echo, '127.0.0.1', 42222)
    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}. Press ctrl-c to quit.')

    async with server:
        await server.serve_forever()

asyncio.run(main())
