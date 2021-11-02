import asyncio
import time
import logging
from datetime import datetime

from server import MetCastProtocol, DataContainer

LINE_TEMPLATE = """(S:MAWS;D:{};T:{};  TAAVG1M:{};PA:   {:.1f};FOO:;BAR://///)1234"""


async def data_producer():
    n = 0
    while True:
        yield create_data(n)
        n += 1
        await asyncio.sleep(1)


async def data_stream(container):
    async for data in data_producer():
        print(f"Stream trying to set data")
        await container.set(data)
        yield data


async def data_consumer(container):
    while True:
        async for data in data_stream(container):
            print(f"Consumer received: {data}")


def create_data(n):
    now = datetime.utcnow()
    date_str = now.strftime("%y%m%d")
    time_str = now.strftime("%H%M%S")
    temp = 15.0 + n * 0.1
    pressure = 1010.0 + n * 0.1
    return LINE_TEMPLATE.format(date_str, time_str, temp, pressure)


async def start_server(container):
    loop = asyncio.get_running_loop()
    server = await loop.create_server(
        lambda: MetCastProtocol(container), "127.0.0.1", 42222
    )
    addr = server.sockets[0].getsockname()
    print(f"Serving on {addr}. Press ctrl-c to quit.")

    async with server:
        await server.serve_forever()


async def main():
    container = DataContainer()
    await asyncio.gather(start_server(container), data_consumer(container))


if __name__ == "__main__":
    asyncio.run(main())
