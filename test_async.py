import asyncio
import time
import logging
from datetime import datetime


LINE_TEMPLATE = """(S:MAWS;D:{};T:{};TAAVG1M:{};PA:{:.1f})"""


class MetCastProtocol(asyncio.Protocol):
    def __init__(self, container):
        self.container = container

    def connection_made(self, transport):
        print("New connection.")
        self.transport = transport
        loop = asyncio.get_event_loop()
        self.task = loop.create_task(self.serve_data())

    def send(self, payload):
        print(payload)
        self.transport.write(bytes(payload, "utf8"))

    def connection_lost(self, exc):
        print("Connection lost.")
        self.task.cancel()
        self.transport.close()

    async def serve_data(self):
        while True:
            async with self.container.lock:
                await self.container.lock.wait()
                data = await self.container.get()
            print(f"Serving: {data}")
            self.send(data)

class DataContainer:
    def __init__(self, lock):
        self.value = ""
        self.lock = lock
    async def get(self):
        return self.value
    async def set(self, value):
        async with self.lock:
            print(f"Container: set value: {value}")
            self.value = value
            self.lock.notify_all()


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
    server = await loop.create_server(lambda: MetCastProtocol(container), '127.0.0.1', 42222)
    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}. Press ctrl-c to quit.')

    async with server:
        await server.serve_forever()

async def main():
    cond = asyncio.Condition()
    container = DataContainer(cond)
    await asyncio.gather(start_server(container), data_consumer(container))

if __name__=="__main__":
    asyncio.run(main())
