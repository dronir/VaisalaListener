import asyncio
import logging


class MetCastProtocol(asyncio.Protocol):
    def __init__(self, container):
        self.container = container

    def connection_made(self, transport):
        """When a client connects, start a task that writes data to the transport
        every time the data is updated."""
        peername = transport.get_extra_info('peername')
        logging.info(f"Server: New connection from {peername}.")
        self.transport = transport
        loop = asyncio.get_event_loop()
        self.task = loop.create_task(self.serve_data())

    async def serve_data(self):
        """The main task: in an infinite loop, wait for the DataContainer's value to
        change, and when it does, send that data to the client."""
        while True:
            async with self.container.condition:
                await self.container.condition.wait()
                data = await self.container.get()
            logging.debug(f"Server: sending data: {data}")
            self.transport.write(bytes(data, "utf8"))

    def connection_lost(self, exc):
        """When a client disconnects, kill the uploader task and close the transport."""
        logging.info("Server: Connection lost.")
        self.task.cancel()
        self.transport.close()



class DataContainer:
    def __init__(self, cond):
        self.value = ""
        self.condition = cond

    async def get(self):
        return self.value

    async def set(self, value):
        """Acquire condition lock, update value, notify all tasks that wait for update."""
        async with self.condition:
            self.value = value
            self.condition.notify_all()


async def start_server(global_config, container):
    config = global_config["broadcast"]
    loop = asyncio.get_running_loop()
    server = await loop.create_server(lambda: MetCastProtocol(container), "", config["port"])
    addr = server.sockets[0].getsockname()
    logging.info(f'Server: Starting TCP serving on {addr}.')
    async with server:
        await server.serve_forever()
