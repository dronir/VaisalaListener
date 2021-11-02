import asyncio
import logging


class MetCastProtocol(asyncio.Protocol):
    def __init__(self, container):
        self.container = container
        self.transport = None
        self.task = None

    def connection_made(self, transport):
        """When a client connects, start a task that writes data to the transport
        every time the container is updated.
        """
        peername = transport.get_extra_info("peername")
        logging.info(f"Server: New connection from {peername}.")
        self.transport = transport
        loop = asyncio.get_event_loop()
        self.task = loop.create_task(self.serve_data())

    async def serve_data(self):
        """The main task: in an infinite loop, wait for the DataContainer's value to
        change, and when it does, send that data to the client.
        """
        # Reminder for future self about how this async condition works:
        # - Entering the `async with` block acquires the condition's lock (waiting for it if necessary).
        # - `condition.wait()` releases the lock, then blocks until the container value is updated.
        # - When notified, the `wait()` call reacquires the lock and returns.
        # - The new value is retrieved from the container with `container.get()`.
        # - Exiting the `with` block releases the lock once again.
        # - Then we send the data we just read and loop back.
        while True:
            async with self.container.condition:
                await self.container.condition.wait()
                data = self.container.get()
            logging.debug(f"Server: sending data: {data}")
            self.transport.write(bytes(data, "utf8"))

    def connection_lost(self, exc):
        """When a client disconnects, kill the uploader task and close the transport."""
        logging.info("Server: Connection lost.")
        self.task.cancel()
        self.transport.close()


class DataContainer:
    """A class that wraps a value and an async Condition. This lets other processes "subscribe"
    to updates of the value by waiting on the Condition. When `set(self, value)` is called,
    the waiting processes will be notified and can get the new value.
    This lets us do stuff with the value every time it changes, and only when it changes.
    """

    def __init__(self):
        self.value = ""
        self.condition = asyncio.Condition()

    def get(self):
        return self.value

    async def set(self, value):
        """Acquire condition lock, update value, notify all tasks that wait for update."""
        # Reminder for future self part 2:
        # - Entering the `async with` block acquires the condition's lock (waiting if necessary).
        # - We update the value in the container.
        # – Then `notify_all` releases the lock and notifies every process that is waiting for it.
        # – One by one, those processes acquire the lock, call `.get()` on this container, and release the lock.
        # - After everyone has done their thing, `notify_all()` reacquires the lock.
        # – Then exiting the `async with` block releases the lock again.
        async with self.condition:
            self.value = value
            self.condition.notify_all()


async def start_server(global_config, container):
    config = global_config["broadcast"]
    loop = asyncio.get_running_loop()
    server = await loop.create_server(
        lambda: MetCastProtocol(container), "", config["port"]
    )
    addr = server.sockets[0].getsockname()
    logging.info(f"Server: Starting TCP serving on {addr}.")
    async with server:
        await server.serve_forever()
