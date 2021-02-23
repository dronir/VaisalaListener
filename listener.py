# Robust Vaisala listener, with error handling and backup buffer

import logging
import sys

import asyncio
import aiohttp
import serial_asyncio

import re
import toml
import time
from datetime import datetime, timezone
from os.path import exists

from server import DataContainer, MetCastProtocol, start_server

#
# InfluxDB uploader
#

async def uploader(global_config, listener):
    """Collect data from queue and upload to InfluxDB or backup.
    """
    config = global_config["uploader"]
    batch = []
    has_failed = False
    logging.info("Uploader: Starting up uploader thread.")

    # Check if there's backup data from previous runs
    backup_ok, batch, E = load_backup(config)
    if not backup_ok:
        logging.error(f"Uploader: Tried to load possible backup data but failed:\n{repr(E)}")
    if len(batch) > 0:
        logging.info(f"Uploader: Loaded {len(batch)} data points from backup.")

    credentials = aiohttp.BasicAuth(login=config["user"], password=config["password"])

    async with aiohttp.ClientSession(auth=credentials) as session:
        if not await check_database(config, session):
            logging.warning(f"Uploader: InfluxDB database at {config['host']}:{config['port']} seems to be down.")

        parser = message_parser(global_config, listener)

        while True:
            while len(batch) < config["batch_size"]:
                # Await for an item coming from the parser
                new_item = await parser.__anext__()
                if new_item is None:
                    continue
                else:
                    batch.append(new_item)
            # TODO: maybe split the following off into a separate function:
            if len(batch) > 0:
                logging.debug("Uploader: Trying to upload batch.")
                payload = "\n".join(batch)
                logging.debug(f"Uploader: Payload:\n{payload}")
                success = await upload_influxdb(config, session, payload)
                if success:
                    logging.debug("Uploader: Upload successful.")
                    batch = []
                    if has_failed:
                        logging.info("Uploader: Trying to read backup buffer.")
                        load_success, batch, E = load_backup(config)
                        if load_success:
                            logging.info(f"Uploader: Found {len(batch)} items in backup.")
                            has_failed = False
                        else:
                            logging.error(f"Uploader: Failed to read backup buffer:\n{repr(E)}")

                else:
                    has_failed = True
                    if config["backup"]:
                        logging.warning("Uploader: Upload failed. Attempting backup...")
                        backup_ok = store_backup(config, payload)
                        if backup_ok:
                            logging.warning(f"Uploader: Backed up data to {config['backup_file']}.")
                        else:
                            logging.error(f"Uploader: Failed to back up data to {config['backup_file']}")
                    batch = []

    logging.info("Uploader: Shutting down.")



async def upload_influxdb(config, session, payload):
    """Upload given payload to InfluxDB instance using given `aiohttp.ClientSession`.

    `Config` is the "uploader" config subset.
    """
    upload_url = build_database_url(config, "write")
    params = {
        "db" : config["database"],
        "precision" : "ms"
    }
    logging.debug(f"Uploader: Trying to upload with url {upload_url}")

    try:
        async with session.post(upload_url, params=params, data=payload, ssl=False) as response:
            if response.status == 204:
                return True
            else:
                logging.error(f"Uploader: Failed to upload. Status code: {response.status}")
                return False
    except aiohttp.client_exceptions.ClientConnectorError:
        logging.error(f"Uploader: Failed to connect to InfluxDB.")
        return False
    #except Exception:
    #    logging.error(f"Uploader: Unexpected error while uploading:\n{repr(E)}")
    #    return False


def store_backup(config, payload):
    """Store the given payload in a text file.

    `Config` is the "uploader" config subset.
    """
    try:
        with open(config["backup_file"], "a") as f:
            f.write(payload)
            f.write("\n")
    except Exception as E:
        return False, E
    else:
        return True, None


def load_backup(config):
    """Retrieve a payload from backup text file.

    `Config` is the "uploader" config subset.
    """
    batch = []
    try:
        if not exists(config["backup_file"]):
            return True, [], None
        with open(config["backup_file"], "r") as f:
            for line in f:
                sline = line.strip()
                if len(sline) > 0:
                    batch.append(sline)
    except Exception as E:
        return False, [], E

    # Clear file if data was loaded successfully.
    with open(config["backup_file"], "w") as f:
        pass
    return True, batch, None




def build_database_url(config, endpoint):
    """Build url to InfluxDB database based on config variables and server endpoint.
    """
    host = config["host"]
    port = config["port"]
    protocol = "https" if config.get("SSL", True) else "http"
    return f"{protocol}://{host}:{port}/{endpoint}"


async def check_database(config, session):
    """Ask InfluxDB database if it's up and running.
    """
    URL = build_database_url(config, "ping")
    try:
        async with session.get(URL, ssl=False) as response:
            if response.status == 204:
                return True
            else:
                logging.error(f"Uploader: Data. Status code: {response.status}")
                return False
    except aiohttp.client_exceptions.ClientConnectorError:
        logging.error(f"Uploader: could not connect to InfluxDB server.")
        return False




#
# Vaisala message parser
#

async def message_parser(global_config, listener):
    """Get raw Vaisala data, verity and parse it into an InfluxDB message and pass on.

    - Gets raw data from "broadcaster" generator.
    - Checks the format.
    - If the checks pass, parses the raw data into the InfluxDB line format.
    - Then yields the parsed data down the pipeline.
    """
    logging.info("Parser: Starting thread.")
    config =  global_config["parser"]
    while True:
        async for data in broadcaster(global_config, listener):
            match = re.search(r"\(.*\)", data)
            data = match.group(0) if match else ""
            try:
                if verify_data(data):
                    parsed = parse_data(config, data)
                    yield parsed
                else:
                    logging.warning(f"Parser: Format check failed for received data:\n{data}")
            except Exception as E:
                logging.error(f"Parser: Unexpected error:\n{repr(E)}")
        logging.warning("Parser: Data stream ended.")
        await asyncio.sleep(1)
    logging.info("Parser: Shutting down.")

LINE_TEMPLATE = "weather{tags} {fields} {timestamp}"

def parse_data(config, raw_data):
    """Parse raw data broadcast string into dictionary.
    Data is assumed to be in valid form (i.e. `verify_data` returns True on it).
    Returns None if there are no valid values in the raw data message.
    """
    raw_data = raw_data.strip("()")
    data = raw_data.split(";")
    fields = {}
    for pair in data:
        key, value = pair.split(":")
        key = key.strip()
        value = value.strip()
        if key == "D":
            date_str = value
        elif key == "T":
            time_str = value
        elif key in config["include"] and not re.fullmatch(r"\/+", value):
            try:
                fields[key] = float(value)
            except ValueError:
                continue
    if len(fields) == 0:
        return None

    time_ns = get_time_ns(date_str, time_str)
    tags = config.get("tags", {})
    tag_str = "," + str_from_dict(tags) if tags else ""
    field_str = str_from_dict(fields)
    return LINE_TEMPLATE.format(tags=tag_str, fields=field_str, timestamp=time_ns)

def str_from_dict(data):
    """Turn a dict into a string of comma-separated key=value pairs.
    """
    return ",".join(["{}={}".format(k, v) for (k,v) in data.items()])


EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

def datetime_to_ns(dt):
    """Convert datetime to nanoseconds from epoch."""
    time_s = (dt - EPOCH).total_seconds()
    return int(time_s * 1000)


def get_time_ns(date_str, time_str):
    """Convert date and time strings into nanoseconds from epoch."""
    time_utc = datetime.strptime(f"{date_str} {time_str}", "%y%m%d %H%M%S")
    time_utc = time_utc.replace(tzinfo=timezone.utc)
    return datetime_to_ns(time_utc)


def verify_data(data):
    """Does data pass various format checks?"""
    return format_match(data) and has_date(data) and has_time(data)


def has_date(data):
    """Check if data has a date string."""
    m = re.search(r"D:\d{6}", data)
    return not (m is None)


def has_time(data):
    """Check if data has a time string."""
    m = re.search(r"T:\d{6}", data)
    return not (m is None)


def format_match(data):
    """Check if data string matches expected regular expression."""
    m = re.fullmatch(r"\(\s*(?:[\w]+\s*:[^;]*)(?:;\s*[\w]+\s*:[^;]*?)*\)", data)
    return not (m is None)



#
# Split data to broadcast server.
#
async def broadcaster(global_config, listener):
    """Split incoming data to a broadcast server if desired.

    If broadcast is off, this generator just reads the data from the "writer"
    and yields it down the pipeline.

    If broadcast is on, the data is put into a container. The broadcast server
    running simultaneously will read the container whenever it updates and gives
    the data to any connected clients.

    Server settings are given in the "broadcast" subset of the global config.

    """
    config = global_config["broadcast"]
    container = global_config.get("broadcast_container", None)
    if config["active"] and container is None:
        logging.error("Broadcaster: No container given. Unable to push data to server.")
    while True:
        async for data in writer(global_config, listener):
            if config["active"] and not (container is None):
                await container.set(data)
            yield data



#
# Local writer (TODO: implement fully if desired)
#
async def writer(global_config, listener):
    """This would be a module to write incoming data to a local file if deried.
    Currently it does nothing: only reads data from the listener and yields it
    down the pipeline.
    """
    while True:
        try:
            async for item in listener(global_config):
                logging.debug(f"Writer: Got data: {item}")
                # TODO: Write item to local
                yield item
        except asyncio.CancelledError as E:
            raise E
        except Exception as E:
            logging.error(f"Writer: {repr(E)}")
        await asyncio.sleep(1)



#
# Debug outputter. This can be used instead of the InfluxDB uploader for testing
# the previous steps in the pipeline.
#
async def debug_output(config, listener):
    """This is meant to replace the uploader for debugging the upstream pipeline.
    It reads data from the parser and prints it to the debug output.
    """
    while True:
        async for item in message_parser(config, listener):
            logging.debug(f"End of pipe: {item}")


#
# Vaisala Serial listener
#

async def serial_listener(global_config):
    """Listen to data broadcast over serial connection.

    Create a new serial connection to the weather station, then while the
    connection is up, listen to data and send it down the pipeline.

    Most of this function is just error handling.
    """
    config = global_config["listener"]["serial"]

    logging.info("Listener: Starting serial listener thread.")

    reader, writer = await connect_serial(config)

    while True:
        if reader is None:
            logging.error("Listener: Not connected to listener. Trying to connect.")
            reader, writer = await connect_serial(config)
            if reader == None:
                # Still failing, wait for a while before trying again
                timeout = config.get("timeout", 2)
                logging.error(f"Listener: waiting {timeout} seconds before retry.")
                await asyncio.sleep(timeout)
            continue

        try:
            data = await reader.readuntil(b")")
            data = data.decode("utf-8").strip()
        except asyncio.IncompleteReadError as E:
            logging.error("Listener: Connection to Vaisala broadcast interrupted. Trying to reconnect.")
            try:
                writer.close()
            except Exception:
                pass
            reader = None
            continue
        except asyncio.CancelledError:
            break
        except Exception as E:
            logging.error(f"Listener: Unexpected error:\n{repr(E)}")

        if data:
            yield data
        else:
            logging.warning("Listener: No data received.")
    logging.info("Listener: Shutting down.")
    writer.close()
    raise asyncio.CancelledError()



async def connect_serial(config):
    """Create new connection to the weather over serial cable.
    """
    baud = 9600
    device = config.get("device", None)

    try:
        reader, writer = await serial_asyncio.open_serial_connection(url=device, baudrate=baud)
    except Exception as E:
        logging.error(f"Listener: Failed to connect serial: {repr(E)}")
        return None, None
    return reader, writer





#
# Vaisala TCP/IP listener
#

async def network_listener(global_config):
    """Listen to data broadcast over TCP/IP.

    Create a new network connection to the weather station, then while the
    connection is up, listen to data and send it down the pipeline.

    Most of this function is just error handling.
    """
    config = global_config["listener"]["network"]
    logging.info("Listener: Starting network listener thread.")
    source, writer = await connect_network(config)

    while True:
        if source is None:
            logging.error("Listener: Not connected to listener. Trying to connect.")
            source, writer = await connect_network(config)
            if source == None:
                # Still failing, wait for a while before trying again
                timeout = config.get("timeout", 2)
                logging.error(f"Listener: waiting {timeout} seconds before retry.")
                await asyncio.sleep(timeout)
            continue

        try:
            data = await source.readuntil(b')')
            data = data.decode("utf-8")
        except asyncio.CancelledError:
            break
        except asyncio.IncompleteReadError as E:
            logging.error("Listener: Connection to Vaisala broadcast interrupted. Trying to reconnect.")
            try:
                writer.close()
            except Exception:
                pass
            source = None
            continue
        except ConnectionResetError:
            logging.error("Listener: Connection reset by peer. Retrying.")
            try:
                writer.close()
            except:
                pass
            source = None
        except Exception as E:
            logging.error(f"Listener: Unexpected error:\n{repr(E)}")
            try:
                writer.close()
            except:
                pass
            source = None

        if data:
            yield data
        else:
            logging.warning("Listener: No data received.")
    logging.info("Listener: Shutting down.")
    writer.close()
    raise asyncio.CancelledError()


async def connect_network(config):
    """Create a new connection to the weather station over local network.
    """
    try:
        reader, writer = await asyncio.open_connection(config["host"], config["port"])
    except Exception as E:
        logging.error("Listener: Unable to connect to Vaisala computer at {host}:{port}.".format(**config))
        logging.error(f"{repr(E)}")
        return None, None
    else:
        logging.info("Listener: Connected to Vaisala computer at {host}:{port}".format(**config))
        return reader, writer



#
# Main function
#

log_levels = {
    "ALL" : logging.DEBUG,
    "INFO" : logging.INFO,
    "WARNINGS" : logging.WARNING,
    "ERRORS" : logging.ERROR
}


def load_config(filename):
    """Load config from TOML file."""
    with open(filename, "r") as f:
        return toml.load(f)



async def main(config):
    common_config = config["common"]
    broadcast_config = config["broadcast"]

    # Set up debug printing, defaulting to "ALL"
    log_lvl = log_levels[common_config.get("debug_level", "ALL")]
    log_format = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(format=log_format, level=log_lvl, datefmt="%H:%M:%S")

    # Create a broadcast server and a data container if broadcast is active.
    if broadcast_config["active"]:
        container = DataContainer()
        config["broadcast_container"] = container
    else:
        container = None

    # Choose the listener function based on config.
    if common_config["source"] == "network":
        listener = network_listener
    elif common_config["source"] == "serial":
        listener = serial_listener
    else:
        logging.error("Source is neither 'network' nor 'serial'.")
        return

    # Start the necessary tasks. Quit on KeyboardInterrupt.
    try:
        if broadcast_config["active"]:
            await asyncio.gather(
                uploader(config, listener),
                start_server(config, container)
            )
        else:
            await uploader(config, listener)
    except KeyboardInterrupt:
        logging.info("Trying to shut down gracefully.")
        loop = asyncio.get_event_loop()
        loop.stop()


if __name__=="__main__":
    """Entry point of the program. Read config from file and call main function."""
    config = load_config(sys.argv[1])
    try:
        asyncio.run(main(config))
    except asyncio.CancelledError:
        logging.info("Main task cancelled.")
    except KeyboardInterrupt:
        logging.info("User requested shutdown.")



###
### TESTS
###

# Run tests with command line: python -m pytest listener_robust.py

def test_url_builder():
    config = {
    "host" : "localhost",
    "port" : 80,
    "SSL" : False
    }
    url = build_database_url(config, "test")
    assert url == "http://localhost:80/test"



def test_str_from_dict():
    params = {
        "foo" : 80,
        "bar" : "test",
        "baz" : 1.2,
    }
    s = str_from_dict(params)

    # Dict has no guaranteed order (though in practice we get the first one)
    alternatives = [
        "foo=80,bar=test,baz=1.2",
        "foo=80,baz=1.2,bar=test",
        "bar=test,foo=80,baz=1.2",
        "bar=test,baz=1.2,foo=80",
        "baz=1.2,foo=80,bar=test",
        "baz=1.2,bar=test,foo=80"
    ]
    assert s in alternatives


def test_has_date():
    assert has_date("(M:FOO;D:140518;B:1.0)")
    assert has_date("(M:FOO;D:140518;B)")
    assert not has_date("(M:FOO;D:14018;B:1.0)")
    assert not has_date("(M:FOO;T:140518;B:1.0)")

def test_has_time():
    assert has_time("(M:FOO;T:140518;B:1.0)")
    assert has_time("(M:FOO;T:140518;B)")
    assert not has_time("(M:FOO;T:14018;B:1.0)")
    assert not has_time("(M:FOO;D:140518;B:1.0)")

def test_format():
    assert format_match("(S:FOO;PA:1000;FOO:123)")
    assert not format_match("(S:FOO;PA:1000;FOO:123")
    assert not format_match("S:FOO;PA:1000;FOO:123)")
    assert not format_match("(S:FOO,PA:1000;FOO:123)")


def test_data_verify():
    assert verify_data("(S:FOO;T:101018;D:010128;PA:1000)")
    assert not verify_data("(S:FOO;T:101018;D:010128;PA:1000")
    assert not verify_data("S:FOO;T:101018;D:010128;PA:1000)")
    assert not verify_data("(S:FOO,T:101018;D:010128;PA:1000)")
