# Robust Vaisala listener, with error handling and backup buffer
import requests
import toml
import logging
import sys
import threading
import concurrent.futures
import time
import telnetlib
import re
from datetime import datetime
from queue import SimpleQueue
from os.path import exists

# A special flag object to push into thread-shared queue to indicate soft shutdown request.
END_QUEUE = object()

#
# InfluxDB uploader
#

def collect_and_upload(config, queue, shutdown):
    """Collect data from queue and upload to InfluxDB or backup."""
    batch = []
    has_failed = False
    logging.info("Uploader: Starting up uploader thread.")
    # Check if there's backup data
    # Try to upload any backup data

    try:
        ok, status = check_database(config)
    except Exception as E:
        logging.error("Uploader: Error:\n{}".format(repr(E)))
        ok = False

    if not ok:
        shutdown.set()

    backup_ok, batch, E = load_backup(config)
    if not backup_ok:
        logging.error("Uploader: Tried to load possible backup data but failed:\n{}".format(repr(E)))
    if len(batch) > 0:
        logging.info("Uploader: Loaded {} data points from backup.".format(len(batch)))

    try:
        while True:
            while len(batch) < config["batch_size"]:
                # This will block forever if queue remains empty:
                new_item = queue.get()
                if new_item is END_QUEUE:
                    logging.info("Uploader: Got END_QUEUE.")
                    shutdown.set()
                    break
                else:
                    batch.append(new_item)
            if len(batch) > 0:
                logging.debug("Uploader: Trying to upload batch.")
                payload = "\n".join(batch)
                logging.debug("Uploader: Payload:\n{}".format(payload))
                success, status = upload_influxdb(config, payload)
                if success:
                    logging.debug("Uploader: Upload successful.")
                    batch = []
                    if has_failed:
                        logging.info("Uploader: Trying to read backup buffer.")
                        load_success, batch, E = load_backup(config)
                        if load_success:
                            logging.info("Uploader: Found {} items in backup.".format(len(batch)))
                            has_failed = False
                        else:
                            logging.error("Uploader: Failed to read backup buffer:\n{}".format(repr(E)))

                else:
                    logging.error("Uploader: Failed to upload data. Error code: {}".format(status))
                    has_failed = True
                    if config["backup"]:
                        logging.error("Uploader: Attempting backup...")
                        backup_ok = store_backup(config, payload)
                        if backup_ok:
                            logging.info("Uploader: Backed up data to {}.".format(config["backup_file"]))
                        else:
                            logging.error("Uploader: Failed to back up data to {}".format(config["backup_file"]))
                    batch = []

            if shutdown.is_set():
                logging.info("Uploader: Encountered shutdown request.")
                break
    except Exception as E:
        logging.error("Uploader: Unexpected error:\n{}".format(repr(E)))
        shutdown.set()
    logging.info("Uploader: Shutting down.")



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

    # Clear file
    with open(config["backup_file"], "w") as f:
        pass
    return True, batch, None


def upload_influxdb(config, payload):
    """Upload given payload to InfluxDB instance.

    `Config` is the "uploader" config subset.
    """
    upload_url = build_http_url(config, "write")
    params = {
        "db" : config["database"]
    }
    logging.debug("Uploader: {}".format(upload_url))

    try:
        r = requests.post(upload_url, data=payload, params=params)
    except Exception as E:
        logging.error("Uploader: Error while trying to upload:\n{}".format(repr(E)))
        return False, -1

    if r.status_code == 204:
        return True, 204
    else:
        return False, r.status_code


def build_http_url(config, path):
    return "http://{host}:{port}/{path}".format(host=config["host"], port=config["port"], path=path)


def check_database(config):
    """Ask InfluxDB database if it's up and running."""
    URL = build_http_url(config, "ping")
    r = requests.get(URL)
    if r.status_code == 204:
        return True, r.status_code
    else:
        return False, r.status_code


def load_config(filename):
    """Load config from TOML file."""
    with open(filename, "r") as f:
        return toml.load(f)

#
# Vaisala message parser
#

def message_parser(config, parser_queue, upload_queue, shutdown):
    """Get raw Vaisala data, verity and parse it into an InfluxDB message and pass on.

    Gets raw data from parser_queue.
    Checks the format.
    If the checks pass, parses the raw data into the InfluxDB line format.
    Then puts the resulting string into upload_queue.
    """
    logging.info("Parser: Starting thread.")
    while True:
        if shutdown.is_set():
            logging.info("Parser: Encountered shutdown request.")
            break
        data = parser_queue.get()
        if data == END_QUEUE:
            logging.info("Parser: Encountered END_QUEUE.")
            shutdown.set()
            break
        try:
            if verify_data(data):
                parsed = parse_data(config["variables"], data)
                upload_queue.put(parsed)
            else:
                logging.warning("Parser: Format check failed for received data:\n{}".format(data))
        except Exception as E:
            logging.error("Parser: Unexpected error:\n{}".format(repr(E)))
    logging.info("Parser: Shutting down.")

LINE_TEMPLATE = "weather{tags} {fields} {timestamp}"

def parse_data(config, raw_data):
    """Parse raw data broadcast string into dictionary.
    Data is assumed to be in valid form (i.e. `verify_data` returns True on it).
    """
    raw_data = raw_data.strip("()")
    data = raw_data.split(";")
    point = {}
    fields = {}
    for pair in data:
        key, value = pair.split(":")
        if key == "D":
            date_str = value
        elif key == "T":
            time_str = value
        elif key in ["TAAVG1M", "RHAVG1M", "DPAVG1M", "QFEAVG1M", "QFFAVG1M", "SRAVG1M", "SNOWDEPTH", "PR", "EXTDC", "STATUS", "PA", "SRRAVG1M", "WD", "WS"]:
            fields[key] = float(value)
    time_ns = get_time_ns(date_str, time_str)
    tags = config.get("tags", {})
    if tags:
        tag_str = "," + str_from_dict(tags)
    else:
        tag_str = ""
    field_str = str_from_dict(fields)
    return LINE_TEMPLATE.format(tags=tag_str, fields=field_str, timestamp=time_ns)

def str_from_dict(data):
    """Turn a dict into a string of comma-separated key=value pairs."""
    return ",".join(["{}={}".format(k, v) for (k,v) in data.items()])


def datetime_to_ns(dt):
    """Convert datetime to nanoseconds from epoch."""
    time_s = time.mktime(dt.timetuple())
    return int(time_s * 1000000000)


def get_time_ns(date_str, time_str):
    """Convert date and time strings into nanoseconds from epoch."""
    time_dt = datetime.strptime("{} {}".format(date_str, time_str), "%y%m%d %H%M%S")
    return datetime_to_ns(time_dt)


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
    m = re.fullmatch(r"\(\w+:\w+(;\w+:[\w\.]+)+\)", data)
    return not (m is None)

#
# Broadcast
#
def broadcast(config, shutdown):
    # TODO: Implement
    pass


#
# Vaisala Serial listener
#

def serial_listener(config, queue, shutdown):
    logging.info("Serial: Starting serial listener thread.")
    ser = connect_serial(config)
    if ser is None:
        shutdown.set()
        return False
    while True:
        if ser is None:
            logging.error("Serial: Not connected to serial device. Trying to reconnect.")
            ser = connect_serial(config)
            continue
        try:
            str = serial_port.read_until(")", timeout=config.get("timeout", 60))
        except Exception as E:
            logging.error("Serial: Error when reading serial device:\n{}".format(repr(E)))
            logging.info("Serial: Trying to reconnect.")
            ser.close()
            ser = connect_serial(config)
            continue
        # TODO: get only the part between parenthesis
        queue.put(str)
        if shutdown.is_set():
            logging.info("Serial: Shutdown requested.")
            break
    logging.info("Serial: Shutting down.")
    return True

def connect_serial(config):
    try:
        ser = serial.Serial(port=config["device"], baudrate=9600, timeout=config.get("timeout", 60))
    except Exception as E:
        logging.error("Serial: Could not open serial device {}:\n{}".format(config["device"], repr(E)))
        return None
    else:
        return ser



#
# Vaisala TCP/IP listener
#

def network_listener(config, parser_queue, shutdown):
    logging.info("Listener: Starting network listener thread.")
    source = connect_source(config)
    if source is None:
        logging.info("Listener: We can't even start. Shutting down.")
        shutdown.set()
        return False


    while True:
        if shutdown.is_set():
            logging.info("Listener: Encountered shutdown request.")
            break
        if source is None:
            logging.error("Listener: Not connected to listener. Trying to connect.")
            source = connect_source(config)
            continue
        try:
            data = source.read_until(b')', timeout=config.get("timeout", 10)).decode("utf-8")
        except EOFError as E:
            logging.error("Listener: Connection to Vaisala broadcast interrupted. Trying to reconnect.")
            source.close()
            source = connect_source(config)
        except Exception as E:
            logging.error("Listener: Unexpected error:\n{}".format(repr(E)))
        if data:
            parser_queue.put(data)
        else:
            logging.warning("Listener: No data received.")

    logging.info("Listener: Shutting down.")
    source.close()
    return True

def connect_source(config):
    try:
        source = telnetlib.Telnet(config["host"], config["port"], config.get("timeout", 10))
    except Exception as E:
        logging.error("Listener: Unable to connect to Vaisala computer at {host}:{port}.".format(**config))
        logging.error("{}".format(repr(E)))
        return None
    else:
        logging.info("Listener: Connected to Vaisala computer at {host}:{port}".format(**config))
        return source



#
# Main function
#

log_levels = {
    "ALL" : logging.DEBUG,
    "INFO" : logging.INFO,
    "WARNINGS" : logging.WARNING,
    "ERRORS" : logging.ERROR
}

def watchdog(shutdown, parser_queue, upload_queue):
    logging.info("Watchdog: Starting.")
    while True:
        if shutdown.is_set():
            logging.info("Watchdog: Shutdown is set. Putting end commands into queues.")
            parser_queue.put(END_QUEUE)
            upload_queue.put(END_QUEUE)
            return


def main(config):
    log_format = "%(asctime)s %(levelname)s %(message)s"
    log_lvl = log_levels[config["common"].get("debug_level", "ALL")]
    logging.basicConfig(format=log_format, level=log_lvl, datefmt="%H:%M:%S")

    shutdown = threading.Event()
    upload_queue = SimpleQueue()
    parser_queue = SimpleQueue()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as thread_pool:
            thread_pool.submit(watchdog, shutdown, parser_queue, upload_queue)
            thread_pool.submit(collect_and_upload, config["uploader"], upload_queue, shutdown)
            thread_pool.submit(message_parser, config["parser"], parser_queue, upload_queue, shutdown)

            source = config["common"].get("source", "")
            if source == "network":
                thread_pool.submit(network_listener, config["listener"]["network"], parser_queue, shutdown)
            elif source == "serial":
                thread_pool.submit(serial_listener, config["listener"]["serial"], parser_queue, shutdown)
            else:
                logging.error("Could not determine listener method (serial or network).")
                shutdown.set()
    except KeyboardInterrupt:
        logging.info("Trying to shut down gracefully.")
        shutdown.set()


if __name__=="__main__":
    main(load_config(sys.argv[1]))



###
### TESTS
###

# Run tests with command line: python -m pytest listener_robust.py

def test_url_builder():
    config = {
    "host" : "localhost",
    "port" : 80
    }
    url = build_http_url(config, "test")
    assert url == "http://localhost:80/test"



def test_str_from_dict():
    params = {
        "foo" : 80,
        "bar" : "test",
        "baz" : 1.2
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
