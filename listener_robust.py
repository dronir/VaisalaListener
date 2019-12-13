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

# 1. Check if database is up and available
# 2. Check if there's data in the backup buffer
#   - If there is, add it to the upload batch
# 3. Start Vaisala listening thread
#   - Listen to Vaisala broadcasts
#   - If Vaisala not available, wait a bit and try again (also warn user)
#   - Get value from Vaisala, parse it and push into queue
# 4. Start InfluxDB upload thread
#   - Get data from queue and put it into the upload batch
#   - Once the upload batch is large enough, attempt to upload it
#   - If the upload failed, save it into the backup buffer and set "failed" flag
#   - If the failed flag is set and an upload succeeds, try to empty the backup buffer
# 5. If user exits, empty queue (to db or backup) and shut down things cleanly.


END_QUEUE = object()

#
# InfluxDB uploader
#

def collect_and_upload(config, queue, shutdown, logging):
    """Collect data from queue and upload to InfluxDB or backup."""
    batch = []
    failed = False
    logging.info("Uploader: Starting up uploader thread.")
    # Check if there's backup data
    # Try to upload any backup data

    try:
        ok, status = check_database(config)
    except Exception as E:
        logging.error("Uploader: Error:\n{}".format(repr(E)))
        shutdown.set()

    if ok:
        logging.info("Uploader: Database connection to {host}:{port} ok.".format(**config))
    else:
        logging.error("Uploader: Database connection to {host}:{port} failed.".format(**config))

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
            logging.debug("Uploader: Trying to upload batch.")
            payload = "\n".join(batch)
            logging.debug("Uploader: Payload:\n{}".format(payload))
            success, status = upload_influxdb(config, payload, logging)
            if success:
                if failed:
                    # Try to clear backup buffer
                    failed = False
                logging.debug("Uploaded: Upload succesful.")
                batch = []
            else:
                failed = True
                # Put batch to backup buffer.
                logging.warning("Uploader: Failed to upload data. Error code: {}".format(status))
                batch = []

            if shutdown.is_set():
                logging.info("Uploader: Encountered shutdown request.")
                break
    except Exception as E:
        logging.error("Uploader: Unexpected error:\n{}".format(repr(E)))
        shutdown.set()
    logging.info("Uploader: Shutting down.")


def upload_influxdb(config, payload, logging):
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
# Vaisala listener
#

def listen(config, queue, shutdown, logging):
    listener_config = config["vaisala"]
    logging.info("Listener: Starting up listener thread.")
    try:
        source = telnetlib.Telnet(listener_config["host"], listener_config["port"], listener_config["timeout"])
    except Exception as E:
        logging.error("Listener: Unable to connect to Vaisala computer at {host}:{port}.".format(**listener_config))
        logging.error("{}".format(repr(E)))
        queue.put(END_QUEUE)
        return False
    else:
        logging.info("Listener: Connected to Vaisala computer at {host}:{port}".format(**listener_config))

    try:
        while True:
            if shutdown.is_set():
                logging.info("Listener: Encountered shutdown request.")
                break
            try:
                data = source.read_until(b')').decode("utf-8")
            except EOFError as E:
                logging.error("Listener: Connection to Vaisala broadcast interrupted.")
                source.close()
                break
            if not data:
                logging.error("Listener: Timeout. No data received from Vaisala computer.")
                break
            try:
                parsed = parse_data(config["variables"], data)
            except Exception as E:
                logging.error("Listener: Parse error:\n{}".format(repr(E)))
                break
            queue.put(parsed)
    except Exception as E:
        logging.error("Listener: Error:\n{}".format(repr(E)))
    logging.info("Listener: Shutting down.")
    queue.put(END_QUEUE)
    source.close()
    return True


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


LINE_TEMPLATE = "weather,{tags} {fields} {timestamp}"

def parse_data(config, raw_data):
    """Parse raw data broadcast string into dictionary."""
    # TODO: add check
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
    tags = config["tags"]
    tag_str = str_from_dict(tags)
    field_str = str_from_dict(fields)
    return LINE_TEMPLATE.format(tags=tag_str, fields=field_str, timestamp=time_ns)


#
# Main function
#


def main(config):
    log_format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=log_format, level=logging.INFO, datefmt="%H:%M:%S")

    shutdown = threading.Event()
    data_queue = SimpleQueue()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as thread_pool:
            thread_pool.submit(collect_and_upload, config["database"], data_queue, shutdown, logging)
            thread_pool.submit(listen, config, data_queue, shutdown, logging)
    except KeyboardInterrupt:
        logging.info("Trying to shut down gracefully.")
        shutdown.set()
        #logging.info("Main: Shutting down Vaisala listener thread.")
        # join listener thread
        #logging.info("Shutting down InfluxDB uploader thread.")
        # join


if __name__=="__main__":
    main(load_config(sys.argv[1]))



###
### TESTS
###

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

def test_data_verify():
    assert verify_data("(S:FOO;T:101018;D:010128;PA:1000)")
