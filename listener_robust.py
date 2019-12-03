# Robust Vaisala listener, with error handling and backup buffer
import requests
import toml
import logging
import sys
import threading
import concurrent.futures
import time
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

def collect_and_upload(db_config, queue, shutdown, logging):
    """Collect data from queue and upload to InfluxDB or backup."""
    batch = []
    logging.info("DB: Starting up uploader thread.")
    # Check if there's backup data
    # Try to upload any backup data

    while True:
        while len(batch) < db_config["batch_size"]:
            new_item = queue.get()
            if new_item is END_QUEUE:
                logging.info("DB: Got END_QUEUE. Starting shutdown.")
                shutdown.set()
                break
            else:
                batch.append(new_item)

        payload = "\n".join(batch)
        success, status = upload_influxdb(config, payload)
        if success:
            batch = []
        else:
            logging.warning("DB: Failed to upload data. Error code: {}".format(status))

        if shutdown.is_set():
            break
    logging.info("DB: Uploader thread shutting down.")


def upload_influxdb(config, payload, shutdown):
    upload_url = build_http_url(config, "write")
    return False, -1


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
        source = telnetlib.Telnet(listener_config["host"], listener_config["port"], 60)
    except:
        logging.error("Listener: Unable to connect to Vaisala computer at {host}:{port}.".format(**listener_config))
        queue.put(END_QUEUE)
        return False
    else:
        logging.info("Listener: Connected to Vaisala computer at {host}:{port}".format(**listener_config))

    while True:
        if shutdown.is_set():
            break
        data = source.read_until(b')', 60)
        if not data:
            logging.error("Listener: Timeout. No data received from Vaisala computer.")
            break
        parsed = parse_data(config["variables"], data.decode("utf-8"))
        logging.info(parsed)
        queue.put(parsed)
    queue.put(END_QUEUE)
    source.close()
    return True


LINE_TEMPLATE = "weather,{tags} {fields} {timestamp}"

def str_from_dict(data):
    """Turn a dict into a string of comma-separated key=value pairs."""
    return ",".join(["{}={}".format(k, v) for (k,v) in data.items()])


def datetime_to_ns(dt):
    """Convert datetime to nanoseconds from epoch."""
    time_s = time.mktime(time_dt.timetuple())
    return int(time_s * 1e9)


def get_time_ns(date_str, time_str):
    """Convert date and time strings into nanoseconds from epoch."""
    time_dt = datetime.strptime("{} {}".format(date_str, time_str), "%y%m%d %H%M%S")
    return datetime_to_ns(time_dt)


def parse_data(config, raw_data):
    """Parse raw data broadcast string into dictionary."""
    raw_data = raw_data.strip("()")
    data = raw_data.split(";")
    point = {}
    fields = {}

    for pair in data:
        key,value = pair.split(":")
        if key == "S":
            tags["station"] = value
        elif key == "D":
            date_str = value
        elif key == "T":
            time_str = value
        elif key in ["TAAVG1M", "RHAVG1M", "DPAVG1M", "QFEAVG1M", "QFFAVG1M", "SRAVG1M", "SNOWDEPTH", "PR", "EXTDC", "STATUS", "PA", "SRRAVG1M", "WD", "WS"]:
            fields[key] = value
        else: pass
    if not (time and date):
        raise ValueError("Date or time not present.")

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
    try:
        db_up, db_status = check_database(config["database"])
    except requests.ConnectionError as E:
        logging.warning("Unable to connect to database at {host}:{port}.".format(**config["database"]))
        db_up = False
        db_status = -1
    except Exception as E:
        logging.error("An unknown error occured when trying to connect to database at {host}:{port}".format(**config["database"]))
        logging.error("Description: " + repr(E))
        db_up = False
        db_status = -2
    else:
        logging.info("Database connection to {host}:{port} ok.".format(**config["database"]))

    shutdown = threading.Event()
    data_queue = SimpleQueue()



    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as thread_pool:
            thread_pool.submit(collect_and_upload, config["database"], data_queue, shutdown, logging)
            thread_pool.submit(listen, config, data_queue, shutdown, logging)
    except KeyboardInterrupt:
        logging.info("Trying to shut down gracefully.")
        shutdown.set()
        logging.info("Shutting down Vaisala listener thread.")
        # join listener thread
        logging.info("Shutting down InfluxDB uploader thread.")
        # join


if __name__=="__main__":
    main(load_config(sys.argv[1]))
