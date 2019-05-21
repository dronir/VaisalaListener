import telnetlib
import codecs
import influxdb as db
from sys import argv, stderr
from datetime import datetime

from config import DB_CONFIG, NET_CONFIG

DBClient = db.InfluxDBClient(**DB_CONFIG)

class MySeriesHelper(db.SeriesHelper):
    """InfluxDB client object."""
    class Meta:
        client = DBClient
        series_name = "weather"
        fields = ["time"]
        tags = ["station"]
        bulk_size = 10
        autocommit = True


def parse_data(raw_data):
    """Parse raw data broadcast string into dictionary."""
    raw_data = raw_data.strip("()")
    data = raw_data.split(";")
    out = {}
    time = ""
    date = "" 
    for pair in data:
        key,value = pair.split(":")
        if key == "S":
            out["station"] = value
        elif key == "D":
            date = value
        elif key == "T":
            time = value
        else:
            out[key] = value
    if not (time and date):
        raise ValueError("Date or time not present.")
    out["datetime"] = datetime.strptime("{} {}".format(date, time), "%y%m%d %H%M%S")
    return out

def save_data(parsed_data):
    # TODO: send data to InfluxDB instance. For now, just print it to show that the listening works.
    print(parsed_data)



def listen():
    try:
        source = telnetlib.Telnet(NET_CONFIG["host"], NET_CONFIG["port"])
    except:
        print("Unable to connect to source.")
        return
    while True:
        data = source.read_until(b")")
        if not data:
            print("No data received")
            source.close()
            return
        parsed = parse_data(data.decode("utf-8"))
        save_data(parsed)

if __name__=="__main__":
    listen()
