import telnetlib
import codecs
import influxdb as db
from sys import argv, stderr
from datetime import datetime
import numpy as np


from config import DB_CONFIG, NET_CONFIG, DB_POUTA_CONFIG

DBClient = db.InfluxDBClient(**DB_CONFIG)
dbname=DB_CONFIG["database"]

#class MySeriesHelper(db.SeriesHelper):
#    """InfluxDB client object."""
#    class Meta:
#        client = DBClient
#        series_name = "weather"
#        fields = ["time"]
#        tags = ["station"]
#        bulk_size = 10
#        autocommit = True


def parse_data(raw_data):
    """Parse raw data broadcast string into dictionary."""
    raw_data = raw_data.strip("()")
    data = raw_data.split(";")
    point = {}; fields = {}; tags = {}; out=[]
    time = ""
    date = ""
    point["measurement"] = "weather"
    for pair in data:
        key,value = pair.split(":")
        if key == "S":
            tags["station"] = value
        elif key == "D":
            date = value
        elif key == "T":
            time = value
        elif key in ["TAAVG1M", "RHAVG1M", "DPAVG1M", "QFEAVG1M", "QFFAVG1M", "SRAVG1M", "SNOWDEPTH", "PR", "EXTDC", "STATUS", "PA", "SRRAVG1M", "WD", "WS"]:
            #print(type(value))
            fields[key] = float(value)
        else: pass
    if not (time and date):
        raise ValueError("Date or time not present.")
    point["time"] = datetime.strptime("{} {}".format(date, time), "%y%m%d %H%M%S")
    point["tags"]=tags; point["fields"]=fields; out.append(point)
    return out

def save_data(parsed_data):
<<<<<<< HEAD
    # TODO: send data to InfluxDB instance. For now, just print it to show that the listening works.
=======
    # TODO
    DBClient.write_points(parsed_data)
>>>>>>> Commit to reset
    print(parsed_data)



def listen():
    #from pudb import set_trace; set_trace()
    '''List of things to add:
       -!!!Decide the structure of the db tables before dumpimg stuff in the db!!!! Right now it submits every single key&value to the dictionary. Stuff like min, max and avg aren't needed, they can be calculated from the actual values.
       -probably other secure connection additions like users and pws and keys and such
       -'''
    try:
        source = telnetlib.Telnet(NET_CONFIG["host"], NET_CONFIG["port"], 60)
        #source.set_debuglevel(100)
        print("Connected to source!")
    except:
        print("Unable to connect to source.")
        return
    #source.open(NET_CONFIG["host"], NET_CONFIG["port"], 60)
    DBClient.switch_database(dbname);
    print("Waiting for data...")
    while True:
        #print("Reading Data...")
        #data = source.read_until(b')')
        data = source.read_until(b')', 60)
        if not data:
            print("No data received")
            source.close()
            return
        #print("Data found!")
        parsed = parse_data(data.decode("utf-8"))
        save_data(parsed)
    source.close()
    return


if __name__=="__main__":
    listen()
