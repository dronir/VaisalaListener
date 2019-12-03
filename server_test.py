import socket
import time
import logging
from datetime import datetime

# This program starts a server on port 9092, sending the same piece of XML
# flight date every second. This can be used as a simple test server for
# aircraft.py

LINE_TEMPLATE = """(S:MAWS;D:{};T:{};TAAVG1M:{};PA:{:.1d})"""


def create_data():
    now = datetime.utcnow()
    date_str = now.strftime("%y%m%d")
    time_str = now.strftime("%H%M%S")
    temp = 20.0
    pressure = 1013.0
    return LINE_TEMPLATE.format(date_str, time_str, temp, pressure)



def serve():
    log_format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=log_format, level=logging.INFO, datefmt="%H:%M:%S")

    hostName = socket.gethostbyname("0.0.0.0")
    logging.info(hostName)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((hostName, 42222))
    sock.listen(5)
    logging.info("Listening...")
    try:
        while True:
            (clientsocket, address) = sock.accept()
            logging.info("Connection from {}".format(address))
            while True:
                message1 = bytes(create_data(), "utf-8")
                try:
                    clientsocket.send(message1)
                except BrokenPipeError:
                    logging.error("Lost connection")
                    break
                except Exception as E:
                    logging.error("Unknown error:\n{}".format(repr(E)))
                time.sleep(1.0)
    except KeyboardInterrupt:
        sock.close()
    except e:
        logging.error("Unknown error:\n{}".format(repr(E)))
        sock.close()

if __name__=="__main__":
    serve()
