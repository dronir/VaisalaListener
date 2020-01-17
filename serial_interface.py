#!/usr/bin/env python

import time
import serial
import os
import socket
import time
import logging
import threading
import concurrent.futures
from datetime import datetime

"""
while True:
    message1 = bytes(get_data(serial_port), "utf-8")
    try:
        clientsocket.send(message1)
    except BrokenPipeError:
        logging.error("Lost connection")
        break
    except Exception as E:
        logging.error("Unknown error:\n{}".format(repr(E)))
"""


class DataContainer:
    def __init__(self):
        self._lock = threading.Lock()
        self._data = ""
    def update(self, data):
        with self._lock:
            self._data = data
    def get(self):
        with self._lock:
            return self._data


def get_data(serial_port, latest_data):
    while True:
        str = serial_port.read_until(")")
        # TODO: add exception handling
        latest_data.update(str)

def handle(clientsocket, address, latest_data):
    old_data = "###"
    while True:
        
        try:
            clientsocket.send(message1)
        except BrokenPipeError:
            logging.error("Lost connection")
            break
        except Exception as E:
            logging.error("Unknown error:\n{}".format(repr(E)))



def serve(serial_port):
    log_format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=log_format, level=logging.INFO, datefmt="%H:%M:%S")

    latest_data = DataContainer()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("localhost", 42222))
    sock.listen(5)
    logging.info("Listening...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as thread_pool:

        thread_pool.submit(listen_serial, serial_port, latest_data)

        while True:
            (clientsocket, address) = sock.accept()
            logging.info("Connection from {}".format(address))
                thread_pool.submit(handle, clientsocket, address, latest_data)
            except KeyboardInterrupt:
                sock.close()
            except Exception as E:
                logging.error("Unknown error:\n{}".format(repr(E)))
                sock.close()
            finally:
                sock.close()



if __name__=="__main__":
    ser = serial.Serial(port='/dev/ttyUSB0', baudrate=9600, timeout=60)
    serve(ser)
