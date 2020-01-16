#!/usr/bin/env python

import time
import serial
import os
import socket
import time
import logging
from datetime import datetime



def get_data(serial_port):
    while True:
        str = serial_port.read_until(")")
        if str:
            return str


def serve(serial_port):
    log_format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=log_format, level=logging.INFO, datefmt="%H:%M:%S")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("localhost", 42222))
    sock.listen(5)
    logging.info("Listening...")
    try:
        while True:
            (clientsocket, address) = sock.accept()
            logging.info("Connection from {}".format(address))
            while True:
                message1 = bytes(get_data(serial_port), "utf-8")
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
    except Exception as E:
        logging.error("Unknown error:\n{}".format(repr(E)))
        sock.close()
    finally:
        sock.close()

if __name__=="__main__":
    ser = serial.Serial(port='/dev/ttyUSB0', baudrate=9600, timeout=60)
    serve(ser)
