import socket
import time
from sys import exit


# This starts a server on the given port and sends out the data string
# approximately once per second.

DATA = """(S:MAWS;D:170303;T:121500;PA:1013.0)"""
PORT = 42222

def serve():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("localhost", PORT))
    sock.listen(5)
    print("Listening")
    try:
        while True:
            (clientsocket, address) = sock.accept()
            print("Connection from {}".format(address))
            while True:
                message1 = bytes(DATA, "utf-8")
                try:
                    clientsocket.send(message1)
                except BrokenPipeError:
                    print("Lost connection")
                    break
                time.sleep(1.0)
    except KeyboardInterrupt:
        sock.close()
        exit()
    except e:
        print(e)
        sock.close()

if __name__=="__main__":
    serve()
