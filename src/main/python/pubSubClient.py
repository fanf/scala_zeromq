#!/usr/bin/python

import zmq, time, sys

def main(port):

    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://127.0.0.1:" + port)

    socket.setsockopt(zmq.SUBSCRIBE, "0")

    while True:
       print(socket.recv())

if __name__ == "__main__":
    main(sys.argv[1])

