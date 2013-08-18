#!/usr/bin/python

import zmq, time, sys

def main(nodeName):

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://127.0.0.1:5000")

    while True:
       socket.send(nodeName)
       time.sleep(1)
       print(socket.recv())

if __name__ == "__main__":
    main(sys.argv[1])

