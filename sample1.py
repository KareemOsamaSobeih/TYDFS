import zmq
import random
import sys
import time

port = "5556"

context = zmq.Context()
socket = context.socket(zmq.PUB)
connection_string  = "tcp://127.0.0.1" + ":" + port
socket.bind(connection_string)

while True:
# topic of the every data_node
    topic = 0
    socket.send_string("%d" % (topic))
    time.sleep(1)

