import zmq
import random
import sys
import time
from configparser import ConfigParser


config = ConfigParser()
config.read('config.ini')

# hard coded number (we agreed to choose that number)
port = config['DataKeeper']['alivePort']
# data_node id
id = int(sys.argv[1])
ips = json.loads( config['DataKeeper']['ips'] )
# machine ip
ip  = ips[id]

context = zmq.Context()
socket = context.socket(zmq.PUB)
connection_string  = "tcp://" + ip + ":" + port
socket.bind(connection_string)


while True:
# topic of the every data_node
    socket.send_string("%d" % (topic))
    time.sleep(1)
