import zmq
import random
import sys
import time
import datetime
import multiprocessing
from configparser import ConfigParser
import json


config = ConfigParser()
config.read('config.ini')


# hard coded number (we agreed to choose that number)
port = config['Master']['alivePort']

#list of ips related to each data_node

ips = json.loads( config['Master']['data_keepers_ips'] )

#number of data_nodes
n = len(ips)


# intialize  look up table 
manager = multiprocessing.Manager()
look_up_table = manager.list()
for i  in range(n):
    entry = manager.dict({"last_time_alive":datetime.datetime.now(),"is_alive": False})
    look_up_table.append(entry)

# connection between  all data_nodes and iam_alive services
context_data_nodes = zmq.Context()
socket_data_nodes = context_data_nodes.socket(zmq.SUB)
socket_data_nodes.setsockopt_string(zmq.SUBSCRIBE, "")


# connect to each data_node
for i in range(n):
    connection_string  = "tcp://" + ips[i] + ":" + port
    socket_data_nodes.connect(connection_string)

poller = zmq.Poller()
poller.register(socket_data_nodes, zmq.POLLIN)


while True:
    socks = dict(poller.poll(1000))
    if socks:
        topic =  int ( socket_data_nodes.recv_string(zmq.NOBLOCK) )
        look_up_table[topic]["last_time_alive"] = datetime.datetime.now()
        look_up_table[topic]["is_alive"] = True

    for i  in range(n):
        # Timeout
        # consider dead
        if( (datetime.datetime.now() - look_up_table[i]["last_time_alive"]).seconds > 1):
            look_up_table[i]["is_alive"] = False
        # print(i,look_up_table[i]["is_alive"])