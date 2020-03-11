import zmq
import sys
from configparser import ConfigParser
import json

def read():
    config = ConfigParser()
    config.read('config.ini')
    global MasterIP, NoofPorts, MasterPorts
    MasterIP = config['Client']['MasterMachineIP']
    NoofPorts = config['Client']['NoofPorts']
    MasterPorts = json.loads(config['Client']['Ports'])


def init_connection():
    global context, master_socket
    context = zmq.Context()
    master_socket = context.socket(zmq.REQ)
    for i in MasterPorts:
        master_socket.connect("tcp://%s:%s" % (MasterIP, i))

def download(filePath, addresses):
    context = zmq.Context()
    downSocket = context.socket(zmq.PULL)
    for address in addresses:
        downSocket.connect("tcp://%s:%s"%(address[0], address[1]))
    data = downSocket.recv()
    with open(filePath, "wb") as file:
        file.write(data)

    for address in addresses:
        downSocket.disconnect("tcp://%s:%s"%(address[0], address[1]))
    downSocket.close()
        
""" if __name__ == '__main__':
    download('data/Client/test1.mp4', [('127.0.0.1', '5000')]) """

if __name__ == '__main__':
    read()
    init_connection()