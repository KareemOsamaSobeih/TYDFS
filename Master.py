import zmq
import sys
from configparser import ConfigParser
import json


def read():
    config = ConfigParser()
    config.read('config.ini')
    global IP, clientPort, dataKeepersIps, dataKeepersPorts, successPort, id
    id = int(sys.argv[1])
    IP = config["Master"]["IP"]
    clientPort = json.loads(config["Master"]["client_ports"])[id]
    dataKeepersIps = json.loads(config["Master"]["data_keepers_ips"])
    dataKeepersPorts = json.loads(config["Master"]["Ports"])
    successPort = config["Master"]["successPort"]


def connect():



read()
connect()