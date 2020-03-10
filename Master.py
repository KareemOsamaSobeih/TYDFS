import zmq
import sys
from configparser import ConfigParser
import json
import random

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
    pass

def makeReplicates(aliveTable, filesTable):
    while True:
        for file in filesTable:
            cnt = 0
            srcNode = -1
            for node in filesTable[file]:
                if aliveTable[node]['alive']:
                    cnt += 1
                    if srcNode == -1:
                        srcNode = node
            if srcNode == -1:
                raise Exception("file:`%s` is lost and can't be replicated" % file)
            while cnt < 3:
                # print(file, cnt)
                dstNode = -1
                for node in range(len(aliveTable)):
                    if aliveTable[node]['alive'] and (node not in filesTable[file]):
                        dstNode = node
                        break
                if dstNode == -1:
                    raise Exception("Can't find a data keeper to replicate file: `%s`" % file)
                sendPort = sendRequest(srcNode, file)
                receiveRequest(dstNode, sendPort)
                cnt += 1
                

def sendRequest(srcNode, file):
    pass

def receiveRequest(dstNode, sendPort):
    pass


if __name__ == '__main__':
    read()
    connect()
    aliveTable = []
    for i in range(10):
        aliveTable.append({'alive': (bool)(random.randint(0, 1))})
    
    filesTable = {'file1': [0, 3, 5], 'file2': [2, 4]}
    # print(aliveTable)
    makeReplicates(aliveTable, filesTable)