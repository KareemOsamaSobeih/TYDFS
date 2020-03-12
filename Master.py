import zmq
import sys
from configparser import ConfigParser
import json
import random
import collections
import multiprocessing

def read():
    config = ConfigParser()
    config.read('config.ini')
    global IP, clientPort, dataKeepersIps, dataKeepersPorts, successPorts, id
    id = int(sys.argv[1])
    IP = config["Master"]["IP"]
    clientPort = json.loads(config["Master"]["client_ports"])[id]
    dataKeepersIps = json.loads(config["Master"]["data_keepers_ips"])
    dataKeepersPorts = json.loads(config["Master"]["Ports"])
    UploadPorts = json.loads(config["DataKeeper"]["uploadPorts"])
    successPorts = json.loads(config["Master"]["successPorts"])


def init_connection():
    global context, client_socket, success_socket, dataKeepers_socket
    dataKeepers_socket = {}
    success_socket = collections.defaultdict(dict)
    context = zmq.Context()
    client_socket = context.socket(zmq.REP)
    client_socket.bind("tcp://%s:%s" % (IP, clientPort))
    for i in dataKeepersIps:
        dataKeepers_socket[i] = context.socket(zmq.REQ)
        for j in dataKeepersPorts:
            dataKeepers_socket[i].connect("tcp://%s:%s" % (i, j))
    for i in dataKeepersIps:
        for j in successPorts:
            success_socket[i][j] = context.socket(zmq.PULL)
            success_socket[i][j].connect("tcp://%s:%s" % (i, j))


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
                
def Init_SharedMemory ():
    sharedMem = multiprocessing.Manager()
    lock_upload = multiprocessing.Lock()
    RRNodeItr= sharedMem.value() = 0
    RRProcessItr = sharedMem.value() = 0   
    UsedPorts = sharedMem.dict()
    numNodes = len(dataKeepersIps)
    numProcesses_in_nodes = len(dataKeepersPorts)
    for i in range(numNodes):
        listOfProcesses = sharedMem.list()
        for j in range (numProcesses_in_nodes):
            listOfProcesses.append(1)
        UsedPorts[i] = listOfProcesses    #1 indicates free port & 0 for busy 
    

def UPload ():
    lock_upload.acquire()
    while UsedPorts[RRNodeItr][RRProcessItr] == 0 :
        if RRNodeItr < numNodes-1:
            RRNodeItr +=1
        else:
            RRNodeItr = 0
            RRProcessItr = RRProcessItr +1 if RRProcessItr < numProcesses_in_nodes-1 else 0
    UsedPorts[RRNodeItr][RRProcessItr] = 0
    lock_upload.release()
    message = {'IP': dataKeepersIps[RRNodeItr] , 'PORT': UploadPorts[RRProcessItr] }
    return (json.dumps (message), RRNodeItr)        
            
    
    
                

def sendRequest(srcNode, file):
    pass

def receiveRequest(dstNode, sendPort):
    pass


if __name__ == '__main__':
    read()
    init_connection()
    Init_SharedMemory ()
    
    while True:
    #  Wait for next request from client
        message = client_socket.recv_json()
        details = json.loads(message)
        if details['req']  == 0:
            freePort, node = UPload ()
            details ['type'] = 0   #0 for client stuff & 1 for replica, if replica add another field for the IP and port of destination machine
            client_socket.send_json(freePort)
        else:
            DOWNLOAD()
        dataKeepers_socket[node].send_json(json.dumps(details))
        dataKeepers_socket[node].recv_string()
    aliveTable = []
    
    """ for i in range(10):
        aliveTable.append({'alive': (bool)(random.randint(0, 1))})
    
    filesTable = {'file1': [0, 3, 5], 'file2': [2, 4]}
    # print(aliveTable)
    makeReplicates(aliveTable, filesTable) """