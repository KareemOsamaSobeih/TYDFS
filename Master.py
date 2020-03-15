import zmq
import sys
import json
import multiprocessing
import Conf
import time
import datetime

class Master(multiprocessing.Process):

    def __readConfiguration(self):
        self.__IP = Conf.MASTER_IP
        self.__clientPort = Conf.MASTER_CLIENT_PORTs[self.__PID]

    def __initConnection(self):
        context = zmq.Context()
        self.__successSocket = context.socket(zmq.PULL)
        self.__clientSocket = context.socket(zmq.REP)
        self.__clientSocket.bind("tcp://%s:%s" % (self.__IP, self.__clientPort))
        self.__dataKeeperSocket = context.socket(zmq.PAIR)
        for dKIP in Conf.DATA_KEEPER_IPs:
            for dKPort in Conf.DATA_KEEPER_SUCCESS_PORTs:
                self.__successSocket.connect("tcp://%s:%s" % (dKIP, dKPort))

        self.__poller = zmq.Poller()
        self.__poller.register(self.__successSocket, zmq.POLLIN)
        self.__poller.register(self.__clientSocket, zmq.POLLIN)


    def __init__(self, PID):
        multiprocessing.Process.__init__(self)
        self.__PID = PID
        self.__readConfiguration()
        
    def makeReplicates(self):
        while True:
            for file in filesTable:
                cnt = 0
                srcNode = -1
                for node in filesTable[file]:
                    if aliveTable[node]['isAlive']:
                        cnt += 1
                        if srcNode == -1:
                            srcNode = node
                if srcNode == -1:
                    raise Exception("file:`%s` is lost and can't be replicated" % file)
                while cnt < 3:
                    # print(file, cnt)
                    dstNode = -1
                    for node in range(len(aliveTable)):
                        if aliveTable[node]['isAlive'] and (node not in filesTable[file]):
                            dstNode = node
                            break
                    if dstNode == -1:
                        raise Exception("Can't find a data keeper to replicate file: `%s`" % file)
                    sendPort = self.sendRequest(srcNode, file)
                    self.receiveRequest(dstNode, sendPort)
                    # blocking wait until success message is received 
                    # and file is added in destination node
                    while dstNode not in filesTable[file]:
                        continue
                    cnt += 1
    
    def sendRequest(self, srcNode, file):
        pass

    def receiveRequest(self, dstNode, sendPort):
        pass
                
    # this method should be called in server main process 
    # to update the files table if a data keeper received a file
    def __checkSuccess(self):
        message = self.__successSocket.recv_json()
        if message['clientID'] == -1:
            usedPorts [message['nodeID']][message['processID']] = False
            return
        if message['file_name'] not in self.filesTable:
            self.filesTable[message['file_name']] = {'ClientID': message['clientID'], 'nodes': []}     
        self.filesTable[message['file_name']]['nodes'].append(message['node_ID'])
        usedPorts [message['nodeID']][message['processID']] = False
        

    def __chooseUploadNode (self):
        numNodes = len(Conf.DATA_KEEPER_IPs)
        numProcessesInNodes = len(Conf.DATA_KEEPER_MASTER_PORTs)
        lockUpload.acquire()
        currentNode = RRNodeItr.value
        currentProcess = RRProcessItr.value
        while usedPorts[RRNodeItr.value][RRProcessItr.value] == True or aliveTable[RRNodeItr.value]['isAlive'] == False:
            RRNodeItr.value +=1
            RRNodeItr.value %=numNodes
            if RRNodeItr.value == 0:
                RRProcessItr.value += 1
                RRProcessItr.value %= numProcessesInNodes
            if RRNodeItr.value == currentNode and RRProcessItr == currentProcess:
                break
        while aliveTable[RRNodeItr.value]['isAlive'] == False:
            RRNodeItr.value +=1
            RRNodeItr.value %=numNodes
            
        usedPorts[RRNodeItr.value][RRProcessItr.value] = True
        lockUpload.release()
        freePort = {'IP': Conf.DATA_KEEPER_IPs[RRNodeItr.value] ,
         'PORT': Conf.DATA_KEEPER_UPLOAD_PORTs[RRProcessItr.value] }
        return freePort
            
    def __receiveUploadRequest(self):
        freePort = self.__chooseUploadNode() 
        self.__clientSocket.send_json(freePort)
        print("msg sent to client")

    def __receiveDownloadRequest(self, fileName):
        lockUpload.acquire()
        freePorts = []
        size = 0
        for i in filesTable[fileName]['nodes']:
            if aliveTable[i] == False:
                continue
            Node_IP = Conf.DATA_KEEPER_IPs[i]
            for j in range(len(Conf.DATA_KEEPER_MASTER_PORTs)):
                if(usedPorts[i][j] == False):
                    usedPorts[i][j] = True
                    Node_Port = Conf.DATA_KEEPER_MASTER_PORTs[j]
                    if len(freePorts) == 0:
                        msg = {'requestType': 'download', 'mode' : 1, 'fileName': fileName}
                        self.__dataKeeperSocket.connect("tcp://%s:%s" % (Node_IP, Node_Port))
                        self.__dataKeeperSocket.send_json(msg)
                        msg = self.__dataKeeperSocket.recv_pyobj()
                        size = msg['size']
                    freePorts.append({'Node': Node_IP, 'Port': Node_Port})
        lockUpload.release()
        MOD = len(freePorts)
        j = 0
        downloadPorts = []
        for i in freePorts:
            self.__dataKeeperSocket.connect("tcp://%s:%s" % (i['Node'], i['Port']))
            self.__dataKeeperSocket.send_json({'fileName': fileName, 'mode': 2, 'm': j, 'MOD': MOD})
            j += 1
            downloadPort = self.__dataKeeperSocket.recv_pyobj()
            self.__dataKeeperSocket.disconnect("tcp://%s:%s"% (i['Node'], i['Port']))
            downloadPorts.append(downloadPort)
        self.__clientSocket.send_json({'freeports': downloadPorts, 'numberofchunks': size})

    
    def __receiveRequestFromClient(self):
        message = self.__clientSocket.recv_json()
        print("msg got")
        if message['requestType'] == 'upload':
            self.__receiveUploadRequest()
        if message['requestType'] == 'download':
            self.__receiveDownloadRequest(message['file'])
              

    def run(self):
        self.__initConnection()
        while True:
            # check if any message is received from client or Data keeper
            socks = dict(self.__poller.poll())
            if self.__successSocket in socks and socks[self.__successSocket] == zmq.POLLIN:
                # success message from data keeper
                self.__checkSuccess()
            elif self.__clientSocket in socks and socks[self.__clientSocket] == zmq.POLLIN:
                # request from client
                self.__receiveRequestFromClient()
    
    def haertBeatsConfiguration(self):
        context = zmq.Context()
        self.__aliveSocket = context.socket(zmq.SUB)
        self.__aliveSocket.setsockopt_string(zmq.SUBSCRIBE, "")
        # connect to each data_node
        for dKIP in Conf.DATA_KEEPER_IPs:
            self.__aliveSocket.connect("tcp://%s:%s"%(dKIP, Conf.ALIVE_PORT))

        self.__alivePoller = zmq.Poller()
        self.__alivePoller.register(self.__aliveSocket, zmq.POLLIN)

    def heartBeats(self):
        while True:
            socks = dict(self.__alivePoller.poll(1000))
            if socks:
                topic =  int ( self.__aliveSocket.recv_string(zmq.NOBLOCK) )
                aliveTable[topic]["lastTimeAlive"] = datetime.datetime.now()
                aliveTable[topic]["isAlive"] = True

            for i  in range(len(Conf.DATA_KEEPER_IPs)):
                # Timeout
                # consider dead
                if( (datetime.datetime.now() - aliveTable[i]["lastTimeAlive"]).seconds > 1):
                    aliveTable[i]["isAlive"] = False
                print(i,aliveTable[i]["isAlive"])

if __name__ == '__main__':
    # initialize shared memory
    manager = multiprocessing.Manager()
    lockUpload = multiprocessing.Lock()
    RRNodeItr = manager.Value('i',0) 
    RRProcessItr = manager.Value('i',0)
    usedPorts = manager.dict()
    aliveTable = manager.list()
    numNodes = len(Conf.DATA_KEEPER_IPs)
    numProcessesInNodes = len(Conf.DATA_KEEPER_MASTER_PORTs)
    for i in range(numNodes):
        listOfProcesses = manager.list()
        entry = manager.dict({"lastTimeAlive":datetime.datetime.now(),"isAlive": False})
        aliveTable.append(entry)
        for j in range (numProcessesInNodes):
            listOfProcesses.append(False)
        usedPorts[i] = listOfProcesses    #1 indicates free port & 0 for busy 
    filesTable = manager.dict()

    servers = []
    for i in range(len(Conf.MASTER_CLIENT_PORTs)):
        servers.append(Master(i))
        servers[i].start()
    
    # start one different process to handle alive messages from data keepers
    servers[0].haertBeatsConfiguration()
    aliveProcess = multiprocessing.Process(target=servers[0].heartBeats)
    aliveProcess.start()

    # start one different process to handle generating replicates
    # replicaProcess = multiprocessing.Process(target=servers[0].makeReplicates)
    # replicaProcess.start()

    for i in range(len(Conf.MASTER_CLIENT_PORTs)):
        servers[i].join()
    # replicaProcess.join()
    aliveProcess.join()
