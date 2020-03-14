import zmq
import sys
import json
import multiprocessing
import Conf
import time
import datetime

class Master:

    def __readConfiguration(self):
        self.__IP = Conf.MASTER_IP
        self.__clientPort = Conf.MASTER_CLIENT_PORTs[self.__PID]

    def __initConnection(self):
        self.__dataKeepersSocket = {}
        self.__context = zmq.Context()
        self.__successSocket = self.__context.socket(zmq.PULL)
        self.__clientSocket = self.__context.socket(zmq.REP)
        self.__clientSocket.bind("tcp://%s:%s" % (self.__IP, self.__clientPort))
        for dKIP in Conf.DATA_KEEPER_IPs:
            self.__dataKeepersSocket[dKIP] = self.__context.socket(zmq.REQ)
            for dKPort in Conf.DATA_KEEPER_MASTER_PORTs:
                self.__dataKeepersSocket[dKIP].connect("tcp://%s:%s" % (dKIP, dKPort))
        for dKIP in Conf.DATA_KEEPER_IPs:
            for dKPort in Conf.DATA_KEEPER_SUCCESS_PORTs:
                self.__successSocket.connect("tcp://%s:%s" % (dKIP, dKPort))

        self.__poller = zmq.Poller()
        self.__poller.register(self.__successSocket, zmq.POLLIN)
        self.__poller.register(self.__clientSocket, zmq.POLLIN)


    def __initResources(self, filesTable, aliveTable, lockUpload, usedPorts, RRNodeItr, RRProcessItr):
        self.__filesTable = filesTable
        self.__aliveTable = aliveTable
        self.__lockUpload = lockUpload
        self.__usedPorts = usedPorts
        self.__RRNodeItr = RRNodeItr
        self.__RRProcessItr = RRProcessItr

    def __init__(self, PID, filesTable, aliveTable, lockUpload, usedPorts, RRNodeItr, RRProcessItr):
        self.__PID = PID
        self.__readConfiguration()
        self.__initConnection()
        self.__initResources(filesTable, aliveTable, lockUpload, usedPorts, RRNodeItr, RRProcessItr)
        serverProcess = multiprocessing.Process(target=self.serverProcess)
        serverProcess.start()
        serverProcess.join()
        
    def makeReplicates(self):
        while True:
            for file in self.__filesTable:
                cnt = 0
                srcNode = -1
                for node in self.__filesTable[file]:
                    if self.__aliveTable[node]['is_alive']:
                        cnt += 1
                        if srcNode == -1:
                            srcNode = node
                if srcNode == -1:
                    raise Exception("file:`%s` is lost and can't be replicated" % file)
                while cnt < 3:
                    # print(file, cnt)
                    dstNode = -1
                    for node in range(len(self.__aliveTable)):
                        if self.__aliveTable[node]['is_alive'] and (node not in self.__filesTable[file]):
                            dstNode = node
                            break
                    if dstNode == -1:
                        raise Exception("Can't find a data keeper to replicate file: `%s`" % file)
                    sendPort = self.sendRequest(srcNode, file)
                    self.receiveRequest(dstNode, sendPort)
                    # blocking wait until success message is received 
                    # and file is added in destination node
                    while dstNode not in self.__filesTable[file]:
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
        if message['file_name'] not in self.__filesTable:
            self.__filesTable[message['file_name']] = [message['clientID']]     
        self.__filesTable[message['file_name']].append(message['node_ID'])
        self.__usedPorts [message['nodeID']][message['processID']] = False
        

    def __chooseUploadNode (self):
        numNodes = len(Conf.DATA_KEEPER_IPs)
        numProcessesInNodes = len(Conf.DATA_KEEPER_MASTER_PORTs)
        self.__lockUpload.acquire()
        currentNode = self.__RRNodeItr.value
        currentProcess = self.__RRProcessItr.value
        while self.__usedPorts[self.__RRNodeItr.value][self.__RRProcessItr.value] == True :
            self.__RRNodeItr.value +=1
            self.__RRNodeItr.value %=numNodes
            if self.__RRNodeItr.value == 0:
                self.__RRProcessItr.value += 1
                self.__RRProcessItr.value %= numProcessesInNodes
            if self.__RRNodeItr.value == currentNode and self.__RRProcessItr == currentProcess:
                break
        self.__usedPorts[self.__RRNodeItr.value][self.__RRProcessItr.value] = True
        self.__lockUpload.release()
        freePort = {'IP': Conf.DATA_KEEPER_IPs[self.__RRNodeItr.value] ,
         'PORT': Conf.DATA_KEEPER_UPLOAD_PORTs[self.__RRProcessItr.value] }
        return freePort
            
    def __receiveUploadRequest(self):
        freePort = self.__chooseUploadNode() 
        self.__clientSocket.send_json(freePort)
        # print("msg sent to client")

    def __receiveRequestFromClient(self):
        message = self.__clientSocket.recv_json()
        # print("msg got")
        if message['requestType'] == 'upload':
            self.__receiveUploadRequest()
              
        # else:
        #     DOWNLOAD()
        # print("DK prompted")

    def serverProcess(self):
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
        self.__aliveSocket = self.__context.socket(zmq.SUB)
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
                self.__aliveTable[topic]["lastTimeAlive"] = datetime.datetime.now()
                self.__aliveTable[topic]["isAlive"] = True

            for i  in range(len(Conf.DATA_KEEPER_IPs)):
                # Timeout
                # consider dead
                if( (datetime.datetime.now() - self.__aliveTable[i]["lastTimeAlive"]).seconds > 1):
                    self.__aliveTable[i]["isAlive"] = False
                # print(i,self.__aliveTable[i]["isAlive"])

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
        servers.append(Master(i, filesTable, aliveTable, lockUpload, usedPorts, RRNodeItr, RRProcessItr))
    
    # start one different process to handle alive messages from data keepers
    servers[0].haertBeatsConfiguration()
    aliveProcess = multiprocessing.Process(target=servers[0].heartBeats)
    aliveProcess.start()
    aliveProcess.join()

    # start one different process to handle generating replicates
    replicaProcess = multiprocessing.Process(target=servers[0].makeReplicates)
    replicaProcess.start()
    replicaProcess.join()
    # filesTable = {'file1': [0, 3, 5], 'file2': [2, 4]}
