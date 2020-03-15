import zmq
import sys
import multiprocessing
import Conf
import time
import os
    
class DataKeeper(multiprocessing.Process):

    def __readConfiguration(self):
        self.__IP = Conf.DATA_KEEPER_IPs[self.__ID]
        self.__masterPort = Conf.DATA_KEEPER_MASTER_PORTs[self.__PID]
        self.__successPort = Conf.DATA_KEEPER_SUCCESS_PORTs[self.__PID]
        self.__downloadPort = Conf.DATA_KEEPER_DOWNLOAD_PORTs[self.__PID]
        self.__uploadPort = Conf.DATA_KEEPER_UPLOAD_PORTs[self.__PID]

    def __initConnection(self):
        context = zmq.Context()
        self.__masterSocket = context.socket(zmq.REP)
        self.__masterSocket.bind("tcp://%s:%s" % (self.__IP, self.__masterPort))
        self.__successSocket = context.socket(zmq.PUSH)
        self.__successSocket.bind("tcp://%s:%s" % (self.__IP, self.__successPort))
        self.__downloadSocket = context.socket(zmq.PUSH)
        self.__downloadSocket.bind("tcp://%s:%s" % (self.__IP, self.__downloadPort))
        self.__uploadSocket = context.socket(zmq.PULL)
        self.__uploadSocket.bind("tcp://%s:%s" % (self.__IP, self.__uploadPort))
        self.__poller = zmq.Poller()
        self.__poller.register(self.__masterSocket, zmq.POLLIN)
        self.__poller.register(self.__uploadSocket, zmq.POLLIN)

    def __init__(self, ID, PID):
        multiprocessing.Process.__init__(self)
        self.__ID = ID
        self.__PID = PID
        self.__readConfiguration()
    
    def run (self):
        self.__initConnection()
        while True:
            # check if any message is received from client or Data keeper
            socks = dict(self.__poller.poll())
            if self.__masterSocket in socks and socks[self.__masterSocket] == zmq.POLLIN:
                # Request message from master
                self.__receiveRequestsFromMaster()
            elif self.__uploadSocket in socks and socks[self.__uploadSocket] == zmq.POLLIN:
                # Upload request, whether from client or a node
                self.__receiveUploadRequest()
                
    def __receiveRequestsFromMaster(self):
        # Need to be modified to receive message prompting it to send to client or to a given-address node
        msg = self.__masterSocket.recv_json()
        print("msg received in DK")
        
        if msg['requestType'] == 'download':
            self.__receiveDownloadRequest(msg)
        elif msg['requestType'] == 'replica':
            pass
    
    def __receiveUploadRequest(self):
        rec = self.__uploadSocket.recv_pyobj()
        print("video rec")
        with open(rec['fileName'], "wb") as out_file:  # open for [w]riting as [b]inary
                out_file.write(rec['file'].data)
        # lockSave.acquire()
        # self.__storage[rec['fileName']] = {'file': rec['file'], 'clientID': rec['clientID']}
        # lockSave.release()
        successMessage = {'fileName': rec['fileName'], 'clientID': rec['clientID'], 'nodeID': self.__ID, 'processID': self.__PID }
        self.__successSocket.send_json(successMessage)
        print("done")

    def __receiveDownloadRequest(self, msg):
        chunksize = Conf.CHUNK_SIZE
        if msg['mode'] == 1:
            fileName = msg['fileName']
            size = os.stat(fileName).st_size
            size = (size+chunksize-1)//chunksize
            self.__masterSocket.send_pyobj({'size': size})
            msg = self.__masterSocket.recv_json()
        MOD = msg['MOD']
        m = msg['m']  # data keeper downloads all chuncks where chunk number % MOD = m
        fileName = msg['fileName']
        self.__masterSocket.send_pyobj({'IP': self.__IP, 'PORT' : self.__downloadPort})
        self.__downloadToClient(MOD, m, fileName)

    def __downloadToClient(self, MOD, m, fileName):
        chunksize = Conf.CHUNK_SIZE
        size = os.stat(fileName).st_size
        size = (size+chunksize-1)//chunksize
        NumberofChuncks = size//MOD + (size%MOD >= m)
        with open(fileName, "rb") as file:
            for i in range(NumberofChuncks):
                step = chunksize*i
                file.seek(step*MOD + chunksize*m)
                chunk = file.read(chunksize)
                self.__downloadSocket.send_pyobj({'chunckNumber': i*MOD+m, 'data': chunk})
        successMessage = {'fileName': fileName, 'clientID': -1, 'nodeID': self.__ID, 'processID': self.__PID }
        self.__successSocket.send_json(successMessage)


    def __sendToClient(self, filePath):
        with open(filePath, "rb") as file:
            self.__downloadSocket.send(file.read())
    
    def __heartBeatsConfiguration(self):
        context = zmq.Context()
        self.__aliveSocket = context.socket(zmq.PUB)
        self.__aliveSocket.bind("tcp://%s:%s"%(self.__IP, Conf.ALIVE_PORT))

    def heartBeats(self):
        self.__heartBeatsConfiguration()
        while True:
            # print("alive")
            self.__aliveSocket.send_string("%d" % (self.__ID))
            time.sleep(1)
    

if __name__ == '__main__':
    
    manager = multiprocessing.Manager()
    # lockSave = multiprocessing.Lock()
    # storage = manager.dict()
    
    ID = int(sys.argv[1])
    numOfProcesses = len(Conf.DATA_KEEPER_MASTER_PORTs)
    processes = []
    for i in range(numOfProcesses):
        processes.append(DataKeeper(ID, i))
        processes[i].start()

    # start one differnt process to send heartbeats to the Master
    heartBeatsProcess = multiprocessing.Process(target=processes[0].heartBeats)
    heartBeatsProcess.start()

    for i in range(numOfProcesses):
        processes[i].join()
    heartBeatsProcess.join()

