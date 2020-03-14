import zmq
import sys
import multiprocessing
import Conf
import time
    
class DataKeeper:

    def __readConfiguration(self):
        self.__IP = Conf.DATA_KEEPER_IPs[self.__ID]
        self.__masterPort = Conf.DATA_KEEPER_MASTER_PORTs[self.__PID]
        self.__successPort = Conf.DATA_KEEPER_SUCCESS_PORTs[self.__PID]
        self.__downloadPort = Conf.DATA_KEEPER_DOWNLOAD_PORTs[self.__PID]
        self.__uploadPort = Conf.DATA_KEEPER_UPLOAD_PORTs[self.__PID]

    def __initConnection(self):
        self.__context = zmq.Context()
        self.__masterSocket = self.__context.socket(zmq.REP)
        self.__masterSocket.bind("tcp://%s:%s" % (self.__IP, self.__masterPort))
        self.__successSocket = self.__context.socket(zmq.PUSH)
        self.__successSocket.bind("tcp://%s:%s" % (self.__IP, self.__successPort))
        self.__downloadSocket = self.__context.socket(zmq.PUSH)
        self.__downloadSocket.bind("tcp://%s:%s" % (self.__IP, self.__downloadPort))
        self.__uploadSocket = self.__context.socket(zmq.PULL)
        self.__uploadSocket.bind("tcp://%s:%s" % (self.__IP, self.__uploadPort))
        self.__poller = zmq.Poller()
        self.__poller.register(self.__masterSocket, zmq.POLLIN)
        self.__poller.register(self.__clientSocket, zmq.POLLIN)

    def __init__(self, ID, PID, lockSave, storage):
        self.__ID = ID
        self.__PID = PID
        self.__lockSave = lockSave
        self.__storage = storage
        self.__readConfiguration()
        self.__initConnection()
        serverProcess = multiprocessing.Process(target=self.dataKeeperProcess)
        serverProcess.start()
        serverProcess.join()
    
    def dataKeeperProcess (self):
        while True:
            # check if any message is received from client or Data keeper
            socks = dict(self.__poller.poll())
            if self.__masterSocket in socks and socks[self.__masterSocket] == zmq.POLLIN:
                # Request message from master
                self.receiveRequestsFromMaster()
            elif self.__clientSocket in socks and socks[self.__clientSocket] == zmq.POLLIN:
                # Upload request, whether from client or a node
                self.__receiveUploadRequest()
                
    def __receiveRequestsFromMaster(self):
        #Need to be modified to receive message prompting it to send to client or to a given-address node
        while True:
            msg = self.__masterSocket.recv_json()
            # print("msg received in DK")
            self.__masterSocket.send_string("")
            
            if msg['requestType'] == 'download':
                self.__receiveDownloadRequest()
            elif msg['requestType'] == 'replica':
                pass
    
    def __receiveUploadRequest(self):
        rec = self.__uploadSocket.recv_pyobj()
        #print("video rec")
        with open(rec['fileName'], "wb") as out_file:  # open for [w]riting as [b]inary
                out_file.write(rec['file'].data)
        self.__lockSave.acquire()
        #self.__storage[rec['fileName']] = {'file': rec['file'], 'clientID': rec['clientID']}
        self.__lockSave.release()
        successMessage = {'fileName': rec['fileName'], 'clientID': rec['clientID'], 'nodeID': self.__ID, 'processID': self.__PID }
        self.__successSocket.send_json(successMessage)
        #print("done")

    def __receiveDownloadRequest(self):
        pass

    def __sendToClient(self, filePath):
        with open(filePath, "rb") as file:
            self.__downloadSocket.send(file.read())
    
    def heartBeatsConfiguration(self):
        self.__aliveSocket = self.__context.socket(zmq.PUB)
        self.__aliveSocket.bind("tcp://%s:%s"%(self.__IP, Conf.ALIVE_PORT))

    def heartBeats(self):
        while True:
            self.__aliveSocket.send_string("%d" % (self.__ID))
            time.sleep(1)
    

if __name__ == '__main__':
    
    manager = multiprocessing.Manager()
    lockSave = multiprocessing.Lock()
    storage = manager.dict()
    
    ID = int(sys.argv[1])
    numOfProcesses = len(Conf.DATA_KEEPER_MASTER_PORTs)
    processes = []
    for i in range(numOfProcesses):
        processes.append(DataKeeper(ID, i, lockSave, storage))

    # start one differnt process to send heartbeats to the Master
    processes[0].heartBeatsConfiguration()
    heartBeatsProcess = multiprocessing.Process(target=processes[0].heartBeats)
    heartBeatsProcess.start()
    heartBeatsProcess.join()

    #send("data/DataKeeper/SampleVideo.mp4")
