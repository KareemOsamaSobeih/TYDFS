import zmq
import sys
from configparser import ConfigParser
import json
import multiprocessing
def read():
    config = ConfigParser()
    config.read('config.ini')
    global IP, masterPort, successPort, downloadPort, uploadPort, pid
    pid = int(sys.argv[1])
    IP = config["DataKeeper"]["IP"]
    masterPort = json.loads(config["DataKeeper"]["Ports"])[pid]
    successPort = json.loads(config['DataKeeper']['successPorts'])[pid]
    downloadPort = json.loads(config['DataKeeper']['downloadPorts'])[pid]
    uploadPort = json.loads(config['DataKeeper']['uploadPorts'])[pid]
    
def init_connection():
    global context, master_socket, success_socket, download_socket, upload_socket
    context = zmq.Context()
    master_socket = context.socket(zmq.REP)
    master_socket.bind("tcp://%s:%s" % (IP, masterPort))
    success_socket = context.socket(zmq.PUSH)
    success_socket.bind("tcp://%s:%s" % (IP, successPort))
    download_socket = context.socket(zmq.PUSH)
    download_socket.bind("tcp://%s:%s" % (IP, downloadPort))
    upload_socket = context.socket(zmq.PULL)
    upload_socket.bind("tcp://%s:%s" % (IP, uploadPort))
    
def Init_sharedMemory():
    sharedMem = multiprocessing.Manager()
    global lock_save, storage
    lock_save = multiprocessing.Lock()
    storage = sharedMem.dict()
def send(filePath):
    with open(filePath, "rb") as file:
        download_socket.send(file.read())
    

if __name__ == '__main__':
    read()
    init_connection()
    Init_sharedMemory()
    while True:
        msg = master_socket.recv_json()
        print("msg received in DK")
        master_socket.send_string("")
        msg = json.loads(msg)
        if msg['req'] == 0:    #Normal upload
            rec = upload_socket.recv_pyobj()
            print("ideo rec")
            lock_save.acquire()
            storage[rec[0]] = list(rec[1],rec[2])
            lock_save.release()
            print("done")
        #else:       for download or replica stuff
            
            
            
        
        
    #send("data/DataKeeper/SampleVideo.mp4")
