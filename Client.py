import zmq
import sys
from configparser import ConfigParser
import json

#Client takes arguments as follow:-
#1- Client ID
#2- 0 for upload, 1 for download
#3- Video Name
 
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
    downSocket = context.socket(zmq.PULL)
    for address in addresses:
        downSocket.connect("tcp://%s:%s"%(address[0], address[1]))
    data = downSocket.recv()
    with open(filePath, "wb") as file:
        file.write(data)

    for address in addresses:
        downSocket.disconnect("tcp://%s:%s"%(address[0], address[1]))
    downSocket.close()
        

if __name__ == '__main__':
    read()
    init_connection()
    ID = int(sys.argv[1])
    req = int(sys.argv[2])
    video = sys.argv[3]
    vidObj = open(video, 'rb').read()
    to_send = [video,vidObj]
    msg = {'req': req, 'file': video }  #req = 0 for upload  & 1 for download
    msg = json.dumps(msg)
    master_socket.send_json(msg)
    freePort = json.loads(master_socket.recv_json())
    if req == 0:
        socket_upload = context.socket(zmq.PUSH)
        socket_upload.connect("tcp://%s:%s"%(freePort['IP'], freePort['PORT'])) 
        socket_upload.send_pyobj(to_send)
    #else:            #for download
    socket_upload.disconnect("tcp://%s:%s"%(freePort['IP'], freePort['PORT']))
    
    #download('data/Client/test2.mp4', [('127.0.0.1', '6005')])
