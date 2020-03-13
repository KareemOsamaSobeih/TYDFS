import zmq
import sys
import Conf

#Client takes arguments as follow:-
#1- Client ID
#2- 0 for upload, 1 for download
#3- Video Name
 
class Client:

    def __initConnection(self):
        self.__context = zmq.Context()
        self.__masterSocket = self.__context.socket(zmq.REQ)
        for port in Conf.MASTER_CLIENT_PORTs:
            self.__masterSocket.connect("tcp://%s:%s" % (Conf.MASTER_IP, port))

    def __init__(self, ID):
        self.__ID = ID
        self.__initConnection()

    def __downloadFromNode(self, filePath, addresses):
        downSocket = self.__context.socket(zmq.PULL)
        for address in addresses:
            downSocket.connect("tcp://%s:%s"%(address[0], address[1]))
        data = downSocket.recv()
        with open(filePath, "wb") as file:
            file.write(data)

        for address in addresses:
            downSocket.disconnect("tcp://%s:%s"%(address[0], address[1]))
        downSocket.close()
        
    def sendUploadRequest(self, fileName, filePath):
        with open(filePath, "rb") as file:
            toSend = {'fileName':fileName, 'file':file, 'clientID': self.__ID}
            msg = {'requestType': 'upload', 'file': fileName } 
            #print("video fetched")
            self.__masterSocket.send_json(msg)
            #print ("msg sent")
            freePort = self.__masterSocket.recv_json()
            #print("msg returned")

            socket_upload = self.__context.socket(zmq.PUSH)
            socket_upload.connect("tcp://%s:%s"%(freePort['IP'], freePort['PORT'])) 
            socket_upload.send_pyobj(toSend)

            socket_upload.disconnect("tcp://%s:%s"%(freePort['IP'], freePort['PORT']))
            socket_upload.close()

    def sendDownloadRequest(self, filename):
        pass

if __name__ == '__main__':
    
    ID = int(sys.argv[1])
    cl1 = Client(ID)

    while True:
        command = str.lower(input("Enter command [upload, download, exit]: "))
        if command == 'exit':
            break
        elif command == 'upload':
            fileName = input("Enter filename: ")
            filePath = input("Enter filePath: ")
            cl1.sendUploadRequest(fileName, filePath)
        elif command == 'download':
            fileName = input("Enter filename: ")
            cl1.sendDownloadRequest(fileName)
        else:
            print("%s: command not found" % (command))

    #download('data/Client/test2.mp4', [('127.0.0.1', '6005')])
