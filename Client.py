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

    def __downloadFromNode(self, filePath, masterMsg):
        addresses = masterMsg['freeports']
        numberOfChunks = masterMsg['numberofchunks']
        downSocket = self.__context.socket(zmq.PULL)
        for address in addresses:
            downSocket.connect("tcp://%s:%s"%(address['IP'], address['PORT']))
        data = []
        while numberOfChunks > 0:
            msg = downSocket.recv_pyobj()
            data.append(msg)
            numberOfChunks -= 1
        data.sort(key=lambda i: i['chunckNumber'])
        
        with open(filePath, "wb") as file:
            for i in data:
                file.write(i['data'])

        for address in addresses:
            downSocket.disconnect("tcp://%s:%s"%(address['IP'], address['PORT']))
        downSocket.close()
        return 'successful download'
        
    def sendUploadRequest(self, fileName, filePath):
        with open(filePath, "rb") as file:
            toSend = {'fileName':fileName, 'file':file.read(), 'clientID': self.__ID}
            msg = {'requestType': 'upload', 'file': fileName } 
            print("video fetched")
            self.__masterSocket.send_json(msg)
            print ("msg sent")
            freePort = self.__masterSocket.recv_json()
            print("msg returned")

            socket_upload = self.__context.socket(zmq.PUSH)
            socket_upload.connect("tcp://%s:%s"%(freePort['IP'], freePort['PORT'])) 
            socket_upload.send_pyobj(toSend)

            socket_upload.disconnect("tcp://%s:%s"%(freePort['IP'], freePort['PORT']))
            socket_upload.close()

    def sendDownloadRequest(self, fileName, filePath):
        requestMsg = {'requestType': 'download', 'file': fileName}
        self.__masterSocket.send_json(requestMsg)
        responseMsg = self.__masterSocket.recv_json()
        if  len(responseMsg['freeports']) == 0:
            return 'Failed to download'
        ret = self.__downloadFromNode(filePath, responseMsg)
        print(ret)

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
            filePath = input("Enter filePath: ")
            cl1.sendDownloadRequest(fileName, filePath)
        else:
            print("%s: command not found" % (command))

