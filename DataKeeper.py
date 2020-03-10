import zmq
ipAddress = '127.0.0.1'
port = '5000'

def send(filePath):
    context = zmq.Context()
    upSocket = context.socket(zmq.PUSH)
    upSocket.bind("tcp://%s:%s"%(ipAddress, port))
    with open(filePath, "rb") as file:
        upSocket.send(file.read())
    upSocket.close()

if __name__ == '__main__':
    send("data/DataKeeper/SampleVideo.mp4")