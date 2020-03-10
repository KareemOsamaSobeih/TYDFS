import zmq

def download(filePath, addresses):
    context = zmq.Context()
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
    download('data/Client/test1.mp4', [('127.0.0.1', '5000')])