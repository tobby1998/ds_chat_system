import uuid
import socket
import GroupManager
import ClientManager


class Server:
    def __init__(self):
        self.myUUID = str(uuid.uuid1())
        # self.serverIP = ' '
        # self.serverPort = 0
        # self.socketaddress = (self.serverIP, self.serverPort)

        # TCP socket setting
        self.workerList = list([])
        self.serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # self.serverSocket.bind(('', 0))
        self.serverSocket.bind(('192.168.1.100', 1212))
        self.serverAddress = self.serverSocket.getsockname()
        self.serverSocket.listen(1)

    def getMyUUID(self):
        return self.myUUID

    def getWorkerList(self):
        return self.workerList

    def getServerSocket(self):
        return self.serverSocket


if __name__ == '__main__':
    server = Server()
    # introduction myself
    print(F'============SERVER-{server.myUUID} is Ready================\n')
    print(F'I am at {str(server.serverAddress)}\n')

    # initialize GroupManager to manage server group
    groupManager = GroupManager.GroupManager(server, 'GroupManager')
    groupManager.start()
    # print(groupManager.me)

    # initialize ClientManager to manage clients
    clientManager = ClientManager.ClientManager(server, groupManager, 'ClientManager')
    clientManager.start()
    # print(clientManager)
    # print(groupManager.clientManager)
