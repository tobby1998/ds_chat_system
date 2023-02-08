from basics import Message
import socket
import threading
import GroupManager
import ClientHandler


class ClientManager(threading.Thread):
    def __init__(self, server, groupManager, name):
        threading.Thread.__init__(self)
        self.serverPort = groupManager.managerPort
        self.server = server
        self.manager = groupManager
        self.manager.setClientManager(self)
        self.setName(name)
        self.replicationManager = self.manager.getReplicationManager()

        self.workerList = []        # list to store the clientHandler
        self.socketDict = dict()    # dict to store the sockets and clients

    def run(self):
        i = 0              # number the socket
        print(F'ClientManager is ready at port: {str(self.serverPort)}\n')
        while True:
            print('accepting client connection...\n')

            # once a client TCP message comes, generate a clientSocket and a new handler thread
            clientSocket, addr = self.server.serverSocket.accept()
            worker = ClientHandler.ClientHandler(self.server, clientSocket, self, i)
            self.workerList.append(worker)
            worker.start()
            self.socketDict.update({clientSocket: str(i)})
            i += 1

    def getGroupManager(self):
        return self.manager

    def getReplicationManager(self):
        return self.replicationManager

    def getWorkerList(self):
        return self.workerList

    def removeWorker(self, clientHandler):
        self.workerList.remove(clientHandler)

    # A method to inform all servers in the server group that a new client has connected
    def informClientConnect(self, clientHandler):
        connectedClient = clientHandler.getLogin()
        print('Accepted connection from ' + connectedClient)
        m = self.manager.constructMessage(Message.MsgTyp.Confirm_Connect_Client, connectedClient)
        self.manager.send(m)

    # A method to inform all servers in the server group that a client has disconnected
    def informClientDisconnect(self, clientHandler):
        disconnectedClient = clientHandler.getLogin()
        print('disconnect: ', disconnectedClient)
        m = self.manager.constructMessage(Message.MsgTyp.Confirm_Disconnect_Client, disconnectedClient)
        self.manager.send(m)

