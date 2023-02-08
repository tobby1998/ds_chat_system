import threading
import socket
import time
import ClientManager
import Server
from basics import Message


class ClientHandler(threading.Thread):
    def __init__(self, server, clientSocket, clientManager, i):
        threading.Thread.__init__(self)
        self.server = server
        self.clientSocket = clientSocket
        self.clientManager = clientManager
        self.sockNumber = i

        self.loginID = None
        self.groupChatMemberSet = set()     # a set to store the members of chat group
        self.sentMessages = dict()          # the messages that have been sent

        self.msgCoder = Message.ClientMessageCoder()    # an object to pack or unpack the message

    def run(self):
        self.handleClientSocket()

    # unpacked message structure -> a dict
    # msg_dic['source_id'] = strin.split("s$#")[1].split("*$s")[0]
    # msg_dic['target_id'] = strin.split("t$#")[1].split("*$t")[0]
    # msg_dic['type'] = str(strin.split("typ$#")[1].split("*$typ")[0])
    # msg_dic['content'] = strin.split("con$#")[1].split("*$con")[0]
    # msg_dic['uuid'] = strin.split("uuid$#")[1].split("*$uuid")[0]

    def handleClientSocket(self):
        while True:
            # print(self.sockNumber)
            try:
                message = str(self.clientSocket.recv(10240).decode())
                if message is not None:
                    msgSegment = self.msgCoder.msgUnpacking(message)
                    if msgSegment.get('type').lower() == 'Login'.lower():
                        self.handleLogin(msgSegment)

                    elif msgSegment.get('type').lower() == 'LogOff'.lower() \
                            or msgSegment.get('type').lower() == 'quit'.lower():
                        # print('megSegment in')
                        self.handleLogoff()
                        break

                    elif msgSegment.get('type').lower() == 'msg'.lower():
                        self.handleMessage(msgSegment)

                    elif msgSegment.get('type').lower() == 'getlist'.lower():
                        self.handleListMessage(msgSegment)

                    # elif msgSegment.get('type').lower() == 'leave'.lower():
                        # self.handleLeave(msgSegment)

                    else:
                        unknown = self.msgCoder.msgPack(None, None, 'unknown', msgSegment.get('type'))
                        self.clientSocket.sendall(unknown.encode())
            except (ConnectionResetError, OSError):
                # if client suddenly fails, operate logoff automatically
                self.handleLogoff()

    def getLogin(self):
        return self.loginID

    def send(self, msg):
        if self.loginID != 0:
            self.clientSocket.sendall(msg.encode())

    def handleLogin(self, msgSegment):
        # type: #'login' # content: 'loginID'
        if msgSegment.get('content') is not None:
            loginID = msgSegment.get('content')

            msg = self.msgCoder.msgPack(None, None, None, 'ok login!')
            self.clientSocket.sendall(msg.encode())

            self.loginID = loginID
            print('User ' + str(self.loginID) + ' logged in successfully!\n')
            self.clientManager.informClientConnect(self)
            self.sendOldMsg()

            workerList = self.clientManager.getWorkerList()
            print(F'worker List: {workerList}\n')

            # update socketDict
            for each in self.clientManager.socketDict.keys():
                if self.clientManager.socketDict.get(each) == str(self.sockNumber):
                    self.clientManager.socketDict.update({each: self.loginID})
            print(F'sockDict: {self.clientManager.socketDict}\n')

            # send current user other online users
            for each in workerList:
                if each.getLogin() is not None:
                    # print(F'getLogin: {each.getLogin()}\n')
                    # print(F'getSelfLogin: {self.loginID}\n')
                    if each.getLogin() != self.loginID:
                        m = self.msgCoder.msgPack(None, None, 'online', each.getLogin())
                        self.clientSocket.sendall(m.encode())

            # send other online users this user login
            online = self.msgCoder.msgPack(None, None, 'online', self.loginID)
            for each in workerList:
                if each.getLogin() != self.loginID:
                    each.clientSocket.sendall(online.encode())

    def sendOldMsg(self):
        time.sleep(1)
        # take out the old messages from the storageList
        msgList = self.clientManager.getReplicationManager().getStorageList(self.loginID)
        print(F'Old messages: <{msgList}>')

        if msgList is not None:
            workerList = self.clientManager.getWorkerList()

            for worker in workerList:
                if str(self.loginID).lower() == worker.getLogin().lower():
                    content = '========================= Unread messages =========================\n'
                    msg = self.msgCoder.msgPack(len(msgList), 'System', 'msg', content)
                    worker.send(msg)

                    i = 0
                    while i < len(msgList):
                        try:
                            worker.send(msgList[i])
                        except (IOError, OSError):
                            print(F'Error occurred when sending data...\n{msgList[i]}')
                        i += 1
                    contentEnd = '===================== End of Unread Messages ======================\n'
                    m = self.msgCoder.msgPack(None, 'System', 'msg', contentEnd)
                    worker.send(m)
                    break

    def handleLogoff(self):
        if self in self.clientManager.getWorkerList():
            self.clientManager.removeWorker(self)
            workerList = self.clientManager.getWorkerList()
            print(F'workerList: {workerList}\n')

            # send other users the current user's status
            msg = self.msgCoder.msgPack(None, None, 'offline', self.loginID)
            for each in workerList:
                if each.getLogin() != self.loginID:
                    # print('not equal')
                    each.clientSocket.sendall(msg.encode())

            # close socket
            self.clientSocket.shutdown(socket.SHUT_RDWR)
            self.clientSocket.close()
            # tell server group disconnect
            self.clientManager.informClientDisconnect(self)

    def handleMessage(self, msgSegment):
        receiver = msgSegment.get('target_id')
        messageNum = msgSegment.get('subtype')
        content = msgSegment.get('content')
        temp = []

        print('Handle Message ID ' + messageNum)
        if content == 'logoff':
            self.handleLogoff()
            return

        if '_end' in messageNum:
            return

        # update the new receiver
        if self.sentMessages.get(receiver) is None:
            self.sentMessages.update({receiver: temp})

        # check if there is duplicated message
        if messageNum in self.sentMessages.get(receiver):
            print('discarding duplicate message...\n')
            return

        # check the flag that it is group message
        isGroup = (receiver[0] == '#')
        isContained = False
        workerList = self.clientManager.getWorkerList()
        # print(F'workerList: {workerList}\n')

        # check the target client is served by this server
        for worker in workerList:
            # print('recevier.lower: ', receiver.lower())
            # print('worker.lower:', worker.getLogin().lower())
            if receiver.lower() == worker.getLogin().lower():
                isContained = True

        # print('C', isContained)
        # print('T', isGroup)

        for worker in workerList:
            if isGroup: # group message
                if worker.isMemberOfTopic(receiver):
                    outMsg = self.msgCoder.msgPack(messageNum, receiver + ':' + str(self.loginID), 'msg', content)
                    worker.send(outMsg)
                    self.sentMessages.get(receiver).append(messageNum)
            else:
                # print('recevier.lower2: ', receiver.lower())
                # print('worker.lower2:', worker.getLogin().lower())

                # one client message
                if receiver.lower() == worker.getLogin().lower():
                    outMsg = self.msgCoder.msgPack(messageNum, receiver, 'msg', content)
                    try:
                        worker.send(outMsg)
                        # print('sent')
                        self.sentMessages.get(receiver).append(messageNum)
                        # remove the messages to this receiver in replication
                        self.clientManager.getReplicationManager().getStorageList(receiver)
                    except (IOError, OSError):
                        # if the target client is disconnected, the message will be stored in replication
                        print(F'{receiver} disconnected\n')
                        self.clientManager.getReplicationManager().storeReplicationData(receiver, messageNum, outMsg)
                        isContained = True

        # not contained means the receiver is not on my server, so use cross server sending by replication
        if not isContained:
            print(F'{receiver} not reachable')
            replicationMessage = self.msgCoder.msgPack(messageNum, receiver, 'msg', content)
            self.clientManager.getReplicationManager().storeReplicationData(receiver, messageNum, replicationMessage)
            print(F'message "{content}" stored')

    # return the current online users
    def handleListMessage(self, msgSegment):
        content = msgSegment.get('content')
        if content.lower() == 'OnlineUser'.lower():
            clients = self.clientManager.getGroupManager().getReplicationManager().getConnectedClients()
            outContent = str()
            for client in clients:
                if client != self.loginID:
                    outContent = outContent + str(client) + ' '
            outMsg = self.msgCoder.msgPack('OnlineUser', None, 'getlist', outContent)
            self.clientSocket.sendall(outMsg.encode())


    def isMemberOfGroup(self, user):
        return user in self.groupChatMemberSet
