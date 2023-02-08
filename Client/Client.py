import sys
import time

sys.path.append('D:\\python\\Pycharm_workspace\\DSproject_1')
import socket
import threading
import re
import ClientConnectManager
import UserStatusListener
import ChatListener
from basics import Message


class Client:
    def __init__(self, userName):

        self.userName = userName
        self.MsgCounter = userName + '_0'

        self.synchronizer = threading.Event()    # synchronizer to synchronize two threads

        self.returnToChoose = False              # a flag to indicate if to return to play interface
        self.readerStart = False                 # indicate if reader is working

        # server and TCP things
        self.serverName = None                   # at first no server information
        self.serverAddress = None
        self.serverPort = None

        self.userStatusListenerList = []         # online notification
        self.chatMessageListenersList = []       # message notification

        self.msgCoder = Message.ClientMessageCoder()       # message packer

    def close(self):
        if self.clientSocket is not None:
            print('Socket closing...')
            self.clientSocket.shutdown(socket.SHUT_RDWR)
            # print('shutted')
            self.clientSocket.close()
            self.readerStart = False
            # print('status:', getattr(self.clientSocket, '_closed'))

    def setServer(self, serverId, address, serverPort):
        self.serverName = serverId
        self.serverAddress = address
        self.serverPort = serverPort

    # A method to connect to the given Server
    def connect(self):
        while self.serverName is None:
            time.sleep(1)

        print(
            'Current server: ' + str(self.serverName) + ' at ' + str(self.serverAddress) + ' : ' + str(self.serverPort))
        self.clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.clientSocket.connect((self.serverAddress, self.serverPort))
            return self.login(self.userName)

        except ConnectionRefusedError:
            return False

    def addUserStatusListener(self, listener):
        self.userStatusListenerList.append(listener)

    def addChatListener(self, listener):
        self.chatMessageListenersList.append(listener)

    def removeUserStatusListener(self, listener):
        self.userStatusListenerList.remove(listener)

    def removeChatListener(self, listener):
        self.chatMessageListenersList.remove(listener)

    # the operating interface
    def play(self):
        # print('active111: ', threading.active_count())
        print('========================= Chat Room =========================\n')
        print('Press #1 -> Show online users and chat with them\n')
        print('Press #2 -> Directly send messages to a custom username\n(if the receiver offline, messages will be '
              'sent after it online)\n')
        print('Press #3 -> Logoff\n')

        while True:
            p = input('please choose one of three options\n')
            if p in ['1', '2', '3']:
                break

        print('========================= Loading... =========================\n')
        if p == '1':
            self.requireList('OnlineUser')
            print('Loading current online users...\n')

            self.synchronizer.wait()
            # print('returnIndex', self.returnToChoose)
            self.synchronizer.clear()
            if not self.returnToChoose:
                while True:
                    currentUserName = input('Enter the name of user you want to chat with:\n')
                    if len(currentUserName.strip()) == 0:
                        print('please select a valid partner to chat with, otherwise you are speaking with air:)')
                        continue
                    else:
                        print('You are chatting with ', currentUserName)
                        break

                self.sendMsg(currentUserName)

            else:
                self.returnToChoose = False
                self.play()

            return

        elif p == '2':
            self.requireList('OnlineUser')
            print('Loading...\n')

            self.synchronizer.wait()
            self.synchronizer.clear()
            while True:
                currentUserName = input('Enter the name of user you want to chat with:\n')
                if len(currentUserName.strip()) == 0:
                    print('please select a valid partner to chat with, otherwise you are speaking with air:)')
                    continue
                else:
                    print('You are chatting with ', currentUserName)
                    break
            self.sendMsg(currentUserName)

            return

        elif p == '3':
            print('=======================Logging out...========================\n')
            self.logoff()
            return

        else:
            print('Please enter a valid number\n')
            self.play()

    def logoff(self):
        m = self.msgCoder.msgPack(None, None, 'logoff', None)
        # print('in logoff')
        self.clientSocket.sendall(m.encode())

    def login(self, userName):
        if self.readerStart:
            self.play()

        else:
            m = self.msgCoder.msgPack(None, None, 'login', userName)

            self.clientSocket.sendall(m.encode())
            # print('sent')

            responsePacked = self.clientSocket.recv(10240).decode()
            # print('res: ', responsePacked)
            responseUnpacked = self.msgCoder.msgUnpacking(responsePacked)
            if type(responseUnpacked) == str:
                if responseUnpacked.lower() == 'ok login!'.lower():
                    self.startReader()
                    return True
                else:
                    return False
            else:
                if responseUnpacked.get('content').lower() == 'ok login!'.lower():

                    self.startReader()
                    return True
                else:
                    return False

    def startReader(self):
        reader = threading.Thread(target=self.readMessage)
        reader.start()
        self.readerStart = True

    def readMessage(self):
        # print('Reading messages...\n')
        try:
            # if socket is closed, reader will be finished
            while not getattr(self.clientSocket, '_closed'):
                message = self.clientSocket.recv(10240).decode()
                # print(F'unsegment:{message}\n')
                # print(F'long:{len(message)}\n')

                if message is not None and len(message) != 0:

                    msgSegment = self.msgCoder.msgUnpacking(message)

                    if msgSegment.get('type').lower() == 'Online'.lower():
                        self.handleOnline(msgSegment)

                    elif msgSegment.get('type').lower() == 'Offline'.lower():
                        self.handleOffline(msgSegment)

                    elif msgSegment.get('type').lower() == 'msg'.lower():
                        self.handleMessage(msgSegment)

                    elif msgSegment.get('type').lower() == 'getlist'.lower():
                        self.handleListMessage(msgSegment)

                else:
                    self.close()

            self.readerStart = False

        except ConnectionResetError:
            print('==============Sorry, server failed, reconnecting...=============')
            self.readerStart = False

    def handleOnline(self, msgSegment):
        login = msgSegment.get('content')
        for each in self.userStatusListenerList:
            each.onlinePrint(login)

    def handleOffline(self, msgSegment):
        login = msgSegment.get('content')
        for each in self.userStatusListenerList:
            each.offlinePrint(login)

    def handleMessage(self, msgSegment):
        login = msgSegment.get('target_id')
        content = msgSegment.get('content')

        for each in self.chatMessageListenersList:
            if self.userName == login:
                login = msgSegment.get('subtype').split('_')[0]
            each.onMessage(login, content)

    def handleListMessage(self, msgSegment):
        type = msgSegment.get('subtype')
        if type == 'OnlineUser':
            content = msgSegment.get('content')
            OnlineMember = content.split()
            if len(OnlineMember) == 0:
                print('No user is online')
                self.returnToChoose = True
                self.synchronizer.set()
                return

            for chatMessageListener in self.chatMessageListenersList:
                for each in OnlineMember:
                    chatMessageListener.onMessage('User', each)

            self.synchronizer.set()

    def requireList(self, user):
        require = self.msgCoder.msgPack(None, None, 'getList', user)
        self.clientSocket.sendall(require.encode())

    def sendMsg(self, currentUserName):
        while True:
            msgContent = input('me:')
            if msgContent.lower() == 'quit'.lower():
                print('====Chat End====\n')
                break
            else:
                self.constructMsgAndSend(currentUserName, msgContent)

        self.play()

    def constructMsgAndSend(self, receiver, msgContent):
        m = self.msgCoder.msgPack(self.MsgCounter, receiver, 'msg', msgContent)
        self.clientSocket.sendall(m.encode())
        self.increaseMessageCounter()

    def increaseMessageCounter(self):
        temp = str(self.MsgCounter).split('_')
        count = int(temp[1])
        count += 1
        self.MsgCounter = temp[0] + '_' + str(count)


if __name__ == '__main__':
    # set Username
    name = input('please enter your username (Username can not be changed anymore): ')
    while (' ' in name) or (name.__eq__('')) or (re.match('[0-9]+$', name)):
        name = input("Username can't contain whitespaces, be only a number or be empty!\nPlease enter a valid name :")
        if not (' ' in name) and not (name.__eq__('')) and not (re.match('[0-9]+$', name)):
            break
    print('===============Name set successfully!==============\n')

    client = Client(name)
    # set multicast address
    multicastGroup = '224.1.1.1'
    port = 8888

    # initialize the clientConnectMessage
    clientConnectManager = ClientConnectManager.ClientConnectManager(client, multicastGroup, port,
                                                                     'Client ConnectManager')
    clientConnectManager.start()

    userStatusListener = UserStatusListener.UserStatusListener()
    chatListener = ChatListener.ChatListener()

    client.addUserStatusListener(userStatusListener)
    client.addChatListener(chatListener)

    if client.connect():
        print('Connect successful!')
        client.play()
    else:
        print('Connect failed, please restart and try again...')
