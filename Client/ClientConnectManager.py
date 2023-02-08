import threading
import socket
from basics import Message
from basics import Participant
import ConnectMessageListener
import struct
import TimerTask
import pickle


class ClientConnectManager(threading.Thread):
    def __init__(self, client, multicastGroup, port, name):
        threading.Thread.__init__(self)
        self.client = client
        self.setName(name)
        self.lock = threading.Lock()

        # multicast address
        self.multicastGroup = multicastGroup
        self.mAddress = ('', port)
        self.multicastPort = port
        self.multicastPoint = (self.multicastGroup, self.multicastPort)

        # multicast setting
        self.cmSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ttl = struct.pack('b', 1)
        self.cmSocket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
        print('ClientConnectManager is ready!')

    def run(self):
        listener = ConnectMessageListener.ConnectMessageListener(self.multicastGroup, self.multicastPort,
                                                                 'Connection Message Listener')
        listener.start()

        print('Connecting to a server...')
        # initialize a thread to send multicast every 3 secs, which to wait ready of server groups
        timerTask = TimerTask.TimerTask(self.client.userName, 3, self.cmSocket)
        timerTask.start()

        while True:
            message = listener.getFromQueue()
            # print('message get')
            if message is None:
                continue
            else:
                msgTyp = message.getTyp()
                # print("get from Queue", msgTyp)

                if msgTyp == Message.MsgTyp.Reply_Connect_Client:
                    if message.getReceiver().getIdentifier() == self.client.userName:
                        # print(message)
                        # if client receives this message, means server group has already contacts with client
                        timerTask.cancel()
                        self.buildConnection(message)

                # if server dead, server group will inform client to switch
                elif msgTyp == Message.MsgTyp.Client_Switch_Server:
                    if message.getReceiver().getIdentifier() == self.client.userName:
                        # print(message)
                        self.updateConnection(message)

                # elif msgTyp == Message.MsgTyp.Request_Join_Client:
                    # print('hahhhaha')

    def connectToServer(self):
        connectMsg = self.constructMessage(Message.MsgTyp.Request_Join_Client)
        connectMsg = pickle.dumps(connectMsg)
        self.cmSocket.send(connectMsg)

    def buildConnection(self, message):
        server = message.getContent()
        # print('server:', server)
        serverID = server.getIdentifier()
        addressServer = server.getAddress()
        portServer = server.getPort()
        self.client.setServer(serverID, addressServer, portServer)

    # relogin the client
    def updateConnection(self, message):
        self.client.close()
        self.buildConnection(message)
        # print('active00: ', threading.active_count())

        if not self.client.connect():
            print('Connect failed. Retrying...\n')
            self.connectToServer()
        else:
            print('===============Connection Successful!==============\n')
            # print('active: ', threading.active_count())
            m = self.constructMessage(Message.MsgTyp.Update_Connect_Client)
            serverClientPair = dict({self.client.userName: message.getContent()})
            m.setContent(serverClientPair)
            self.mSend(m)
            self.client.readerStart = True
            self.client.login(self.client.userName)

    def constructMessage(self, msgTpy):
        me = Participant.Participant(self.client.userName, None, 0)
        mC = Participant.Participant(None, '224.1.1.1', 8888)
        typMsg = msgTpy
        m = Message.Message(me, mC, typMsg)
        return m

    def mSend(self, m):
        message = pickle.dumps(m)
        self.cmSocket.sendto(message, self.multicastPoint)
