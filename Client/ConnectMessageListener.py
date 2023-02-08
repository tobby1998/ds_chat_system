import socket
from basics import Message
from basics import Concurrentqueue
import threading
import pickle
import struct


class ConnectMessageListener(threading.Thread):
    def __init__(self, idAddress, port, name):
        threading.Thread.__init__(self)
        self.cmListener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.deliveryQueue = Concurrentqueue.Concurrentqueue()
        self.setName(name)

        # multicast setting
        self.multicastGroup = idAddress
        self.multicastPort = port
        self.serverAddress = ('', 8888)
        self.cmListener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.cmListener.bind(self.serverAddress)
        self.group = socket.inet_aton(self.multicastGroup)
        self.mreq = struct.pack("4sl", self.group, socket.INADDR_ANY)
        self.cmListener.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, self.mreq)

    def getFromQueue(self):
        return self.deliveryQueue.pop()

    def run(self):
        print('ConnectMessageListener start...')
        while True:
            # print('waiting to receive message...')
            data, address = self.cmListener.recvfrom(10240)
            # print('received message:')
            message = pickle.loads(data)
            '''
            if message.getTyp() != Message.MsgTyp.Heart_Beat:
                print(message.getTyp())
                print('sending acknowledgement to', address)
            '''
            # ack = pickle.dumps('ack')
            # self.cmSocket.sendto(ack, address)

            # only listen these two types of messages
            if message is not None:
                msgTyp = message.getTyp()

                if msgTyp == Message.MsgTyp.Request_Join_Client:
                    # print('match!')

                    self.deliveryQueue.put(message)

                if msgTyp == Message.MsgTyp.Reply_Connect_Client or msgTyp == Message.MsgTyp.Client_Switch_Server:
                    # print('put in Queue')
                    self.deliveryQueue.put(message)
