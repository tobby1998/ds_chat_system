import threading
# import sys

# sys.path.append('D:\\python\\Pycharm_workspace\\DSproject')

from basics import Concurrentqueue
import socket
from basics import Message
import struct
import sys
import pickle
from basics import VectorClock


class MessageManager(threading.Thread):
    def __init__(self, uuid, ipAddress, port, manager, name):
        threading.Thread.__init__(self)
        self.socketMessManager = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.groupManager = manager
        self.setName(name)
        self.holdBackList = []
        self.deliverQueue = Concurrentqueue.Concurrentqueue()
        self.groupManagerQueue = Concurrentqueue.Concurrentqueue()
        self.checkDuplicate = []

        # multicast setting
        self.multicastGroup = '224.1.1.1'
        self.serverAddress = ('', 8888)
        self.socketMessManager.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socketMessManager.bind(self.serverAddress)
        self.group = socket.inet_aton(self.multicastGroup)
        self.mreq = struct.pack("4sl", self.group, socket.INADDR_ANY)
        self.socketMessManager.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, self.mreq)

    def run(self):
        # listen to the specific multicast port
        print('LISTENER: Listener is started...')

        while True:
            # print('MessManager is waiting to receive message')
            data, address = self.socketMessManager.recvfrom(10240)
            # print('MessManager has received message:')
            inMessage = pickle.loads(data)
            # print(inMessage)
            if inMessage.getTyp() != Message.MsgTyp.Heart_Beat:
                print(inMessage.getTyp())
                print('MessManager is sending acknowledgement to', address)
            # ack = pickle.dumps('ack')
            # self.socketMessManager.sendto(ack, address)

            assert inMessage is not None
            if (str(inMessage.getTyp()).__eq__('MsgTyp.Heart_Beat') or
                    str(inMessage.getTyp()).__eq__('MsgTyp.No_Connection')):
                self.putHeartbeatMessage(inMessage)
            elif ('Join' in str(inMessage.getTyp()) or
                  'Connect' in str(inMessage.getTyp()) or
                  'Leave' in str(inMessage.getTyp()) or
                  'Election' in str(inMessage.getTyp()) or
                  'Disconnect' in str(inMessage.getTyp()) or
                  str(inMessage.getTyp()).__eq__('MsgTyp.Client_Switch_Server')) or\
                    inMessage.getTyp() == Message.MsgTyp.Clear_Cross_Server_Map:
                self.putMembershipMessage(inMessage)
            elif inMessage.getTyp() == Message.MsgTyp.Update_Replication_Storage or \
                    inMessage.getTyp() == Message.MsgTyp.Initiate_Message_Delivery:
                self.incomingReplication(inMessage)

    def putMembershipMessage(self, message):
        self.groupManagerQueue.put(message)

    def getFromDeliverQueue(self):
        return self.deliverQueue.pop()

    def getFromGroupManagerQueue(self):
        return self.groupManagerQueue.pop()

    def putHeartbeatMessage(self, message):
        self.groupManagerQueue.put(message)

    def incomingReplication(self, message):
        if not self.groupManager.reliableMC:
            self.groupManagerQueue.put(message)
        else:
            self.causalOrderDeliver(message)

    # deliver the message according to causality
    # reliable causal ordering for replication message
    def causalOrderDeliver(self, message):
        if (not message.getTyp() == Message.MsgTyp.Request_Join_Server) and \
                message.getVectorClock().isHappenBefore(self.groupManager.getGroupVc()):
            return
        # print('self ', self.groupManager.getGroupVc().vectorTimestamp)
        # print('message: ,', message.getVectorClock().vectorTimestamp)
        # print('duplicate:', self.checkDuplicate)

        # deliver message at most once
        for each in self.checkDuplicate:
            if each.getUuid() == message.getUuid():
                print('discarding the duplicate message')
                return

        # if the causality conditions are not met, the message will be placed in the holdBackList
        if not self.meetCausality(message):
            self.holdBackList.append(message)
            self.checkDuplicate.append(message)
        else:
            # print('causal ordering p1\n')
            self.groupManagerQueue.put(message)
            self.checkDuplicate.append(message)
            print('Causal Delivered\n')
            self.groupManager.getGroupVc().causalOrderUpdate(message)

            # check whether there are messages waiting in the holdBackList
            if len(self.holdBackList) == 0:
                return

            # if there are messages, go through the list and find the message which can be moved
            # into the deliverQueue. Until there is no qualified message or the list is empty
            loop = True
            while loop:
                loop = False
                if len(self.holdBackList) == 0:
                    break
                for each in self.holdBackList:
                    if self.meetCausality(each):
                        # print('causal ordering p01\n')
                        self.groupManagerQueue.put(each)
                        self.groupManager.getGroupVc().causalOrderUpdate(each)
                        print('causal delivered from HBL\n')
                        self.holdBackList.remove(each)
                        loop = True

    # check the satisfaction of causal conditions
    def meetCausality(self, message):
        inVC = VectorClock.VectorClock.fromVectorClock(message.getVectorClock())
        messageSender = message.getSender().getIdentifier()
        if messageSender == self.groupManager.managerUUID and \
                len(self.groupManager.getMemberServersAndTheirClients()) == 1:
            if self.groupManager.getGroupVc().getVectorTimestamp().get(messageSender) < \
                    inVC.getVectorTimestamp().get(messageSender):
                return False
            else:
                # print('meeting\n')
                return True

        #   VGj[j]=VGi[j]+1
        # print('front:', self.groupManager.getGroupVc().getVectorTimestamp().get(messageSender))
        # print('behind:', inVC.getVectorTimestamp().get(messageSender))
        if ((self.groupManager.getGroupVc().getVectorTimestamp().get(messageSender) + 1) !=
                inVC.getVectorTimestamp().get(messageSender)):
            return False
        # VGj[k]<=VGi[k] for any k!=j
        pSetExSender = set(self.groupManager.getGroupVc().getVectorTimestamp().keys())
        pSetExSender.remove(messageSender)
        for each in pSetExSender:
            # print('svc:', self.groupManager.getGroupVc().getVectorTimestamp().get(each))
            # print('inVC', inVC.getVectorTimestamp().get(each))
            if (self.groupManager.getGroupVc().getVectorTimestamp().get(each) <
                    inVC.getVectorTimestamp().get(each)):
                return False
        # print('meeting!!!\n')
        return True


# test
if __name__ == '__main__':
    mess = MessageManager(1, 1, 1, 1, 1)
    mess.start()
