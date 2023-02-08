from basics import Message
from basics import Participant
import GroupManager
import threading


class ReplicationManager:
    def __init__(self, groupManager, me):
        self.groupManager = groupManager
        self.me = me

        # the messages that are stored in the Map, which will be sent to the disconnect clients served by this server
        self.storageMap = dict()

        # the messages to the clients which are served by another server
        self.crossServerMessages = dict()

        # an object to pack and unpack the message
        self.msgCoder = Message.ClientMessageCoder()
        self.lock = threading.Lock()

    # send messages in storageMap
    def sendReplicationData(self, storageMap):
        print(F'Sending replicationMap: {storageMap}\n')
        storageMap = self.searchAndRemoveDuplication(storageMap)
        storageDataToReplicate = Message.Message(self.me, self.me, Message.MsgTyp.Update_Replication_Storage)
        storageDataToReplicate.setContent(storageMap)
        self.groupManager.coSend(storageDataToReplicate)

    # find the clients that still online
    def getConnectedClients(self):
        serverDict = dict(self.groupManager.getMemberServersAndTheirClients())
        clients = []
        print('serverDict: ', serverDict)
        for each in serverDict.keys():
            if len(serverDict.get(each)) != 0:
                for client in serverDict.get(each):
                    clients.append(client)
        print('clientsList: ', clients)
        return clients

    # send message to the clients that are not on this server
    def distributeMessage(self, crossServerMessage):
        msg = Message.Message(self.me, self.me, Message.MsgTyp.Initiate_Message_Delivery)
        msg.setContent(crossServerMessage)
        self.groupManager.coSend(msg)

    # sort out the storage the Map
    def searchAndRemoveDuplication(self, storageMap):
        containedMessageCounters = []
        for each in storageMap.keys():
            if each == 'System':
                continue

            containedMessages = []

            i = 0
            while i < len(storageMap.get(each)):
                msg = storageMap.get(each)[i]
                msgSegment = self.msgCoder.msgUnpacking(msg)
                num = msgSegment.get('subtype')
                nid = num.split('_')

                if num not in containedMessageCounters:
                    if len(nid) == 2:
                        containedMessageCounters.append(num)
                        containedMessages.append(msg)
                i += 1

            storageMap.get(each).clear()
            for msg in containedMessages:
                storageMap.get(each).append(msg)

        return storageMap

    # given a receiver, return a message list of this client
    def getStorageList(self, receiver):
        self.lock.acquire()
        if receiver in self.storageMap:
            msgList = self.storageMap.get(receiver)
            self.storageMap.pop(receiver)
            self.lock.release()
            return msgList
        else:
            self.lock.release()
        return None

    # store the unsent messages
    def storeReplicationData(self, receiver, messageNum, outMsg):
        with self.lock:
            connectedClients = self.getConnectedClients()
            if receiver in connectedClients:
                # message for delivery to Client that is on other servers
                if receiver in self.crossServerMessages.keys():

                    for containedMessage in self.crossServerMessages.get(receiver):
                        msgSegment = self.msgCoder.msgUnpacking(containedMessage)
                        print('content:,', msgSegment.get('content'))
                        num = msgSegment.get('subtype').split('_')[1]
                        if num == messageNum:
                            return
                        elif num == 'end':
                            print('found delivered in msg')

                        print('receiver: ', receiver)

                    self.crossServerMessages.get(receiver).append(outMsg)
                else:
                    tempList = [outMsg]
                    self.crossServerMessages.update({receiver: tempList})

            # the messages to offline clients are stored by storageMap
            elif receiver in self.storageMap.keys():
                # print('storageMap: ', self.storageMap)
                # print('stored')
                for containedMessage in self.storageMap.get(receiver):
                    msgSegment = self.msgCoder.msgUnpacking(containedMessage)
                    num = msgSegment.get('subtype').split('_')[1]
                    if num == messageNum:
                        # print('matched, no action!')
                        return
                    elif num == 'end':
                        print('found delivered in storage!')
                self.storageMap.get(receiver).append(outMsg)
                # print('storageMap2: ', self.storageMap)

            else:
                tempList = [outMsg]
                self.storageMap.update({receiver: tempList})
                # print('storageMap0: ', self.storageMap)

            # update the current message container to every server
            # send the crossServer messages
            if len(self.crossServerMessages) != 0:
                self.distributeMessage(self.crossServerMessages)
                print('sending instant message')
            elif len(self.storageMap) != 0:
                self.sendReplicationData(self.storageMap)
                print('sending storage message')

    def getStorageMap(self):
        return self.storageMap

    def getCrossServerMessage(self):
        return self.crossServerMessages
