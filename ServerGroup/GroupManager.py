import sys

sys.path.append('D:\\python\\Pycharm_workspace\\DSproject_12r')

from basics import Participant
from basics import VectorClock
from basics import GroupView
from basics import Message
import ReplicationManager
import SafeCounter
import HeartbeatDetector
import MessManager
import threading
from threading import Timer
import pickle
import struct
import socket
import functools
import time
import random
import copy


class GroupManager(threading.Thread):
    def __init__(self, server, name):
        threading.Thread.__init__(self)
        self.clientManager = None
        self.managerUUID = server.myUUID
        self.setName(name)
        self.serverAddress = server.serverSocket.getsockname()
        self.viewCounter = SafeCounter.SafeCounter(1)
        self.lock = threading.Lock()
        self.msgCoder = Message.ClientMessageCoder()

        # Server's Address
        self.managerIP, self.managerPort = self.serverAddress[0], self.serverAddress[1]
        self.me = Participant.Participant(server.myUUID, self.managerIP, self.managerPort)
        # self.groupVC = VectorClock.VectorClock.fromString(self.managerUUID)
        self.groupVC = VectorClock.VectorClock(dict())

        # Group's multicast Address
        self.multicastGroup = '224.1.1.1'
        self.multicastPort = 8888
        self.multicastPoint = (self.multicastGroup, self.multicastPort)

        # set multicast socket
        self.socketMC = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # self.socketMC.settimeout(2.0)
        ttl = struct.pack('b', 1)
        self.socketMC.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)

        # initialize messageListener to listen Multicast messages
        self.messageListener = MessManager.MessageManager(self.managerUUID, self.multicastGroup,
                                                          self.multicastPort, self,
                                                          'Message Listener')
        self.messageListener.start()

        # each GroupManager has a replication object to realize replication function
        self.replicationManager = ReplicationManager.ReplicationManager(self, self.me)

        # some containers
        self.lastHeartBeat = []
        self.electionSet = set()             # the set contains the servers that join the election
        self.electionSet.add(self.managerUUID)
        self.electionConfirmBuffer = set()   # the set contains the servers that confirm to join the election
        self.joiningMembers = []             # the list contains the incoming servers but not confirmed by server group
        self.leavingMembers = set()          # the list contains the leaving servers but not confirmed by group
        self.memberServersAndTheirClients = dict()  # a mapping from server to their clients
        self.view = None                     # groupView
        self.infoServer = dict()             # a mapping from ServerID to Server Participant object
        self.infoServer.update({self.me.getIdentifier(): self.me})
        self.newViewsList = []               # a container of new group view
        self.viewMessageBuffer = []          # a container of Ack messages
        self.connectingClients = set()       # a container of connecting clients but not be confirmed
        self.acknowledgedParticipants = set()  # the servers which are acked
        self.ackMessagesToRemove = []          # the ack messages which will be removed
        self.clientServerMapping = dict()    # a mapping from clients to Servers

        # some state flag
        self.timeout = False
        self.inElection = False              # indicate the server in election
        self.firstMessage = True             # indicate the server is still not ready
        self.isLeader = True                 # I am the leader
        self.viewTransfer = True             # the group view is changing
        self.reliableMC = False              # If false, reliable multicast will not be used until True

        # some variables
        self.leader = None                   # indicate who is leader

    def run(self):
        # each starting server will find group firstly
        self.findGroup()

        # initialize the heatBeatDetector
        heartBeatDetector = HeartbeatDetector.HeartbeatDetector(self.me, self, 'HeartBeat')
        heartBeatDetector.start()

        # initiate a TimerTask for starting first election
        timertask = Timer(6, self.electionTimerTask)
        timertask.start()

        # a message will be eventually retrieved from GM queue.
        # Run infinitely
        while True:
            message = self.messageListener.getFromGroupManagerQueue()
            '''
            if message.getTyp() == Message.MsgTyp.Update_Replication_Storage:
                print(F'Typ: {message.getTyp()}\n')
                print(F'message from : {message.getSender().getIdentifier()}\n')
                print(F'mVC: {message.getVectorClock().vectorTimestamp}\n')
                print(F'self: {self.groupVC.vectorTimestamp}')
            '''
            if message is None:
                continue
            else:
                msgTyp = message.getTyp()

                if self.firstMessage:
                    # Leader responds to Request_Join_Server with Update_Join_Server
                    if msgTyp == Message.MsgTyp.Update_Join_Server:
                        timertask.cancel()
                        self.firstMessage = False
                        self.inElection = False
                        self.isLeader = False

                    elif msgTyp == Message.MsgTyp.Request_Join_Server:
                        # other server requests to join, if I start first, I will receive this message from other
                        # servers

                        if str(message.getSender().getIdentifier()) != \
                                str(self.me.getIdentifier()):
                            if not self.inElection:
                                self.electionSet.add(message.getSender().getIdentifier())
                                m = self.constructMessage(Message.MsgTyp.Election_Prepare, self.electionSet)
                                self.send(m)
                                self.infoServer.update({message.getSender().getIdentifier(): message.getSender()})
                            else:
                                self.joiningMembers.append(message.getSender().getIdentifier())
                        else:
                            print('My group manager works!')

                    elif msgTyp == Message.MsgTyp.Election_Start:
                        # if someone sent this message, means the election is called by him, so I cancel the timer task
                        timertask.cancel()
                        self.inElection = True
                        self.handlesElection(message)

                    # this two messages are used to confirm the current election servers
                    elif msgTyp == Message.MsgTyp.Election_Prepare or \
                            msgTyp == Message.MsgTyp.Election_Confirm:
                        self.handlesElection(message)

                    # sent by HearBeat Detector
                    elif msgTyp == Message.MsgTyp.No_Connection:
                        # some one left before the election finished
                        s = str(message.getContent())
                        self.leavingMembers.add(s)

                if self.view is None:
                    # I don't have view and I am waiting the leader update the gV
                    if msgTyp == Message.MsgTyp.Update_Join_Server:
                        # means someone(leader) already updated the groupView
                        self.handlesUpdate(message)

                    # to confirm the join in behavior
                    elif msgTyp == Message.MsgTyp.Ack_Join_Server:
                        self.handlesAck(message)

                else:
                    # the group is successfully built, now comes a new server
                    if msgTyp == Message.MsgTyp.Request_Join_Server:
                        self.reliableMC = False
                        if str(message.getSender().getIdentifier()) != str(self.me.getIdentifier()):
                            self.joiningMembers.append(message.getSender().getIdentifier())

                            # only leader handles this newcomer
                            if self.isLeader:
                                self.handlesJoinRequest(message)

                    elif msgTyp == Message.MsgTyp.Update_Join_Server:
                        self.handlesUpdate(message)

                    elif msgTyp == Message.MsgTyp.Update_Leave_Server:
                        self.handlesLeave(message)

                    elif msgTyp == Message.MsgTyp.No_Connection:
                        self.handlesNoConnection(message)

                    # sent by heartbeat detector in a frequency
                    elif msgTyp == Message.MsgTyp.Heart_Beat:
                        self.handleHeartBeat(message)

                    elif msgTyp == Message.MsgTyp.Ack_Join_Server \
                            or msgTyp == Message.MsgTyp.Ack_Leave_Server:

                        self.handlesAck(message)

                    # messages from clients
                    elif msgTyp == Message.MsgTyp.Request_Join_Client \
                            or msgTyp == Message.MsgTyp.Update_Connect_Client \
                            or msgTyp == Message.MsgTyp.Confirm_Connect_Client \
                            or msgTyp == Message.MsgTyp.Confirm_Disconnect_Client \
                            or msgTyp == Message.MsgTyp.Client_Switch_Server \
                            or msgTyp == Message.MsgTyp.Reply_Connect_Client:

                        self.handlesClientsInfo(message)

                    # send messages which must be cross server delivered
                    elif msgTyp == Message.MsgTyp.Initiate_Message_Delivery:
                        # print('in Deliver')
                        receivedUnsentMessage = message.getContent()
                        delivered = []
                        myClients = self.memberServersAndTheirClients.get(self.me.getIdentifier())
                        for receiver in receivedUnsentMessage.keys():
                            if (receiver in myClients) and len(receivedUnsentMessage.get(receiver)) != 0:
                                print(F'My Clients are: {myClients}\n')
                                print(F'Sending following messages:\n {receivedUnsentMessage.get(receiver)}\n')
                                for client in self.clientManager.getWorkerList():
                                    for msg in receivedUnsentMessage.get(receiver):
                                        if client.getLogin() == receiver:
                                            msgSegment = self.msgCoder.msgUnpacking(msg)
                                            if '_end' not in msgSegment.get('subtype'):
                                                client.send(msg)
                                                delivered.append(msg)
                                                # print('listCross1: ', len(
                                                # self.replicationManager.crossServerMessages))
                                                self.replicationManager.crossServerMessages.clear()
                                                # print('listCross2: ', len(
                                                # self.replicationManager.crossServerMessages))
                                                clearMsg = Message.Message(self.me, self.me,
                                                                           Message.MsgTyp.Clear_Cross_Server_Map)
                                                self.send(clearMsg)

                    elif msgTyp == Message.MsgTyp.Clear_Cross_Server_Map:
                        self.replicationManager.crossServerMessages.clear()
                        # print('clear', self.replicationManager.crossServerMessages)

                    # send incoming unsent messages to clientManager
                    elif msgTyp == Message.MsgTyp.Update_Replication_Storage:
                        receivedUnsentMessage = message.getContent()

                        for receiver in receivedUnsentMessage.keys():
                            listToSend = receivedUnsentMessage.get(receiver)
                            for msg in listToSend:
                                num = self.msgCoder.msgUnpacking(msg).get('subtype').split('_')[1]
                                print(F'receiver:{receiver}, num:{num}, msg:{msg}')
                                self.replicationManager.storeReplicationData(receiver, num, msg)

    def findGroup(self):
        print('GroupManager: Sending joining message...')
        joinMsg = Message.Message(self.me, Participant.Participant(None, self.multicastGroup, self.multicastPort),
                                  Message.MsgTyp.Request_Join_Server)
        # print('F', str(self.me.getIdentifier()))
        self.send(joinMsg)
        # self.i += 1
        # print('request sent times', self.i)

    def electionTimerTask(self):
        self.timeout = True
        # print('election:', self.electionSet)
        if len(self.electionSet) == 1:
            # only me so I am the leader!
            self.winElection()
            self.refreshVC()
        else:
            # Election initialize
            self.inElection = True
            m = self.constructMessage(Message.MsgTyp.Election_Start, self.electionSet)
            print('Election starts!')
            print('Election members are' + str(self.electionSet))
            for each in self.electionSet:
                print(each)
            self.send(m)

    # attach the current vc and send to multicast, this kind of messages is ip-multicast
    # no incremental of vc.
    def send(self, message):
        message.setVectorClock(self.groupVC)
        message = pickle.dumps(message)
        self.socketMC.sendto(message, self.multicastPoint)

    # attach the vc, increase the vC to realize the causal ordering multicast
    # for replication. in closed group
    def coSend(self, message):
        if self.reliableMC:
            self.groupVC.meSendMessage(self.managerUUID)
            message.setVectorClock(self.groupVC)
            message = pickle.dumps(message)
            self.socketMC.sendto(message, self.multicastPoint)
        else:
            self.send(message)

    # return the heart beat list
    def getHeartBeatList(self):
        with self.lock:
            beatList = self.lastHeartBeat
        return beatList

    # called after the clientManager starts
    def setClientManager(self, clientManager):
        self.clientManager = clientManager

    def getClientManager(self):
        return self.clientManager

    # the leader operation after Election
    # generate a new view, change state flag, show the current members
    def winElection(self):
        self.getMemberServersAndTheirClients().update({self.me.getIdentifier(): set()})
        # print('get:', self.getMemberServersAndTheirClients())
        self.view = GroupView.GroupView(self.getMemberServersAndTheirClients(), 1)
        self.viewTransfer = False
        self.firstMessage = False
        print("I'm the Leader\n")
        self.printCurrentMember()
        print('=========================================\n')

    def constructMessage(self, msgTyp, content):
        message = Message.Message(self.me, Participant.Participant(None, self.multicastGroup, self.multicastPort),
                                  msgTyp)
        message.setContent(content)
        return message

    def constructMessageTyp(self, msgTyp):
        message = Message.Message(self.me, Participant.Participant(None, self.multicastGroup, self.multicastPort),
                                  msgTyp)
        return message

    def handlesElection(self, message):
        msgTyp = message.getTyp()
        receivedMemberSet = set(message.getContent())
        # print('received:', receivedMemberSet)

        # prepare message to help server update the current participants
        if msgTyp == Message.MsgTyp.Election_Prepare:
            # print(message)
            self.electionSet = self.electionSet.union(receivedMemberSet)
            # print('received u', self.electionSet)

        # if the condition meets, send confirm message with current election set
        elif msgTyp == Message.MsgTyp.Election_Start:
            if receivedMemberSet != self.electionSet:
                self.inElection = True
                self.electionSet = self.electionSet.union(receivedMemberSet)
                m = self.constructMessage(Message.MsgTyp.Election_Start, self.electionSet)
                self.electionConfirmBuffer.clear()
                self.send(m)
            else:
                m = self.constructMessage(Message.MsgTyp.Election_Confirm, self.electionSet)
                self.send(m)

        # election!
        elif msgTyp == Message.MsgTyp.Election_Confirm:
            self.electionConfirmBuffer.add(message.getSender().getIdentifier())
            # print('electionBuffer:', self.electionConfirmBuffer)
            tmp = set(self.electionSet)
            # print('tmp1:', tmp)
            # print('leaving:', self.leavingMembers)
            tmp.difference_update(self.leavingMembers)
            # print('tmp2:', tmp)
            if self.electionConfirmBuffer == self.electionSet or self.electionConfirmBuffer == tmp:
                self.inElection = False
                self.firstMessage = False
                self.leavingMembers.clear()
                sortedList = self.electionSort()
                # print('sortedList:', sortedList)
                leader = sortedList[0]

                print('======================Election successful!======================\n')
                print('Leader is:', leader)

                if leader == self.me.getIdentifier():
                    self.winElection()
                    self.electionConfirmBuffer.remove(self.me.getIdentifier())
                    memberDict = self.getMemberServersAndTheirClients()
                    for each in self.electionConfirmBuffer:
                        memberDict.update({each: set()})
                        newView = GroupView.GroupView(memberDict, self.viewCounter.addAndGet(1))
                        print('newView', newView.groupView, newView.viewCount)
                        m = self.constructMessage(Message.MsgTyp.Update_Join_Server, newView)
                        self.send(m)
                else:
                    self.isLeader = False

    # election according to uuid
    def electionSort(self):
        with self.lock:
            self.electionConfirmBuffer.add(self.me.getIdentifier())
            joiningServer = list(self.electionConfirmBuffer)
            joiningServer.sort(key=functools.cmp_to_key(LeaderCompare))
        return joiningServer

    def getMemberServersAndTheirClients(self):
        return self.memberServersAndTheirClients

    def printCurrentMember(self):
        with self.lock:
            print('Current members:')
            # print('getSC: ', self.getMemberServersAndTheirClients())
            # print('info', self.infoServer)
            for each in self.getMemberServersAndTheirClients().keys():
                # print('each: ', each)
                print(each + ' at ' + str(self.infoServer.get(each).getAddress()) + ':'
                      + str(self.infoServer.get(each).getPort()))
                if len(self.getMemberServersAndTheirClients().get(each)) != 0:
                    print(str(self.getMemberServersAndTheirClients().get(each)))

    # update the current servers
    def handlesUpdate(self, message):
        self.viewTransfer = True
        newView = message.getContent()
        self.updateInfoServer(message.getSender())
        # print('newView, Update:', newView.groupView)

        # don't reply if this view doesn't contain me!
        if self.me.getIdentifier() not in newView.groupView:
            return
        # if I am ready, I will send ACK message.
        reply = Message.Message(self.me, Participant.Participant(None, self.multicastGroup, self.multicastPort),
                                Message.MsgTyp.Ack_Join_Server)
        reply.setContent(message.getContent())
        self.send(reply)
        # print('joining1:', self.joiningMembers)

        # modify the joining list
        with self.lock:
            # print('in lock!')
            lastView = self.getNewestView()
            newComer = set(newView.groupView.keys())
            newComer.difference_update(set(lastView.groupView.keys()))
            for each in newComer:
                if each in self.joiningMembers:
                    self.joiningMembers.remove(each)
            # print('joining2', self.joiningMembers)

    def handleHeartBeat(self, message):
        if message.getTyp() != Message.MsgTyp.Heart_Beat:
            raise ValueError('This is not a Heart Beat Message!')
        currentTime = time.perf_counter_ns() / 1000000

        # if it is a new Server which means there is no its heart beat in the list,
        # add its name and current time
        if message.getSender().getIdentifier() not in self.lastHeartBeat:
            self.lastHeartBeat.append(message.getSender().getIdentifier())
            self.lastHeartBeat.append(currentTime)
            # if there exits the id, cover the old value with a new one
            print('last heart beat:', self.lastHeartBeat)
        else:
            if len(self.lastHeartBeat) > 1:
                index = 0
                for i in range(len(self.lastHeartBeat)):
                    if self.lastHeartBeat[i] == message.getSender().getIdentifier():
                        index = i
                        break
                self.lastHeartBeat[index] = message.getSender().getIdentifier()
                self.lastHeartBeat[index + 1] = currentTime
                # each server have two positions in the list. [id, current time]

    def handlesJoinRequest(self, message):
        if message.getTyp() != Message.MsgTyp.Request_Join_Server:
            raise ValueError('This is not a joining request')
        self.viewTransfer = True

        newComer = message.getSender()
        # update the Server info
        self.updateInfoServer(newComer)
        reply = Message.Message(self.me, Participant.Participant(None, self.multicastGroup, self.multicastPort),
                                Message.MsgTyp.Update_Join_Server)

        # generate a new view due to the newcomer
        newView = self.getnewViewFromJoin(newComer)
        self.printView(newView)
        reply.setContent(newView)
        self.send(reply)

        with self.lock:
            for each in self.joiningMembers:
                if each == message.getSender().getIdentifier():
                    self.joiningMembers.remove(each)

    def handlesAck(self, ack):
        receivedView = ack.getContent()
        self.updateInfoServer(ack.getSender())

        # if I don't have a view -> I am the newComer
        # store the received new view and extract the numbers from it
        if self.view is None:
            # if true, reliable multicast is paused until false
            self.receivedFlush = True
            self.viewMessageBuffer.append(ack)
            # print('in none')
            # print('viewB: ', self.viewMessageBuffer)

            # compare the twp group view and add to the newView list
            if not self.viewContainsComparator(self.newViewsList, receivedView):
                self.newViewsList.append(receivedView)
        else:
            # I have already a view
            if receivedView.viewCount > self.view.viewCount:
                self.receivedFlush = True
                self.viewMessageBuffer.append(ack)
                # print('already in a view')
                # print('viewB: ', self.viewMessageBuffer)
                if not self.viewContainsComparator(self.newViewsList, receivedView):
                    self.newViewsList.append(receivedView)
            else:
                return
        # print('newList: ', self.newViewsList)

        # install the view, after that, the setup of the server group finishes
        self.installView()
        self.refreshVC()

        # if this list is not empty, which means the view maybe not the newest one, and installView is needed
        if len(self.newViewsList) == 0:
            # print('newlist empty!')
            self.viewTransfer = False
            self.handlesWaitingClients(self.connectingClients)

    def handlesLeave(self, message):
        msgTyp = message.getTyp()
        if msgTyp != Message.MsgTyp.Update_Leave_Server:
            raise ValueError('This is not a update leave server message')
        self.viewTransfer = False
        reply = Message.Message(self.me, Participant.Participant(None, self.multicastGroup, self.multicastPort),
                                Message.MsgTyp.Ack_Leave_Server)
        newView = message.getContent()
        reply.setContent(newView)
        self.send(reply)

    def handlesNoConnection(self, message):
        # now the group is changing, no reliable multicast until again be a closed group
        if self.reliableMC:
            self.reliableMC = False

        currentNewestView = self.getNewestView()
        # print('currentNewest: ', currentNewestView.groupView)
        # print('get00: ', self.getMemberServersAndTheirClients())

        failedID = str(message.getContent())
        failedServer = self.findParticipant(failedID)
        if failedID not in currentNewestView.groupView.keys():
            # print('return')
            return

        self.viewTransfer = True
        if message.getGroupView().viewCount < self.view.viewCount or failedID is None:
            # Happens if the second LOST_CONNECTION message is processed after the new view
            # is installed
            # print('mviewCount:', message.getGroupView().viewCount)
            # print('viewCount: ', self.view.viewCount)
            # print('failed: ', failedID)
            # print('failed', failedServer)
            pass
        else:
            self.leavingMembers.add(failedID)

            # generate a new container to store the current servers
            stillAlive = copy.deepcopy(self.getMemberServersAndTheirClients())
            # print('get000: ', self.getMemberServersAndTheirClients())
            for each in self.leavingMembers:
                if each in stillAlive:
                    stillAlive.pop(each)
            # print('get:', self.getMemberServersAndTheirClients())
            # print('leaving', self.leavingMembers)
            # print('still', stillAlive)

            # check if the failed one is the leader, if it is, take the first one in the list to be leader
            if failedID == self.leader:
                print('==================Leader is dead======================\n')
                self.leader = list(stillAlive.keys())[0]
                print(F'=========The new leader is: {str(self.leader)}=======\n')
                if self.leader == self.me.getIdentifier():
                    self.isLeader = True
                    print("===============I'm the new leader!================\n")

            # If I am the leader, update the new GV
            if self.isLeader:
                self.viewTransfer = True
                viewAfterLeave = GroupView.GroupView(stillAlive, self.viewCounter.addAndGet(1))
                if not self.viewContainsComparator(self.newViewsList, viewAfterLeave):
                    self.newViewsList.append(viewAfterLeave)

                msgLeave = self.constructMessage(Message.MsgTyp.Update_Leave_Server, viewAfterLeave)
                self.send(msgLeave)
                # print('informstillAlive: ', stillAlive)

                self.informsRelatedClients(stillAlive)

                if len(self.joiningMembers) != 0:
                    for each in self.joiningMembers:
                        stillAlive.update({each: set()})

                    msgJoin = Message.Message(self.me,
                                              Participant.Participant(None, self.multicastGroup, self.multicastPort),
                                              Message.MsgTyp.Update_Join_Server)
                    viewAfterJoin = GroupView.GroupView(stillAlive, self.viewCounter.addAndGet(1))
                    msgJoin.setContent(viewAfterJoin)
                    self.send(msgJoin)

    # compare if the view is in the newViewList
    def viewContainsComparator(self, newViewsList, receivedView):
        if len(newViewsList) == 0:
            return False
        for each in newViewsList:
            if each.getID() != receivedView.getID() and each != newViewsList[-1]:
                continue
            elif each.getID() != receivedView.getID() and each == newViewsList[-1]:
                return False
                # newViewsList.append(receivedView)
            else:
                return True

    # A method to generate the newest view, considering the currently installed view
    # and all pending ones in the newViewList
    def getNewestView(self):
        if len(self.newViewsList) == 0:
            # either not received a view or all views are already installed
            if self.view is not None:
                newestView = self.view
                # print('not none')
            else:
                newestView = GroupView.GroupView(dict({self.me.getIdentifier(): set()}), 0)
                # print('newest:', newestView.groupView)
        else:
            # there are views to be installed. Generate a new view from the newest one(the last in the list)
            newestView = self.newViewsList[len(self.newViewsList) - 1]
        return newestView

    # generate a new View after joining
    def getnewViewFromJoin(self, newComer):
        currentNewestView = self.getLastInstalledView()
        dictNew = currentNewestView.groupView
        dictNew.update({newComer.getIdentifier(): set()})
        counter = self.viewCounter.addAndGet(1)
        newView = GroupView.GroupView(dictNew, counter)
        return newView

    # A method to get the currently installed view
    def getLastInstalledView(self):
        return self.view

    def printView(self, view):
        for each in set(view.groupView.keys()):
            print(each + ' at ' + str(self.infoServer.get(each).getAddress())
                  + ':' + str(self.infoServer.get(each).getPort()))
            if len(view.groupView.get(each)) != 0:
                print(view.groupView.get(each))
        print('View number:', view.viewCount)

    # update the Info due to the newcomer
    def updateInfoServer(self, newComer):
        newComerUUID = newComer.getIdentifier()
        self.infoServer.update({newComerUUID: newComer})
        print('update: ', self.infoServer)

    # check and remove the server that is failed to join in or no connection anymore
    def refreshInfoServer(self):
        infoServerKey = set(self.infoServer.keys())
        # print('before:', self.infoServer)
        for each in infoServerKey:
            if each not in self.view.groupView.keys():
                self.infoServer.pop(each)
        # print('gv: ', self.view.groupView.keys())
        # print('info:', self.infoServer)

    # before group view installed, the client has to wait for the assigned server
    def handlesWaitingClients(self, waitingClients):
        print('waitingClients are: ', waitingClients)
        if len(waitingClients) != 0:
            for each in waitingClients:
                serverTarget = self.getNewServer(self.getMemberServersAndTheirClients().keys())

                # inform client to connect to which server
                reply = Message.Message(self.me,
                                        Participant.Participant(each, None, 0),
                                        Message.MsgTyp.Reply_Connect_Client)
                reply.setContent(serverTarget)
                self.send(reply)
                serverClientPair = dict({each: serverTarget})

                # inform Servers update the gV
                inform = self.constructMessage(Message.MsgTyp.Update_Connect_Client, serverClientPair)
                self.send(inform)

    def installView(self):
        with self.lock:
            self.newViewsList.sort(key=functools.cmp_to_key(ViewCompare))
        viewToInstall = self.newViewsList[0]
        if viewToInstall is not None:
            # print('install in if')
            requiredParticipants = set(viewToInstall.groupView.keys())
            # print('participants1:', requiredParticipants)
            for each in self.newViewsList:
                requiredParticipants.intersection_update(set(each.groupView.keys()))
            # print('participants2:', requiredParticipants)
            for each in self.viewMessageBuffer:
                # collect the processes that have sent the ACK
                viewInBuffer = each.getContent()
                if viewInBuffer.viewCount == viewToInstall.viewCount:
                    self.acknowledgedParticipants.add(each.getSender().getIdentifier())
                    self.ackMessagesToRemove.append(each)
            # print('ackparticipant: ', self.acknowledgedParticipants)

            # if all required processes have sent the ACK message, it is time to install the new view
            if requiredParticipants == self.acknowledgedParticipants:
                # find out the changed member
                diff = self.getChangedMembers(viewToInstall, self.view)

                self.view = viewToInstall
                # print('view: ', self.view.groupView)
                if not self.isLeader:
                    # Leader may have bigger count than the installed one
                    # Follower always have the same count as the installed one
                    self.viewCounter.set(self.view.viewCount)

                self.memberServersAndTheirClients = self.view.groupView
                self.leader = list(self.getMemberServersAndTheirClients().keys())[0]
                print('leader: ', self.leader)
                # clean up the buffer
                self.acknowledgedParticipants.clear()
                requiredParticipants.clear()
                # print('new:', self.newViewsList)
                if len(self.newViewsList) > 0:
                    del self.newViewsList[0]
                print(self.newViewsList)

                for each in self.viewMessageBuffer:
                    if each in self.ackMessagesToRemove:
                        self.viewMessageBuffer.remove(each)
                self.ackMessagesToRemove.clear()

                if diff is not None:
                    for each in diff:
                        if each in self.joiningMembers:
                            self.joiningMembers.remove(each)
                        if each in self.leavingMembers:
                            self.leavingMembers.remove(each)

                self.refreshInfoServer()

                # print('Remaining: ', len(self.newViewsList))
                print('===================New View Installed=======================')
                if len(self.newViewsList) != 0:
                    self.installView()

    # A method compares two input objects and output a list containing
    # the difference between them.
    def getChangedMembers(self, v1, v2):
        if v1 is None and v2 is None:
            return None
        if v1 is not None and v2 is None:
            return list(v1.groupView.keys())

        # if above two reach, v2 is not none
        if v1 is None:
            return list(v2.groupView.keys())

        if len(v1.groupView.keys()) > len(v2.groupView.keys()):
            larger = list(v1.groupView.keys())
            smaller = list(v2.groupView.keys())
            for each in larger:
                if each in smaller:
                    larger.remove(each)
            return larger

        elif len(v1.groupView.keys()) < len(v2.groupView.keys()):
            larger = list(v2.groupView.keys())
            smaller = list(v1.groupView.keys())
            for each in larger:
                if each in smaller:
                    larger.remove(each)
            return larger
        else:
            return None

    # For the identification, all servers are represented by uuid.
    # A method to get the real participant
    def findParticipant(self, serverID):
        with self.lock:
            # print('info: ', self.infoServer)
            server = self.infoServer.get(serverID)
        return server

    def findLeavingParticipant(self, string):
        return string

    # if a server dead, tell the clients that are connecting to it.
    # tell the client new server, and tell server the new pair
    def informsRelatedClients(self, stillAlive):
        # get the involved clients
        involvedClients = []
        # print('leavingMem: ', self.leavingMembers)
        # print('get0: ', self.getMemberServersAndTheirClients())

        for each in self.leavingMembers:
            # print('get: ', self.getMemberServersAndTheirClients().get(each))
            if self.getMemberServersAndTheirClients().get(each) is not None:
                involvedClients.extend(list(self.getMemberServersAndTheirClients().get(each)))
            else:
                print('No Client serverd by the dead server, no worry!')
                return
        if len(involvedClients) != 0:
            for each in involvedClients:
                # involvedClients contains Strings
                existingServer = set(stillAlive)
                targetServerID = self.getNewServer(existingServer)
                targetServer = self.findParticipant(targetServerID)

                # Tell the clients the new connection info
                toClients = Message.Message(self.me, Participant.Participant(each, None, 0),
                                            Message.MsgTyp.Client_Switch_Server)
                toClients.setContent(targetServer)

                # tell server update the gV
                serverClientPair = {each: targetServer}
                toServer = Message.Message(self.me,
                                           Participant.Participant(None, self.multicastGroup, self.multicastPort),
                                           Message.MsgTyp.Update_Connect_Client)
                toServer.setContent(serverClientPair)

                self.send(toClients)
                self.send(toServer)

    # randomly assign a server in the group
    def getNewServer(self, existingServer):
        size = len(existingServer)
        assert size != 0
        n = random.randint(0, 100)
        existingServer = list(existingServer)
        return existingServer[n % size]

    def handlesClientsInfo(self, message):
        msgTyp = message.getTyp()
        # print('in handleClientsInfo')
        if msgTyp == Message.MsgTyp.Request_Join_Client:
            self.connectingClients.add(message.getSender().getIdentifier())
            if self.viewTransfer:
                return

            if self.isLeader:
                serverTargetID = self.getNewServer(self.getMemberServersAndTheirClients().keys())
                serverTarget = self.findParticipant(serverTargetID)

                # only the request client can process this message
                reply = Message.Message(self.me, message.getSender(), Message.MsgTyp.Reply_Connect_Client)
                reply.setContent(serverTarget)
                self.send(reply)

                # tell others to update the group view
                serverClientPair = dict({message.getSender().getIdentifier(): serverTarget})
                inform = Message.Message(self.me,
                                         Participant.Participant(None, self.multicastGroup, self.multicastPort),
                                         Message.MsgTyp.Update_Connect_Client)
                inform.setContent(serverClientPair)
                self.send(inform)

        if msgTyp == Message.MsgTyp.Update_Connect_Client:
            serverClientPair = message.getContent()
            self.clientServerMapping.update(serverClientPair)
            # print('clientServerMapping: ', self.clientServerMapping)

            # add the new coming Client at the right position in the gV
        if msgTyp == Message.MsgTyp.Confirm_Connect_Client:
            server = message.getSender()
            confirmedClient = str(message.getContent())
            self.getMemberServersAndTheirClients().get(server.getIdentifier()).add(confirmedClient)
            self.clientServerMapping.pop(confirmedClient)
            # print('connC: ', self.connectingClients)
            # print('co', confirmedClient)nf:
            if confirmedClient in self.connectingClients:
                self.connectingClients.remove(confirmedClient)

        if msgTyp == Message.MsgTyp.Confirm_Disconnect_Client:
            leavingClient = message.getContent()
            # print('getDisContent:', leavingClient)
            server = message.getSender()
            self.removeSocket(self.getMemberServersAndTheirClients().get(server.getIdentifier()), leavingClient)
            self.getMemberServersAndTheirClients().get(server.getIdentifier()).remove(leavingClient)
            # print('getMSC: ', self.getMemberServersAndTheirClients())

        self.printCurrentMember()

    # if client leaves, close and remove the socket from the dict
    def removeSocket(self, clientSet, leavingClient):
        # print('clientManager1: ', self.clientManager.socketDict)
        # print('getMSCServer: ', clientSet)
        for each in clientSet:
            if each == leavingClient:
                for sock in self.getClientManager().socketDict.keys():
                    if leavingClient == self.getClientManager().socketDict.get(sock):
                        self.getClientManager().socketDict.update({sock: 'Already Closed!'})
                        sock.close()
                        # print('clientManager2: ', self.clientManager.socketDict)

    def getReplicationManager(self):
        return self.replicationManager

    def refreshVC(self):
        servers = list(self.getMemberServersAndTheirClients().keys())
        # print('getMM: ', self.getMemberServersAndTheirClients())
        self.groupVC = VectorClock.VectorClock(dict())
        for each in servers:
            self.groupVC.addProcess(each)
        print('current groupVC: ', self.groupVC.vectorTimestamp)
        self.reliableMC = True

    def getGroupVc(self):
        return self.groupVC

def LeaderCompare(o1, o2):
    if o1 < o2:
        return 1
    elif o1 > o2:
        return -1
    else:
        return 0


def ViewCompare(o1, o2):
    if o1.viewCount > o2.viewCount:
        return 1
    elif o1.viewCount < o2.viewCount:
        return -1
    else:
        return 0
