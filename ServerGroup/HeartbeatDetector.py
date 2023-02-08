import threading
from basics import Message
import time
import socket


class HeartbeatDetector(threading.Thread):
    def __init__(self, me, groupManager, name):
        threading.Thread.__init__(self)
        self.currentTime = None
        self.me = me
        self.groupManager = groupManager
        self.setName(name)
        self.startTime = time.perf_counter_ns() / 1000000
        # frequency = 1000 milliseconds
        self.frqcy = 1000

    def run(self):
        while True:
            # send the heartBeat at a frequency
            self.currentTime = time.perf_counter_ns() / 1000000
            if self.currentTime - self.frqcy > self.startTime:
                self.startTime = self.currentTime
                heartBeat = Message.Message(self.me, self.me, Message.MsgTyp.Heart_Beat)
                self.groupManager.send(heartBeat)

            toRemoveParticipant = []

            i = 0
            while i < len(self.groupManager.getHeartBeatList()) - 1:
                self.currentTime = time.perf_counter_ns() / 1000000

                # if the heartBeat will not be heard by others in a while, it sends no connection
                if (self.currentTime - self.groupManager.getHeartBeatList()[i + 1] > 5 * self.frqcy) \
                        and (self.groupManager.getHeartBeatList()[i] != self.me.getIdentifier()):
                    # print('ctime:', i, self.currentTime)
                    # print('GHB: ', i, self.groupManager.getHeartBeatList()[i + 1])
                    noConnection = Message.Message(self.me, self.me, Message.MsgTyp.No_Connection)

                    # now the group is changing, no reliable multicast until again be a closed group
                    if self.groupManager.reliableMC:
                        self.groupManager.reliableMC = False

                    noConnection.setGroupView(self.groupManager.view)
                    noConnection.setContent(self.groupManager.getHeartBeatList()[i])
                    toRemoveParticipant.append(i)
                    # print('no connection sent')

                    self.groupManager.send(noConnection)
                    time.sleep(0.1)

                i += 2

            for each in toRemoveParticipant:
                self.groupManager.getHeartBeatList().pop(each)
                self.groupManager.getHeartBeatList().pop(each)
