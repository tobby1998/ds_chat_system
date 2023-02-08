import time
import threading
import ClientConnectManager
from basics import Participant
from basics import Message
import pickle


class TimerTask(threading.Thread):
    def __init__(self, meID, period, mcSocket):
        threading.Thread.__init__(self)
        self.running = True
        self.period = period
        self.meID = meID
        self.mcSocket = mcSocket
        self.multicastPoint = ('224.1.1.1', 8888)

    def cancel(self):
        self.running = False

    def run(self):
        while self.running:
            self.connectToServer()
            time.sleep(self.period)

    def connectToServer(self):
        me = Participant.Participant(self.meID, None, 0)
        mC = Participant.Participant(None, '224.1.1.1', 8888)
        connectMsg = Message.Message(me, mC, Message.MsgTyp.Request_Join_Client)
        connectMsg = pickle.dumps(connectMsg)
        self.mcSocket.sendto(connectMsg, self.multicastPoint)








