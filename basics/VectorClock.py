import threading
from readerwriterlock import rwlock

marker = rwlock.RWLockFair()


class VectorClock:
    def __init__(self, vectorTimestamp):
        self.vectorTimestamp = vectorTimestamp.copy()

    # different initialize functions
    @classmethod
    def fromString(cls, myUUID):
        cls.vectorTimestamp = dict()
        cls.vectorTimestamp.update({str(myUUID): 0})
        return cls(cls.vectorTimestamp)

    @classmethod
    def fromVectorClock(cls, vcToCopy):
        cls.vectorTimestamp = dict()
        for each in vcToCopy.vectorTimestamp.keys():
            cls.vectorTimestamp.update({each: vcToCopy.vectorTimestamp.get(each)})
        return cls(cls.vectorTimestamp)

    def addProcess(self, pID):
        writeMarker = marker.gen_wlock()
        writeMarker.acquire()
        self.vectorTimestamp.update({str(pID): 0})
        writeMarker.release()

    def getVectorTimestamp(self):
        readMarker = marker.gen_rlock()
        readMarker.acquire()
        try:
            return self.vectorTimestamp
        finally:
            readMarker.release()

    def put(self, uuid, num):
        writeMarker = marker.gen_wlock()
        writeMarker.acquire()
        self.vectorTimestamp.update({uuid: num})
        writeMarker.release()

    # remove a process from the group vector clock due to leaving or disconnecting
    # pID: the UUID of the process to be removed
    def removeProcess(self, pID):
        writeMarker = marker.gen_wlock()
        writeMarker.acquire()
        self.vectorTimestamp.pop(pID)
        writeMarker.release()

    # Compare two vector clocks to identify the happen-before relation.
    # raise ValueError if the size of the vector clocks don't match.
    # Output true if {@code self} happened before {@code vectorClock}
    def isHappenBefore(self, vectorClock):
        readMarker = marker.gen_rlock()
        readMarker.acquire()
        try:
            if len(self.vectorTimestamp) != len(vectorClock.getVectorTimestamp()):
                raise ValueError("The size of the vector clocks don't match")
            for each in vectorClock.vectorTimestamp.keys():
                if (self.vectorTimestamp.get(each) >=
                        vectorClock.getVectorTimestamp().get(each)):
                    return False
            return True
        finally:
            readMarker.release()

    # Increment time stamp of pi by 1 when pi sends a message to the Group G that
    # this vectorclock is attached to
    def meSendMessage(self, pi):
        writeMarker = marker.gen_wlock()
        writeMarker.acquire()
        currentStamp = self.vectorTimestamp.get(pi)
        currentStamp += 1
        self.vectorTimestamp.update({pi: currentStamp})
        writeMarker.release()

    # Update the Group vector clock after a message is added to the delivery queue
    # by causal ordering deliver
    # @param message the message to be delivered
    def causalOrderUpdate(self, message):
        writeMarker = marker.gen_wlock()
        writeMarker.acquire()
        sender = message.getSender().getIdentifier()
        current = self.vectorTimestamp.get(sender)
        current += 1
        self.vectorTimestamp.update({sender: current})
        print(F'=======================current{self.vectorTimestamp}===============================\n')
        writeMarker.release()

    def getDict(self):
        print(self.vectorTimestamp)


# test,
# causalorderUpdate still not
if __name__ == '__main__':
    c = dict({'YiQianRong': 111, 'ShaoQixiang': 112,
              'Fangziming': 113})
    vC = VectorClock(c)
    vC.addProcess('son of YQR')
    vC.put('son of FZM', 113)
    print(vC.getVectorTimestamp())
    vC1 = VectorClock(vC.getVectorTimestamp())
    vC.meSendMessage('son of FZM')
    print(vC.getVectorTimestamp())
