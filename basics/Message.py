from enum import Enum
import time
import uuid


# Message inside the serverGroup
class Message:
    def __init__(self, sender, receiver, msgTyp):
        self.vectorClock = None
        self.groupView = None
        self.content = None
        self.sender = sender
        self.receiver = receiver
        self.type = msgTyp
        self.msgID = uuid.uuid1()

    def getSender(self):
        return self.sender

    def getReceiver(self):
        return self.receiver

    def getTyp(self):
        return self.type

    def setGroupView(self, groupView):
        self.groupView = groupView

    def getGroupView(self):
        return self.groupView

    def setContent(self, content):
        self.content = content

    def getContent(self):
        return self.content

    def setVectorClock(self, vectorClock):
        self.vectorClock = vectorClock

    def getVectorClock(self):
        return self.vectorClock

    def getUuid(self):
        return self.msgID


# the message types inside the server group
class MsgTyp(Enum):
    # Joining Request
    Request_Join_Server = 1
    Request_Join_Client = 2
    # View change initiated by leader due to leaving or joining
    Update_Leave_Server = 3
    Update_Join_Server = 4
    # Election State
    Election_Start = 5
    Election_Confirm = 6
    Election_Prepare = 7
    # ack
    Ack_Join_Server = 8       # Acknowledgement to all server when updating group view due to new server
    Ack_Leave_Server = 9      # Acknowledgement to all server when updating group view due to failing/leaving
    # HeartbeatDetector
    Heart_Beat = 10
    No_Connection = 11

    # Client message
    Reply_Connect_Client = 12
    Update_Connect_Client = 13
    Client_Switch_Server = 14
    Confirm_Connect_Client = 15
    Confirm_Disconnect_Client = 16

    # ReplicationManager
    Update_Replication_Storage = 17
    Initiate_Message_Delivery = 18
    Clear_Cross_Server_Map = 19


# messages (un)packer for chatMessages
# message format: subtype, receiver, type, content, uuid
class ClientMessageCoder:
    def __init__(self, subtype=0, target_id=0, typ=0, content="", uuid=""):
        self.s_id = subtype
        self.t_id = target_id
        self.type = typ
        self.con = content
        self.vc = 0
        self.messageID = uuid

    # pack the msg, return a string
    def msgPack(self, s_id, t_id, typ, con):
        current_time = time.strftime("%Y-%m-%d-%H-%M-%S")
        uuid = current_time + '-' + str(s_id)
        stren = "Ms$#{s_id}*$st$#{t_id}*$ttyp$#{typ}*$typcon$#{con}*$conuuid$#{uuid}*$uuidME". \
            format(s_id=str(s_id), t_id=str(t_id), typ=str(typ), con=str(con), uuid=str(uuid))
        return stren

    # unpack a string, return a dict
    def msgUnpacking(self, strin):
        if strin is None:
            return None
        if "$#" not in strin:
            return strin
        msg_dic = dict()
        msg_dic['subtype'] = strin.split("s$#")[1].split("*$s")[0]
        msg_dic['target_id'] = strin.split("t$#")[1].split("*$t")[0]
        msg_dic['type'] = str(strin.split("typ$#")[1].split("*$typ")[0])
        msg_dic['content'] = strin.split("con$#")[1].split("*$con")[0]
        msg_dic['uuid'] = strin.split("uuid$#")[1].split("*$uuid")[0]
        return msg_dic


# test
if __name__ == '__main__':
    m = Message(1, 1, MsgTyp.Heart_Beat)
    print(m.getTyp())
