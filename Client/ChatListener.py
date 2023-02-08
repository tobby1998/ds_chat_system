class Interface(object):
    def onMessage(self, fromLogin, msgContent):
        pass


class ChatListener(Interface):
    def onMessage(self, fromLogin, msgContent):
        print(fromLogin + ': ' + msgContent)
