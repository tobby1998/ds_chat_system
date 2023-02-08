class Interface(object):
    def online(self, login):
        pass

    def offline(self, login):
        pass


class UserStatusListener(Interface):
    def onlinePrint(self, loginID):
        print('Online: ' + loginID)

    def offlinePrint(self, loginID):
        print('Offline: ' + loginID)
