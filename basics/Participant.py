class Participant:
    def __init__(self, identifier, address, port):
        self.identifier = identifier
        self.address = address
        self.port = port

    def getIdentifier(self):
        return self.identifier

    def getAddress(self):
        return self.address

    def getPort(self):
        return self.port


# test
if __name__ == '__main__':
    c = Participant('Qianrong', 12711, 100)
    a = c.getIdentifier()
    print(a)

# comparison function not complete
