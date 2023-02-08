import threading


# a threading safe counter for the groupView number
class SafeCounter:
    def __init__(self, n):
        self.i = n
        self.lock = threading.Lock()

    def increment(self, n):
        with self.lock:
            self.i = self.i + n

    def addAndGet(self, n):
        with self.lock:
            self.i = self.i + n
        return self.i

    def set(self, n):
        with self.lock:
            self.i = n
