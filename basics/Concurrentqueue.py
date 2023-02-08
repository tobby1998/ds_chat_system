import threading


class Concurrentqueue:
    def __init__(self):
        self.queue = []
        self.lock = threading.Lock()
        self.condition = threading.Condition()

    def put(self, obj):
        with self.lock:
            # print('write in: %s' % obj)
            self.queue.append(obj)
            # print('the current queue:', self.queue)

        self.condition.acquire()
        self.condition.notify()
        self.condition.release()

    def get(self, index):
        with self.lock:
            item = self.queue[index]
        return item

    def size(self):
        with self.lock:
            size = len(self.queue)
        return size

    def pop(self, block=True, timeout=None):
        if self.size() == 0:
            if block:
                self.condition.acquire()
                self.condition.wait(timeout=timeout)
                self.condition.release()
            else:
                return None

        with self.lock:
            item = None
            if len(self.queue) > 0:
                item = self.queue.pop(0)
        # print('====>' * 10)
        # print('get item: %s' % item)
        # print('====>' * 10)
        return item


# test
if __name__ == '__main__':
    c = Concurrentqueue()
    a = dict({'YiQianrong': 1, 'QixiangShao': 1})
    c.put(a)
    c.put(2)
    c.put(3)
    print(c.get())
    print(c.size())
    print(c.get())
    print(c.size())
    print(c.get())
    print(c.get())
