import uuid


class GroupView:
    def __init__(self, groupView, viewCount):
        self.groupView = groupView
        self.viewCount = viewCount
        self.gVID = str(uuid.uuid1())

    def __str__(self):
        return 'View No.' + str(self.viewCount)

    def getID(self):
        return self.gVID


# test
if __name__ == '__main__':
    c = GroupView({'YiQianRong': {'DS', 'DO', 'AM'}, 'ShaoQixiang': {'DS', 'DO'},
                   'Fangziming': {'DS', 'DO', 'AT2'}}, 100)

    print(c.__str__())

