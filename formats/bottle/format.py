''' libcchdo.bottle.format '''

class format:

    def __init__(self, datafile):
        self.datafile = datafile

    def read(self, handle):
        raise NotImplementedError

    def write(self, handle):
        raise NotImplementedError
