''' libcchdo.common.format '''

class format:

    def __init__(self, datafile):
        self.datafile = datafile

    def read(self):
        raise NotImplementedError

    def write(self):
        raise NotImplementedError
