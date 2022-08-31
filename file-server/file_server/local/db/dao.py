class DataAccessObject(object):

    def __init__(self, conn):
        super().__init__()
        self._conn = conn