class DAOFactory(object):

    def __init__(self):
        super().__init__()

    def epoch_dao(self, conn):
        raise Exception('Not implemented!')

    def file_dao(self, conn):
        raise Exception('Not implemented!')