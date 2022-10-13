class DAOFactory(object):

    def __init__(self):
        super().__init__()

    def user_dao(self, conn):
        raise Exception('Not implemented!')
    
    def file_dao(self, conn):
        raise Exception('Not implemented!')
    
    def directory_dao(self, conn):
        raise Exception('Not implemented!')