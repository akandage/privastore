from .dao import DataAccessObject

class FileDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)