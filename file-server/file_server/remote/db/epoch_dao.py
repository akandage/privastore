from ...db.dao import DataAccessObject

class EpochDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)
    
    def create_epoch(self, epoch_no, marker_id):
        raise Exception('Not implemented!')