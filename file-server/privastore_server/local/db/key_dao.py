from ...db.dao import DataAccessObject
from ...key import Key

class KeyDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)
    
    def get_key(self, key_id: str) -> Key:
        raise Exception('Not implemented!')
    
    def get_system_key(self) -> Key:
        return self.get_key('system')
    