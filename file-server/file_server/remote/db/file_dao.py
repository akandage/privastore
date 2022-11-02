from ...db.dao import DataAccessObject

class FileDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)
    
    def create_file(self, remote_id, file_size):
        raise Exception('Not implemented!')
    
    def get_file_metadata(self, remote_id):
        raise Exception('Not implemented!')

    def file_modified(self, remote_id):
        raise Exception('Not implemented!')

    def commit_file(self, remote_id):
        raise Exception('Not implemented!')

    def remove_file(self, remote_id):
        raise Exception('Not implemented!')