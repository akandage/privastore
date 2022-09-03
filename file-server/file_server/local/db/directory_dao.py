from .dao import DataAccessObject

class DirectoryDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)
    
    '''
        Create a directory.

        path - Path to the new directory - array of directory names.
        directory_name - Name of the new directory.
        is_hidden - Hidden directory flag.
    '''
    def create_directory(self, path, directory_name, is_hidden=False):
        raise Exception('Not implemented!')