from .dao import DataAccessObject

class DirectoryDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)
    
    '''
        Create a directory.

        path - Path to the new directory - array of directory names.
        directory_name - Name of the new directory.
        is_hidden - Hidden directory flag.

        Throws DirectoryError is path doesn't exist or if directory already
        exists.
        Throws FileError if a file with that name exists in the path.
    '''
    def create_directory(self, path, directory_name, is_hidden=False):
        raise Exception('Not implemented!')
    
    '''
        Create a file.

        path - Path to the new file - array of directory names.
        directory_name - Name of the new file.
        is_hidden - Hidden directory flag.

        Throws DirectoryError is path doesn't exist of if directory with
        that name exists in the path. 
        Throws FileError if file with that name exists in the path.
    '''
    def create_file(self, path, file_name, is_hidden=False):
        raise Exception('Not implemented')
    
    '''
        Remove a file.

        path - Path to file.
        directory_name - Name of the file.
        is_hidden - Hidden directory flag.
        delete - If true actually delete the file from db, otherwise set the
                 "removed" flag.

        Throws DirectoryError if path doesn't exist or if file is a directory.
        Throws FileError if file is not found or file is hidden and is_hidden
        flag is not set.
    '''
    def remove_file(self, path, file_name, delete=False, is_hidden=False):
        raise Exception('Not implemented')

    '''
        List directory entries.

        path - Path to the directory.
        show_hidden - Show hidden directories.
    
        Returns a list of tuples indicating the entry type ('d' for directory
         or 'f' for file) and the entry name.
        Throws DirectoryError is path doesn't exist.
    '''
    def list_directory(self, path, show_hidden=False):
        raise Exception('Not implemented!')