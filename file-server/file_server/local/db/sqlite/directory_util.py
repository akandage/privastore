from ....error import DirectoryError
from ...file_transfer_status import FileTransferStatus

def query_directory_id(cur, parent_directory_id, directory_name):
    cur.execute('''
        SELECT L.child_id FROM ps_directory AS D INNER JOIN ps_link AS L ON L.child_id = D.id 
        WHERE L.parent_id = ? AND D.name = ?
    ''', (parent_directory_id, directory_name))
    res = cur.fetchone()
    if res is None:
        return None
    return res[0]

def query_file_id(cur, parent_directory_id, file_name):
    cur.execute('''
        SELECT F.id FROM ps_file AS F INNER JOIN ps_file_version AS V ON F.id = V.file_id 
        WHERE F.parent_id = ? AND F.name = ? AND V.transfer_status <> ?
    ''', (parent_directory_id, file_name, FileTransferStatus.RECEIVING_FAILED.value))
    res = cur.fetchone()
    if res is None:
        return None
    return res[0]

def traverse_path(cur, path):
    # Start from root directory and iterate to the last directory in the path.
    #
    directory_id = 1
    for directory_name in path:
        directory_id = query_directory_id(cur, directory_id, directory_name)
        if directory_id is None:
            raise DirectoryError('Invalid path to directory [{}]'.format('/' + '/'.join(path)))
    return directory_id