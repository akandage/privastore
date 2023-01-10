from ...db.dao import DataAccessObject
from ...remote_client import RemoteEndpoint, RemoteCredentials

class RemoteDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)
    
    def get_remote_credentials(self, cluster_name: str) -> RemoteCredentials:
        raise Exception('Not implemented!')

    def get_remote_servers(self, cluster_name: str) -> list[RemoteEndpoint]:
        raise Exception('Not implemented!')