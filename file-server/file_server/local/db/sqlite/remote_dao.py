import logging
from ....error import RemoteServerError, FileServerErrorCode
from ..remote_dao import RemoteDAO, RemoteCredentials, RemoteEndpoint

class SqliteRemoteDAO(RemoteDAO):

    def __init__(self, conn):
        super().__init__(conn)

    def get_remote_credentials(self, cluster_name):
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('''
                    SELECT username, password
                    FROM ps_remote_cluster
                    WHERE name = ?
                ''', (cluster_name,))
                res = cur.fetchone()
                if res is None:
                    raise RemoteServerError('Cluster [{}] not found!'.format(cluster_name))
                # TODO: Don't use plaintext password.
                username, password = res
                self._conn.commit()
                return RemoteCredentials(username, password)
            except Exception as e:
                logging.error('Query error {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
        finally:
            try:
                cur.close()
            except:
                pass

    def get_remote_servers(self, cluster_name):
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('''
                    SELECT S.hostname, S.port, S.use_ssl
                    FROM ps_remote_server AS S INNER JOIN ps_remote_cluster AS C ON S.cluster_id = C.id
                    WHERE C.name = ?
                ''', (cluster_name,))
                remote_servers = list()
                for hostname, port, use_ssl in cur.fetchall():
                    remote_servers.append(RemoteEndpoint(hostname, port, bool(use_ssl)))
                self._conn.commit()
                return remote_servers
            except Exception as e:
                logging.error('Query error {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
        finally:
            try:
                cur.close()
            except:
                pass