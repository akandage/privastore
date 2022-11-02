'''
    File server controller.

    db_conn_mgr - database connection manager
    session_mgr - session store
    store - file store
'''
class Controller(object):

    def __init__(self, db_conn_mgr, session_mgr, store):
        self._db_conn_mgr = db_conn_mgr
        self._session_mgr = session_mgr
        self._store = store
    
    def db_conn_mgr(self):
        return self._db_conn_mgr
    
    def session_mgr(self):
        return self._session_mgr

    def store(self):
        return self._store

    def login_user(self, username, password):
        raise Exception('Not implemented!')
    
    def heartbeat_session(self, session_id):
        self._session_mgr.renew_session(session_id)
    
    def logout_user(self, session_id):
        self._session_mgr.end_session(session_id)
