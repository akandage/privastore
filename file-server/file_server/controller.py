'''
    File server controller.

    session_mgr - session store
'''
class Controller(object):

    def __init__(self, session_mgr):
        self._session_mgr = session_mgr
    
    def login_user(self, username, password):
        raise Exception('Not implemented!')
    
    def heartbeat_session(self, session_id):
        self._session_mgr.renew_session(session_id)
    
    def logout_user(self, session_id):
        self._session_mgr.end_session(session_id)
