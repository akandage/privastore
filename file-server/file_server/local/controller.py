import logging

class Controller(object):

    '''
        Local file server controller.

        conn_pool - database connection pool
        sessions - session store
    '''
    def __init__(self, conn_pool, sessions, dao_factory):
        super().__init__()
        self._conn_pool = conn_pool
        self._sessions = sessions
        self._dao_factory = dao_factory
    
    def login_user(self, username, password):
        logging.debug('User [{}] login attempt'.format(username))
        conn = self._conn_pool.acquire(timeout=30)
        logging.debug('Acquired database connection')
        if conn:
            user_dao = self._dao_factory.user_dao(conn)
            user_dao.login(username, password)
            logging.debug('User [{}] logged in successfully'.format(username))
            session_id = self._sessions.start_session(username)
            return session_id
        else:
            raise Exception('Database connection pool empty')