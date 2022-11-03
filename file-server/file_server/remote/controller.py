from ..controller import Controller

class RemoteServerController(Controller):

    def __init__(self, dao_factory, db_conn_mgr, session_mgr, store):
        super().__init__(db_conn_mgr, session_mgr, store)
        self._dao_factory = dao_factory