from ..controller import Controller

class RemoteServerController(Controller):

    def __init__(self, auth_config, dao_factory, db_conn_mgr, session_mgr, store):
        super().__init__(db_conn_mgr, session_mgr, store)
        self._auth_config = auth_config
        self._dao_factory = dao_factory