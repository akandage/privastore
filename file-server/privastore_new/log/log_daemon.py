import logging
from threading import RLock
import time

from ..daemon import Daemon
from ..db.conn_pool import DbConnectionPool
from ..db.log_dao import LogDAOFactory
from .log_observer import LogObserver

class LogDaemon(Daemon):

    def __init__(self, store_config, conn_pool: DbConnectionPool, dao_factory: LogDAOFactory):
        super().__init__('log-daemon')
        self._conn_pool = conn_pool
        self._dao_factory = dao_factory
        self._observers = list[LogObserver]()
        self._lock = RLock()
        self._poll_interval = float(store_config.get('log-poll-interval', 0.1))

    def conn_pool(self) -> DbConnectionPool:
        return self._conn_pool
    
    def dao_factory(self) -> LogDAOFactory:
        return self._dao_factory

    def add_observer(self, obs: LogObserver):
        with self._lock:
            self._observers.append(obs)
    
    def run(self):
        self._started.set()
        logging.debug('Log daemon started')
        now = last_poll_t = round(time.time())
        curr_seq_no = 0
        while not self._stop.wait(self._poll_interval):
            now = round(time.time())
            if now >= last_poll_t + self._poll_interval:
                conn = self.conn_pool().acquire()
                try:
                    try:
                        log_dao = self.dao_factory().log_dao(conn)
                        log_entries = log_dao.get_log_entries(curr_seq_no+1)
                        with self._lock:
                            for log_entry in log_entries:
                                for obs in self._observers:
                                    obs.on_log_entry(log_entry)
                                curr_seq_no = max(curr_seq_no, log_entry.seq_no())
                                logging.debug('{}'.format(log_entry.to_dict()))
                        logging.debug('Last sequence number processed: {}'.format(curr_seq_no))
                    except Exception as e:
                        logging.error('Error polling log: {}'.format(str(e)))
                finally:
                    self.conn_pool().release(conn)
                last_poll_t = now
        self._stopped.set()
        logging.debug('Log daemon stopped')