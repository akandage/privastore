import os
import requests
from .server import Server
import shutil
import time
import unittest
from .util.logging import config_logging

HOSTNAME = 'localhost'
PORT = 8090
URL = 'http://{}:{}{{}}'.format(HOSTNAME, PORT)

class TestServer(unittest.TestCase):
    
    def get_config(self) -> dict:
        raise Exception('Not implemented!')

    def get_test_dir(self) -> str:
        raise Exception('Not implemented!')

    def server_factory(self) -> Server:
        raise Exception('Not implemented!')

    def cleanup(self):
        try:
            shutil.rmtree(self.get_test_dir())
        except:
            pass
    
    def setUp(self):
        self.cleanup()
        os.mkdir(self.get_test_dir())
        self.config = None
        config = self.get_config()
        config_logging(config['logging']['log-level'])

    def tearDown(self):
        self.server.stop()
        self.server.join()
        self.cleanup()

    def start_server(self):
        self.server = self.server_factory()
        self.server.setup_db()
        self.server.start()
        self.server.wait_started()

    def stop_server(self):
        if self.server:
            self.server.stop()
            self.server.join()
            self.server = None

    def restart_server(self):
        self.stop_server()
        self.server = self.server_factory()
        self.server.start()
        self.server.wait_started()

    def send_request(self, url, headers={}, method=requests.get, data=None):
        r = method(url, headers=headers, data=data)
        if r.status_code == 200:
            content_len = r.headers.get('Content-Length')
            if content_len is not None:
                content_len = int(content_len)
                if content_len > 0:
                    content_type = r.headers.get('Content-Type')
                    if content_type is not None and content_type.startswith('application/json'):
                        return r.json()
        return r
    
    def wait_for(self, cb, args=list(), kwargs=dict(), timeout=90, interval=0.1):
        start_t = now = time.time()
        end_t = start_t + timeout

        while now < end_t:
            kwargs['timeout'] = max(0, end_t - now)

            if cb(*args, **kwargs):
                return True

            time.sleep(interval)
            now = time.time()

        return False