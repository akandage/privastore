import os
import requests
import shutil
import unittest
from .util.logging import config_logging

HOSTNAME = 'localhost'
PORT = 8090
URL = 'http://{}:{}{{}}'.format(HOSTNAME, PORT)

class TestServer(unittest.TestCase):
    
    def get_config(self):
        raise Exception('Not implemented!')

    def get_test_dir(self):
        raise Exception('Not implemented!')

    def server_factory(self):
        raise Exception('Not implemented!')

    def cleanup(self):
        try:
            shutil.rmtree(self.get_test_dir())
        except:
            pass
    
    def setUp(self):
        self.cleanup()
        os.mkdir(self.get_test_dir())
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

    def restart_server(self):
        self.server.stop()
        self.server.join()
        self.server = self.server_factory()
        self.server.start()
        self.server.wait_started()

    def send_request(self, url, headers={}, method=requests.get, data=None):
        r = method(url, headers=headers, data=data)
        content_len = r.headers.get('Content-Length')
        if content_len is not None:
            content_len = int(content_len)
            if content_len > 0:
                content_type = r.headers.get('Content-Type')
                if content_type is not None and content_type.startswith('application/json'):
                    return r.json()
        return r