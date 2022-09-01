from http.server import BaseHTTPRequestHandler

class HttpRequestHandler(BaseHTTPRequestHandler):

    def __init__(self, controller):
        super().__init__()
        self._controller = controller