class SocketWrapper(object):

    def __init__(self, sock):
        self._sock = sock
        self._bytes_read = 0
        self._error = False
    
    def read(self, size):
        if self._error:
            raise Exception('Socket broken')
        try:
            data = self._sock.read(size)
        except Exception as e:
            self._error = True
            raise e
        self._bytes_read += len(data)
        return data
    
    def write(self, data):
        if self._error:
            raise Exception('Socket broken')
        try:
            return self._sock.write(data)
        except Exception as e:
            self._error = True
            raise e

    def flush(self):
        return self._sock.flush()

    def close(self):
        return self._sock.close()

    def bytes_read(self):
        return self._bytes_read