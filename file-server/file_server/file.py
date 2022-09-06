class File(object):

    def __init__(self, file_id=None, mode='r', encode_chunk=None, decode_chunk=None):
        self._file_id = file_id or self.generate_file_id()
        self._encode_chunk = encode_chunk
        self._decode_chunk = decode_chunk
    
    def generate_file_id(self):
        raise Exception('Not implemented!')
    
    def append_chunk(self, chunk_bytes):
        raise Exception('Not implemented')
    
    def read_chunk(self):
        raise Exception('Not implemented!')

    def close(self):
        raise Exception('Not implemented!')