from ..error import NotImplementedError

class FileHandle(object):

    def __init__(self):
        pass

    def uid(self) -> str:
        raise NotImplementedError()

    def read_chunk(self) -> bytes:
        raise NotImplementedError()

    def append_chunk(self, data: bytes) -> int:
        raise NotImplementedError()

    def close() -> None:
        raise NotImplementedError()