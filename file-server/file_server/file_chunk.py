from .error import FileChunkError
from .util.crypto import sha256
import os

def get_file_chunk_encoder(cipher_factory):
    def encode_chunk(chunk_bytes, f=None):
        if len(chunk_bytes) == 0:
            raise Exception('Chunk cannot be empty!')
        enc, iv = cipher_factory()
        enc_bytes = enc.update(chunk_bytes)
        enc_bytes += enc.finalize()
        checksum = sha256(enc_bytes)
        chunk_length = len(enc_bytes).to_bytes(4, 'big', signed=False)

        if f is not None:
            f.write(checksum)
            f.write(chunk_length)
            f.write(enc_bytes)
            f.write(iv)
            f.flush()
            return

        return checksum + chunk_length + enc_bytes + iv

    return encode_chunk

def get_file_chunk_decoder(cipher_factory):
    def decode_chunk(chunk_bytes=None, f=None):
        if f is not None:
            checksum = f.read(32)
            if len(checksum) < 32:
                raise FileChunkError('Invalid checksum')
            chunk_length = f.read(4)
            if len(checksum) < 32:
                raise FileChunkError('Invalid chunk length')
            chunk_length = int.from_bytes(chunk_length, 'big', signed=False)
            enc_bytes = f.read(chunk_length)
            if len(enc_bytes) < chunk_length:
                raise FileChunkError('Missing chunk data')
            iv = f.read()
            dec = cipher_factory(iv)
            dec_bytes = dec.update(enc_bytes)
            dec_bytes += dec.finalize()
            return dec_bytes
        elif chunk_bytes is not None and len(chunk_bytes) > 0:
            # TODO
            raise Exception('Not implemented!')
        else:
            raise Exception()

    return decode_chunk