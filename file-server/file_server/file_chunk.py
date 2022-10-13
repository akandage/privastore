from .error import FileChunkError
from .util.crypto import sha256
from .util.file import write_all
import os

def default_chunk_encoder(chunk_bytes, chunk_file=None):
    if chunk_file is not None:
        write_all(chunk_file, chunk_bytes)
        chunk_file.flush()
        return len(chunk_bytes)
    return chunk_bytes

def default_chunk_decoder(chunk_bytes=None, chunk_file=None):
    if chunk_file is not None:
        chunk_bytes = chunk_file.read()
        return chunk_bytes
    elif chunk_bytes is not None and len(chunk_bytes) > 0:
        return chunk_bytes
    else:
        raise Exception()

def get_encrypted_chunk_encoder(cipher_factory):
    def encode_chunk(chunk_bytes, chunk_file=None):
        chunk_length = len(chunk_bytes)
        if chunk_length == 0:
            raise Exception('Chunk cannot be empty!')
        enc, iv = cipher_factory()
        chunk_checksum = sha256(chunk_bytes)
        enc_bytes = enc.update(chunk_checksum)
        enc_bytes += enc.update(chunk_bytes)
        enc_bytes += enc.finalize()
        checksum = sha256(enc_bytes)
        enc_length = len(enc_bytes).to_bytes(4, 'big', signed=False)
        chunk_length = chunk_length.to_bytes(4, 'big', signed=False)

        if chunk_file is not None:
            file_size = 0
            file_size += write_all(chunk_file, checksum)
            file_size += write_all(chunk_file, chunk_length)
            file_size += write_all(chunk_file, enc_length)
            file_size += write_all(chunk_file, enc_bytes)
            file_size += write_all(chunk_file, iv)
            chunk_file.flush()
            return file_size

        return checksum + chunk_length + enc_length + enc_bytes + iv

    return encode_chunk

def get_encrypted_chunk_decoder(cipher_factory):
    def decode_chunk(chunk_bytes=None, chunk_file=None):
        if chunk_file is not None:
            checksum = chunk_file.read(32)
            if len(checksum) < 32:
                raise FileChunkError('Invalid checksum')
            chunk_length = chunk_file.read(4)
            if len(chunk_length) < 4:
                raise FileChunkError('Invalid chunk length')
            chunk_length = int.from_bytes(chunk_length, 'big', signed=False)
            enc_length = chunk_file.read(4)
            if len(enc_length) < 4:
                raise FileChunkError('Invalid encrypted chunk length')
            enc_length = int.from_bytes(enc_length, 'big', signed=False)
            enc_bytes = chunk_file.read(enc_length)
            if len(enc_bytes) < enc_length:
                raise FileChunkError('Missing encrypted chunk data')
            if sha256(enc_bytes) != checksum:
                raise FileChunkError('Checksum mismatch')
            iv = chunk_file.read()
            dec = cipher_factory(iv)
            dec_bytes = dec.update(enc_bytes)
            dec_bytes += dec.finalize()
            chunk_checksum = dec_bytes[:32]
            chunk_bytes = dec_bytes[32:(chunk_length+32)]
            if sha256(chunk_bytes) != chunk_checksum:
                raise FileChunkError('Chunk checksum mismatch')
            return chunk_bytes
        elif chunk_bytes is not None and len(chunk_bytes) > 0:
            # TODO
            raise Exception('Not implemented!')
        else:
            raise Exception()

    return decode_chunk