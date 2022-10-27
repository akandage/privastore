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

#
# Number of bytes to use encoding a length value.
#
CHUNK_LENGTH_BYTES = 4

#
# Endianness of length value bytes.
#
CHUNK_LENGTH_ENDIANNESS = 'big'

#
# Checksum length (SHA256)
#
CHECKSUM_LENGTH = 32

#
# Offsets of data within the decrypted block.
#
CHUNK_LENGTH_START = CHECKSUM_LENGTH
CHUNK_LENGTH_END = CHUNK_LENGTH_START + CHUNK_LENGTH_BYTES
CHUNK_START = CHUNK_LENGTH_END

def get_encrypted_chunk_encoder(cipher_factory):
    def encode_chunk(chunk_bytes, chunk_file=None):
        chunk_length = len(chunk_bytes)
        if chunk_length == 0:
            raise Exception('Chunk cannot be empty!')
        enc, iv = cipher_factory()
        chunk_checksum = sha256(chunk_bytes)
        chunk_length = chunk_length.to_bytes(CHUNK_LENGTH_BYTES, CHUNK_LENGTH_ENDIANNESS, signed=False)
        enc_bytes = enc.update(chunk_checksum)
        enc_bytes += enc.update(chunk_length)
        enc_bytes += enc.update(chunk_bytes)
        enc_bytes += enc.finalize()
        enc_length = len(enc_bytes).to_bytes(CHUNK_LENGTH_BYTES, CHUNK_LENGTH_ENDIANNESS, signed=False)
        
        checksum = sha256(enc_length, enc_bytes, iv)

        if chunk_file is not None:
            file_size = 0
            file_size += write_all(chunk_file, checksum)
            file_size += write_all(chunk_file, enc_length)
            file_size += write_all(chunk_file, enc_bytes)
            file_size += write_all(chunk_file, iv)
            chunk_file.flush()
            return file_size

        return checksum + enc_length + enc_bytes + iv

    return encode_chunk

def get_encrypted_chunk_decoder(cipher_factory):
    def decode_chunk(chunk_bytes=None, chunk_file=None):
        if chunk_file is not None:
            checksum = chunk_file.read(CHECKSUM_LENGTH)
            if len(checksum) < CHECKSUM_LENGTH:
                raise FileChunkError('Invalid checksum')
            enc_length_bytes = chunk_file.read(CHUNK_LENGTH_BYTES)
            if len(enc_length_bytes) < CHUNK_LENGTH_BYTES:
                raise FileChunkError('Invalid encrypted chunk length')
            enc_length = int.from_bytes(enc_length_bytes, CHUNK_LENGTH_ENDIANNESS, signed=False)
            enc_bytes = chunk_file.read(enc_length)
            if len(enc_bytes) < enc_length:
                raise FileChunkError('Missing encrypted chunk data')
            iv = chunk_file.read()
            if sha256(enc_length_bytes, enc_bytes, iv) != checksum:
                raise FileChunkError('Checksum mismatch')
            dec = cipher_factory(iv)
            dec_bytes = dec.update(enc_bytes)
            dec_bytes += dec.finalize()
            chunk_checksum = dec_bytes[:CHECKSUM_LENGTH]
            chunk_length = dec_bytes[CHUNK_LENGTH_START:CHUNK_LENGTH_END]
            chunk_length = int.from_bytes(chunk_length, CHUNK_LENGTH_ENDIANNESS)
            chunk_bytes = dec_bytes[CHUNK_START:(CHUNK_START+chunk_length)]
            if len(chunk_bytes) < chunk_length:
                raise FileChunkError('Chunk data missing')
            elif len(chunk_bytes) > chunk_length:
                raise FileChunkError('Extra chunk data')
            if sha256(chunk_bytes) != chunk_checksum:
                raise FileChunkError('Chunk checksum mismatch')
            return chunk_bytes
        elif chunk_bytes is not None and len(chunk_bytes) > 0:
            # TODO
            raise Exception('Not implemented!')
        else:
            raise Exception()

    return decode_chunk