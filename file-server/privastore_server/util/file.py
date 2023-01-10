import configparser
from typing import BinaryIO

KILOBYTE = 1024
MEGABYTE = 1024 * KILOBYTE
GIGABYTE = 1024 * MEGABYTE

def chunked_copy(in_file: BinaryIO, out_file: BinaryIO, file_size: int, chunk_size: int):
    bytes_read = 0
    bytes_copied = 0
    buf = bytes()
    buf_len = 0

    while bytes_copied < file_size:
        read_size = min(file_size - bytes_read, chunk_size - buf_len)
        if read_size > 0:
            data = in_file.read(read_size)
            read = len(data)
        else:
            read = 0

        if read == 0:
            if buf_len > 0:
                write_all(out_file, buf)
                bytes_copied += buf_len
            break
        elif read > read_size:
            raise Exception()

        buf += data
        buf_len += read
        bytes_read += read

        if buf_len == chunk_size:
            write_all(out_file, buf)
            bytes_copied += buf_len
            buf = bytes()
            buf_len = 0
    
    out_file.flush()
    return bytes_copied

def read_all(file: BinaryIO, read_len: int):
    read = 0
    buf = bytes()
    while read < read_len:
        r = file.read(read_len-read)
        r_len = len(r)
        if r_len == 0:
            raise Exception('No data read!')
        buf += r
        read += r_len
    return buf

def write_all(file: BinaryIO, data: bytes):
    data_len = len(data)
    written = 0
    while written < data_len:
        w = file.write(data[written:])
        if w == 0:
            raise Exception('No data written!')
        written += w
    return data_len

def parse_mem_size(mem_size: str) -> int:
    try:
        if mem_size.endswith('KB'):
            return round(KILOBYTE * float(mem_size[:-2]))
        elif mem_size.endswith('MB'):
            return round(MEGABYTE * float(mem_size[:-2]))
        elif mem_size.endswith('GB'):
            return round(GIGABYTE * float(mem_size[:-2]))
        elif mem_size.endswith('B'):
            return int(mem_size[:-1])
    except:
        pass
    raise Exception('Unrecognized memory size [{}]'.format(mem_size))

def str_mem_size(mem_size: int) -> str:
    if mem_size >= GIGABYTE:
        return '{:.2f}GB'.format(mem_size/GIGABYTE)
    elif mem_size >= MEGABYTE:
        return '{:.2f}MB'.format(mem_size/MEGABYTE)
    elif mem_size >= KILOBYTE:
        return '{:.2f}KB'.format(mem_size/KILOBYTE)
    return '{}B'.format(mem_size)

def str_path(path: list[str]) -> str:
    try:
        return '/' + '/'.join(path)
    except:
        return '[invalid path]'

def read_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

def config_bool(config_val: str) -> bool:
    try:
        config_val = config_val.lower()
        if config_val == 'true' or config_val == '1':
            return True
        elif config_val == 'false' or config_val == '0':
            return False
    except:
        pass
    raise Exception('Invalid boolean configuration value')
    