KILOBYTE = 1024
MEGABYTE = 1024 * KILOBYTE
GIGABYTE = 1024 * MEGABYTE

def chunked_transfer(in_file, out_file, file_size, chunk_size):
    bytes_transferred = 0
    buf = bytes()
    buf_len = 0

    while bytes_transferred < file_size:
        read_size = chunk_size - buf_len
        data = in_file.read(read_size)
        read = len(data)

        if read == 0:
            if buf_len > 0:
                out_file.write(buf)
                bytes_transferred += buf_len
            break
        elif read > read_size:
            raise Exception()

        buf += data
        buf_len += read

        if buf_len == chunk_size:
            write_all(out_file, buf)
            bytes_transferred += buf_len
            buf = bytes()
            buf_len = 0
    
    out_file.flush()
    return bytes_transferred

def write_all(file, data):
    data_len = len(data)
    written = 0
    while written < data_len:
        w = file.write(data[written:])
        if w == 0:
            raise Exception('No data written!')
        written += w
    return data_len

def parse_mem_size(mem_size):
    try:
        if mem_size.endswith('KB'):
            return round(KILOBYTE * int(mem_size[:-2]))
        elif mem_size.endswith('MB'):
            return round(MEGABYTE * int(mem_size[:-2]))
        elif mem_size.endswith('GB'):
            return round(GIGABYTE * int(mem_size[:-2]))
        elif mem_size.endswith('B'):
            return int(mem_size[:-1])
    except:
        pass
    raise Exception('Unrecognized memory size [{}]'.format(mem_size))

def str_mem_size(mem_size):
    if mem_size >= GIGABYTE:
        return '{:.2f}GB'.format(mem_size/GIGABYTE)
    elif mem_size >= MEGABYTE:
        return '{:.2f}MB'.format(mem_size/MEGABYTE)
    elif mem_size >= KILOBYTE:
        return '{:.2f}KB'.format(mem_size/KILOBYTE)
    return '{}B'.format(mem_size)

def str_path(path):
    try:
        return '/' + '/'.join(path)
    except:
        return '[invalid path]'