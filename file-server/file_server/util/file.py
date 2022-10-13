KILOBYTE = 1024
MEGABYTE = 1024 * KILOBYTE
GIGABYTE = 1024 * MEGABYTE

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