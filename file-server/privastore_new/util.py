import time

def format_datetime(t: time.struct_time):
    if type(t) is not time.struct_time:
        t = time.localtime(t)

    return time.strftime('%Y-%m-%d %H:%M:%S', t)

def str_path(path: list[str]):
    return '/' + '/'.join(path)