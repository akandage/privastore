import time

def format_datetime(t):
    if type(t) is not time.struct_time:
        t = time.localtime(t)

    return time.strftime('%Y-%m-%d %H:%M:%S', t)