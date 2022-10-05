def str_path(path):
    try:
        return '/' + '/'.join(path)
    except:
        return '[invalid path]'