import hashlib

def hash_user_password(password):
    if type(password) is not bytes:
        password = bytes(password, 'utf-8')

    return sha256(password)

def sha256(*args):
    hash = hashlib.sha256()
    for b in args:
        hash.update(b)
    return hash.digest()