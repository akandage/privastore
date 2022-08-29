import hashlib

'''
    Hash a password.
'''
def hash_user_password(password):
    if type(password) is not bytes:
        password = bytes(password, 'utf-8')

    return hashlib.sha256(password).digest()