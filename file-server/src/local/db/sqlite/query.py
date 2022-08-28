import logging
import util.crypto

def login_user(conn, username, password):
    cur = conn.cursor()
    try:
        try:
            cur.execute('SELECT password_hash FROM ps_user_account WHERE username = ?', username)
            res = cur.fetchone()
        except Exception as e:
            logging.error('Query error {}'.format(str(e)))
            try:
                conn.rollback()
                logging.debug('Rolled back')
            except Exception as e1:
                logging.error('Error rolling back {}'.format(str(e1)))
            return

        if res:
            password_hash, = res
            if password_hash == util.crypto.hash_user_password(password):
                return True
            else:
                return False
        else:
            raise Exception('User not found!')

    finally:
        try:
            cur.close()
        except:
            pass