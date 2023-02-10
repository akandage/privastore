import logging
import os
import sqlite3

from ....crypto.util import hash_user_password
from ....error import DatabaseError

def setup_db(db_config):
    db_path = db_config.get('sqlite-db-path', 'local_server.db')

    if os.path.exists(db_path):
        raise DatabaseError('SQLite database [{}] exists!'.format(db_path))

    logging.debug('Setting up SQLite database \'{}\''.format(db_path))
    conn = sqlite3.connect(db_path)
    try:
        create_user_account_table(conn)
    finally:
        try:
            conn.close()
        except:
            pass

def create_user_account_table(conn: sqlite3.Connection):
    logging.debug('Setting up ps_user_account table')
    conn.execute(
        '''
        CREATE TABLE ps_user_account (
            username VARCHAR(50) PRIMARY KEY NOT NULL,
            password_hash BLOB(32) NOT NULL
        )
        '''
    )
    # Create admin user with default password.
    conn.execute(
        '''
        INSERT INTO ps_user_account (username, password_hash) VALUES 
        (?, ?)
        ''',
        ('psadmin', hash_user_password('psadmin'))
    )
    conn.commit()