import logging
import os
import sqlite3

def setup_db(db_config):
    db_path = db_config.get('sqlite-db-path', 'local_server.db')

    if os.path.exists(db_path):
        raise Exception('SQLite database [{}] exists!'.format(db_path))

    logging.debug('Setting up SQLite database \'{}\''.format(db_path))
    conn = sqlite3.connect(db_path)
    try:
        create_file_table(conn)
    finally:
        try:
            conn.close()
        except:
            pass

def create_file_table(conn):
    logging.debug('Setting up ps_file_table')
    conn.execute(
        '''
        CREATE TABLE ps_remote_file (
            id INTEGER PRIMARY KEY NOT NULL,
            remote_id VARCHAR(38) UNIQUE NOT NULL,
            file_size INTEGER NOT NULL DEFAULT 0,
            is_committed BOOLEAN NOT NULL DEFAULT 0,
            created_timestamp INTEGER NOT NULL DEFAULT 0,
            modified_timestamp INTEGER NOT NULL DEFAULT 0
        )
        '''
    )