import logging
import sqlite3
from ....util.crypto import hash_user_password

def setup_db(db_config):
    db_path = db_config.get('sqlite-db-path', 'local_server.db')

    logging.debug('Setting up SQLite database \'{}\''.format(db_path))
    conn = sqlite3.connect(db_path)
    try:
        create_user_account_table(conn)
        create_directory_table(conn)
        create_file_table(conn)
        create_file_version_table(conn)
    finally:
        try:
            conn.close()
        except:
            pass

def sqlite_conn_factory(db_path, isolation_level=None):
    return lambda: sqlite3.connect(db_path, isolation_level=isolation_level)

def create_user_account_table(conn):
    logging.debug('Setting up ps_user_account table')
    conn.execute(
        '''
        CREATE TABLE ps_user_account (
            username VARCHAR(50) PRIMARY KEY NOT NULL,
            password_hash VARCHAR(32) NOT NULL
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

def create_directory_table(conn):
    logging.debug('Setting up ps_directory table')
    conn.execute(
        '''
        CREATE TABLE ps_directory (
            id INTEGER PRIMARY KEY NOT NULL,
            name VARCHAR(256) NOT NULL,
            is_hidden BOOLEAN NOT NULL DEFAULT 0,
            is_removed BOOLEAN NOT NULL DEFAULT 0
        )
        '''
    )
    # Create root directory.
    conn.execute("INSERT INTO ps_directory (id, name) VALUES (1, '/')")
    conn.execute(
        '''
        CREATE TABLE ps_link (
            parent_id INTEGER NOT NULL,
            child_id INTEGER NOT NULL,
            PRIMARY KEY (parent_id, child_id),
            FOREIGN KEY (parent_id) REFERENCES ps_directory (id) ON DELETE CASCADE,
            FOREIGN KEY (child_id) REFERENCES ps_directory (id) ON DELETE CASCADE
        )
        '''
    )
    conn.commit()

def create_file_table(conn):
    logging.debug('Setting up ps_file table')
    conn.execute(
        '''
        CREATE TABLE ps_file (
            id INTEGER PRIMARY KEY NOT NULL,
            name VARCHAR(256) NOT NULL,
            parent_id INTEGER NOT NULL,
            is_hidden BOOLEAN NOT NULL DEFAULT 0,
            is_removed BOOLEAN NOT NULL DEFAULT 0,
            UNIQUE (name, parent_id),
            FOREIGN KEY (parent_id) REFERENCES ps_directory (id) ON DELETE CASCADE
        )
        '''
    )
    conn.commit()

def create_file_version_table(conn):
    logging.debug('Setting up ps_file_version table')
    conn.execute(
        '''
        CREATE TABLE ps_file_version (
            file_id INTEGER NOT NULL,
            version INTEGER NOT NULL,
            local_id VARCHAR(39) UNIQUE NULL,
            remote_id VARCHAR(39) UNIQUE NULL,
            size_bytes INTEGER NOT NULL,
            transfer_status INTEGER NOT NULL,
            FOREIGN KEY (file_id) REFERENCES ps_file (id) ON DELETE CASCADE,
            PRIMARY KEY (file_id, version)
        )
        '''
    )
    conn.commit()