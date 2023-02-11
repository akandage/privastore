import logging
import os
import sqlite3

from ....crypto.util import hash_user_password
from ....error import DatabaseError
from ....key import Key

def setup_db(db_config):
    db_path = db_config.get('sqlite-db-path', 'local_server.db')

    if os.path.exists(db_path):
        raise DatabaseError('SQLite database [{}] exists!'.format(db_path))

    logging.debug('Setting up SQLite database \'{}\''.format(db_path))
    conn = sqlite3.connect(db_path)
    try:
        create_user_account_table(conn)
        create_key_table(conn)
        create_directory_table(conn)
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

def create_key_table(conn: sqlite3.Connection):
    logging.debug('Setting up ps_key table')
    conn.execute(
        '''
        CREATE TABLE ps_key (
            id INTEGER PRIMARY KEY NOT NULL,
            name VARCHAR(256) UNIQUE NOT NULL,
            key_bytes BLOB(32) NOT NULL,
            algorithm VARCHAR(25) NOT NULL,
            is_system BOOLEAN NOT NULL DEFAULT 0,
            owner VARCHAR(50) NOT NULL,
            FOREIGN KEY (owner) REFERENCES ps_user_account (username) ON DELETE CASCADE
        )
        '''
    )
    algorithm = 'aes-256-cbc'
    key_bytes = Key.generate_key_bytes(algorithm)
    conn.execute("INSERT INTO ps_key (id, name, key_bytes, algorithm, is_system, owner) VALUES (?, ?, ?, ?, ?, ?)", (1, 'system', key_bytes, algorithm, True, 'psadmin'))
    conn.commit()

def create_directory_table(conn: sqlite3.Connection):
    logging.debug('Setting up ps_directory table')
    conn.execute(
        '''
        CREATE TABLE ps_directory (
            id INTEGER PRIMARY KEY NOT NULL,
            name VARCHAR(256) NOT NULL,
            uuid VARCHAR(38) UNIQUE NOT NULL,
            created_timestamp INTEGER NOT NULL,
            modified_timestamp INTEGER NOT NULL,
            owner VARCHAR(50) NOT NULL,
            FOREIGN KEY (owner) REFERENCES ps_user_account (username) ON DELETE CASCADE
        )
        '''
    )
    conn.execute(
        '''
        CREATE TABLE ps_directory_link (
            parent_id INTEGER NOT NULL,
            child_id INTEGER NOT NULL,
            FOREIGN KEY (parent_id) REFERENCES ps_directory (id) ON DELETE CASCADE,
            FOREIGN KEY (child_id) REFERENCES ps_directory (id) ON DELETE CASCADE,
            PRIMARY KEY (parent_id, child_id)
        )
        '''
    )
    conn.commit()