import logging
import os
import sqlite3
import time

from ....crypto.util import hash_user_password
from ....directory import Directory
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
        create_file_table(conn)
        create_log_table(conn)
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
            uid VARCHAR(38) UNIQUE NOT NULL,
            created_timestamp INTEGER NOT NULL,
            modified_timestamp INTEGER NOT NULL,
            owner VARCHAR(50) NOT NULL,
            FOREIGN KEY (owner) REFERENCES ps_user_account (username) ON DELETE CASCADE
        )
        '''
    )
    # Create root directory (each user has its own).
    now = round(time.time())
    conn.execute('INSERT INTO ps_directory (name, uid, created_timestamp, modified_timestamp, owner) VALUES (?, ?, ?, ?, ?)', ('/', Directory.generate_uid(), now, now, 'psadmin'))
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

def create_file_table(conn: sqlite3.Connection):
    logging.debug('Setting up ps_file table')
    conn.execute(
        '''
        CREATE TABLE ps_file (
            id INTEGER PRIMARY KEY NOT NULL,
            parent_id INTEGER NOT NULL,
            name VARCHAR(256) NOT NULL,
            uid VARCHAR(38) UNIQUE NOT NULL,
            mime_type VARCHAR(100) NOT NULL,
            created_timestamp INTEGER NOT NULL,
            modified_timestamp INTEGER NOT NULL,
            owner VARCHAR(50) NOT NULL,
            FOREIGN KEY (parent_id) REFERENCES ps_directory (id) ON DELETE CASCADE,
            FOREIGN KEY (owner) REFERENCES ps_user_account (username) ON DELETE CASCADE
        )
        '''
    )
    conn.execute(
        '''
        CREATE TABLE ps_file_data (
            id INTEGER PRIMARY KEY NOT NULL,
            uid VARCHAR(39) UNIQUE NOT NULL,
            file_id INTEGER NULL,
            version INTEGER NULL,
            size INTEGER NOT NULL,
            FOREIGN KEY (file_id) REFERENCES ps_file (id) ON DELETE SET NULL
        )
        '''
    )
    conn.commit()

def create_log_table(conn: sqlite3.Connection):
    logging.debug('Setting up ps_log table')
    conn.execute(
        '''
        CREATE TABLE ps_log (
            id INTEGER PRIMARY KEY NOT NULL,
            epoch_no INTEGER NOT NULL,
            entry_type INTEGER NOT NULL
        )
        '''
    )
    conn.execute(
        '''
        CREATE TABLE ps_log_entry (
            entry_id INTEGER NOT NULL,
            chunk_id INTEGER NOT NULL,
            PRIMARY KEY (entry_id, chunk_id),
            FOREIGN KEY (entry_id) REFERENCES ps_log (id) ON DELETE CASCADE
        )
        '''
    )
    conn.commit()