import logging
import os
import sqlite3
from ....key import Key
from ....util.crypto import hash_user_password

def setup_db(db_config):
    db_path = db_config.get('sqlite-db-path', 'local_server.db')

    if os.path.exists(db_path):
        raise Exception('SQLite database [{}] exists!'.format(db_path))

    logging.debug('Setting up SQLite database \'{}\''.format(db_path))
    conn = sqlite3.connect(db_path)
    try:
        create_user_account_table(conn)
        create_key_table(conn)
        create_directory_table(conn)
        create_file_table(conn)
        create_file_data_table(conn)
        create_file_version_table(conn)
        create_remote_server_table(conn)
    finally:
        try:
            conn.close()
        except:
            pass

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

def create_key_table(conn):
    logging.debug('Setting up ps_key table')
    conn.execute(
        '''
        CREATE TABLE ps_key (
            id INTEGER PRIMARY KEY NOT NULL,
            name VARCHAR(50) UNIQUE NOT NULL,
            key_bytes VARCHAR(32) NOT NULL,
            algorithm VARCHAR(50) NOT NULL,
            is_system BOOLEAN NOT NULL DEFAULT 0
        )
        '''
    )
    conn.execute("INSERT INTO ps_key (id, name, key_bytes, algorithm, is_system) VALUES (?, ?, ?, ?, ?)", (0, 'null', bytes(), 'null', False))
    algorithm = 'aes-256-cbc'
    key_bytes = Key.generate_key_bytes(algorithm)
    conn.execute("INSERT INTO ps_key (id, name, key_bytes, algorithm, is_system) VALUES (?, ?, ?, ?, ?)", (1, 'system', key_bytes, algorithm, True))
    conn.commit()

def create_directory_table(conn):
    logging.debug('Setting up ps_directory table')
    conn.execute(
        '''
        CREATE TABLE ps_directory (
            id INTEGER PRIMARY KEY NOT NULL,
            name VARCHAR(256) NOT NULL,
            is_hidden BOOLEAN NOT NULL DEFAULT 0
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
            file_type INTEGER NOT NULL,
            parent_id INTEGER NOT NULL,
            is_hidden BOOLEAN NOT NULL DEFAULT 0,
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
            created_timestamp INTEGER NOT NULL,
            file_data_id INTEGER NOT NULL,
            FOREIGN KEY (file_id) REFERENCES ps_file (id) ON DELETE CASCADE,
            FOREIGN KEY (file_data_id) REFERENCES ps_file_data (id) ON DELETE RESTRICT,
            PRIMARY KEY (file_id, version)
        )
        '''
    )
    conn.commit()

def create_file_data_table(conn):
    logging.debug('Setting up ps_file_data table')
    conn.execute(
        '''
        CREATE TABLE ps_file_data (
            id INTEGER PRIMARY KEY NOT NULL,
            created_timestamp INTEGER NOT NULL,
            key_id INTEGER NOT NULL DEFAULT 0,
            local_id VARCHAR(38) UNIQUE NULL,
            remote_id VARCHAR(38) UNIQUE NULL,
            file_size INTEGER NOT NULL DEFAULT 0,
            size_on_disk INTEGER NOT NULL DEFAULT 0,
            total_chunks INTEGER NOT NULL DEFAULT 0,
            uploaded_chunks INTEGER NOT NULL DEFAULT 0,
            downloaded_chunks INTEGER NOT NULL DEFAULT 0,
            local_transfer_status INTEGER NOT NULL,
            remote_transfer_status INTEGER NOT NULL,
            FOREIGN KEY (key_id) REFERENCES ps_key (id) ON DELETE RESTRICT
        )
        '''
    )
    conn.commit()

def create_remote_server_table(conn):
    logging.debug('Setting up ps_remote_cluster table')
    conn.execute(
        '''
        CREATE TABLE ps_remote_cluster (
            id INTEGER PRIMARY KEY NOT NULL,
            name VARCHAR(256) NOT NULL,
            username VARCHAR(50) NOT NULL,
            password VARCHAR(50) NOT NULL
        )
        '''
    )
    # TODO: Don't store plaintext password.
    conn.execute("INSERT INTO ps_remote_cluster (id, name, username, password) VALUES (1, 'default-cluster', 'psadmin', 'psadmin')")
    logging.debug('Setting up ps_remote_server table')
    conn.execute(
        '''
        CREATE TABLE ps_remote_server (
            hostname VARCHAR(256) NOT NULL,
            port INTEGER NOT NULL,
            cluster_id INTEGER NOT NULL,
            use_ssl BOOLEAN NOT NULL DEFAULT 0,
            PRIMARY KEY (hostname, port),
            FOREIGN KEY (cluster_id) REFERENCES ps_remote_cluster (id) ON DELETE CASCADE
        )
        '''
    )
    conn.execute("INSERT INTO ps_remote_server (hostname, port, cluster_id) VALUES ('localhost', 9090, 1)")
    conn.commit()