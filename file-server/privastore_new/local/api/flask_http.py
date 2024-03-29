from flask import Flask, jsonify, request
from functools import wraps
from http import HTTPStatus
import logging
import urllib.parse

from ...error import AuthenticationError, DirectoryError, FileError, HttpError, LogError, SessionError
from ...log.log_entry import LogEntry
from ...log.log_entry_type import LogEntryType
from ..server import get_local_server

API_VERSION = 1
SESSION_ID_HEADER = 'x-privastore-session-id'

app = Flask(__name__)

@app.errorhandler(AuthenticationError)
def http_error_handler(e: AuthenticationError):
    return jsonify(e.to_dict()), HTTPStatus.UNAUTHORIZED

@app.errorhandler(DirectoryError)
def http_error_handler(e: HttpError):
    ec = e.error_code()
    code = HTTPStatus.INTERNAL_SERVER_ERROR

    if ec == DirectoryError.DIRECTORY_NOT_FOUND or ec == DirectoryError.INVALID_PATH:
        code = HTTPStatus.NOT_FOUND
    elif ec == DirectoryError.DIRECTORY_EXISTS:
        code = HTTPStatus.CONFLICT
    elif ec == DirectoryError.INVALID_DIRECTORY_ID or ec == DirectoryError.INVALID_DIRECTORY_NAME:
        code = HTTPStatus.BAD_REQUEST

    return jsonify(e.to_dict()), code

@app.errorhandler(FileError)
def http_error_handler(e: HttpError):
    ec = e.error_code()
    code = HTTPStatus.INTERNAL_SERVER_ERROR

    if ec == FileError.FILE_NOT_FOUND:
        code = HTTPStatus.NOT_FOUND
    elif ec == FileError.FILE_EXISTS:
        code = HTTPStatus.CONFLICT
    elif ec == FileError.INVALID_FILE_ID or ec == FileError.INVALID_FILE_NAME:
        code = HTTPStatus.BAD_REQUEST

    return jsonify(e.to_dict()), code

@app.errorhandler(HttpError)
def http_error_handler(e: HttpError):
    return jsonify(e.to_dict()), HTTPStatus.BAD_REQUEST

@app.errorhandler(SessionError)
def http_error_handler(e: SessionError):
    ec = e.error_code()
    code = HTTPStatus.INTERNAL_SERVER_ERROR

    if ec == SessionError.INVALID_SESSION_ID:
        code = HTTPStatus.BAD_REQUEST
    elif ec == SessionError.SESSION_NOT_FOUND:
        code = HTTPStatus.UNAUTHORIZED

    return jsonify(e.to_dict()), code

def session_id_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        session_id = request.headers.get(SESSION_ID_HEADER)
        if not session_id:
            raise HttpError(f'Missing required {SESSION_ID_HEADER} header')
        return f(*args, **kwargs)
    return wrapper

@app.route(f'/{API_VERSION}/login', methods=['POST'])
def login_user():
    if not request.authorization:
        raise HttpError('Missing HTTP basic auth')
    
    username = request.authorization.username
    password = request.authorization.password
    logging.debug('User [{}] login'.format(username))

    server = get_local_server()
    conn = server.conn_pool().acquire()
    try:
        user_dao = server.dao_factory().user_dao(conn)
        user_dao.login_user(username, password)
        logging.debug('User [{}] logged in'.format(username))
    finally:
        server.conn_pool().release(conn)

    session_id = server.session_mgr().start_session(username)
    return {'session-id': session_id}, HTTPStatus.OK, {SESSION_ID_HEADER: session_id}

@app.route(f'/{API_VERSION}/heartbeat', methods=['PUT'])
@session_id_required
def heartbeat():
    session_id = request.headers.get(SESSION_ID_HEADER)
    server = get_local_server()
    server.session_mgr().renew_session(session_id)
    return '', HTTPStatus.OK

@app.route(f'/{API_VERSION}/logout', methods=['POST'])
@session_id_required
def logout_user():
    session_id = request.headers.get(SESSION_ID_HEADER)
    server = get_local_server()
    server.session_mgr().end_session(session_id)
    return '', HTTPStatus.OK

@app.route(f'/{API_VERSION}/directory/<name>', methods=['PUT'])
@session_id_required
def create_directory(name):
    session_id = request.headers.get(SESSION_ID_HEADER)
    server = get_local_server()
    server.session_mgr().renew_session(session_id)
    user = server.session_mgr().get_session_user(session_id)
    conn = server.conn_pool().acquire()
    try:
        conn.set_autocommit(False)
        conn.begin_transaction()
        try:
            dir_dao = server.dao_factory().directory_dao(conn)
            log_dao = server.dao_factory().log_dao(conn)
            parent_uid = request.args.get('parent')
            if parent_uid is None:
                parent_dir = dir_dao.get_root_directory(user)
                parent_uid = parent_dir.uid()
            created = dir_dao.create_directory(parent_uid, name, user)
            log_dao.create_log_entry(LogEntry(LogEntryType.CREATE_DIRECTORY, created.to_dict()))
            conn.commit()
        except DirectoryError as e:
            conn.rollback_nothrow()
            raise e
        except LogError as e:
            conn.rollback_nothrow()
            raise e
        except Exception as e:
            conn.rollback_nothrow()
            raise e
    finally:
        server.conn_pool().release(conn)
    return jsonify(created.to_dict()), HTTPStatus.OK

@app.route(f'/{API_VERSION}/file', methods=['POST'])
@session_id_required
def create_file():
    session_id = request.headers.get(SESSION_ID_HEADER)
    server = get_local_server()
    file_store = server.file_store()
    server.session_mgr().renew_session(session_id)
    user = server.session_mgr().get_session_user(session_id)
    created_file_versions = list()

    content_type = request.headers.get('Content-Type')
    if content_type.startswith('multipart/form-data'):
        for file in request.files.values():
            logging.debug('Uploading file {}'.format(file.filename))
            fh = file_store.open_for_writing()
            try:
                fh.append_all(file.stream)
            except Exception as e:
                logging.error('Error uploading file [{}] data: {}'.format(file.filename, str(e)))
                fh.remove_nothrow()
                raise e

            conn = server.conn_pool().acquire()
            try:
                conn.set_autocommit(False)
                conn.begin_transaction()
                try:
                    dir_dao = server.dao_factory().directory_dao(conn)
                    log_dao = server.dao_factory().log_dao(conn)
                    parent_uid = request.args.get('parent')
                    if parent_uid is None:
                        parent_dir = dir_dao.get_root_directory(user)
                        parent_uid = parent_dir.uid()
                    if not dir_dao.file_exists(parent_uid, file.filename, user):
                        file = dir_dao.create_file(parent_uid, file.filename, file.mimetype, user)
                        log_dao.create_log_entry(LogEntry(LogEntryType.CREATE_FILE, file.to_dict()))
                    else:
                        file = dir_dao.get_file(parent_uid, file.filename, user)
                    created_version = dir_dao.create_file_version(file.uid(), fh.uid(), user)
                    log_dao.create_log_entry(LogEntry(LogEntryType.CREATE_FILE_VERSION, created_version.to_dict()))
                    created_file_versions.append(created_version.to_dict())
                    conn.commit()
                except DirectoryError as e:
                    conn.rollback_nothrow()
                    fh.remove_nothrow()
                    raise e
                except FileError as e:
                    conn.rollback_nothrow()
                    fh.remove_nothrow()
                    raise e
                except Exception as e:
                    conn.rollback_nothrow()
                    fh.remove_nothrow()
                    raise e
            finally:
                server.conn_pool().release(conn)
    else:
        raise HttpError('Invalid Content-Type')
    
    return jsonify(created_file_versions), HTTPStatus.OK