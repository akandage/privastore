from flask import Flask, jsonify, request
from functools import wraps
from http import HTTPStatus
import logging

from ...error import AuthenticationError, HttpError, SessionError
from ..server import get_local_server

API_VERSION = 1
SESSION_ID_HEADER = 'x-privastore-session-id'

app = Flask(__name__)

@app.errorhandler(AuthenticationError)
def http_error_handler(e: AuthenticationError):
    return jsonify(e.to_dict()), HTTPStatus.UNAUTHORIZED

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