from flask import Flask, jsonify, request

from ...error import AuthenticationError, HttpError

app = Flask(__name__)

@app.errorhandler(AuthenticationError)
def http_error_handler(e: AuthenticationError):
    return jsonify(e.to_dict()), 401

@app.errorhandler(HttpError)
def http_error_handler(e: HttpError):
    return jsonify(e.to_dict()), 400

@app.route("/login")
def login_user():
    if not request.authorization:
        raise HttpError('Missing HTTP basic auth')
