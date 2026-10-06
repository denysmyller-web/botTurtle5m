import hmac
import os
from functools import wraps
from flask import jsonify, redirect, request, session, url_for
from werkzeug.security import check_password_hash


def configure_security(app):
    app.config.update(
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE='Strict',
        SESSION_COOKIE_SECURE=os.getenv('COOKIE_SECURE', '0') == '1',
        PERMANENT_SESSION_LIFETIME=1800,
    )


def credentials():
    username = (os.getenv('ADMIN_USERNAME', '') or 'admin').strip() or 'admin'
    plain_password = os.getenv('ADMIN_PASSWORD', '').strip()
    password_hash = os.getenv('ADMIN_PASSWORD_HASH', '').strip()
    return username, plain_password, password_hash


def allow_default_local_access():
    _, plain_password, password_hash = credentials()
    host = (request.host or '').lower()
    if host.startswith(('127.0.0.1', 'localhost', '0.0.0.0')):
        return True
    return not password_hash and (not plain_password or plain_password == 'admin')


def password_matches(password):
    _, plain_password, password_hash = credentials()

    if password_hash and check_password_hash(password_hash, password):
        return True

    if plain_password and hmac.compare_digest(password, plain_password):
        return True

    if allow_default_local_access() and hmac.compare_digest(password, 'admin'):
        return True

    return False


def login_required(view):
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if session.get('authenticated') is True:
            return view(*args, **kwargs)
        if allow_default_local_access():
            session['authenticated'] = True
            return view(*args, **kwargs)
        if request.path.startswith('/api/'):
            return jsonify({'ok': False, 'error': 'Login necessário.'}), 401
        return redirect(url_for('api.login'))
    return wrapped_view
