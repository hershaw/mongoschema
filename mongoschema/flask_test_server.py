# 3rd party
from flask import Flask, request

# from this project
from test import User, EmailEntry
from base import register_flask_app, AuthError

app = Flask(__name__)

app.config.update(dict(
    DEBUG=True,
))


def custom_response(_):
    # Doesn't matter what is passed here, will just return empty object
    return '{"1": 1}'

register_flask_app(app, '/api/v0')

User.static_route('list')
User.static_route('create', func='custom_create', methods=['POST'])
User.static_route('useless-function', custom_response=custom_response)
User.doc_route('get')
User.doc_route('update', methods=['PATCH'])
User.doc_route('remove', methods=['DELETE'])
User.doc_route('get-username')
User.doc_route('get-with-params')
User.doc_route('set-username', methods=['PATCH'])
User.doc_route('useless-function', custom_response=custom_response)


def default_auth(*args):
    if not request.args.get('authparam') == 'supersecret':
        print('authparam is not supersecret')
        raise AuthError(401)


def custom_auth(email):
    if not request.args.get('authparam') == 'even_more_secret':
        print('authparam is not even more secret')
        raise AuthError(404)


def default_static():
    if not request.args.get('authparam') == 'default_static':
        print('authparams (%s) is not default_static' % request.args)
        raise AuthError(404)


def custom_static():
    if not request.args.get('authparam') == 'admin_for_real':
        print('authparams is not admin_for_real')
        raise AuthError(404)


EmailEntry.set_auth(default_auth, default_static)

EmailEntry.static_route('custom_static', auth=custom_static, methods=['POST'])
EmailEntry.static_route('create', methods=['POST'])
EmailEntry.doc_route('update', methods=['PATCH'], auth=custom_auth)

print(app.url_map)

if __name__ == '__main__':
    app.run(port=9002)
