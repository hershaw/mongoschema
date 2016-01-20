# 3rd party
from flask import Flask

# from this project
from test import User, EmailEntry
from base import register_flask_app

app = Flask(__name__)

app.config.update(dict(
    DEBUG=True,
))


def custom_response(_):
    # Doesn't matter what is passed here, will just return empty object
    return '{"1": 1}'

PATH_PREFIX = '/api/v0'

register_flask_app(app, PATH_PREFIX)
User.static_route('list')
User.static_route('create', func='custom_create', methods=['POST'])
User.doc_route('get')
User.doc_route('update', methods=['PATCH'])
User.doc_route('remove', methods=['DELETE'])
User.doc_route('get-username')
User.doc_route('set-username', methods=['PATCH'])
User.doc_route('useless-function', custom_response=custom_response)
User.static_route('useless-function', custom_response=custom_response)


EmailEntry.static_route('create', methods=['POST'])

print(app.url_map)

if __name__ == '__main__':
    app.run(port=9002)
