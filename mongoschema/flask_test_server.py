# 3rd party
from flask import Flask, request

# from this project
from test import User

app = Flask(__name__)

app.config.update(dict(
    DEBUG=True,
))

User.register_app(app)
User.doc_route('get-username')
User.doc_route('set-username', methods=['PATCH'])

print(app.url_map)

if __name__ == '__main__':
    app.run(port=9002)
