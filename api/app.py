import os

from dotenv import load_dotenv
from flask import Flask

from api.routes import chicago_crimes_blueprint

# loading environment variables which are defined in .env file
load_dotenv()
debug_mode = os.environ.get('FLASK_DEBUG') == '1'
application = Flask(__name__)
application.config['SECRET_KEY'] = os.environ['FLASK_SECRET_KEY']

# register crimes routes blueprint to our application
application.register_blueprint(chicago_crimes_blueprint)

if __name__ == '__main__':
    application.run(debug=debug_mode, port=8000, host='0.0.0.0')
