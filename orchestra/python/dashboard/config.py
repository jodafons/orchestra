__all__ = [
    'SECRET_KEY',
    'SECURITY_URL_PREFIX',
    'SECURITY_PASSWORD_HASH',
    'SECURITY_PASSWORD_SALT',
    'SECURITY_LOGIN_URL',
    'SECURITY_LOGOUT_URL',
    'SECURITY_REGISTER_URL',
    'SECURITY_POST_LOGIN_VIEW',
    'SECURITY_POST_LOGOUT_VIEW',
    'SECURITY_POST_REGISTER_VIEW',
    'SECURITY_REGISTERABLE'
]

# Create dummy secrey key so we can use sessions
SECRET_KEY = '74FUFRFdtwAhGLge'

# Create in-memory database
SQLALCHEMY_DATABASE_URI = 'postgresql://postgres:DmtS_FjA7e+dSkP&@localhost:5433/qualimeter_internal'
SQLALCHEMY_ECHO = False

# Flask-Security config
SECURITY_URL_PREFIX = "/admin"
SECURITY_PASSWORD_HASH = "pbkdf2_sha512"
SECURITY_PASSWORD_SALT = "Krj6cd2mW63pER7Hjy8bsUbXYLY6t"

# Flask-Security URLs, overridden because they don't put a / at the end
SECURITY_LOGIN_URL = "/login/"
SECURITY_LOGOUT_URL = "/logout/"
SECURITY_REGISTER_URL = "/register/"

SECURITY_POST_LOGIN_VIEW = "/admin/"
SECURITY_POST_LOGOUT_VIEW = "/admin/"
SECURITY_POST_REGISTER_VIEW = "/admin/"

# Flask-Security features
SECURITY_REGISTERABLE = True
SECURITY_SEND_REGISTER_EMAIL = True
SQLALCHEMY_TRACK_MODIFICATIONS = False

# Constants
INITIAL_SLEEP_TIME = 10

# LightDBEmulator API
light_db_endpoint = "http://192.168.88.155:3112/emulator"
