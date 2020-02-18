from os import environ

DB_USER = environ['DB_USER']
DB_PASSWORD = environ['DB_PASSWORD']
DB_NAME = environ['DB_NAME']
DB_HOST = environ['DB_HOST']
DB_PORT = environ['DB_PORT']
ALCHEMY_DRIVER = environ['ALCHEMY_DRIVER']
DATABASE_URL = '{}://{}:{}@{}:{}/{}'.format(
    ALCHEMY_DRIVER, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME,
)
