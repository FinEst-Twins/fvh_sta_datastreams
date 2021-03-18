import os

basedir = os.path.abspath(os.path.dirname(__file__))


def get_env_variable(name):
    try:
        return os.environ[name]
    except KeyError:
        message = "Expected environment variable '{}' not set.".format(name)
        raise Exception(message)


class Config(object):

    SECRET_KEY = get_env_variable("SECRET_KEY")
    DEBUG = True
    CSRF_ENABLED = True
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    WTF_CSRF_ENABLED = True
    HOSTED_URL = get_env_variable("BASE_URL")


class ProductionConfig(Config):
    DEBUG = False
    SQLALCHEMY_DATABASE_URI = get_env_variable("DATABASE_URL")
    SECRET_KEY = get_env_variable("SECRET_KEY")


class DevelopmentConfig(Config):
    DEVELOPMENT = True
    TESTING = False
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = get_env_variable("DATABASE_URL")
    SECRET_KEY = get_env_variable("SECRET_KEY")


class TestingConfig(Config):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = get_env_variable("DATABASE_TEST_URL")
    SECRET_KEY = get_env_variable("SECRET_KEY")
