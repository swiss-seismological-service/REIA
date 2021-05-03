import os


class Config(object):
    SECRET_KEY = os.environ.get(
        'SECRET_KEY') or 'EC9F71CFE9D117F9EFB21EA96F88D'
    SERVER_NAME = 'localhost:5000'
