from flask_wtf.csrf import CSRFProtect
from flask_assets import Environment
from celery import Celery

csrf = CSRFProtect()
assets = Environment()
celery_app = Celery()
