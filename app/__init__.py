from flask import Flask, render_template
from flask.logging import default_handler
from logging.handlers import RotatingFileHandler

import os
import logging

from app.blueprints import frontend, api
from app.extensions import csrf, assets, celery_app
from app.extensions.celery_tasks import init_celery
from app.bundles import bundles

from datamodel import session
from config import get_config


def create_app(config=get_config()):

    app = Flask(__name__)
    app.config.from_object(config)

    register_blueprints(app)

    initialize_extensions(app)

    if not app.debug:
        configure_logging(app)

    register_error_handlers(app)

    # specify additional actions on app teardown
    app_teardown(app)

    return app


def initialize_extensions(app):
    # init csrf
    csrf.init_app(app)

    # init and register static asset bundles
    assets.init_app(app)
    register_assets()

    # bind celery into app context
    init_celery(app, celery_app)


def register_assets():
    assets.register('js_bundle', bundles['js_bundle'])
    assets.register('css_bundle', bundles['css_bundle'])


def register_blueprints(app):
    app.register_blueprint(frontend)
    app.register_blueprint(api)


def configure_logging(app):
    # Set up logger
    if not os.path.exists('logs'):
        os.mkdir('logs')

    app.logger.removeHandler(default_handler)
    file_handler = RotatingFileHandler('logs/app.log', maxBytes=10240,
                                       backupCount=10)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))
    file_handler.setLevel(logging.INFO)
    app.logger.addHandler(file_handler)

    app.logger.setLevel(logging.INFO)
    app.logger.info('project startup')


def register_error_handlers(app):
    # TODO: create templates for errorhandlers
    # @app.errorhandler(400)
    # def bad_request(e):
    #     return render_template('404.html'), 400

    # @app.errorhandler(403)
    # def forbidden(e):
    #     return render_template('404.html'), 403

    @ app.errorhandler(404)
    def page_not_found(e):
        return render_template('404.html'), 404

    # @app.errorhandler(405)
    # def method_not_allowed(e):
    #     return render_template('404.html'), 405

    # @app.errorhandler(500)
    # def server_error(e):
    #     return render_template('404.html'), 500


def app_teardown(app):
    @ app.teardown_appcontext
    def shutdown_session(exception=None):
        session.remove()
