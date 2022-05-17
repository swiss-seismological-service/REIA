# from flask import Flask, render_template
# from celery import Celery

# from flask.logging import default_handler
# from logging.handlers import RotatingFileHandler

# import os
# import logging

# # from app.extensions import csrf

# from core.database import session
# from config import get_config


# CELERY_TASK_LIST = [
#     # 'app.blueprints.api.tasks',
# ]


# def create_celery_app(app=None):
#     """
#     Create a new Celery object and tie together the Celery config to
#     the app's config.
#     Wrap all tasks in the context of the application.

#     :param app: Flask app
#     :return: Celery app
#     """
#     app = app or create_app()

#     celery = Celery(app.import_name, include=CELERY_TASK_LIST)
#     celery.conf.update(app.config)
#     TaskBase = celery.Task

#     class ContextTask(TaskBase):
#         abstract = True

#         def __call__(self, *args, **kwargs):
#             with app.app_context():
#                 return TaskBase.__call__(self, *args, **kwargs)

#     celery.Task = ContextTask

#     return celery


# def create_app(config=get_config()):
#     """
#     Create a Flask application using the app factory pattern.

#     :return: Flask app
#     """
#     app = Flask(__name__)
#     app.config.from_object(config)

#     register_blueprints(app)

#     initialize_extensions(app)

#     if not app.debug:
#         configure_logging(app)

#     register_error_handlers(app)

#     # specify additional actions on app teardown
#     app_teardown(app)

#     return app


# def initialize_extensions(app):
#     """ Register 0 or more extensions (mutates the app passed in).

#     :param app: Flask application instance
#     :return: None
#     """
#     # init csrf
#     # csrf.init_app(app)


# def register_blueprints(app):
#     """ Register 0 or more blueprints (mutates the app passed in).

#     :param app: Flask application instance
#     :return: None
#     """
#     from app.blueprints import api
#     app.register_blueprint(api)


# def configure_logging(app):
#     """ Register and configure logging (mutates the app passed in).

#     :param app: Flask application instance
#     :return: None
#     """
#     # Set up logger
#     if not os.path.exists('logs'):  # pragma: no cover
#         os.mkdir('logs')

#     app.logger.removeHandler(default_handler)
#     file_handler = RotatingFileHandler('logs/app.log', maxBytes=10240,
#                                        backupCount=10)
#     file_handler.setFormatter(logging.Formatter(
#         '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))
#     file_handler.setLevel(logging.INFO)
#     app.logger.addHandler(file_handler)

#     app.logger.setLevel(logging.INFO)
#     app.logger.info('project startup')


# def register_error_handlers(app):
#     """ Register error handlers (mutates the app passed in).

#     :param app: Flask application instance
#     :return: None
#     """
#     # TODO: create templates for errorhandlers
#     # @app.errorhandler(400)
#     # def bad_request(e):
#     #     return render_template('404.html'), 400

#     # @app.errorhandler(403)
#     # def forbidden(e):
#     #     return render_template('404.html'), 403

#     @ app.errorhandler(404)
#     def page_not_found(e):
#         return render_template('404.html'), 404

#     # @app.errorhandler(405)
#     # def method_not_allowed(e):
#     #     return render_template('404.html'), 405

#     # @app.errorhandler(500)
#     # def server_error(e):
#     #     return render_template('404.html'), 500


# def app_teardown(app):
#     """ Register actions executed at app teardown

#     :param app: Flask application instance
#     :return: None
#     """
#     @ app.teardown_appcontext
#     def shutdown_session(exception=None):
#         session.remove()
