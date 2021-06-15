from . import celery_app
import time


def init_celery(app, celery):
    celery.name = app.import_name
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask


@celery_app.task(name='app.tasks.test')
def test():
    print('starting task')
    time.sleep(5)
    print('task done')
