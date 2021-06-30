from app import create_app
from app import create_celery_app
app = create_app()

celery = create_celery_app(app)
