from ..app import create_celery_app
from ..config import config_from_env

config = config_from_env()

celery_app = create_celery_app(config)
