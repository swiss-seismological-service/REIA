import logging.config
import os

os.makedirs('logs', exist_ok=True)
logging.config.fileConfig('logging.conf')
