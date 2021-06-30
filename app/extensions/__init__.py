from flask_wtf.csrf import CSRFProtect
from flask_assets import Environment

csrf = CSRFProtect()
assets = Environment()
