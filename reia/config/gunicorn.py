# -*- coding: utf-8 -*-

import os
from distutils.util import strtobool

bind = f"0.0.0.0:{os.getenv('PORT', '8000')}"
accesslog = "-"
access_log_format = "%(h)s %(l)s %(u)s %(t)s '%(r)s' %(s)s %(b)s '%(f)s' '%(a)s' in %(D)sµs"  # noqa: E501

workers = int(os.getenv("WEB_CONCURRENCY", 4))
threads = int(os.getenv("WEB_THREADS", 2))
timeout = 600
reload = bool(strtobool(os.getenv("WEB_RELOAD", "false")))
