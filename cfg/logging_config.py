import os; path = os.path.dirname(os.path.dirname(__file__))
from datetime import datetime, timezone
timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
date = datetime.now(timezone.utc).date().strftime('%Y%m%d')

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {
            'format': '%(asctime)s : [%(levelname)s] : %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.FileHandler',
            'filename' : f'{path}/logs/{timestamp}_error_report.log',
            'mode' : 'a',
            'delay' : True
        },
        'upload' : {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.FileHandler',
            'filename' : f'{path}/Error_Files/{date}_upload_error_report.log',
            'mode' : 'a',
            'delay' : True
        }
    },
    'loggers': {
        'collector': {  # root logger
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': False
        },
        'uploader' : {
            'handlers' : ['upload'],
            'level' : 'WARN',
            'propagate' : False
        }
    }
}
