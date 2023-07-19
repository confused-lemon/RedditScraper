import os; path = os.path.dirname(os.path.dirname(__file__))
from datetime import datetime; timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

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
    },
    'loggers': {
        'root': {  # root logger
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': False
        }
    }
}
