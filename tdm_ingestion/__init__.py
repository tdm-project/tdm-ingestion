import logging
import logging.config


LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'format': '{asctime} - {name} - {levelname} - {message}',
            'style': '{'
        }
    },
    'handlers': {
        'console': {
            'level': logging.DEBUG,
            'class': 'logging.StreamHandler',
            'formatter': 'default',
        },
    },
    'loggers': {
        'tdm_ingestion': {
            'handlers': ['console'],
            'level': logging.DEBUG,
            'propagate': True
        }
    }
}

logging.config.dictConfig(LOGGING)
