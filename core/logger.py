import os, logging
from logging.handlers import TimedRotatingFileHandler

base_dir = 'logs'

# Создаем базовую директорию
os.makedirs(base_dir, exist_ok=True)

logger_config = {
	'version': 1,
	'disable_existing_loggers': False,
	'formatters': {
		'cli_format': {
			'format': '{asctime} - {levelname} - {name} - {module}:{funcName}:{lineno} - {message}',
			'style': '{'
		},
	},
	'handlers': {
		'console': {
			'class': 'logging.StreamHandler',
			'level': 'DEBUG',
			'formatter': 'cli_format'
		},
        'tool': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'level': 'DEBUG',
            'filename': 'logs/tool.log',
            'when': 'W0',
            'interval': 1,
            'backupCount': 4,
            'formatter': 'cli_format'
        },
		'test_loading_csv': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'level': 'DEBUG',
            'filename': 'logs/test_loading_csv.log',
            'when': 'W0',
            'interval': 1,
            'backupCount': 4,
            'formatter': 'cli_format'
        },
	},
	'loggers': {
        'tool_logger': {
			'level': 'CRITICAL',
			'handlers': ['tool'],
			'propagate': False
		},
        'test_loading_csv_logger': {
			'level': 'CRITICAL',
			'handlers': ['test_loading_csv'],
			'propagate': False
		}
	},
}