
import logging

import colorlog


def get_logger(name: str, level=logging.INFO):
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        fmt='%(prefix_log_color)s%(asctime)s %(log_color)s%(levelname)s %(prefix_log_color)s[%(filename)s %(funcName)s:%(lineno)d] %(message_log_color)s- %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            "DEBUG": "light_white",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
        },
        secondary_log_colors={
		    'message': {
                'INFO': 'white',
                'WARNING': 'white',
			    'ERROR':    'red',
			    'CRITICAL': 'red'
		    },
            'prefix': {
                'INFO': 'light_cyan',
                'WARNING': 'light_cyan',
			    'ERROR':    'light_cyan',
			    'CRITICAL': 'light_cyan'
            }
	    }
    ))

    logger = colorlog.getLogger(name)
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger

