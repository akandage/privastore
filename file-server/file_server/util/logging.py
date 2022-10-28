import logging

def config_logging(log_level):
    if log_level == 'CRITICAL':
        log_level = logging.CRITICAL
    elif log_level == 'ERROR':
        log_level = logging.ERROR
    elif log_level == 'WARN':
        log_level = logging.WARN
    elif log_level == 'INFO':
        log_level = logging.INFO
    elif log_level == 'DEBUG':
        log_level = logging.DEBUG
    
    # TODO: Configurability of log message format.
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(thread)d:%(funcName)s:%(filename)s:%(lineno)d - %(message)s', level=log_level)