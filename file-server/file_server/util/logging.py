import logging
import sys
import traceback

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

def log_exception_stack():
    exc_info = sys.exc_info()
    if exc_info is None or len(exc_info) < 3:
        return
    tb = exc_info[2]
    if tb is None:
        return
    try:
        # Try to log it properly.
        ss = traceback.extract_stack(tb)
        logging.error(''.join(ss.format()))
    except:
        # Fallback.
        traceback.print_tb(tb)