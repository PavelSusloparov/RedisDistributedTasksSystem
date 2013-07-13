__all__ = ['ERROR', 'WARNING', 'INFO', 'DEBUG',
           'WARNING_MONITOR', 'DEBUG_MONITOR']
__revision__ = '$Id'

import logging
from ..utils import load_config

redis_config = load_config('redis')
monitor_config = load_config('monitor')
MONDB = True if monitor_config['mondb'] == '+' else False

formatter = logging.Formatter('%(asctime)-15s %(levelname)s: %(message)s')

manager_logger = logging.getLogger('manager')
hdlr = logging.FileHandler(redis_config['manager_service_log'])
hdlr.setFormatter(formatter)
manager_logger.addHandler(hdlr)
manager_logger.setLevel(redis_config['manager_log_level'])

conn_logger = logging.getLogger('connection')
hdlr = logging.FileHandler(redis_config['redis_connection_log'])
hdlr.setFormatter(formatter)
conn_logger.addHandler(hdlr)
conn_logger.setLevel(redis_config['redis_log_level'])

def ERROR_MONITOR(data):
    conn_logger.error('{}'.format(data))

def WARNING_MONITOR(data):
    conn_logger.warning('{}'.format(data))

def INFO_MONITOR(data):
    conn_logger.info('{}'.format(data))

def DEBUG_MONITOR(data):
    conn_logger.debug('{}'.format(data))

def ERROR(data):
    manager_logger.error('{}'.format(data))

def WARNING(data):
    manager_logger.warning('{}'.format(data))

def INFO(data):
    manager_logger.info('{}'.format(data))

def DEBUG(data):
    manager_logger.debug('{}'.format(data))
