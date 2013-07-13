__all__ = ['ERROR', 'WARNING', 'INFO', 'DEBUG', 'MONITOR']
__revision__ = '$Id'

import logging
from ..utils import load_config

tasq_config = load_config('tasq')

formatter = logging.Formatter('%(asctime)-15s %(levelname)s: %(message)s')

tasq_logger = logging.getLogger('tasq')
hdlr = logging.FileHandler(tasq_config['tasq_service_log'])
hdlr.setFormatter(formatter)
tasq_logger.addHandler(hdlr)
tasq_logger.setLevel(tasq_config['tasq_log_level'])

monitoring_logger = logging.getLogger('monitoring')
hdlr = logging.FileHandler(tasq_config['monitor_log'])
hdlr.setFormatter(formatter)
monitoring_logger.addHandler(hdlr)
monitoring_logger.setLevel(tasq_config['monitor_log_level'])

def MONITOR(data):
    monitoring_logger.info('{}'.format(data))

def ERROR(data):
    tasq_logger.error('{}'.format(data))

def WARNING(data):
    tasq_logger.warning('{}'.format(data))

def INFO(data):
    tasq_logger.info('{}'.format(data))

def DEBUG(data):
    tasq_logger.debug('{}'.format(data))
