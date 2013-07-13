__all__ = ['RedisManagerEvent', 'RedisTasqEvent', 'exception']
__revision__ = '$Id'

try:
    from tesla import event
    from tesla.event.exception import exception

except ImportError:

    class event(object):
        """
        Fake tesla.event module.
        All events are dropped if no tesla package available.
        """

        @staticmethod
        def record(event):
            pass

        class Event(object):
            pass

    def exception(*args, **kwargs):
        pass


class RedisManagerEvent(event.Event):
    """
    MonDb event for ha_manager.
    """
    table_name = 'redis_manager_events'
    db_fields = [
        ('replication_chain', '%(replication_chain)s'),
        ('process_affected', '%(process_affected)d'),
        ('event_type', '%(event_type)s'),
        ('process_type', '%(process_type)s')
    ]

    def __init__(self):
        super(RedisManagerEvent, self).__init__()
        self.replication_chain = None
        self.process_affected = None
        self.event_type = None
        self.process_type = None

    def record(self):
        event.record(self)
        self.__init__()


class RedisTasqEvent(event.Event):
    """
    MonDb event for tasq.
    """
    table_name = 'redis_tasq_events'
    db_fields = [
        ('workers', '%(workers)s'),
        ('task_queues', '%(task_queues)s'),
        ('db_size', '%(db_size)d'),
        ('master_node', '%(master_node)s')
    ]

    def __init__(self):
        super(RedisTasqEvent, self).__init__()
        self.workers = None
        self.task_queues = None
        self.db_size = None
        self.master_node = None

    def record(self):
        event.record(self)
        self.__init__()