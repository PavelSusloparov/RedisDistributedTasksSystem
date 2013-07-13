The system has 3 subsustem:

 1) Distributed task queue system working with Redis key-value store.

 2) Redis system replications.

 3) Reliable connection for Redis key-value store.


Distributed task queue system working with Redis key-value store.

    'tasq' folder has distributed task queue system.
    configuration is in config/tasq.conf

    System has sufficiency functionality for working with redis.
    More info about distributed task queue system on celery example: http://celeryproject.org/

    The main goal of distributed task queue is scalability and tasks control.

Redis system replications.

    'manager' folder has redis system replication.
    configuration is in config/redis.conf

    Replication system for Redis.
    The main goal that redis has main node and many replications.
    When main node is down, replication node is used.
    When main replication is down, second replication node is used.


Reliable connection for Redis key-value store.

    'connection' folder has redis system replication.
    configuration is in config/redis.conf

    Safe connection for redis. Connect with redis system replication.
    Use for safe data pipe on production.