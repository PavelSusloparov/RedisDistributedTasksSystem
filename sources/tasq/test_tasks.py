__all__ = ['TestTask', 'DivisionTask', 'ArrayTask',
            'MultiplicationTask', 'AddTask']
__revision__ = '$Id'

import random
from . import Task

class TestTaskException(Exception):
    pass

class TestTask(Task):
    def __init__(self):
        Task.__init__(self)
        self.failure_percent = 40
        self.set_guaranteed(True)
        self.set_workers(('localhost', 2))

    def process(self, value):
        failure = random.randint(1, 100) < self.failure_percent
        if failure:
            raise TestTaskException('Artificially failed task')
        else:
            return value


class DivisionTask(Task):
    def __init__(self):
        Task.__init__(self)

    def process(self, value):
        result = int(value) / 2
        return result


class ArrayTask(Task):
    def __init__(self):
        Task.__init__(self)

    def process(self, value):
        return value


class MultiplicationTask(Task):
    def __init__(self):
        Task.__init__(self)

    def process(self, value):
        result = int(value) * 2
        return result


class AddTask(Task):
    def __init__(self):
        Task.__init__(self)
        self.add_value = 100

    def process(self, value):
        result = int(value) + self.add_value
        return result
