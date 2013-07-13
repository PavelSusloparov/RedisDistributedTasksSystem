from redis.tasq.test_tasks import MultiplicationTask as Task

t = Task()
input_values = range(10, 11)
t.enqueue(input_values, chord=True, sync=True)
t.solve()