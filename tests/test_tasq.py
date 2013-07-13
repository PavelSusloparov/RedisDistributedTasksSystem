from redis.tasq.test_tasks import TestTask, DivisionTask, ArrayTask, AddTask, MultiplicationTask
import time
import unittest

class TestTasqFunctional(unittest.TestCase):

    def setUp(self):
        self.input_values = range(10, 20)

    def test_failed_tasks(self):
        t_task = TestTask()
        t_task.enqueue(self.input_values, chord=True, sync=True)
        t_task_result = t_task.solve()
        self.assertNotEqual(t_task_result, None)

    def test_user_chord_init(self):
        di_task = DivisionTask()
        di_task.enqueue(self.input_values, chord='TEST_CHORD', sync=True)
        di_task_result = di_task.solve()
        self.assertNotEqual(di_task_result, None)

    def test_tasks_by_one(self):
        a_task = ArrayTask()
        a_task.enqueue(self.input_values, sync=True)
        a_task_result = a_task.solve()
        self.assertNotEqual(a_task_result, None)
        self.assertEqual(type(a_task_result), list)

    def test_tasks_chord(self):
        add_task = AddTask()
        add_task.enqueue(self.input_values, chord=True, sync=True)
        add_task_result = add_task.solve()
        self.assertNotEqual(add_task_result, None)

    def test_is_sync(self):
        m_task = MultiplicationTask()
        m_task.enqueue(self.input_values, chord=True, sync=True)
        m_result = m_task.solve()
        self.assertNotEqual(m_result, None)


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTasqFunctional)
    unittest.TextTestRunner(verbosity=2).run(suite)
