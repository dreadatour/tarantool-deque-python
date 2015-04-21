"""
Tests for tarantool delayed queue tube.
"""
import time
import unittest

from tarantool_deque import Deque


class TubeBaseTestCase(unittest.TestCase):
    """
    Base test case for deque tube tests.
    """
    tube_name = 'test_tube'

    @classmethod
    def setUpClass(cls):
        # connect to tarantool once
        cls.deque = Deque('127.0.0.1', 33016, user='test', password='test')

        # connect to test tube
        cls.tube = cls.deque.tube(cls.tube_name)

    def _cleanup_tube(self):
        """
        Delete all tasks in all deque tubes.
        """
        for tube in self.deque.tubes.values():
            task = tube.take(timeout=0)
            while task:
                task.delete()
                task = tube.take(timeout=0)

    def setUp(self):
        # delete all tasks in all tubes
        self._cleanup_tube()

    def tearDown(self):
        # report all tasks in all deque tubes as executed
        self._cleanup_tube()


class TubeTestCase(TubeBaseTestCase):
    """
    Tests for tarantool delayed queue tube.
    """
    def test_tube(self):
        # get test tube and check it is equal with setuped tube
        tube1 = self.deque.tube(self.tube_name)
        self.assertEqual(self.tube, tube1)

        # get test tube again and check it is equal with setuped tube
        tube2 = self.deque.tube(self.tube_name)
        self.assertEqual(self.tube, tube2)

        # check if all tubes are equal
        self.assertEqual(tube1, tube2)

    def test_tasks_order(self):
        # put few tasks in tube
        task11 = self.tube.put('foo')
        task21 = self.tube.put('bar')
        task31 = self.tube.put('baz')

        # take all these tasks back
        task12 = self.tube.take(timeout=0)
        task22 = self.tube.take(timeout=0)
        task32 = self.tube.take(timeout=0)

        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

        # check if tasks order is correct (first in, first out)
        self.assertEqual(task11.task_id, task12.task_id)
        self.assertEqual(task11.data, task12.data)
        self.assertEqual(task12.data, 'foo')

        self.assertEqual(task21.task_id, task22.task_id)
        self.assertEqual(task21.data, task22.data)
        self.assertEqual(task22.data, 'bar')

        self.assertEqual(task31.task_id, task32.task_id)
        self.assertEqual(task31.data, task32.data)
        self.assertEqual(task32.data, 'baz')

        # ack all tasks
        self.assertTrue(task12.ack())
        self.assertTrue(task22.ack())
        self.assertTrue(task32.ack())

        # still no tasks in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_status(self):
        # put new task in tube
        task = self.tube.put('foo')
        # this task status is 'ready'
        self.assertEqual(task.status, 'r')
        self.assertEqual(task.status_name, 'ready')

        # take this task from tube
        task = self.tube.take(timeout=0)
        # task was taken
        self.assertTrue(task)
        # task status is 'taken'
        self.assertEqual(task.status, 't')
        self.assertEqual(task.status_name, 'taken')

        # ack this task
        self.assertTrue(task.ack())
        # task status is 'done' now
        self.assertEqual(task.status, '-')
        self.assertEqual(task.status_name, 'done')

    def test_task_status_delayed(self):
        # put new task in tube set task 'delay' to 0.1 second
        task = self.tube.put('foo', delay=0.1)
        # this task status is 'delayed'
        self.assertEqual(task.status, '~')
        self.assertEqual(task.status_name, 'delayed')

        # sleep more than 0.1 second - task will be ready soon
        time.sleep(0.2)

        # this task status is 'ready' now
        task.peek()
        self.assertEqual(task.status, 'r')
        self.assertEqual(task.status_name, 'ready')

        # take this task from tube
        task = self.tube.take(timeout=0)
        # release task and set 'delay' to 0.1 second
        self.assertTrue(task.release(delay=0.1))
        # this task status is 'delayed'
        self.assertEqual(task.status, '~')
        self.assertEqual(task.status_name, 'delayed')

        # sleep more than 0.1 second - task will be ready soon
        time.sleep(0.2)

        # this task status is 'ready' now
        task.peek()
        self.assertEqual(task.status, 'r')
        self.assertEqual(task.status_name, 'ready')

        # take and ack this task
        self.assertTrue(self.tube.take(timeout=0).ack())
        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_put_delay(self):
        # put new task in tube; set task 'delay' to 0.1 second
        task = self.tube.put('foo', delay=0.1)
        # task status is 'delayed'
        self.assertEqual(task.status, '~')
        # no tasks yet in tube (delay is working)
        self.assertIsNone(self.tube.take(timeout=0))

        # sleep more than 0.2 second - task will be ready soon
        time.sleep(0.2)
        # take and ack this task
        self.assertTrue(self.tube.take(timeout=0).ack())

        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_put_ttl(self):
        # put new task in tube; set task 'time to live' to one second
        self.tube.put('foo', ttl=1)
        # take and ack this task
        self.assertTrue(self.tube.take(timeout=0).ack())

        # put new task in tube; set task 'time to live' to zero (dead task)
        self.tube.put('foo', ttl=0)
        # no tasks will be taken from tube (task is dead)
        self.assertIsNone(self.tube.take(timeout=0))

        # put new task in tube; set task 'time to live' to 0.1 second (float)
        self.tube.put('foo', ttl=0.1)
        # sleep more than 0.1 second
        time.sleep(0.2)
        # no tasks will be taken from tube (task is dead after 'ttl' seconds)
        self.assertIsNone(self.tube.take(timeout=0))

        # put new task in tube; set task 'time to live' to 1 second (integer)
        self.tube.put('foo', ttl=1)
        # sleep more than 1 second
        time.sleep(1.1)
        # no tasks will be taken from tube (task is dead after 'ttl' seconds)
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_put_ttl_delay(self):
        # put new task in tube
        # set task 'time to live' to 0.2 second and delay to 0.1 second
        task = self.tube.put('foo', ttl=0.2, delay=0.1)
        # this task status is 'delayed'
        self.assertEqual(task.status, '~')

        # sleep more than 0.1 second, but less, than 0.2 second
        time.sleep(0.15)

        # this task status is 'ready' now
        task.peek()
        self.assertEqual(task.status, 'r')

        # sleep more (0.1 second)
        time.sleep(0.15)

        # this task status is dead now (after 'ttl' + 'delay' seconds)
        with self.assertRaises(Deque.DatabaseError):
            task.peek()

        # no tasks will be taken (task is dead after 'ttl' + 'delay' seconds)
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_take(self):
        # no tasks in tube within 'timeout' seconds (integer)
        time_start = time.time()
        self.assertIsNone(self.tube.take(timeout=1))
        self.assertTrue(1 <= time.time() - time_start < 2)

        # no tasks in tube within 'timeout' seconds (float)
        time_start = time.time()
        self.assertIsNone(self.tube.take(timeout=.3))
        self.assertTrue(.3 <= time.time() - time_start < .5)

        # put new task in tube
        self.tube.put('foo')

        # take this task from tube
        task = self.tube.take(timeout=0)
        # this task status is 'taken'
        self.assertEqual(task.status, 't')

        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

        # ack this task
        self.assertTrue(task.ack())

    def test_task_ack(self):
        # put new task in tube
        self.tube.put('foo')
        # take this task from tube
        task = self.tube.take(timeout=0)

        # ack this task
        self.assertTrue(task.ack())
        # this task status is 'done'
        self.assertEqual(task.status, '-')

        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_release(self):
        # put new task in tube
        self.tube.put('foo')
        # take this task from tube
        task = self.tube.take(timeout=0)

        # release task
        self.assertTrue(task.release())
        # task status is 'ready'
        self.assertEqual(task.status, 'r')

        # take this task from tube
        task = self.tube.take(timeout=0)
        # this task status is 'taken'
        self.assertEqual(task.status, 't')

        # ack this task
        self.assertTrue(task.ack())
        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_release_delay(self):
        # put new task in tube
        self.tube.put('foo')
        # take this task from tube
        task = self.tube.take(timeout=0)

        # release task and set 'delay' to 0.1 second
        self.assertTrue(task.release(delay=0.1))
        # task status is 'delayed'
        self.assertEqual(task.status, '~')
        # no tasks yet in tube (delay is working)
        self.assertIsNone(self.tube.take(timeout=0))

        # sleep more than 0.1 second - task will be ready soon
        time.sleep(0.2)
        # take and ack this task
        self.assertTrue(self.tube.take(timeout=0).ack())

        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_peek(self):
        # put new task in tube
        self.tube.put('foo')

        # take this task from tube
        task = self.tube.take(timeout=0)
        # task status is now 'taken'
        self.assertEqual(task.status, 't')

        # release this task (this way, yes, we need to keep task status)
        self.deque.release(self.tube, task.task_id)

        # task status is still 'taken'
        self.assertEqual(task.status, 't')
        # peek task
        self.assertTrue(task.peek())
        # task status is now 'ready'
        self.assertEqual(task.status, 'r')

        # take this task from tube
        task = self.tube.take(timeout=0)
        # ack this task
        self.assertTrue(task.ack())
        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_delete(self):
        # put new task in tube
        self.tube.put('foo')
        # take this task from tube
        task = self.tube.take(timeout=0)

        # delete task
        self.assertTrue(task.delete())

        # task status is 'done'
        self.assertEqual(task.status, '-')

        # tack cannot be acked
        with self.assertRaises(Deque.DatabaseError):
            task.ack()

        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_destructor(self):
        # put new task in tube
        task = self.tube.put('foo')

        # task is not taken - must not send exception
        task.__del__()

        # take this task from tube
        task = self.tube.take(timeout=0)

        # task in taken - must be released and not send exception
        task.__del__()

        # take this task back from tube
        task = self.tube.take(timeout=0)
        # ack this task
        self.assertTrue(task.ack())
        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))
