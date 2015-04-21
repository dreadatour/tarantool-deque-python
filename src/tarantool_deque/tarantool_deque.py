# -*- coding: utf-8 -*-
"""
Python bindings for Tarantool delayed queue script.

See also: https://github.com/dreadatour/tarantool-deque-python
"""
import threading

import tarantool


TASK_STATUS = {
    '~': 'delayed',
    'r': 'ready',
    't': 'taken',
    '-': 'done',
}


class Task(object):
    """
    Tarantool deque task wrapper.
    """
    def __init__(self, tube, task_id, status, data):
        self.tube = tube
        self.deque = tube.deque
        self.task_id = task_id
        self.status = status
        self.data = data

    def __str__(self):
        return "Task (id: {0}, status: {1})".format(self.task_id,
                                                    self.status_name)

    def __del__(self):
        if self.status == 't':
            try:
                self.release()
            except self.deque.DatabaseError:
                pass

    @property
    def status_name(self):
        """
        Returns status full name.
        """
        return TASK_STATUS.get(self.status, 'UNKNOWN')

    @classmethod
    def create_from_tuple(cls, tube, the_tuple):
        """
        Create task from tuple.

        Returns `Task` instance.
        """
        if the_tuple is None:
            return

        if not the_tuple.rowcount:
            raise Deque.ZeroTupleException("Error creating task")

        row = the_tuple[0]

        return cls(tube, task_id=row[0], status=row[1], data=row[2])

    def update_from_tuple(self, the_tuple):
        """
        Update task from tuple.
        """
        if not the_tuple.rowcount:
            raise Deque.ZeroTupleException("Error updating task")

        row = the_tuple[0]

        self.status = row[1]
        self.data = row[2]

    def ack(self):
        """
        Report task successful execution.

        Returns `True` is task is acked (task status is 'done' now).
        """
        the_tuple = self.deque.ack(self.tube, self.task_id)

        self.update_from_tuple(the_tuple)

        return bool(self.status == '-')

    def release(self, delay=None):
        """
        Put the task back into the deque.

        May contain a possible new `delay` before the task is executed again.

        Returns `True` is task is released (task status is 'ready'
        or 'delayed' if `delay` is set now).
        """
        the_tuple = self.deque.release(self.tube, self.task_id, delay=delay)

        self.update_from_tuple(the_tuple)

        if delay is None:
            return bool(self.status == 'r')
        else:
            return bool(self.status == '~')

    def peek(self):
        """
        Look at a task without changing its state.

        Always returns `True`.
        """
        the_tuple = self.deque.peek(self.tube, self.task_id)

        self.update_from_tuple(the_tuple)

        return True

    def delete(self):
        """
        Delete task (in any state) permanently.

        Returns `True` is task is deleted.
        """
        the_tuple = self.deque.delete(self.tube, self.task_id)

        self.update_from_tuple(the_tuple)

        return bool(self.status == '-')


class Tube(object):
    """
    Tarantol deque tube wrapper.
    """
    def __init__(self, deque, name):
        self.deque = deque
        self.name = name

    def cmd(self, cmd_name):
        """
        Returns tarantool deque command name for current tube.
        """
        return 'deque.tube.{0}:{1}'.format(self.name, cmd_name)

    def put(self, data, ttl=None, delay=None):
        """
        Enqueue a task.

        Returns a `Task` object.
        """
        cmd = self.cmd('put')
        args = (data,)

        params = dict()
        if ttl is not None:
            params['ttl'] = ttl
        if delay is not None:
            params['delay'] = delay
        if params:
            args += (params,)

        the_tuple = self.deque.tnt.call(cmd, args)

        return Task.create_from_tuple(self, the_tuple)

    def take(self, timeout=None):
        """
        Get a task from deque for execution.

        Waits `timeout` seconds until a READY task appears in the deque.

        Returns either a `Task` object or `None`.
        """
        the_tuple = self.deque.take(self, timeout=timeout)

        if the_tuple.rowcount:
            return Task.create_from_tuple(self, the_tuple)

    def drop(self):
        """
        Drop entire query (if there are no in-progress tasks or workers).
        """
        return self.deque.drop(self)


class Deque(object):
    """
    Tarantool deque wrapper.

    Usage:

        >>> from tarantool_deque import Deque
        >>> deque = Deque('127.0.0.1', 33013, user='test', password='test')
        >>> tube = deque.tube('delayed_queue')
        # Put tasks into the deque
        >>> tube.put([1, 2, 3])
        >>> tube.put([2, 3, 4])
        # Get tasks from deque
        >>> task1 = tube.take()
        >>> task2 = tube.take()
        >>> print(task1.data)
            [1, 2, 3]
        >>> print(task2.data)
            [2, 3, 4]
        # Release tasks (put them back to deque)
        >>> del task2
        >>> del task1
        # Take task again
        >>> print(tube.take().data)
            [1, 2, 3]
        # Take task and mark it as complete
        >>> tube.take().ack()
            True
    """
    DatabaseError = tarantool.DatabaseError
    NetworkError = tarantool.NetworkError

    class BadConfigException(Exception):
        """
        Bad config deque exception.
        """
        pass

    class ZeroTupleException(Exception):
        """
        Zero tuple deque exception.
        """
        pass

    def __init__(self, host='localhost', port=33013, user=None, password=None):
        if not host or not port:
            raise Deque.BadConfigException(
                "Host and port params must be not empty"
            )

        if not isinstance(port, int):
            raise Deque.BadConfigException("Port must be int")

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.tubes = {}
        self._lockinst = threading.Lock()
        self._conclass = tarantool.Connection
        self._tnt = None

    @property
    def tarantool_connection(self):
        """
        Tarantool connection getter.

        Uses `tarantool.Connection` by default.
        """
        if self._conclass is None:
            self._conclass = tarantool.Connection
        return self._conclass

    @tarantool_connection.setter
    def tarantool_connection(self, con_cls):
        """
        Tarantool connection setter.

        Must be class with methods call and __init__.
        """
        if con_cls is None:
            self._conclass = tarantool.Connection
        elif 'call' in dir(con_cls) and '__init__' in dir(con_cls):
            self._conclass = con_cls
        else:
            raise TypeError("Connection class must have connect"
                            " and call methods or be None")
        self._tnt = None

    @property
    def tarantool_lock(self):
        """
        Tarantool lock getter.

        Use `threading.Lock` by default.
        """
        if self._lockinst is None:
            self._lockinst = threading.Lock()

        return self._lockinst

    @tarantool_lock.setter
    def tarantool_lock(self, lock):
        """
        Tarantool lock setter.

        Must be locking instance with methods __enter__ and __exit__.
        """
        if lock is None:
            self._lockinst = threading.Lock()
        elif '__enter__' in dir(lock) and '__exit__' in dir(lock):
            self._lockinst = lock
        else:
            raise TypeError("Lock class must have `__enter__`"
                            " and `__exit__` methods or be None")

    @property
    def tnt(self):
        """
        Get or create tarantool connection.
        """
        if self._tnt is None:
            with self.tarantool_lock:
                if self._tnt is None:
                    self._tnt = self.tarantool_connection(
                        self.host,
                        self.port,
                        user=self.user,
                        password=self.password
                    )
        return self._tnt

    def take(self, tube, timeout=None):
        """
        Get a task from deque for execution.

        Waits `timeout` seconds until a READY task appears in the deque.
        If `timeout` is `None` - waits forever.

        Returns tarantool tuple object.
        """
        cmd = tube.cmd('take')
        args = ()

        if timeout is not None:
            args += (timeout,)

        return self.tnt.call(cmd, args)

    def ack(self, tube, task_id):
        """
        Report task successful execution.

        Ack is accepted only from the consumer, which took the task
        for execution. If a consumer disconnects, all tasks taken
        by this consumer are put back to READY state (released).

        Returns tarantool tuple object.
        """
        cmd = tube.cmd('ack')
        args = (task_id,)

        return self.tnt.call(cmd, args)

    def release(self, tube, task_id, delay=None):
        """
        Put the task back into the deque.

        Used in case of a consumer for any reason can not execute a task.
        May contain a possible new `delay` before the task is executed again.

        Returns tarantool tuple object.
        """
        cmd = tube.cmd('release')
        args = (task_id,)

        if delay is not None:
            args += ({'delay': delay},)

        return self.tnt.call(cmd, args)

    def peek(self, tube, task_id):
        """
        Look at a task without changing its state.

        Returns tarantool tuple object.
        """
        cmd = tube.cmd('peek')
        args = (task_id,)

        return self.tnt.call(cmd, args)

    def delete(self, tube, task_id):
        """
        Delete task (in any state) permanently.

        Returns tarantool tuple object.
        """
        cmd = tube.cmd('delete')
        args = (task_id,)

        return self.tnt.call(cmd, args)

    def drop(self, tube):
        """
        Drop entire query (if there are no in-progress tasks or workers).

        Returns `True` on successful drop.
        """
        cmd = tube.cmd('drop')
        args = ()

        the_tuple = self.tnt.call(cmd, args)

        return bool(the_tuple.return_code == 0)

    def tube(self, name):
        """
        Create tube object, if not created before.

        Returns `Tube` object.
        """
        tube = self.tubes.get(name)

        if tube is None:
            tube = Tube(self, name)
            self.tubes[name] = tube

        return tube
