"""
Tests for tarantool deque.
"""
import threading
import unittest

import tarantool
from tarantool_deque import Deque


class YetAnotherTarantoolConnection(tarantool.Connection):
    """
    Overriden tarantool connection.
    """
    pass


class FakeGoodConnection(object):
    """
    Good fake tarantool connection object.
    """
    def __init__(self):
        pass

    def call(self):
        pass


class FakeGoodLock(object):
    """
    Good fake lock object.
    """
    def __enter__(self):
        pass

    def __exit__(self):
        pass


class BadFake(object):
    """
    Bad fake for tarantool connection and lock objects.
    """
    pass


class DequeBaseTestCase(unittest.TestCase):
    """
    Base test case for deque tests.
    """
    def setUp(self):
        # create new tarantool connection before each test
        self.deque = Deque('127.0.0.1', 33016, user='test', password='test')


class QueueConnectionTestCase(DequeBaseTestCase):
    def test_connection(self):
        # get deque tarantool connection
        connection = self.deque.tnt

        # check if deque tarantool connection is the same
        self.assertEqual(connection, self.deque.tnt)

        # check if deque tarantool connection by default
        # is `tarantool.connection.Connection` instanse
        self.assertIsInstance(connection, tarantool.connection.Connection)
        self.assertIsInstance(self.deque.tnt, tarantool.connection.Connection)

    def test_connection_reset(self):
        # deque `_tnt` attribute must be None by default (right after connect)
        self.assertIsNone(self.deque._tnt)
        # touch deque `tnt` property (this will setup deque `_tnt` attribute)
        self.assertIsNotNone(self.deque.tnt)
        # deque `_tnt` attribute is not None now
        self.assertIsNotNone(self.deque._tnt)

        # set deque custom tarantool connection
        self.deque.tarantool_connection = YetAnotherTarantoolConnection
        # deque connection must be `YetAnotherTarantoolConnection` after setup
        self.assertEqual(self.deque.tarantool_connection,
                         YetAnotherTarantoolConnection)

        # deque `_tnt` attribute must be None after setup tarantool connection
        self.assertIsNone(self.deque._tnt)
        # touch deque `tnt` property (this will setup deque `_tnt` attribute)
        self.assertIsNotNone(self.deque.tnt)
        # deque `_tnt` attribute is not None now
        self.assertIsNotNone(self.deque._tnt)

        # reset deque tarantool connection
        self.deque.tarantool_connection = None
        # deque connection must be `tarantool.Connection` by default
        self.assertEqual(self.deque.tarantool_connection,
                         tarantool.Connection)

        # deque `_tnt` attribute must be None after reset tarantool connection
        self.assertIsNone(self.deque._tnt)
        # touch deque `tnt` property (this will setup deque `_tnt` attribute)
        self.assertIsNotNone(self.deque.tnt)
        # deque `_tnt` attribute is not None now
        self.assertIsNotNone(self.deque._tnt)

    def test_connection_good(self):
        # set custom fake tarantool connection
        self.deque.tarantool_connection = FakeGoodConnection
        # deque tarantoo connection must be `FakeGoodConnection` after setup
        self.assertEqual(self.deque.tarantool_connection,
                         FakeGoodConnection)

        # reset deque tarantool connection
        self.deque.tarantool_connection = None
        # deque tarantool connection must be `tarantool.Connection` by default
        self.assertEqual(self.deque.tarantool_connection,
                         tarantool.Connection)

    def test_connection_bad(self):
        # if we will try to set bad tarantool connection for deque it will fall
        with self.assertRaises(TypeError):
            self.deque.tarantool_connection = BadFake

        # if we will try to set bad tarantool connection for deque it will fall
        with self.assertRaises(TypeError):
            self.deque.tarantool_connection = lambda x: x

        # deque tarantool connection must be `tarantool.Connection` anyway
        self.assertEqual(self.deque.tarantool_connection,
                         tarantool.Connection)


class QueueLockTestCase(DequeBaseTestCase):
    def setUp(self):
        # create new tarantool connection before each test
        self.deque = Deque('127.0.0.1', 33013, user='test', password='test')

    def test_lock_reset(self):
        # deque tarantool lock is `threading.Lock` by default
        self.assertIsInstance(self.deque.tarantool_lock,
                              type(threading.Lock()))

        # set deque tarantool lock to `threading.RLock`
        self.deque.tarantool_lock = threading.RLock()
        # deque tarantool lock is `threading.RLock` now
        self.assertIsInstance(self.deque.tarantool_lock,
                              type(threading.RLock()))

        # reset deque tarantool lock
        self.deque.tarantool_lock = None
        # deque tarantool lock is `threading.Lock` by default after reset
        self.assertIsInstance(self.deque.tarantool_lock,
                              type(threading.Lock()))

    def test_lock_good(self):
        # set custom fake deque tarantool lock
        self.deque.tarantool_lock = FakeGoodLock()
        # deque tarantool lock is `FakeGoodLock` now
        self.assertIsInstance(self.deque.tarantool_lock,
                              FakeGoodLock)

        # reset deque tarantool lock
        self.deque.tarantool_lock = None
        # deque tarantool lock is `threading.Lock` by default after reset
        self.assertIsInstance(self.deque.tarantool_lock,
                              type(threading.Lock()))

    def test_lock_bad(self):
        # if we will try to set bad tarantool lock for deque it will fall
        with self.assertRaises(TypeError):
            self.deque.tarantool_lock = BadFake

        # if we will try to set bad tarantool lock for deque it will fall
        with self.assertRaises(TypeError):
            self.deque.tarantool_lock = BadFake()

        # if we will try to set bad tarantool lock for deque it will fall
        with self.assertRaises(TypeError):
            self.deque.tarantool_lock = lambda x: x

        # deque tarantool lock is `threading.Lock` anyway
        self.assertIsInstance(self.deque.tarantool_lock,
                              type(threading.Lock()))
