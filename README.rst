======================
tarantool-deque-python
======================

Python bindings for `Tarantool delayed queue <https://github.com/dreadatour/tarantool-deque/>`_ (Tarantool 1.6 ONLY).

Library depends on:

* tarantool>0.4

Basic usage can be found in tests. Description on every command is in source code.

Big thanks to `Eugine Blikh <https://github.com/bigbes>`_, `Dmitriy Shveenkov <https://github.com/shveenkov/>`_ and `Alexandr Emelin <https://github.com/FZambia/>`_.

For install of latest "stable" version type:

.. code-block:: bash

    # using pip
    $ pip install tarantool-deque
    # or using easy_install
    $ easy_install tarantool-deque

For install bleeding edge type:

.. code-block:: bash

    $ pip install git+https://github.com/dreadatour/tarantool-deque-python.git

For configuring Deque in `Tarantool <http://tarantool.org>`_ read manual `Here <https://github.com/dreadatour/tarantool-deque>`_.

Then just **import** it, create **Deque**, create **Tube**, **put** and **take** some elements:

.. code-block:: python

    >>> from tarantool_queue import Deque
    >>> deque = Deque('localhost', 33013, user='test', password='test')
    >>> tube = deque.tube('name_of_tube')
    >>> tube.put([1, 2, 3])  # put new task in queue
    >>> task = tube.take()  # take task from queue
    >>> task.data  # read data from it
    [1, 2, 3]
    >>> task.ack()  # move this task into state DONE
    True
