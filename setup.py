#!/usr/bin/env python
# -*- coding: utf-8 -*-
import codecs
import os
import re

from setuptools import find_packages, setup


def read(*parts):
    """
    Read file content and return it as string.
    """
    filename = os.path.join(os.path.dirname(__file__), *parts)
    with codecs.open(filename, encoding='utf-8') as fp:
        return fp.read()


def find_version(*file_paths):
    """
    Find package version from file.
    """
    version_file = read(*file_paths)
    version_match = re.search(r"""^__version__\s*=\s*(['"])(.+?)\1""",
                              version_file, re.M)
    if version_match:
        return version_match.group(2)
    raise RuntimeError("Unable to find version string.")


setup(
    name='tarantool-deque',
    version=find_version('src', 'tarantool_deque', '__init__.py'),
    license='MIT',
    description='Python bindings for Tarantool delayed queue script',
    long_description=read('README.rst'),
    author='Vladimir Rudnyh',
    author_email='rudnyh@corp.mail.ru',
    url='https://github.com/dreadatour/tarantool-deque-python',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    install_requires=[
        'tarantool>0.4'
    ],
    tests_require=[
        'tarantool>0.4'
    ],
    test_suite='tests',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database :: Front-Ends',
        'Environment :: Console'
    ]
)
