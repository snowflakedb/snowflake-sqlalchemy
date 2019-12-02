#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#

from os import path
from setuptools import setup
from codecs import open

THIS_DIR = path.dirname(path.realpath(__file__))

try:
    from generated_version import VERSION
except:
    from version import VERSION
version = '.'.join([str(v) for v in VERSION if v is not None])

with open(path.join(THIS_DIR, 'DESCRIPTION.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='snowflake-sqlalchemy',
    version=version,
    description='Snowflake SQLAlchemy Dialect',
    long_description=long_description,
    author='Snowflake Computing, Inc',
    author_email='support@snowflake.net',
    license='Apache License, Version 2.0',
    url='https://www.snowflake.net/',
    keywords="Snowflake db database cloud analytics warehouse",
    download_url='https://www.snowflake.net/',
    use_2to3=False,

    install_requires=[
        'sqlalchemy<2.0.0',
        'snowflake-connector-python',
    ],
    namespace_packages=[
        'snowflake'
    ],
    packages=[
        'snowflake.sqlalchemy',
    ],
    package_dir={
        'snowflake.sqlalchemy': '.',
    },
    package_data={
        'snowflake.sqlalchemy': ['LICENSE.txt'],
    },
    entry_points={
        'sqlalchemy.dialects': [
            'snowflake=snowflake.sqlalchemy:dialect',
        ]
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',

        'Environment :: Console',
        'Environment :: Other Environment',

        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',

        'License :: OSI Approved :: Apache Software License',

        'Operating System :: OS Independent',

        'Programming Language :: SQL',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',

        'Topic :: Database',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Scientific/Engineering :: Information Analysis',
    ],
    extras_requires={
        'development': [
            'pytest==4.6.6',  # Last Python 2.7 supported version
            'pytest-cov',
            'pytest-rerunfailures',
            'pytest-timeout',
            'coverage',
            'pexpect',
            'mock',
            'pytz',
            'pytzdata',
            'Cython',
            'more-itertools==4.3.0;python_version=="2.7"',  # Last Python 2.7 supported version
            'more-itertools;python_version!="2.7"',
            'numpy==1.16.5;python_version=="2.7"',  # Last Python 2.7 supported version
            'numpy;python_version!="2.7"',
            'pandas==0.24.2;python_version=="2.7" or python_version=="3.5"',
            'pandas<1.0.0;python_version>"3.5"',
        ]
    },
)
