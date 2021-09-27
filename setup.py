#!/usr/bin/env python

from setuptools import setup

setup(name='tap-mysql',
      version='1.17.5',
      description='Singer.io tap for extracting data from MySQL',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_mysql'],
      install_requires=[
          'attrs==16.3.0',
          'pendulum==1.2.0',
          'singer-python==5.9.0',
          'PyMySQL==0.9.3',
          'backoff==1.8.0',
          'mysql-replication==0.22',
      ],
      extras_require={
          'dev': ['pylint==2.8.3']
      },
      entry_points='''
          [console_scripts]
          tap-mysql=tap_mysql:main
      ''',
      packages=['tap_mysql', 'tap_mysql.sync_strategies'],
)
