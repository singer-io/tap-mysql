#!/usr/bin/env python

from setuptools import setup

setup(name='tap-mysql',
      version='0.1.2',
      description='Singer.io tap for extracting data from MySQL',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_mysql'],
      install_requires=[
          'attrs==16.3.0',
          'pendulum==1.2.0',
          'singer-python==1.7.0',
          'PyMySQL==0.7.11',
      ],
      entry_points='''
          [console_scripts]
          tap-mysql=tap_mysql:main
      ''',
      packages=['tap_mysql'],
)
