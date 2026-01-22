#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-intercom',
      version='2.2.4',
      description='Singer.io tap for extracting data from the Intercom API',
      author='jeff.huth@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_intercom'],
      install_requires=[
          'backoff==2.2.1',
          'requests==2.32.4',
          'singer-python==6.0.1'
      ],
      entry_points='''
          [console_scripts]
          tap-intercom=tap_intercom:main
      ''',
      packages=find_packages(),
      package_data={
          'schemas': ['tap_intercom/schemas/*.json']
       },
       include_package_data=True,
      extras_require={
          'dev': [
              'pylint',
              'ipdb',
              'parameterized'
          ]
      }
      )
