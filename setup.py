# -*- coding: utf-8 -*-
try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

REQUIRES = ['six', 'pecan', 'setuptools >= 1.1.6',
    'cassandra-driver', 'pymongo', 'python-keystoneclient',
    'python-swiftclient']

setup(
    name='deuce',
    version='0.1',
    description='Deuce - Block-level de-duplication as-a-service',
    author='Rackspace',
    author_email='',
    install_requires=REQUIRES,
    test_suite='deuce',
    zip_safe=False,
    include_package_data=True,
    packages=find_packages(exclude=['tests'])
)
