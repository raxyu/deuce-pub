# -*- coding: utf-8 -*-
try:
    from distutils.core import setup
    from setuptools import find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

REQUIRES = ['six', 'pecan', 'setuptools >= 1.1.6',
    'cassandra-driver', 'pymongo']

setup(
    name='deuce',
    version='0.1',
    description='Deuce - Block-level de-duplication as-a-service',
    license='',
    url='',
    author='Rackspace',
    author_email='',
    include_package_data=True,
    install_requires=REQUIRES,
    test_suite='deuce',
    zip_safe=False,
    data_files=[('bin', ['config.py']),
                ('bin/templates',
                    ['deuce/templates/error.html',
                    'deuce/templates/index.html',
                    'deuce/templates/layout.html']),
                ('bin/public/css',
                    ['deuce/public/css/style.css']),
                ('bin/public/images',
                    ['deuce/public/images/logo.png'])],
    packages=find_packages(exclude=['tests'])
)
