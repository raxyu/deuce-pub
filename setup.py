# -*- coding: utf-8 -*-
import sys
try:
    from distutils.core import setup
    from setuptools import find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages
else:
    REQUIRES = ['configobj', 'falcon', 'six', 'setuptools >= 1.1.6',
                'cassandra-driver', 'pymongo', 'msgpack-python',
                'python-swiftclient', 'asyncio', 'aiohttp']
    setup(
        name='deucecnc',
        version='0.1',
        description='',
        license='Apache License 2.0',
        url='',
        author='Rackspace',
        author_email='',
        include_package_data=True,
        install_requires=REQUIRES,
        test_suite='deucecnc',
        zip_safe=False,
        entry_points={
            'console_scripts': [
                'deuce-cnc-server = deucecnc.cmd.server:run',
            ]
        },
        packages=find_packages(exclude=['tests*'])
    )
