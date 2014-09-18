"""WSGI Transport Driver"""

from transport.wsgi import driver

# Hoist into package namespace
Driver = driver.Driver
