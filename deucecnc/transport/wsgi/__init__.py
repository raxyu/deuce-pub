"""WSGI Transport Driver"""

from deucecnc.transport.wsgi import driver

# Hoist into package namespace
print ('yud transport wsgi init to create a driver.... ')
Driver = driver.Driver
