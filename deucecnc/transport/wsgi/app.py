import deucecnc.util.log as logging
from deucecnc.transport.wsgi import Driver

app_container = Driver()
logging.setup()
app = app_container.app
