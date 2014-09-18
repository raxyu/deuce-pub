import util.log as logging
from transport.wsgi import Driver

app_container = Driver()
logging.setup()
app = app_container.app
