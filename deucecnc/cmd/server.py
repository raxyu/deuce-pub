from deucecnc.common import cli
from deucecnc.transport.wsgi.driver import Driver


@cli.runnable
def run():
    app_container = Driver()
    app_container.listen()
