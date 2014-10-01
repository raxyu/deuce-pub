from deucecnc.model.tasks import Tasks
from deucecnc.model.list import List
from deucecnc.model.register import Register
from deucecnc.model.workers import WorkerValidation, WorkerCleanup
import importlib
from deucecnc import conf
import deucecnc


deucecnc.db_driver = None


def _load_driver(classname):
    """Creates of the instance of the specified
    class given the fully-qualified name. The module
    is dynamically imported.
    """
    pos = classname.rfind('.')
    module_name = classname[:pos]
    class_name = classname[pos + 1:]

    mod = importlib.import_module(module_name)
    return getattr(mod, class_name)(conf.cnc_driver.db_module)


def init_model():
    deucecnc.db_driver = _load_driver(conf.cnc_driver.driver)
