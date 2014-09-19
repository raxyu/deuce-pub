from model.task import Tasks
from drivers.cassandra import cassandradriver
import importlib


Tasks.driver = None


def _load_driver(classname):
    """Creates of the instance of the specified
    class given the fully-qualified name. The module
    is dynamically imported.
    """
    pos = classname.rfind('.')
    module_name = classname[:pos]
    class_name = classname[pos + 1:]

    mod = importlib.import_module(module_name)
    return getattr(mod, class_name)()


def init_model():
    Tasks.driver = _load_driver(
        'drivers.cassandra.cassandradriver.CassandraDriver')
