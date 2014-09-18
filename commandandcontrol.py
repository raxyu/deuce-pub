import logging
import falcon
import hooks.authhook
from controllers.controller_auth import Auth
from controllers.controller_register import Register, UnRegister, DebugList
from drivers.cassandra import CassandraDriver


# falcon.API instances are callable WSGI apps
app = falcon.API(before=[hooks.authhook.AuthHook])

driver = CassandraDriver()
register = Register(driver)
unregister = UnRegister(driver)

debuglist = DebugList(driver)

auth = Auth()

app.add_route('/register/{project_id}/{vault_id}', register)
app.add_route('/unregister/{project_id}/{vault_id}', unregister)
app.add_route('/debuglist/{project_id}', debuglist)
app.add_route('/auth/{project_id}', auth)
