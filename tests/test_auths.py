
"""
Using authbase.py and openstackauth.py to talk to Openstack identity.
"""

import falcon
import json
from tests import V1Base

from deucecnc.util import authbase
from deucecnc.util import openstackauth


class TestAuths(V1Base):

    def test_authbase(self):

        auth = self.assertRaises(authbase.AuthenticationError,
            lambda: authbase.AuthenticationBase())
        auth = self.assertRaises(authbase.AuthenticationError,
            lambda: authbase.AuthenticationBase(userid='userid'))
        auth = self.assertRaises(authbase.AuthenticationError,
            lambda: authbase.AuthenticationBase(userid='userid',
            usertype='tenant_name'))
        auth = self.assertRaises(authbase.AuthenticationError,
            lambda: authbase.AuthenticationBase(userid='userid',
            usertype='tenant_name', credentials='credentials'))

        auth = authbase.AuthenticationBase(
            userid='userid', usertype='tenant_name',
            credentials='credentials', auth_method='password',
            datacenter='datacenter', auth_url='auth_url')

        self.assertEqual(auth.userid, 'userid')
        self.assertEqual(auth.usertype, 'tenant_name')
        self.assertEqual(auth.credentials, 'credentials')
        self.assertEqual(auth.authmethod, 'password')
        self.assertEqual(auth.datacenter, 'datacenter')
        self.assertEqual(auth.authurl, 'auth_url')

    def test_openstackauth(self):
        auth = self.assertRaises(authbase.AuthenticationError,
            lambda: openstackauth.OpenStackAuthentication())
        auth = openstackauth.OpenStackAuthentication(
            userid='quattrodev', usertype='wrong type',
            credentials='9d9877aaf81bb63e2ef784ed980232cb',
            auth_method='apikey',
            datacenter='',
            auth_url='https://auth.api.rackspacecloud.com/v2.0')
        client = self.assertRaises(authbase.AuthenticationError,
            lambda: auth.get_client())

        auth = openstackauth.OpenStackAuthentication(
            userid='quattrodev', usertype='tenant_name',
            credentials='9d9877aaf81bb63e2ef784ed980232cb',
            auth_method='wrong type',
            datacenter='',
            auth_url='https://auth.api.rackspacecloud.com/v2.0')
        client = self.assertRaises(authbase.AuthenticationError,
            lambda: auth.get_client())

        auth = openstackauth.OpenStackAuthentication(
            userid='quattrodev', usertype='tenant_name',
            credentials='9d9877aaf81bb63e2ef784ed980232cb',
            auth_method='token',
            datacenter='',
            auth_url='https://auth.api.rackspacecloud.com/v2.0')
        # client = auth.get_client()

        auth = openstackauth.OpenStackAuthentication(
            userid='quattrodev', usertype='tenant_name',
            credentials='9d9877aaf81bb63e2ef784ed980232cb',
            auth_method='apikey',
            datacenter='',
            auth_url='https://auth.api.rackspacecloud.com/v2.0')
        # client = auth.get_client()

        auth = openstackauth.OpenStackAuthentication(
            userid='quattrodev', usertype='tenant_id',
            credentials='9d9877aaf81bb63e2ef784ed980232cb',
            auth_method='apikey',
            datacenter='',
            auth_url='https://auth.api.rackspacecloud.com/v2.0')
        # client = auth.get_client()

        auth = openstackauth.OpenStackAuthentication(
            userid='quattrodev', usertype='user_name',
            credentials='9d9877aaf81bb63e2ef784ed980232cb',
            auth_method='apikey',
            datacenter='',
            auth_url='https://auth.api.rackspacecloud.com/v2.0')
        # client = auth.get_client()

        # token = auth.GetToken()
        # ttl = auth.IsExpired()
        # res = auth._AuthToken()
        # time = auth.AuthExpirationTime()
