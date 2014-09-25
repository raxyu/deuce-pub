"""
Deuce Authentication API
"""
import abc
import json
import requests
import logging
import datetime
import time


# TODO: Add a base Auth class
# TODO: Convert below to use KeystoneClient

class AuthenticationError(Exception):
    pass


class AuthCredentialsErrors(AuthenticationError):
    pass


class AuthExpirationError(AuthenticationError):
    pass


class AuthenticationBase(object):
    """
    Authentication Interface Class
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, userid=None, usertype=None,
                 credentials=None, auth_method=None,
                 datacenter=None, auth_url=None):
        """
        :param userid:  string - User Identifier, e.g username, userid, etc.)
        :param usertype: string - Type of User Identifier
                         values: user_id, user_name, tenant_name, tenant_id
        :param credentials: string - User Credentials for Authentication
                                     e.g. password
        :param auth_method: string - Type of User Credentials
                                     values: apikey, password, token

        :param auth_url: string - Authentication Service URL to use
        :param datacenter: string - Datacenter to autheniticate in
                                    e.g. identity.rackspace.com
        """
        if userid is None:
            raise AuthenticationError(
                'Required Parameter, userid, not specified.')

        if usertype is None:
            raise AuthenticationError(
                'Required Parameter, usertype, not specified.')

        if credentials is None:
            raise AuthenticationError(
                'Required Parameter, credentials, not specified.')

        if auth_method is None:
            raise AuthenticationError(
                'Required Parameter, auth_method, not specified.')

        self.__catalog = {}
        self.__catalog['user'] = userid
        self.__catalog['usertype'] = usertype
        self.__catalog['credentials'] = credentials
        self.__catalog['auth_method'] = auth_method
        self.__catalog['datacenter'] = datacenter
        self.__catalog['auth_url'] = auth_url

    @property
    def userid(self):
        """Return the User Identifier used for authentication
        """
        return self.__catalog['user']

    @property
    def usertype(self):
        """Return the type of user credentials used for authencation
        """
        return self.__catalog['usertype']

    @property
    def credentials(self):
        """Return the User Crentidals used for authentication
        """
        return self.__catalog['credentials']

    @property
    def authmethod(self):
        """Return the Authentication Method used for authentication
        """
        return self.__catalog['auth_method']

    @property
    def datacenter(self):
        """Return the Datacenter (region) authentication was performed against
        """
        return self.__catalog['datacenter']

    @property
    def authurl(self):
        """Return the Authentication URL used for authentication
        """
        return self.__catalog['auth_url']

    @abc.abstractmethod
    def GetToken(self, retry=5):
        """Retrieve an Authentication Token

        :param retry: integer - number of times to retry getting a token
        :returns: string - authentication token or None if failure
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def IsExpired(self, fuzz=0):
        """Has the token expired

        :param fuzz: integer - number of seconds to add to the current time
                               to determine if it the authentication will
                               expire in a given time frame
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def AuthToken(self):
        """Access the current AuthToken, retrieving it if necessary

        :returns: string - authentication token
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def AuthExpirationTime(self):
        """Retrieve the time at which the token will expire

        :returns: datetime.datetime - date/time the token expires at
        """
        raise NotImplementedError()
