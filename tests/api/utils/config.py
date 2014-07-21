from cafe.engine.models.data_interfaces import ConfigSectionInterface


class deuceConfig(ConfigSectionInterface):
    """
    Defines the config values for deuce
    """
    SECTION_NAME = 'deuce'

    @property
    def base_url(self):
        """
        deuce endpoint
        """
        return self.get('base_url')

    @property
    def use_auth(self):
        """
        authenticate the user connection
        """
        return self.get_boolean('use_auth')

    @property
    def version(self):
        """
        deuce api version
        """
        return self.get('version')


class authConfig(ConfigSectionInterface):
    """
    Defines the auth config values
    """
    SECTION_NAME = 'auth'

    @property
    def base_url(self):
        """
        auth endpoint
        """
        return self.get('base_url')

    @property
    def user_name(self):
        """
        user account
        """
        return self.get('user_name')

    @property
    def api_key(self):
        """
        user's api key
        """
        return self.get('api_key')

    @property
    def tenant_id(self):
        """
        user's tenant id
        """
        return self.get('tenant_id')
