
import six
from abc import ABCMeta, abstractmethod, abstractproperty


@six.add_metaclass(ABCMeta)
class BlockStorageDriver(object):

    """Defines an abstract class for implementing a block storage
    driver for Deuce. The block storage driver is only responsible
    for storing and retrieving individual blocks and has no notion
    of files (files exist only as a notion in the metadata layer).
    """

    @abstractmethod
    def block_exists(self, request_headers, vault_id, block_id):
        """Determines if the specified block exists in the vault.

        :param request_headers: The HTTP request header,
            may have project_id, storage URL and token
        :param vault_id: The ID of the vault to check
        :param block_id: The ID of the block to check"""
        raise NotImplementedError

    @abstractmethod
    def create_vault(self, request_headers, vault_id):
        """Allocates space in the storage backend for the specified
        vault ID

        :param request_headers: The HTTP request header,
            may have project_id, storage URL and token
        :param vault_id: The ID of the vault"""
        raise NotImplementedError

    @abstractmethod
    def delete_vault(self, request_headers, vault_id):
        """Deletes the storage allocation for this vault.

        :param request_headers: The HTTP request header,
            may have project_id, storage URL and token
        :param vault_id: The ID of the vault to delete
        :pre: The vault must be empty
        """
        raise NotImplementedError

    @abstractmethod
    def vault_exists(self, request_headers, vault_id):
        """Determines if block space has been allocated for the
        specified vault_id

        :param request_headers: The HTTP request header,
            may have project_id, storage URL and token
        :param vault_id: The ID of the vault to check for"""
        raise NotImplementedError

    @abstractmethod
    def get_vault_statistics(self, request_headers, vault_id):
        """Return the statistics on the vault.

        :param request_headers: The HTTP request header,
            may have project_id, storage URL and token
        "param vault_id: The ID of the vault to gather statistics for"""
        raise NotImplementedError

    @abstractmethod
    def get_block_obj(self, request_headers, vault_id, block_id):
        """Returns a single file-like object

        :param request_headers: The HTTP request header,
            may have project_id, storage URL and token
        :param vault_id: The ID of the vault
        :param block_id: The ID of the block"""

        raise NotImplementedError

    @abstractmethod
    def store_block(self, request_headers, vault_id, block_id,
            block_data):
        """Stores the block into the specified vault

        :param request_headers: The HTTP request header,
            may have project_id, storage URL and token
        :param vault_id: The ID of the vault
        :param block_id: The ID of the block
        :param block_data: The data body of the block"""
        raise NotImplementedError

    @abstractmethod
    def block_exists(self, request_headers, vault_id, block_id):
        """Determines if the specified block exists in the
        vault.

        :param request_headers: The HTTP request header,
            may have project_id, storage URL and token
        :param vault_id: The ID of the vault
        :param block_id: The ID of the block"""

        raise NotImplementedError

    @abstractmethod
    def delete_block(self, request_headers, vault_id, block_id):
        """Deletes the specified block from storage

        :param request_headers: The HTTP request header,
            may have project_id, storage URL and token
        :param vault_id: The ID of the vault
        :param block_id: The ID of the block"""

        raise NotImplementedError

    def create_blocks_generator(self, request_headers, vault_id, block_gen):
        """Returns a generator of file-like objects that are
        ready to read. These objects will get closed
        individually.

        :param request_headers: The HTTP request header,
            may have project_id, storage URL and token
        :param vault_id: The ID of the vault
        :param block_gen: The ID list of the blocks"""

        return (self.get_block_obj(request_headers, vault_id, block_id)
            for block_id in block_gen)
