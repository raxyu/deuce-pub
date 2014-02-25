
import six
from abc import ABCMeta, abstractmethod, abstractproperty

# Note: calling NotImplementedError in each abstract method
# is to enable 100% code coverage when testing


@six.add_metaclass(ABCMeta)
class MetadataStorageDriver(object):
    """MetadataStorageDriver is an abstract base class that
    defines all functions necessary for a Deuce metadata
    driver.
    """

    @abstractmethod
    def create_file(self, project_id, vault_id, file_id):
        """Creates a representation of an empty file."""
        raise NotImplementedError

    @abstractmethod
    def delete_file(self, project_id, vault_id, file_id):
        """Deletes the file from storage."""
        raise NotImplementedError

    @abstractmethod
    def has_file(self, project_id, vault_id, file_id):
        """Determines if the specified file exists in the vault."""
        raise NotImplementedError

    @abstractmethod
    def finalize_file(self, project_id, vault_id, file_id):
        """Finalizes a file that has been de-duped. This
        check ensures that all blocks have been marked have
        been uploaded and that there are no 'gaps' in the
        metadata that comprise the file."""
        raise NotImplementedError

    @abstractmethod
    def is_finalized(self, project_id, vault_id, file_id):
        """Determines if this file has been finalized"""
        raise NotImplementedError

    @abstractmethod
    def create_block_generator(self, project_id, vault_id,
            marker=0, limit=0):
        """Creates and returns a generator that will return
        the ID of each block file. The file must previously
        have been finalized."""
        raise NotImplementedError

    @abstractmethod
    def create_file_generator(self, project_id, vault_id,
            marker=0, limit=0, finalized=True):
        """Creates and returns a generator that will return
        the ID of each block file. The file must previously
        have been finalized."""
        raise NotImplementedError

    @abstractmethod
    def create_file_block_generator(self, project_id, vault_id, file_id,
            offset=0, limit=0):
        """Creates and returns a generator that will return
        the ID of each block contained in the specified
        file. The file must previously have been finalized."""
        raise NotImplementedError

    @abstractmethod
    def has_block(self, project_id, vault_id, block_id):
        """Determines if the vault has the specified block."""
        raise NotImplementedError

    @abstractmethod
    def assign_block(self, project_id, vault_id, file_id, block_id, offset):
        """Assigns the specified block to a particular offset in
        the file. No check is performed as to whether or not the
        block overlaps (it can't be done since a block that doesn't
        yet exist in storage can be assigned to a file).

        :param vault_id: The vault containing the file
        :param file_id: The ID of the file
        :param block_id: The ID of the block being assigned to the file
        :param offset: The position of the block in"""
        raise NotImplementedError

    @abstractmethod
    def register_block(self, project_id, vault_id, block_id, size):
        """Registers a block in the metadata driver."""
        raise NotImplementedError

    @abstractmethod
    def unregister_block(self, project_id, vault_id, block_id):
        """Unregisters (removes) the block from the metadata
        store"""
        raise NotImplementedError
