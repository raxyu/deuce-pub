
import six
from abc import ABCMeta, abstractmethod, abstractproperty

# Note: calling NotImplementedError in each abstract method
# is to enable 100% code coverage when testing


class OverlapError(Exception):
    """OverlapError is raised when finalizing
    a file is attempted but is not possible
    because two blocks overlap each other in
    the file"""
    def __init__(self, project_id, vault_id, file_id, block_id,
            startpos, endpos):
        """Creates a new OverlapError Exception

        :param vault_id: The vault containing the file
        :param file_id: The file containing the overlap
        :param block_id: The ID of the overlapping block
        :param startpos: The first overlapped byte
        :param endpos: The last overlapped block
        """
        self.project_id = project_id
        self.vault_id = vault_id
        self.file_id = file_id
        self.block_id = block_id
        self.startpos = startpos
        self.endpos = endpos

        msg = "[{0}/{1} Overlap at block {2} file {3} at [{4}-{5}]".format(
            project_id, vault_id, block_id, file_id, startpos, endpos)

        Exception.__init__(self, msg)


class GapError(Exception):
    """GapError is raised becasue a file can
    not be finalized because a portion of the
    file is not covered by a block"""
    def __init__(self, project_id, vault_id, file_id, startpos, endpos):
        """Creates a new OverlapError Exception

        :param vault_id: The vault containing the file
        :param file_id: The file containing the overlap
        :param startpos: The first position of the detected gap
        :param endpos: The last position of the detected gap
        """
        self.project_id = project_id
        self.vault_id = vault_id
        self.file_id = file_id
        self.startpos = startpos
        self.endpos = endpos

        msg = "[{3}\{4}] Gap in file {0} from {1}-{2}".format(
            file_id, startpos, endpos, project_id, vault_id)

        Exception.__init__(self, msg)


@six.add_metaclass(ABCMeta)
class MetadataStorageDriver(object):
    """MetadataStorageDriver is an abstract base class that
    defines all functions necessary for a Deuce metadata
    driver.
    """

    @abstractmethod
    def create_vaults_generator(self, project_id,
            marker=None, limit=None):
        """Creates and returns a generator that will return
        the vault IDs.

        :param project_id: The Project ID for this block
        :param marker: The vault_id to start of the list
        :param limit: Number of returned items
        """
        raise NotImplementedError

    @abstractmethod
    def create_vault(self, project_id, vault_id):
        """Creates a representation of a vault."""
        raise NotImplementedError

    @abstractmethod
    def delete_vault(self, project_id, vault_id):
        """Deletes the vault from metadata."""
        raise NotImplementedError

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
    def finalize_file(self, project_id, vault_id, file_id, file_size=None):
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
            marker=None, limit=None):
        """Creates and returns a generator that will return
        the ID of each block file. The file must previously
        have been finalized."""
        raise NotImplementedError

    @abstractmethod
    def create_file_generator(self, project_id, vault_id,
            marker=None, limit=None, finalized=True):
        """Creates and returns a generator that will return
        the ID of each block file. The file must previously
        have been finalized."""
        raise NotImplementedError

    @abstractmethod
    def create_file_block_generator(self, project_id, vault_id, file_id,
            offset=None, limit=None):
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
    def get_block_data(self, project_id, vault_id, block_id):  # TODO: rename
        """Returns the size of the block"""
        raise NotImplementedError

    @abstractmethod
    def unregister_block(self, project_id, vault_id, block_id):
        """Unregisters (removes) the block from the metadata
        store"""
        raise NotImplementedError
