
import re

VAULT_ID_MAX_LEN = 128
VAULT_ID_REGEX = re.compile('^[a-zA-Z0-9_\-]+$')

BLOCK_ID_REGEX = re.compile('\\b[0-9a-f]{40}\\b')

FILE_ID_REGEX = re.compile(
    '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')


class ValidationFailed(ValueError):

    """User input was inconsistent with API restrictions"""

    def __init__(self, msg, *args, **kwargs):
        msg = msg.format(*args, **kwargs)
        super(ValidationFailed, self).__init__(msg)


def validate_vault_id(value):
    if not VAULT_ID_REGEX.match(value):
        raise ValidationFailed('Invalid vault id {0}'.format(value))

    if len(value) > VAULT_ID_MAX_LEN:
        raise ValidationFailed('Vault ID exceeded max len {0}'.format(
            VAULT_ID_MAX_LEN))


def validate_block_id(value):
    if not BLOCK_ID_REGEX.match(value):
        raise ValidationFailed('Invalid Block ID {0}'.format(value))


def validate_file_id(value):
    if not FILE_ID_REGEX.match(value):
        raise ValidationFailed('Invalid File ID {0}'.format(value))
