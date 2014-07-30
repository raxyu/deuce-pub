

import os
import io
import shutil
from swiftclient.exceptions import ClientException
import hashlib

container_path = '/tmp/swift_mocking'


def _get_vault_path(vault_id):
    return os.path.join(container_path, vault_id)


def _get_block_path(vault_id, block_id):
    vault_path = _get_vault_path(vault_id)
    return os.path.join(vault_path, str(block_id))


# Create Vault
def put_container(url,
            token,
            container,
            response_dict):
    path = _get_vault_path(container)
    if not os.path.exists(path):
        shutil.os.makedirs(path)
    else:
        raise ClientException('mocking')

    block_path = os.path.join(path, 'blocks')
    if not os.path.exists(block_path):
        shutil.os.makedirs(block_path)
    response_dict['status'] = 201


# Check Vault
def head_container(url,
            token,
            container):
    path = _get_vault_path(container)
    if os.path.exists(path):
        response_headers = {}
        response_headers['content-type'] = 'mocking_ret'
        response_headers['x-container-bytes-used'] = 0
        response_headers['x-container-object-count'] = 0
        # If we really wanted to be pendantic about this field
        # then we'd set this to zero and find the epoch to 5 decimals
        # on each file below and take the latest (max) value between
        # them all
        response_headers['x-timestamp'] = 987654321.12345

        total_size = 0
        object_count = 0
        for root, dirs, files in os.walk(path):
            total_size = total_size + sum(
                os.path.getsize(
                    os.path.join(root, name)) for name in files)
            object_count = object_count + len(files)

        response_headers['x-container-bytes-used'] = total_size
        response_headers['x-container-object-count'] = object_count

        return response_headers
    else:
        raise ClientException('mocking')


# Delete Vault
def delete_container(url,
            token,
            container,
            response_dict):
    try:
        path = _get_vault_path(container)
        blockpath = os.path.join(path, 'blocks')
        if os.listdir(path) == [] or os.listdir(blockpath) == []:
            shutil.rmtree(path)
            response_dict['status'] = 201
        else:
            raise ClientException('mocking')
    except:
        raise ClientException('mocking')


# Store Block
def put_object(url,
            token,
            container,
            name,
            contents,
            content_length,
            response_dict,
            etag=None):

    blocks_path = os.path.join(_get_vault_path(container), 'blocks')
    if not os.path.exists(blocks_path):
        raise ClientException('mocking')

    path = _get_block_path(container, name)

    with open(path, 'wb') as outfile:
        outfile.write(contents)

    mdhash = hashlib.md5()
    mdhash.update(contents)
    response_dict['status'] = 201
    return mdhash.hexdigest()


# Check Block
def head_object(url,
            token,
            container,
            name):

    path = _get_block_path(container, name)
    if not os.path.exists(path):
        raise ClientException('mocking')
    return 'mocking_ret'


# Delete Block
def delete_object(url,
            token,
            container,
            name,
            response_dict):
    path = _get_block_path(container, name)
    if os.path.exists(path):
        os.remove(path)
        response_dict['status'] = 201
    else:
        raise ClientException('mocking')


# Get Block
def get_object(url,
            token,
            container,
            name,
            response_dict):
    path = _get_block_path(container, name)

    if not os.path.exists(path):
        raise ClientException('mocking')

    buff = ""
    with open(path, 'rb') as infile:
        buff = infile.read()

    return dict(), buff


def get_keystoneclient_2_0(auth_url,
            user,
            key,
            os_options):
    if user == 'failing_auth_hook':
        raise ClientException('mocking auth failure')
    return 'mocking_project_id', 'mocking_project_token'
