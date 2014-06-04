
# TODO: Need implementation read/write/delete to the local disk.

from swiftclient.exceptions import ClientException


def put_container(url,
            token,
            container,
            response_dict):
    if container == 'notmatter':
        raise ClientException('mocking')
    response_dict['status'] = 201


def head_container(url,
            token,
            container):
    if container == 'notmatter':
        raise ClientException('mocking')
    return 'mocking_ret'


def delete_container(url,
            token,
            container,
            response_dict):
    if container == 'notmatter':
        raise ClientException('mocking')

    response_dict['status'] = 201


def put_object(url,
            token,
            container,
            name,
            contents,
            content_length,
            response_dict):
    if container == 'notmatter':
        raise ClientException('mocking')
    response_dict['status'] = 201


def head_object(url,
            token,
            container,
            name):
    if container == 'notmatter':
        raise ClientException('mocking')
    return 'mocking_ret'


def delete_object(url,
            token,
            container,
            name,
            response_dict):
    if container == 'notmatter':
        raise ClientException('mocking')
    response_dict['status'] = 201


def get_object(url,
            token,
            container,
            name,
            response_dict):
    if container == 'notmatter':
        raise ClientException('mocking')
    return 'mock_ret', 'mock_ret'
