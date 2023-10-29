import datetime
import difflib
import logging
import posixpath

import zyn.errors
import zyn.exception


def timestamp_to_datetime(timestamp):
    return datetime.datetime.utcfromtimestamp(timestamp)


def get_logger(name, level):
    logger = logging.getLogger(name)
    logging.basicConfig(
        format='ZynClient %(asctime)-15s %(filename)s:%(lineno)s %(levelname)s: %(message)s',
    )
    logger.setLevel(level)
    return logger


def check_server_response(rsp):
    if rsp.is_error():
        desc = zyn.errors.error_to_string(rsp.error_code())
        raise zyn.exception.ZynServerException(rsp.error_code(), desc)


def normalized_remote_path(path):
    if path is None or not path:
        raise ValueError('Empty path')
    path = path.replace('\\', '/')
    remote_path = posixpath.normpath(path)
    if remote_path.startswith('//'):
        remote_path = remote_path[1:]
    return remote_path


def split_remote_path(path):
    if '//' in path:
        raise ValueError('Split path must be normalized')
    slash = None
    if path.endswith('/'):
        slash = '/'
        path = path[:-1]
    path_1, path_2 = posixpath.split(path)
    if not path_1 or not path_2:
        raise ValueError('Path could not be split, path="{}"'.format(path))
    if slash is not None:
        path_2 += slash
    return path_1, path_2


def join_remote_paths(list_of_paths):
    path = posixpath.normpath('/'.join(list_of_paths))
    if path.startswith('//'):
        path = path[1:]
    return path


def unhandled(msg=None):
    if msg is not None:
        raise RuntimeError(msg)
    raise RuntimeError()


def edit_random_access_file(
        connection,
        node_id,
        revision,
        content_original,
        content_edited,
        logger,
):

    remote_index_offset = 0
    differ = difflib.SequenceMatcher(None, content_original, content_edited)

    for type_of_change, i1, i2, j1, j2 in differ.get_opcodes():

        logger.debug(
            ('type="{}", remote_index_offset={}, ' +
             '(i1={}, i2={}) "{}" - (j1={}, j2={}) "{}"').format(
                 type_of_change,
                 remote_index_offset,
                 i1,
                 i2,
                 content_original[i1:i2],
                 j1,
                 j2,
                 content_edited[j1:j2]
             ))

        if type_of_change == 'equal':
            pass

        elif type_of_change == 'delete':
            delete_size = i2 - i1
            remote_index = i1 + remote_index_offset
            remote_index_offset -= delete_size
            rsp = connection.ra_delete(
                node_id,
                revision,
                remote_index,
                delete_size
            )
            check_server_response(rsp)
            revision = rsp.as_delete_rsp().revision

        elif type_of_change == 'replace':
            delete_size = i2 - i1
            remote_index = i1 + remote_index_offset
            if delete_size > 0:
                rsp = connection.ra_delete(
                    node_id,
                    revision,
                    remote_index,
                    delete_size
                )
                check_server_response(rsp)
                revision = rsp.as_delete_rsp().revision

            insert_size = j2 - j1
            remote_index_offset += insert_size - delete_size
            rsp = connection.ra_insert(
                node_id,
                revision,
                remote_index,
                content_edited[j1:j2]
            )
            check_server_response(rsp)
            revision = rsp.as_write_rsp().revision

        elif type_of_change == 'insert':
            remote_index = i1 + remote_index_offset
            insert_size = j2 - j1
            remote_index_offset += insert_size
            rsp = connection.ra_insert(
                node_id,
                revision,
                remote_index,
                content_edited[j1:j2]
            )
            check_server_response(rsp)
            revision = rsp.as_insert_rsp().revision
            insert_size = j2 - j1

        else:
            raise RuntimeError('Unhandled change type, type="{}"'.format(
                type_of_change
            ))

    return revision
