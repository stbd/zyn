import difflib
import logging
import os.path
import posixpath

import zyn_util.errors
import zyn_util.exception


def verbose_count_to_log_level(verbose_count):
    logger = logging.getLogger()
    level = logging.WARNING
    if verbose_count == 1:
        level = logging.INFO
    elif verbose_count == 2:
        level = logging.DEBUG
    elif verbose_count > 2:
        logger.warn('Maximum number of verbose flags is 2, greater value is ignored')
    logger.setLevel(level)


def check_server_response(rsp):
    if rsp.is_error():
        desc = zyn_util.errors.error_to_string(rsp.error_code())
        raise zyn_util.exception.ZynServerException(rsp.error_code(), desc)


def join_paths(list_of_paths):
    path = posixpath.normpath('/'.join(list_of_paths))
    if path.startswith('//'):
        path = path[1:]
    return path


def to_remote_path(path):
    path = path.replace('\\', '/')
    return path


def local_path(paths):
    path_local = zyn_util.util.join_paths(paths)
    return os.path.normpath(path_local)


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
