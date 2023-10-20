import logging
import ssl
import socket

import certifi

log = logging.getLogger(__name__)


class ZynSocket:
    def __init__(self, socket, tls_socket=None, tls_context=None):
        self._socket = socket
        self._socket_tls = tls_socket
        self._tls_context = tls_context

    def _create_socket(remote_address, remote_port):
        info = socket.getaddrinfo(remote_address, remote_port, type=socket.SOCK_STREAM)[0]
        s = socket.socket(info[0], info[1])
        s.connect(info[4])
        return s

    def create_tls(remote_address, remote_port, path_certificate=None, remote_hostname=None):
        log.info(
            'Creating TLS connection to {}:{} (path_ceritificate="{}", '
            'remote_hsotname="{}")'.format(
                remote_address,
                remote_port,
                path_certificate,
                remote_hostname,
        ))
        socket = ZynSocket._create_socket(remote_address, remote_port)
        context = ssl.create_default_context()
        context.load_verify_locations(path_certificate or certifi.where())
        tls_socket = context.wrap_socket(
            socket,
            server_hostname=remote_hostname or remote_address,
        )
        return ZynSocket(socket, context, tls_socket)

    def create_no_tls(remote_address, remote_port):
        log.info(f'Creating connection to {remote_address}:{remote_port}')
        s = ZynSocket._create_socket(remote_address, remote_port)
        return ZynSocket(s)

    def socket(self):
        return self._socket_tls or self._socket

    def settimeout(self, timeout):
        return self._socket.settimeout(timeout)

    def recv(self, size=None):
        if size is None:
            return self.socket().recv(1024)
        else:
            return self.socket().recv(size)

    def sendall(self, data):
        return self.socket().sendall(data)

    def close(self):
        self.socket().shutdown(socket.SHUT_WR)
        self.socket().close()
