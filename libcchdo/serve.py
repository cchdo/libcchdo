from SocketServer import TCPServer
from socket import socket, AF_INET, SOCK_DGRAM, error as sockerr


def get_local_host(remote=None):
    if remote is None:
        remote = 'whatismyip.akamai.com'

    s = socket(AF_INET, SOCK_DGRAM)
    s.connect((remote, 80))
    sockname = s.getsockname()
    s.close()
    host = sockname[0]
    return host


def open_server_on_high_port(http_req_handler_class,
                             start_port=None, max_port=None):
    if start_port is None:
        start_port = 60000
    if max_port is None:
        max_port = 2 ** 16

    port = start_port
    while True:
        try:
            httpd = TCPServer(('', port), http_req_handler_class)
            break
        except sockerr:
            port += 1
            if port >= max_port:
                port = start_port
    return httpd, port
