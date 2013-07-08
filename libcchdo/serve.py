from contextlib import closing
from gzip import GzipFile
from SocketServer import TCPServer
from socket import socket, AF_INET, SOCK_DGRAM, error as sockerr
from SimpleHTTPServer import SimpleHTTPRequestHandler
from webbrowser import open as webopen, Error as WebError

from libcchdo.log import LOG
from libcchdo.util import StringIO


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


class FileHTTPRequestHandler(SimpleHTTPRequestHandler):
    def accept_gzip(self):
        try:
            accept_encoding = self.headers['accept-encoding']
            if 'gzip' in accept_encoding and 'deflate' in accept_encoding:
                return True
            else:
                return False
        except KeyError:
            return False

    def gzip_file(self, fff, compresslevel=6):
        with closing(StringIO()) as strio:
            gz = GzipFile(mode='wb', compresslevel=compresslevel, fileobj=strio)
            gz.write(fff)
            gz.flush()
            gz.close()
            return strio.getvalue()

    def flushing_write_str(self, resp, chunk_size=2 ** 9):
        """Speed up large resource loading by flushing chunks to the browser.

        Adapted from http://stackoverflow.com/questions/312443

        """
        wfile = self.wfile
        for i in xrange(0, len(resp), chunk_size):
            wfile.write(resp[i:i + chunk_size])
            wfile.flush()

    def do_GET(self):
        try:
            response = self.server.file_responses[self.path]
            self.send_response(200, 'OK')
            self.send_header('Content-type', response[1])
            if self.accept_gzip() and response[2]:
                try:
                    resp = self.file_responses_gzipped[self.path]
                except AttributeError:
                    resp = self.gzip_file(response[0])
                    self.file_responses_gzipped = {self.path: resp}
                except KeyError:
                    resp = self.gzip_file(response[0])
                    self.file_responses_gzipped[self.path] = resp
                self.send_header('Content-Encoding', 'gzip')
            else:
                resp = response[0]
            for k, v in response[3]:
                self.send_header(k, v)
            self.send_header('Content-Length', len(resp))
            self.end_headers()
            self.flushing_write_str(resp)
        except KeyError:
            self.send_response(404, 'Not Found')
            self.end_headers()


class SimpleHTTPServer():
    def __init__(self, remote=None, req_handler=FileHTTPRequestHandler):
        self.host = get_local_host(remote)
        self.httpd, self.port = open_server_on_high_port(req_handler)

    def host_port(self):
        return 'http://{host}:{port}'.format(host=self.host, port=self.port)

    def register(self, path, response,
                 mime_type='text/html', gzippable=True, headers={}):
        resp_tuple = (response, mime_type, gzippable, headers)
        try:
            self.httpd.file_responses[path] = resp_tuple
        except AttributeError:
            self.httpd.file_responses = {path: resp_tuple}

    def open_browser(self):
        try:
            # FIXME TODO be smarter about how to open a web browser.
            #webopen(self.host_port())
            raise WebError()
        except WebError:
            LOG.info(u'Please open the following URL in your browser.')
            print self.host_port()

    def serve_forever(self):
        try:
            print 'Press Ctrl-C when finished.'
            while True:
                self.httpd.handle_request()
        except KeyboardInterrupt:
            pass
