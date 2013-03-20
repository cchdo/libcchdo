import os
from csv import reader as csv_reader
from contextlib import closing
from gzip import GzipFile
from webbrowser import open as webopen
from urlparse import urlparse, parse_qsl
from SimpleHTTPServer import SimpleHTTPRequestHandler

from lxml.html import builder as E, tostring, fromstring

from libcchdo import LOG, StringIO, get_library_abspath
from libcchdo.serve import get_local_host, open_server_on_high_port


class CSVViewerHTTPServer(SimpleHTTPRequestHandler):
    def do_GET(self):
        params = dict(parse_qsl(self.path[2:]))

        try:
            accept_encoding = self.headers['accept-encoding']
            if 'gzip' in accept_encoding and 'deflate' in accept_encoding:
                gz = True
            else:
                gz = False
        except KeyError:
            gz = False

        if self.path == '/':
            self.send_response(200, 'OK')
            self.send_header('Content-type', 'text/html')
            if gz:
                self.send_header('Content-Encoding', 'gzip')
            self.end_headers()
            if gz:
                self.wfile.write(self.server.view_html_gz)
            else:
                self.wfile.write(self.server.view_html)
        elif self.path == '/csv_view.css':
            self.send_response(200, 'OK')
            self.send_header('Content-type', 'text/css')
            self.end_headers()
            self.wfile.write(self.server.view_css)


def accordion_group(header, table, ingroup=False):
    body_class = 'accordion-body collapse'
    if ingroup:
        body_class += ' in'
    group = E.DIV(
        E.CLASS('accordion-group'),
        E.DIV(
            E.CLASS('accordion-heading'),
            E.A(
                E.CLASS('accordion-toggle ' + header),
                header,
                **{
                    'data-toggle': 'collapse',
                    'data-parent': '#dfaccordion',
                    'href': '#collapse' + header
                }),
        ),
        E.DIV(
            E.CLASS(body_class),
            E.DIV(
                E.CLASS('accordion-inner'),
                table,
            ),
            id='collapse' + header,
        ),
    )
    return group


def view(path):
    filename = os.path.basename(path)

    bootstrap_assets_root = '//twitter.github.com/bootstrap/assets'

    html = E.HTML(
        E.HEAD(
            E.META(charset='utf-8'),
            E.TITLE(filename),
            E.LINK(href='{0}/css/bootstrap.css'.format(bootstrap_assets_root),
                   rel='stylesheet'),
            E.LINK(href='/csv_view.css', rel='stylesheet'),
        ),
        lang='en',
    )
    body = E.BODY()
    html.append(body)

    df = E.DIV(
        E.CLASS('datafile accordion'),
        id='dfaccordion',
    )
    body.append(df)

    flag_indices = []
    flag_ending = '_FLAG_W'

    with open(filename, 'rb') as csvf:
        reader = csv_reader(csvf)
        table_type = None
        for row in reader:
            if row[0] == 'BOTTLE' or row[0] == 'CTD':
                row = (','.join(row), )
                if table_type != 'stamp':
                    table_type = 'stamp'
                    table = E.TABLE(E.CLASS('stamp'))
                    df.append(accordion_group('stamp', table))
            elif row[0].startswith('#'):
                row[0] = row[0][1:]
                row = (','.join(row), )
                if table_type != 'comments':
                    table_type = 'comments'
                    table = E.TABLE(E.CLASS('comments'))
                    df.append(accordion_group('comments', table))
            elif row[0] == 'END_DATA':
                if table_type != 'enddata':
                    table_type = 'enddata'
                    table = E.TABLE(E.CLASS('enddata'))
                    df.append(accordion_group('enddata', table))
            else:
                if table_type != 'data':
                    table_type = 'data'
                    table = E.TABLE(E.CLASS(
                        'data table table-striped table-hover table-condensed'))
                    #df.append(accordion_group('data', table, ingroup=True))
                    df.append(table)

            if table_type == 'data':
                if not flag_indices:
                    for i, p in enumerate(row):
                        if not p.endswith(flag_ending):
                            continue
                        flag_indices.append(i)
                        row[i] = fromstring(
                            row[i][:-len(flag_ending)] + '<br>' + 
                            flag_ending[1:])

            tr = E.TR()
            for i, e in enumerate(row):
                if table_type == 'data' and flag_indices:
                    if i in flag_indices:
                        tr.append(
                            E.TD(E.CLASS('flag flag' + str(e).strip()), e))
                    else:
                        tr.append(E.TD(e))
                else:
                    tr.append(E.TD(e))
            table.append(tr)

    body.append(
        fromstring('''\
<script src="//platform.twitter.com/widgets.js"></script>
<script src="{0}/js/jquery.js"></script>
<script src="{0}/js/bootstrap-transition.js"></script>
<script src="{0}/js/bootstrap-collapse.js"></script>
'''.format(bootstrap_assets_root))
    )

    host = get_local_host()
    httpd, port = open_server_on_high_port(CSVViewerHTTPServer)
    callback_uri = 'http://{host}:{port}'.format(host=host, port=port)

    httpd.view_html = tostring(html)
    with closing(StringIO()) as strio:
        gz = GzipFile(mode='wb', compresslevel=9, fileobj=strio)
        gz.write(httpd.view_html)
        gz.flush()
        gz.close()
        httpd.view_html_gz = strio.getvalue()
    httpd.view_css = open(
        os.path.join(get_library_abspath(), 'resources', 'csv_view.css')).read()

    print callback_uri
    webopen(callback_uri)

    try:
        print 'Press Ctrl-C when finished.'
        while True:
            httpd.handle_request()
    except KeyboardInterrupt:
        pass
