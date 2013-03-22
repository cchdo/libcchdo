import os
from csv import reader as csv_reader

from lxml.html import builder as E, tostring, fromstring

from libcchdo import LOG, get_library_abspath
from libcchdo.serve import SimpleHTTPServer


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

    server = SimpleHTTPServer()
    server.register('/', tostring(html))
    server.register('/csv_view.css', open(
        os.path.join(get_library_abspath(), 'resources', 'csv_view.css')).read(),
        mime_type='text/css')
    server.open_browser()
    server.serve_forever()
