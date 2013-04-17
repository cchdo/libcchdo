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

    bootstrap_assets_root = '//twitter.github.io/bootstrap/assets'

    html = E.HTML(
        E.HEAD(
            E.META(charset='utf-8'),
            E.TITLE(filename),
            E.LINK(href='{0}/css/bootstrap.css'.format(bootstrap_assets_root),
                   rel='stylesheet'),
            E.LINK(href='//mottie.github.io/tablesorter/css/theme.blue.css', rel='stylesheet'),
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
        first_data_row = False
        parameters = []
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
                    first_data_row = True
                    table = E.TABLE(E.CLASS(
                        'data table table-striped table-hover table-condensed'),
                        id='data-table')
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

            if first_data_row:
                tr = E.TR(E.CLASS('tablesorter-stickyHeader'))
            else:
                tr = E.TR()
            for i, elem in enumerate(row):
                if table_type == 'data' and flag_indices:
                    if first_data_row:
                        cell = E.TH
                        try:
                            param = elem.text
                        except AttributeError:
                            param = elem
                        parameters.append(param)
                    else:
                        cell = E.TD
                        param = parameters[i]

                    if i in flag_indices:
                        if first_data_row:
                            classname = E.CLASS('flag flag' + param)
                        else:
                            classname = E.CLASS('flag flag' + param + ' flag' + elem)
                        tr.append(cell(classname, elem))
                    else:
                        tr.append(cell(elem))
                else:
                    tr.append(E.TD(elem))
            if first_data_row:
                table.append(E.THEAD(tr))
                first_data_row = False
            else:
                table.append(tr)

    # Yes, hotlinking. Sorry!
    body.append(
        fromstring('''\
<script src="//platform.twitter.com/widgets.js"></script>
<script src="//ajax.googleapis.com/ajax/libs/jquery/1.9.1/jquery.min.js"></script>
<script src="{0}/js/bootstrap-transition.js"></script>
<script src="{0}/js/bootstrap-collapse.js"></script>
<script src="//mottie.github.io/tablesorter/js/jquery.tablesorter.min.js"></script>
<script src="//mottie.github.io/tablesorter/js/jquery.tablesorter.widgets.min.js"></script>
<script src="/csv_view.js"></script>
'''.format(bootstrap_assets_root))
    )

    server = SimpleHTTPServer()
    server.register('/', tostring(html))
    server.register('/csv_view.css', open(
        os.path.join(get_library_abspath(), 'resources', 'csv_view.css')).read(),
        mime_type='text/css')
    server.register('/csv_view.js', open(
        os.path.join(get_library_abspath(), 'resources', 'csv_view.js')).read(),
        mime_type='text/javascript')
    server.open_browser()
    server.serve_forever()
