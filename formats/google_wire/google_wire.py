"""libcchdo.formats.google_wire.google_wire"""

import datetime

try:
    from math import isnan
except ImportError: # Cover when < python-2.6
    def isnan(n):
        return n != n


def write(self, handle):
    """How to write a Google Wire Protocol Javascript object literal"""
    global_headers = sorted(self.globals.keys())
    column_headers = self.column_headers()
    columns = global_headers + column_headers

    def column_type(col):
        if col == 'EXPOCODE' or col == 'SECT_ID':
            return 'string'
        elif col == '_DATETIME':
            return 'datetime'
        else:
            return 'number'

    wire_columns = ["{id:'%s',label:'%s',type:'%s'}" % \
                    (col, col, column_type(col)) for col in columns]
    global_values = [self.globals[key] for key in global_headers]

    def wire_row(i):
        raw_values = global_values + \
                     [self.columns[hdr][i] for hdr in column_headers]

        def raw_to_str(raw):
            if isinstance(raw, float):
                if isnan(raw):
                    return '-Infinity'
                return str(raw)
            if isinstance(raw, datetime.datetime):
                return 'new Date(%s)' % raw.strftime('%Y,%m,%d,%H,%M')
            else:
                return "'%s'" % str(raw)

        row_values = ['{v:%s}' % raw_to_str(raw) for raw in raw_values]
        return '{c:[%s]}' % ','.join(row_values)

    wire_rows = [wire_row(i) for i in range(len(self))]
    handle.write("{cols:[%s],rows:[%s]}" % \
                 (','.join(wire_columns), ','.join(wire_rows)))
