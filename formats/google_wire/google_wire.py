"""libcchdo.formats.google_wire.google_wire"""

import datetime

import libcchdo.fns

def _column_type(col, obj):
    if col == 'EXPOCODE' or col == 'SECT_ID':
        return 'string'
    elif col == '_DATETIME':
        if type(obj) is datetime.datetime:
            return 'datetime'
        elif type(obj) is datetime.date:
            return 'date'
    else:
        return 'number'


def _raw_to_str(raw, json=False):
    if isinstance(raw, float):
        if libcchdo.fns.isnan(raw):
            if json:
                return None
            else:
                return '-Infinity'
        return raw
    if isinstance(raw, datetime.datetime):
        if json:
            nums = (','.join(['%d'] * 5)) % \
                   (raw.year, raw.month, raw.day, raw.hour, raw.minute)
            return 'Date(%s)' % nums
        else:
            return 'new Date(%s)' % raw.strftime('%Y,%m,%d,%H,%M')
    else:
        if json:
            return str(raw)
        else:
            return "'%s'" % str(raw)


def _json_row(self, i, global_values, column_headers):
    raw_values = global_values + \
                 [self.columns[hdr][i] for hdr in column_headers]

    row_values = [{'v': _raw_to_str(raw, True)} for raw in raw_values]
    return {'c': row_values}


def _json(self, handle, column_headers, columns, global_values):
    import json
    json_columns = [{'id': col, 'label': col,
                     'type': _column_type(col, global_values[0])} \
               for col in columns]
    json_rows = [_json_row(self, i, global_values, column_headers) \
                 for i in range(len(self))]
    wire_obj = {'cols': json_columns, 'rows': json_rows}
    handle.write(json.dumps(wire_obj, allow_nan=False, separators=(',', ':')))


def _wire_row(self, i, global_values, column_headers):
    raw_values = global_values + \
                 [self.columns[hdr][i] for hdr in column_headers]

    row_values = ['{v:%s}' % _raw_to_str(raw) for raw in raw_values]
    return '{c:[%s]}' % ','.join(row_values)


def _wire(self, handle, column_headers, columns, global_values):
    wire_columns = ["{id:'%s',label:'%s',type:'%s'}" % \
                    (col, col, _column_type(col, global_values[0]))\
                    for col in columns]

    wire_rows = [_wire_row(self, i, global_values, column_headers) \
                 for i in range(len(self))]

    handle.write("{cols:[%s],rows:[%s]}" % (','.join(wire_columns),
                                            ','.join(wire_rows)))


def write(self, handle, json=False):
    """How to write a Google Wire Protocol Javascript object literal.
       Args:
           json - whether to return a valid JSON object or just a Google
                  Wire Protocol object.
       Returns:
           a Google Wire Protocol object that represents the data file.
           This is different from a JSON object which is returned if
           json is True.
    """
    global_headers = sorted(self.globals.keys())
    column_headers = self.column_headers()
    columns = global_headers + column_headers
    global_values = [self.globals[key] for key in global_headers]

    if json:
        _json(self, handle, column_headers, columns, global_values)
    else:
        _wire(self, handle, column_headers, columns, global_values)
