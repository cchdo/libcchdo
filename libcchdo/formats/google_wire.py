import datetime

from .. import LOG
from .. import fns
from ..fns import Decimal
from ..model.datafile import DataFileCollection


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


def _get_values(self, hdr):
    if hdr.endswith('_FLAG_W') or hdr.endswith('_FLAG_I'):
        param = hdr[:hdr.find('_FLAG')]
        if hdr.endswith('W'):
            return self[param].flags_woce
        else:
            return self[param].flags_igoss
    return self[hdr]


def _getter(self, hdr, i):
    return _get_values(self, hdr)[i]


def _raw_values(self, i, global_values, column_headers):
    return global_values + [_getter(self, hdr, i) for hdr in column_headers]



def _json_row(self, i, global_values, column_headers):
    row_values = [{'v': raw} for raw in _raw_values(self, i, global_values,
                                                    column_headers)]
    return {'c': row_values}


def _json(self, handle, column_headers, columns, global_values):
    import json
    json_columns = [{'id': col, 'label': col,
                     'type': _column_type(col, global_values[0])} \
                    for col in columns]
    json_rows = [_json_row(self, i, global_values, column_headers) \
                 for i in range(len(self))]
    wire_obj = {'cols': json_columns, 'rows': json_rows}

    class serializer(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, datetime.datetime):
                nums = (','.join(['%d'] * 5)) % \
                       (o.year, o.month, o.day, o.hour, o.minute)
                return 'Date(%s)' % nums
            if isinstance(o, Decimal):
                return float(o)
            return json.JSONEncoder.default(self, o)

    json.dump(wire_obj, handle, allow_nan=False, separators=(',', ':'),
              cls=serializer)


def _raw_to_str(raw, column):
    if raw is None:
        return None
    if isinstance(raw, float):
        if fns.isnan(raw):
            return '-Infinity'
        if column.parameter:
            return float(column.parameter.format % raw)
        return raw
    if isinstance(raw, datetime.datetime):
        return 'new Date(%s)' % raw.strftime('%Y,%m,%d,%H,%M')
    else:
        return "'%s'" % str(raw)


def _wire_row(self, i, global_values, column_headers):
    raw_values = global_values + \
                 [_getter(self, hdr, i) for hdr in column_headers]

    row_values = ['{v:%s}' % _raw_to_str(raw, _get_values(self, hdr)) \
                  for raw in _raw_values(self, i,
                                         global_values, column_headers)]
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
    if type(self) == DataFileCollection:
        self = self.to_data_file()
    global_headers = sorted(self.globals.keys())
    column_headers = []
    for column in self.sorted_columns():
        param_name = column.parameter.mnemonic_woce()
        column_headers.append(param_name)
        if column.is_flagged_woce():
            column_headers.append('%s_FLAG_W' % param_name)
        if column.is_flagged_igoss():
            column_headers.append('%s_FLAG_I' % param_name)
    columns = global_headers + column_headers
    global_values = [self.globals[key] for key in global_headers]

    if json:
        _json(self, handle, column_headers, columns, global_values)
    else:
        _wire(self, handle, column_headers, columns, global_values)
