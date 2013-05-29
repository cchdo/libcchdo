from datetime import datetime, date
from json import JSONEncoder, dump

from libcchdo.log import LOG
from libcchdo.fns import Decimal, isnan
from libcchdo.model.datafile import DataFileCollection


def _column_type(col, obj):
    if col == 'EXPOCODE' or col == 'SECT_ID':
        return 'string'
    elif col == '_DATETIME':
        if type(obj) is datetime:
            return 'datetime'
        elif type(obj) is date:
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


class DefaultJSONSerializer(JSONEncoder):
    def serialize_datetime(self, o):
        return o.strftime('%FT%T')

    def serialize_date(self, o):
        return o.strftime('%F')

    def default(self, o):
        if isinstance(o, datetime):
            return self.serialize_datetime(o)
        elif isinstance(o, date):
            return self.serialize_date(o)
        return JSONEncoder.default(self, o)


class GoogleWireJSONSerializer(DefaultJSONSerializer):
    def serialize_datetime(self, o):
        nums = (','.join(['%d'] * 5)) % \
               (o.year, o.month, o.day, o.hour, o.minute)
        return 'Date(%s)' % nums

    def serialize_date(self, o):
        nums = (','.join(['%d'] * 3)) % (o.year, o.month, o.day)
        return 'Date(%s)' % nums

    def default(self, o):
        if isinstance(o, datetime):
            return self.serialize_datetime(o)
        elif isinstance(o, date):
            LOG.error(
                u"Date was provided when datetime expected. Please report "
                "this issue.")
            return self.serialize_date(o)
        elif isinstance(o, Decimal):
            return float(o)
        return JSONEncoder.default(self, o)


def _json(self, handle, column_headers, columns, global_values):
    json_columns = [{'id': col, 'label': col,
                     'type': _column_type(col, global_values[0])} \
                    for col in columns]
    json_rows = [_json_row(self, i, global_values, column_headers) \
                 for i in range(len(self))]
    wire_obj = {'cols': json_columns, 'rows': json_rows}

    dump(wire_obj, handle, allow_nan=False, separators=(',', ':'),
              cls=GoogleWireJSONSerializer)


def _raw_to_str(raw, column):
    if raw is None:
        return None
    if isinstance(raw, float):
        if isnan(raw):
            return '-Infinity'
        if column.parameter:
            return float(column.parameter.format % raw)
        return raw
    if isinstance(raw, datetime):
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
