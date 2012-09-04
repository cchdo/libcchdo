import datetime
import os
import re
import tempfile

import numpy as np

from ... import LOG
from ... import fns
from ...db.model import std
from .. import netcdf as nc
from .. import woce


NC_BOTTLE_VAR_TO_WOCE_PARAM = dict(std.session().query(
    std.Parameter.name_netcdf, std.Parameter.name).all())


VARATTRS = frozenset(('time', 'latitude', 'longitude', 'woce_date',
                      'woce_time', 'cast', 'station', ))


def ascii(x):
    return x.encode('ascii', 'replace')


def read(self, handle):
    """How to read a Bottle NetCDF file."""
    filename = handle.name
    nc_file = nc.Dataset(filename, 'r')
    
    attrs = nc_file.__dict__
    expocode = attrs.get('EXPOCODE')
    self.globals['header'] = attrs.get('ORIGINAL_HEADER')
    station = attrs.get('STATION_NUMBER').strip()
    cast = attrs.get('CAST_NUMBER').strip()
    bottle_numbers = attrs.get('BOTTLE_NUMBERS', '').split()
    bottle_flags = attrs.get('BOTTLE_QUALITY_CODES', [])[:]
    section_id = attrs.get('WOCE_ID')
    bottom_depth = attrs.get('BOTTOM_DEPTH_METERS')

    vars = nc_file.variables

    time = vars['time'][:][0]
    latitude = vars['latitude'][:][0]
    longitude = vars['longitude'][:][0]
    woce_date = vars['woce_date'][:][0]
    woce_time = vars.get('woce_time', [None])[:][0]
    dtime = woce.strptime_woce_date_time(woce_date, woce_time)

    calculated_time = nc.EPOCH + datetime.timedelta(minutes=int(time))
    # TODO Probably should trust dtime more because it is translated directly
    # from WOCE time.
    if type(dtime) is datetime.date:
    	calculated_time = calculated_time.date()
    if dtime != calculated_time:
        LOG.warn(('Datetime declarations in Bottle NetCDF file '
                  'do not match (%s, %s)') % (dtime, calculated_time))

    varstation = ''.join(filter(None, vars['station'][:].tolist())).strip()
    varcast = ''.join(filter(None, vars['cast'][:].tolist())).strip()

    if varstation != station:
        LOG.warn(('Station declarations in Bottle NetCDF file '
                  'do not match (%s, %s)') % (station, varstation))

    if varcast != cast:
        LOG.warn(('Cast declarations in Bottle NetCDF file '
                  'do not match (%s, %s)') % (cast, varcast))

    # Create global columns if they do not exist
    globals_to_vars = {
        'EXPOCODE': ('', expocode),
        'SECT_ID': ('', section_id),
        'STNNBR': ('', station),
        'CASTNO': ('', cast),
        'DEPTH': ('METERS', bottom_depth),
        '_DATETIME': ('', dtime),
    }
    gs = globals_to_vars.keys()
    self.create_columns(gs)
    self.create_columns(('BTLNBR', ))

    # Fill global columns with data
    dimensions = len(nc_file.dimensions['pressure'])
    vlo = len(self)
    vhi = vlo + dimensions
    for g, var in globals_to_vars.items():
        self[g].values[vlo:vhi] = [var[1]] * dimensions

    self['BTLNBR'].values[vlo:vhi] = bottle_numbers

    # First pass to create columns
    qc_vars = {}
    for name in frozenset(vars.keys()) - VARATTRS:
        variable = vars[name]
        if name.endswith(nc.QC_SUFFIX):
            qc_vars[NC_BOTTLE_VAR_TO_WOCE_PARAM[
                name[:-len(nc.QC_SUFFIX)]]] = variable
        else:
            name = NC_BOTTLE_VAR_TO_WOCE_PARAM.get(name, name)
            
            if name == 'drop':
                continue

            self.create_columns((name, ))
            self[name].values[vlo:vhi] = variable[:].tolist()

            # Quick conversions to uniform data format
            self[name].values[vlo:vhi] = map(
                fns.in_band_or_none,
                self[name].values[vlo:vhi])

    # Second pass to put in flags
    for name, variable in qc_vars.items():
        if name in self.columns:
            self[name].flags_woce[vlo:vhi] = variable[:].tolist()
        else:
            # The column is probably a global
            pass

    # Pad out columns that aren't present in this read to maintain
    # file structure.
    nones = [None for i in range(vlo, vhi)]
    for c in self.columns.values():
        if len(c) < vhi:
            c.values[vlo:vhi] = nones
            if c.is_flagged_woce():
                c.flags_woce[vlo:vhi] = nones
            if c.is_flagged_igoss():
                c.flags_igoss[vlo:vhi] = nones

    nc_file.close()

    self.check_and_replace_parameters()


STATIC_PARAMETERS_PER_CAST = ('EXPOCODE', 'SECT_ID', 'STNNBR', 'CASTNO',
    '_DATETIME', 'LATITUDE', 'LONGITUDE', 'DEPTH', 'BTLNBR', 'SAMPNO', )


UNKNOWN = 'UNKNOWN'


UNSPECIFIED_UNITS = 'unspecified'


def _lambda_or_unknown(fn, unknown=UNKNOWN):
    """Attempt to return the result of fn; on error return unknown."""
    try:
        return fn()
    except (KeyError, IndexError):
        return unknown


def create_common_variables(df, nc_file):
    """Add variables to the netcdf file object such as date, time etc."""
    # Coordinate variables
    # Take the time of the first tripped bottle for the cast, in accordance with
    # WOCE spec.
    dtime = min(df['_DATETIME'])

    var_time = nc_file.createVariable('time', 'i', ('time',))
    var_time.long_name = 'time'
    # Java OceanAtlas 5.0.2 requires ISO 8601 with space separator.
    var_time.units = 'minutes since %s' % nc.EPOCH.isoformat(' ')
    var_time.data_min = int(nc.minutes_since_epoch(dtime))
    var_time.data_max = var_time.data_min
    var_time.C_format = '%10d'
    var_time[:] = var_time.data_min

    var_latitude = nc_file.createVariable('latitude', 'f', ('latitude',))
    var_latitude.long_name = 'latitude'
    var_latitude.units = 'degrees_N'
    var_latitude.data_min = float(df['LATITUDE'][0])
    var_latitude.data_max = var_latitude.data_min
    var_latitude.C_format = '%9.4f'
    var_latitude[:] = var_latitude.data_min

    var_longitude = nc_file.createVariable('longitude', 'f', ('longitude',))
    var_longitude.long_name = 'longitude'
    var_longitude.units = 'degrees_E'
    var_longitude.data_min = float(df['LONGITUDE'][0])
    var_longitude.data_max = var_longitude.data_min
    var_longitude.C_format = '%9.4f'
    var_longitude[:] = var_longitude.data_min

    woce_datetime = woce.strftime_woce_date_time(dtime)

    var_woce_date = nc_file.createVariable('woce_date', 'i', ('time',))
    var_woce_date.long_name = 'WOCE date'
    var_woce_date.units = 'yyyymmdd UTC'
    var_woce_date.data_min = int(woce_datetime[0] or -9)
    var_woce_date.data_max = var_woce_date.data_min
    var_woce_date.C_format = '%8d'
    var_woce_date[:] = var_woce_date.data_min
    
    if woce_datetime[1]:
        var_woce_time = nc_file.createVariable('woce_time', 'i2', ('time',))
        var_woce_time.long_name = 'WOCE time'
        var_woce_time.units = 'hhmm UTC'
        var_woce_time.data_min = int(woce_datetime[1] or -9)
        var_woce_time.data_max = var_woce_time.data_min
        var_woce_time.C_format = '%4d'
        var_woce_time[:] = var_woce_time.data_min
    
    # Hydrographic specific
    
    var_station = nc_file.createVariable('station', 'c', ('string_dimension',))
    var_station.long_name = 'STATION'
    var_station.units = UNSPECIFIED_UNITS
    var_station.C_format = '%s'
    var_station[:] = nc.simplest_str(df['STNNBR'][0]).ljust(len(var_station))
    
    var_cast = nc_file.createVariable('cast', 'c', ('string_dimension',))
    var_cast.long_name = 'CAST'
    var_cast.units = UNSPECIFIED_UNITS
    var_cast.C_format = '%s'
    var_cast[:] = nc.simplest_str(df['CASTNO'][0]).ljust(len(var_cast))


def create_and_fill_data_variables(df, nc_file):
    """Add variables to the netcdf file object that correspond to data."""
    # Create data variables and fill them
    for column in df.sorted_columns():
        parameter = column.parameter
        if not parameter:
        	continue
        if parameter.mnemonic_woce() in STATIC_PARAMETERS_PER_CAST:
            continue
        parameter_name = ascii(parameter.name_netcdf or parameter.name)
        var = nc_file.createVariable(parameter_name, 'f8', ('pressure',))
        var.long_name = parameter_name
        var.units = ascii(parameter.units.name) if parameter.units \
            else UNSPECIFIED_UNITS
        compact_column = filter(None, column)

        if compact_column:
            var.data_min = float(min(compact_column))
            var.data_max = float(max(compact_column))
        else:
            var.data_min = float('-inf')
            var.data_max = float('inf')

        if var.long_name == 'pressure':
            var.positive = 'down'
            var.units = 'dbar'

        if parameter.format:
            var.C_format = ascii(parameter.format)
        else:
            # TODO TEST
            LOG.warn(u'Parameter {0} has no format'.format(parameter.name))
            var.C_format = '%s'
        var.WHPO_Variable_Name = ascii(parameter.name)
        var[:] = column.values

        if column.is_flagged_woce():
            qc_param_name = parameter_name + nc.QC_SUFFIX
            var.OBS_QC_VARIABLE = qc_param_name
            vfw = nc_file.createVariable(qc_param_name, 'i2', ('pressure',))
            vfw.long_name = qc_param_name + '_flag'
            vfw.units = 'woce_flags'
            vfw.C_format = '%1d'
            vfw[:] = column.flags_woce


def write(self, handle):
    """How to write a Bottle NetCDF file."""
    STRLEN = 40

    temp = tempfile.NamedTemporaryFile()
    nc_file = nc.Dataset(temp.name, 'w', format='NETCDF3_CLASSIC')

    # Define dimension variables
    makeDim = nc_file.createDimension
    makeDim('time', 1)
    makeDim('pressure', len(self))
    makeDim('latitude', 1)
    makeDim('longitude', 1)
    makeDim('string_dimension', STRLEN)

    # Define dataset attributes
    nc_file.EXPOCODE = _lambda_or_unknown(lambda: self['EXPOCODE'][0])
    nc_file.Conventions = 'COARDS/WOCE'
    nc_file.WOCE_VERSION = '3.0'
    nc_file.WOCE_ID = _lambda_or_unknown(lambda: self['SECT_ID'][0])
    nc_file.DATA_TYPE = 'WOCE Bottle'
    nc_file.STATION_NUMBER = _lambda_or_unknown(
        lambda: nc.simplest_str(self['STNNBR'][0]))
    nc_file.CAST_NUMBER = _lambda_or_unknown(
        lambda: nc.simplest_str(self['CASTNO'][0]))
    nc_file.BOTTOM_DEPTH_METERS = int(
        max(self['DEPTH'].values) or woce.FILL_VALUE)
    nc_file.BOTTLE_NUMBERS = ' '.join(
        map(nc.simplest_str, self['BTLNBR'].values))
    if self['BTLNBR'].is_flagged_woce():
        # Java OceanAtlas 5.0.2 and possibly before requires bottle quality
        # codes to be shorts.
        btl_quality_codes = \
            np.array(self['BTLNBR'].flags_woce).astype(np.int16)
        nc_file.BOTTLE_QUALITY_CODES = btl_quality_codes

    nc_file.Creation_Time = fns.strftime_iso(datetime.datetime.utcnow())

    header = 'BOTTLE,%s\n' % self.globals['stamp'] + self.globals['header']
    nc_file.ORIGINAL_HEADER = header

    nc_file.WOCE_BOTTLE_FLAG_DESCRIPTION = woce.BOTTLE_FLAG_DESCRIPTION
    nc_file.WOCE_WATER_SAMPLE_FLAG_DESCRIPTION = \
        woce.WATER_SAMPLE_FLAG_DESCRIPTION

    create_and_fill_data_variables(self, nc_file)
    create_common_variables(self, nc_file)

    nc_file.close()
    handle.write(temp.read())
    temp.close()
