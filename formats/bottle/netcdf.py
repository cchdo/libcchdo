import datetime
import os
import re
import tempfile

import libcchdo
import libcchdo.db.model.std
import libcchdo.formats.netcdf as nc
import libcchdo.formats.woce as woce


NC_BOTTLE_VAR_TO_WOCE_PARAM = dict(libcchdo.db.model.std.session().query(
        libcchdo.db.model.std.Parameter.name_netcdf,
        libcchdo.db.model.std.Parameter.name).all())


VARATTRS = frozenset(('time', 'latitude', 'longitude', 'woce_date',
                      'woce_time', 'cast', 'station', ))


def read(self, handle):
    """How to read a Bottle NetCDF file."""
    filename = handle.name
    nc_file = nc.Dataset(filename, 'r')
    
    attrs = nc_file.__dict__
    expocode = attrs.get('EXPOCODE')
    self.globals['header'] = attrs.get('ORIGINAL_HEADER')
    station = attrs.get('STATION_NUMBER')
    cast = attrs.get('CAST_NUMBER')
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
    dtime = libcchdo.formats.woce.strptime_woce_date_time(woce_date, woce_time)

    calculated_time = nc.EPOCH + datetime.timedelta(minutes=int(time))
    # TODO Probably should trust dtime more because it is translated directly
    # from WOCE time.
    if dtime != calculated_time:
        libcchdo.LOG.warn(('Datetime declarations in Bottle NetCDF file '
                           'do not match (%s, %s)') % (dtime, calculated_time))

    varstation = ''.join(filter(None, vars['station'][:].tolist()))
    varcast = ''.join(filter(None, vars['cast'][:].tolist()))

    if varstation != station:
        libcchdo.LOG.warn(('Station declarations in Bottle NetCDF file '
                           'do not match (%s, %s)') % (station, varstation))

    if varcast != cast:
        libcchdo.LOG.warn(('Cast declarations in Bottle NetCDF file '
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
            name = NC_BOTTLE_VAR_TO_WOCE_PARAM[name]
            
            if name == 'drop':
                continue

            self.create_columns((name, ))
            self[name].values[vlo:vhi] = variable[:].tolist()

            # Quick conversions to uniform data format
            self[name].values[vlo:vhi] = map(
                libcchdo.fns.in_band_or_none,
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
    '_DATETIME', 'LATITUDE', 'LONGITUDE', 'DEPTH', )


def write(self, handle):
    """How to write a Bottle NetCDF file."""
    UNKNOWN = 'UNKNOWN'
    UNSPECIFIED_UNITS = 'unspecified'
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
    nc_file.EXPOCODE = self['EXPOCODE'][0] or UNKNOWN
    nc_file.Conventions = 'COARDS/WOCE'
    nc_file.WOCE_VERSION = '3.0'
    nc_file.WOCE_ID = self['SECT_ID'][0] or UNKNOWN
    nc_file.DATA_TYPE = 'WOCE Bottle'
    nc_file.STATION_NUMBER = nc.simplest_str(self['STNNBR'][0]) or UNKNOWN
    nc_file.CAST_NUMBER = nc.simplest_str(self['CASTNO'][0]) or UNKNOWN
    nc_file.BOTTOM_DEPTH_METERS = int(max(self['DEPTH'].values))
    nc_file.BOTTLE_NUMBERS = ' '.join(map(nc.simplest_str, self['BTLNBR'].values))
    if self['BTLNBR'].is_flagged_woce():
        nc_file.BOTTLE_QUALITY_CODES = ' '.join(map(str, self['BTLNBR'].flags_woce))
    nc_file.Creation_Time = libcchdo.fns.strftime_iso(datetime.datetime.utcnow())

    header_filter = re.compile('BOTTLE|db_to_exbot|jjward')
    header = '# Previous stamp: %s\n' % self.globals['stamp'] + "\n".join(
        [x for x in self.globals['header'].split("\n") if not header_filter.match(x)])
    nc_file.ORIGINAL_HEADER = header

    nc_file.WOCE_BOTTLE_FLAG_DESCRIPTION = woce.BOTTLE_FLAG_DESCRIPTION
    nc_file.WOCE_WATER_SAMPLE_FLAG_DESCRIPTION = woce.WATER_SAMPLE_FLAG_DESCRIPTION

    # Coordinate variables
    dtime = min(self['_DATETIME'])

    var_time = nc_file.createVariable('time', 'i', ('time',))
    var_time.long_name = 'time'
    var_time.units = 'minutes since %s' % libcchdo.fns.strftime_iso(nc.EPOCH)
    var_time.data_min = nc.minutes_since_epoch(dtime)
    var_time.data_max = var_time.data_min
    var_time.C_format = '%10d'
    var_time[:] = var_time.data_min

    var_latitude = nc_file.createVariable('latitude', 'f', ('latitude',))
    var_latitude.long_name = 'latitude'
    var_latitude.units = 'degrees_N'
    var_latitude.data_min = self['LATITUDE'][0]
    var_latitude.data_max = var_latitude.data_min
    var_latitude.C_format = '%9.4f'
    var_latitude[:] = var_latitude.data_min

    var_longitude = nc_file.createVariable('longitude', 'f', ('longitude',))
    var_longitude.long_name = 'longitude'
    var_longitude.units = 'degrees_E'
    var_longitude.data_min = self['LONGITUDE'][0]
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
    var_station[:] = nc.simplest_str(self['STNNBR'][0]).ljust(len(var_station))
    
    var_cast = nc_file.createVariable('cast', 'c', ('string_dimension',))
    var_cast.long_name = 'CAST'
    var_cast.units = UNSPECIFIED_UNITS
    var_cast.C_format = '%s'
    var_cast[:] = nc.simplest_str(self['CASTNO'][0]).ljust(len(var_cast))

    # Create data variables and fill them
    for param, column in self.columns.iteritems():
        parameter = column.parameter
        if not parameter:
        	continue
        if parameter.mnemonic_woce() in STATIC_PARAMETERS_PER_CAST:
            continue
        parameter_name = (parameter.name_netcdf or 
                          parameter.name).encode('ascii', 'replace')
        var = nc_file.createVariable(parameter_name, 'f8', ('pressure',))
        var.long_name = parameter_name
        var.units = parameter.units.name.encode('ascii', 'replace') if \
            parameter.units else UNSPECIFIED_UNITS
        compact_column = filter(None, column)
        if compact_column:
            var.data_min = min(compact_column)
            var.data_max = max(compact_column)
        else:
            var.data_min = float('-inf')
            var.data_max = float('inf')
        var.C_format = parameter.format.encode('ascii', 'replace')
        var[:] = column.values

        if column.is_flagged_woce():
            vfw = nc_file.createVariable(parameter_name + nc.QC_SUFFIX, 'i2', ('pressure',))
            vfw.long_name = parameter_name + nc.QC_SUFFIX
            vfw[:] = column.flags_woce

    nc_file.close()
    handle.write(temp.read())
    temp.close()
