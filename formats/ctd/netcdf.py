'''
Handler for CTD NetCDF files
'''

import libcchdo
import datetime
import tempfile
import sys

import libcchdo.formats.woce as woce
import libcchdo.formats.netcdf as nc


NC_CTD_VAR_TO_WOCE_PARAM = {
    'cast': 'CASTNO',
    'temperature': 'CTDTMP',
    'time': 'drop',
    'woce_date': 'DATE',
    'oxygen': 'CTDOXY',
    'salinity': 'CTDSAL',
    'pressure': 'CTDPRS',
    'station': 'STNNBR',
    'longitude': 'LONGITUDE',
    'latitude': 'LATITUDE',
    'woce_time': 'TIME',
}


GLOBALS_TO_RENAME_AS = {
    'CAST_NUMBER': 'CASTNO',
    'STATION_NUMBER': 'STNNBR',
    'BOTTOM_DEPTH_METERS': 'DEPTH',
    'WOCE_ID': 'SECT_ID',
    'EXPOCODE': 'EXPOCODE',
}


STATIC_PARAMETERS_PER_CAST = ('EXPOCODE', 'SECT_ID', 'STNNBR', 'CASTNO',
    '_DATETIME', 'LATITUDE', 'LONGITUDE', 'DEPTH', )


def read(self, handle):
    '''How to read a CTD NetCDF file.'''
    filename = handle.name
    nc_file = nc.Dataset(filename, 'r')
    # Create columns for all the variables and get all the data.
    # Map the nc_ctd variable to drop to skip the variable.
    qc_vars = {}
    # First pass to create columns
    for name, variable in nc_file.variables.items():
        if name.endswith(nc.QC_SUFFIX):
            qc_vars[NC_CTD_VAR_TO_WOCE_PARAM[name[:-len(nc.QC_SUFFIX)]]] = variable
        elif name == 'sampno' or name == 'btlnbr': #XXX
            continue #XXX
        else:
            name = NC_CTD_VAR_TO_WOCE_PARAM[name]

            if name == 'drop':
                continue

            self.columns[name] = libcchdo.Column(name)
            self.columns[name].values = variable[:].tolist()

            # Do some quick transformations from NetCDF pecularities to standard data format
            if name in ['STNNBR', 'CASTNO']:
                # CCHDO NetCDFs have STNNBR and CASTNO as an array of characters.
                # Collapse them into a string.
                self.columns[name].values = [''.join(self.columns[name].values)]
            elif name in ['DATE']:
                # Translate string date YYYYMMDD to date object
                string = str(self.columns[name].values[0])
                self.columns[name].values[0] = '%s-%s-%s' % \
                    (string[0:4], string[4:6], string[6:8])
            if name == 'CTDSAL':
                self.columns[name].values = map(
                    lambda x: None if libcchdo.fns.equal_with_epsilon(-9.99, x) \
                              else x,
                    self.columns[name].values)

            # Check for globals
            if len(self.columns[name].values) <= 1:
                # If the column has only one data point it should be in the globals
                self.globals[name] = self.columns[name].get(0)
                del self.columns[name]

    # Second pass to put in flags
    for name, variable in qc_vars.items():
        if name in self.columns:
            self.columns[name].flags_woce = variable[:].tolist()
        else:
            # The column is probably a global
            pass

    # Rename globals to CCHDO recognized ones
    global_attrs = nc_file.__dict__
    for g, param in GLOBALS_TO_RENAME_AS.items():
        self.globals[param] = str(global_attrs[g])

    self.globals['stamp'] = global_attrs['ORIGINAL_HEADER']

    # Clean up
    nc_file.close()

    self.check_and_replace_parameters()


WOCE_CTD_FLAG_DESCRIPTION = '::%s::' % ':'.join((
    '1=Not calibrated',
    '2=Acceptable measurement',
    '3=Questionable measurement',
    '4=Bad measurement',
    '5=Not reported',
    '6=Interpolated over >2 dbar interval',
    '7=Despiked',
    '8=Not assigned for CTD data',
    '9=Not sampled',))


def netcdf_variable_name_from_column(column):
    if not column.parameter.description:
        libccho.LOG.warn('Bad parameter description %s' % column.parameter)
        return None
    n = column.parameter.description.lower()
    n = n.replace('ctd ', '')
    return n.replace('(', '_').replace(')', '_').replace(' ', '_')


def write(self, handle):
    '''How to write a CTD NetCDF file.'''
    UNKNOWN = 'UNKNOWN'
    UNSPECIFIED_UNITS = 'unspecified'
    STRLEN = 40

    tmp = tempfile.NamedTemporaryFile()
    strdate = str(self.globals['DATE'])
    strtime = str(self.globals['TIME'])
    isowocedate = datetime.datetime(
            int(strdate[:4]), int(strdate[4:6]), int(strdate[6:]),
            int(strtime[:2]), int(strtime[2:]))
    nc_file = nc.Dataset(tmp.name, 'w', format='NETCDF3_CLASSIC')

    # Define dimensions variables
    makeDim = nc_file.createDimension
    makeDim('time', 1)
    makeDim('pressure', len(self))
    makeDim('latitude', 1)
    makeDim('longitude', 1)
    makeDim('string_dimension', STRLEN)

    # Define dataset attributes
    nc_file.EXPOCODE = self.globals['EXPOCODE']
    nc_file.Conventions = 'COARDS/WOCE'
    nc_file.WOCE_VERSION = '3.0'
    try:
        nc_file.WOCE_ID = self.globals['SECT_ID']
    except KeyError:
        nc_file.WOCE_ID = UNKNOWN
    nc_file.DATA_TYPE = 'WOCE CTD'
    nc_file.STATION_NUMBER = self.globals['STNNBR'] or UNKNOWN
    nc_file.CAST_NUMBER = self.globals['CASTNO'] or UNKNOWN
    nc_file.BOTTOM_DEPTH_METERS = nc.simplest_str(float(self.globals['DEPTH']))
    nc_file.Creation_Time = libcchdo.fns.strftime_iso(datetime.datetime.utcnow())
    nc_file.ORIGINAL_HEADER = self.globals['header']
    nc_file.WOCE_CTD_FLAG_DESCRIPTION = WOCE_CTD_FLAG_DESCRIPTION

    # Coordinate variables
    if 'TIME' not in self.globals:
        raise AttributeError('(XXX) "TIME" not in self.globals; abort')
    else:
        var_time = nc_file.createVariable('time', 'i', ('time', ))
        var_time.long_name = 'time'
        var_time.units = 'minutes since %s' % libcchdo.fns.strftime_iso(nc.EPOCH)
        var_time.data_min = nc.minutes_since_epoch(isowocedate)
        var_time.data_max = var_time.data_min
        var_time.C_format = '%10d'
        var_time[:] = var_time.data_min

    if 'LATITUDE' not in self.globals:
        raise AttributeError('(XXX) "LATITUDE" not in self.globals; abort')
    else:
        var_latitude = nc_file.createVariable('latitude', 'f', ('latitude',))
        var_latitude.long_name = 'latitude'
        var_latitude.units = 'degrees_N'
        var_latitude.data_min = float(self.globals['LATITUDE'])
        var_latitude.data_max = var_latitude.data_min
        var_latitude.C_format = '%9.4f'
        var_latitude[:] = var_latitude.data_min

    if 'LONGITUDE' not in self.globals:
        raise AttributeError('(XXX) "LONGITUDE" not in self.globals; abort')
    else:
        var_longitude = nc_file.createVariable('longitude', 'f', ('longitude',))
        var_longitude.long_name = 'longitude'
        var_longitude.units = 'degrees_E'
        var_longitude.data_min = float(self.globals['LONGITUDE'])
        var_longitude.data_max = var_longitude.data_min
        var_longitude.C_format = '%9.4f'
        var_longitude[:] = var_longitude.data_min

    woce_datetime = woce.strftime_woce_date_time(isowocedate)

    if 'DATE' not in self.globals:
        raise AttributeError('(XXX) "DATE" not in self.globals; abort')
    else:
        var_woce_date = nc_file.createVariable('woce_date', 'i', ('time',))
        var_woce_date.long_name = 'WOCE date'
        var_woce_date.units = 'yyyymmdd UTC'
        var_woce_date.data_min = int(woce_datetime[0] or -9)
        var_woce_date.data_max = var_woce_date.data_min
        var_woce_date.C_format = '%8d'
        var_woce_date[:] = var_woce_date.data_min

    if 'TIME' not in self.globals:
        raise AttributeError('(XXX) "TIME" not in self.globals; abort')
    else:
        var_woce_time = nc_file.createVariable('woce_time', 'i2', ('time',))
        var_woce_time.long_name = 'WOCE time'
        var_woce_time.units = 'hhmm UTC'
        var_woce_time.data_min = int(woce_datetime[1] or -9)
        var_woce_time.data_max = var_woce_time.data_min
        var_woce_time.C_format = '%4d'
        var_woce_time[:] = var_woce_time.data_min

    var_station = nc_file.createVariable('station', 'c', ('string_dimension', ))
    var_station.long_name = 'STATION'
    var_station.units = UNSPECIFIED_UNITS
    var_station.C_format = '%s'
    var_station[:] = nc.simplest_str(self.globals['STNNBR']).ljust(len(var_station))
    
    var_cast = nc_file.createVariable('cast', 'c', ('string_dimension', ))
    var_cast.long_name = 'CAST'
    var_cast.units = UNSPECIFIED_UNITS
    var_cast.C_format = '%s'
    var_cast[:] = nc.simplest_str(self.globals['CASTNO']).ljust(len(var_cast))

    # Create data variables and fill them
    for param, column in self.columns.iteritems():
        parameter = column.parameter
        parameter_name = parameter.mnemonic_woce()
        if parameter_name in STATIC_PARAMETERS_PER_CAST:
            continue
        var = nc_file.createVariable(
                  parameter.full_name.encode('ascii', 'replace'), 'f8',
                  ('pressure',))
        var.long_name = parameter.full_name.encode('ascii', 'replace')
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
            vfw = nc_file.createVariable(parameter.name + nc.QC_SUFFIX, 'i2', ('pressure',))
            vfw.long_name = parameter.name + nc.QC_SUFFIX
            vfw[:] = column.flags_woce

    nc_file.close()
    handle.write(tmp.read())
    tmp.close()
