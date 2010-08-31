import datetime
import os
from warnings import warn

import libcchdo
import libcchdo.formats.netcdf as nc
import libcchdo.formats.woce


NETCDF_EPOCH = datetime.datetime(1980, 1, 1, 0, 0, 0)


NC_BOTTLE_VAR_TO_WOCE_PARAM = {
    'pressure': 'CTDPRS',
    'temperature': 'CTDTMP',
    'oxygen': 'CTDOXY',
    'salinity': 'CTDSAL',
    'silicate': 'SILCAT',
    'nitrate': 'NITRAT',
    'nitrite': 'NITRIT',
    'freon_11': 'CFC-11',
    'partial_co2_temperature': 'PCO2TMP',
    'alkalinity': 'ALKALI',
    'freon_113': 'CFC113',
    'total_carbon': 'TCARBN',
    'ctd_raw': 'CTDRAW',
    'freon_12': 'CFC-12',
    'theta': 'THETA',
    'bottle_oxygen': 'OXYGEN',
    'bottle_oxygen': 'OXYGEN',
    'phosphate': 'PHSPHT',
    'partial_pressure_of_co2': 'PCO2',
    'bottle_salinity': 'SALNTY',
    'tritium': 'TRITUM',
    'helium': 'HELIUM',
    'delta_helium_3': 'DELHE3',
    'tritium_error': 'TRITER',
}


WOCE_BOTTLE_FLAG_DESCRIPTION = ':'.join([
    ':',
    '1 = Bottle information unavailable.',
    '2 = No problems noted.',
    '3 = Leaking.',
    '4 = Did not trip correctly.',
    '5 = Not reported.',
    ('6 = Significant discrepancy in measured values between Gerard '
     'and Niskin bottles.'),
    '7 = Unknown problem.',
    ('8 = Pair did not trip correctly. Note that the Niskin bottle '
     'can trip at an unplanned depth while the Gerard trips '
     'correctly and vice versa.'),
    '9 = Samples not drawn from this bottle.',
    "\n"])


WOCE_WATER_SAMPLE_FLAG_DESCRIPTION = ':'.join([
    ':',
    ('1 = Sample for this measurement was drawn from water bottle '
     'but analysis not received.'),
    '2 = Acceptable measurement.',
    '3 = Questionable measurement.',
    '4 = Bad measurement.',
    '5 = Not reported.',
    '6 = Mean of replicate measurements.',
    '7 = Manual chromatographic peak measurement.',
    '8 = Irregular digital chromatographic peak integration.',
    '9 = Sample not drawn for this measurement from this bottle.',
    "\n"])


VARATTRS = frozenset(('time', 'latitude', 'longitude', 'woce_date',
                      'woce_time', 'cast', 'station', ))


def read(self, handle):
    """How to read a Bottle NetCDF COARDS file."""
    filename = handle.name
    nc_file = nc.Dataset(filename, 'r')
    
    attrs = nc_file.__dict__
    expocode = attrs['EXPOCODE']
    self.header = attrs['ORIGINAL_HEADER']
    station = attrs['STATION_NUMBER']
    cast = attrs['CAST_NUMBER']
    bottle_numbers = attrs['BOTTLE_NUMBERS'].split()
    bottle_flags = attrs['BOTTLE_QUALITY_CODES'][:]
    section_id = attrs['WOCE_ID']
    bottom_depth = attrs['BOTTOM_DEPTH_METERS']

    vars = nc_file.variables

    time = vars['time'][:][0]
    latitude = vars['latitude'][:][0]
    longitude = vars['longitude'][:][0]
    dtime = libcchdo.formats.woce.strptime_woce_date_time(
        vars['woce_date'][:][0], vars['woce_time'][:][0])

    calculated_time = NETCDF_EPOCH + datetime.timedelta(minutes=int(time))
    # Probably should trust dtime more because it is translated directly
    # from WOCE time.
    if dtime != calculated_time:
        warn(('Datetime declarations in Bottle NetCDF file '
              'do not match (%s, %s)') % (dtime, calculated_time))

    varstation = ''.join(filter(None, vars['station'][:].tolist()))
    varcast = ''.join(filter(None, vars['cast'][:].tolist()))

    if varstation != station:
        warn(('Station declarations in Bottle NetCDF file '
              'do not match (%s, %s)') % (station, varstation))

    if varcast != cast:
        warn(('Cast declarations in Bottle NetCDF file '
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
        self.columns[g].values[vlo:vhi] = [var[1]] * dimensions

    self.columns['BTLNBR'].values[vlo:vhi] = bottle_numbers

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
            self.columns[name].values[vlo:vhi] = variable[:].tolist()

            # Quick conversions to uniform data format
            self.columns[name].values[vlo:vhi] = map(
                libcchdo.fns.in_band_or_none,
                self.columns[name].values[vlo:vhi])

    # Second pass to put in flags
    for name, variable in qc_vars.items():
        if name in self.columns:
            self.columns[name].flags_woce[vlo:vhi] = variable[:].tolist()
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


def write(self, handle):
    """How to write a Bottle NetCDF COARDS file."""
    # This time, the handle is actually a path to a tempdir to give to the
    # NetCDF library to write in.
    expocode = self.columns['EXPOCODE'][0]
    station = self.columns['STNNBR'][0].rjust(5, '0')
    cast = self.columns['CASTNO'][0].rjust(5, '0')
    filename = '_'.join(expocode, station, cast, 'hy1') + '.nc'
    fullpath = os.path.join(handle, filename)

    nc_file = nc.Dataset(fullpath, 'w')

    # Write dimension variables
    dim_time = nc_file.createDimension('time', 1)
    dim_lat = nc_file.createDimension('latitude', 1)
    dim_lng = nc_file.createDimension('longitude', 1)
    dims_variable = (dim_time, dim_lat, dim_lng)
    dims_static = (dim_time, dim_lat, dim_lng)

    dim_string = nc_file.createDimension('string_dimension', 10)
    dims_string = (dim_string, dim_time)

    # Sometimes, there's no WOCE Section associated with a certain STNNBR
    # and CASTNO. In that case, let the user known it's an UNKNOWN section
    sect = self.columns['SECT_ID'][0] or 'UNKNOWN'

    nc_file.EXPOCODE = expocode
    nc_file.Conventions = 'COARDS/WOCE'
    nc_file.WOCE_VERSION = '3.0'
    nc_file.WOCE_ID = sect
    nc_file.DATA_TYPE = 'Bottle'
    nc_file.STATION_NUMBER = station
    nc_file.CAST_NUMBER = cast
    nc_file.BOTTOM_DEPTH_METERS = max(self.columns['DEPTH'].values)
    nc_file.BOTTLE_NUMBERS = ' '.join(self.columns['BTLNBR'].values)
    if self.columns['BTLNBR'].is_flagged_woce():
        nc_file.BOTTLE_QUALITY_CODES = ' '.join(
            self.columns['BTLNBR'].flags_woce)
    nc_file.Creation_Time = libcchdo.fns.strftime_iso(datetime.datetime.now())
    header_filter = compile('BOTTLE|db_to_exbot|jjward')
    header = '# Previous stamp: ' + self.stamp + "\n" + "\n".join(
        [x for x in self.header.split("\n") if not header_filter.match(x)])
    nc_file.ORIGINAL_HEADER = header
    nc_file.WOCE_BOTTLE_FLAG_DESCRIPTION = WOCE_BOTTLE_FLAG_DESCRIPTION
    nc_file.WOCE_WATER_SAMPLE_FLAG_DESCRIPTION = WOCE_WATER_SAMPLE_FLAG_DESCRIPTION

    ncvar = {}
    ncflagvar = {}
    for param, column in iter(self.columns):
        parameter = column.parameter
        parameter_name = parameter.mnemonic
        # continue if STATIC_PARAMETERS_PER_CAST.include parameter_name
        # TODO
    var_time = nc_file.createVariable('time', 'f', dims_static)
    var_time.long_name = 'time'
    var_time.units = 'minutes since 1980-01-01 00:00:00'
    var_time.data_min = 0
    var_time.data_max = 0
    var_time.C_format = '%10d'

    var_latitude = nc_file.createVariable('latitude', 'f', dims_static)
    var_latitude.long_name = 'latitude'
    var_latitude.units = 'degrees_N'
    var_latitude.data_min = 0
    var_latitude.data_max = 0
    var_latitude.C_format = '%9.4f'

    var_longitude = nc_file.createVariable('longitude', 'f', dims_static)
    var_longitude.long_name = 'longitude'
    var_longitude.units = 'degrees_E'
    var_longitude.data_min = 0
    var_longitude.data_max = 0
    var_longitude.C_format = '%9.4f'

    var_woce_date = nc_file.createVariable('woce_date', 'i', dims_static)
    var_woce_date.long_name = 'WOCE date'
    var_woce_date.units = 'yyyymdd UTC'
    var_woce_date.data_min = 0 #long min
    var_woce_date.data_max = 0 #long max
    var_woce_date.C_format = '%8d'
    
    var_woce_time = nc_file.createVariable('woce_time', 'i', dims_static)
    var_woce_time.long_name = 'WOCE time'
    var_woce_time.units = 'hhmm UTC'
    var_woce_time.data_min = 0 #long min
    var_woce_time.data_max = 0 #long max
    var_woce_time.C_format = '%4d'
    
    # Hydrographic specific
    
    var_station = nc_file.createVariable('station', 'c', dims_string)
    var_station.long_name = 'STATION'
    var_station.units = 'unspecified'
    var_station.C_format = '%s'
    
    var_cast = nc_file.createVariable('cast', 'c', dims_string)
    var_cast.long_name = 'CAST'
    var_cast.units = 'unspecified'
    var_cast.C_format = '%s'

    # Write out pairs TODO

    datetime = self.columns['DATE'][0]+self.columns['TIME']
    time_from_epoch = datetime # TODO
    cchdo_epoch_offset = datetime.date(1980, 01, 01)
    var_time[:] = (time_from_epoch - cchdo_epoch_offset)

    nc_file.close()
