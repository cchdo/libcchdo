"""Handler for CTD NetCDF files"""

import datetime
import tempfile
import sys

from ... import LOG
from ... import fns
from ...model import datafile
from .. import woce
from .. import netcdf as nc


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
    'TRANSM': 'XMISS', 
}


GLOBALS_TO_RENAME_AS = {
    'CAST_NUMBER': 'CASTNO',
    'STATION_NUMBER': 'STNNBR',
    'BOTTOM_DEPTH_METERS': 'DEPTH',
    'WOCE_ID': 'SECT_ID',
    'EXPOCODE': 'EXPOCODE',
}


def read(self, handle):
    '''How to read a CTD NetCDF file.'''
    filename = handle.name
    nc_file = nc.Dataset(filename, 'r')

    nc.check_variable_ranges(nc_file)

    # Create columns for all the variables and get all the data.
    # Map the nc_ctd variable to drop to skip the variable.
    qc_vars = {}
    # First pass to create columns
    for name, variable in nc_file.variables.items():
        if name.endswith(nc.QC_SUFFIX):
            pname = name[:-len(nc.QC_SUFFIX)]
            try:
                qc_vars[NC_CTD_VAR_TO_WOCE_PARAM[pname]] = variable
            except KeyError:
                LOG.warn(
                    'Missing NetCDF to WOCE parameter mapping for %s' % pname)
        elif name == 'sampno' or name == 'btlnbr': #XXX
            continue #XXX
        else:
            name = NC_CTD_VAR_TO_WOCE_PARAM.get(name, 'drop')

            if name == 'drop':
                continue

            self.columns[name] = datafile.Column(name)
            self.columns[name].values = variable[:].tolist()

            # Do some transformations from NetCDF pecularities to standard data format
            if name in ['STNNBR', 'CASTNO']:
                # CCHDO NetCDFs have STNNBR and CASTNO as an array of characters.
                # Collapse them into a string.
                self.columns[name].values = [''.join(filter(None, self.columns[name].values))]
            elif name in ['DATE']:
                # Translate string date YYYYMMDD to date object
                string = str(self.columns[name].values[0])
                self.columns[name].values[0] = '%s%s%s' % \
                    (string[0:4], string[4:6], string[6:8])
            if name == 'CTDSAL':
                self.columns[name].values = map(
                    lambda x: None if fns.equal_with_epsilon(-9.99, x) \
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


def _create_common_variables(df, nc_file, woce_datetime):
    if 'LATITUDE' not in df.globals:
        raise AttributeError('"LATITUDE" not in globals; abort')
    else:
        latitude = float(df.globals['LATITUDE'])

    if 'LONGITUDE' not in df.globals:
        raise AttributeError('"LONGITUDE" not in globals; abort')
    else:
        longitude = float(df.globals['LONGITUDE'])

    stnnbr = df.globals.get('STNNBR', nc.UNKNOWN)
    castno = df.globals.get('CASTNO', nc.UNKNOWN)

    nc.create_common_variables(
        nc_file, latitude, longitude, woce_datetime, stnnbr, castno)


def write(self, handle):
    '''How to write a CTD NetCDF file.'''
    if 'DATE' not in self.globals:
        raise AttributeError('"DATE" not in globals; abort')
    if 'TIME' not in self.globals:
        raise AttributeError('"TIME" not in globals; abort')
    strdate = str(self.globals.get('DATE', nc.UNKNOWN))
    strtime = str(self.globals.get('TIME', nc.UNKNOWN))
    woce_datetime = woce.strptime_woce_date_time(strdate, strtime)

    tmp = tempfile.NamedTemporaryFile()
    nc_file = nc.Dataset(tmp.name, 'w', format='NETCDF3_CLASSIC')

    nc.define_dimensions(nc_file, len(self))

    # Define dataset attributes
    nc.define_attributes(
        nc_file, 
        self.globals.get('EXPOCODE', nc.UNKNOWN),
        self.globals.get('SECT_ID', nc.UNKNOWN),
        'WOCE CTD',
        self.globals.get('STNNBR', nc.UNKNOWN),
        self.globals.get('CASTNO', nc.UNKNOWN),
        int(self.globals.get('DEPTH', woce.FILL_VALUE)),
        )

    nc_file.ORIGINAL_HEADER = self.globals.get('header', '')
    nc_file.WOCE_CTD_FLAG_DESCRIPTION = woce.CTD_FLAG_DESCRIPTION

    nc.create_and_fill_data_variables(self, nc_file)
    _create_common_variables(self, nc_file, woce_datetime)

    nc_file.close()
    handle.write(tmp.read())
    tmp.close()
