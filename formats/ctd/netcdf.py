'''libcchdo.formats.ctd.netcdf'''

import libcchdo


try:
    from netCDF3 import Dataset
except ImportError, e:
    raise ImportError('%s\n%s' % (e,
        ("You should get netcdf4-python from http://code.google.com/p/"
         "netcdf4-python and install the NetCDF 3 module as directed by the "
         "README.")))


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


QC_SUFFIX = '_QC'


def read(self, handle):
    '''How to read a CTD NetCDF file.'''
    filename = handle.name
    handle.close()
    nc_file = Dataset(filename, 'r')
    # Create columns for all the variables and get all the data.
    # Map the nc_ctd variable to drop to skip the variable.
    qc_vars = {}
    # First pass to create columns
    for name, variable in nc_file.variables.items():
        if name.endswith(QC_SUFFIX):
            qc_vars[NC_CTD_VAR_TO_WOCE_PARAM[name[:-len(QC_SUFFIX)]]] = variable
        else:
            name = NC_CTD_VAR_TO_WOCE_PARAM[name]

            if name is 'drop':
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
                self.columns[name].values[0] = '%s-%s-%s' % (string[0:4], string[4:6], string[6:8])

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

    # Get stamp
    self.stamp = global_attrs['ORIGINAL_HEADER']

    # Clean up
    nc_file.close()

#def write(self, handle): #TODO
#  '''How to write a CTD NetCDF file.'''
