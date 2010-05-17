'''libcchdo.formats.ctd.netcdf'''

import libcchdo

try:
    from netCDF3 import Dataset
except ImportError, e:
    raise ImportError('%s\n%s' % (e,
        ("You should get netcdf4-python from http://code.google.com/p/"
         "netcdf4-python and install the NetCDF 3 module as directed by the "
         "README.")))

def read(self, handle):
    '''How to read a CTD NetCDF file.'''
    filename = handle.name
    handle.close()
    nc_file = Dataset(filename, 'r')
    # Create columns for all the variables and get all the data.
    # Map the nc_ctd variable to drop to skip the variable.
    nc_ctd_var_to_woce_param = {'cast': 'CASTNO',
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
    qc_vars = {}
    # First pass to create columns
    for name, variable in nc_file.variables.items():
        if name.endswith('_QC'):
            qc_vars[nc_ctd_var_to_woce_param[name[:-3]]] = variable
        else:
            name = nc_ctd_var_to_woce_param[name]
            if name is 'drop':
                continue
            self.columns[name] = libcchdo.Column(name)
            self.columns[name].values = variable[:].tolist()
            # CCHDO NetCDFs have STNNBR and CASTNO as an array of characters.
            # Collapse them into a string.
            if name in ['STNNBR', 'CASTNO']:
                self.columns[name].values = [''.join(self.columns[name].values)]
            # Translate string date YYYYMMDD to date object
            if name in ['DATE']:
                string = str(self.columns[name].values[0])
                self.columns[name].values[0] = '%s-%s-%s' % (string[0:4], string[4:6], string[6:8])
            # If the column has only one data point it should be in the globals
            if len(self.columns[name].values) <= 1:
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
    globals_to_rename_as = {'CAST_NUMBER': 'CASTNO',
                            'STATION_NUMBER': 'STNNBR',
                            'BOTTOM_DEPTH_METERS': 'DEPTH',
                            'WOCE_ID': 'SECT_ID',
                            'EXPOCODE': 'EXPOCODE',
                           }
    for g, param in globals_to_rename_as.items():
        self.globals[param] = str(global_attrs[g])
    # Get stamp
    self.stamp = global_attrs['ORIGINAL_HEADER']
    # Clean up
    nc_file.close()

#def write(self, handle): #TODO
#  '''How to write a CTD NetCDF file.'''
