'''libcchdo.formats.ctd.netcdf'''

import libcchdo
import datetime
import tempfile
from warnings import warn
import sys

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


QC_SUFFIX = '_QC'


def read(self, handle):
    '''How to read a CTD NetCDF file.'''
    filename = handle.name
    nc_file = nc.Dataset(filename, 'r')
    # Create columns for all the variables and get all the data.
    # Map the nc_ctd variable to drop to skip the variable.
    qc_vars = {}
    # First pass to create columns
    for name, variable in nc_file.variables.items():
        if name.endswith(QC_SUFFIX):
            qc_vars[NC_CTD_VAR_TO_WOCE_PARAM[name[:-len(QC_SUFFIX)]]] = variable
        elif name == "sampno" or name == "btlnbr": #XXX
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

    # Get stamp
    self.stamp = global_attrs['ORIGINAL_HEADER']

    # Clean up
    nc_file.close()


WOCE_CTD_FLAG_DESCRIPTION = """\
::1=Not calibrated
2=Acceptable measurement
3=Questionable measurement
4=Bad measurement
5=Not reported
6=Interpolated over >2 dbar interval
7=Despiked
8=Not assigned for CTD data
9=Not sampled::""".replace("\n", ":")


def netcdf_variable_name_from_column(column):
    if not column.parameter.description:
        warn("Bad parameter description %s" % column.parameter)
        return None
    n = column.parameter.description.lower()
    n = n.replace("ctd ", "")
    return n.replace("(", "_").replace(")", "_").replace(" ", "_")


def write(self, handle):
    '''How to write a CTD NetCDF file.'''
    tmp = tempfile.NamedTemporaryFile()
    strdate = str(self.globals["DATE"])
    strtime = str(self.globals["TIME"])
    isowocedate = datetime.datetime(
            int(strdate[:4]), int(strdate[4:6]), int(strdate[6:]),
            int(strtime[:2]), int(strtime[2:]))
    nc_file = nc.Dataset(tmp.name, "w", format="NETCDF3_CLASSIC")
    nc_file.EXPOCODE = self.globals["EXPOCODE"]
    nc_file.Conventions = "COARDS/WOCE"
    nc_file.WOCE_VERSION = "3.0"
    nc_file.WOCE_ID = self.globals["SECT"] if "SECT" in self.globals else \
                          self.globals["SECT_ID"] if "SECT_ID" in \
                          self.globals else ""
    nc_file.DATA_TYPE = "WOCE CTD"
    nc_file.STATION_NUMBER = self.globals["STNNBR"]
    nc_file.CAST_NUMBER = self.globals["CASTNO"]
    nc_file.BOTTOM_DEPTH_METERS = int(self.globals["DEPTH"])
    nc_file.Creation_Time = '%s %sZ' % (libcchdo.LIBVER,
                                        datetime.datetime.utcnow().isoformat())
    nc_file.ORIGINAL_HEADER = "" #TODO
    nc_file.WOCE_CTD_FLAG_DESCRIPTION = WOCE_CTD_FLAG_DESCRIPTION

    # Dimensions
    nc_file.createDimension("time", 1)
    nc_file.createDimension("pressure", len(self))
    nc_file.createDimension("latitude", 1)
    nc_file.createDimension("longitude", 1)
    nc_file.createDimension("string_dimension", 40)

    ### Variables
    # Time
    if "TIME" not in self.globals:
        raise RuntimeError("(XXX) 'TIME' not in self.globals; abort")
    var_time = nc_file.createVariable("time", "i", ("time", ))
    var_time.long_name = "time"
    var_time.units = "minutes since 1980-01-01 00:00:00"
    epoch = datetime.datetime(1980, 1, 1, 0, 0, 0)
    delta = isowocedate - epoch
    minutes = int(delta.days * 24 * 60 + delta.seconds / 60 + \
                  delta.microseconds / 60 / 1e9)
    var_time.data_min = minutes
    var_time.data_max = minutes
    var_time.C_format = "%10d"
    # Pressure
    if "CTDPRS" not in self.columns:
        raise RuntimeError("(XXX) 'CTDPRS' not in self.columns; abort")
    var_pressure = nc_file.createVariable("pressure", "d", ("pressure", ))
    var_pressure.long_name = "pressure"
    var_pressure.units = "dbar"
    var_pressure.positive = "down"
    ctdprs = map(libcchdo.fns.identity_or_oob, self.columns["CTDPRS"].values)
    var_pressure.data_min = min(ctdprs)
    var_pressure.data_max = max(ctdprs)
    var_pressure.C_format = "%8.1f"
    var_pressure.WHPO_Variable_Name = "CTDPRS"
    var_pressure.OBS_QC_VARIABLE = "pressure_QC"
    # Pressure QC
    var_pressure_qc = nc_file.createVariable("pressure_QC", "i",
            ("pressure", ))
    var_pressure_qc.long_name = "pressure_QC_flag"
    var_pressure_qc.units = "woce_flags"
    var_pressure_qc.C_format = "%1d"
    # Temperature
    if "CTDTMP" not in self.columns:
        raise RuntimeError("(XXX) 'CTDTMP' not in self.columns; abort")
    var_temperature = nc_file.createVariable("temperature", "d",
            ("pressure", ))
    var_temperature.long_name = "temperature"
    var_temperature.units = "its-90"
    ctdtmp = map(libcchdo.fns.identity_or_oob, self.columns["CTDTMP"].values)
    var_temperature.data_min = min(ctdtmp)
    var_temperature.data_max = max(ctdtmp)
    var_temperature.C_format = "%8.4f"
    var_temperature.WHPO_Variable_Name = "CTDTMP"
    var_temperature.OBS_QC_VARIABLE = "temperature_QC"
    # Temperature QC
    var_temperature_qc = nc_file.createVariable("temperature_QC", "i",
            ("pressure", ))
    var_temperature_qc.long_name = "temperature_QC_flag"
    var_temperature_qc.units = "woce_flags"
    var_temperature_qc.C_format = "%1d"
    # Salinity
    if "CTDSAL" not in self.columns:
        raise RuntimeError("(XXX) 'CTDSAL' not in self.columns; abort")
    var_salinity = nc_file.createVariable("salinity", "d", ("pressure", ))
    var_salinity.long_name = "salinity"
    var_salinity.units = "pss-78"
    ctdsal = map(libcchdo.fns.identity_or_oob, self.columns["CTDSAL"].values)
    var_salinity.data_min = min(ctdsal)
    var_salinity.data_max = max(ctdsal)
    var_salinity.C_format = "%8.4f"
    var_salinity.WHPO_Variable_Name = "CTDSAL"
    var_salinity.OBS_QC_VARIABLE = "salinity_QC"
    # Salinity QC
    var_salinity_qc = nc_file.createVariable("salinity_QC", "i",
            ("pressure", ))
    var_salinity_qc.long_name = "salinity_QC_flag"
    var_salinity_qc.units = "woce_flags"
    var_salinity_qc.C_format = "%1d"
    # Oxygen
    if "CTDOXY" not in self.columns:
        raise "(XXX) 'oxygen' not in self.columns; abort"
    var_oxygen = nc_file.createVariable("oxygen", "d", ("pressure", ))
    var_oxygen.long_name = "oxygen"
    var_oxygen.units = "umol/kg"
    ctdoxy = map(libcchdo.fns.identity_or_oob, self.columns["CTDOXY"].values)
    var_oxygen.data_min = min(ctdoxy)
    var_oxygen.data_max = max(ctdoxy)
    var_oxygen.C_format = "%8.1f"
    var_oxygen.WHPO_Variable_Name = "CTDOXY"
    var_oxygen.OBS_QC_VARIABLE = "oxygen_QC"
    # Oxygen QC
    var_oxygen_qc = nc_file.createVariable("oxygen_QC", "i",
            ("pressure", ))
    var_oxygen_qc.long_name = "oxygen_QC_flag"
    var_oxygen_qc.units = "woce_flags"
    var_oxygen_qc.C_format = "%1d"
    # Latitude
    if "LATITUDE" not in self.globals:
        raise "(XXX) 'LATITUDE' not in self.globals; abort"
    var_latitude = nc_file.createVariable("latitude", "d", ("latitude", ))
    var_latitude.long_name = "latitude"
    var_latitude.units = "degrees_N"
    var_latitude.data_min = float(self.globals["LATITUDE"])
    var_latitude.data_max = float(self.globals["LATITUDE"])
    var_latitude.C_format = "%9.4f"
    # Longitude
    if "LONGITUDE" not in self.globals:
        raise "(XXX) 'LONGITUDE' not in self.globals; abort"
    var_longitude = nc_file.createVariable("longitude", "d", ("longitude", ))
    var_longitude.long_name = "longitude"
    var_longitude.units = "degrees_E"
    var_longitude.data_min = float(self.globals["LONGITUDE"])
    var_longitude.data_max = float(self.globals["LONGITUDE"])
    var_longitude.C_format = "%9.4f"
    # WOCE date
    if "DATE" not in self.globals:
        raise "(XXX) 'DATE' not in self.globals; abort"
    var_woce_date = nc_file.createVariable("woce_date", "i", ("time", ))
    var_woce_date.long_name = "WOCE date"
    var_woce_date.units = "yyyymmdd UTC"
    var_woce_date.data_min = float(self.globals["DATE"])
    var_woce_date.data_max = float(self.globals["DATE"])
    var_woce_date.C_format = "%8d"
    # WOCE time
    if "TIME" not in self.globals:
        raise "(XXX) 'TIME' not in self.globals; abort"
    var_woce_time = nc_file.createVariable("woce_time", "i", ("time", ))
    var_woce_time.long_name = "WOCE time"
    var_woce_time.units = "hhmm UTC"
    var_woce_time.data_min = float(self.globals["TIME"])
    var_woce_time.data_max = float(self.globals["TIME"])
    var_woce_time.C_format = "%4d"
    # Station
    var_station = nc_file.createVariable("station", "c",
            ("string_dimension", ))
    var_station.long_name = "STATION"
    var_station.units = "unspecified"
    var_station.C_format = "%s"
    # Cast
    var_cast = nc_file.createVariable("cast", "c", ("string_dimension", ))
    var_cast.long_name = "CAST"
    var_cast.units = "unspecified"
    var_cast.C_format = "%s"
    ##################################################
    # Sample FIXME,XXX-XXX
    var_sampno = nc_file.createVariable("sampno", "c", ("string_dimension", ))
    var_sampno.long_name = "SAMPNO"
    var_sampno.units = "unspecified"
    var_sampno.C_format = "%s"
    # Bottle FIXME,XXX-XXX
    var_btlnbr = nc_file.createVariable("btlnbr", "c", ("string_dimension", ))
    var_btlnbr.long_name = "BTLNBR"
    var_btlnbr.units = "unspecified"
    var_btlnbr.C_format = "%s"
    ##################################################
    #####TODO add 1 hr to cast times

    var_time[:] = minutes
    var_latitude[:] = [self.globals["LATITUDE"]]
    var_longitude[:] = [self.globals["LONGITUDE"]]
    var_woce_date[:] = [int(self.globals["DATE"])]
    var_woce_time[:] = int(self.globals["TIME"])
    var_station[:] = self.globals["STNNBR"].ljust(len(var_station))
    var_cast[:] = self.globals["CASTNO"].ljust(len(var_cast))

    for column in self.columns.values():
        #print >> sys.stderr, str(col)
        name = netcdf_variable_name_from_column(column)
        if not name:
            tmp.close() # FIXME?
            return

        var = nc_file.variables[name] if name in nc_file.variables else \
              nc_file.createVariable(name, "f8", ("pressure", ))
        #TODO other stuff
        var[:] = map(libcchdo.fns.identity_or_oob, column.values)
        if column.is_flagged_woce():
            var = nc_file.variables[name + "_QC"]
            var[:] = map(lambda x: libcchdo.fns.identity_or_oob(x, 9),
                    column.flags_woce)

    nc_file.close()
    handle.write(tmp.read())
    tmp.close()
