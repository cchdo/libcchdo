"""Format handlers for water column microstructure data.

"""

from copy import deepcopy
import sys
from datetime import datetime

from libcchdo.formats import netcdf as nc


def pad_str(s, l):
    return s.ljust(l, '\x00')


def create_var(nc_file, varname, strtype, dimensions, fill_value=None):
    if fill_value is None:
        if strtype == 'c':
            fill_value = ' '
        else:
            raise ValueError('Unknown fill value')
    var = nc_file.createVariable(
        varname, strtype, dimensions, fill_value=fill_value)
    return var

    
def fit_pow2(x):
    """Return the smallest power of 2 >= x."""
    i = 1
    while 2 ** i < x:
        i += 1
    return 2 ** i


def strdim(n):
    """Return string representing a dimension. e.g. STRING2"""
    return 'STRING{0}'.format(n)
    

def set_var_str(nc_file, varname, s, comment=None, l=None):
    if l is None:
        l = len(s)
    l = fit_pow2(l)

    if len(s) > l:
        raise ValueError(
            'Given string is too long. {0} < {1}'.format(l, len(s)))

    var = create_var(nc_file, varname, 'c', (strdim(l),))
    if comment is not None:
        var.comment = comment
    var[:] = pad_str(s, l)
    return var


parameter_units = {
    'Kappa': 'm2 s-1',
    'Lon': 'degrees',
    'Eps': 'm2 s-3',
    'p': 'dbars',
    'T': 'degrees C',
    'Lat': 'degrees',
    'Sa': 'PSU',
    'Bottom depth': 'meters',
    'N2': 's-2',
}


def write(self, handle):
    """Write microstructure netCDF format."""
    # We're going to go by profile!

    self.globals['BOTTOM'] = None # TODO ???

    #print >> sys.stderr, 'p', self['pgrid']
    #print >> sys.stderr, 'T', self['tave']
    #print >> sys.stderr, 'Sa', self['s_ave']
    #print >> sys.stderr, 'eps', self['epl']
    #print >> sys.stderr, 'K', [] # TODO ???
    #print >> sys.stderr, 'N2', [] # TODO ???

    #print >> sys.stderr, 'ep1', self['ep1']
    #print >> sys.stderr, 'ep2', self['ep2']

    # Basically write an Argo netCDF file with some adaptations

    with nc.buffered_netcdf(handle, 'w', format='NETCDF3_CLASSIC') as nc_file:
        nc_file.createDimension('DATE_TIME', 14)
        for i in range(1, 9)[::-1]:
            n = 2 ** i
            nc_file.createDimension(strdim(n), n)
        try:
            nc_file.createDimension('N_PROF', 1)
        except RuntimeError:
            raise AttributeError("There is no data to be written.")
        nc_file.createDimension('N_PARAM', 2)
        nc_file.createDimension('N_LEVELS', 2)
        nc_file.createDimension('N_CALIB', 2)
        nc_file.createDimension('N_HISTORY')

        # Argo does these as strings in the data. should we do that or leave
        # them as attributes?

        set_var_str(nc_file, 'DATA_TYPE', 'Microstructure', 'Data type', 16)
        set_var_str(nc_file, 'FORMAT_VERSION', '0.5', 'File format version', 4)
        set_var_str(nc_file, 'HANDBOOK_VERSION', '', 'Data handbook version', 4)

        var = create_var(nc_file, 'REFERENCE_DATE_TIME', 'c', ('DATE_TIME',))
        var.comment = 'Date of reference for Julian days'
        var.conventions = 'YYYYMMDDHHMISS'
        var[:] = '19500101000000'

        var = create_var(nc_file, 'PLATFORM_NUMBER', 'c', ('N_PROF', strdim(8), ))
        var.comment = 'Float unique identifier'
        var.conventions = 'WMO float identifier : A9IIIII'
        var[:] = pad_str('HRP2', 8)

        var = create_var(nc_file, 'PROJECT_NAME', 'c', ('N_PROF', strdim(64), ))
        var.comment = 'Name of the project'
        var[:] = pad_str('DIMES', 64)

        var = create_var(nc_file, 'PI_NAME', 'c', ('N_PROF', strdim(64), ))
        var.comment = 'Name of the principal investigator'
        var[:] = pad_str('Jim Ledwell', 64)

        var = create_var(nc_file, 'STATION_PARAMETERS', 'c', ('N_PROF', 'N_PARAM', strdim(16), ))
        var.long_name = 'List of available parameters for the station'
        var.conventions = 'Argo reference table 3'
        var[:] = [pad_str('', 16), pad_str('', 16)]

        var = create_var(nc_file, 'CYCLE_NUMBER', 'i', ('N_PROF',), fill_value=99999)
        var.long_name = 'Float cycle number'
        var.conventions = '0..N, 0 : launch cycle (if exists), 1 : first complete cycle'
        var[:] = 1

        var = create_var(nc_file, 'DIRECTION', 'c', ('N_PROF',))
        var.long_name = 'Direction of the station profiles'
        var.conventions = 'A: ascending profiles, D: descending profiles'
        var[:] = 'D'

        var = create_var(nc_file, 'DATA_CENTRE', 'c', ('N_PROF', strdim(2), ))
        var.long_name = 'Data centre in charge of float data processing'
        var.conventions = 'Argo reference table 4'
        var[:] = pad_str('', 2)

        var = create_var(nc_file, 'DATE_CREATION', 'c', ('DATE_TIME',))
        var.comment = 'Date of file creation'
        var.conventions = 'YYYYMMDDHHMISS'
        var[:] = datetime.utcnow().strftime('%Y%m%d%H%M%S')

        var = create_var(nc_file, 'DATE_UPDATE', 'c', ('DATE_TIME',))
        var.comment = 'Date of update of this file'
        var.conventions = 'YYYYMMDDHHMISS'
        var[:] = datetime.utcnow().strftime('%Y%m%d%H%M%S')

        var = create_var(nc_file, 'DC_REFERENCE', 'c', ('N_PROF', strdim(32), ))
        var.long_name = 'Station unique identifier in data centre'
        var.conventions = 'Data centre conventions'
        var[:] = pad_str('', 32)

        var = create_var(nc_file, 'DATA_STATE_INDICATOR', 'c', ('N_PROF', strdim(4), ))
        var.long_name = 'Degree of processing the data have passed through'
        var.conventions = 'Argo reference table 6'
        var[:] = pad_str('', 4)

        var = create_var(nc_file, 'DATA_MODE', 'c', ('N_PROF',))
        var.long_name = 'Delayed mode or real time data'
        var.conventions = 'R : real time; D : delayed mode; A : real time with adjustment'
        var[:] = 'D'

        var = create_var(nc_file, 'INST_REFERENCE', 'c', ('N_PROF', strdim(64), ))
        var.long_name = 'Instrument type'
        var.conventions = 'Brand, type, serial number'
        var[:] = pad_str('HRP, 2', 64)

        var = create_var(nc_file, 'WMO_INST_TYPE', 'c', ('N_PROF', strdim(64), ))
        var.long_name = 'Coded instrument type'
        var.conventions = 'Argo reference table 8'
        var[:] = pad_str('', 64)

        var = create_var(nc_file, 'JULD', 'd', ('N_PROF',), fill_value=999999.)
        var.long_name = 'Julian day (UTC) of the station relative to REFERENCE_DATE_TIME'
        var.units = 'days since 1950-01-01 00:00:00 UTC'
        var.conventions = 'Relative julian days with decimal part (as parts of day)'
        #var[:] = 

        var = create_var(nc_file, 'JULD_QC', 'c', ('N_PROF',))
        var.long_name = 'Quality on Date and Time'
        var.conventions = 'Argo reference table 2'
        var[:] = ' '

        var = create_var(nc_file, 'JULD_LOCATION', 'd', ('N_PROF',), fill_value=999999.)
        var.long_name = 'Julian day (UTC) of the location relative to REFERENCE_DATE_TIME'
        var.units = 'days since 1950-01-01 00:00:00 UTC'
        var.conventions = 'Relative julian days with decimal part (as parts of day)'
        #var[:] = 

        var = create_var(nc_file, 'LATITUDE', 'd', ('N_PROF',), fill_value=99999.)
        var.long_name = 'Latitude of the station, best estimate'
        var.units = 'degree_north'
        var.valid_min = -90.
        var.valid_max = 90.
        var[:] = self.globals['LATITUDE']

        var = create_var(nc_file, 'LONGITUDE', 'd', ('N_PROF',), fill_value=99999.)
        var.long_name = 'Longitude of the station, best estimate'
        var.units = 'degree_east'
        var.valid_min = -180.
        var.valid_max = 180.
        var[:] = self.globals['LONGITUDE']

        var = create_var(nc_file, 'POSITION_QC', 'c', ('N_PROF',))
        var.long_name = 'Quality on position (latitude and longitude)'
        var.conventions = 'Argo reference table 2'
        var[:] = ' '

        var = create_var(nc_file, 'POSITIONING_SYSTEM', 'c', ('N_PROF', strdim(8), ))
        var.long_name = 'Positioning system'
        var[:] = pad_str('', 8)

        var = create_var(nc_file, 'PROFILE_PRES_QC', 'c', ('N_PROF',))
        var.long_name = 'Global quality flag of PRES profile'
        var.conventions = 'Argo reference table 2a'
        var[:] = ' '

        var = create_var(nc_file, 'PROFILE_TEMP_QC', 'c', ('N_PROF',))
        var.long_name = 'Global quality flag of TEMP profile'
        var.conventions = 'Argo reference table 2a'
        var[:] = ' '

        def create_parameter(nc_file, name, long_name, units, valid_min,
                             valid_max, comment, C_format, FORTRAN_format,
                             resolution, fill_value):
            var = create_var(nc_file, name, 'f', ('N_PROF', 'N_LEVELS', ), fill_value=fill_value)
            var.long_name = long_name
            var.units = units
            var.valid_min = valid_min
            var.valid_max = valid_max
            var.comment = comment
            var.C_format = C_format
            var.FORTRAN_format = FORTRAN_format
            var.resolution = resolution

            var_qc = create_var(nc_file, name + '_QC', 'c', ('N_PROF', 'N_LEVELS', ))
            var_qc.long_name = 'quality flag'
            var_qc.conventions = 'Argo reference table 2'

            name_adj = name + '_ADJUSTED'

            var_adj = create_var(nc_file, name_adj, 'f', ('N_PROF', 'N_LEVELS', ), fill_value=fill_value)
            var_adj.long_name = long_name
            var_adj.units = units
            var_adj.valid_min = valid_min
            var_adj.valid_max = valid_max
            var_adj.comment = comment
            var_adj.C_format = C_format
            var_adj.FORTRAN_format = FORTRAN_format
            var_adj.resolution = resolution

            var_adj_qc = create_var(nc_file, name_adj + '_QC', 'c', ('N_PROF', 'N_LEVELS', ))
            var_adj_qc.long_name = 'quality flag'
            var_adj_qc.conventions = 'Argo reference table 2'

            var_adj_error = create_var(nc_file, name_adj + '_ERROR', 'f', ('N_PROF', 'N_LEVELS', ), fill_value=fill_value)
            var_adj_error.long_name = long_name
            var_adj_error.units = units
            var_adj_error.comment = 'Contains the error on the adjusted values as determined by the delayed mode QC process.'
            var_adj_error.C_format = C_format
            var_adj_error.FORTRAN_format = FORTRAN_format
            var_adj_error.resolution = resolution
            return (var, var_qc, var_adj, var_adj_qc, var_adj_error)

        create_parameter(
            nc_file, 'LAT',
            long_name='latitude',
            units='degrees_north',
            valid_min=-90.,
            valid_max=90.,
            comment='Assigned after cast',
            C_format='%2.6f',
            FORTRAN_format='F2.6',
            resolution=0.0001,
            fill_value=99.)

        create_parameter(
            nc_file, 'LON',
            long_name='longitude',
            units='degrees_east',
            valid_min=-180.,
            valid_max=180.,
            comment='Assigned after cast',
            C_format='%3.6f',
            FORTRAN_format='F3.6',
            resolution=0.0001,
            fill_value=999.)

        create_parameter(
            nc_file, 'BOT',
            long_name='bottom_depth',
            units='meters',
            valid_min=0.,
            valid_max=7000.,
            comment='Assigned after cast',
            C_format='%4i',
            FORTRAN_format='I4',
            resolution=1,
            fill_value=9999.)

        create_parameter(
            nc_file, 'PRES',
            long_name='SEA PRESSURE',
            units='decibar',
            valid_min=0.,
            valid_max=12000.,
            comment='In situ measurement, sea surface = 0',
            C_format='%7.1f',
            FORTRAN_format='F7.1',
            resolution=0.1,
            fill_value=99999.)

        create_parameter(
            nc_file, 'TEMP',
            long_name='SEA TEMPERATURE IN SITU ITS-90 SCALE',
            units='degree_Celsius',
            valid_min=-2.,
            valid_max=40.,
            comment='In situ measurement',
            C_format='%9.3f',
            FORTRAN_format='F9.3',
            resolution=0.001,
            fill_value=99999.)

        create_parameter(
            nc_file, 'Sal',
            long_name='salininty',
            units='psu',
            valid_min=0.,
            valid_max=40.,
            comment='Derived from in situ measurements of conductivity and temperature',
            C_format='%9.3f',
            FORTRAN_format='F9.3',
            resolution=0.001,
            fill_value=9999.)

        create_parameter(
            nc_file, 'N2',
            long_name='n2',
            units='s^-2',
            valid_min=-2.,
            valid_max=40.,
            comment='calculated; Brunt-Vaisala',
            C_format='%9.3f',
            FORTRAN_format='F9.3',
            resolution=0.001,
            fill_value=99999.)

        create_parameter(
            nc_file, 'eps',
            long_name='epsilon',
            units='m^2 * s^-3',
            valid_min=1.e-15,
            valid_max=10.,
            comment='calculated',
            C_format='%9.3f',
            FORTRAN_format='F9.3',
            resolution=0.001,
            fill_value=99999.)

        # eddy diffusivity
        create_parameter(
            nc_file, 'Kappa',
            long_name='kappa',
            units='m^2 * s^-1',
            valid_min=1.e-7,
            valid_max=20.,
            comment='calculated',
            C_format='%9.3f',
            FORTRAN_format='F9.3',
            resolution=0.001,
            fill_value=99999.)

        # Units as given by Amy Waterhouse 2012-11-07
        # Lat (latitude): degrees
        # Lon (longitude): degrees
        # Bottom depth: meters
        # p (pressure): dbars
        # T (temperature): degrees C
        # Sa (salinity) -- does salinity have units these days? PSU
        # N2 : s-2
        # Eps (epsilon): [m2 s-3]
        # Kappa: m2 s-1
        #
        # P, T are measured directly. Bottom depth is likely input manually coming from
        # a ship board echosounder. Also, lat / lon input after the fact.
        #
        # Sa is derived from the conductivity - temperature profiles.
        #
        # Kappa, eps, N2 are calculated by me.
        #
        # Acceptable range - for eps, anything below 1e-15 is suspect. for Kappa,
        # anything below 1e-7 is also at the noise level. High ends of Kappa would be
        # anything over 10.

        var = create_var(nc_file, 'PARAMETER', 'c', ('N_PROF', 'N_CALIB', 'N_PARAM', strdim(16), ))
        var.long_name = "List of parameters with calibration information"
        var.conventions = "Argo reference table 3"

        var = create_var(nc_file, 'SCIENTIFIC_CALIB_EQUATION', 'c', ('N_PROF', 'N_CALIB', 'N_PARAM', strdim(256), ))
        var.long_name = "Calibration equation for this parameter"

        var = create_var(nc_file, 'SCIENTIFIC_CALIB_COEFFICIENT', 'c', ('N_PROF', 'N_CALIB', 'N_PARAM', strdim(256), ))
        var.long_name = "Calibration coefficients for this equation"

        var = create_var(nc_file, 'SCIENTIFIC_CALIB_COMMENT', 'c', ('N_PROF', 'N_CALIB', 'N_PARAM', strdim(256), ))
        var.long_name = "Comment applying to this parameter calibration"

        var = create_var(nc_file, 'CALIBRATION_DATE', 'c', ('N_PROF', 'N_CALIB', 'N_PARAM', 'DATE_TIME', ))

        var = create_var(nc_file, 'HISTORY_INSTITUTION', 'c', ('N_HISTORY', 'N_PROF', strdim(4), ))
        var.long_name = "Institution which performed action"
        var.conventions = "Argo reference table 4"

        var = create_var(nc_file, 'HISTORY_STEP', 'c', ('N_HISTORY', 'N_PROF', strdim(4), ))
        var.long_name = "Step in data processing"
        var.conventions = "Argo reference table 12"

        var = create_var(nc_file, 'HISTORY_SOFTWARE', 'c', ('N_HISTORY', 'N_PROF', strdim(4), ))
        var.long_name = "Name of software which performed action"
        var.conventions = "Institution dependent"

        var = create_var(nc_file, 'HISTORY_SOFTWARE_RELEASE', 'c', ('N_HISTORY', 'N_PROF', strdim(4), ))
        var.long_name = "Version/release of software which performed action"
        var.conventions = "Institution dependent"

        var = create_var(nc_file, 'HISTORY_REFERENCE', 'c', ('N_HISTORY', 'N_PROF', strdim(64), ))
        var.long_name = "Reference of database"
        var.conventions = "Institution dependent"

        var = create_var(nc_file, 'HISTORY_DATE', 'c', ('N_HISTORY', 'N_PROF', 'DATE_TIME', ))
        var.long_name = "Date the history record was created"
        var.conventions = "YYYYMMDDHHMISS"

        var = create_var(nc_file, 'HISTORY_ACTION', 'c', ('N_HISTORY', 'N_PROF', strdim(4), ))
        var.long_name = "Action performed on data"
        var.conventions = "Argo reference table 7"

        var = create_var(nc_file, 'HISTORY_PARAMETER', 'c', ('N_HISTORY', 'N_PROF', strdim(16), ))
        var.long_name = "Station parameter action is performed on"
        var.conventions = "Argo reference table 3"

        var = create_var(nc_file, 'HISTORY_START_PRES', 'f', ('N_HISTORY', 'N_PROF', ), fill_value=99999.)
        var.long_name = "Start pressure action applied on"
        var.units = "decibar"

        var = create_var(nc_file, 'HISTORY_STOP_PRES', 'f', ('N_HISTORY', 'N_PROF', ), fill_value=99999.)
        var.long_name = "Stop pressure action applied on"
        var.units = "decibar"

        var = create_var(nc_file, 'HISTORY_PREVIOUS_VALUE', 'f', ('N_HISTORY', 'N_PROF', ), fill_value=99999.)
        var.long_name = "Parameter/Flag previous value before action"

        var = create_var(nc_file, 'HISTORY_QCTEST', 'c', ('N_HISTORY', 'N_PROF', strdim(16), ))
        var.long_name = "Documentation of tests performed, tests failed (in hex form)"
        var.conventions = "Write tests performed when ACTION=QCP$; tests failed when ACTION=QCF$"

        #var_depth = nc_file.createVariable(
        #    'DEPTH', 'f', ('DEPTH',), fill_value=-99999.0)
        #var_depth.long_name = 'Depth of each measurement'
        #var_depth.standard_name = 'depth'
        #var_depth.units = 'meters'
        #var_depth.valid_min = 0.0
        #var_depth.valid_max = 12000.0
        ## Subject: OceanSITES: more on QC flags, uncertainty, depth
        ## Interpolated from latitude and pressure.
        #var_depth.QC_indicator = 8
        #var_depth.QC_procedure = 2 # See above
        #var_depth.uncertainty = 1.0 # A decibar
        #if version == '1.1':
        #    var_depth.axis = 'down' # oceanic
        #elif version == '1.2':
        #    var_depth.positive = 'down'
        #    var_depth.axis = 'Z'
        #    var_depth.reference = 'sea_level' # TODO is this right?
        #    var_depth.coordinate_reference_frame = 'urn:ogc:crs:EPSG::5113'

        #since_1950 = isowocedate - datetime.datetime(1950, 1, 1)
        #var_time[:] = [since_1950.days + since_1950.seconds/86400.0]
        #var_latitude[:] = [self.globals['LATITUDE']]
        #var_longitude[:] = [self.globals['LONGITUDE']]

        #for column in self.columns.values():
        #    try:
        #        name = column.parameter.name_netcdf
        #    except AttributeError:
        #        LOG.warn('No netcdf name for parameter: %s' % column.parameter)
        #        continue
        #    try:
        #        assert name
        #    except AssertionError:
        #        LOG.warn('Netcdf name for parameter is not specified: %s' % \
        #                 column.parameter)
        #        continue

        #    if name in param_to_oceansites.keys():
        #        name = param_to_oceansites[name]
        #        # Write variable
        #        var = nc_file.createVariable(
        #            name, 'f8', ('DEPTH',), fill_value=float('nan'))# TODO fill value?
        #        # TODO ref table 3 for fill_value
        #        variable = oceansites_variables[name]
        #        var.long_name = variable['long'] or ''
        #        var.standard_name = variable['std'] or ''
        #        var.units = variable['units'] or ''
        #        var.QC_indicator = 2 # Probably good data
        #        var.QC_procedure = 5 # Data manually reviewed
        #        var.valid_min = float(column.parameter.bound_lower)
        #        var.valid_max = float(column.parameter.bound_upper)
        #        # TODO nominal sensor depth in meters positive in direction of
        #        # DEPTH:positive
        #        var.sensor_depth = 999.0
        #        var.uncertainty = oceansites_uncertainty[name]
        #        var.cell_methods = ('TIME: point DEPTH: average '
        #                            'LATITUDE: point LONGITUDE: point')
        #        var.DM_indicator = 'D'
        #        var[:] = column.values
        #        # Write QC variable
        #        if column.is_flagged_woce():
        #            qc_var_name = name + nc.QC_SUFFIX
        #            var.ancillary_variables = qc_var_name
        #            flag = nc_file.createVariable(
        #                qc_var_name, 'b', ('DEPTH',), fill_value=-128)
        #            flag.long_name = 'quality flag'
        #            flag.conventions = 'OceanSITES reference table 2'
        #            flag.valid_min = 0
        #            flag.valid_max = 9
        #            flag.flag_values = 0#, 1, 2, 3, 4, 5, 6, 7, 8, 9 TODO??
        #            flag.flag_meanings = FLAG_MEANINGS
        #            flag[:] = [
        #                WOCE_to_OceanSITES_flag[f] for f in column.flags_woce]
        #    else:
        #        LOG.info(("Parameter '%s' is not mapped to an OceanSITES "
        #                  'variable. Skipping.') % name)
        #    if name is 'PRES':
        #        # Fun using Sverdrup's depth integration with density.
        #        localgrav = \
        #            depth.grav_ocean_surface_wrt_latitude(
        #                self.globals['LATITUDE'])
        #        sal_tmp_pres = zip(self['CTDSAL'].values,
        #                           self['CTDTMP'].values,
        #                           column.values)
        #        density_series = [depth.density(*args) for args in sal_tmp_pres]

        #        try: 
        #            if None in density_series:
        #                # Can't perform integration with missing data points.
        #                raise ValueError
        #            var_depth.comment = \
        #                ('Calculated using integration of insitu density. '
        #                 'Sverdrup, et al. 1942')
        #            depth_series = depth.depth(
        #                localgrav, self['CTDPRS'].values, density_series)
        #        except ValueError:
        #            depth_series = fallback_depth_unesco(
        #                self.globals['LATITUDE'], self['CTDPRS'].values, var_depth)
        #        except IndexError:
        #            depth_series = fallback_depth_unesco(
        #                self.globals['LATITUDE'], self['CTDPRS'].values, var_depth)

        #        var_depth[:] = depth_series

        ## Write timeseries information, if given
        #timeseries_info = pick_timeseries_or_timeseries_info(
        #    self, timeseries, timeseries_info)
        #self.globals['_timeseries_info'] = timeseries_info

        #site_code = 'UNKNOWN'
        #if timeseries_info:
        #    site_code = timeseries_info['site_code']

        #    for var in VARIABLES_TO_TRANSFER:
        #        nc_file.__setattr__(var, timeseries_info[var])

        #nc_file.title = ('%s CTD Timeseries '
        #                 'ExpoCode=%s Station=%s Cast=%s') % \
        #    (site_code, self.globals['EXPOCODE'],
        #     self.globals['STNNBR'], self.globals['CASTNO'])

        #self.globals['OS_id'] = file_and_timeseries_info_to_id(
        #    self, timeseries_info, type='CTD', version=version)
        #nc_file.id = self.globals['OS_id']

        #nc.check_variable_ranges(nc_file)
