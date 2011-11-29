import datetime
import string
import math
import sys
import tempfile

from ... import LOG
from ... import fns
from ...algorithms import depth
from .. import netcdf as nc


# List of OceanSITES versions in increasing order
OCEANSITES_VERSIONS = ('1.1', '1.2', )


WOCE_to_OceanSITES_flag = {
    1: 3, # Not calibrated -> Bad data that are potentially
          #                   correctable (re-calibration)
    2: 1, # Acceptable measurement -> Good data
    3: 2, # Questionable measurement -> Probably good data
    4: 4, # Bad measurement -> Bad data
    5: 9, # Not reported -> Missing value
    6: 8, # Interpolated over >2 dbar interval -> Interpolated value
    7: 5, # Despiked -> Value changed
    9: 9  # Not sampled -> Missing value
}


OCEANSITES_PREFIX = 'OS'


TIMESERIES_INFO = {
    'BATS': {
        'platform_code': 'BATS-1',
        'institution': 'Bermuda Institute of Ocean Sciences',
        'institution_references': 'http://bats.bios.edu/',
        'site_code': 'BATS',
        'array': 'BERMUDA',
        'references': 'http://cchdo.ucsd.edu/search?query=group:BATS',
        'comment': ('BIOS-BATS CTD data from SIO, translated to '
                    'OceanSITES NetCDF by SIO'),
        'summary': 'BIOS-BATS CTD data Bermuda',
        'area': 'Atlantic - Sargasso Sea',
        'institution_references': 'http://bats.bios.edu/',
        'contact': 'rodney.johnson@bios.edu',
        'pi_name': 'Rodney Johnson',
        'data_codes': 'SOT',
    },
    'HOT': {
        'platform_code': 'ALOHA',
        'institution': ("University of Hawai'i School of Ocean and "
                        "Earth Science and Technology"),
        'site_code': 'HOT',
        'array': 'HOT',
        'references': 'http://cchdo.ucsd.edu/search?query=group:HOT',
        'comment': ('HOT CTD data from SIO, translated to OceanSITES '
                    'NetCDF by SIO'),
        'summary': "HOT CTD data Hawai'i",
        'area': "Pacific - Hawai'i",
        'institution_references': 
            'http://hahana.soest.hawaii.edu/hot/hot_jgofs.html',
        'contact': 'santiago@soest.hawaii.edu',
        'pi_name': 'Roger Lukas',
        'data_codes': 'SOT',
    },
}
TIMESERIES_INFO['BATS_BATS-1'] = dict(TIMESERIES_INFO['BATS'])
TIMESERIES_INFO['BATS_HYDROS'] = dict(TIMESERIES_INFO['BATS'])
TIMESERIES_INFO['BATS_HYDROS']['platform_code'] = 'BHYDROS'


# CTD variables
param_to_oceansites = {
    'ctd_pressure': 'PRES',
    'ctd_temperature': 'TEMP',
    'ctd_oxygen': 'DOXY',
    'ctd_salinity': 'PSAL',
    'pressure': 'PRES',
    'temperature': 'TEMP',
    'oxygen1': 'DOXY',
    'salinity': 'PSAL',
    'fluorescence': 'FLU2',
}


oceansites_variables = {
    'TEMP': {'long': 'sea water temperature',
             'std': 'sea_water_temperature',
             'units': 'degree_Celsius'},
    'DOXY': {'long': 'dissolved oxygen', 'std': 'dissolved_oxygen',
             'units': 'micromole/kg'},
    'PSAL': {'long': 'sea water salinity', 'std': 'sea_water_salinity',
             'units': 'psu'},
    # valid_min 0.0, valid_max 12000.0, QC_indicator =7,
    # QC_procedure = 5, uncertainty 2.0
    'PRES': {'long': 'sea water pressure', 'std': 'sea_water_pressure',
             'units': 'decibars'},
    # TODO find out what the units for Fluorescense should be.
    # Not Real Fluoresence Units. Supposedly is unitless but you know how that
    # story goes.
    'FLU2': {'long': 'fluorescense', 'std': 'fluorescense',
             'units': 'rfu'},
}


oceansites_uncertainty = {
    'TEMP': 0.002,
    'PSAL': 0.005,
    'DOXY': float('inf'),
    'PRES': float('inf'),
    'FLU2': float('inf'),
}


FLAG_MEANINGS = ' '.join([
    'no_qc_performed',
    'good_data',
    'probably_good_data',
    'bad_data_that_are_potentially_correctable',
    'bad_data',
    'value_changed',
    'not_used',
    'nominal_value',
    'interpolated_value',
    'missing_value',
])


VARIABLES_TO_TRANSFER = (
    'platform_code institution institution_references site_code '
    'array references comment summary area institution_references '
    'contact pi_name').split()


def pick_timeseries_or_timeseries_info(df, timeseries=None, timeseries_info=None):
    if timeseries is not None:
        # BATS needs special name handling because there are two different
        # stations, BATS and Hydrostation S
        if timeseries == 'BATS':
            try:
                if df.globals['STNNBR'] == 'HYDROS':
                    return TIMESERIES_INFO['BATS_HYDROS']
                return TIMESERIES_INFO['BATS_BATS-1']
            except KeyError:
                pass
        return TIMESERIES_INFO[timeseries]
    else:
        return timeseries_info


def file_and_timeseries_info_to_id(file, timeseries_info, version='1.2'):
    assert version in OCEANSITES_VERSIONS
    platform_code = timeseries_info.get('platform_code', 'UNKNOWN')
    # the default "identifier" part of the id
    identifier = '%s%s' % (file.globals['STNNBR'], file.globals['CASTNO'])
    if version == '1.2':
        deployment_code = identifier
        # Refer to nc_file.data_mode below
        data_mode = 'D'
        return '_'.join((OCEANSITES_PREFIX, platform_code, deployment_code, data_mode))
    elif version == '1.1':
        config_code = identifier
        data_codes = timeseries_info.get('data_codes', 'D')
        return '_'.join((OCEANSITES_PREFIX, platform_code, config_code, data_codes))


def _WOCE_to_OceanSITES_flag(woce_flag):
    try:
        return WOCE_to_OceanSITES_flag[woce_flag]
    except KeyError:
        LOG.warn(('WOCE flag %d was given that does not have '
                           'translation into OceanSITES.') % woce_flag)
        return 6


def _fallback_depth_unesco(lat, preses, var_depth):
    LOG.info(('Falling back from depth integration to Unesco '
              'method.'))
    var_depth.comment = \
        'Calculated using Unesco 1983 Saunders and Fofonoff method.'
    return map(lambda pres: depth.depth_unesco(pres, lat), preses)


#def read(self, handle): TODO
#    '''How to read a CTD NetCDF OceanSITES file.'''


def write(self, handle, timeseries=None, timeseries_info={}, version='1.2'):
    '''How to write a CTD NetCDF OceanSITES file.
    Versions:
    1.1
    1.2
    '''
    assert version in OCEANSITES_VERSIONS

    # netcdf library wants to write its own files.
    tmp = tempfile.NamedTemporaryFile()
    strdate = str(self.globals['DATE']) 
    strtime = str(self.globals['TIME']).rjust(4, '0')
    isowocedate = datetime.datetime(
        int(strdate[0:4]), int(strdate[4:6]), int(strdate[6:8]),
        int(strtime[0:2]), int(strtime[2:4]))
    nc_file = nc.Dataset(tmp.name, 'w', format='NETCDF3_CLASSIC')
    nc_file.data_type = 'OceanSITES time-series CTD data'
    nc_file.format_version = version
    # 2011-11-28 Jing Zhou says platform code may be left blank as NA myshen
    nc_file.wmo_platform_code = ''
    if version == '1.2':
        nc_file.platform_code = ''
    nc_file.date_update = fns.strftime_iso(datetime.datetime.utcnow())
    nc_file.source = 'Shipborne observation'
    nc_file.history = ''.join([isowocedate.isoformat(), "Z data collected\n",
                       datetime.datetime.utcnow().isoformat(),
                       "Z date file translated/written"])
    nc_file.data_mode = 'D'
    nc_file.quality_control_indicator = '1'
    nc_file.quality_index = 'B'
    if version == '1.1':
        nc_file.conventions = 'OceanSITES Manual 1.1, CF-1.1'
    elif version == '1.2':
        nc_file.Conventions = 'CF-1.4, OceanSITES 1.1'
    nc_file.netcdf_version = '3.x'
    nc_file.naming_authority = 'OceanSITES'
    nc_file.cdm_data_type = 'Station'
    nc_file.geospatial_lat_min = str(self.globals['LATITUDE'])
    nc_file.geospatial_lat_max = str(self.globals['LATITUDE'])
    nc_file.geospatial_lon_min = str(self.globals['LONGITUDE'])
    nc_file.geospatial_lon_max = str(self.globals['LONGITUDE'])
    nc_file.geospatial_vertical_min = 0
    nc_file.geospatial_vertical_max = int(self.globals['DEPTH'])
    nc_file.geospatial_vertical_positive = 'down'
    nc_file.author = 'Shen:Diggs (Scripps)'
    if version == '1.1':
        nc_file.data_assembly_center = 'SIO'
    elif version == '1.2':
        nc_file.data_assembly_center = 'CCHDO'
    nc_file.distribution_statement = (
        'Follows CLIVAR (Climate Varibility and Predictability) '
        'standards, cf. http://www.clivar.org/data/data_policy.php. '
        'Data available free of charge. User assumes all risk for use of '
        'data. User must display citation in any publication or product '
        'using data. User must contact PI prior to any commercial use of '
        'data.')
    nc_file.citation = ('These data were collected and made freely '
                        'available by the OceanSITES project and the '
                        'national programs that contribute to it.')
    nc_file.update_interval = 'void'
    if version == '1.1':
        nc_file.qc_manual = "OceanSITES User's Manual v1.1"
    elif version == '1.2':
        nc_file.qc_manual = \
            "http://www.ocensites.org/dat a/quality_control_manual.pdf"
    nc_file.time_coverage_start = fns.strftime_iso(isowocedate)
    nc_file.time_coverage_end = fns.strftime_iso(isowocedate)

    nc_file.createDimension('TIME')
    try:
        nc_file.createDimension('DEPTH', len(self))
    except RuntimeError:
        raise AttributeError("There is no data to be written.")
    nc_file.createDimension('LATITUDE', 1)
    nc_file.createDimension('LONGITUDE', 1)
    nc_file.createDimension('POSITION', 1)

    # OceanSITES coordinate variables
    var_time = nc_file.createVariable(
        'TIME', 'd', ('TIME',), fill_value=999999.0)
    var_time.long_name = 'time'
    var_time.standard_name = 'time'
    var_time.units = 'days since 1950-01-01T00:00:00Z'
    var_time.valid_min = 0.0
    var_time.valid_max = 90000.0
    var_time.QC_indicator = 7 # Matthias Lankhorst
    var_time.QC_procedure = 5 # Matthias Lankhorst
    # 1/24 assuming a typical cast lasts one hour Matthias Lankhorst
    var_time.uncertainty = 0.0417
    var_time.axis = 'T'

    var_latitude = nc_file.createVariable(
        'LATITUDE', 'f', ('LATITUDE',), fill_value=99999.0)
    var_latitude.long_name = 'Latitude of each location'
    var_latitude.standard_name = 'latitude'
    var_latitude.units = 'degrees_north'
    var_latitude.valid_min = -90.0
    var_latitude.valid_max = 90.0
    var_latitude.QC_indicator = 7 # Matthias Lankhorst
    var_latitude.QC_procedure = 5 # Matthias Lankhorst
    var_latitude.uncertainty = 0.0045 # Matthias Lankhorst
    var_latitude.axis = 'Y'
    if version == '1.1':
        pass
    elif version == '1.2':
        var_latitude.reference = 'WGS84'
        var_latitude.coordinate_reference_frame = 'urn:ogc:crs:EPSG::4326'

    var_longitude = nc_file.createVariable(
        'LONGITUDE', 'f', ('LONGITUDE',), fill_value=99999.0)
    var_longitude.long_name = 'Longitude of each location'
    var_longitude.standard_name = 'longitude'
    var_longitude.units = 'degrees_east'
    var_longitude.valid_min = -180.0
    var_longitude.valid_max = 180.0
    var_longitude.QC_indicator = 7 # Matthias Lankhorst
    var_longitude.QC_procedure = 5 # Matthias Lankhorst
    # Matthias Lankhorst
    var_longitude.uncertainty = 0.0045 / math.cos(
        float(self.globals['LATITUDE']))
    var_longitude.axis = 'X'
    if version == '1.1':
        pass
    elif version == '1.2':
        var_longitude.reference = 'WGS84'
        var_longitude.coordinate_reference_frame = 'urn:ogc:crs:EPSG::4326'

    var_depth = nc_file.createVariable(
        'DEPTH', 'f', ('DEPTH',), fill_value=-99999.0)
    var_depth.long_name = 'Depth of each measurement'
    var_depth.standard_name = 'depth'
    var_depth.units = 'meters'
    var_depth.valid_min = 0.0
    var_depth.valid_max = 12000.0
    # Subject: OceanSITES: more on QC flags, uncertainty, depth
    # Interpolated from latitude and pressure.
    var_depth.QC_indicator = 8
    var_depth.QC_procedure = 2 # See above
    var_depth.uncertainty = 1.0 # A decibar
    if version == '1.1':
        var_depth.axis = 'down' # oceanic
    elif version == '1.2':
        var_depth.positive = 'down'
        var_depth.axis = 'Z'
        var_depth.reference = 'sea_level' # TODO is this right?
        var_depth.coordinate_reference_frame = 'urn:ogc:crs:EPSG::5113'

    since_1950 = isowocedate - datetime.datetime(1950, 1, 1)
    var_time[:] = [since_1950.days + since_1950.seconds/86400.0]
    var_latitude[:] = [self.globals['LATITUDE']]
    var_longitude[:] = [self.globals['LONGITUDE']]

    for column in self.columns.values():
        try:
            name = column.parameter.name_netcdf
        except AttributeError:
            LOG.warn('No netcdf name for parameter: %s' % column.parameter)
            continue
        try:
            assert name
        except AssertionError:
            LOG.warn('Netcdf name for parameter is not specified: %s' % \
                     column.parameter)
            continue

        if name in param_to_oceansites.keys():
            name = param_to_oceansites[name]
            # Write variable
            var = nc_file.createVariable(
                name, 'f8', ('DEPTH',), fill_value=float('nan'))# TODO fill value?
            # TODO ref table 3 for fill_value
            variable = oceansites_variables[name]
            var.long_name = variable['long'] or ''
            var.standard_name = variable['std'] or ''
            var.units = variable['units'] or ''
            var.QC_indicator = 2 # Probably good data
            var.QC_procedure = 5 # Data manually reviewed
            var.valid_min = float(column.parameter.bound_lower)
            var.valid_max = float(column.parameter.bound_upper)
            # TODO nominal sensor depth in meters positive in direction of
            # DEPTH:positive
            var.sensor_depth = 999.0
            var.uncertainty = oceansites_uncertainty[name]
            var.cell_methods = ('TIME: point DEPTH: average '
                                'LATITUDE: point LONGITUDE: point')
            var.DM_indicator = 'D'
            var[:] = column.values
            # Write QC variable
            if column.is_flagged_woce():
                qc_var_name = name + nc.QC_SUFFIX
                var.ancillary_variables = qc_var_name
                flag = nc_file.createVariable(
                    qc_var_name, 'b', ('DEPTH',), fill_value=-128)
                flag.long_name = 'quality flag'
                flag.conventions = 'OceanSITES reference table 2'
                flag.valid_min = 0
                flag.valid_max = 9
                flag.flag_values = 0#, 1, 2, 3, 4, 5, 6, 7, 8, 9 TODO??
                flag.flag_meanings = FLAG_MEANINGS
                flag[:] = map(_WOCE_to_OceanSITES_flag, column.flags_woce)
        else:
            LOG.info(("Parameter '%s' is not mapped to an OceanSITES "
                      'variable. Skipping.') % name)
        if name is 'PRES':
            # Fun using Sverdrup's depth integration with density.
            localgrav = \
                depth.grav_ocean_surface_wrt_latitude(
                    self.globals['LATITUDE'])
            sal_tmp_pres = zip(self['CTDSAL'].values,
                               self['CTDTMP'].values,
                               column.values)
            density_series = [depth.density(*args) for args in sal_tmp_pres]

            try: 
                if None in density_series:
                    # Can't perform integration with missing data points.
                    raise ValueError
                var_depth.comment = \
                    ('Calculated using integration of insitu density. '
                     'Sverdrup, et al. 1942')
                depth_series = depth.depth(
                    localgrav, self['CTDPRS'].values, density_series)
            except ValueError:
                depth_series = _fallback_depth_unesco(
                    self.globals['LATITUDE'], self['CTDPRS'].values, var_depth)
            except IndexError:
                depth_series = _fallback_depth_unesco(
                    self.globals['LATITUDE'], self['CTDPRS'].values, var_depth)

            var_depth[:] = depth_series

    # Write timeseries information, if given
    timeseries_info = pick_timeseries_or_timeseries_info(
        self, timeseries, timeseries_info)
    if timeseries_info:
        nc_file.title = ('%s CTD Timeseries '
                         'ExpoCode=%s Station=%s Cast=%s') % \
            (timeseries_info['site_code'], self.globals['EXPOCODE'],
             self.globals['STNNBR'], self.globals['CASTNO'])

        for var in VARIABLES_TO_TRANSFER:
            nc_file.__setattr__(var, timeseries_info[var])

        nc_file.id = file_and_timeseries_info_to_id(
            self, timeseries_info, version)

    nc.check_variable_ranges(nc_file)

    nc_file.close()

    handle.write(tmp.read())
    tmp.close()
