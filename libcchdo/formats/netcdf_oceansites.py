"""Common utilities that NetCDF OceanSITES handlers need.

The timeseries information for OceanSITES are obtained from
http://www.jcommops.org/FTPRoot/OceanSITES/documents/network_status/oceansites_station_data.xls

"""


from datetime import datetime
from collections import defaultdict
from math import cos

from libcchdo.log import LOG
from libcchdo.fns import strftime_iso
from libcchdo.util import memoize
from libcchdo.algorithms import depth


__all__ = [
    'OCEANSITES_VERSIONS', 'OCEANSITES_PREFIX', 'TIMESERIES_INFO',
    'OCEANSITES_TIMESERIES', 'create_oceansites_nc', 'write_columns',
    'write_timeseries_info_title_and_id',
]


# List of OceanSITES versions in increasing order
OCEANSITES_VERSIONS = ['1.1', '1.2', ]


OCEANSITES_PREFIX = 'OS'


class WOCE_to_OceanSITES_flag_dict(defaultdict):
    """Exactly like defaultdict but passes the key to the default_factory."""
    def __init__(self, dict):
        super(WOCE_to_OceanSITES_flag_dict, self).__init__(None, dict)

    def __missing__(self, key):
        self[key] = 6
        LOG.warn(u'WOCE flag {0} was given that does not have translation into '
                 'OceanSITES.'.format(key))
        return self[key]


WOCE_to_OceanSITES_flag = WOCE_to_OceanSITES_flag_dict({
    1: 3, # Not calibrated -> Bad data that are potentially
          #                   correctable (re-calibration)
    2: 1, # Acceptable measurement -> Good data
    3: 2, # Questionable measurement -> Probably good data
    4: 4, # Bad measurement -> Bad data
    5: 9, # Not reported -> Missing value
    6: 8, # Interpolated over >2 dbar interval -> Interpolated value
    7: 5, # Despiked -> Value changed
    9: 9, # Not sampled -> Missing value
})


TIMESERIES_INFO = {
    'BATS': {
        'platform_code': 'BATS-1',
        'institution': 'Bermuda Institute of Ocean Sciences',
        'institution_references': 'http://bats.bios.edu/',
        'site_code': 'BATS',
        'array': 'BERMUDA',
        'references': 'http://cchdo.ucsd.edu/search?query=group:BATS',
        'comment': ('BIOS-BATS {data_type} data from SIO, translated to '
                    'OceanSITES NetCDF by SIO'),
        'summary': 'BIOS-BATS {data_type} data Bermuda',
        'area': 'Atlantic - Sargasso Sea',
        'institution_references': 'http://bats.bios.edu/',
        'contact': 'rodney.johnson@bios.edu',
        'pi_name': 'Rodney Johnson',
        'data_codes': 'SOT',
    },
    'HOT': {
        'platform_code': 'HOT',
        'institution': ("University of Hawai'i School of Ocean and "
                        "Earth Science and Technology"),
        'site_code': 'ALOHA',
        'array': 'HOT',
        'references': 'http://cchdo.ucsd.edu/search?query=group:HOT',
        'comment': ('HOT {data_type} data from SIO, translated to OceanSITES '
                    'NetCDF by SIO'),
        'summary': "HOT {data_type} data Hawai'i",
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


OCEANSITES_TIMESERIES = TIMESERIES_INFO.keys()


TIMESERIES_VARIABLES = [
    'platform_code', 'institution', 'institution_references', 'site_code',
    'array', 'references', 'comment', 'summary', 'area',
    'institution_references', 'contact', 'pi_name']


def pick_timeseries_or_timeseries_info(df, timeseries=None,
                                       timeseries_info=None):
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


class OSVar(object):
    """An OceanSITES variable."""
    def __init__(self, short_name, long_name, standard_name, units,
                 uncertainty=None):
        """OceanSITES variable

        short_name is the OceanSITES name for the variable. It is assigned based
        on SeaDataNet's precedents but may be bent to allow for multiple
        variables of similar type

        long_name, standard_name, and units are CF attributes.

        The long name is just a well capitalized form of the variable name.
        The standard name is based on the CF standard parameters list.
        Units must be a udunits unit.

        uncertainty is an OceanSITES attribute. It is a float and defaults to
        infinity.

        """
        self.short_name = short_name
        self._long_name = long_name
        self._standard_name = standard_name
        self._units = units
        self._uncertainty = uncertainty
        self._fill_value = None

    @property
    def long_name(self):
        return self._long_name or ''

    @property
    def standard_name(self):
        return self._standard_name or ''

    @property
    def units(self):
        return self._units or ''

    @property
    def uncertainty(self):
        return self._uncertainty or float('inf')

    @property
    def fill_value(self):
        # Defaulting to 99999 based on Argo format reference table 3.
        return self._fill_value or float(99999)

    def __unicode__(self):
        return u'OSVar({0})'.format(self.short_name)

    def __repr__(self):
        return u'OSVar({0!r}, {1!r}, {2!r}, {3!r}, {4!r})'.format(
            self.short_name, self.long_name, self.standard_name, self.units,
            self.uncertainty)


class ParamToOS(dict):
    """Map parameter names to OceanSITES.

    Register parameter name and OSVar pairs.

    """
    def __init__(self):
        self._param_to_os = {}
        self._os_vars = {}

    def _register(self, pname, osvar):
        """Register parameter name to map to an OSVar.

        osvar may be an OSVar or string.

        """
        if isinstance(osvar, OSVar):
            try:
                self.register_osvar(osvar)
                self._param_to_os[pname] = osvar.short_name
            except ValueError:
                LOG.warn(
                    u'Cannot register {0} without standard name'.format(osvar))
        else:
            try:
                self._os_vars[osvar]
            except KeyError:
                raise ValueError(
                    u'OceanSITES variable {0} is undefined. Cannot register '
                    'with {1}'.format(osvar, pname))
            self._param_to_os[pname] = osvar

    def register(self, *args):
        """Register parameter name to map to an OSVar.

        Provide either a parameter name, osvar pair or a list of pairs.

        """
        if len(args) == 2:
            self._register(*args)
        elif len(args) == 1 and isinstance(args[0], dict):
            for arg in args[0]:
                self._register(arg, args[0][arg])
        else:
            raise ValueError(u'Unhandled type passed to register')

    def register_osvar(self, osvar):
        """Add OSVar to map."""
        if not osvar.standard_name:
            raise ValueError(u'No standard name given.')
        self._os_vars[osvar.short_name] = osvar
        self._param_to_os[osvar.short_name] = osvar.short_name

    def register_osvars(self, *osvars):
        for osvar in osvars:
            self.register_osvar(osvar)

    def convert(self, pname):
        """Return the OceanSITES name and OSVar for the parameter name.

        """
        try:
            os_name = self._param_to_os[pname]
        except KeyError:
            raise KeyError(u'Unregistered parameter name {0!r}'.format(pname))
        try:
            return os_name, self._os_vars[os_name]
        except KeyError:
            raise ValueError(
                u'OceanSITES name {0!r} has no definition.'.format(os_name))


PRESSURE_VARIABLES = ['CTDPRS', 'CTDRAW', 'REVPRS', 'DWNPRS']


BTL_SALINITY_VARIABLES = ['SALNTY', 'SOMSAL', ]


SALINITY_VARIABLES = ['CTDSAL', ] + BTL_SALINITY_VARIABLES


OXYGEN_VARIABLES = ['CTDOXY', 'DWNOXY', 'OXYGEN', ]


TEMPERATURE_VARIABLES = ['CTDTMP', 'REVTMP', 'SBE35', ]


@memoize
def get_param_to_os():
    param_to_os = ParamToOS()
    param_to_os.register_osvars(
        OSVar(u'TEMP', 'sea water temperature', 'sea_water_temperature',
              'degree_Celsius', 0.002),
        OSVar(u'DOXY', 'dissolved oxygen',
              'mass_concentration_of_oxygen_in_sea_water', 'micromole/kg'),
        OSVar(u'DOXY1', 'oxygen',
              'mole_concentration_of_dissolved_molecular_oxygen_in_sea_water',
              'micromole/kg'),
        OSVar(u'DOXY_TEMP', 'oxygen fix temperature',
              'temperature_of_sensor_for_oxygen_in_sea_water',
              'degree_Celsius'),
        OSVar(
            u'PSAL', 'sea water salinity', 'sea_water_salinity', 'psu', 0.005),
        OSVar(u'PSAL1', 'ctd salinity', 'sea_water_practical_salinity',
              'pss-78', 0.005),
        # valid_min 0.0, valid_max 12000.0, QC_indicator =7,
        # QC_procedure = 5, uncertainty 2.0
        OSVar(u'PRES', 'sea water pressure', 'sea_water_pressure', 'decibars'),
        # TODO find out what the units for Fluorescense should be.
        # Not Real Fluoresence Units. Supposedly is unitless but you know how
        # that story goes.
        OSVar(u'FLU2', 'fluorescense', 'fluorescense', 'rfu'),
        OSVar(u'DEPTH', 'depth', 'depth', 'meters'),
    )

    param_to_os_registrants = {
        'CTDSAL': u'PSAL1',

        'FLUOR': u'FLU2',

        'DEPTH': u'DEPTH',
        'SIG0': OSVar(
            u'SIGTH', 'sigma theta', 'sea_water_sigma_theta', 'kg/m^3'),
        'ALKALI': OSVar(
            u'ALK', 'alkalinity',
            'sea_water_alkalinity_expressed_as_mole_equivalent', 'uequiv'),
        'NO2+NO3': OSVar(
            u'NO31', 'nitrate + nitrite',
            'mole_concentration_of_nitrate_and_nitrite_in_sea_water',
            'umol/kg'),
        'NITRIT': OSVar(
            u'NO21', 'nitrite', 'mole_concentration_of_nitrite_in_sea_water',
            'umol/kg'),
        'PHSPHT': OSVar(
            u'PO41', 'phosphate',
            'mole_concentration_of_phosphate_in_sea_water', 'umol/kg'),
        'SILCAT': OSVar(
            u'SILC1', 'silicate', 'mole_concentration_of_silicate_in_sea_water',
            'umol/kg'),
        # SeaDataNet CORG
        'POC': OSVar(u'CORG', 'particulate organic carbon', 'WC_POC', 'ug/kg'),
        # SeaDataNet NTOT
        'PON': OSVar(
            u'NTOT', 'particulate organic nitrogen', 'WC_PON', 'ug/kg'),
        # TODO find standard_name
        'TOC': OSVar(u'TOC', 'total organic carbon', None, 'umol/kg'),
        # SeaDataNet TDNT
        'DON': OSVar(
            u'TDNT1', 'dissolved organic nitrogen', 'WC_DissNitrogen',
            'umol/kg'),
        # TODO verify that using the standard_name for inorganic nitrogen is ok
        'TON': OSVar(
            u'TDNT', 'total organic nitrogen', 'WC_DissNitrogen', 'umol/kg'),
        # SeaDataNet BATX
        'BACT': OSVar(
            u'BATX', 'bacteria enumeration', 'BactTaxaAbundWater',
            'cells*10^8/kg'),
    }
    for v in PRESSURE_VARIABLES:
        param_to_os_registrants[v] = u'PRES'
    for v in BTL_SALINITY_VARIABLES:
        param_to_os_registrants[v] = u'PSAL'
    for v in OXYGEN_VARIABLES:
        param_to_os_registrants[v] = u'DOXY1'
    for v in TEMPERATURE_VARIABLES:
        param_to_os_registrants[v] = u'TEMP'
    param_to_os.register(param_to_os_registrants)

    return param_to_os


OS_TEXT = {
    'DISTRIBUTION_STATEMENT': """\
Follows CLIVAR (Climate Varibility and Predictability) standards, cf.
http://www.clivar.org/data/data_policy.php.  Data available free of charge. User
assumes all risk for use of data. User must display citation in any publication
or product using data. User must contact PI prior to any commercial use of
data.""",
    'CITATION': """\
These data were collected and made freely available by the OceanSITES project
and the national programs that contribute to it.""",
    'FLAG_MEANINGS': ' '.join([
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
    ]),
    'CELL_METHODS': ' '.join([
        'TIME: point',
        'DEPTH: average',
        'LATITUDE: point',
        'LONGITUDE: point',
    ]),
    'DEPTH_CALCULATED_SVERDRUP': (
        'Calculated using integration of insitu density. Sverdrup, et al. 1942'
    ),
    'DEPTH_CALCULATED_UNESCO_1983': (
        'Calculated using Unesco 1983 Saunders and Fofonoff method.'
    ),
}



def data_assembly_center(version):
    if version == '1.1':
        return 'SIO'
    elif version == '1.2':
        return 'CCHDO'


def qc_manual(version):
    if version == '1.1':
        return "OceanSITES User's Manual v1.1"
    elif version == '1.2':
        return "http://www.ocensites.org/data/quality_control_manual.pdf"


def fractional_days_since(dt, since=datetime(1950, 1, 1)):
    """Return a float representation of a datetime as seconds since a time.

    """
    td = dt - since
    return td.days + td.seconds / 86400.0


def _sanitize_os_version(version):
    """Make sure the OceanSITES manual version is a real version."""
    if not version:
        version = '1.2'
    assert version in OCEANSITES_VERSIONS
    return version


def create_oceansites_nc(df, filename, data_type, version=None):
    from libcchdo.formats import netcdf as nc
    info = {
        'date_start': df.globals['_DATETIME'],
        'lat': df.globals['LATITUDE'],
        'lon': df.globals['LONGITUDE'],
        'depths': df.globals['DEPTH'],
        'data_length': len(df),
    }

    version = _sanitize_os_version(version)
    nc_file = nc.Dataset(filename, 'w', format='NETCDF3_CLASSIC')
    nc_file.data_type = 'OceanSITES time-series {data_type} data'.format(
        data_type=data_type)
    nc_file.format_version = version
    # 2011-11-28 Jing Zhou says platform code may be left blank as NA myshen
    nc_file.wmo_platform_code = ''
    if version == '1.2':
        nc_file.platform_code = ''
    now = datetime.utcnow()
    nc_file.date_update = strftime_iso(now)
    nc_file.source = 'Shipborne observation'
    nc_file.history = ''.join([
        info['date_start'].isoformat(), "Z data collected\n", now.isoformat(),
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

    nc_file.geospatial_lat_min = str(info['lat'])
    nc_file.geospatial_lat_max = str(info['lat'])
    nc_file.geospatial_lon_min = str(info['lon'])
    nc_file.geospatial_lon_max = str(info['lon'])
    nc_file.geospatial_vertical_min = 0
    nc_file.geospatial_vertical_max = str(info['depths'])
    nc_file.geospatial_vertical_positive = 'down'

    nc_file.author = 'Shen:Diggs (Scripps)'
    nc_file.data_assembly_center = data_assembly_center(version)
    nc_file.distribution_statement = OS_TEXT['DISTRIBUTION_STATEMENT']
    nc_file.citation = OS_TEXT['CITATION']
    nc_file.update_interval = 'void'
    nc_file.qc_manual = qc_manual(version)
    nc_file.time_coverage_start = strftime_iso(info['date_start'])
    nc_file.time_coverage_end = strftime_iso(info['date_start'])

    nc_file.createDimension('TIME')
    try:
        nc_file.createDimension('DEPTH', info['data_length'])
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
    var_time[:] = [fractional_days_since(info['date_start'])]

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
    var_latitude[:] = [info['lat']]

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
    var_longitude.uncertainty = 0.0045 / cos(float(info['lat']))
    var_longitude.axis = 'X'
    if version == '1.1':
        pass
    elif version == '1.2':
        var_longitude.reference = 'WGS84'
        var_longitude.coordinate_reference_frame = 'urn:ogc:crs:EPSG::4326'
    var_longitude[:] = [info['lon']]

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
    return nc_file


def _find_first(df, parameters):
    for c in df.sorted_columns():
        if c.parameter.name in parameters:
            return c
    return None


def _calculate_depth(self, nc_file):
    """Calculate a DEPTH column based on a series of methods."""
    var_depth = nc_file.variables['DEPTH']
    try:
        depths = self['_ACTUAL_DEPTH']
        var_depth[:] = depths.values
        return
    except KeyError:
        pass

    # If there is no _ACTUAL_DEPTH column, calculate it using pressure,
    # salinity, and temperature. _ACTUAL_DEPTH is used because CCHDO's
    # parameter list includes DEPTH which is actually Bottom Depth.

    # Fun using Sverdrup's depth integration with density.
    pres = _find_first(self, PRESSURE_VARIABLES)
    salt = _find_first(self, SALINITY_VARIABLES)
    temp = _find_first(self, TEMPERATURE_VARIABLES)
    lat = self.globals['LATITUDE']

    try:
        localgrav = depth.grav_ocean_surface_wrt_latitude(lat)
    except OverflowError, err:
        LOG.error(u'Unable to calculate gravity for latitude. Sin algorithm '
                  'probably oscillates.')
        return
    try: 
        sal_tmp_pres = zip(salt.values, temp.values, pres.values)
        density_series = [depth.density(*args) for args in sal_tmp_pres]
        if None in density_series:
            raise ValueError(
                u'Cannot perform depth integration with missing data points')
        var_depth.comment = OS_TEXT['DEPTH_CALCULATED_SVERDRUP']
        var_depth[:] = depth.depth(localgrav, pres.values, density_series)
        return
    except (AttributeError, IndexError, ValueError):
        pass
    try:
        LOG.info(u'Falling back from depth integration to Unesco method.')
        var_depth.comment = OS_TEXT['DEPTH_CALCULATED_UNESCO_1983']
        var_depth[:] = [depth.depth_unesco(pres, lat) for pres in pres.values]
    except AttributeError:
        raise ValueError(u'Cannot convert non-existant pressures to depths.')


def write_columns(self, nc_file, converter=get_param_to_os()):
    from libcchdo.formats import netcdf as nc
    LOG.debug(u'writing columns')

    # Because it is possible for multiple parameter names to be mapped to the
    # same OceanSITES name, start in order so the less important ones are
    # ignored
    for column in self.sorted_columns():
        # Determine the parameter's OceanSITES name and CF name
        pname = column.parameter.name
        try:
            name, variable = converter.convert(pname)
        except KeyError:
            LOG.warn(
                u'Parameter name {0!r} is not mapped to an OceanSITES '
                'variable. Skipping.'.format(pname))
            continue
        except ValueError:
            LOG.warn(
                u'OceanSITES variable {0!r} does not have CF and OceanSITES '
                'information. Skipping.'.format(name))
            continue

        LOG.debug(
            u'{pname} mapped to {osname}'.format(pname=pname, osname=name))

        # Write variable
        # documentation says to refer to ref table 3 for fill_value but that is
        # likely a holdout from when the document was likely copied from Argo's
        # manuals. See OSVar for the compromise.
        try:
            var = nc_file.createVariable(
                name, 'f8', ('DEPTH',), fill_value=variable.fill_value)
        except RuntimeError:
            LOG.warn(
                u'{0!r} already present in netCDF file. Skipping.'.format(name))
            continue
        var.long_name = variable.long_name
        var.standard_name = variable.standard_name
        var.units = variable.units
        var.QC_indicator = 2 # Probably good data
        var.QC_procedure = 5 # Data manually reviewed
        try:
            var.valid_min = float(column.parameter.bound_lower)
        except TypeError:
            LOG.warn(
                u'No lower bound defined for {0!r}.'.format(column.parameter))
        try:
            var.valid_max = float(column.parameter.bound_upper)
        except TypeError:
            LOG.warn(
                u'No upper bound defined for {0!r}.'.format(column.parameter))
        # TODO nominal sensor depth in meters positive in direction of
        # DEPTH:positive
        var.sensor_depth = 999.0
        var.uncertainty = variable.uncertainty
        var.cell_methods = OS_TEXT['CELL_METHODS']
        var.DM_indicator = 'D'
        data = [variable.fill_value if x is None else x for x in column.values]
        short = len(self) - len(data)
        if short:
            data += [variable.fill_value] * short
        var[:] = data
        # Write QC variable
        if column.is_flagged_woce():
            qc_var_name = name + nc.QC_SUFFIX
            var.ancillary_variables = str(qc_var_name)
            flag = nc_file.createVariable(
                qc_var_name, 'b', ('DEPTH',), fill_value=-128)
            flag.long_name = 'quality flag'
            flag.conventions = 'OceanSITES reference table 2'
            flag.valid_min = 0
            flag.valid_max = 9
            flag.flag_values = list(range(10))
            flag.flag_meanings = OS_TEXT['FLAG_MEANINGS']
            flag[:] = [WOCE_to_OceanSITES_flag[f] for f in column.flags_woce]
    _calculate_depth(self, nc_file)


def write_timeseries_info_title_and_id(
        self, nc_file, data_type, timeseries, timeseries_info, version=None):
    from libcchdo.formats import netcdf as nc
    assert data_type in ['CTD', 'BTL']
    # Write timeseries information, if given
    timeseries_info = pick_timeseries_or_timeseries_info(
        self, timeseries, timeseries_info)
    self.globals['_timeseries_info'] = timeseries_info

    site_code = 'UNKNOWN'
    if timeseries_info:
        site_code = timeseries_info['site_code']
        for var in TIMESERIES_VARIABLES:
            nc_file.__setattr__(
                var, timeseries_info[var].format(data_type=data_type))

    # Title is free format but can be the file name (OceanSITES ID)
    nc_file.title = (
        '{site} {data_type} Timeseries ExpoCode={expo} Station={stn} '
        'Cast={cast}').format(
            site=site_code, data_type=data_type, expo=self.globals['EXPOCODE'],
            stn=self.globals['STNNBR'], cast=self.globals['CASTNO'])

    # Pass along OS_id to zip writing so it can be used in filename generation
    self.globals['_OS_id'] = file_and_timeseries_info_to_id(
        self, timeseries_info, type=data_type, version=version)
# TODO ENSURE THAT THIS ID IS UNIQUE WHEN WRITING THE ZIP FILE, THEORETICALLY IT
# IS ALREADY UNIQUE WHEN WRITING THE CRUISE SO MAKING SURE IT IS unique in the
# cruise will be sufficient
    nc_file.id = self.globals['_OS_id']

    nc.check_variable_ranges(nc_file)


def oceansites_id(platform_code, deployment_code, data_mode='D', partx=None,
                  version=None):
    """Return the OceanSITES id given the platform code and deployment code.

    This is the basis for the OceanSITES filename. (just add .nc)

    Arguments:

    platform_code - given by OceanSITES
    deployment_code - unique for deployment (may be date or number)
    data_mode - refer to OceanSITES User manual 1.2 Section 5.1.1
    partx - optional user defined field for data id

    """
    version = _sanitize_os_version(version)
    if not partx:
        partx = None
    default_id = '_'.join(
        filter(None, (OCEANSITES_PREFIX, platform_code, deployment_code,
                      data_mode, partx)))
    if version == '1.1' or version == '1.2':
        return default_id
    return default_id


def file_and_timeseries_info_to_id(
        file, timeseries_info=None, type=None, data_mode=None, version=None):
    """Convert netcdf file and timeseries information into an OceanSITES id.

    Arguments:
    file - the netcdf file
    timeseries_info - some information about a timeseries
    type - either CTD or BTL or None. This is an optional user defined field to
        identify data.
    version - the version of OceanSITES manual to use

    """
    version = _sanitize_os_version(version)
    try:
        platform_code = timeseries_info['platform_code']
    except (KeyError, AttributeError):
        platform_code = 'UNKNOWN'

    # the default "identifier" part of the id, ends up being deployment code for
    # 1.2 and config code for 1.1
    identifier = '{stn}-{cruise}-{cast}'.format(
        cruise=file.globals['_OS_ID'], stn=file.globals['STNNBR'],
        cast=file.globals['CASTNO'])
    if not data_mode:
        data_mode = 'D'
    data_mode = data_mode

    if version == '1.2':
        deployment_code = identifier
        return oceansites_id(platform_code, deployment_code, data_mode,
                             partx=type, version=version)
    elif version == '1.1':
        config_code = identifier
        if timeseries_info:
            data_codes = timeseries_info.get('data_codes', data_mode)
        else:
            data_codes = ''
        return oceansites_id(platform_code, config_code, data_codes,
                             partx=type, version=version)


def write_zip_factory(module):
    def get_filename(dfile):
        """Return filename for OceanSITES cast datafile."""
        return '{os_id}.nc'.format(os_id=dfile.globals['_OS_id'])

    def write(self, handle, timeseries=None, timeseries_info={}, version=None):
        """Write a ZIP file containing multiple OceanSITES cast files.

        """
        from libcchdo.formats.zip import write as zip_write
        zip_write(self, handle, module, get_filename, timeseries=timeseries,
              timeseries_info=timeseries_info, version=version)
    return write

