"""Common utilities that NetCDF OceanSITES handlers need.

The timeseries information for OceanSITES are obtained from
http://www.jcommops.org/FTPRoot/OceanSITES/documents/network_status/oceansites_station_data.xls

"""


from collections import defaultdict

from .. import LOG, fns
from ..algorithms import depth


__all__ = ['OCEANSITES_VERSIONS', 'OCEANSITES_PREFIX',
           'WOCE_to_OceanSITES_flag', 'TIMESERIES_INFO',
           'pick_timeseries_or_timeseries_info', 'param_to_oceansites',
           'oceansites_variables', 'oceansites_uncertainty', 'FLAG_MEANINGS',
           'VARIABLES_TO_TRANSFER', 'oceansites_id',
           'file_and_timeseries_info_to_id', 'fallback_depth_unesco', ]


# List of OceanSITES versions in increasing order
OCEANSITES_VERSIONS = ('1.1', '1.2', )


OCEANSITES_PREFIX = 'OS'


def _WOCE_to_OceanSITES_flag_default(woce_flag):
    LOG.warn(('WOCE flag %d was given that does not have translation into '
              'OceanSITES.') % woce_flag)
    return 6


WOCE_to_OceanSITES_flag = defaultdict(_WOCE_to_OceanSITES_flag_default, {
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


def oceansites_id(platform_code, deployment_code, data_mode='D', partx='',
                  version='1.2'):
    """ Return the OceanSITES id given the platform code and deployment code.

        This is the basis for the OceanSITES filename. (just add .nc)

        platform_code - given by OceanSITES
        deployment_code - unique for deployment (may be date or number)
        data_mode - refer to OceanSITES User manual 1.2 Section 5.1.1
        partx - optional user defined field for data id

    """
    if not partx:
        partx = None
    if version == '1.1' or version == '1.2':
        return '_'.join(filter(None, (OCEANSITES_PREFIX, platform_code,
                                      deployment_code, data_mode, partx)))
    return '_'.join(filter(None, (OCEANSITES_PREFIX, platform_code,
                                  deployment_code, data_mode, partx)))


def file_and_timeseries_info_to_id(file, timeseries_info=None, type=None,
                                   version='1.2'):
    """ Converts a read netcdf file and timeseries information into an
    OceanSITES id

        Arguments:
            file - the netcdf file
            timeseries_info - some information about a timeseries
            type - either CTD or BOT or None
            version - the version of OceanSITES manual to use
    """
    assert version in OCEANSITES_VERSIONS
    if timeseries_info:
        platform_code = timeseries_info.get('platform_code', 'UNKNOWN')
    else:
        platform_code = 'UNKNOWN'
    # the default "identifier" part of the id, ends up being deployment code for
    # 1.2 and config code for 1.1
    identifier = '%s%s-%s' % (file.globals['STNNBR'], file.globals['CASTNO'],
                              file.globals['DATE'])
    data_mode = file.globals['OS_data_mode']
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


def fallback_depth_unesco(lat, preses, var_depth):
    LOG.info(('Falling back from depth integration to Unesco '
              'method.'))
    var_depth.comment = \
        'Calculated using Unesco 1983 Saunders and Fofonoff method.'
    return map(lambda pres: depth.depth_unesco(pres, lat), preses)
