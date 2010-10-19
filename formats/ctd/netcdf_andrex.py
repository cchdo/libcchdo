import datetime
import string
import math
import os
import sys
import tempfile

from ... import LOG
from ... import fns
from ...model import datafile
from .. import woce
from .. import netcdf as nc


IGNORED_VARIABLES = [
    'pad_variable',
    'press_filt_bin_average',
    # Internal temperature of CTD sensor
    'pressure_temp',
    'scan',
    'altimeter',
    'depth',
    'potemp_cal',
    'potemp2_cal',
    'transmittance_filt'
    'backscatter_filt',
    'sigma0_cal',
    'sigma2_cal',
    'sigma4_cal',
    'temp1_filt',
    'temp2_filt',
    'cond1_filt',
    'cond2_filt',
    'oxygen_filt',
    'temp2_cal',
    'cond1_cal',
    'cond2_cal',
    'psal2_cal',
]


ANDREX_VAR_TO_WOCE_PARAM = {
    'press_filt': 'CTDPRS',
    #'temp1_filt': 'CTDTMP1',
    #'temp2_filt': 'CTDTMP2',
    #'cond1_filt': 'CTDCOND1',
    #'cond2_filt': 'CTDCOND2',
    #'oxygen_filt': 'CTDOXY',
    'fluor_filt': 'FLUOR',
    'transmittance_filt': 'XMISS',
    'temp1_cal': 'CTDTMP',
    #'temp2_cal': 'CTDTMP2C',
    #'cond1_cal': 'CTDCOND1C',
    #'cond2_cal': 'CTDCOND2C',
    'oxygen_cal': 'CTDOXY',
    'psal_cal': 'CTDSAL',
    #'psal2_cal': 'CTDSAL2C',
}


ANDREX_UNIT_TO_WOCE_UNIT = {
	'dbar': 'DBAR',
	'degc90': 'ITS-90',
	'umol/kg': 'UMOL/KG',
	'ug/l': 'UG/L',
	'percent': '%TRANS',
	'pss-78': 'PSS-78',
}


def read(self, handle):
    """How to read a CTD NetCDF Andrex file."""
    filename = handle.name
    nc_file = nc.Dataset(filename, 'r')

    for name, variable in nc_file.variables.items():
        if name in IGNORED_VARIABLES:
        	continue
        try:
            name = ANDREX_VAR_TO_WOCE_PARAM[name]
        except KeyError:
            LOG.warn("Unable to convert Andrex variable %s" % name)
            continue
        unit = variable.units
        try:
            unit = ANDREX_UNIT_TO_WOCE_UNIT[unit]
        except KeyError:
            LOG.warn("Unable to convert Andrex unit %s" % unit)
        self.columns[name] = datafile.Column(name, unit)
        self.columns[name].values = variable[:].tolist()[0]

    def comment(x):
        return '# %s' %x

    self.globals['header'] = \
        '\n'.join(nc_file.__dict__['comment'].split(
            nc_file.__dict__['comment_delimiter_string'])[1:-2]).replace('| ', '\n')
    self.globals['header'] = '\n'.join(map(comment, self.globals['header'].split('\n')))

    other_info = {
    	'mstar_string': nc_file.__dict__['mstar_string'],
        'openflag': nc_file.__dict__['openflag'],
        'date_file_updated': nc_file.__dict__['date_file_updated'],
        'mstar_time_origin': nc_file.__dict__['mstar_time_origin'],
        'time_convention': nc_file.__dict__['time_convention'],
        'dataname': nc_file.__dict__['dataname'],
        'version': nc_file.__dict__['version'],
        'platform_type': nc_file.__dict__['platform_type'],
        'platform_identifier': nc_file.__dict__['platform_identifier'],
        'platform_number': nc_file.__dict__['platform_number'],
        'instrument_identifier': nc_file.__dict__['instrument_identifier'],
        'recording_interval': nc_file.__dict__['recording_interval'],
        'instrument_depth_metres': nc_file.__dict__['instrument_depth_metres'],
        'mstar_site': nc_file.__dict__['mstar_site'],
    }
    self.globals['header'] += '\n%s' % '\n'.join(
                                  map(comment, str(other_info).split('\n')))

    self.globals['LATITUDE'] = nc_file.__dict__['latitude']
    self.globals['LONGITUDE'] = nc_file.__dict__['longitude']

    dtime = nc_file.__dict__['data_time_origin']
    dtime = datetime.datetime(*map(int, dtime))
    self.globals['stamp'] = '20101019SIOCCHDOMYS'
    self.globals['EXPOCODE'] = 'ANDREX'
    dataname = other_info['dataname']
    if dataname.endswith('rawdata'):
    	dataname = 'ctd_jr239_045'
    datainfo = dataname.split('_')
    self.globals['SECT_ID'] = datainfo[1]
    self.globals['STNNBR'] = datainfo[2]
    self.globals['CASTNO'] = '1'
    self.globals['DATE'], self.globals['TIME'] = \
        woce.strftime_woce_date_time(dtime)
    self.globals['DEPTH'] = nc_file.__dict__['water_depth_metres']

    nc_file.close()

    self.check_and_replace_parameters()
