import datetime
import re

from ... import LOG
from ... import fns
from ...fns import ddm_to_dd, Decimal

def read(self, handle, remove_calcualte=True):
    """How to read an SBE 9 ASCII CTD File"""

    l = handle.readline()
    if not l.lower().startswith('* sea-bird sbe 9 data file:'):
        raise ValueError(('Expected SBE 9 identifier:'
                           '\'* Sea-Bird SBE 9 Data File:\'\n'
                           'Instead got: %s') % l)

    headers = []
    parameter_map = {'prDM':'CTDPRS',
                     'depSM':'_depSM', # This is not bottom depth
                     't090C':'CTDTMP',
                     't190C':'_CTDTMP2',
                     'c0mS':'_c0mS',
                     'c1mS':'_c1mS',
                     'altM':'_altM',
                     'latitude':'LATITUDE',
                     'longitude':'LONGITUDE',
                     'timeS':'_timeS',
                     'timeY':'_DATETIME', # need to convert and split
                     'flECO-AFL':'_flECO-AFL',
                     'par':'_par',
                     'sbeox0ML/L':'CTDOXY', # convert to umol/kg
                     'xmiss':'XMISS',
                     'altM':'_altM',
                     'sigma':'_sigma',
                     'potemp090C':'_potemp090C',
                     'potemp190C':'_potemp190C',
                     'sal00':'CTDSAL',
                     'sal11':'_CTDSAL2',
                     'svCM':'_svCM',
                     'flag':'_flag',
                     }
    units_map = {'db':'DBAR',
                 'PSU':'PSS-78',
                 'ITS-90':'ITS-90',
                 'salt water':'meters',
                 'sigma':'kg/m3',
                 'Chen-Millero':'m/s',
                 }
    calculated=['_depSM',
                '_c0mS',
                '_c1mS',
                '_altM',
                '_sigma',
                '_potemp090C',
                '_potemp190C',
                '_svCM',
                ]

    columns = []
    units = []
    while l and not l.startswith('*END*'):
        # This is what needs to happen here
        # 1) all the headers need to be saved up to the *END* tag
        # 2) all the things that start with # name (some number) need to be
        # extracted for information as this is where the column order is
        # 3) the default will be to take the first instance of the duplicated
        # measurements such as temperature and salinity. Letting the user chose
        # can be a later addition, perhaps only care if the difference between
        # the channels is to large
        headers.append(l.decode('raw_unicode_escape'))
        if '# name' in l:
            s = l.split('=')
            i = [int(d) for d in s[0].split() if d.isdigit()]
            for key in parameter_map.iterkeys():
                if key in l:
                    columns.append(parameter_map[key])
            try:
                re_units = re.compile('(?<=\[).*(?=\])')
                m = re_units.search(l)
                unit = m.group(0)
            except (IndexError, AttributeError):
                unit = ""
            
            for key in units_map.iterkeys():
                if key in unit:
                    unit = units_map[key]
            units.append(unit)

        if 'NMEA' in l:
           s = l.split('=')
           if 'Latitude' in s[0]:
               lat = s[1].split()
               lat = fns.ddm_to_dd(lat)
               self.globals['LATITUDE'] = lat
           elif 'Longitude' in s[0]:
               lon = s[1].split()
               lon = fns.ddm_to_dd(lon)
               self.globals['LONGITUDE'] = lon
           elif 'Time' in s[0]:
               dt = datetime.datetime.strptime(s[1].strip(), '%b %d %Y %H:%M:%S')
               self.globals['DATE'] = dt.strftime('%Y%m%d')
               self.globals['TIME'] = dt.strftime('%H%M')

        l = handle.readline()

    # Check columns and units to match length
    if len(columns) is not len(units):
        raise ValueError(("Expected as many columns as units in file. "
                          "Found %d columns and %d units.") % \
                         (len(columns), len(units)))

    self.create_columns(columns, units, None)
    LOG.info(columns)
    LOG.info(units)
    
    l = handle.readline()
    while l:
        if l == '':
            break
        values = l.split()

        if len(columns) is not len(values):
            raise ValueError(
                ("Expected as many columns as values in file (%s). Found %d "
                 "columns and %d values at data line %d") % \
                 (handle.name, len(columns), len(values), len(self) + 1))

        for column, value in zip(columns, values):
            col = self.columns[column]
            col.append(value, flag_woce=2)
        l = handle.readline()
    if remove_calculated: #TODO make this better
        for param in calculated:
            del self.columns[param]
