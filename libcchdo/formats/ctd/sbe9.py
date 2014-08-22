from datetime import datetime
import re
from logging import getLogger


log = getLogger(__name__)


from libcchdo.fns import ddm_to_dd


def read(self, handle, salt='first', temp='first'):
    """How to read an SBE 9 ASCII CTD File"""

    l = handle.readline()
    if not l.lower().startswith('* sea-bird sbe 9 data file:'):
        raise ValueError(('Expected SBE 9 identifier:'
                           '\'* Sea-Bird SBE 9 Data File:\'\n'
                           'Instead got: %s') % l)

    parameter_map = {'prDM':'CTDPRS',
                     'tn90C':'CTDTMP',
                     'sbeox0ML/L':'CTDOXY', # convert to umol/kg
                     'xmiss':'XMISS',
                     'salnn':'CTDSAL',
                     }

    units_map = {'db':'DBAR',
                 'PSU':'PSS-78',
                 'ITS-90':'ITS-90',
                 'ITS-68':'ITS-68',
                 }

    # infos that will be tracked
    headers = []
    columns = []
    units = []
    index = [] # the column position of the things we are keeping
    salts = [] # for storing multiple salts
    temps = [] # for the multiple temps
    num_cols = 0 # to verify that each record has the expected number of params
    bad_flag = None

    # regex for various things
    re_units = re.compile('(?<=\[).*(?=\])')
    re_salts = re.compile('sal\d\d')
    re_temp = re.compile('t\d90C')

    while l and not l.startswith('*END*'):
        # This is what needs to happen here
        # 1) all the headers need to be saved up to the *END* tag
        # 2) all the things that start with # name (some number) need to be
        # extracted for information as this is where the column order is
        # 3) Calculated parameters like sigma and potential temperature should
        # be discarded. Maybe have an option to keep(?)

        headers.append(l.decode('raw_unicode_escape'))

        if l.startswith('** CAST'):
            self.globals['CASTNO'] = l.split(':')[1].strip()
            log.info("CASTNO: " + self.globals['CASTNO'])
            l = handle.readline()
            continue

        if l.startswith('** STATION'):
            self.globals['STNNBR'] = l.split(':')[1].strip()
            log.info('STNNBR: ' + self.globals['STNNBR'])
            l = handle.readline()
            continue

        if l.startswith('** CRUISE'):
            self.globals['EXPOCODE'] = l.split(':')[1].strip()
            log.info('EXPOCODE: ' + self.globals['EXPOCODE'])
            l = handle.readline()
            continue
            
        if 'NMEA' in l:
           s = l.split('=')
           if 'Latitude' in s[0]:
               lat = s[1].split()
               lat = ddm_to_dd(lat)
               self.globals['LATITUDE'] = lat
               l = handle.readline()
               continue

           elif 'Longitude' in s[0]:
               lon = s[1].split()
               lon = ddm_to_dd(lon)
               self.globals['LONGITUDE'] = lon
               l = handle.readline()
               continue
           
           elif 'UTC' in s[0]:
               dt = datetime.strptime(s[1].strip(), '%b %d %Y %H:%M:%S')
               self.globals['DATE'] = dt.strftime('%Y%m%d')
               self.globals['TIME'] = dt.strftime('%H%M')
               l = handle.readline()
               continue

        if '# bad_flag' in l:
            s = l.split('=')
            bad_flag = s[1].strip()
            log.info('BAD_FLAG: ' + bad_flag)
            l = handle.readline()
            continue


        if '# name' in l:
            s = l.split('=')
            i = [int(d) for d in s[0].split() if d.isdigit()]
            num_cols += 1

            for key in parameter_map.iterkeys():
                if key == 'tn90C':
                    m = re_temp.search(l)
                    if m is not None:
                        temps.append((i, m.group(0)))

                if key == 'salnn':
                    m = re_salts.search(l)
                    if m is not None:
                        salts.append((i, m.group(0)))

                if key in l:
                    columns.append(parameter_map[key])
                    index.append(i)
                    
                    try:
                        m = re_units.search(l)
                        unit = m.group(0)
                    except (IndexError, AttributeError):
                        unit = ""
            
                    for key in units_map.iterkeys():
                        if key in unit:
                            unit = units_map[key]
                    units.append(unit)


        l = handle.readline()
    # Have yet to encounter a SBE file with bottom depth in it, JOA will crash
    # if the DEPTH is blank
    self.globals['DEPTH'] = '-999'
    self.globals['SECT_ID'] = ''
    if 'STNNBR' not in self.globals:
        # not sure if critical is approprate here, but this causes a ton of
        # problems for the exchange writer, and ODV
        log.critical('Station number not found, this is bad')
        log.warn('Station Number will be determined from file name, this may '
                'not work')
        self.globals['STNNBR'] = ''.join(re.findall("\d+", handle.name))

    if 'CASTNO' not in self.globals:
        self.globals['CASTNO'] = ''
        log.warn('Cast Number not found, this can cause problems')

    if 'EXPOCODE' not in self.globals:
        self.globals['EXPOCODE'] = ''
        log.warn('ExpoCode not found, this can cause problems')

    if len(temps) == 1:
        index.append(temps[0][0])
        columns.append("CTDTMP")
        units.append("ITS-90")

    if len(temps) > 1:
        if temp == 'first':
            index.append(temps[0][0])
            columns.append("CTDTMP")
            units.append("ITS-90")
            log.warn("%i Temperatures found, using first", len(temps))
            s = ''
            for i, temp in enumerate(temps):
                s += str(i) + ':' + temp[1] + ' '
            log.info('The temp may be chosen by specifying the index, %s', s)
        else:
            index.append(temps[temp][0])
            columns.append("CTDTMP")
            units.append("ITS-90")
            log.info('User specified temperature, channel: %d, name: %s',
                    temps[temp][0][0], temps[temp][1])
            
    if len(salts) == 1:
        index.append(salts[0][0])
        columns.append("CTDSAL")
        units.append("PSS-78")

    if len(salts) > 1:
        if salt == 'first':
            index.append(salts[0][0])
            columns.append("CTDSAL")
            units.append("PSS-78")
            log.warn("%i Salinites found, using first", len(salts))
            s = ''
            for i, salt in enumerate(salts):
                s += str(i) + ':' + salt[1] + ' '
            log.info('The salinity may be chosen by specifying the index, %s', s)
        else:
            #assuming the salinity index is given... will blow up if not
            index.append(salts[salt][0])
            columns.append("CTDSAL")
            units.append("PSS-78")
            log.info('User specified salinity, channel: %d, name: %s',
                    salts[salt][0][0], salts[salt][1])

    
    # Check columns and units to match length
    if len(columns) is not len(units):
        raise ValueError(("Expected as many columns as units in file. "
                          "Found %d columns and %d units.") % \
                         (len(columns), len(units)))

    self.create_columns(columns, units, None)
    
    l = handle.readline()
    while l:
        if l == '':
            break
        values = l.split()

        if num_cols is not len(values):
            raise ValueError(
                ("Expected as many columns as values in file (%s). Found %d "
                 "columns and %d values at data line %d") % \
                 (handle.name, len(columns), len(values), len(self) + 1))

        keep = []
        for i in index:
            keep.append(values[i[0]])

        for column, value in zip(columns, keep):
            col = self.columns[column]
            flag = 2
            if value == bad_flag:
                value = None
                flag = 9
            col.append(value, flag_woce=flag)
        l = handle.readline()
