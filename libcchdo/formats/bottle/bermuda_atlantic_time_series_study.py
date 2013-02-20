from datetime import datetime
from re import compile
from copy import copy

from libcchdo import LOG
from libcchdo.fns import Decimal, equal_with_epsilon
from libcchdo.model.datafile import DataFile, DataFileCollection
from libcchdo.db.model.nodc_ship import ship_code
from libcchdo.datadir import create_expocode
from libcchdo.formats.netcdf_oceansites import param_to_os, OSVar
from libcchdo.formats.bermuda_atlantic_time_series_study import (
    bats_time_to_dt, BATS_SECT_ID, correct_longitude, collapse_globals)


def _read_header_sections(self, handle):
    """Read sections of a BATS bottle file up to the variables section.

    This reads up to the "/Variables" line as the header and leaves the rest of
    the file untouched.

    """
    sections = {}

    curr = None
    l = handle.readline()
    while True:
        if l is None:
            break

        if l.startswith('/'):
            section_name = l[1:].strip()
            if section_name == 'Variables':
                break
            curr = sections[section_name] = []
        else:
            curr.append(l.strip())
        l = handle.readline()
    return sections


def _read_variables(self, handle):
    """Parse variables from the line right after /Variables.

    """
    l = handle.readline().strip()
    variables = [x.strip() for x in l.split(',')]
    descriptions = [None] * len(variables)
    units = [None] * len(variables)
    return zip(variables, descriptions, units)


def _parse_variables_from_section(section):
    """Parse the /Variables list.

    """
    var_re = compile('^([\w_()-]+)\s+=\s(.*)$')
    descr_re = compile('^(.*)\s\((.*)\)')

    variables = []
    descriptions = []
    units = []
    for l in section:
        match = var_re.match(l)
        if not match:
            continue
        varname, varunits = match.groups()
        variables.append(varname)

        match = descr_re.match(varunits)
        if match:
            d, u = match.groups()
            descriptions.append(d)
            units.append(u)
        else:
            descriptions.append(varunits)
            units.append(None)
    return zip(variables, descriptions, units)


def _get_variables(self, handle, sections):
    """Return the variables for the file.

    The bats_bottle.txt (the core file available) contains an incomplete
    variable line. This means we must obtain the variables from the Variable
    list section

    """
    return _parse_variables_from_section(sections['Variable list'])


def _parse_bats_id(id):
    """
    A unique bottle id which identifies cruise, cast, and Nisken number
    9 digit number !####$$@@, where,
    !    =Cruise type, 1=BATS core, 2=BATS Bloom a, and 3=BATS Bloom b, 
    #### =Cruise number
    $$   =Cast number, 1-80=CTD casts, 81-99=Hydrocasts (i.e. 83 = Data from
          Hydrocast number 3)
    @@   =Niskin number
    e.g. 100480410 is BATS core 48, cast 4, Niskin 10

    """
    type_id = id[0]
    cruise_id = id[1:5]
    cast = id[5:7]
    nisk = id[7:9]

    if type_id == '1':
        # Short for BATS core
        type = 'BATSCR'
    elif type_id == '2':
        # Short for BATS Bloom a
        type = 'BATSBLMA'
    elif type_id == '3':
        # Short for BATS Bloom b
        type = 'BATSBLMB'
    else:
        LOG.error(u'Invalid BATS cruise type {0} for id {1}'.format(type, id))
        return None
    try:
        num = int(cruise_id)
    except TypeError:
        LOG.error(u'Invalid BATS cruise number {0} for id {1}'.format(num, id))
        return None

    cast_type = None
    try:
        cast = int(cast)
    except TypeError:
        LOG.error(u'Invalid BATS cast number {0} for id {1}'.format(cast, id))
        return None
    if 1 <= cast and cast <= 80:
        cast_type = 'CTD'
        cast_num = cast
    elif 81 <= cast and cast <= 99:
        cast_type = 'Hydrocast'
        cast_num = cast

    try:
        nisk = int(nisk)
    except TypeError:
        LOG.error(u'Invalid Niskin number {0} for id {1}'.format(nisk, id))
        return None

    return type, type_id, num, cruise_id, cast_type, cast_num, nisk


def _ship_from_cruise_num(cruise_num):
    """
    R/V Weatherbird I (cruises 1,2,8,9,10,11,12 and 13)
    R/V Cape Henlopen (cruises 3,4,5,6 and 7)
    R/V Cape Hatteras (cruises 33,52,52a,53,53a,54,54a,55,55a and
        56,195,196,196a 208,208a,209)
    R/V Weatherbird II (all other cruises through 207)
    R/V Atlantic Explorer (cruises 210 through 241, and 243 onwards)
    R/V Oceanus (cruise 242)

    """
    if cruise_num in [1, 2, 8, 9, 10, 11, 12, 13]:
        return 'R/V Weatherbird I'
    elif cruise_num in [3, 4, 5, 6, 7]:
        return 'R/V Cape Henlopen'
    elif cruise_num in [33, 52, 53, 54, 55, 56, 195, 196, 208, 209]:
        return 'R/V Cape Hatteras'
    elif cruise_num <= 207:
        return 'R/V Weatherbird II'
    elif 210 <= cruise_num and cruise_num <= 241 or cruise_num >= 243:
        return 'R/V Atlantic Explorer'
    elif cruise_num == 242:
        return 'R/V Oceanus'
    else:
        LOG.error(u'Ship is unknown for cruise id {0}'.format(cruise_num))
        return None


# CF Standard names
# http://cf-pcmdi.llnl.gov/documents/cf-standard-names/standard-name-table/22/
# cf-standard-name-table.html
#
# SeaDataNet CF variables reference
# http://seadatanet.maris2.nl/v_bodc_vocab/welcome.aspx
# P021, BODC Parameter Discovery Vocabulary
bats_to_param = {
    'Depth': '_ACTUAL_DEPTH',
    'Temp': 'CTDTMP',
    'CTD_S': 'CTDSAL',
    'Sal1': 'SALNTY',
    'Sig-th': 'SIG0',
    'O2(1)': 'OXYGEN',
    'Alk': 'ALKALI',
    'NO31': 'NO2+NO3',
    'NO21': 'NITRIT',
    'PO41': 'PHSPHT',
    'Si1': 'SILCAT',
    'Bact': 'BACT',
}

param_to_os.register({
    'OxFixT': u'DOXY_TEMP',
    # TODO find standard_name
    'Anom1': OSVar(u'ANOM1', 'oxygen anomaly', None, 'umol/kg'),
    'CO2': OSVar(
        u'CO2', 'dissolved inorganic carbon',
        'mole_concentration_of_dissolved_inorganic_carbon_in_sea_water',
        'umol/kg'),
    'POP': OSVar(
        u'POP', 'pop',
        ('mole_concentration_of_particulate_organic_matter_expressed_as_'
         'phosphorus_in_sea_water'), 'umol/kg'),
    # SeaDataNet TDPX
    'TDP': OSVar(
        u'TDPX', 'total dissolved phosphorus', 'WC_DissPhosphorus', 'nmol/kg'),
    # TODO find standard_name
    'SRP': OSVar(u'SRP', 'low-level phosphorus', None, 'nmol/kg'),
    # TODO find standard_name
    'BSi': OSVar(u'BSI', 'particulate bigenic silica', None, 'umol/kg'),
    # TODO find standard_name
    'LSi': OSVar(u'LSI', 'particulate lithogenic silica', None, 'umol/kg'),
    # TODO find standard_name
    'Pro': OSVar(u'PRO', 'prochlorococcus', None, 'cells/ml'),
    # TODO find standard_name
    'Syn': OSVar(u'SYN', 'synechococcus', None, 'cells/ml'),
    # TODO find standard_name
    'Piceu': OSVar(u'PICEU', 'picoeukaryotes', None, 'cells/ml'),
    # TODO find standard_name
    'Naneu': OSVar(u'NANEU', 'nanoeukaryotes', None, 'cells/ml'),
})


def read(self, handle, metadata=None):
    """How to read a Bottle Bermuda Atlantic Time-Series Study file.

    This function reads bats_bottle.txt.

    Arguments:
    self - (special case, see NOTE) dictionary
    metadata - (optional) BATS cruise metadata to be used to find port dates

    NOTE: The result for this method is a special case. The bottle file format
    contains the entire BATS holdings while the internal data format splits data
    up by cruises. Because cruises for timeseries are split by file for cruise,
    the end result is a dictionary with cruise_ids as keys to
    DatafileCollections (cruises) containing Datafiles (casts). 

    """
    sections = _read_header_sections(self, handle)
    _read_variables(self, handle)
    parameters = _get_variables(self, handle, sections)

    # Add DON for note in Variables list stating DON is reported for TON prior
    # to BATS 121
    parameters.append(['DON', None, 'umol/kg'])

    manual_parameters = [
        ['BTLNBR', ''],
        ['_DATETIME', ''],
        ['LATITUDE', ''],
        ['LONGITUDE', ''],
        ['_ACTUAL_DEPTH', 'METERS'],
    ]
    columns = [x[0] for x in manual_parameters]
    units = [x[1] for x in manual_parameters]

    s = None
    for i, (var, d, u) in enumerate(parameters):
        if var == 'Depth':
            s = i + 1
            continue
        # Only want to add parameters after Depth. The others were done manually.
        if s is None:
            continue
        try:
            var = bats_to_param[var]
        except KeyError:
            pass
        columns.append(var)
        units.append(u)

    template_df = DataFile()
    template_df.create_columns(columns, units)
    template_df.check_and_replace_parameters(convert=False)

    for sec, lines in sections.items():
        if sec == 'Variables list':
            continue
        if sec != 'Comments':
            continue
        template_df.globals['_{0}'.format(sec)] = '\n'.join(lines)

    df = None
    params_auto = parameters[s:]
    dfi = 0
    for i, l in enumerate(handle):
        parts = l.split()

        id = parts[0]
        (cruise_type, type_id, cruise_num, cruise_id, cast_type, cast_id, 
         nisk_id) = _parse_bats_id(id)
        ship = _ship_from_cruise_num(cruise_num)
        if not ship:
            ship = 'R/V Atlantic Explorer'

        if (    df is None or 
                df.globals['_OS_ID'] != cruise_id or
                df.globals['STNNBR'] != cruise_type or
                df.globals['CASTNO'] != cast_id):
            if df:
                # Done reading one cast. Finalize it.
                LOG.info(u'finalizing cast {0} {1} {2}'.format(
                    df.globals['_OS_ID'], df.globals['STNNBR'],
                    df.globals['CASTNO']))
                try:
                    meta = metadata[cruise_id]
                    port_date = meta['dates'][0]
                except (TypeError, KeyError):
                    port_date = None
                if not port_date:
                    port_date = min(df['_DATETIME'])
                df.globals['EXPOCODE'] = create_expocode(
                    ship_code(ship, raise_on_unknown=False), port_date)
                df.globals['DEPTH'] = max(df['_ACTUAL_DEPTH'])
                collapse_globals(df, ['_DATETIME', 'LATITUDE', 'LONGITUDE'])
                # Normalize all the parameter column lengths. There may be
                # columns that did not get data written to them so make sure
                # they are just as long as the rest
                length = len(df)
                for c in df.columns.values():
                    c.set_length(length)
                try:
                    dfc = self[df.globals['_OS_ID']]
                except KeyError:
                    dfc = self[df.globals['_OS_ID']] = DataFileCollection()
                dfc.files.append(df)
                dfi = 0

            # Create a new cast
            df = copy(template_df)
            df.globals['SECT_ID'] = BATS_SECT_ID
            df.globals['_SHIP'] = ship
            df.globals['_OS_ID'] = cruise_id
            df.globals['STNNBR'] = cruise_type
            df.globals['CASTNO'] = cast_id

        df['BTLNBR'].set(dfi, nisk_id)

        dt_ascii = datetime.strptime(parts[1] + parts[3], '%Y%m%d%H%M')
        dt_deci = bats_time_to_dt(parts[2])
        if dt_ascii != dt_deci:
            LOG.warn(
                u'Dates differ on data row {0}: {5} {1!r}={2} '
                '{3!r}={4}'.format(i, parts[1] + parts[3], dt_ascii, parts[2],
                                   dt_deci, dt_deci - dt_ascii))
        df['_DATETIME'].set(dfi, dt_ascii)

        df['LATITUDE'].set(dfi, Decimal(parts[4]))
        df['LONGITUDE'].set(dfi, Decimal(correct_longitude(parts[5])))
        df['_ACTUAL_DEPTH'].set_check_range(dfi, Decimal(parts[6]))

        parts_auto = parts[s:]
        for p, v in zip(params_auto, parts_auto):
            param = p[0]
            try:
                param = bats_to_param[param]
            except KeyError:
                pass
            if cruise_num < 121 and param == 'TON':
                param = 'DON'
            
            if (    equal_with_epsilon(v, -9) or 
                    equal_with_epsilon(v, -9.9) or
                    equal_with_epsilon(v, -9.99)
                ):
                df[param].set_check_range(dfi, None)
            # TODO determine whether -10 is just bad formatting for -9.9
            elif equal_with_epsilon(v, -10):
                #LOG.warn(u'Possible missing data value {0}'.format(v))
                df[param].set_check_range(dfi, None)
            elif v == 0:
                LOG.warn(
                    u'Data under detection limit, set flag to '
                    'WOCE water sample questionable measurement')
                df[param].set_check_range(dfi, None, flag=3)
            else:
                df[param].set_check_range(dfi, Decimal(v))

        dfi += 1
        if i % 100 == 0:
            LOG.info(u'processed {0} lines'.format(i))
