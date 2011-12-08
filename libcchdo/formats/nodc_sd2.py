"""
Reader coded to spec from JOA About SD2 page

NODC SD2 format is a standard data exchange format created in the days of
80-column punch cards. It has many deficiencies, for example in terms of
handling the full data value resolution provided by today's oceanographic
equipment, in accepting modern preferences for units, and especially in
accepting data for the many oceanographic tracers such as CFCs, helium,
tritium, and radiocarbon that have proved to be of interest and value.  But it
is such a rigidly and well described format that all data which adhere to the
format specification can be read by a computer application which can read any
of the data. Hence many oceanographers have access to computer applications
which can use or import SD2 format data. NODC is now in the process of
replacing its recommended standard exchange format for oceanographic profile
data. If this transition is completed by the time Version 2.0 of the Atlas of
Ocean Sections is completed, we will provide the section data in that format as
well. Below is a description of the NODC Station Data (SD) format. Please see
the NODC web site at http://www.nodc.noaa.gov for further information. 
--------------------------------------------------------------------------------
MASTER RECORD 1:
****** *********** ****** ******

START  ATTRIBUTES ITEM
COLUMN
------     ----   ------


     1       I1   CONTINUATION INDICATOR
     2       1X   BLANK
     3       I2   NODC REFERENCE NUMBER - COUNTRY
     5       I1   NODC REFERENCE NUMBER - FILE CODE  always "5"
     6       I4   NODC REFERENCE NUMBER - CRUISE NUMBER
    10       I4   NODC CONSECUTIVE STATION NUMBER
    14       I2   DATA TYPE
    16       2X   BLANK
    18       I4   TEN-DEGREE SQUARE, WMO
    22       I2   ONE-DEGREE SQUARE, WMO
    24       I2   TWO-DEGREE SQUARE, WMO
    26       I1   FIVE-DEGREE SQUARE, WMO
    27       A1   N OR S      HEMISPHERE OF LATITUDE
    28       I2   DEGREES LATITUDE
    30       I2   MINUTES LATITUDE
    32       I1   MINUTES LATITUDE, TENTHS
    33       A1   W OR E      HEMISPHERE OF LONGITUDE
    34       I3   DEGREES LONGITUDE
    37       I2   MINUTES LONGITUDE
    39       I1   MINUTES LONGITUDE, TENTHS
    40       I1   QUARTER OF ONE-DEGREE SQUARE, WMO
    41       I2   YEAR, GMT
    43       I2   MONTH OF YEAR, GMT
    45       I2   DAY OF MONTH, GMT
    47     F3.1   STATION TIME, GMT HOURS TO TENTHS
    50       I2   DATA ORIGIN - COUNTRY
    52       I2   DATA ORIGIN - INSTITUTION
    54       A2   DATA ORIGIN - PLATFORM
    56       I5   BOTTOM DEPTH (WHOLE METERS)
 ** 61       I4   EFFECTIVE DEPTH (WHOLE METERS)
 ** 65     F3.1   CAST DURATION (HOURS TO TENTHS)
 ** 68       A1   CAST DIRECTION (U=UP,D=DOWN,A=AVG OF UP & DOWN CASTS)
    69       1X   BLANK
 ** 70       I1   DATA USE CODE
    71       I4   MINIMUM DEPTH
    75       I4   MAXIMUM DEPTH
    79       I1   ALWAYS 2 NEXT RECORD INDICATOR
    80       I1   ALWAYS 1 RECORD INDICATOR

 ** FIELD DEFINED BY NODC, CALCULATION NOT DONE BY THIS FACILITY.
--------------------------------------------------------------------------------
MASTER RECORD 2:
****** *********** ****** ******

START  ATTRIBUTES ITEM
COLUMN
------     ----   ------

     1       I4   DEPTH DIFFERENCE (BOTTOM DEPTH - MAXIMUM DEPTH)
 **  5       2X   SAMPLE INTERVAL
 **  7       A1   % SALINITY OBSERVED(0=1-9%, 9=90-99%, - = 0)
 **  8       A1   % OXYGEN OBSERVED(0=1-9%, 9=90-99%, - = 0)
 **  9       A1   % PHOSPHATE OBSERVED(0=1-9%, 9=90-99%, - = 0)
 ** 10       A1   % TOTAL PHOSPHOROUS OBSERVED(0=1-9%, 9=90-99%, - = 0)
 ** 11       A1   % SILICATE OBSERVED(0=1-9%, 9=90-99%, - = 0)
 ** 12       A1   % NITRITE OBSERVED(0=1-9%, 9=90-99%, - = 0)
 ** 13       A1   % NITRATE OBSERVED(0=1-9%, 9=90-99%, - = 0)
 ** 14       A1   % PH OBSERVED(0=1-9%, 9=90-99%, - = 0)
    15       A3   ORIGINATOR'S CRUISE IDENTIFIER
    18       A9   ORIGINATOR'S STATION IDENTIFIER
    27       I2   WATER COLOR FOREL-ULE SCALE (00-21)
    29       I2   WATER TRANSPARENCY SECCHI DEPTH (WHOLE METERS)
    31       I2   WAVE DIRECTION - WMO CODE 0885
    33       A1   WAVE HEIGHT - WMO CODE 1555
 ** 34       I1   SEA STATE
 ** 35       A2   WIND FORCE
 ** 37       I1   FILE UPDATE CODE
    38       A1   WAVE PERIOD - WMO CODE 3155
    39       I2   WIND DIRECTION - WMO CODE 0877
    41       I2   WIND SPEED  (KNOTS)
    43     F5.1   BAROMETRIC PRESSURE, MILLIBARS
    48     F4.1   DRY BULB TEMPERATURE,CELSIUS
    52       I1   DRY BULB TEMP,PRECISION (0=WHOLE DEG,1=TENTHS,9=BLANK)
    53     F4.1   WET BULB TEMPERATURE,CELSIUS
    57       I1   WET BULB TEMP,PRECISION (0=WHOLE DEG,1=TENTHS,9=BLANK)
    58       A2   WEATHER (X IN COL. 58 INDICATES WMO CODE 4501)
    60       I1   CLOUD TYPE - WMO CODE 0500
    61       I1   CLOUD AMOUNT - WMO CODE 2700
    62       I3   NUMBER OF OBSERVED DEPTHS
**  65       I2   NUMBER OF STANDARD DEPTH LEVELS
    67       I3   NUMBER OF DETAIL DEPTHS
    70       9X   BLANK
    79       I1   NEXT RECORD INDICATOR
    80       I1   ALWAYS 2   RECORD INDICATOR

 ** FIELD DEFINED BY NODC, NO DATA SAMPLED OR
    CALCULATION NOT DONE BY THIS FACILITY.
--------------------------------------------------------------------------------
DATA RECORD:
****** *********** ****** ******

START  ATTRIBUTES ITEM
COLUMN
------     ----   ------


     1       I5   DEPTH, WHOLE METERS
     6       I1   DEPTH QUALITY INDICATOR
     7       A1   THERMOMETRIC DEPTH FLAG
     8     F5.3   TEMPERATURE, CELSIUS
    13       I1   TEMPERATURE, PRECISION (1,2, OR 3, 9=BLANK)
    14       I1   TEMPERATURE QUALITY INDICATOR
    15     F5.3   SALINITY, PRACTICAL SALINITY UNITS
    20       I1   SALINITY PRECISION (1,2, OR 3, 9=BLANK)
    21       I1   SALINITY QUALITY INDICATOR
 ** 22       I4   SIGMA-T
 ** 26       I1   SIGMA-T QUALITY INDICATOR
 ** 27       I5   SOUND SPEED (METERS/SECOND TO TENTHS)
 ** 32       I1   SOUND SPEED PRECISION
    33     F4.2   OXYGEN, MILLILITERS/LITER
    37       I1   OXYGEN PRECISION (1 OR 2, 9=BLANK)
    38       I1   OXYGEN QUALITY INDICATOR
 ** 39       I1   DATA RANGE CHECK FLAGS   PHOSPHATE > 4.00
 ** 40       I1    0=IN RANGE,             TOTAL PHOSPHATE < PHOSPHATE
 ** 41       I1    1=OUT OF RANGE          SILICATE > 300.0
 ** 42       I1                            NITRITE > 4.0
 ** 43       I1                            NITRATE > 45.0
 ** 44       I1                            PH < 7.40 OR > 8.50
    45      F3.1  CAST START TIME OR MESSENGER RELEASE TIME
    48       I1   CAST NUMBER
    49     F4.2   INORGANIC PHOSPHATE (MICROGRAM-ATOMS/LITER)
    53       I1   INORGANIC PHOSPHATE, PRECISION (1,2 OR 9=BLANK)
 ** 54     F4.2   TOTAL PHOSPHOROUS
 ** 58       I1   TOTAL PHOSPHOROUS, PRECISION (1, 2 OR 9=BLANK)
    59     F4.1   SILICATE (MICROGRAM-ATOMS/LITER)
    63       I1   SILICATE PRECISION (1 OR 9=BLANK)
    64     F3.2   NITRITE (MICROGRAM-ATOMS/LITER)
    67       I1   NITRITE PRECISION (1, 2 OR 9=BLANK)
    68     F3.1   NITRATE (MICROGRAM-ATOMS/LITER)
    71       I1   NITRATE PRECISION (1 OR 9=BLANK)
    72     F3.2   PH
    75       I1   PH, PRECISION
    76       2X   BLANK
 ** 78       I1   DENSITY INVERSION FLAG
    79       I1   NEXT RECORD TYPE
    80       I1   RECORD TYPE

 ** FIELD DEFINED BY NODC, NO DATA SAMPLED OR
       CALCULATION NOT DONE BY THIS FACILITY.

NODC numeric CODES
http://www.nodc.noaa.gov/General/NODC-Archive/numcode.txt
"""


import datetime
import collections

from ..fns import Decimal


_MAX_GRATICULE_PRECISION = 4


_DATA_TYPE_CODES = {
    19: 'NANSEN CAST',
    22: 'NODC SELECTED DEPTHS FROM CTD/STD',
    62: 'ORIGINATOR SELECTED DEPTHS FROM CTD/STD',
}


_NODC_0608_TO_WOCE_FLAGS = collections.defaultdict(lambda: 2, {
        # Depth measured by uncorrected wire-out (possible error)
        6: 3,
        7: 3,
        8: 3,
        # TODO do WOCE flags care about density inversion?
        9: 2,
    })


def read(self, handle):
    """How to read an NODC SD2 file."""

    self.create_columns(
        ('EXPOCODE', 'STNNBR', 'CASTNO', '_DATETIME', 'LATITUDE', 'LONGITUDE', ))
    self.create_columns(
        ('BOTTOM', 'DEPTH', 'CTDTMP', 'SALNTY', 'OXYGEN', 'PHSPHT', 'SILCAT', 'NITRIT', 'NITRAT', 'PH'),
        ('METERS', 'METERS', 'DEG C', 'PSU', 'ML/L', 'UMOL/L', 'UMOL/L', 'UMOL/L', 'UMOL/L', '', ))


    def int_or_none(i):
        try:
            return int(i)
        except ValueError:
            return None

    current_station = None
    current_cast = 1

    while handle:
        line = handle.readline()
        if not line:
            break

        if line[79] == '2':
            # Nothing in MR 2 that matters.
            continue

        #print 'line:', line,
        #print 'rule:', '|-`-*+-`-*' * 8

        if line[79] == '1':
            station = {}
            raw_line = {
                'continuation_indicator': int_or_none(line[0]),
                'nodc_ref_num_country': int_or_none(line[2:4]),
                'file_code': int_or_none(line[4]),
                'nodc_ref_num_cruise_number': int_or_none(line[5:9]),
                'nodc_consecutive_station_number': int_or_none(line[9:13]),
                'data_type': int_or_none(line[13:15]),
                'ten-degree_square': int_or_none(line[17:21]),
                'one-degree_square': int_or_none(line[21:23]),
                'two-degree_square': int_or_none(line[23:25]),
                'five-degree_square': int_or_none(line[25]),
                'hemisphere_of_latitude': line[26],
                'degrees_latitude': int_or_none(line[27:29]),
                'minutes_latitude': int_or_none(line[29:31]),
                'minutes_latitude_tenths': int_or_none(line[31]),
                'hemisphere_of_longitude': line[32],
                'degrees_longitude': int_or_none(line[33:36]),
                'minutes_longitude': int_or_none(line[36:38]),
                'minutes_longitude_tenths': int_or_none(line[38]),
                'quarter_of_one_degree_square': int_or_none(line[39]),
                'year_gmt': int_or_none(line[40:42]),
                'month_of_year_gmt': int_or_none(line[42:44]),
                'day_of_month_gmt': int_or_none(line[44:46]),
                'station_time_gmt_hours_to_tenths': line[46:49],

                # This is marked as platform on 
                # http://www.nodc.noaa.gov/General/NODC-Archive/sd2.html
                'data_origin_country': line[49:51],
                # These two are marked blank
                'data_origin_institution': line[51:53],
                'data_origin_platform': line[53:55],
                'bottom_depth': line[55:60],
                'effective_depth': int_or_none(line[60:64]),
                'cast_duration_hours_to_tenths': line[64:67],
                'cast_direction': line[67],
                'data_use_code': line[69],
                'minimum_depth': int_or_none(line[70:74]),
                'maximum_depth': int_or_none(line[74:78]),
                'always_2_next_record_indicator': line[78],
                'always_1_record_indicator': line[79],
            }

            assert raw_line['file_code'] == 5, \
                "Master Record 1 is corrupt. File Code should always be 5."
            assert raw_line['always_2_next_record_indicator'] == '2' or \
                   raw_line['always_2_next_record_indicator'].strip() == '', \
                "Master Record 1 is corrupt."
            assert raw_line['always_1_record_indicator'] == '1', \
                "Not master record 1. Algorithm is wrong."

            if raw_line['continuation_indicator']:
                continuation_indicator = raw_line['continuation_indicator']
                # 0 - one station record
                # 1 - first station record
                # 2-8 - intermediate records
                # 9 - last station record
                # TODO handle multiple records per stations

            station['EXPOCODE'] = raw_line['nodc_ref_num_cruise_number']
            station['STNNBR'] = raw_line['nodc_consecutive_station_number']
            station['_DATA_TYPE'] = _DATA_TYPE_CODES.get(raw_line['data_type'],
                                                         'UNKNOWN')

            if not raw_line['hemisphere_of_latitude'] in ('N', 'S'):
                raise ValueError(
                    ("Master Record 1 is corrupt. Latitude hemisphere must be "
                     "N or S."))

            latitude = str(
                (1 if raw_line['hemisphere_of_latitude'] == 'N' else -1) * \
                (raw_line['degrees_latitude'] + 
                 raw_line['minutes_latitude'] / 60.0 + 
                 raw_line['minutes_latitude_tenths'] / 600.0))
            latitude = latitude[:latitude.find('.') + \
                                 _MAX_GRATICULE_PRECISION + 1]
            station['LATITUDE'] = Decimal(latitude)

            if not raw_line['hemisphere_of_longitude'] in ('E', 'W'):
                raise ValueError(
                    ("Master Record 1 is corrupt. Longitude hemisphere must be "
                     "E or W."))

            longitude = str(
                (1 if raw_line['hemisphere_of_longitude'] == 'E' else -1) * \
                (raw_line['degrees_longitude'] + 
                 raw_line['minutes_longitude'] / 60.0 + 
                 raw_line['minutes_longitude_tenths'] / 600.0))
            longitude = longitude[:longitude.find('.') + \
                                   _MAX_GRATICULE_PRECISION + 1]
            station['LONGITUDE'] = Decimal(longitude)

            hours = int(raw_line['station_time_gmt_hours_to_tenths'][:2])
            minutes = int(raw_line['station_time_gmt_hours_to_tenths'][2]) * 6

            station['_DATETIME'] = datetime.datetime(
                *(1900 + raw_line['year_gmt'], raw_line['month_of_year_gmt'],
                  raw_line['day_of_month_gmt']), hour=hours, minute=minutes)

            try:
                station['BOTTOM'] = int(raw_line['bottom_depth'])
            except ValueError:
                station['BOTTOM'] = None

            current_station = station
        elif line[79] == '2':
            raw_line = {
                'depth_difference': line[0:4],
                'sample_interval': line[4:6],
                'salinity_observed': line[6],
                'oxygen_observed': line[7],
                'phosphate_observed': line[8],
                'total_phosphorous_observed': line[9],
                'silicate_observed': line[10],
                'nitrite_observed': line[11],
                'nitrate_observed': line[12],
                'ph_observed': line[13],
                'originators_cruise_identifier': line[13:17],
                'originators_station_identifier': int_or_none(line[17:26]),
                'water_color': int_or_none(line[26:28]),
                'water_transparency': int_or_none(line[28:30]),
                'wave_direction': int_or_none(line[30:32]),
                'wave_height': line[32],
                'sea_state': int_or_none(line[33]),
                'wind_force': line[34:36],
                'file_update_code': int_or_none(line[36]),
                'wave_period': line[37],
                'wind_direction': int_or_none(line[38:40]),
                'wind_speed': int_or_none(line[40:42]),
                'barometric_pressure': line[42:47],
                'dry_bulb_temperature': line[47:51],
                'dry_bulb_temperature_precision': int_or_none(line[51]),
                'wet_bulb_temperature': line[52:56],
                'wet_bulb_temperature_precision': int_or_none(line[56]),
                'weather': line[57:59],
                'cloud_type': int_or_none(line[59]),
                'cloud_amount': int_or_none(line[60]),
                'number_of_observed_depths': int_or_none(line[61:64]),
                'number_of_standard_depth_levels': int_or_none(line[64:66]),
                'number_of_detail_depths': int_or_none(line[66:69]),
                'blank': line[69:78],
                'next_record_indicator': line[78],
                'always_2_record_indicator': line[79],
            }
            # Effectively nothing here to care about.
        elif line[79] == '3':
            raw_line = {
                'depth': int_or_none(line[0:5]),
                'depth_quality_indicator': int_or_none(line[5]),
                'thermometric_depth_flag': line[6],
                'temperature': line[7:12],
                'temperature_precision': int_or_none(line[12]),
                'temperature_quality_indicator': int_or_none(line[13]),
                'salinity': line[14:19],
                'salinity_precision': int_or_none(line[19]),
                'salinity_quality_indicator': int_or_none(line[20]),
                'sigma-t': int_or_none(line[21:25]),
                'sigma-t_quality_indicator': int_or_none(line[25]),
                'sound_speed': int_or_none(line[26:31]),
                'sound_speed_precision': int_or_none(line[31]),
                'oxygen': line[32:36],
                'oxygen_precision': int_or_none(line[36]),
                'oxygen_quality_indicator': int_or_none(line[37]),
                'data_range_check_flags_phosphate': int_or_none(line[38]),
                'data_range_check_flags_total': int_or_none(line[39]),
                'data_range_check_flags_silicate': int_or_none(line[40]),
                'data_range_check_flags_nitrite': int_or_none(line[41]),
                'data_range_check_flags_nitrate': int_or_none(line[42]),
                'data_range_check_flags_ph': int_or_none(line[43]),
                'cast_start_time_or_messenger_release_time': line[44:47],
                'cast_number': int_or_none(line[47]),
                'inorganic_phosphate': line[48:52],
                'inorganic_phosphate_precision': int_or_none(line[52]),
                'total_phosphorous': line[53:57],
                'total_phosphorous_precision)': int_or_none(line[57]),
                'silicate': line[58:62],
                'silicate_precision': int_or_none(line[62]),
                'nitrite': line[63:66],
                'nitrite_precision': int_or_none(line[66]),
                'nitrate': line[67:70],
                'nitrate_precision': int_or_none(line[70]),
                'ph': line[71:74],
                'ph_precision': int_or_none(line[74]),
                'blank': line[75:77],
                'density_inversion_flag': int_or_none(line[77]),
                'next_record_type': int_or_none(line[78]),
                'record_type': int_or_none(line[79]),
            }

            sample = {}

            assert raw_line['record_type'] == 3, \
                ("Only observations are handled by this reader. "
                 "Interpolations are not handled.")

            sample['DEPTH'] = raw_line['depth']
            sample['DEPTH_QC'] = raw_line['depth_quality_indicator']
            p = raw_line['temperature_precision']
            if p and p != 9:
                x = raw_line['temperature'].strip()
                sample['TEMPERATURE'] = Decimal(
                    '%s.%s' % (x[:-p], x[-p:])) 
                sample['TEMPERATURE_QC'] = \
                    raw_line['temperature_quality_indicator']
            p = raw_line['salinity_precision']
            if p and p != 9:
                x = raw_line['salinity'].strip()
                sample['SALINITY'] = Decimal(
                    '%s.%s' % (x[:-p], x[-p:]))
                sample['SALINITY_QC'] = raw_line['salinity_quality_indicator']
            p = raw_line['oxygen_precision']
            if p and p != 9:
                x = raw_line['oxygen'].strip()
                sample['OXYGEN'] = Decimal(
                    '%s.%s' % (x[:-p], x[-p:]))
                sample['OXYGEN_QC'] = raw_line['oxygen_quality_indicator']
            try:
                x = raw_line['cast_start_time_or_messenger_release_time']
                sample['TIME'] = x[:2] + str(int(x[2]) / 600.0)
            except ValueError:
                pass
            sample['CASTNO'] = raw_line['cast_number']
            # TODO ensure this is inorganic
            p = raw_line['inorganic_phosphate_precision']
            if p and p != 9:
                x = raw_line['inorganic_phosphate'].strip()
                sample['PHSPHT'] = Decimal('%s.%s' % (x[:-p], x[-p:]))
            p = raw_line['silicate_precision']
            if p and p != 9:
                x = raw_line['silicate'].strip()
                sample['SILCAT'] = Decimal('%s.%s' % (x[:-p], x[-p:]))
            p = raw_line['nitrite_precision']
            if p and p != 9:
                x = raw_line['nitrite'].strip()
                sample['NITRIT'] = Decimal('%s.%s' % (x[:-p], x[-p:]))
            p = raw_line['nitrate_precision']
            if p and p != 9:
                x = raw_line['nitrate'].strip()
                sample['NITRAT'] = Decimal('%s.%s' % (x[:-p], x[-p:]))
            # TODO which PH is this?
            p = raw_line['ph_precision']
            if p and p != 9:
                x = raw_line['ph'].strip()
                sample['PH'] = Decimal('%s.%s' % (x[:-p], x[-p:]))

            if not current_station:
                raise ValueError(("Malformed SD2 file: Data record found "
                                  "before master record"))
            if current_station['_DATA_TYPE'] == 'NANSEN CAST':
                merged_row = {
                    'EXPOCODE': current_station['EXPOCODE'],
                    'STNNBR': current_station['STNNBR'],
                    'LATITUDE': current_station['LATITUDE'],
                    'LONGITUDE': current_station['LONGITUDE'],
                    '_DATETIME': current_station['_DATETIME'],
                    'BOTTOM': current_station['BOTTOM'],
                    'CASTNO': sample['CASTNO'],
                    'DEPTH': sample['DEPTH'],
                    'DEPTH_FLAG_W': _NODC_0608_TO_WOCE_FLAGS[sample['DEPTH_QC']],
                    # TODO figure out what parameter this should be
                    'CTDTMP': sample['TEMPERATURE'],
                    'CTDTMP_FLAG_W': _NODC_0608_TO_WOCE_FLAGS[sample['TEMPERATURE_QC']],
                    'SALNTY': sample['SALINITY'],
                    'SALNTY_FLAG_W': _NODC_0608_TO_WOCE_FLAGS[sample['SALINITY_QC']],
                    'OXYGEN': sample['OXYGEN'],
                    'OXYGEN_FLAG_W': _NODC_0608_TO_WOCE_FLAGS[sample['OXYGEN_QC']],
                    'PHSPHT': sample.get('PHSPHT', None),
                    'SILCAT': sample.get('SILCAT', None),
                    'NITRIT': sample.get('NITRIT', None),
                    'NITRAT': sample.get('NITRAT', None),
                    'PH': sample.get('PH', None),
                }
                try:
                    merged_row['_DATETIME'].hour = sample['TIME'][:2]
                    merged_row['_DATETIME'].minute = sample['TIME'][2:]
                except KeyError:
                    pass
                i = len(self)
                self['EXPOCODE'].set(i, merged_row['EXPOCODE'])
                self['STNNBR'].set(i, merged_row['STNNBR'])
                self['CASTNO'].set(i, merged_row['CASTNO'])
                self['LATITUDE'].set(i, merged_row['LATITUDE'])
                self['LONGITUDE'].set(i, merged_row['LONGITUDE'])
                self['_DATETIME'].set(i, merged_row['_DATETIME'])
                self['BOTTOM'].set(i, merged_row['BOTTOM'])
                self['DEPTH'].set(i, merged_row['DEPTH'], merged_row['DEPTH_FLAG_W'])
                self['CTDTMP'].set(i, merged_row['CTDTMP'], merged_row['CTDTMP_FLAG_W'])
                self['SALNTY'].set(i, merged_row['SALNTY'], merged_row['SALNTY_FLAG_W'])
                self['OXYGEN'].set(i, merged_row['OXYGEN'], merged_row['OXYGEN_FLAG_W'])
                self['PHSPHT'].set(i, merged_row['PHSPHT'])
                self['SILCAT'].set(i, merged_row['SILCAT'])
                self['NITRIT'].set(i, merged_row['NITRIT'])
                self['NITRAT'].set(i, merged_row['NITRAT'])
                self['PH'].set(i, merged_row['PH'])
            else:
                # CTD
                raise NotImplementedError("Can't read SD2 CTDs yet")

    for key, column in self.columns.items():
        if len(filter(None, column.values)) == 0 and \
           len(filter(None, column.flags_woce)) == 0 and \
           len(filter(None, column.flags_igoss)) == 0:
           del self.columns[key]

    self.globals['stamp'] = ''
    self.globals['header'] = ''

    self.check_and_replace_parameters()
