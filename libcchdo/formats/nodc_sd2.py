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
"""


def read(self, handle):
    """How to read an NODC SD2 file."""
    expocode, sect, sectid, ship, program = handle.readline()[1:].split()
    dates = handle.readline()
    unknown = handle.readline()
    pi = handle.readline()
    for i in range(7):
        handle.readline()

    self.create_columns(
        ('EXPOCODE', 'SECT_ID', '_DATETIME', 'LATITUDE', 'LONGITUDE'))

    # PRES -> CTDPRS CCHDO has no other measurement of pressure
    # TEMP -> CTDTMP Probably not reference nor potential
    # PSAL -> SALNTY Probably a bottle (PSU doesn't match CCHDO's PSS-78)
    # CPHL -> CHLORA (mg/m**3 doesn't match CCHDO's ug/kg)
    # PHOS -> PHSPHT (umol/l doesn't match CCHDO's umol/kg)
    # NTRZ -> NO2+NO3 (umol/l doesn't match CCHDO's umol/kg)
    self.create_columns(
        ('CTDPRS', 'CTDTMP', 'SALNTY', 'CHLORA', 'PHSPHT', 'NO2+NO3'),
        ('DBAR', 'DEG C', 'PSU', 'MG/M3', 'UMOL/L', 'UMOL/L'))

    while handle:
        line = handle.readline()
        if not line:
            break

        if line[79] == '1':
            print {
                'continuation_indicator': bool(line[0]),
                'nodc_ref_num_country': int(line[2:4]),
                'nodc_ref_num_file_code': int(line[4]),
                'nodc_ref_num_cruise_number': int(line[5:9]),
                'nodc_consecutive_station_number': int(line[9:13]),
                'data_type': int(line[13:15]),
                'ten-degree_square': int(line[17:21]),
                'one-degree_square': int(line[21:23]),
                'two-degree_square': int(line[23:25]),
                'five-degree_square': int(line[25]),
                'hemisphere_of_latitude': line[26],
                'degrees_latitude': int(line[27:29]),
                'minutes_latitude': int(line[29:31]),
                'minutes_latitude_tenths': int(line[31]),
                'hemisphere_of_longitude': line[32],
                'degrees_longitude': int(line[33:36]),
                'minutes_longitude': int(line[36:38]),
                'minutes_longitude_tenths': int(line[38]),
                'quarter_of_one_degree_square': int(line[39]),
                'year_gmt': int(line[40:42]),
                'month_of_year_gmt': int(line[42:44]),
                'day_of_month_gmt': int(line[44:46]),
                'station_time_gmt_hours_to_tenths': line[46:49],
                'data_origin_country': line[49:51],
                'data_origin_institution': line[51:53],
                'data_origin_platform': line[53:55],
                'bottom_depth': line[55:60],
                'effective_depth': int(line[60:64]),
                'cast_duration_hours_to_tenths': line[64:67],
                'cast_direction': line[67],
                'data_use_code': line[69],
                'minimum_depth': int(line[70:74]),
                'maximum_depth': int(line[74:78]),
                'always_2_next_record_indicator': line[78],
                'always_1_record_indicator': int(line[79]),
            }
        elif line[79] == '2':
            print {
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
                'originators_station_identifier': int(line[17:28]),
                'minutes_latitude': int(line[29:31]),
                'minutes_latitude_tenths': int(line[31]),
                'hemisphere_of_longitude': line[32],
                'degrees_longitude': int(line[33:36]),
                'minutes_longitude': int(line[36:38]),
                'minutes_longitude_tenths': int(line[38]),
                'quarter_of_one_degree_square': int(line[39]),
                'year_gmt': int(line[40:42]),
                'month_of_year_gmt': int(line[42:44]),
                'day_of_month_gmt': int(line[44:46]),
                'station_time_gmt_hours_to_tenths': line[46:49],
                'data_origin_country': line[49:51],
                'data_origin_institution': line[51:53],
                'data_origin_platform': line[53:55],
                'bottom_depth': line[55:60],
                'effective_depth': int(line[60:64]),
                'cast_duration_hours_to_tenths': line[64:67],
                'cast_direction': line[67],
                'data_use_code': line[69],
                'minimum_depth': int(line[70:74]),
                'maximum_depth': int(line[74:78]),
                'always_2_next_record_indicator': line[78],
                'always_1_record_indicator': int(line[79]),
            }
        elif line[79] == '3':
            print 'Data'

    self.globals['stamp'] = ''
    self.globals['header'] = ''

    self.check_and_replace_parameters()
