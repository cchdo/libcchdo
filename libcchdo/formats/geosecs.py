""" Reader for GEOSECS Atlantic, Pacific, and Indian Ocean expeditions

A.  ORIGINATOR IDENTIFICATION

 1.  NAME AND ADDRESS OF INSTITUTION, LABORATORY WITH WHICH SUBMITTED DATA
      ARE ASSOCIATED:
     STS/Oceanographic Data Facility
     Scripps Institution of Oceanography
     University of California, San Diego  A-014
     La Jolla, CA 92093

 2.  EXPEDITION DURING WHICH DATA WERE COLLECTED:
     GEOSECS Atlantic, Pacific and Indian Oceans

 3.  CRUISE NUMBER USED BY ORIGINATOR TO IDENTIFY DATA IN THIS SHIPMENT:
     30  = Atlantic
     31  = Pacific
     135 = Indian Ocean

 4.  PLATFORM NAME:
     KNORR
     MELVILLE
     MELVILLE

 5.  PLATFORM TYPE:
     Research Vessel

 6.  PLATFORM AND OPERATOR NATIONALITY:
     PLATFORM: U.S.A.
     OPERATOR: U.S.A.

 7.  DATES: MO/DA/YR
	    Atlantic     Pacific    Indian Ocean
     FROM:  07/18/72     08/22/73    12/04/77
     TO:    04/01/73     06/10/74    04/24/78

 8.  RELEASE DATE IF DATA PROPRIETARY:
         N.A.

 9.  ARE DATA DECLARED NATIONAL PROGRAM (DNP)?
      Yes

10.  PERSON TO WHOM INQUIRIES CONCERNING DATA SHOULD BE ADDRESSED:
      ADDRESS SAME AS # 1.
      Robert T. Williams    or   Kristin M. Sanborn
        (619) 534-4426             (619) 534-1904

********DATA FORMAT
1.  RECORD TYPES
MASTER INFORMATION (AS PER NODC STATION DATA II) - IDENTIFIED BY A 1 IN
                   LAST CHARACTER OF logICAL RECORD OF 80 CHARACTERS

MASTER INFORMATION (AS PER NODC STATION DATA II) - IDENTIFIED BY A 2 IN
                   LAST CHARACTER OF logICAL RECORD OF 80 CHARACTERS

DATA RECORD (MODIFIED VERSION OF NODC STATION DATA II)  - IDENTIFIED BY A 3
              IN LAST CHARACTER OF logICAL RECORD OF 80 CHARACTERS

DATA RECORD - IDENTIFIED BY A 4 IN
              LAST CHARACTER OF logICAL RECORD OF 80 CHARACTERS

DATA RECORD - IDENTIFIED BY A 5 IN
              LAST CHARACTER OF logICAL RECORD OF 80 CHARACTERS

DATA RECORD - IDENTIFIED BY A 6 IN
              LAST CHARACTER OF logICAL RECORD OF 80 CHARACTERS

DATA RECORD - IDENTIFIED BY A 7 IN
              LAST CHARACTER OF logICAL RECORD OF 80 CHARACTERS

2. DESCRIPTION OF FILE ORGANIZATION

   logICAL RECORD LENGTH OF 80 CHARACTERS
   PHYSICAL RECORD LENGTH OF 3200 CHARACTERS
   FOR EACH STATION, TWO MASTER RECORD FOLLOWED BY AT LEAST ONE DATA
   RECORD (RECORD 3)FOR EACH LEVEL THEN MAXIMUM OF 4 DATA RECORDS FOR
      THE NISKINS AND MAXIMUM OF 2 FOR GERARDS
   FOR EACH OCEAN 2 FILES ONE NISKIN DATA SECOND GERARD FILE
   ATLANTIC FIRST 2 FILES FOLLOWED BY PACIFIC THEN INDIAN OCEAN
   FILE 1 = ATLANTIC NISKIN DATA
   FILE 2 = ATLANTIC GERARD DATA
   FILE 3 = PACIFIC NISKIN DATA
   FILE 4 = PACIFIC GERARD DATA
   FILE 5 = INDIAN OCEAN NISKIN DATA
   FILE 6 = INDIAN OCEAN GERARD DATA
   FILE 7 = DOCUMENTATION

3. ATTRIBUTES AS EXPRESSED IN FORTRAN

4.  LABEL
    SCRIPPS INSTITUTION OF OCEANOGRAPHY
    STS/OCEANOGRAPHIC DATA FACILITY    TAPE # 2
    ASCII;1600BPI NRZI;9-TRACK;PARITY ODD;
    FILES=7;BLOCK=3200;RECORD LENGTH=80
    PROJECT: GEOSECS SHIPBOARD/SHOREBASED DATA
    DATE: 5 FEBRUARY 1986

MASTER INFORMATION #1

STARTING FIELD
 COLUMN  WIDTH ATTRIBUTES  USAGE DESCRIPTION

    1     (1)      I1      NON-ZERO INDICATES MULTI-RECORD STATION
    2     (3)      A3      ORIGINATIOR'S NATIONALITY
   14     (2)      I2      19 = ROSETTE/NISKIN ID
   18     (4)      I4      CANADIAN 10-DEGREE SQUARE
   22     (2)      I2      ONE-DEGREE SQUARE;  CANADIAN SYSTEM
   24     (2)      I2      TWO-DEGREE SQAUARE; CANADIAN
   26     (1)      I1      FIVE-DEGREE SQUARE; CANADIAN
   27     (1)      A1      HEMISPHERE OF LATITUDE N OR S
   28     (2)      I2      DEGREES LATITUDE
   30     (2)      I2      MINUTES LATITUDE
   32     (1)      I1      MINUTES LATITUDE, TENTHS
   33     (1)      A1      HEMISPHERE OF LONGITUDE E OR W
   34     (3)      I3      DEGREES LONGITUDE
   37     (2)      I2      MINUTES LATITUDE
   39     (1)      I1      MINUTES LATITUDE, TENTHS
   41     (2)      I2      YEAR
   43     (2)      I2      MONTH OF YEAR, GMT
   45     (2)      I2      DAY OF MONTH, GMT
   50     (6)      A6      ALPHANUMERIC SHIP CODE; LEFT JUSTIFIED
   56     (5)      I5      DEPTH TO BOTTOM, METERS
   71     (4)      I4      FIRST OBSERVED DEPTH
   75     (4)      I4      MAXIMUM SAMPLING DEPTH
   79     (1)      I1      NEXT RECORD INDICATOR - ALWAYS 2
   80     (1)      I1      RECORD INDICATOR - ALWAYS 1

 MASTER INFORMATION #2

STARTING FIELD
 COLUMN  WIDTH ATTRIBUTES  USAGE DESCRIPTION

   15     (3)      A3      ORIGINATOR'S CRUISE
   18     (9)      A9      ORIGINATOR'S STATION (SEQUENCE #)
   62     (3)      I3      NUMBER OF OBSERVED DEPTH
   67     (3)      I3      NUMBER OF DETAIL DEPTHS
   79     (1)      I1      NEXT RECORD INDICATOR - ALWAYS 3
   80     (1)      I1      RECORD INDICATOR - ALWAYS 2

OBSERVED DEPTH INFORMATION

STARTING FIELD  ATTRIBUTES
 COLUMN  WIDTH             USAGE DESCRIPTION

    1     (5)      I5      DEPTH, METERS
    6     (1)      A1      PRESSURE QUALITY INDICATOR
    7     (1)      A1      TOTAL CO2 QUALITY INDICATOR
    8     (5)    F5.3      TEMPERATURE, CELSIUS
   13     (1)      I1      TEMPERATURE, PRECISION
   14     (1)      A1      TEMPERATURE QUALITY INDICATOR
   15     (5)    F5.3      SALINITY, PER MIL
   20     (1)      I1      SALINITY PRECISION
   21     (1)      A1      SALINITY QUALITY INDICATOR
   22     (1)      A1      OXYGEN QUALITY INDICATOR
   23     (1)      A1      PHOSPHATE QUALITY INDICATOR
   24     (1)      A1      NITRITE QUALITY INDICATOR
   25     (1)      A1      NITRATE QUALITY INDICATOR
   26     (1)      A1      ALKALINITY QUALITY INDICATOR
   27     (5)      I5      PRESSURE,DECIBARS
   32     (1)      A1      SILICATE QUALITY INDICATOR
   33     (4)      I4      OXYGEN, MICROMOLES/KIlogRAM
   37     (4)      I4      ALKALINITY, MICROEQUIVALENTS/KIlogRAM
   41     (4)      I4      TOTAL CO2, MICROMOLES/KIlogRAM
   45     (2)      I2      CAST NUMBER
   47     (2)      I2      BOTTLE NUMBER
   49     (4)    F4.2      PHOSPHATE, MICROMOLES/KIlogRAM
   53     (1)      I1      PHOSPHATE PRECISION
   54     (4)     F4.0     GC CO2, MICROMOLES/KIlogRAM
   58     (1)      I1      GC CO2, QUALITY INDICATOR
   59     (4)    F4.1      SILICATES, MICROMOLES/KIlogRAM
   63     (1)      I1      SILICATES PRECISION
   64     (3)    F3.2      NITRITES, MICROMOLES/KIlogRAM
   67     (1)      I1      NITRITES PRECISION
   68     (3)    F3.1      NITRATES, MICROMOLES/KIlogRAM
   71     (1)      I1      NITRATE PRECISION
   72     (3)     F3.2     PH
   75     (1)      I1      PH, PRECISION
   76     (3)      3X
   79     (1)      I1      NEXT RECORD TYPE
   80     (1)      I1      RECORD TYPE - ALWAYS 3
                     IF DATA NOT PRESENT, PRECISION FIELD = 9
                     QUALITY INDICATOR = FOOTNOTE (SEE ATTACHMENT #1)
                     PRECISION = NUMBER OF ACCEPTABLE DECIMAL PLACES

ATTACHMENT #1
ALPHABETIC CHARACTERS FOUND IN DATA RECORD 1 DESIGNATED AS QUALITY INDICATORS,
INDICATE THE FOLLOWING:
   A DATA TAKEN FROM CTD DOWN TRACE
   B TEMPERATURE CALCULATED FROM UNPROTECTED THERMOMETER
   C DEPTH CALCULATED FROM WIRE OUT
   D DATA EXTRACTED FROM CTD RECORDS (NORMALLY TAKEN BY DISRETE MEASUREMENTS;
     AS IS THE CASE FOR SALINITIES)
   E VALUES ORIGINALLY REPORTED IN REVERSE ORDER
   G DATA APPEARING TO BE IN ERROR, BUT WHICH HAS BEEN VERIFIED BY OTHER
     MEASUREMENTS
   H THERMOMETRIC DATA (NORMALLY MEASURED BY CTD; AS IS THE CASE FOR
      TEMPERATURES)
   K KNOWN ERROR
   P PRETRIP OR POSTTRIP
   U UNCERTAIN DATA

 PRECISION INDICATORS REFER TO ACCURACY AS RECOMMENDED BY OUR DATA FACILITY

NISKIN SHOREBASED DATA DATA RECORD 4

STARTING FIELD  ATTRIBUTES
 COLUMN  WIDTH             USAGE DESCRIPTION

    1    (3)       F3.0   #PARTICULATE CONCENTRATION    (UG/KG)    LDGO
    4    (1)         1X
    5    (2)       F2.1   #PARTICULATE CONC. PRECISION  (2 SIGMA)   LDGO
    7    (3)       F3.0   #PARTICULATE CONCENTRATION    (UG/KG)     WHOI
   10    (4)       F4.1   #BARIUM                       (NM/KG)     LSU
   14    (4)       F4.1   #BARIUM                       (NM/KG)     MIT
   18    (1)         1X
   19    (5)       I5     #TOTAL ORGANIC CARBON         (UM/KG)     OSU
   24    (4)       I4     #T ORGANIC CARBON PRECISION   (UM/KG)     OSU
   28    (1)         1X
   29    (4)       F4.2   #delta C13                    (PER MIL)   UH
   33    (1)         1X
   34    (4)       F4.2   #delta C13                    (PER MIL)   SIO
   38    (2)
   40    (3)       F3.1   #delta O-18 (DIS O2)          (PER MIL)   UH
   43    (4)       F4.2   #delta 18-O                   (PER MIL)   SIO
   47    (4)       F4.1   #DEUTERIUM                    (PERCENT)   SIO
   51    (3)       F4.1   #RA226                        (DPM/100KG) LDGO
   55    (2)         2X
   57    (2)       F2.1   #RA226  PRECISION             (DPM/100KG) LDGO
   59    (4)       F4.1   #RA226                        (DPM/100KG) SIO
   63    (2)         2X
   65    (2)       F2.1   #RA226  PRECISION             (DPM/100KG) SIO
   67    (2)       A2     #RA226  FOOTNOTE                          SIO
   69    (4)       F4.1   #RA226                        (DPM/100KG) USC
   73    (2)         2X
   75    (2)       F2.1   #RA226 PRECISION              (DPM/100KG) USC
   77    (2)         2X
   79    (1)       I1      NEXT RECORD TYPE
   80    (1)       I1      RECORD TYPE - ALWAYS 4

NISKIN SHOREBASED DATA DATA RECORD 5

STARTING FIELD  ATTRIBUTES
 COLUMN  WIDTH             USAGE DESCRIPTION

    1    (1)         1X
    2    (3)       F3.1    #PB210 (SOL)                 (DPM/100KG) SIO
    5    (2)         2X
    7    (2)       F2.1    #PB210 (SOL) PRECISION       (DPM/100KG) SIO
    9    (2)       A2      #PB210 (SOL) FOOTNOTE                    SIO
   11    (1)         1X
   12    (3)       F3.1    #PB210 (SOL)                 (DPM/100KG) WHOI
   15    (2)         2X
   17    (2)       F2.1    #PB210 (SOL) PRECISION       (DPM/100KG) WHOI
   19    (1)         1X
   20    (3)       F3.1    #PB210 (SOL)                 (DPM/100KG) YALE
   23    (2)         2X
   25    (2)       F2.1    #PB210 (SOL) PRECISION       (DPM/100KG) YALE
   27    (1)         1X
   28    (3)       F3.1    #PB210 (TOT)                 (DPM/100KG) SIO
   31    (2)         2X
   33    (2)       F2.1    #PB210 (TOT) PRECISION       (DPM/100KG) SIO
   35    (2)       A2      #PB210 (TOT) FOOTNOTE                    SIO
   37    (1)         1X
   38    (3)       F3.1    #PB210 (TOT)                 (DPM/100KG) YALE
   41    (2)         2X
   43    (2)       F2.1    #PB210 (TOT) PRECISION       (DPM/100KG) YALE
   45    (1)         1X
   46    (3)       F3.2    #PB210 (PAR)                 (DPM/100KG) SIO
   49    (1)         1X
   50    (3)       F3.2    #PB210 (PAR) PRECISION       (DPM/100KG) SIO
   53    (2)       A2      #PB210 (PAR) FOOTNOTE                    SIO
   55    (1)         1X
   56    (3)       F3.2    #PB210 (PAR)                 (DPM/100KG) YALE
   59    (1)         1X
   60    (3)       F3.2    #PB210 (PAR) PRECISION       (DPM/100KG) YALE
   63    (1)         1X
   64    (3)       F3.1    #PO210 (SOL)                 (DPM/100KG) SIO
   67    (2)         2X
   69    (2)       F2.1    #PO210 (SOL) PRECISION       (DPM/100KG) SIO
   71    (2)       A2      #PO210 (SOL) FOOTNOTE                    SIO
   73    (6)         6X
   79    (1)       I1      NEXT RECORD TYPE
   80    (1)       I1      RECORD TYPE - ALWAYS 5

NISKIN SHOREBASED DATA DATA RECORD 6

STARTING FIELD  ATTRIBUTES
 COLUMN  WIDTH             USAGE DESCRIPTION

    1    (1)         1X
    2    (3)       F3.1    #PO210 (SOL)                 (DPM/100KG) WHOI
    5    (2)         2X
    7    (2)       F2.1    #PO210 (SOL) PRECISION       (DPM/100KG) WHOI
    9    (5)         5X
   14    (3)       F3.1    #PO210 (SOL)                 (DPM/100KG) YALE
   17    (4)         4X
   21    (2)       F2.1    #PO210 (SOL) PRECISION       (DPM/100KG) YALE
   23    (1)         1X
   24    (3)       F3.2    #PO210 (PAR)                 (DPM/100KG) SIO
   27    (1)         1X
   28    (3)       F3.2    #PO210 (PAR) PRECISION       (DPM/100KG) SIO
   31    (2)       A2      #PO210 (PAR) FOOTNOTE                    SIO
   33    (1)         1X
   34    (4)       F4.2    #TRITIUM                     (TU)        UM
   38    (2)         2X
   40    (3)       F3.2    #TRITIUM    PRECISION        (SIGMA)     UM
   43    (2)         2X
   45    (3)       F3.2    #HELIUM                      (CC/KG * 10**-5) MCM
   48    (1)         1X
   49    (3)       F3.2    #HELIUM PRECISION            (CC/KG * 10**-5) MCM
   52    (2)       A2      #HELIUM FOOTNOTE                              MCM
   54    (2)         2X
   56    (3)       F3.2    #HELIUM                      (CC/KG * 10**-5) SIO
   59    (1)         1X
   60    (3)       F3.2    #HELIUM PRECISION            (CC/KG * 10**-5) SIO
   63    (2)       A2      #HELIUM FOOTNOTE                              SIO
   65    (2)         2X
   67    (3)       F3.2    #HELIUM                      (CC/KG * 10**-5) WHOI
   70    (1)         1X
   71    (2)       F2.1    #HELIUM PRECISION            (CC/KG * 10**-5) WHOI
   73    (6)         6X
   79    (1)       I1      NEXT RECORD TYPE
   80    (1)       I1      RECORD TYPE - ALWAYS 6

NISKIN SHOREBASED DATA DATA RECORD 7

STARTING FIELD  ATTRIBUTES
 COLUMN  WIDTH             USAGE DESCRIPTION

    1    (1)         1X
    2    (3)       F3.1     #delta 3HE                  (PERCENT)        MCM
    5    (1)         1X
    6    (2)       F2.1     #delta 3HE PRECISION        (PERCENT)        MCM
    8    (2)       A2       #delta 3HE FOOTNOTE                          MCM
   10    (1)         1X
   11    (3)       F3.1     #delta 3HE                  (PERCENT)        SIO
   13    (1)         1X
   15    (2)       F2.1     #delta 3HE PRECISION        (PERCENT)        SIO
   17    (2)       A2       #delta 3HE FOOTNOTE                          SIO
   19    (1)         1X
   20    (3)       F3.1     #delta 3HE                  (PERCENT)        WHOI
   23    (3)       F3.2     #delta 3HE PRECISION        (PERCENT)        WHOI
   26    (1)         1X
   27    (3)       F3.1     #DELTA HE/NE                (PERCENT)        MCM
   30    (2)         2X
   32    (2)       F2.1    #DELTA HE/NE PRECISION       (PERCENT)        MCM
   34    (1)         1X
   35    (3)       F3.1    #DELTA HE/NE                 (PERCENT)        SIO
   38    (1)         1X
   39    (2)       F2.1    #DELTA HE/NE PRECISION       (PERCENT)        SIO
   41    (2)       A2      #DELTA HE/NE FOOTNOTE                         SIO
   43    (2)         2X
   45    (4)       F4.2    #NEON                        (CC/KG * 10**-5) MCM
   49    (1)         1X
   50    (3)       F3.2    #NEON PRECISION              (CC/KG * 10**-5) MCM
   53    (2)       A2      #NEON FOOTNOTE                                MCM
   55    (2)         2X
   57    (4)       F4.2    #NEON                        (CC/KG * 10**-5) SIO
   62    (3)       F3.2    #NEON PRECISION              (CC/KG * 10**-5) SIO
   65    (2)       A2      #NEON FOOTNOTE                                SIO
   67    (12)       12X
   79    (1)       I1      NEXT RECORD TYPE
   80    (1)       I1      RECORD TYPE - ALWAYS 7


GERARD SHOREBASED DATA DATA RECORD 4

STARTING FIELD  ATTRIBUTES
 COLUMN  WIDTH             USAGE DESCRIPTION

   1     (1)         1X
   2     (3)       F3.1     SR-90                       (DPM/100KG)  WHOI
   5     (2)         2X
   7     (2)       F2.1     SR-90 PRECISION             (DPM/100KG)  WHOI
   9     (1)         1X
  10     (3)       F3.1     CS-137                      (DPM/100KG)  WHOI
  13     (1)         1X
  14     (2)       F2.1     CS-137 PRECISION            (DPM/100KG)  WHOI
  16     (5)       F5.3     239,240 PU                  (DPM/100KG)  WHOI
  21     (4)       F4.3     239,240 PU PRECISION        (DPM/100KG)  WHOI
  25     (1)         1X
  26     (3)       F3.1     PB-210 (SOL)                (DPM/100KG) SIO
  29     (2)         2X
  31     (2)       F2.1     PB-210 (SOL) PRECISION      (DPM/100KG) SIO
  33     (2)       A2       PB-210 (SOL) FOOTNOTE                   SIO
  35     (5)         5X
  40     (3)       F3.2     PB-210 (PAR)                (DPM/100KG) SIO
  43     (1)         1X
  44     (3)       F3.2     PB-210 (PAR) PRECISION      (DPM/100KG) SIO
  47     (2)       A2       PB-210 (PAR) FOOTNOTE                   SIO
  49     (5)         5X
  54     (3)       F3.1     PB-210 (TOT)                (DPM/100KG) SIO
  57     (2)         2X
  59     (2)       F2.1     PB-210 (TOT) PRECISION      (DPM/100KG) SIO
  61     (2)       A2       PB-210 (TOT) FOOTNOTE                   SIO
  63     (4)         4X
  67     (4)       F4.1     RA-226                      (DPM/100KG) LDGO
  71     (2)         2X
  73     (2)       F2.1     RA-226 PRECISION            (DPM/100KG) LDGO
  75     (4)         4X
  79     (1)       I1      NEXT RECORD TYPE
  80     (1)       I1      RECORD TYPE - ALWAYS 4

GERARD SHOREBASED DATA DATA RECORD 5

STARTING FIELD  ATTRIBUTES
 COLUMN  WIDTH             USAGE DESCRIPTION


    1    (4)       F4.1    #RA-226                      (DPM/100KG) SIO
    5    (2)         2X
    7    (2)       F2.1    #RA-226 PRECISION            (DPM/100KG) SIO
    9    (2)       A2       RA-226 FOOTNOTE                         SIO
   11    (4)         4X
   15    (4)       F4.1    #RA-226                      (DPM/100KG) USC
   19    (2)         2X
   21    (2)       F2.1    #RA-226 PRECISION            (DPM/100KG) USC
   23    (4)       F4.2    #RA-228                      (DPM/100KG) LDGO
   27    (4)       F4.2    #RA-228 PRECISION            (DPM/100KG) LDGO
   31    (1)         1X
   32    (3)       F3.2    #RA-228                      (DPM/100KG) NOOW
   35    (1)         1X
   36    (3)       F3.2    #RA-228 PRECISION            (DPM/100KG) NOOW
   39    (2)       A2      #RA-228 FOOTNOTE                         NOOW
   41    (6)         6X
   47    (3)       F3.2    #TH-228                      (DPM/100KG) LDGO
   50    (1)         1X
   51    (3)       F3.2    #TH-228 PRECISION            (DPM/100KG) LDGO
   54    (1)         1X
   55    (4)       F4.2    #TRITIUM                     (TU)        UM
   59    (2)         2X
   61    (3)       F3.2    #TRITIUM PRECISION           (SIGMA)     UM
   64    (1)         1X
   65    (5)       F5.1    #BIG DELTA C-14              (PER MIL)   UW
   70    (1)         1X
   71    (5)       F5.1    #BIG DELTA C-14              (PER MIL)   UM
   76    (3)         3X
   79    (1)       I1      NEXT RECORD TYPE
   80    (1)       I1      RECORD TYPE - ALWAYS 5

ATTACHMENT #2 FOOTNOTE EXPLANATION FOR NISKIN SHOREBASED DATA
 INSTITUTION   PROPERTY  FOOTNOTE
 SIO           HELIUM    E = ANALYZED, BUT BAD NUMBER - LEAKED DURING STORAGE
               HE-3      F = DUPLICATE SAMPLES ANALYZED
               NEON      L=LEAKED DURING ANALYSIS - SAMPLE LOST
               PB-210    G=UNRELIABLE OR QUESTIONABLE, PROBABLY DUE TO
                           MEASUREMENTS
               RA-226    H=SUSPECT

 MCM           NEON      W=MCM CONSIDERS WRONG DUE TO SAMPLER LEAKS
               HE-3
               HELIUM
FOOTNOTE EXPLANATION FOR GERARD SHOREBASED DATA
 INSTITUTION   PROPERTY   FOOTNOTE
 NOOW           RA-228    <=LESS THAN

 SIO            PB-210    S=UNRELIABLE OR QUESTIONABLE, DUE TO SAMPLE
                            COLLECTED SPECIFICALLY FROM SOME GERARD BOTTLES
                            WHICH COLLECTED UNRELIABLE SAMPLES REPEATEDLY AS
                            INDICATED BY THE DATA
                          G=UNRELIABLE OR QUESTIONABLE, PROBABLY DUE TO
                            MEASUREMENTS

"""

import sys
import os
import datetime
import collections
from logging import getLogger


log = getLogger(__name__)


from libcchdo import config
from libcchdo.units import convert
from libcchdo.fns import Decimal
from libcchdo.formats.formats import (
    get_filename_fnameexts, is_filename_recognized_fnameexts,
    is_file_recognized_fnameexts)


_fname_extensions = ['.shore']


def get_filename(basename):
    """Return the filename for this format given a base filename.

    This is a basic implementation using filename extensions.

    """
    return get_filename_fnameexts(basename, _fname_extensions)


def is_filename_recognized(fname):
    """Return whether the given filename is a match for this file format.

    This is a basic implementation using filename extensions.

    """
    return is_filename_recognized_fnameexts(fname, _fname_extensions)


def is_file_recognized(fileobj):
    """Return whether the file is recognized based on its contents.

    This is a basic non-implementation.

    """
    return is_file_recognized_fnameexts(fileobj, _fname_extensions)


_MAX_GRATICULE_PRECISION = 4


# conversion from attachment 1 to WOCE water sample flag
# TODO confirm this mapping
class Attachment1(dict):
    def __missing__(self, key):
        # default missing quality indicator to flag 2
        if not key:
            return 2


attachment1 = Attachment1({
    'A': 2,
    'B': 2,
    'C': 2,
    'D': 2,
    'E': 2,
    'G': 2,
    'H': 2,
    'K': 4,
    'P': 3,
    'U': 3,
    ' ': 2,
})


# conversion from attachment 2 to WOCE water sample flag
# TODO confirm this mapping
class Attachment2(dict):
    def __missing__(self, key):
        # default empty footnotes to flag 2
        if not key[2] or not key[2].strip():
            return 2


attachment2 = Attachment2({
    # Niskin
    # Analyzed but bad number, leaked during storage
    ('SIO', 'HELIUM', 'E', ): 4,
    # Duplicate samples analyzed
    ('SIO', 'HE-3', 'F', ): 6,
    # leaked during analysis - sample lost
    ('SIO', 'NEON', 'L', ): 1,
    # unreliable or questionable, probably due to measurements
    ('SIO', 'PB-210', 'G', ): 3,
    # suspect
    ('SIO', 'RA-226', 'H', ): 3,
    # MCM considers wrong due to sampler leaks
    ('MCM', 'NEON', 'W', ): 4,
    ('MCM', 'HE-3', 'W', ): 4,
    ('MCM', 'HELIUM', 'W', ): 4,

    # Gerard
    # less than TODO What does this less than mean?
    ('NOOW', 'RA-228', '<', ): 2, 
    # unreliable or questionable, probably due to measurements
    ('SIO', 'PB-210', 'G', ): 3,
})


def int_or_none(i):
    try:
        return int(i)
    except ValueError:
        return None


class LineReader(object):
    def __init__(self, fields=[]):
        self._fields = fields

    def add_field(key, start, length=1, type=str):
        self._fields.append((key, start, length, type, ))

    def read(self, line):
        d = {}

        for key, start, length, t in self._fields:
            chars = line[start:start + length]
            if t is str:
                value = chars
            elif t is int:
                value = int_or_none(chars)
            else:
                if type(t) is tuple:
                    chars = chars.strip()
                    if not chars:
                        value = None
                    else:
                        #print key, chars, t
                        value = Decimal(chars) * Decimal(10).power(-t[1])
                else:
                    value = chars
            d[key] = value

        return d


def set(sample, param, raw, raw_precis=None, raw_qc=None, raw_footnote=None):
    sample[param] = raw

    if raw_precis:
        pass
        #print 'precision:', param, raw, raw_precis

    if raw_qc:
        sample[param + '_QC'] = raw_qc

    if raw_footnote:
        sample[param + '_FOOTNOTE'] = raw_footnote


def _read_master_information_1(line):
    raw = LineReader([
        ('continuation_indicator', 0, 1, int),
        ('originator_nationality', 1, 3, str),
        ('rosette_niskin_id', 13, 2, int),
        ('canadian_10_deg_square', 17, 4, int),
        ('one-degree_square_canadian', 21, 2, int),
        ('two-degree_square_canadian', 23, 3, int),
        ('five-degree_square_canadian', 25, 1, int),
        ('hemisphere_of_latitude', 26, 1, str),
        ('degrees_latitude', 27, 2, int),
        ('minutes_latitude', 29, 2, int),
        ('minutes_latitude_tenths', 31, 1, int),
        ('hemisphere_of_longitude', 32, 1, str),
        ('degrees_longitude', 33, 3, int),
        ('minutes_longitude', 36, 2, int),
        ('minutes_longitude_tenths', 38, 1, int),
        ('year_gmt', 40, 2, int),
        ('month_of_year_gmt', 42, 2, int),
        ('day_of_month_gmt', 44, 2, int),
        ('alphanumeric_ship_code', 49, 6, str),
        ('depth_to_bottom', 55, 5, int),
        ('first_observed_depth', 70, 4, int),
        ('maximum_sampling_depth', 74, 4, int),
        ('always_2_next_record_indicator', 78, 1, int),
        ('always_1_record_indicator', 79, 1, int),
    ]).read(line)

    assert raw['always_2_next_record_indicator'] == 2, \
        "Master Record 1 is corrupt."

    station = {
        '_cur_sample': None,
        '_samples': [],
    }

    if not raw['hemisphere_of_latitude'] in ('N', 'S'):
        raise ValueError(
            ("Master Record 1 is corrupt. Latitude hemisphere must be "
             "N or S."))

    latitude = str(
        (1 if raw['hemisphere_of_latitude'] == 'N' else -1) * \
        (raw['degrees_latitude'] + 
         raw['minutes_latitude'] / 60.0 + 
         raw['minutes_latitude_tenths'] / 600.0))
    latitude = latitude[:latitude.find('.') + \
                         _MAX_GRATICULE_PRECISION + 1]
    station['LATITUDE'] = Decimal(latitude)

    if not raw['hemisphere_of_longitude'] in ('E', 'W'):
        raise ValueError(
            ("Master Record 1 is corrupt. Longitude hemisphere must be "
             "E or W."))

    longitude = str(
        (1 if raw['hemisphere_of_longitude'] == 'E' else -1) * \
        (raw['degrees_longitude'] + 
         raw['minutes_longitude'] / 60.0 + 
         raw['minutes_longitude_tenths'] / 600.0))
    longitude = longitude[:longitude.find('.') + \
                           _MAX_GRATICULE_PRECISION + 1]
    station['LONGITUDE'] = Decimal(longitude)

    station['_DATETIME'] = datetime.datetime(
        *(1900 + raw['year_gmt'], raw['month_of_year_gmt'],
          raw['day_of_month_gmt']))

    station['BOTTOM'] = raw['depth_to_bottom']

    return station


def _read_master_information_2(line, station):
    raw = LineReader([
        ('originators_cruise', 14, 3, str),
        ('originators_station', 17, 9, str),
        ('num_observed_depth', 61, 3, int),
        ('num_detail_depths', 66, 3, int),
        ('always_3_next_record_indicator', 78, 1, int),
        ('always_2_record_indicator', 79, 1, int),
    ]).read(line)
    assert raw['always_3_next_record_indicator'] == 3, \
        "Master Record 2 is corrupt."
    assert raw['always_2_record_indicator'] == 2, \
        "Not master record 2. Algorithm is wrong."
    station['STNNBR'] = raw['originators_station'].strip()


def _read_observed_depth_information(line, station):
    if not station:
        raise ValueError(
            "Malformed GEOSECS SD2 file: Data record before master record")

    raw = LineReader([
        ('depth', 0, 5, int),
        ('pressure_quality_indicator', 5, 1, str),
        ('total_co2_quality_indicator', 6, 1, str),
        ('temperature', 7, 5, (5, 3)),
        ('temperature_precision', 12, 1, int),
        ('temperature_quality_indicator', 13, 1, str),
        ('salinity', 14, 5, (5, 3)),
        ('salinity_precision', 19, 1, int),
        ('salinity_quality_indicator', 20, 1, str),
        ('oxygen_quality_indicator', 21, 1, str),
        ('phosphate_quality_indicator', 22, 1, str),
        ('nitrite_quality_indicator', 23, 1, str),
        ('nitrate_quality_indicator', 24, 1, str),
        ('alkalinity_quality_indicator', 25, 1, str),
        ('pressure', 26, 5, int),
        ('silicate_quality_indicator', 31, 1, str),
        ('oxygen', 32, 4, int),
        ('alkalinity', 36, 4, int),
        ('total_co2', 40, 4, int),
        ('cast_number', 44, 2, int),
        ('bottle_number', 46, 2, int),
        ('phosphate', 48, 4, (4, 2)),
        ('phosphate_precision', 52, 1, int),
        ('gc_co2', 53, 4, (4, 0)),
        ('gc_co2_quality_indicator', 57, 1, int),
        ('silicates', 58, 4, (4, 1)),
        ('silicates_precision', 62, 1, int),
        ('nitrites', 63, 3, (3, 2)),
        ('nitrites_precision', 63, 1, int),
        ('nitrates', 67, 3, (3, 1)),
        ('nitrates_precision', 70, 1, int),
        ('ph', 71, 3, (3, 2)),
        ('ph_precision', 74, 1, int),
        ('next_record_type', 78, 1, int),
        ('record_type', 79, 1, int),
    ]).read(line)

    if station['_cur_sample']:
        station['_samples'].append(station['_cur_sample'])
    station['_cur_sample'] = sample = {}

    sample['DEPTH'] = raw['depth']
    sample['DEPTH_QC'] = attachment1[raw['pressure_quality_indicator']]

    sample['CASTNO'] = raw['cast_number']
    sample['BTLNBR'] = raw['bottle_number']

    if not raw['temperature_quality_indicator']:
        raise ValueError('no temp quality')
    if raw['temperature_precision'] != 9:
        set(sample, 'TEMPERATURE', raw['temperature'],
            raw['temperature_precision'],
            attachment1[raw['temperature_quality_indicator']])
    if raw['salinity_precision'] != 9:
        set(sample, 'SALINITY', raw['salinity'],
            raw['salinity_precision'],
            attachment1[raw['salinity_quality_indicator']])
    set(sample, 'PRESSURE', raw['pressure'],
        None, attachment1[raw['pressure_quality_indicator']])
    set(sample, 'OXYGEN', raw['oxygen'],
        None, attachment1[raw['oxygen_quality_indicator']])
    set(sample, 'ALKALINITY', raw['alkalinity'],
        None, attachment1[raw['alkalinity_quality_indicator']])
    set(sample, 'TOTAL_CO2', raw['total_co2'],
        None, attachment1[raw['total_co2_quality_indicator']])
    set(sample, 'PHOSPHATE', raw['phosphate'],
        None, attachment1[raw['phosphate_quality_indicator']])
    set(sample, 'GC_CO2', raw['gc_co2'],
        None, attachment1[raw['gc_co2_quality_indicator']])

    if raw['silicates_precision'] != 9:
        set(sample, 'SILICATES', raw['silicates'],
            raw['silicates_precision'], None)
    if raw['nitrites_precision'] != 9:
        set(sample, 'NITRITES', raw['nitrites'],
            raw['nitrites_precision'], None)
    if raw['nitrates_precision'] != 9:
        set(sample, 'NITRATES', raw['nitrates'],
            raw['nitrates_precision'], None)
    if raw['ph_precision'] != 9:
        set(sample, 'PH', raw['ph'], raw['ph_precision'], None)
    return raw


def _read_niskin_shorebased_4(line, station):
    raw = LineReader([
        ('particulate_conc_ldgo', 0, 3, (3, 0)),
        ('particulate_conc_precision_ldgo', 4, 2, (2, 1)),
        ('particulate_conc_whoi', 6, 3, (3, 0)),
        ('barium_lsu', 9, 4, (4, 1)),
        ('barium_mit', 13, 4, (4, 1)),
        ('toc_osu', 18, 5, int),
        ('toc_precision_osu', 23, 4, int), 
        ('delta_c13_uh', 28, 4, (4, 2)),
        ('delta_c13_sio', 33, 4, (4, 2)),
        ('delta_o18_dis_o2_uh', 39, 3, (3, 1)),
        ('delta_o18_sio', 42, 4, (4, 2)),
        ('deuterium_sio', 46, 4, (4, 1)),
        ('ra226_ldgo', 50, 3, (4, 1)),
        ('ra226_precision_ldgo', 56, 2, (2, 1)),
        ('ra226_sio', 58, 4, (4, 1)),
        ('ra226_precision_sio', 64, 2, (2, 1)),
        ('ra226_footnote_sio', 66, 2, (4, 1)),
        ('ra226_usc', 68, 4, (4, 1)),
        ('ra226_precision_usc', 74, 2, (2, 1)),
        ('next_record_type', 78, 1, int),
        ('record_type_always_4', 79, 1, int),
    ]).read(line)

    sample = station['_cur_sample']
    set(sample, 'SPM_LDGO', raw['particulate_conc_ldgo'],
        raw['particulate_conc_precision_ldgo'])
    set(sample, 'SPM_WHOI', raw['particulate_conc_whoi'], None)
    set(sample, 'BARIUM_LSU', raw['barium_lsu'], None)
    set(sample, 'BARIUM_MIT', raw['barium_mit'], None)
    set(sample, 'TOC_OSU', raw['toc_osu'], raw['toc_precision_osu'])
    set(sample, 'DELC13_UH', raw['delta_c13_uh'], None)
    set(sample, 'DELC13_SIO', raw['delta_c13_sio'], None)
    set(sample, 'DELO18_SIO', raw['delta_o18_sio'], None)
    set(sample, 'RA-226_LDGO', raw['ra226_ldgo'], raw['ra226_precision_ldgo'])
    set(sample, 'RA-226_SIO', raw['ra226_sio'], raw['ra226_precision_sio'],
        attachment2[('SIO', 'RA-226', raw['ra226_footnote_sio'])])
    set(sample, 'RA-226_USC', raw['ra226_usc'], raw['ra226_precision_usc'])


def _read_niskin_shorebased_5(line, station):
    raw = LineReader([
        ('pb210_sol_sio', 1, 3, (3, 1)),
        ('pb210_sol_precision_sio', 6, 2, (2, 1)),
        ('pb210_sol_footnote_sio', 8, 2, str),
        ('pb210_sol_whoi', 11, 3, (3, 1)),
        ('pb210_sol_precision_whoi', 16, 2, (2, 1)),
        ('pb210_sol_yale', 19, 3, (3, 1)),
        ('pb210_precision_yale', 24, 2, (2, 1)),
        ('pb210_tot_sio', 27, 3, (3, 1)),
        ('pb210_tot_precision_sio', 32, 2, (2, 1)),
        ('pb210_tot_footnote_sio', 34, 2, str),
        ('pb210_tot_yale', 37, 3, (3, 1)),
        ('pb210_tot_precision_yale', 42, 2, (2, 1)),
        ('pb210_par_sio', 45, 3, (3, 2)),
        ('pb210_par_precision_sio', 49, 3, (3, 2)),
        ('pb210_par_footnote_sio', 52, 2, str),
        ('pb210_par_yale', 55, 3, (3, 2)),
        ('pb210_par_precision_yale', 59, 3, (3, 2)),
        ('po210_sol_sio', 63, 3, (3, 1)),
        ('po210_sol_precision_sio', 68, 2, (2, 1)),
        ('po210_sol_footnote_sio', 70, 2, str),
        ('next_record_type', 78, 1, int),
        ('record_type_always_5', 79, 1, int),
    ]).read(line)

    sample = station['_cur_sample']


def _read_niskin_shorebased_6(line, station):
    raw = LineReader([
        ('po210_sol_whoi',  1, 3, (3, 1)),
        ('po210_sol_precision_whoi',  6, 2, (2, 1)),
        ('po210_sol_yale', 13, 3, (3, 1)),
        ('po210_sol_precision_yale', 20, 2, (2, 1)),
        ('po210_par_sio', 23, 3, (3, 2)),
        ('po210_par_precision_sio', 27, 3, (3, 2)),
        ('po210_par_footnote_sio', 30, 2, str),
        ('tritium_um', 33, 4, (4, 2)),
        ('tritium_precision_um', 39, 3, (3, 2)),
        ('helium_mcm', 44, 3, (3, 2)),
        ('helium_precision_mcm', 48, 3, (3, 2)),
        ('helium_footnote_mcm', 51, 2, str),
        ('helium_sio', 55, 3, (3, 2)),
        ('helium_precision_sio', 59, 3, (3, 2)),
        ('helium_footnote_sio', 62, 2, str),
        ('helium_whoi', 66, 3, (3, 2)),
        ('helium_precision_whoi', 70, 2, (2, 1)),
        ('next_record_type', 78, 1, int),
        ('record_type_always_6', 79, 1, int),
    ]).read(line)

    sample = station['_cur_sample']
    set(sample, 'TRITUM_UM', raw['tritium_um'],
        raw['tritium_precision_um'], None)
    set(sample, 'HELIUM_MCM', raw['helium_mcm'],
        raw['helium_precision_mcm'], None,
        attachment2[('MCM', 'HELIUM', raw['helium_footnote_mcm'])])
    set(sample, 'HELIUM_SIO', raw['helium_sio'],
        raw['helium_precision_sio'], None,
        attachment2[('SIO', 'HELIUM', raw['helium_footnote_sio'])])
    set(sample, 'HELIUM_WHOI', raw['helium_whoi'],
        raw['helium_precision_whoi'])


def _read_niskin_shorebased_7(line, station):
    raw = LineReader([
        ('delta_3he_mcm',  1, 3, (3, 1)),
        ('delta_3he_precision_mcm',  5, 2, (2, 1)),
        ('delta_3he_footnote_mcm',  7, 2, str),
        ('delta_3he_sio', 10, 3, (3, 1)),
        ('delta_3he_precision_sio', 14, 2, (2, 1)),
        ('delta_3he_footnote_sio', 16, 2, str),
        ('delta_3he_whoi', 19, 3, (3, 1)),
        ('delta_3he_precision_whoi', 22, 3, (3, 2)),
        ('delta_hene_mcm', 26, 3, (3, 1)),
        ('delta_hene_precision_mcm', 31, 2, (2, 1)),
        ('delta_hene_sio', 34, 3, (3, 1)),
        ('delta_hene_precision_sio', 38, 2, (2, 1)),
        ('delta_hene_footnote_sio', 40, 2, str),
        ('neon_mcm', 44, 4, (4, 2)),
        ('neon_precision_mcm', 49, 3, (3, 2)),
        ('neon_footnote_mcm', 52, 2, str),
        ('neon_sio', 56, 4, (4, 2)),
        ('neon_precision_sio', 61, 3, (3, 2)),
        ('neon_footnote_sio', 64, 2, str),
        ('next_record_type', 78, 1, int),
        ('record_type_always_7', 79, 1, int),
    ]).read(line)

    sample = station['_cur_sample']
    set(sample, 'DELHE3_MCM', raw['delta_3he_mcm'],
        raw['delta_3he_precision_mcm'], None,
        attachment2[('MCM', 'HE-3', raw['delta_3he_footnote_mcm'])])
    set(sample, 'DELHE3_SIO', raw['delta_3he_sio'],
        raw['delta_3he_precision_sio'], None,
        attachment2[('SIO', 'HE-3', raw['delta_3he_footnote_sio'])])
    set(sample, 'NEON_MCM', raw['neon_mcm'],
        raw['neon_precision_mcm'], None,
        attachment2[('MCM', 'NEON', raw['neon_footnote_mcm'])])
    set(sample, 'NEON_SIO', raw['neon_sio'],
        raw['neon_precision_sio'], None,
        attachment2[('SIO', 'NEON', raw['neon_footnote_sio'])])


def _read_gerard_shorebased_4(line, station):
    raw = LineReader([
        ('sr90_whoi', 1, 3, (3, 1)),
        ('sr90_precision_whoi', 6, 2, (2, 1)),
        ('cs137_whoi',  9, 3, (3, 1)),
        ('cs137_precision_whoi', 13, 2, (2, 1)),
        ('239_240_pu_whoi', 15, 5, (5, 3)),
        ('239_240_pu_precision_whoi', 20, 4, (4, 3)),
        ('pb210_sol_sio', 25, 3, (3, 1)),
        ('pb210_sol_precision_sio', 30, 2, (2, 1)),
        ('pb210_sol_footnote_sio', 32, 2, str),
        ('pb210_par_sio', 39, 3, (3, 2)),
        ('pb210_par_precision_sio', 43, 3, (3, 2)),
        ('pb210_par_footnote_sio', 46, 2, str),
        ('pb210_tot_sio', 53, 3, (3, 1)),
        ('pb210_tot_precision_sio', 58, 2, (2, 1)),
        ('pb210_tot_footnote_sio', 60, 2, str),
        ('ra226_ldgo', 66, 4, (4, 1)),
        ('ra226_precision_ldgo', 72, 2, (2, 1)),
        ('next_record_type', 78, 1, int),
        ('record_type_always_4', 79, 1, int),
    ]).read(line)

    sample = station['_cur_sample']
    set(sample, 'SR-90_WHOI', raw['sr90_whoi'], raw['sr90_precision_whoi'])
    set(sample, 'CS-137_WHOI', raw['cs137_whoi'], raw['cs137_precision_whoi'])
    set(sample, 'RA-226_LDGO', raw['ra226_ldgo'], raw['ra226_precision_ldgo'])


def _read_gerard_shorebased_5(line, station):
    raw = LineReader([
        ('ra226_sio', 0, 4, (4, 1)),
        ('ra226_precision_sio', 6, 2, (2, 1)),
        ('ra226_footnote_sio', 8, 2, str),
        ('ra226_usc', 14, 4, (4, 1)),
        ('ra226_precision_usc', 20, 2, (2, 1)),
        ('ra228_ldgo', 22, 4, (4, 2)),
        ('ra228_precision_ldgo', 26, 4, (4, 2)),
        ('ra228_noow', 31, 3, (3, 2)),
        ('ra228_precision_noow', 35, 3, (3, 2)),
        ('ra228_footnote_noow', 38, 2, str),
        ('th228_ldgo', 46, 3, (3, 2)),
        ('th228_precision_ldgo', 50, 3, (3, 2)),
        ('tritium_um', 54, 4, (4, 2)),
        ('tritium_precision_um', 60, 3, (3, 2)),
        ('big_delta_c14_uw', 64, 5, (5, 1)),
        ('big_delta_c14_um', 70, 5, (5, 1)),
        ('next_record_type', 78, 1, int),
        ('record_type_always_5', 79, 1, int),
    ]).read(line)

    sample = station['_cur_sample']
    set(sample, 'RA-226_SIO', raw['ra226_sio'],
        raw['ra226_precision_sio'], None,
        attachment2[('SIO', 'RA-226', raw['ra226_footnote_sio'])])
    set(sample, 'RA-226_USC', raw['ra226_usc'], raw['ra226_precision_usc'])
    set(sample, 'RA-228_LDGO', raw['ra228_ldgo'], raw['ra228_precision_ldgo'])
    set(sample, 'RA-228_NOOW', raw['ra228_noow'],
        raw['ra228_precision_noow'], None,
        attachment2[('NOOW', 'RA-228', raw['ra228_footnote_noow'])])
    set(sample, 'TRITIUM_UM', raw['tritium_um'], raw['tritium_precision_um'])
    set(sample, 'DELC14_UW', raw['big_delta_c14_uw'])
    set(sample, 'DELC14_UM', raw['big_delta_c14_um'])


def print_line(line, ruler=True):
    log.debug('line: %s' % line)
    if ruler:
        log.debug('ruler:%s' % '_1_3_5_7_9' * 8)


def read(self, handle):
    """How to read a GEOSECS file."""

    filename = os.path.basename(handle.name).replace('.shore', '')
    if filename.endswith('nis'):
        gerard_not_niskin = False
    elif filename.endswith('ger'):
        gerard_not_niskin = True
    else:
        raise ValueError(
            'Unknown GEOSECS file type. Could not differentiate Gerard or Niskin')

    parameters = [
        ['EXPOCODE', None],
        ['STNNBR', None],
        ['CASTNO', None],
        ['BTLNBR', None],
        ['SAMPNO', None],
        ['_DATETIME', None],
        ['LATITUDE', None],
        ['LONGITUDE', None], 
        ['DEPTH', 'METERS'],

        # Observed depth information
        ['CTDPRS', 'DBAR'],
        ['CTDTMP', 'DEG C'],
        ['SALNTY', 'PSU'],
        ['CTDPRS', 'DBAR'],
        ['OXYGEN', 'UMOL/KG'],
        ['ALKALI', 'UEQUIV/KG'],
        ['TCARBN', 'UMOL/KG'],
        ['PHSPHT', 'UMOL/KG'],
        ['SILCAT', 'UMOL/KG'],
        ['NITRIT', 'UMOL/KG'],
        ['NITRAT', 'UMOL/KG'],
        ['PH', ''],
    ]

    if gerard_not_niskin:
        bottle_specific_parameters = [
            # Gerard Record 4
            ['SR-90', 'DPM/100KG'],
            ['CS-137', 'DPM/100KG'],
            #['239,240PU', 'DPM/100KG'],
            #['PB-210_SOL', 'DPM/100KG'],
            #['PB-210_PAR', 'DPM/100KG'],
            #['PB-210_TOT', 'DPM/100KG'],
            ['RA-226', 'DPM/100KG'],
            # Gerard Record 5
            ['RA-228', 'DPM/100KG'],
            #['TH-228', 'DPM/100KG'],
            ['TRITUM', 'TU'],
            ['DELC14', 'PER MIL'],
        ]
    else:
        bottle_specific_parameters = [
            # Niskin Record 4
            ['SPM', 'UG/KG'], # Suspended Particulate Matter Dave Muus 2012-05-15
            ['BARIUM', 'NM/KG'],
            ['TOC', 'UM/KG'],
            ['DELC13', 'PER MIL'],
            ['DELO18', 'PER MIL'],
            #['DEUTERIUM', 'PERCNT'],
            ['RA-226', 'DPM/100KG'],
            # Niskin Record 5
            #['PB210_SOL', 'DPM/100KG'],
            #['PB210_TOT', 'DPM/100KG'],
            #['PB210_PAR', 'DPM/100KG'],
            #['PO210_SOL', 'DPM/100KG'],
            # Niskin Record 6
            #['PB210_PAR', 'DPM/100KG'],
            ['TRITUM', 'TU'],
            ['HELIUM', 'CC/KG * 10**-5'], # CC/KG * 10 ** -5 / 2.2415 = NMOL/KG for HElium per Bill Jenkins WHOI May 3, 2006
            # Niskin Record 7
            ['DELHE3', 'PERCNT'],
            #['DELTA HE/NE', 'PERCNT'],
            ['NEON', 'CC/KG * 10**-5'], # same for helium, Bill Jenkins May 15, 2012
        ]
    parameters.extend(bottle_specific_parameters)

    params = []
    units = []
    for p, u in parameters:
        params.append(p)
        units.append(u)

    self.create_columns(params, units)

    stations = []
    cur_station = None

    i = 0
    while handle:
        line = handle.readline()
        if not line:
            break

        try:
            record = line[79]
        except IndexError:
            log.error('Record on line %d is too short. Skipping line.' % i)
            continue

        # custom corrections
        if i == 1447 and filename == 'atl_ger':
            # shift 20 10 to the left for atl_ger.shore line 1447
            line = line[:18] + line[19:25] + line[18] + line[25:]

        try:
            if record == '1':
                if cur_station:
                    if cur_station['_cur_sample']:
                        cur_station['_samples'].append(
                            cur_station['_cur_sample'])
                        del cur_station['_cur_sample']
                    stations.append(cur_station)
                cur_station = _read_master_information_1(line)
            elif record == '2':
                _read_master_information_2(line, cur_station)
            elif record == '3':
                raw_line = _read_observed_depth_information(line, cur_station)
            elif record == '4':
                if gerard_not_niskin:
                    _read_gerard_shorebased_4(line, cur_station)
                else:
                    _read_niskin_shorebased_4(line, cur_station)
            elif record == '5':
                if gerard_not_niskin:
                    _read_gerard_shorebased_5(line, cur_station)
                else:
                    _read_niskin_shorebased_5(line, cur_station)
            elif record == '6':
                if gerard_not_niskin:
                    raise ValueError('Gerard data does not have record 6')
                else:
                    _read_niskin_shorebased_6(line, cur_station)
            elif record == '7':
                if gerard_not_niskin:
                    raise ValueError('Gerard data does not have record 7')
                else:
                    _read_niskin_shorebased_7(line, cur_station)
            else:
                # Unknown record
                pass
        except Exception, e:
            log.error('Failed to read line %d: %s' % (i, e))
            import traceback
            traceback.print_exc()
            continue

        i += 1

    expocodes = {
        'atl': '316N19720718',
        'pac': '318M19730822',
        'ind': '318M19771204', 
    }

    try:
        expocode = expocodes[filename[:3]]
    except KeyError:
        log.warning('unknown GEOSECS expocode')
        expocode = 'UNKNOWN'

    def combine_measures(values, qcs):
        """ Combine measurements from multiple institutions of the same parameter

        """
        return (values[0], qcs[0])

    j = 0
    for station in stations:
        station['EXPOCODE'] = expocode
        station['_DATA_TYPE'] = 'BOTTLE'

        samples = station['_samples']
        del station['_samples']

        for sample in samples:
            merged_row = {
                'EXPOCODE': station.get('EXPOCODE', None),
                'STNNBR': station.get('STNNBR', None),
                'LATITUDE': station.get('LATITUDE', None),
                'LONGITUDE': station.get('LONGITUDE', None),
                '_DATETIME': station.get('_DATETIME', None),
                'BTLNBR': sample.get('BTLNBR', None),
                'SAMPNO': sample.get('BTLNBR', None),
                'CASTNO': sample.get('CASTNO', None),
                'CTDTMP': sample.get('TEMPERATURE', None),
                'CTDTMP_FLAG_W': sample.get('TEMPERATURE_QC', None),
                'SALNTY': sample.get('SALINITY', None),
                'SALNTY_FLAG_W': sample.get('SALINITY_QC', None),
                'OXYGEN': sample.get('OXYGEN', None),
                'OXYGEN_FLAG_W': sample.get('OXYGEN_QC', None),
                'PHSPHT': sample.get('PHSPHT', None),
                'SILCAT': sample.get('SILCAT', None),
                'NITRIT': sample.get('NITRIT', None),
                'NITRAT': sample.get('NITRAT', None),
                'PH': sample.get('PH', None),
            }

            self['EXPOCODE'].set(j, merged_row['EXPOCODE'])
            self['STNNBR'].set(j, merged_row['STNNBR'])
            self['CASTNO'].set(j, merged_row['CASTNO'])
            self['BTLNBR'].set(j, merged_row['BTLNBR'])
            self['SAMPNO'].set(j, merged_row['SAMPNO'])
            self['LATITUDE'].set(j, merged_row['LATITUDE'])
            self['LONGITUDE'].set(j, merged_row['LONGITUDE'])
            self['_DATETIME'].set(j, merged_row['_DATETIME'])
            self['DEPTH'].set(j, station.get('BOTTOM', None))
            self['CTDPRS'].set(j, sample.get('PRESSURE', None), sample.get('PRESSURE_QC'))
            self['CTDTMP'].set(j, merged_row['CTDTMP'], merged_row['CTDTMP_FLAG_W'])
            self['SALNTY'].set(j, merged_row['SALNTY'], merged_row['SALNTY_FLAG_W'])
            self['OXYGEN'].set(j, merged_row['OXYGEN'], merged_row['OXYGEN_FLAG_W'])
            self['PHSPHT'].set(j, merged_row['PHSPHT'])
            self['SILCAT'].set(j, merged_row['SILCAT'])
            self['NITRIT'].set(j, merged_row['NITRIT'])
            self['NITRAT'].set(j, merged_row['NITRAT'])
            self['PH'].set(j, merged_row['PH'])

            if gerard_not_niskin:
                self['SR-90'].set(j, sample.get('SR-90_WHOI', None))
                self['CS-137'].set(j, sample.get('CS-137_WHOI', None))
                self['RA-226'].set(j, *combine_measures(
                        [
                            sample.get('RA-226_LDGO', None),
                            sample.get('RA-226_SIO', None),
                            sample.get('RA-226_USC', None),
                        ],
                        [
                            None,
                            sample.get('RA-226_SIO_QC', None),
                            None,
                        ]
                    )
                )
                self['RA-228'].set(j, *combine_measures(
                        [
                            sample.get('RA-228_LDGO', None),
                            sample.get('RA-228_NOOW', None),
                        ],
                        [
                            None,
                            sample.get('RA-228_NOOW_QC', None),
                        ]
                    )
                )
                self['TRITUM'].set(j, sample.get('TRITIUM_UM', None))
                self['DELC14'].set(j, sample.get('DELC14_UW_UM', None))
            else:
                self['SPM'].set(j, sample.get('SPM_LDGO_WHOI', None))
                self['BARIUM'].set(j, *combine_measures(
                        [
                            sample.get('BARIUM_LSU', None),
                            sample.get('BARIUM_MIT', None),
                        ],
                        [None, None],
                    )
                )
                self['TOC'].set(j, sample.get('TOC_OSU', None))
                self['DELC13'].set(j, *combine_measures(
                        [
                            sample.get('DELC13_UH', None),
                            sample.get('DELC13_SIO', None),
                        ],
                        [None, None],
                    )
                )
                self['DELO18'].set(j, sample.get('DELO18_SIO', None))
                self['RA-226'].set(j, *combine_measures(
                        [
                            sample.get('RA-226_LDGO', None),
                            sample.get('RA-226_SIO', None),
                        ],
                        [None, sample.get('RA-226_SIO_QC', None)],
                    )
                )
                self['TRITUM'].set(j, sample.get('TRITUM_UM', None))
                self['HELIUM'].set(j, *combine_measures(
                        [
                            sample.get('HELIUM_MCM', None),
                            sample.get('HELIUM_SIO', None),
                            sample.get('HELIUM_WHOI', None),
                        ],
                        [
                            sample.get('HELIUM_MCM_QC', None),
                            sample.get('HELIUM_SIO_QC', None),
                            None,
                        ],
                    )
                )
                self['DELHE3'].set(j, *combine_measures(
                        [
                            sample.get('DELHE3_MCM', None),
                            sample.get('DELHE3_SIO', None),
                        ],
                        [
                            sample.get('DELHE3_MCM_QC', None),
                            sample.get('DELHE3_SIO_QC', None),
                        ],
                    )
                )
                self['NEON'].set(j, *combine_measures(
                        [
                            sample.get('NEON_MCM', None),
                            sample.get('NEON_SIO', None),
                        ],
                        [
                            sample.get('NEON_MCM_QC', None),
                            sample.get('NEON_SIO_QC', None),
                        ],
                    )
                )


            j += 1

    # clean up empty columns
    for key, column in self.columns.items():
        if (    len(filter(None, column.values)) == 0 and 
                len(filter(None, column.flags_woce)) == 0 and 
                len(filter(None, column.flags_igoss)) == 0):
            log.debug('Deleting empty column %s' % key)
            del self.columns[key]

    self.globals['stamp'] = config.stamp()
    self.globals['header'] = ''

    self.unit_converters[('DPM/100KG', u'DM/.1MG')] = convert.equivalent
    self.unit_converters[('PSU', u'PSS-78')] = convert.equivalent
    self.unit_converters[('PER MIL', u'/MILLE')] = convert.equivalent
    self.unit_converters[('CC/KG * 10**-5', u'NMOL/KG')] = \
        convert.cc_per_kilogram_e_neg_5_to_nanomole_per_kilogram

    self.check_and_replace_parameters()
