""" Test case for libcchdo.formats.summary """

import StringIO
from unittest import TestCase

import libcchdo
import libcchdo.model.datafile
import libcchdo.formats.summary.woce as sumwoce
import libcchdo.formats.summary.hot as sumhot

def fp_eq(a, b, epsilon=0.00001):
  return abs(a-b) < epsilon

class TestSummaryFile(TestCase):
  def setUp(self):
    self.sample_woce = """\
I8S     R/V Revelle     15 Feb 2007 - 13 Mar 2007  20070502CCHDOSCD
SHIP/CRS     WOCE                 CAST         UTC EVENT         POSITION             UNC HT ABOVE WIRE   MAX  NO. OF                                                                
EXPOCODE     SECT   STNNBR CASTNO TYPE DATE   TIME  CODE LATITUDE   LONGITUDE   NAV DEPTH   BOTTOM  OUT PRESS BOTTLES PARAMETERS                              COMMENTS                       
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
33RR20070204 I8S         1      1  ROS 021507 1424    BE 65 48.65 S  84 33.00 E GPS   450                             test
33RR20070204 I8S         1      1  ROS 021507 1442    BO 65 48.65 S  84 33.01 E GPS   450        6  435   439      16 1-8,23-24,27,43,104-112                                                
33RR20070204 I8S         2      1  ROS 021507 1705    BE 65 46.09 S  84 32.09 E GPS  1257            11                
"""
    self.sample_hot = """\
                                       Hawaii Ocean Time-Series   Cruise HOT-134
   Ship  Section Sta Cast Cast  Date     Time            Position               Depth   Pres. Number  Parameters Comments
  Cruise  WHP ID  #    #  Type         UTC Code   Lat.       Lon.       Code   Max  Hgt  Max Bottles 
-------------------------------------------------------------------------------------------------------------------------
33KI134/1  PRS2    1   1   ROS 011402 2334  BE  21 20.66 N  158 16.26 W  GPS  1503  492 1020     16          1,2    Dual T, C sensors
33KI134/1  PRS2    2   1   ROS 011502 1259  BE  22 45.00 N  158 00.06 W  GPS  4720   -3 4806     24  1,2,3,4,5,6    Dual T, C sensors
33KI134/1  PRS2    2  15   ROS 011702 0856  BE  22 44.97 N  158 00.01 W  GPS  4720   -3 4806     12          1,2    Dual T, C sensors
"""

  def test_read_summary_woce(self):
    self.file = libcchdo.model.datafile.SummaryFile()
    self.buff = StringIO.StringIO(self.sample_woce)
    sumwoce.read(self.file, self.buff)

    cs = self.file.columns
    self.assertEqual(['33RR20070204'] * 3, cs['EXPOCODE'].values)
    self.assertEqual(['I8S'] * 3, cs['SECT_ID'].values)
    self.assertEqual([1, 1, 2], cs['STNNBR'].values)
    self.assertEqual([1, 1, 1], cs['CASTNO'].values)
    self.assertEqual(['ROS'] * 3, cs['_CAST_TYPE'].values)
    self.assertEqual(['20070215', '20070215', '20070215'], cs['DATE'].values)
    self.assertEqual([1424, 1442, 1705], cs['TIME'].values)
    self.assertEqual(['BE', 'BO', 'BE'], cs['_CODE'].values)

    self.assertTrue(fp_eq(-65.81083, cs['LATITUDE'].values[0]))
    self.assertTrue(fp_eq(-65.81083, cs['LATITUDE'].values[1]))
    self.assertTrue(fp_eq(-65.76816, cs['LATITUDE'].values[2]))

    self.assertTrue(fp_eq(84.549999, cs['LONGITUDE'].values[0]))
    self.assertTrue(fp_eq(84.55016, cs['LONGITUDE'].values[1]))
    self.assertTrue(fp_eq(84.53483, cs['LONGITUDE'].values[2]))

    self.assertEqual(['GPS'] * 3, cs['_NAV'].values)
    self.assertEqual([450, 450, 1257], cs['DEPTH'].values)
    self.assertEqual([None, 439, None], cs['_MAX_PRESSURE'].values)
    self.assertEqual([None, 16, None], cs['_NUM_BOTTLES'].values)
    self.assertEqual(['test', '1-8,23-24,27,43,104-112', None], cs['_PARAMETERS'].values)
    self.assertEqual([None] * 3, cs['_COMMENTS'].values)

  def test_read_summary_hot(self):
    self.file = libcchdo.model.datafile.SummaryFile()
    self.buff = StringIO.StringIO(self.sample_hot)
    sumhot.read(self.file, self.buff)

    cs = self.file.columns
    self.assertEqual(['33KI134_1'] * 3, cs['EXPOCODE'].values)
    self.assertEqual(['PRS2'] * 3, cs['SECT_ID'].values)
    self.assertEqual([1, 2, 2], cs['STNNBR'].values)
    self.assertEqual([1, 1, 15], cs['CASTNO'].values)
    self.assertEqual(['ROS'] * 3, cs['_CAST_TYPE'].values)
    self.assertEqual(['20020114', '20020115', '20020117'], cs['DATE'].values)
    self.assertEqual([2334, 1259, 856], cs['TIME'].values)
    self.assertEqual(['BE'] * 3, cs['_CODE'].values)

    self.assertTrue(fp_eq(21.344333333, cs['LATITUDE'].values[0]))
    self.assertTrue(fp_eq(22.75, cs['LATITUDE'].values[1]))
    self.assertTrue(fp_eq(22.74950, cs['LATITUDE'].values[2]))

    self.assertTrue(fp_eq(-158.271, cs['LONGITUDE'].values[0]))
    self.assertTrue(fp_eq(-158.001, cs['LONGITUDE'].values[1]))
    self.assertTrue(fp_eq(-158.000166, cs['LONGITUDE'].values[2]))

    self.assertEqual(['GPS'] * 3, cs['_NAV'].values)
    self.assertEqual([1503, 4720, 4720], cs['DEPTH'].values)
    self.assertEqual([16, 24, 12], cs['_NUM_BOTTLES'].values)
    self.assertEqual([1020, 4806, 4806], cs['_MAX_PRESSURE'].values)
    self.assertEqual(['1,2', '1,2,3,4,5,6', '1,2'], cs['_PARAMETERS'].values)
    self.assertEqual(['Dual T, C sensors'] * 3, cs['_COMMENTS'].values)
