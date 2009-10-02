""" Test case for libcchdo.SummaryFile.read_Summary_HOT """

import libcchdo
from unittest import TestCase
from io import StringIO

class TestReadSummaryHot(TestCase):
  sample = """
                                       Hawaii Ocean Time-Series   Cruise HOT-134
   Ship  Section Sta Cast Cast  Date     Time            Position               Depth   Pres. Number  Parameters Comments
  Cruise  WHP ID  #    #  Type         UTC Code   Lat.       Lon.       Code   Max  Hgt  Max Bottles 
-------------------------------------------------------------------------------------------------------------------------
33KI134/1  PRS2    1   1   ROS 011402 2334  BE  21 20.66 N  158 16.26 W  GPS  1503  492 1020     16          1,2    Dual T, C sensors
33KI134/1  PRS2    2   1   ROS 011502 1259  BE  22 45.00 N  158 00.06 W  GPS  4720   -3 4806     24  1,2,3,4,5,6    Dual T, C sensors
33KI134/1  PRS2    2  15   ROS 011702 0856  BE  22 44.97 N  158 00.01 W  GPS  4720   -3 4806     12          1,2    Dual T, C sensors
  """

  def test_read(self):
    self.file = libcchdo.SummaryFile()
    self.buff = StringIO(self.sample)
    self.file.read_Summary_HOT(self.buff)

    cs = self.file.columns
    self.assertEqual(['33KI134_1'] * 3, cs['EXPOCODE'].values)
    self.assertEqual(['PRS2'] * 3, cs['SECT_ID'].values)
    self.assertEqual([1, 2, 2], cs['STNNBR'].values)
    self.assertEqual([1, 1, 15], cs['CASTNO'].values)
    self.assertEqual(['ROS'] * 3, cs['_CAST_TYPE'].values)
    self.assertEqual(['20020114', '20020115', '20020117'], cs['DATE'].values)
    self.assertEqual([2334, 1259, 856], cs['TIME'].values)
    self.assertEqual(['BE'] * 3, cs['_CODE'].values)
    self.assertEqual([-21.344333333333335, -22.75, -22.749500000000001], cs['LATITUDE'].values)
    self.assertEqual([-158.27099999999999, -158.001, -158.00016666666667], cs['LONGITUDE'].values)
    self.assertEqual(['GPS'] * 3, cs['_NAV'].values)
    self.assertEqual([1503, 4720, 4720], cs['DEPTH'].values)
    self.assertEqual([16, 24, 12], cs['_NUM_BOTTLES'].values)
    self.assertEqual([1020, 4806, 4806], cs['_MAX_PRESSURE'].values)
    self.assertEqual(['1,2', '1,2,3,4,5,6', '1,2'], cs['_PARAMETERS'].values)
    self.assertEqual(['Dual T, C sensors'] * 3, cs['_COMMENTS'].values)

