""" Test case for libcchdo global functions """

from unittest import TestCase

import libcchdo
import libcchdo.formats.woce


class TestFormatsWoce(TestCase):

  def test_woce_lat_to_dec_lat(self):
    toks = ['12', '34.567', 'N']
    self.assertAlmostEqual(12.57611666667,
        libcchdo.formats.woce.woce_lat_to_dec_lat(toks))
    toks = ['12', '34.567', 'S']
    self.assertAlmostEqual(-12.57611666667,
        libcchdo.formats.woce.woce_lat_to_dec_lat(toks))

  def test_woce_lng_to_dec_lng(self):
    toks = ['12', '34.567', 'E']
    self.assertAlmostEqual(12.57611666667,
        libcchdo.formats.woce.woce_lng_to_dec_lng(toks))
    toks = ['12', '34.567', 'W']
    self.assertAlmostEqual(-12.57611666667,
        libcchdo.formats.woce.woce_lng_to_dec_lng(toks))

  def test_dec_lat_to_woce_lat(self):
    self.assertEqual('12 34.57 N',
        libcchdo.formats.woce.dec_lat_to_woce_lat(12.576116666667))
    self.assertEqual('12 34.57 S',
        libcchdo.formats.woce.dec_lat_to_woce_lat(-12.576116666667))

  def test_dec_lng_to_woce_lng(self):
    self.assertEqual(' 12 34.57 E',
        libcchdo.formats.woce.dec_lng_to_woce_lng(12.576116666667))
    self.assertEqual(' 12 34.57 W',
        libcchdo.formats.woce.dec_lng_to_woce_lng(-12.576116666667))


