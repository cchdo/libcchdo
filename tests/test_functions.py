""" Test case for libcchdo global functions """

import libcchdo
from unittest import TestCase
from math import pi

class TestFunctions(TestCase):

  def test_uniquify(self):
    xs = ['a', 'b', 'a', 'a', 'b']
    self.assertEqual(['a', 'b'], libcchdo.uniquify(xs))

  def test_strip_all(self):
    xs = ['a', 'b   ', '   c', '  d   ']
    self.assertEqual(['a', 'b', 'c', 'd'], libcchdo.strip_all(xs))

  def test_connect_mysql(self):
    c = libcchdo.connect_mysql()
    cursor = c.cursor()
    cursor.execute("SELECT cruises.id FROM cruises JOIN documents LIMIT 1");
    self.assert_(cursor.fetchone())

  def test_connect_postgresql(self):
    c = libcchdo.connect_postgresql()
    cursor = c.cursor()
    cursor.execute("SELECT id FROM parameters LIMIT 1");
    self.assert_(cursor.fetchone());

  def test_read_arbitrary(self):
    self.assertRaises(ValueError, libcchdo.read_arbitrary, 'notacchdofile.txt')
    self.assertRaises(ValueError, libcchdo.read_arbitrary, 'test_functions.py')
    # TODO check more

  def test_great_circle_distance(self):
    self.assertEqual(14095.0562929323, libcchdo.great_circle_distance(0, 0, 0, 180))
    # TODO check this value

  def test_deg_to_rad(self):
    self.assertEqual(0, libcchdo.deg_to_rad(0))
    self.assertEqual(pi/2, libcchdo.deg_to_rad(90))
    self.assertEqual(pi, libcchdo.deg_to_rad(180))
    self.assertEqual(3*pi/2, libcchdo.deg_to_rad(270))
    self.assertEqual(2*pi, libcchdo.deg_to_rad(360))

  def test_woce_lat_to_dec_lat(self):
    toks = ['12', '34.567', 'N']
    self.assertAlmostEqual(12.57611666667, libcchdo.woce_lat_to_dec_lat(toks))
    toks = ['12', '34.567', 'S']
    self.assertAlmostEqual(-12.57611666667, libcchdo.woce_lat_to_dec_lat(toks))

  def test_woce_lng_to_dec_lng(self):
    toks = ['12', '34.567', 'E']
    self.assertAlmostEqual(12.57611666667, libcchdo.woce_lng_to_dec_lng(toks))
    toks = ['12', '34.567', 'W']
    self.assertAlmostEqual(-12.57611666667, libcchdo.woce_lng_to_dec_lng(toks))

  def test_dec_lat_to_woce_lat(self):
    self.assertEqual('12 34.57 N', libcchdo.dec_lat_to_woce_lat(12.576116666667))
    self.assertEqual('12 34.57 S', libcchdo.dec_lat_to_woce_lat(-12.576116666667))

  def test_dec_lng_to_woce_lng(self):
    self.assertEqual(' 12 34.57 E', libcchdo.dec_lng_to_woce_lng(12.576116666667))
    self.assertEqual(' 12 34.57 W', libcchdo.dec_lng_to_woce_lng(-12.576116666667))

