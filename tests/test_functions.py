""" Test case for libcchdo global functions """

from unittest import TestCase
from math import pi
import tempfile
import os

import libcchdo
import libcchdo.db.connect


class TestFunctions(TestCase):

  def test_uniquify(self):
    xs = ['a', 'b', 'a', 'a', 'b']
    self.assertEqual(['a', 'b'], libcchdo.uniquify(xs))

  def test_strip_all(self):
    xs = ['a', 'b   ', '   c', '  d   ']
    self.assertEqual(['a', 'b', 'c', 'd'], libcchdo.strip_all(xs))

  # Database connections

  def test_connect_mysql(self):
    c = libcchdo.db.connect.cchdo()
    cursor = c.cursor()
    cursor.execute("SELECT id FROM parameter_descriptions LIMIT 1")
    self.assert_(cursor.fetchone())
    cursor.close()
    c.close()

#  def test_connect_postgresql(self):
#    c = libcchdo.db.connect.cchdotest()
#    cursor = c.cursor()
#    cursor.execute("SELECT id FROM parameters LIMIT 1")
#    self.assert_(cursor.fetchone())
#    cursor.close()
#    c.close()

  def test_read_arbitrary(self):
    td = tempfile.mkstemp('notacchdofile.txt')
    self.assertRaises(ValueError, libcchdo.read_arbitrary, os.fdopen(td[0]))
    td = tempfile.mkstemp('test_functions.py')
    self.assertRaises(ValueError, libcchdo.read_arbitrary, os.fdopen(td[0]))
    # TODO check more

  def test_great_circle_distance(self):
    self.assertEqual(14095.0562929323, libcchdo.great_circle_distance(0, 0, 0, 180))
    # TODO check this value
