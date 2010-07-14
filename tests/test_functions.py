""" Test case for libcchdo global functions """

from unittest import TestCase
from math import pi
import tempfile
import os

import libcchdo


class TestFunctions(TestCase):

  def test_uniquify(self):
    xs = ['a', 'b', 'a', 'a', 'b']
    self.assertEqual(['a', 'b'], libcchdo.uniquify(xs))

  def test_strip_all(self):
    xs = ['a', 'b   ', '   c', '  d   ']
    self.assertEqual(['a', 'b', 'c', 'd'], libcchdo.strip_all(xs))

  def test_read_arbitrary(self):
    td = tempfile.mkstemp('notacchdofile.txt')
    self.assertRaises(ValueError, libcchdo.read_arbitrary, os.fdopen(td[0]))
    td = tempfile.mkstemp('test_functions.py')
    self.assertRaises(ValueError, libcchdo.read_arbitrary, os.fdopen(td[0]))
    # TODO check more

  def test_great_circle_distance(self):
    self.assertEqual(14095.0562929323, libcchdo.great_circle_distance(0, 0, 0, 180))
    # TODO check this value
