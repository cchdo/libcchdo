"""Unit test for libcchdo/Column"""

import sys
sys.path = ['/'.join(sys.path[0].split('/')[:-1])]+sys.path
import libcchdo
from unittest import TestCase,main

class Columns(TestCase):
  def setUp(self):
    self.column = libcchdo.Column("EXPOCODE")

  def test_unknown_parameter(self):
    self.assertRaises(NameError, libcchdo.Column, "NotAParameter")

  def test_initialization(self):
    self.assertEqual(self.column.parameter.woce_mnemonic, "EXPOCODE") # The column did not initialize to the correct parameter
    # TODO This should compare with a parameter object when we have a db.
    self.assertEqual(self.column.values, []) # Missing values array.
    self.assertEqual(self.column.flags_woce, []) # Missing WOCE flags array
    self.assertEqual(self.column.flags_igoss, []) # Missing IGOSS flags array
  
  def test_get(self):
    self.assertRaises(IndexError, self.column.get, 0)
    self.column[0] = 1
    self.assertEqual(self.column.get(0), 1)
    self.assertEqual(self.column[0], 1)
    self.assertRaises(IndexError, self.column.get, 1)
    self.assertRaises(IndexError, self.column.__getitem__, 1)
  
  def test_length(self):
    self.assertEqual(len(self.column), 0)
    self.column[0] = 1
    self.assertEqual(len(self.column), 1)
    self.column[2] = 2
    self.assertEqual(len(self.column), 3)
  
  def test_set(self):
    self.column.set(1, 2, 3, 4)
    self.assertEqual(self.column[1], 2)
    self.assertEqual(self.column.flags_woce[1], 3)
    self.assertEqual(self.column.flags_igoss[1], 4)
    self.assertEqual(len(self.column), 2)
  
  def test_flagged_woce(self):
    self.assertFalse(self.column.is_flagged_woce()) # Column has WOCE flags when there should not be
    self.column[0] = 1
    self.assertFalse(self.column.is_flagged_woce()) # Column has WOCE flags when there should not be
    self.column.set(0, 1, 2, 3)
    self.assertTrue(self.column.is_flagged_woce()) # Column did not have WOCE flags when there should have been
  
  def test_flagged_igoss(self):
    self.assertFalse(self.column.is_flagged_igoss()) # Column has IGOSS flags when there should not be
    self.column[0] = 1
    self.assertFalse(self.column.is_flagged_igoss()) # Column has IGOSS flags when there should not be
    self.column.set(0, 1, 2, 3)
    self.assertTrue(self.column.is_flagged_igoss()) # Column did not have IGOSS flags when there should have been

if __name__ == '__main__':
  main()
