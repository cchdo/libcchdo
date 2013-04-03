""" Test case for ..model.datafile.Column """


from unittest import TestCase

from libcchdo.model.datafile import Column
from libcchdo.db.model import std


class TestColumn(TestCase):

    def setUp(self):
        self.column = Column('EXPOCODE')
  
    def test_initialization(self):
        parameter = std.find_by_mnemonic('EXPOCODE')
  
        # The column did not initialize to the correct parameter
        self.assertEqual(self.column.parameter.mnemonic_woce(), 'EXPOCODE')
  
        # Missing values array.
        self.assertEqual(self.column.values, [])
 
        # Missing WOCE flags array
        self.assertEqual(self.column.flags_woce, [])
 
        # Missing IGOSS flags array
        self.assertEqual(self.column.flags_igoss, [])
 
    def test_create_column_with_parameter(self):
        """Creating a column with a given parameter object should set it as
           that column's parameter object.
        """
        param = std.make_contrived_parameter('testparameter')
        column = Column(param)
        self.assertEqual(column.parameter, param)
    
    def test_get(self):
        self.assertEqual(None, self.column.get(0))
        self.column[0] = 1
        self.assertEqual(self.column.get(0), 1)
        self.assertEqual(self.column[0], 1)
        self.assertEqual(None, self.column.get(1))
        self.assertEqual(None, self.column.__getitem__(1))
    
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

    def test_set_i(self):
        """Make sure setting to an index past the current length of the list
           doesn't raise an index exception and works as expected.
        """
        self.column.set(2, 1, 1, 1)
        self.assertEqual(len(self.column), 3)

    def test_append(self):
        self.column.append(2, 3, 4)
        self.assertEqual(len(self.column), 1)
        self.assertEqual(len(self.column.flags_woce), 1)
        self.assertEqual(len(self.column.flags_igoss), 1)

    def test_iter(self):
        self.column.append(1, 2, 3)
        self.column.append(4, 5, 6)
        arr = [x for x in self.column]
        self.assertEqual([1, 4], arr)
    
    def test_contains(self):
        self.column.append(1, 2, 3)
        self.column.append(4, 5, 6)
        self.assertTrue(1 in self.column)
        self.assertFalse(2 in self.column)

    def test_is_flagged_woce(self):
        self.assertFalse(self.column.is_flagged_woce())
        self.column.append(1)
        self.assertFalse(self.column.is_flagged_woce())
        self.column.append(2, 3, 4)
        self.assertTrue(self.column.is_flagged_woce())
    
    def test_is_flagged_igoss(self):
        self.assertFalse(self.column.is_flagged_igoss())
        self.column.append(1)
        self.assertFalse(self.column.is_flagged_igoss())
        self.column.append(2, 3, 4)
        self.assertTrue(self.column.is_flagged_igoss())
    
    def test_is_flagged(self):
        self.assertFalse(self.column.is_flagged())
        self.column.append(1)
        self.assertFalse(self.column.is_flagged())
        self.column.append(2, 3)
        self.assertTrue(self.column.is_flagged())
    
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

    def test_str(self):
        str(self.column) # TODO

    def test_cmp(self):
        self.assertFalse(self.column < self.column)
        self.assertFalse(self.column > self.column)
        self.assertTrue(self.column >= self.column)
        self.column.parameter = None
        self.assertFalse(self.column >= self.column)



