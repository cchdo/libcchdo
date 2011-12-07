import unittest
from decimal import Decimal

from ..algorithms import depth as X


class TestColumn(unittest.TestCase):

#  def test_specific_volume(self):
#    self.assertEqual(1.000033251, X.specific_volume(0, 5, 0))
#    self.assertEqual(0.957736964, X.specific_volume(0, 5, 10000))
#    self.assertEqual(1.00296078,  X.specific_volume(0, 25, 0))
#    self.assertEqual(0.963482064, X.specific_volume(0, 25, 10000))
#    self.assertEqual(0.973069835, X.specific_volume(35, 5, 0))
#    self.assertEqual(0.935025857, X.specific_volume(35, 5, 10000))
#    self.assertEqual(0.977189409, X.specific_volume(35, 25, 0))
#    self.assertEqual(0.941142660, X.specific_volume(35, 25, 10000))

    def test_density(self):
        """ Test values from UNESCO 44 pg -19- """
        self.assertAlmostEqual(X.density( 0, 5,      0), Decimal('999.96675'), 5)
        self.assertAlmostEqual(X.density( 0, 5,  10000), Decimal('1044.12802'), 5)
        self.assertAlmostEqual(X.density( 0, 25,     0), Decimal('997.04796'), 5)
        self.assertAlmostEqual(X.density( 0, 25, 10000), Decimal('1037.90204'), 5)

        self.assertAlmostEqual(X.density(35, 5,      0), Decimal('1027.67547'), 5)
        self.assertAlmostEqual(X.density(35, 5,  10000), Decimal('1069.48914'), 5)
        self.assertAlmostEqual(X.density(35, 25,     0), Decimal('1023.34306'), 5)
        self.assertAlmostEqual(X.density(35, 25, 10000), Decimal('1062.53817'), 5)
