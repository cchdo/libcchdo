""" Test case for libcchdo.Column """

import libcchdo
from unittest import TestCase

class TestColumn(TestCase):
#  def test_specific_volume(self):
#    self.assertEqual(1.000033251, libcchdo.specific_volume(0, 5, 0))
#    self.assertEqual(0.957736964, libcchdo.specific_volume(0, 5, 10000))
#    self.assertEqual(1.00296078,  libcchdo.specific_volume(0, 25, 0))
#    self.assertEqual(0.963482064, libcchdo.specific_volume(0, 25, 10000))
#    self.assertEqual(0.973069835, libcchdo.specific_volume(35, 5, 0))
#    self.assertEqual(0.935025857, libcchdo.specific_volume(35, 5, 10000))
#    self.assertEqual(0.977189409, libcchdo.specific_volume(35, 25, 0))
#    self.assertEqual(0.941142660, libcchdo.specific_volume(35, 25, 10000))

  def test_density(self):
    self.assertAlmostEqual(libcchdo.density( 0, 5,      0),  999.96675, 5)
    self.assertAlmostEqual(libcchdo.density( 0, 5,  10000), 1044.12802, 5)
    self.assertAlmostEqual(libcchdo.density( 0, 25,     0),  997.04796, 5)
    self.assertAlmostEqual(libcchdo.density( 0, 25, 10000), 1037.90204, 5)

    self.assertAlmostEqual(libcchdo.density(35, 5,      0), 1027.67547, 5)
    self.assertAlmostEqual(libcchdo.density(35, 5,  10000), 1069.48914, 5)
    self.assertAlmostEqual(libcchdo.density(35, 25,     0), 1023.34306, 5)
    self.assertAlmostEqual(libcchdo.density(35, 25, 10000), 1062.53817, 5)
