"""Test cases for libcchdo.algorithms.depth """

from unittest import TestCase
import datetime
import sys

import libcchdo.algorithms.depth as depth

class TestAlgorithmsDepth(TestCase):

    def test_grav_ocean_surface_wrt_latitude(self):
        self.assertAlmostEqual(9.780318, depth.grav_ocean_surface_wrt_latitude(0))

    def test_depth(self):
        print depth.depth(9.8, [1], [1])
        self.assertRaises(AssertionError, depth.depth, 9.8, [1,2 ], [1])
        print depth.depth(9.8, [16], [2])
        #print depth.depth(9.8, [16, 16], [2, 2])
        #depth has an issue with sequences of length 2 to integrate over.
        print depth.depth(9.8, [1, 2, 3, 4, 5], [5, 4, 3, 2, 1])
        # TODO

    def test_density(self):
        self.assertTrue(depth.density(None, 1, 1) is None)

    def test_depth_unesco(self):
        print depth.depth_unesco(1, 0)
        # TODO
