"""Test cases for ..algorithms.depth """

from unittest import TestCase

from libcchdo.algorithms import depth
from libcchdo.fns import _decimal

class TestAlgorithmsDepth(TestCase):

    def test_grav_ocean_surface_wrt_latitude(self):
        self.assertAlmostEqual(_decimal('9.780318'), depth.grav_ocean_surface_wrt_latitude(0))
        self.assertAlmostEqual(_decimal('9.80738775'), depth.grav_ocean_surface_wrt_latitude(-60.4987683333))

    def test_depth(self):
        # TODO these numbers almost equal may be imprecise
        self.assertAlmostEqual(_decimal('0.0000102040783'), depth.depth(9.8, [1], [1])[0])
        self.assertRaises(AssertionError, depth.depth, 9.8, [1, 2], [1])
        self.assertAlmostEqual(_decimal('0.000000'), depth.depth(9.8, [16], [2])[0])
        #print depth.depth(9.8, [16, 16], [2, 2])
        #depth has an issue with sequences of length 2 to integrate over.
        answer = [_decimal('0.000002040815871720217975820810286'),
                  _decimal('226.7572705863743630694763440'),
                  _decimal('518.3022651720940178880441487'),
                  _decimal('926.4651666301443099770898946'),
                  _decimal('1606.736517457033660694942632')]
        result = depth.depth(9.8, [1, 2, 3, 4, 5], [5, 4, 3, 2, 1])
        for aaa, bbb in zip(answer, result):
            self.assertAlmostEqual(aaa, bbb)

    def test_density(self):
        self.assertTrue(depth.density(None, 1, 1) is None)

    def test_depth_unesco(self):
        print depth.depth_unesco(1, 0)
        # TODO
