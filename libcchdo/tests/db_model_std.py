import unittest

from decimal import Decimal

from ..db.model import std


class TestDbModelStd(unittest.TestCase):

    def test_populate_library_database_parameters(self):
        with std.closing(std.session()) as sesh:
            std._populate_library_database_parameters(sesh)
            sesh.flush()
            sesh.rollback()

    def test_find_by_mnemonic(self):
        def okay(x):
            if x:
                self.assertTrue(type(x) is std.Parameter)

        # Test something that should come up easily
        okay(std.find_by_mnemonic(u'CTDOXY'))

        # Test something that needs an alias lookup
        okay(std.find_by_mnemonic(u'TALK'))

    def test_parameter_is_in_range(self):
        p = std.Parameter('_test')
        p.bound_lower = 0.0
        p.bound_upper = 1.0
        self.assertTrue(p.is_in_range(0.5))
        self.assertFalse(p.is_in_range(-1.0))
        self.assertFalse(p.is_in_range(2.0))

        p.bound_lower = Decimal(0.0)
        p.bound_upper = Decimal(1.0)
        self.assertTrue(p.is_in_range(0.5))
        self.assertFalse(p.is_in_range(-1.0))
        self.assertFalse(p.is_in_range(2.0))
