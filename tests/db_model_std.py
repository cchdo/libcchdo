import unittest

from ..db.model import std


class TestDbModelStd(unittest.TestCase):

    def test_find_by_mnemonic(self):
        def okay(x):
            if x:
                self.assertTrue(type(x) is std.Parameter)

        # Test something that should come up easily
        okay(std.find_by_mnemonic('CTDOXY'))

        # Test something that needs an alias lookup
        okay(std.find_by_mnemonic('TALK'))
