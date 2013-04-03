import unittest

from libcchdo.db import Enum


class TestDbInit(unittest.TestCase):

    def test_Enum(self):
        Enum(['1', '2', '3'])

    def test_Enum_no_values(self):
        self.assertRaises(AssertionError, Enum, None)

    def test_Enum_process_bind_param(self):
        e = Enum([None, '1', '2', '3'], empty_to_none=True)
        self.assertTrue(e.process_bind_param('', None) is None)

        e = Enum(['1', '2', '3'], empty_to_none=True)
        self.assertRaises(AssertionError, e.process_bind_param, '', None)

        e = Enum(['1', '2', '3'])
        self.assertRaises(AssertionError, e.process_bind_param, '', None)

        e = Enum(['1', '2', '3'])
        self.assertEquals('1', e.process_bind_param('1', None))

    def test_Enum_process_result_value(self):
        e = Enum(['1', '2', '3'], strict=True)
        self.assertRaises(AssertionError, e.process_result_value, '5', None)

        e = Enum(['1', '2', '3'])
        self.assertEquals('5', e.process_result_value('5', None))


