import unittest

from libcchdo.db import connect


class TestDbConnect(unittest.TestCase):

    def test_connect_mysql(self):
        c = connect.cchdo()
