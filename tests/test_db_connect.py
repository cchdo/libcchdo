""" Test case for libcchdo.db.connect """

from unittest import TestCase
import MySQLdb

import libcchdo
import libcchdo.db.connect


class TestDbConnect(TestCase):

    def test_connect_mysql(self):
        c = libcchdo.db.connect.cchdo()
        cursor = c.cursor()
        cursor.execute("SELECT id FROM parameter_descriptions LIMIT 1")
        self.assert_(cursor.fetchone())
        cursor.close()
        c.close()
  
#  def test_connect_postgresql(self):
#    c = libcchdo.db.connect.cchdotest()
#    cursor = c.cursor()
#    cursor.execute("SELECT id FROM parameters LIMIT 1")
#    self.assert_(cursor.fetchone())
#    cursor.close()
#    c.close()

    def test_reconnect_to_closed_connection(self):
        c = libcchdo.db.connect.cchdo()
        cursor = c.cursor()
        cursor.execute("SELECT id FROM parameter_descriptions LIMIT 1")
        self.assert_(cursor.fetchone())
        cursor.close()
        c.close()
        c = libcchdo.db.connect.cchdo()

    def test_connect_bogus(self):
        cred = {
            'user': 'bogus',
            'passwd': 'bogus',
            'host': 'goship.ucsd.edu',
            'db': 'cchdo'
        }
        self.assertRaises(IOError, libcchdo.db.connect._connect,
            MySQLdb, MySQLdb.Error, **cred)


