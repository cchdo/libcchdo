""" Test case for libcchdo.DataFile """

import libcchdo
from unittest import TestCase

class TestDataFile(TestCase):
  def setUp(self):
    self.file = libcchdo.DataFile()
    self.file.columns['EXPOCODE'] = libcchdo.Column('EXPOCODE')
    self.c = self.file.columns['EXPOCODE']

  def tearDown(self):
    self.file = None

  def test_init(self):
    self.assertEqual(len(self.file.columns), 1)
    self.assertEqual(self.file.stamp, None)
    self.assertEqual(self.file.header, '')
    self.assertEqual(self.file.footer, None)
    self.assertEqual(self.file.globals, {})

  def test_expocodes(self):
    self.c.append('A')
    self.assertEqual(['A'], self.file.expocodes())
    self.c.append('B')
    self.assertEqual(['A', 'B'], self.file.expocodes())
    self.c.append('A')
    self.assertEqual(['A', 'B'], self.file.expocodes()) # Expocodes returns unique expocodes.

  def test_len(self):
    self.assertEqual(len(self.file), 0)
    self.c.append('A')
    self.assertEqual(len(self.file), 1)
    self.c.append('A')
    self.assertEqual(len(self.file), 2)

  def test_sorted_columns(self):
    self.file.columns['CASTNO'] = libcchdo.Column('CASTNO')
    self.file.columns['STNNBR'] = libcchdo.Column('STNNBR')
    self.assertEqual(['EXPOCODE', 'STNNBR', 'CASTNO'], map(lambda c: c.parameter.woce_mnemonic, self.file.sorted_columns()))

  def test_get_property_for_columns(self):
    pass # This is tested by the following tests.

  def test_column_headers(self):
    self.assertEqual(['EXPOCODE'], self.file.column_headers())
    self.file.columns['STNNBR'] = libcchdo.Column('STNNBR')
    self.assertEqual(['EXPOCODE', 'STNNBR'], self.file.column_headers())

  def test_formats(self):
    self.file.columns['CTDOXY'] = libcchdo.Column('CTDOXY')
    self.assertEqual(['11s', '8.1f'], self.file.formats())

  def test_to_hash(self):
    pass

  def test_create_columns(self):
    parameters = ['CTDOXY']
    units = ['UMOL/KG']
    self.file.create_columns(parameters, units)
