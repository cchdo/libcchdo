from unittest import TestCase

from libcchdo.model.datafile import DataFile, Column
from libcchdo.fns import _decimal


class TestDataFile(TestCase):

    def setUp(self):
        self.file = DataFile()
        self.c = self.file.columns['EXPOCODE'] = Column('EXPOCODE')
  
    def tearDown(self):
        self.file = None
  
    def test_init(self):
        self.assertEqual(len(self.file.columns), 1)
        self.assertEqual(self.file.footer, None)
        self.assertEqual(self.file.globals, {'stamp': '', 'header': ''})

    def test_expocodes(self):
        self.c.append('A')
        self.assertEqual(['A'], self.file.expocodes())
        self.c.append('B')
        self.assertEqual(['A', 'B'], self.file.expocodes())
        self.c.append('A')
        self.assertEqual(['A', 'B'], self.file.expocodes()) # Expocodes returns unique expocodes.
  
    def test_len(self):
        c = self.file.columns['EXPOCODE']
        del self.file.columns['EXPOCODE']
        self.assertEqual(len(self.file), 0)
        self.file.columns['EXPOCODE'] = c
        self.assertEqual(len(self.file), 0)
        self.c.append('A')
        self.assertEqual(len(self.file), 1)
        self.c.append('A')
        self.assertEqual(len(self.file), 2)
  
    def test_sorted_columns(self):
        self.file.columns['CASTNO'] = Column('CASTNO')
        self.file.columns['STNNBR'] = Column('STNNBR')
        expected = ['EXPOCODE', 'STNNBR', 'CASTNO']
        received = map(lambda c: c.parameter.mnemonic_woce(), self.file.sorted_columns())
        # If lengths are equal and all expected in received, then assume equal
        self.assertEqual(len(expected), len(received))
        self.assertTrue(all( [x in received for x in expected] ))
  
    def test_get_property_for_columns(self):
        pass # This is tested by the following tests.
  
    def test_column_headers(self):
        self.assertEqual(['EXPOCODE'], self.file.column_headers())
        self.file.columns['STNNBR'] = Column('STNNBR')
        expected = ['EXPOCODE', 'STNNBR']
        received = self.file.column_headers()
        # If lengths are equal and all expected in received, then assume equal
        self.assertEqual(len(expected), len(received))
        self.assertTrue(all( [x in received for x in expected] ))
  
    def test_formats(self):
        self.file.columns['CTDOXY'] = Column('CTDOXY')
        self.file.check_and_replace_parameters()
        # Order of columns may be wrong
        self.assertEqual(['%11s', '%9.4f'], self.file.formats())
  
    def test_to_dict(self):
        self.file.to_dict()
        pass # TODO

    def test_str(self):
        str(self.file)
  
    def test_create_columns(self):
        parameters = ['CTDOXY']
        units = ['UMOL/KG']
        self.file.create_columns(parameters, units)

    def test_column_append(self):
        self.assertEqual(self.c.values, [])
        self.c.set(2, 'test')
        self.assertEqual(self.c.values, [None, None, 'test'])
        self.assertEqual(self.c.flags_woce, [])
        self.c.append('test2', 'flag2')
        self.assertEqual(self.c.values, [None, None, 'test', 'test2'])
        self.assertEqual(self.c.flags_woce, [None, None, None, 'flag2'])

    def test_calculate_depths(self):
        self.file['_ACTUAL_DEPTH'] = Column('_ACTUAL_DEPTH')
        self.assertEqual(('actual', []), self.file.calculate_depths())

        del self.file['_ACTUAL_DEPTH']
        self.file.globals['LATITUDE'] = -60.4987683333
        self.file.create_columns(['CTDPRS', 'CTDSAL', 'CTDTMP'])
        with self.assertRaises(OverflowError):
            self.file.calculate_depths()

        self.file.globals['LATITUDE'] = 0
        self.assertEqual(('unesco1983', []), self.file.calculate_depths())

        self.file['CTDPRS'].values = [1]
        self.file['CTDSAL'].values = [1]
        self.file['CTDTMP'].values = [1]

        self.assertEqual(
            ('sverdrup', [_decimal('1.021723814950101286444879340E-8')]),
            self.file.calculate_depths())

    def test_check_and_replace_parameter_contrived(self):
        """Contrived parameters are not checked."""
        col = Column('_DATETIME')
        col.check_and_replace_parameter(self.file, convert=False)


class TestColumn(TestCase):
    def test_decimal_places_requires_decimal(self):
        ccc = Column('test')
        ccc.values = [
            _decimal('-999.0000'),
            20.12355,
            _decimal('-999.00'),
        ]
        with self.assertRaises(ValueError):
            ccc.decimal_places()

    def test_decimal_places(self):
        """A column's decimal places is the max number of places after a decimal
        in the column.

        """
        ccc = Column('test')
        ccc.values = [
            _decimal('-999.0000'),
            _decimal('19.0'),
            _decimal('-999.000'),
            _decimal('-999.00'),
        ]
        self.assertEqual(4, ccc.decimal_places())

    def test_diff(self):
        aaa = Column('aaa')
        bbb = Column('aaa')
        # Make sure can diff on Nones, results in no diff.
        aaa.flags_woce = [None]
        bbb.flags_woce = [None]
        diff = aaa.diff(bbb)
        self.assertFalse(diff['diff'])
