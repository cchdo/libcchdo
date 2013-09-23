from unittest import TestCase
from StringIO import StringIO
from contextlib import closing

from libcchdo.model.datafile import DataFile
from libcchdo.formats.ctd import exchange as ctdex
from libcchdo.fns import _decimal


class TestCTDExchange(TestCase):

    def test_functional_write(self):
        dfile = DataFile()
        dfile.create_columns(['CTDPRS', 'CTDOXY'])
        dfile['CTDPRS'].parameter.display_order = 0
        dfile['CTDOXY'].parameter.display_order = 1
        dfile['CTDPRS'].values = map(_decimal, ['2.0', '4.0'])
        dfile['CTDOXY'].values = map(_decimal, ['254.0', '253.1'])
        dfile['CTDOXY'].flags_woce = [2, 3]

        with closing(StringIO()) as buff:
            ctdex.write(dfile, buff)
            result = buff.getvalue().split('\n')
            self.assertEqual([
                u'        2.0', u'      254.0', u'2'], result[4].split(','))

    def test_num_headers(self):
        """The number of headers header counts itself as a header."""
        with closing(StringIO()) as buff:
            dfile = DataFile()
            dfile.globals['LONGITUDE'] = '0.000'
            ctdex.write(dfile, buff)

            result = buff.getvalue().split('\n')
            self.assertEqual('2', result[1].split(' = ')[1].lstrip())

    def test_write_exchange_decimal_places(self):
        """Decimal places should be kept from the original data."""
        with closing(StringIO()) as buff:
            dfile = DataFile()
            dfile.globals['LONGITUDE'] = _decimal('0.0000000')
            dfile.create_columns(['CTDPRS'])
            dfile['CTDPRS'].values = [_decimal('10.0001'), None]
            ctdex.write(dfile, buff)

            result = buff.getvalue().split('\n')
            # Decimal('0.0000000') is converted to 0E-7 by str. The formatting
            # has to be done manually.
            self.assertEqual('0.0000000', result[2].split(' = ')[1].lstrip())
