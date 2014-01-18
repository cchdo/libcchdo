from StringIO import StringIO
from contextlib import closing

from libcchdo.tests import BaseTestCase, sample_file
from libcchdo.log import LOG
from libcchdo.fns import Decimal
from libcchdo.formats import exchange
from libcchdo.model.datafile import DataFile, Column


class TestFormatsExchange(BaseTestCase):
    def test_read_warn_bad_flag(self):
        with closing(StringIO()) as fff:
            fff.name = 'testfile'
            fff.write('123,a\n')
            fff.flush()
            fff.seek(0)
            dfile = DataFile()
            dfile['CTDSAL'] = Column('CTDSAL')
            exchange.read_data(dfile, fff, ['CTDSAL', 'CTDSAL_FLAG_W'])
        lines = [
            "Bad WOCE flag 'a' for CTDSAL on data row 0",
        ]
        self.assertTrue(self.ensure_lines(lines))

    def test_read_err_flag_col_no_data_col(self):
        with closing(StringIO()) as fff:
            dfile = DataFile()
            with self.assertRaises(KeyError):
                exchange.read_data(dfile, fff, ['CTDSAL_FLAG_W'])
        lines = [
            "Flag column CTDSAL_FLAG_W exists without parameter column CTDSAL",
        ]
        self.assertTrue(self.ensure_lines(lines))

    def test_read_btlnbr_as_string(self):
        with closing(StringIO()) as fff:
            fff.write('SIO1,33.24\n')
            fff.write('01,32.10\n')
            fff.flush()
            fff.seek(0)
            dfile = DataFile()
            dfile['BTLNBR'] = Column('BTLNBR')
            dfile['CTDSAL'] = Column('CTDSAL')
            exchange.read_data(dfile, fff, ['BTLNBR', 'CTDSAL'])
            self.assertEqual(dfile['BTLNBR'].values, ['SIO1', '01'])
            self.assertEqual(
                dfile['CTDSAL'].values, [Decimal('33.24'), Decimal('32.10')])
