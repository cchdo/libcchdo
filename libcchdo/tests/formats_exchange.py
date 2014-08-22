from StringIO import StringIO
from contextlib import closing
from logging import getLogger


log = getLogger(__name__)


from libcchdo.tests import BaseTestCase, sample_file
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

    def test_empty_header_gives_correct_stamp(self):
        """If a file is empty, the stamp reader should still return a tuple."""
        type_stamp = exchange.parse_type_and_stamp_line('')
        self.assertEqual(type_stamp, ('', ''))

    def test_parse_type_and_stamp_line(self):
        type_stamp = exchange.parse_type_and_stamp_line('BOTTLE,20090101XXXXXX')
        self.assertEqual(type_stamp, ('BOTTLE', '20090101XXXXXX'))

    def test_read_unknown_parameter_fillvalue(self):
        """Reading data for a parameter with unknown format should still check
           for out of band.

        """
        with closing(StringIO()) as fff:
            fff.name = 'testfile'
            fff.write('-999,9,1,012\n')
            fff.write('11,2,-999,123\n')
            fff.flush()
            fff.seek(0)
            dfile = DataFile()
            dfile['CTDPRS'] = Column('CTDPRS')
            dfile['UNKPARAM'] = Column('UNKPARAM')
            dfile['BTLNBR'] = Column('BTLNBR')
            exchange.read_data(dfile, fff, ['CTDPRS', 'CTDPRS_FLAG_W', 'UNKPARAM', 'BTLNBR'])
        self.assertEqual(None, dfile['CTDPRS'].values[0])
        self.assertEqual('012', dfile['BTLNBR'].values[0])
        self.assertEqual('123', dfile['BTLNBR'].values[1])
        self.assertEqual(None, dfile['UNKPARAM'].values[1])
