from contextlib import closing

from libcchdo.util import StringIO
from libcchdo.tests import BaseTestCase, sample_file
from libcchdo.formats import exchange
from libcchdo.model.datafile import DataFile, Column


class TestFormatsExchange(BaseTestCase):

    def test_read_data_btlnbr_as_string(self):
        with closing(StringIO()) as fff:
            fff.write('BTLNBR\n')
            fff.write('\n')
            fff.write('12\n')

            fff.flush()
            fff.seek(0)

            dfile = DataFile()
            columns = ['BTLNBR']
            exchange.read_data(dfile, fff, columns)

            self.assertTrue(isinstance(dfile['BTLNBR'].values[0], basestring))

