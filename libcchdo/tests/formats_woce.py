from contextlib import closing

from libcchdo.util import StringIO
from libcchdo.tests import BaseTestCase, sample_file
from libcchdo.log import LOG
from libcchdo.formats import exchange
from libcchdo.model.datafile import DataFile, Column


class TestFormatsExchange(BaseTestCase):

    def test_read_data_btlnbr_as_string(self):
        with closing(StringIO()) as fff:
            fff.write('SIO1\n')
            fff.write('01\n')
        exchange.read_data()
