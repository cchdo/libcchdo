"""Test scripts entry point for hydro."""

import sys
from contextlib import closing
from StringIO import StringIO
from tempfile import mkstemp
import os
from os import fdopen
from argparse import Namespace
import logging
from unittest import TestCase

from libcchdo import scripts


class NullDevice(object):
    """Ignore test output."""
    def write(self, sss):
        """Ignore the test output."""
        pass


class TestScripts(TestCase):
    """Make sure hydro runs."""

    def test_main(self):
        """Make sure hydro runs without any arguments."""
        saved_stderr = sys.stderr
        sys.stderr = NullDevice()
        try:
            scripts.main()
            self.fail('Should have exited with error, needs sub commands.')
        except SystemExit:
            pass
        sys.stderr = saved_stderr

    def test_main_commands(self):
        """Make sure hydro commands runs."""
        sys.argv = ['hydro', 'commands']
        try:
            saved_stderr = sys.stderr
            err = sys.stderr = StringIO()
            scripts.main()
            sys.stderr = saved_stderr
            self.assertEqual(err[0:5], 'usage')
        except SystemExit:
            pass

    def test_reorder_columns(self):
        """Ensure columns reorder correctly."""
        with closing(StringIO()) as fff:
            fff.name = 'testfile_ct1.csv'
            fff.write('CTD,20140310SIOCCHMYS\n')
            fff.write('NUMBER_HEADERS = 1\n')
            fff.write('CTDPRS,FLUOR,CTDPRS_FLAG_W,CONTRIVED\n')
            fff.write('DBAR,MG/M^2,,\n')
            fff.write('5.0,0.51,2,123\n')
            fff.flush()
            fff.seek(0)

            order = ['CTDPRS', 'CONTRIVED']
            answer = ['CTDPRS', 'CTDPRS_FLAG_W', 'CONTRIVED']
            fid, path = mkstemp()
            try:
                args = Namespace()
                args.input_file = fff
                args.input_type = None
                with fdopen(fid, 'w') as ooo:
                    args.output_file = ooo
                    args.order = ','.join(order)
                    scripts.reorder_columns(args)

                with open(path) as fff:
                    lines = fff.read().split('\n')
                    self.assertEqual(lines[2].split(','), answer)
            finally:
                os.unlink(path)
