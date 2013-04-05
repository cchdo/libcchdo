"""Test scripts entry point for hydro."""

import sys
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
            scripts.main()
        except SystemExit:
            pass
