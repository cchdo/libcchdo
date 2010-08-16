#!/usr/bin/env python

from distutils.core import setup, Command
from unittest import TextTestRunner, defaultTestLoader
import glob
import os
import sys
import inspect

sys.path.insert(0, "/".join(sys.path[0].split("/")[:-1]))
sys.path.insert(0, os.path.split(os.path.abspath(inspect.getfile(
                       inspect.currentframe())))[0])

import os

# http://da44en.wordpress.com/2002/11/22/using-distutils/
class TestCommand(Command):
    description = "Runs tests"
    user_options = []

    def initialize_options(self):
        self._dir = os.getcwd()
    def finalize_options(self):
        pass
    def run(self):
        '''Finds all the tests modules in tests/, and runs them.'''
        testfiles = []
        for t in glob.glob(os.path.join(self._dir, 'tests', 'test_*.py')):
            testfiles.append('.'.join(
                    ['tests', os.path.splitext(os.path.basename(t))[0]]))
        try:
            tests = defaultTestLoader.loadTestsFromNames(testfiles)
            TextTestRunner(verbosity = 2).run(tests)
        except AttributeError, e:
            raise ImportError(("It's likely that you have an import error "
                               "in your test file:\n\t%s\nCheck this file's "
                               "imports.") % e)


class CleanCommand(Command):
    description = "Cleans directories of .pyc files"
    user_options = []

    def initialize_options(self):
        self._clean_me = [ ]
        for root, dirs, files in os.walk('.'):
            for f in files:
                if f.endswith('.pyc'):
                    self._clean_me.append(os.path.join(root, f))
    def finalize_options(self):
        pass
    def run(self):
        for clean_me in self._clean_me:
            try:
                os.unlink(clean_me)
            except:
                pass


# API for coverage-python: http://nedbatchelder.com/code/coverage/api.html
class CoverageCommand (TestCommand):
    description = "Check test coverage"
    user_options = []
    def initialize_options (self):
        import coverage
        # distutils.core.Command is old-style, so use old supermethod call
        TestCommand.initialize_options(self)
        if ".coverage" in os.listdir(self._dir):
            os.unlink(".coverage")
        self.cov = coverage.coverage()
        self.cov.start()
    def finalize_options (self):
        pass
    def run (self):
        TestCommand.run(self)
        self.cov.stop()
        self.cov.save()
        self.cov.report(file=sys.stdout)


if __name__ == '__main__':
    setup(name="libcchdo",
          version="0.4",
          description="libcchdo setup",
          cmdclass = {'test': TestCommand,
                      'clean': CleanCommand,
                      'coverage': CoverageCommand,
                     }
         )
