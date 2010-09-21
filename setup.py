#!/usr/bin/env python

from distutils.core import setup, Command
from unittest import TextTestRunner, defaultTestLoader
import glob
import os
import sys
import inspect
import shutil

PACKAGE_NAME = 'libcchdo'
PACKAGE_PATH = os.path.abspath(os.path.split(inspect.stack()[0][1])[0])
COVERAGE_PATH = os.path.join(PACKAGE_PATH, 'doc/coverage')


def absolute_import_libcchdo():
    import imp
    module_path, module_name = os.path.split(PACKAGE_PATH)
    imp.load_module(PACKAGE_NAME, *imp.find_module(module_name, [module_path]))


class TestCommand(Command):
    """http://da44en.wordpress.com/2002/11/22/using-distutils/"""
    description = "Runs tests"
    user_options = []

    def initialize_options(self):
        absolute_import_libcchdo()
        self._dir = os.getcwd()

    def finalize_options(self):
        pass

    def run(self):
        """Finds all the tests modules in tests/ and runs them."""
        testfiles = []
        for t in glob.glob(os.path.join(self._dir, 'tests', 'test_*.py')):
            testfiles.append('.'.join(
                    ['tests', os.path.splitext(os.path.basename(t))[0]]))
        try:
            tests = defaultTestLoader.loadTestsFromNames(testfiles)
            TextTestRunner(verbosity = 2).run(tests)
        except AttributeError, e:
            raise ImportError(("It's likely there is an import error in the "
                               "file that defines this module:\n\t%s\n\t"
                               "The test modules are in tests/.") % e)


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
        print "All clean."

    def run(self):
        db_file = os.path.join(PACKAGE_PATH, 'db', 'cchdo_data.db')
        if os.path.exists(db_file):
        	os.unlink(db_file)
        if os.path.isdir(COVERAGE_PATH):
            shutil.rmtree(COVERAGE_PATH)

        for clean_me in self._clean_me:
            try:
                os.unlink(clean_me)
            except:
                pass


class CoverageCommand(TestCommand):
    """API for coverage-python: http://nedbatchelder.com/code/coverage/api.html"""
    description = "Check test coverage"
    user_options = []

    def initialize_options(self):
        import coverage

        # distutils.core.Command is an old-style class so
        # use old supermethod call
        TestCommand.initialize_options(self)

        if '.coverage' in os.listdir(self._dir):
            os.unlink('.coverage')

        self.cov = coverage.coverage()
        self.cov.start()
        absolute_import_libcchdo()

    def finalize_options(self):
        pass

    def run(self):
        TestCommand.run(self)
        self.cov.stop()
        self.cov.save()
        self.cov.report(file=sys.stdout)
        self.cov.html_report(directory=COVERAGE_PATH)
        print os.path.join(COVERAGE_PATH, 'index.html')


if __name__ == '__main__':
    setup(name=PACKAGE_NAME,
          version='0.5',
          description='%s setup' % PACKAGE_NAME,
          requires=['sqlalchemy (>=0.5.8)', 'netCDF3'],
          cmdclass = {'test': TestCommand,
                      'clean': CleanCommand,
                      'coverage': CoverageCommand,
                     }
         )
