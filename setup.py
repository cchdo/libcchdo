#!/usr/bin/env python

from distutils.core import setup, Command
from unittest import TextTestRunner, defaultTestLoader
import glob
import imp
import inspect
import os
import sys


class TestCommand(Command):
    '''
    http://da44en.wordpress.com/2002/11/22/using-distutils/
    '''
    description = "Runs tests"
    user_options = []

    def initialize_options(self):
        self._dir = os.getcwd()

    def finalize_options(self):
        pass

    def run(self):
        '''Finds all the tests modules in tests/ and runs them.'''
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
        pass

    def run(self):
        for clean_me in self._clean_me:
            try:
                os.unlink(clean_me)
            except:
                pass


class CoverageCommand (TestCommand):
    '''
    API for coverage-python: http://nedbatchelder.com/code/coverage/api.html
    '''
    description = "Check test coverage"
    user_options = []

    def initialize_options(self):
        import coverage
        # distutils.core.Command is an old-style class so
        # use old supermethod call
        TestCommand.initialize_options(self)
        if ".coverage" in os.listdir(self._dir):
            os.unlink(".coverage")
        self.cov = coverage.coverage()
        self.cov.start()

    def finalize_options(self):
        pass

    def run(self):
        TestCommand.run(self)
        self.cov.stop()
        self.cov.save()
        self.cov.report(file=sys.stdout)


if __name__ == '__main__':
    package_name = 'libcchdo'
    module_path, module_name = os.path.split(os.path.split(os.path.abspath(
                                   inspect.getfile(inspect.currentframe())))[0])
    sys.path.insert(0, module_path)

    imp.load_module(package_name, *imp.find_module(module_name, [module_path]))

    setup(name=package_name,
          version='0.5',
          description='%s setup' % package_name,
          requires=['sqlalchemy', 'netCDF3'],
          cmdclass = {'test': TestCommand,
                      'clean': CleanCommand,
                      'coverage': CoverageCommand,
                     }
         )
