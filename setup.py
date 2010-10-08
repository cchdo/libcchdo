#!/usr/bin/env python


import distutils.core
import unittest
import glob
import os
import sys
import inspect
import shutil


# DIRECTORY is the absolute path to the directory that contains setup.py
DIRECTORY = os.path.abspath(os.path.split(inspect.stack()[0][1])[0])


PACKAGE_PATH, PACKAGE_NAME = os.path.split(DIRECTORY)


COVERAGE_PATH = os.path.join(DIRECTORY, 'doc', 'coverage')


class TestCommand(distutils.core.Command):
    """http://da44en.wordpress.com/2002/11/22/using-distutils/"""
    description = "Runs tests"
    user_options = []

    def initialize_options(self):
        sys.path.insert(0, PACKAGE_PATH)
        self._dir = DIRECTORY

    def finalize_options(self):
        pass

    def run(self):
        """Finds all the tests modules in tests/ and runs them."""
        testdir = 'tests'
        testfiles = []
        verbosity = 2

        globbed = glob.glob(os.path.join(self._dir, testdir, '*.py'))
        del globbed[globbed.index(os.path.join(self._dir, testdir, '__init__.py'))]
        for t in globbed:
            testfiles.append(
                '.'.join((PACKAGE_NAME, testdir,
                          os.path.splitext(os.path.basename(t))[0])))

        tests = unittest.TestSuite()
        for t in testfiles:
        	__import__(t)
        	tests.addTests(
        	    unittest.defaultTestLoader.loadTestsFromModule(sys.modules[t]))

        unittest.TextTestRunner(verbosity=verbosity).run(tests)
        del sys.path[0]


class CoverageCommand(TestCommand):
    """Check test coverage
       API for coverage-python:
       http://nedbatchelder.com/code/coverage/api.html
    """
    description = "Check test coverage"
    user_options = []

    def initialize_options(self):
        # distutils.core.Command is an old-style class.
        TestCommand.initialize_options(self)

        if '.coverage' in os.listdir(self._dir):
            os.unlink('.coverage')

    def finalize_options(self):
        pass

    def run(self):
        import coverage
        self.cov = coverage.coverage()
        self.cov.start()
        TestCommand.run(self)
        self.cov.stop()
        self.cov.save()
        # Somehow os gets set to None.
        import os
        self.cov.report(file=sys.stdout)
        self.cov.html_report(directory=COVERAGE_PATH)
        # Somehow os gets set to None.
        import os
        print os.path.join(COVERAGE_PATH, 'index.html')


class CleanCommand(distutils.core.Command):
    description = "Cleans directories of .pyc files and documentation"
    user_options = []

    def initialize_options(self):
        self._clean_me = []
        for root, dirs, files in os.walk('.'):
            for f in files:
                if f.endswith('.pyc'):
                    self._clean_me.append(os.path.join(root, f))

    def finalize_options(self):
        print "Clean."

    def run(self):
        for clean_me in self._clean_me:
            try:
                os.unlink(clean_me)
            except:
                pass

class PurgeCommand(CleanCommand):
    description = "Purges directories of .pyc files, caches, and documentation"
    user_options = []

    def initialize_options(self):
        CleanCommand.initialize_options(self)

    def finalize_options(self):
        print "Purged."

    def run(self):
        db_file = os.path.join(DIRECTORY, 'db', 'cchdo_data.db')
        if os.path.exists(db_file):
        	os.unlink(db_file)
        if os.path.isdir(COVERAGE_PATH):
            shutil.rmtree(COVERAGE_PATH)

        doc_dir = os.path.join(DIRECTORY, 'doc')
        if os.path.isdir(doc_dir):
        	shutil.rmtree(doc_dir)

        CleanCommand.run(self)


if __name__ == "__main__":
    distutils.core.setup(name=PACKAGE_NAME,
          version='0.5',
          description="CLIVAR and Carbon Hydrographic Data Office library",
          long_description="Tools for operating on CCHDO's data",
          requires=['sqlalchemy (>=0.5.8)', 'netCDF3'],
          cmdclass = {'test': TestCommand,
                      'coverage': CoverageCommand,
                      'clean': CleanCommand,
                      'purge': PurgeCommand,
                     }
         )
