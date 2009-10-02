#!/usr/bin/env python

from distutils.core import setup, Command
from unittest import TextTestRunner, TestLoader
from glob import glob
from os.path import splitext, basename, join as pjoin, walk
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
    testfiles = [ ]
    for t in glob(pjoin(self._dir, 'tests', '*.py')):
      if not t.endswith('__init__.py'):
        testfiles.append('.'.join(
          ['tests', splitext(basename(t))[0]])
        )
    tests = TestLoader().loadTestsFromNames(testfiles)
    TextTestRunner(verbosity = 2).run(tests)

class CleanCommand(Command):
  description = "Cleans directories of .pyc files"
  user_options = []

  def initialize_options(self):
    self._clean_me = [ ]
    for root, dirs, files in os.walk('.'):
      for f in files:
        if f.endswith('.pyc'):
          self._clean_me.append(pjoin(root, f))
  def finalize_options(self):
    pass
  def run(self):
    for clean_me in self._clean_me:
      try:
        os.unlink(clean_me)
      except:
        pass

setup(name="libcchdo",
      version="0.4",
      description="libcchdo setup",
      cmdclass = {'test': TestCommand,
                  'clean': CleanCommand
                 }
     )

