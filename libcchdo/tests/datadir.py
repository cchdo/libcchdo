"""Test cases for datadir"""
from unittest import TestCase
import os
from contextlib import contextmanager
from tempfile import mkdtemp
from shutil import rmtree
from datetime import datetime

from libcchdo.datadir import processing
from libcchdo.datadir.filenames import README_FILENAME, EXPOCODE_FILENAME
from libcchdo.datadir.util import (
    find_data_directory, is_cruise_dir, is_working_dir, is_data_dir)


@contextmanager
def temp_dir(*args, **kwargs):
    tempdir = mkdtemp()
    try:
        yield tempdir
    finally:
        rmtree(tempdir)


class TestProcessing(TestCase):

    def test_working_dir_path(self):
        test_person = 'person'
        test_title = 'title'
        test_dt = datetime(2000, 1, 2, 3, 4, 5)
        test_sep = '-'

        work_dir = processing.working_dir_name(
            test_person, test_title, test_dt, test_sep)
        answer = '2000.01.02{0}{1}{0}{2}'.format(
            test_sep, test_title, test_person)
        self.assertEqual(work_dir, answer)

    def test_mkdir_working(self):
        expected_files = ['00_README.txt']
        expected_directories = [
            'originals', 'processing', 'submission', 'to_go_online']
        with temp_dir() as tempdir:
            person = 'person'
            working_dir = processing.mkdir_working(tempdir, person)
            entries = os.listdir(working_dir)
            for fname in expected_files:
                self.assertIn(fname, entries)
                self.assertTrue(
                    os.path.isfile(os.path.join(working_dir, fname)))
            for entry in expected_directories:
                self.assertIn(entry, entries)
                self.assertTrue(
                    os.path.isdir(os.path.join(working_dir, entry)))

    def test_is_data_dir(self):
        self.assertFalse(is_data_dir('/'))
        self.assertTrue(is_data_dir('/data'))

    def test_is_cruise_dir(self):
        with temp_dir() as tdir:
            self.assertFalse(is_cruise_dir(tdir))
            with open(os.path.join(tdir, EXPOCODE_FILENAME), 'w') as fff:
                fff.write('TESTEXPO')
            self.assertTrue(is_cruise_dir(tdir))

    def test_is_working_dir(self):
        with temp_dir() as tdir:
            self.assertFalse(is_working_dir(tdir))
            with open(os.path.join(tdir, README_FILENAME), 'w') as fff:
                fff.write('README')
            self.assertTrue(is_working_dir(tdir))

    def test_find_datadir(self):
        self.assertEqual('/data', find_data_directory())
