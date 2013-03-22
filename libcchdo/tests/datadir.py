"""Test cases for datadir"""
from unittest import TestCase
import os
from contextlib import contextmanager
from tempfile import mkdtemp
from shutil import rmtree
from datetime import datetime

from libcchdo.datadir import processing


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

        work_dir = processing.working_dir_path(
            '.', test_person, test_title, test_dt, test_sep)
        answer = './2000.01.02{0}{1}{0}{2}'.format(
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

    #def test_depth(self):
    #    print depth.depth(9.8, [1], [1])
    #    self.assertRaises(AssertionError, depth.depth, 9.8, [1,2 ], [1])
    #    print depth.depth(9.8, [16], [2])
    #    #print depth.depth(9.8, [16, 16], [2, 2])
    #    #depth has an issue with sequences of length 2 to integrate over.
    #    print depth.depth(9.8, [1, 2, 3, 4, 5], [5, 4, 3, 2, 1])
    #    # TODO

    #def test_density(self):
    #    self.assertTrue(depth.density(None, 1, 1) is None)

    #def test_depth_unesco(self):
    #    print depth.depth_unesco(1, 0)
    #    # TODO
