"""Test cases for datadir"""
from unittest import TestCase
import os
from contextlib import contextmanager, closing
from tempfile import mkdtemp
from shutil import rmtree
from datetime import datetime
from zipfile import ZipFile
from StringIO import StringIO

import transaction

from libcchdo.formats.netcdf import nc_dataset_to_stream
from libcchdo.config import get_merger_email
from libcchdo.datadir import processing
from libcchdo.datadir.filenames import README_FILENAME, EXPOCODE_FILENAME
from libcchdo.datadir.util import (
    ReadmeEmail, find_data_directory, is_cruise_dir, is_working_dir,
    is_data_dir)
from libcchdo.datadir.readme import Readme, ProcessingReadme
from libcchdo.db.model.legacy import Event, Cruise, session as lsesh
from libcchdo.datadir.store import LegacyDatastore


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


class TestReadme(TestCase):
    def test_updated_files_manifest(self):
        """Updated files manifest should include stamps."""
        readme = Readme('testexpo', 'processtext')
        with temp_dir() as tdir:
            os.chdir(tdir)
            tgodir = '5.to_go_online'
            try:
                os.makedirs(tgodir)
            except OSError:
                pass
            files = ['hy1.csv', 'ct1.csv', 'ct1.zip', 'hy1.nc', 'nc_hyd.zip',
                     'ctd.nc']
            with open(os.path.join(tdir, tgodir, files[0]), 'w') as fff:
                fff.write('BOTTLE,YYYYMMDDCCHSIOXXX')
            with open(os.path.join(tdir, tgodir, files[1]), 'w') as fff:
                fff.write('CTD,YYYYMMDDCCHSIOXXX')
            with open(os.path.join(tdir, tgodir, files[2]), 'w') as fff:
                with ZipFile(fff, 'w') as zzz:
                    zzz.writestr('0ct1.csv', 'CTD,YYYYMMDDCCHSIOZZZ')
                    zzz.writestr('1ct1.csv', 'CTD,YYYYMMDDCCHSIOXXX')
                    zzz.writestr('2ct1.csv', 'CTD,YYYYMMDDCCHSIOXXX')
            with open(os.path.join(tdir, tgodir, files[3]), 'w') as fff:
                with nc_dataset_to_stream(fff) as ncf:
                    ncf.ORIGINAL_HEADER = 'BOTTLE,YYYYMMDDCCHSIOXXX\n'
            with open(os.path.join(tdir, tgodir, files[4]), 'w') as fff:
                with ZipFile(fff, 'w') as zzz:
                    with closing(StringIO()) as ggg:
                        with nc_dataset_to_stream(ggg) as ncf:
                            ncf.ORIGINAL_HEADER = 'BOTTLE,YYYYMMDDCCHSIOXXX\n'
                        zzz.writestr('0.nc', ggg.getvalue())
                    with closing(StringIO()) as ggg:
                        with nc_dataset_to_stream(ggg) as ncf:
                            ncf.ORIGINAL_HEADER = 'BOTTLE,YYYYMMDDCCHSIOXXX\n'
                        zzz.writestr('1.nc', ggg.getvalue())
            with open(os.path.join(tdir, tgodir, files[5]), 'w') as fff:
                with nc_dataset_to_stream(fff) as ncf:
                    pass
            manifest = readme.updated_files_manifest(files)
            rows = manifest[1].split('\n')
            self.assertEqual('hy1.csv    YYYYMMDDCCHSIOXXX', rows[3])
            self.assertEqual('ct1.csv    YYYYMMDDCCHSIOXXX', rows[4])
            self.assertEqual('ct1.zip    YYYYMMDDCCHSIOXXX', rows[5])
            self.assertEqual('hy1.nc     YYYYMMDDCCHSIOXXX', rows[6])
            self.assertEqual('nc_hyd.zip YYYYMMDDCCHSIOXXX', rows[7])
            self.assertEqual('ctd.nc                      ', rows[8])

    def test_add_processing_note(self):
        transaction.doom()
        dstore = LegacyDatastore()
        cruise = Cruise()
        cruise.ExpoCode = 'EXPO'
        lsesh().add(cruise)
        tempdir = mkdtemp()
        try:
            with open(os.path.join(tempdir, 'uow.json'), 'w') as fff:
                fff.write("""\
{
    "expocode": "EXPO",
    "alias": "ALIAS",
    "data_types_summary": "SUMMARY",
    "params": "PARAMS",
    "q_infos": []
}
""")
            readme = unicode(ProcessingReadme(tempdir))
            note_id = dstore.add_processing_note(
                readme, 'EXPO', 'title', 'summary', [123], dryrun=True)
            event = lsesh().query(Event).get(note_id)
            self.assertEqual(event.Note[0], '=')
        finally:
            rmtree(tempdir)


    def test_email_from_cchdo(self):
        """All emails should be sent with from address cchdo@ucsd.edu"""
        email = ReadmeEmail(dryrun=True)
        self.assertEqual('cchdo@ucsd.edu', email._email['From'])
        self.assertEqual(get_merger_email(), email._email['To'])
