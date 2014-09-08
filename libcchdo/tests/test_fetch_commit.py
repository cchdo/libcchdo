from unittest import TestCase
import os
import os.path
from contextlib import closing
from json import dumps as json_dumps

from libcchdo.scripts import datadir_commit
from libcchdo.db import connect
from libcchdo.db.model.legacy import Base, Document, Cruise, QueueFile
from libcchdo.datadir.util import tempdir, UOWDirName, write_file_manifest
from libcchdo.datadir.filenames import README_FILENAME, UOW_CFG_FILENAME
from libcchdo.datadir.processing import FetchCommitter
from libcchdo.tests import engine_legacy


class TestFetchCommit(TestCase):

    def test_commit(self):
        with tempdir(dir='/tmp') as temp_dir:
            os.chdir(temp_dir)
            with open(os.path.join(temp_dir, README_FILENAME), 'w') as fff:
                fff.write('README\n')
            with open(os.path.join(temp_dir, UOW_CFG_FILENAME), 'w') as fff:
                fff.write(json_dumps({
                    'expocode': 'testexpo',
                    'alias': '',
                    'data_types_summary': '',
                    'params': '',
                    'title': 'CTD',
                    'q_infos': [
                        {
                          "submitted_by": "H.M. van Aken", 
                          "submission_id": 814, 
                          "data_type": "BTL/SUM/CTD files", 
                          "q_id": 1, 
                          "filename": "64PE342.zip", 
                          "date": "2012-05-10", 
                          "id": 814, 
                          "expocode": "64PR20110724"
                        },
                    ],
                    'summary': 'CTD',
                    'tgo_keys': {
                        'tgohy.txt': 'bottle_woce',
                    },
                    'conversions': [],
                    'conversions_checked': False,
                    }))

            on_path = os.path.join(temp_dir, UOWDirName.online)
            os.mkdir(on_path)

            sub_path = os.path.join(temp_dir, UOWDirName.submission)
            os.mkdir(sub_path)
            with open(os.path.join(sub_path, 'testhy.txt'), 'w') as fff:
                fff.write('q_hy')
            with open(os.path.join(sub_path, 'testsu.txt'), 'w') as fff:
                fff.write('q_su')
            with open(os.path.join(sub_path, 'something.txt'), 'w') as fff:
                fff.write('q_something')

            proc_path = os.path.join(temp_dir, UOWDirName.processing)
            os.mkdir(proc_path)
            with open(os.path.join(proc_path, 'supporting.txt'), 'w') as fff:
                fff.write('support')

            tgo_path = os.path.join(temp_dir, UOWDirName.tgo)
            os.mkdir(tgo_path)
            with open(os.path.join(tgo_path, 'tgohy.txt'), 'w') as fff:
                fff.write('tgo_hy')
            with open(os.path.join(tgo_path, 'tgosu.txt'), 'w') as fff:
                fff.write('tgo_su')
            with open(os.path.join(tgo_path, 'something.txt'), 'w') as fff:
                fff.write('q_something')
            with open(os.path.join(tgo_path, 'tgo_hy1.csv'), 'w') as fff:
                fff.write('tgo_hy1')

            online_files = ['testhy.txt', 'testsu.txt', 'original']
            tgo_files = ['tgohy.txt', 'tgosu.txt', 'tgo_hy1.csv', 'something.txt']
            write_file_manifest(temp_dir, online_files, tgo_files)

            fc = FetchCommitter()

            doc = Document()
            doc.ExpoCode = 'testexpo'
            doc.FileType = 'Directory'
            doc.FileName = on_path

            cruise = Cruise()
            cruise.ExpoCode = 'testexpo'

            qfile = QueueFile()
            qfile.id = 1
            qfile.ExpoCode = 'testexpo'

            fc.dstore.Lsesh = connect.scoped(engine_legacy)
            Base.metadata.create_all(engine_legacy)

            fc.dstore.Lsesh.add(cruise)
            fc.dstore.Lsesh.add(doc)
            fc.dstore.Lsesh.add(qfile)

            fc.uow_commit(temp_dir, None, confirm_html=False, send_email=False,
                          dryrun=True)

