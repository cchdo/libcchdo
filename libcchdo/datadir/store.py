import os
from datetime import datetime, date
from contextlib import closing, contextmanager
from urllib2 import HTTPError
from shutil import copy2, copyfileobj
from traceback import format_exc
from tempfile import SpooledTemporaryFile
from cgi import FieldStorage
import tarfile

import transaction

from sqlalchemy.exc import DataError

from libcchdo import LOG
from libcchdo.formats.formats import guess_file_type
from libcchdo.db import connect
from libcchdo.db.model import legacy
from libcchdo.db.model.legacy import QueueFile, Session as Lsesh
from libcchdo.config import (
    get_pycchdo_person_id, get_legacy_datadir_host, get_merger_name_first,
    get_merger_name_last, get_option)
from libcchdo.datadir.util import (
    working_dir_name, dryrun_log_info, mkdir_ensure, checksum_dir, DirName,
    UOWDirName, uow_copy, tempdir, read_file_manifest, regenerate_file_manifest,
    is_uowdir_effectively_empty, checksum_diff_summary)
from libcchdo.datadir.dl import AFTP, SFTP, pushd
from libcchdo.datadir.filenames import (
    README_FILENAME, README_FINALIZED_FILENAME)


def write_cruise_dir_expocode(cruise_dir, expocode):
    """Write cruise directory ExpoCode file.

    """
    path = os.path.join(cruise_dir, EXPOCODE_FILENAME)
    with open(path, 'w') as f:
        f.write(expocode + '\n')


def read_cruise_dir_expocode(cruise_dir):
    """Read cruise directory ExpoCode file."""
    path = os.path.join(cruise_dir, EXPOCODE_FILENAME)
    with open(path) as f:
        return f.read().rstrip()


def _check_working_dir(working_dir):
    if not is_working_dir(working_dir):
        raise ValueError(
            u'Not a working directory {0!r}'.format(
            working_dir))


def working_dir_expocode(working_dir):
    """Return the ExpoCode that is tied to the working dir.

    """
    _check_working_dir(working_dir)
    cwd = os.path.abspath(working_dir)
    while not is_cruise_dir(cwd):
        cwd = os.path.dirname(cwd)
    if not cwd or cwd == '/':
        LOG.error(u'Unable to find cruise directory')
        return None
    return read_cruise_dir_expocode(cwd)


def _copy_uow_online_into_work_dir(uow_dir, work_dir, dir_perms,
                                   file_manifest_set, online_files_set,
                                   tgo_files_set):
    """Copy UOW files to go online into working dir.
    
    Accounts for having to move originally online files into originals.

    """
    new_files = tgo_files_set & file_manifest_set

    removed_files = online_files_set - file_manifest_set
    overwritten_files = online_files_set & new_files

    unchanged_files = online_files_set - removed_files - overwritten_files

    missing_tgo_files = new_files ^ tgo_files_set
    if missing_tgo_files:
        msg = (u'Some files to go online were deleted from the file '
            'manifest:\n{0}\nContinue? (y/[n]) ').format(
            '\n'.join(missing_tgo_files))
        cont = None
        while cont not in ('y', 'n'):
            cont = raw_input(msg).lower()
        if cont != 'y':
            raise ValueError(u'Missing to go online files. Abort.')

    # Retain deleted/overwritten copies in originals directory.
    originals_files = removed_files | overwritten_files
    if originals_files:
        mkdir_ensure(os.path.join(work_dir, DirName.original), dir_perms)
    for fname in originals_files:
        uow_copy(
            uow_dir, UOWDirName.online, work_dir, DirName.original, fname)

    return (new_files, removed_files, overwritten_files, unchanged_files,
            missing_tgo_files)


def _copy_uow_dirs_into_work_dir(uow_dir, work_dir):
    """Copy a UOW's processing contents into a working directory.

    This only handles the subdirectories and leaves the readme file for later.

    """
    if not is_uowdir_effectively_empty(uow_dir, UOWDirName.processing):
        uow_copy(uow_dir, UOWDirName.processing, work_dir, DirName.processing)
    if not is_uowdir_effectively_empty(uow_dir, UOWDirName.submission):
        uow_copy(uow_dir, UOWDirName.submission, work_dir, DirName.submission)
    uow_copy(uow_dir, UOWDirName.tgo, work_dir, DirName.tgo)


class Datastore(object):
    """Abstraction of datafile storage model."""

    def _as_received(self, *ids):
        """Generate the currently As-Received queue files."""
        return []

    def _queuefile_info(self, qfile):
        return {
            'filename': qfile.unprocessed_input,
            'submitted_by': qfile.contact,
            'date': qfile.date_received,
            'data_type': qfile.parameters,
            'q_id': qfile.id,
            'submission_id': qfile.submission_id,
            'expocode': qfile.expocode,
        }

    def as_received_infos(self, *ids):
        """Return queue file information for each queue id."""
        qfis = []
        for qfile in self._as_received(*ids):
            qfis.append(self._queuefile_info(qfile))
        return qfis

    def finalize_readme(self, readme, final_sections, output_fileobj):
        """Write a finalized readme to the UOW dir.

        This is done by removing -UOW- replacement lines and adding the
        finalized sections.

        """
        uow_readme_path = os.path.join(readme.uow_dir, README_FILENAME)
        with open(uow_readme_path) as iii:
            for line in iii:
                if line.startswith('.. -UOW-'):
                    continue
                output_fileobj.write(line)
            output_fileobj.write(final_sections)


def get_datastore(dstore_id=None):
    if dstore_id is None:
        dstore_id = get_option('db', 'dstore', lambda: '')
    if dstore_id == 'pycchdo':
        return PycchdoDatastore()
    else:
        return LegacyDatastore()


class LegacyDatastore(Datastore):
    """Implementation of /data datafile storage model."""
    def __init__(self):
        self.sftp_host = get_legacy_datadir_host()
        self.sftp = SFTP()
        self._aftp = None
        self.cruise_original_dir = None

    @property
    def aftp(self):
        if self._aftp:
            return self._aftp
        self.sftp.connect(self.sftp_host)
        self._aftp = AFTP(self.sftp)
        return self._aftp

    def _queuefile_info(self, qfile):
        return {
            'filename': qfile.unprocessed_input,
            'submitted_by': qfile.contact,
            'date': qfile.date_received,
            'data_type': qfile.parameters,
            'q_id': qfile.id,
            'submission_id': qfile.submission_id,
            'expocode': qfile.expocode,
        }

    def as_received_unmerged_list(self):
        """Return a list of dictionaries representing files that are not merged.

        """
        with closing(legacy.session()) as sesh:
            unmerged_qfs = sesh.query(QueueFile).\
                filter(QueueFile.merged == 0).all()
            qfis = []
            for qf in unmerged_qfs:
                qfi = self._queuefile_info(qf)
                del qfi['date']
                qfi['filename'] = os.path.basename(qfi['filename'])
                qfis.append(qfi)
            return qfis

    def _as_received(self, *ids):
        with closing(legacy.session()) as sesh:
            try:
                ids = map(int, ids)
            except ValueError:
                ids = []
            qfs = sesh.query(QueueFile).filter(QueueFile.id.in_(ids)).all()
            for qf in qfs:
                yield qf

    def fetch_as_received(self, local_path, *ids):
        """Copy the referenced as-received files into the directory.

        """
        qf_info = []
        for qf in self._as_received(*ids):
            if qf.is_merged():
                LOG.info(
                    u'QueueFile {0} is marked already merged'.format(qf.id))
            elif qf.is_hidden():
                LOG.info(u'QueueFile {0} is marked hidden'.format(qf.id))
            path = qf.unprocessed_input
            filename = os.path.basename(path)

            submission_subdir = os.path.join(local_path, str(qf.id))
            mkdir_ensure(submission_subdir, 0775)
            submission_path = os.path.join(submission_subdir, filename)

            with self.aftp.dl(path) as fff:
                if fff is None:
                    LOG.error(u'Unable to download {0}'.format(path))
                    continue
                with open(submission_path, 'w') as ooo:
                    copyfileobj(fff, ooo)

            qfi = self._queuefile_info(qf)
            qfi['id'] = qfi['submission_id']
            qfi['filename'] = filename
            qf_info.append(qfi)
        return qf_info

    @contextmanager
    def _cruise_directory(self, expocode):
        with closing(legacy.session()) as sesh:
            q_docs = sesh.query(legacy.Document).\
                filter(legacy.Document.ExpoCode == expocode).\
                filter(legacy.Document.FileType == 'Directory')
            num_docs = q_docs.count()
            if num_docs < 1:
                LOG.error(
                    u'{0} does not have a directory entry.'.format(expocode))
                raise ValueError()
            elif num_docs > 1:
                LOG.error(
                    u'{0} has more than one directory entry.'.format(expocode))
                raise ValueError()
            yield q_docs.first()

    def _cruise_dir(self, expocode):
        with self._cruise_directory(expocode) as doc:
            return doc.FileName

    def cruise_dir(self, expocode):
        """Return a fully-qualified path to the cruise directory."""
        return '{0}:{1}'.format(
            get_legacy_datadir_host(), self._cruise_dir(expocode))
                
    IGNORED_FILES = ['Queue', 'original']

    def fetch_online(self, path, expocode):
        """Copy the referenced cruise's current datafiles into the directory.

        Download the cruise's online files into path.

        """
        try:
            with self._cruise_directory(expocode) as doc:
                cruise_dir = doc.FileName

            for fname in self.aftp.listdir(cruise_dir):
                online_path = os.path.join(cruise_dir, fname)
                local_path = os.path.join(path, fname)
                try:
                    if self.aftp.isdir(online_path):
                        continue
                    with self.aftp.dl(online_path) as fff:
                        if not fff:
                            LOG.error(
                                u'Could not download {0}'.format(online_path))
                            continue
                        with open(local_path, 'w') as ooo:
                            copyfileobj(fff, ooo)
                except HTTPError, e:
                    os.unlink(local_path)
                    if fname in self.IGNORED_FILES:
                        continue
                    LOG.error(
                        u'Could not download {0}:\n{1!r}'.format(fname, e))
        except IOError, err:
            LOG.error(
                u'Unable to list cruise directory {0}'.format(cruise_dir))
        except ValueError:
            pass

    def fetch_originals(self, path, expocode):
        """Copy the referenced cruise's original datafiles into the directory.
        Download the cruise's original files into path.

        """
        try:
            with self._cruise_directory(expocode) as doc:
                cruise_dir = doc.FileName
        except ValueError:
            return
        originals_dir = os.path.join(cruise_dir, 'original')
        LOG.info(u'Downloading {0}'.format(originals_dir))

        try:
            self.aftp.dl_dir(originals_dir, path)
        except IOError:
            LOG.error(
                u'Unable to download originals directory {0}'.format(path))

    def mark_merged(self, q_ids):
        for qid in q_ids:
            qf = Lsesh.query(QueueFile).filter(QueueFile.id == qid).first()
            if not qf:
                LOG.error(u'Missing QueueFile {0}'.format(qid))
                raise ValueError(u'Unable to mark QueueFile {0} as merged.'.format(
                    qid))
            if qf.is_merged():
                LOG.warn(u'QueueFile {0} is already merged.'.format(qf.id))
            qf.date_merged = date.today()
            qf.set_merged()

    def create_history_note(self, readme, expocode, title, summary,
                                action='Website Update'):
        cruise = Lsesh.query(legacy.Cruise).\
            filter(legacy.Cruise.ExpoCode == expocode).first()
        if not cruise:
            LOG.error(
                u'{0} does not refer to a cruise that exists.'.format(expocode))
            return

        event = legacy.Event()
        event.ExpoCode = cruise.ExpoCode
        event.First_Name = get_merger_name_first()
        event.LastName = get_merger_name_last()
        event.Data_Type = title
        event.Action = action
        event.Date_Entered = datetime.now().date()
        event.Summary = summary
        event.Note = readme
        return event

    def add_history_note(self, readme, expocode, title, summary,
                                action='Website Update'):
        """Add history note for the given readme notes."""
        event = self.create_history_note(
            readme, expocode, title, summary, action)
        Lsesh.add(event)
        Lsesh.flush()
        return event.ID

    def check_online_checksums(self, uow_dir, expocode):
        """A crude check that no online files have changed since UOW fetch.

        This is performed before comitting a UOW in case of multiple mergers
        working on the same cruise.

        """
        
        saved_dryrun = self.aftp.dryrun
        self.aftp.dryrun = False
        try:
            fetch_dir = os.path.join(uow_dir, UOWDirName.online)
            fetch_checksum, fetch_file_checksums = checksum_dir(fetch_dir)
            with tempdir() as temp_dir:
                self.fetch_online(temp_dir, expocode)
                current_checksum, current_file_checksums = \
                    checksum_dir(temp_dir)
                if fetch_checksum != current_checksum:
                    checksum_diff_summary(
                        fetch_file_checksums, current_file_checksums)
                    raise ValueError(
                        u'Cruise online files have changed since the last UOW '
                        'fetch!')
        finally:
            self.aftp.dryrun = saved_dryrun

    def check_cruise_exists(self, expocode, dir_perms, dryrun):
        """Ensure remote cruise original directory exists."""
        self.aftp.dryrun = dryrun
        cruise_dir = self._cruise_dir(expocode)
        self.cruise_original_dir = os.path.join(cruise_dir, 'original')
        if not self.aftp.isdir(self.cruise_original_dir):
            try:
                LOG.info(u'Cruise original directory did not exist. Creating. '
                    '{0}'.format(self.cruise_original_dir))
                self.aftp.mkdir(self.cruise_original_dir, dir_perms)
            except (IOError, OSError), err:
                LOG.error(
                    u'Could not ensure original directory {0} exists: '
                    '{1!r}'.format(self.cruise_original_dir, err))
                raise ValueError()

    def check_fetched_online_unchanged(self, readme):
        """Ensure fetched online files have not changed since fetch."""
        # XXX Datadir specific (Make sure online files have not changed...how?)
        try:
            self.check_online_checksums(
                readme.uow_dir, readme.uow_cfg['expocode'])
        except ValueError, err:
            LOG.error(u'{0} Abort.'.format(err))
            raise err

    def _get_file_manifest(self, uow_dir, work_dir):
        """Read or regenerate the file manifest to put online for a UOW.

        file_manifest is the list of all files that should be online. It is
        composed of those files that are already online, minus those that should
        not be online, plus the files that are new.

        Return: tuple of sets of::
            * all files to be online
            * currently online files (current/to be removed)
            * files ready to go online (new/updated)

        """
        online_files = os.listdir(os.path.join(uow_dir, UOWDirName.online))
        tgo_files = os.listdir(os.path.join(work_dir, DirName.tgo))
        try:
            file_manifest = read_file_manifest(uow_dir)
            if not file_manifest:
                file_manifest = regenerate_file_manifest(
                    uow_dir, online_files, tgo_files)
        except (IOError, OSError):
            file_manifest = regenerate_file_manifest(
                uow_dir, online_files, tgo_files)
        if not file_manifest:
            raise ValueError(u'Empty file manifest. Abort.')
        return set(file_manifest), set(online_files), set(tgo_files)

    def commit(self, readme, person, dir_perms, finalized_readme_path, dryrun):
        """Perform actions needed to put files in work dir and online."""
        # Datadir specific 

        # Prepare a working directory locally to be uploaded
        # Make sure the UOW doesn't already exist.
        work_dir_base = working_dir_name(person, title=readme.uow_cfg['title'])
        try:
            assert self.cruise_original_dir is not None
        except AssertionError:
            raise AssertionError(
                u'check_cruise_exists should be called before committing')
        remote_work_path = os.path.join(self.cruise_original_dir, work_dir_base)
        try:
            if self.aftp.isdir(remote_work_path):
                LOG.error(u'Work directory {work_dir} already exists on '
                          '{host}. Abort.'.format(
                    work_dir=remote_work_path, host=self.sftp_host))
                raise ValueError()
        except IOError:
            pass

        cruise_dir = self._cruise_dir(readme.uow_cfg['expocode'])
        with tempdir(dir='/tmp') as temp_dir:
            work_dir = os.path.join(temp_dir, work_dir_base)
            mkdir_ensure(work_dir, dir_perms)

            # Copy UOW contents into the local working directory
            _copy_uow_dirs_into_work_dir(readme.uow_dir, work_dir)
            try:
                file_sets = self._get_file_manifest(readme.uow_dir, work_dir)
                (new_files, removed_files, overwritten_files, unchanged_files,
                 missing_tgo_files) = _copy_uow_online_into_work_dir(
                    readme.uow_dir, work_dir, dir_perms, *file_sets)
                updated_files = new_files | overwritten_files
            except ValueError, err:
                LOG.error(format_exc(err))
                raise err

            work_readme_path = os.path.join(work_dir, README_FILENAME)
            # Calculate remote work path to use in README
            try:
                final_sections = u'\n'.join(
                    readme.finalize_sections(
                        remote_work_path, cruise_dir, list(updated_files)))
            except ValueError, err:
                LOG.error(u'{0} Abort.\n{1}'.format(err, format_exc(err)))
                raise err

            LOG.debug(u'{0} final sections:\n{1}'.format(
                README_FILENAME, final_sections))

            with open(work_readme_path, 'w') as ooo:
                self.finalize_readme(readme, final_sections, ooo)
            copy2(work_readme_path, finalized_readme_path)

            # All green. Go!
            LOG.info(u'Committing to {0}:{1}'.format(
                self.sftp_host, remote_work_path))

            # upload the working directory
            self.aftp.up_dir(work_dir, remote_work_path)

        # Update the online files. It is ok to overwrite/delete at this point as
        # those affected have already been written to originals.
        for fname in removed_files:
            self.aftp.remove(os.path.join(cruise_dir, fname))
        LOG.info('unchanged:')
        for fname in unchanged_files:
            try:
                self.aftp.up(
                    os.path.join(readme.uow_dir, UOWDirName.online, fname),
                    os.path.join(cruise_dir, fname), suppress_errors=False)
            except IOError, err:
                # It doesn't matter, the file hasn't changed.
                pass
                
        LOG.info('updated/new:')
        for fname in updated_files:
            try:
                self.aftp.up(
                    os.path.join(readme.uow_dir, UOWDirName.tgo, fname),
                    os.path.join(cruise_dir, fname), suppress_errors=False)
            except IOError, err:
                LOG.critical(
                    u'Unable to put {0} online: {1!r}'.format(fname, err))
        # TODO Is there any way we can recover at this point? Some updated files
        # may have been overwritten by now Other than making this loop atomic, I
        # don't see how.
        dryrun_log_info(u'Data file commit completed successfully.', dryrun)

    def commit_postflight(self, readme, email_path, expocode, title, summary,
                          q_ids, finalized_readme_path, dryrun):
        """Perform actions to put history online. This is done post-flight.

        This includes writing history event, marking queue files merged, general
        bookkeeping and notifying the CCHDO community.

        """
        dryrun_log_info('Writing history and notifications.', dryrun)

        note_id = self.add_processing_note(
            readme, expocode, title, summary, q_ids, dryrun)
        return note_id

    def add_processing_note(self, readme, expocode, title, summary, q_ids,
                            dryrun=True):
        """Record processing history note and mark queue files merged.

        """
        note_id = self.add_history_note(readme, expocode, title, summary)
        self.mark_merged(q_ids)
        return note_id


from sqlalchemy import engine_from_config
from pyramid.compat import configparser
from pycchdo import main as pycchdo
from pycchdo.models.serial import (
    DBSession, Change, Cruise, Person, UOW, FSFile, Note
    )
from libcchdo.config import _CONFIG


class PycchdoDatastore(Datastore):
    """Implementation of pycchdo data storage model."""

    def __init__(self):
        self._cruiseobj = None
        try:
            pasteini = _CONFIG.get('pycchdo', 'pasteini')
        except configparser.NoSectionError, err:
            LOG.error(
                u'A pycchdo configuration path must be specified.')
            raise err
        config = configparser.ConfigParser({'here': os.path.dirname(pasteini)})
        config.read(pasteini)
        settings = dict(config.items('app:pycchdo'))
        pycchdo(None, **settings)

    def cruise_dir(self, expocode):
        """Return a fully-qualified path to the cruise directory."""
        LOG.info(u'pycchdo does not use the concept of cruise directories.')
        return ''

    def as_received_unmerged_list(self):
        """Return a list of dictionaries representing files that are not merged.

        """
        qfiles = []
        for avf in Change.filtered_data('pending'):
            qfiles.append(self._queuefile_info(avf))
        return qfiles

    def _queuefile_info(self, qfile):
        return {
            'filename': qfile.value.name,
            'submitted_by': qfile.p_c.name,
            'date': qfile.ts_c,
            'data_type': qfile.attr,
            'q_id': qfile.id,
            'submission_id': qfile.submission.id,
            'expocode': str(qfile.obj.id),
        }

    def _as_received(self, *ids):
        for aid in ids:
            try:
                yield Change.query().get(aid)
            except DataError:
                transaction.begin()

    def fetch_as_received(self, local_path, *ids):
        """Copy the referenced as-received files into the directory.

        """
        qf_info = []
        for qfile in self._as_received(*ids):
            if qfile.accepted:
                LOG.info(
                    u'QueueFile {0} is marked already merged'.format(qfile.id))
            elif qfile.is_rejected():
                LOG.info(u'QueueFile {0} is marked hidden'.format(qfile.id))

            filename = qfile.value.name

            submission_subdir = os.path.join(local_path, str(qfile.id))
            mkdir_ensure(submission_subdir, 0775)
            submission_path = os.path.join(submission_subdir, filename)

            with open(submission_path, 'w') as ooo:
                copyfileobj(qfile.value.open_file(), ooo)

            qfi = self._queuefile_info(qfile)
            qfi['id'] = qfi['submission_id']
            qf_info.append(qfi)
        return qf_info

    def _cruise(self, cid):
        if self._cruiseobj is None:
            try:
                self._cruiseobj = Cruise.get_by_id(cid)
            except ValueError:
                pass
            if not self._cruiseobj:
                raise ValueError(u'Unable to find cruise for {0}'.format(cid))
        return self._cruiseobj

    def fetch_online(self, path, cid):
        """Copy the referenced cruise's current datafiles into the directory.

        Download the cruise's online files into path.

        """
        cruise = self._cruise(cid)
        for key, cfile in cruise.files.items():
            fff = cfile.file
            fname = fff.name
            local_path = os.path.join(path, fname)
            
            try:
                with open(local_path, 'w') as ooo:
                    copyfileobj(fff.open_file(), ooo)
            except (IOError, OSError), err:
                os.unlink(local_path)
                LOG.error(u'Could not download {0}:\n{1}'.format(fname, err))

    def fetch_originals(self, path, cid):
        """Copy the referenced cruise's original datafiles into the directory.

        Download the cruise's original files into path.

        In the case of pycchdo, this is the archive attribute.

        """
        cruise = self._cruise(cid)
        archive = cruise.get('archive')
        if not archive:
            return
        ua_tar = tarfile.open(mode='r', fileobj=archive.open_file())
        try:
            ua_tar.extractall(path)
        finally:
            ua_tar.close()

        # TODO Also maybe include any changes to data files. These are
        # files that have been accepted.
        # 2a. group them into directories by date?

    def check_online_checksums(self, uow_dir, cid):
        """A crude check that no online files have changed since UOW fetch.

        This is performed before comitting a UOW in case of multiple mergers
        working on the same cruise.

        """
        fetch_dir = os.path.join(uow_dir, UOWDirName.online)
        fetch_checksum, fetch_file_checksums = checksum_dir(fetch_dir)
        with tempdir() as temp_dir:
            self.fetch_online(temp_dir, cid)
            current_checksum, current_file_checksums = checksum_dir(temp_dir)
            if fetch_checksum != current_checksum:
                checksum_diff_summary(
                    fetch_file_checksums, current_file_checksums)
                raise ValueError(
                    u'Cruise online files have changed since the last UOW '
                    'fetch!')

    def check_cruise_exists(self, expocode, dir_perms, dryrun):
        """Ensure remote cruise original directory exists."""
        self._cruise(expocode)

    def check_fetched_online_unchanged(self, readme):
        """Ensure fetched online files have not changed since fetch."""
        self.check_online_checksums(
            readme.uow_dir, readme.uow_cfg['expocode'])

    def _get_file_key(self, fname, known_fkeys={}):
        """Return the Cruise key that should be used to store the file.

        This function will check known_fkeys for the fname and return the given
        key if present.

        """
        try:
            return known_fkeys[fname]
        except KeyError:
            pass

        ftype = guess_file_type(fname)
        if not ftype:
            LOG.warn(u'Unable to determine file type for file to go '
                'online: {0}. Omitted.'.format(fname))
            return None

        ftype = ftype.replace('btl.', 'bottle.')
        ftype = ftype.replace('.ex', '.exchange')
        ftype = ftype.replace('.nc', '.netcdf')
        ftype = ftype.replace('.', '_')
        return ftype

    def _get_file_manifest(self, uow_dir, tgo_files):
        """Read or regenerate the file manifest to put online for a UOW.

        file_manifest is the list of all files that should be online. It is
        composed of those files that are already online, minus those that should
        not be online, plus the files that are new.

        Return: tuple of sets of::
            * all files to be online
            * currently online files (current/to be removed)
            * files ready to go online (new/updated)

        """
        online_files = os.listdir(os.path.join(uow_dir, UOWDirName.online))
        try:
            file_manifest = read_file_manifest(uow_dir)
        except (IOError, OSError):
            file_manifest = None
        if not file_manifest:
            file_manifest = regenerate_file_manifest(
                uow_dir, online_files, tgo_files)
        if not file_manifest:
            raise ValueError(u'Empty file manifest. Abort.')
        return set(file_manifest), set(online_files), set(tgo_files)

    def commit(self, readme, person, dir_perms, finalized_readme_path, dryrun):
        """Perform actions needed to store the history and put files online.

        The UOW configuration is optionally allowed to have a 'tgo_keys' key to
        explicitly map to go online file names to Cruise keys.

        """
        uow_cfg = readme.uow_cfg
        uow = UOW()
        DBSession.add(uow)
        cruise = Cruise.get_by_id(uow_cfg['expocode'])

        pid = get_pycchdo_person_id()
        try:
            person = Person.query().get(pid)
        except DataError:
            person = None
        if not person:
            raise ValueError(u'Unable to find or create signer.')

        tgo_path = os.path.join(readme.uow_dir, UOWDirName.tgo)
        uow.results = []
        known_fkeys = uow_cfg.get('tgo_keys', {})
        for fname in os.listdir(tgo_path):
            ftype = self._get_file_key(fname, known_fkeys)
            if ftype is not None:
                fsf = FieldStorage()
                fsf.file = open(os.path.join(tgo_path, fname), 'r')
                fsf.filename = fname
                uow.results.append(cruise.set(person, ftype, fsf))

        # tar up the processing dir
        proc_path = os.path.join(readme.uow_dir, UOWDirName.processing)
        with SpooledTemporaryFile() as fff, pushd(proc_path):
            ua_tar = tarfile.open(mode='w', fileobj=fff)
            try:
                ua_tar.add('.')
            finally:
                ua_tar.close()
            fsf = FieldStorage()
            fsf.file = fff
            fsf.filename = 'processing.tar'
            fsf.type = 'application/x-tar'
            uow.support = FSFile.from_fieldstorage(fsf)

        uow.suggestions = []
        for qinfo in uow_cfg['q_infos']:
            change = Change.query().get(qinfo['q_id'])
            change.accept(person)
            uow.suggestions.append(change)

        # Finalize the readme file. This means adding the conversion and updated
        # manifests.
        tgo_files = [result.file.name for result in uow.results]
        file_sets = self._get_file_manifest(readme.uow_dir, tgo_files)
        updated_files = tgo_files
        try:
            final_sections = u'\n'.join(
                readme.conversions() + \
                readme.updated_files_manifest(updated_files))
        except ValueError, err:
            LOG.error(u'{0} Abort.'.format(err))
            return
        LOG.debug(u'{0} final sections:\n{1}'.format(
            README_FILENAME, final_sections))

        with open(finalized_readme_path, 'w') as fff:
            self.finalize_readme(readme, final_sections, fff)

        title = uow_cfg['title']
        summary = uow_cfg['summary']
        action = 'Website Update'

        readme_str = open(finalized_readme_path, 'r').read()
        note = Note(person, readme_str, action, title, summary)
        cruise.change._notes.append(note)
        uow.note = note

        # All green. Go!
        LOG.info(u'Committing.'.format())

    def commit_postflight(self, readme, email_path, expocode, title, summary,
                          q_ids, finalized_readme_path, dryrun):
        """Perform actions to notify the community. This is done post-flight.

        """
        readme_str = open(finalized_readme_path, 'r').read()
        note = Note.query().filter(Note.body == readme_str).first()
        return note.id
