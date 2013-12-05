import os
from datetime import datetime, date
from contextlib import closing, contextmanager
from urllib2 import HTTPError
from shutil import copy2

from libcchdo import LOG
from libcchdo.db.model import legacy
from libcchdo.db.model.legacy import QueueFile
from libcchdo.config import (
    get_legacy_datadir_host, get_merger_name_first, get_merger_name_last)
from libcchdo.datadir.util import (
    working_dir_name, dryrun_log_info, mkdir_ensure, checksum_dir, copy_chunked,
    DirName, UOWDirName, uow_copy, tempdir, write_file_manifest,
    read_file_manifest, regenerate_file_manifest, is_uowdir_effectively_empty,
    checksum_diff_summary)
from libcchdo.datadir.dl import AFTP, SFTP
from libcchdo.datadir.filenames import README_FILENAME


def _queuefile_info(qfile):
    return {
        'filename': qfile.unprocessed_input,
        'submitted_by': qfile.contact,
        'date': qfile.date_received,
        'data_type': qfile.parameters,
        'q_id': qfile.id,
        'submission_id': qfile.submission_id,
        'expocode': qfile.expocode,
    }


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


def _get_file_manifest(uow_dir, work_dir):
    """Read or regenerate the file manifest to put online for a UOW.

    file_manifest is the list of all files that should be online. It is composed
    of those files that are already online, minus those that should not be
    online, plus the files that are new.

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
    pass


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

    def as_received_unmerged_list(self):
        """Return a list of dictionaries representing files that are not merged.

        """
        with closing(legacy.session()) as sesh:
            unmerged_qfs = sesh.query(QueueFile).\
                filter(QueueFile.merged == 0).all()
            qfis = []
            for qf in unmerged_qfs:
                qfi = _queuefile_info(qf)
                del qfi['date']
                qfi['filename'] = os.path.basename(qfi['filename'])
                qfis.append(qfi)
            return qfis

    def as_received(self, *ids):
        with closing(legacy.session()) as sesh:
            try:
                ids = map(int, ids)
            except ValueError:
                ids = []
            qfs = sesh.query(QueueFile).filter(QueueFile.id.in_(ids)).all()
            for qf in qfs:
                yield qf

    def as_received_infos(self, *ids):
        qfis = []
        for qf in self.as_received(*ids):
            qfis.append(_queuefile_info(qf))
        return qfis

    def fetch_as_received(self, local_path, *ids):
        """Copy the referenced as-received files into the directory.

        """
        qf_info = []
        for qf in self.as_received(*ids):
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
                    copy_chunked(fff, ooo)

            qfi = _queuefile_info(qf)
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
                            copy_chunked(fff, ooo)
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

    def mark_merged(self, session, q_ids):
        for qid in q_ids:
            qf = session.query(QueueFile).filter(QueueFile.id == qid).first()
            if qf.is_merged():
                LOG.warn(u'QueueFile {0} is already merged.'.format(qf.id))
            qf.date_merged = date.today()
            qf.set_merged()

    def add_readme_history_note(self, session, readme, expocode, title, summary,
                                action='Website Update'):
        """Add history note for the given readme notes."""
        cruise = session.query(legacy.Cruise).\
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

        session.add(event)
        session.flush()
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

    def commit(self, readme, person, dir_perms):
        """Perform actions needed to put files in work dir and online."""
        # XXX Datadir specific (Work directory is not used in pycchdo. instead
        # need to do a lot of moving files around)
        cruise_dir = self._cruise_dir(readme.uow_cfg['expocode'])

        # Prepare a working directory locally to be uploaded
        # Make sure the UOW doesn't already exist.
        work_dir_base = working_dir_name(person, title=readme.uow_cfg['title'])
        try:
            assert self.cruise_original_dir is not None
        except AssertionError:
            raise AssertionError(
                u'check_cruise_exists should be called before committing')
        remote_work_path = os.path.join(self.cruise_original_dir, work_dir_base)
        if self.aftp.isdir(remote_work_path):
            LOG.error(u'Work directory {work_dir} already exists on '
                      '{host}. Abort.'.format(
                work_dir=remote_work_path, host=self.sftp_host))
            raise ValueError()

        with tempdir(dir='/tmp') as temp_dir:
            work_dir = os.path.join(temp_dir, work_dir_base)
            mkdir_ensure(work_dir, dir_perms)

            # Copy UOW contents into the local working directory
            _copy_uow_dirs_into_work_dir(readme.uow_dir, work_dir)
            try:
                file_sets = _get_file_manifest(readme.uow_dir, work_dir)
                (new_files, removed_files, overwritten_files, unchanged_files,
                 missing_tgo_files) = _copy_uow_online_into_work_dir(
                    readme.uow_dir, work_dir, dir_perms, *file_sets)
                updated_files = new_files | overwritten_files
            except ValueError, err:
                LOG.error(err)
                raise err

            # Finalize the readme file. This means adding the conversion,
            # directories, and updated manifests

            # Calculate remote work path to use in README
            try:
                finalize_sections = u'\n'.join(
                    readme.finalize_sections(
                        remote_work_path, cruise_dir, list(updated_files)))
            except ValueError, err:
                LOG.error(u'{0} Abort.'.format(err))
                return
            LOG.debug(u'{0} final sections:\n{1}'.format(
                README_FILENAME, finalize_sections))

            # Clean out -UOW- replacement lines from README
            work_readme_path = os.path.join(work_dir, README_FILENAME)
            uow_readme_path = os.path.join(readme.uow_dir, README_FILENAME)
            with open(uow_readme_path) as iii:
                with open(work_readme_path, 'w') as ooo:
                    for line in iii:
                        if line.startswith('.. -UOW-'):
                            continue
                        ooo.write(line)
                    ooo.write(finalize_sections)
            finalized_readme_path = os.path.join(
                readme.uow_dir, '00_README.finalized.txt')
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
# TODO Is there any way we can recover at this point? Some updated files may
# have been overwritten by now
# Other than making this loop atomic, I don't see how.
        return finalized_readme_path

    def add_processing_note(self, readme, expocode, title, summary, q_ids,
                            dryrun=True):
        """Record processing history note and mark queue files merged.

        """
        with closing(legacy.session()) as session:
            note_id = self.add_readme_history_note(
                session, readme, expocode, title, summary)
            self.mark_merged(session, q_ids)

            if dryrun:
                dryrun_log_info(
                    u'rolled back history note and merged statuses', dryrun)
                session.rollback()
            else:
                session.commit()
        return note_id
