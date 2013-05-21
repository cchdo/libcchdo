"""Utilities for processing data files and putting them online.

Aims to automate many data directory tasks with an eye on the obsolecense of the
datadir in favor of the website.

"""
import os
import os.path
from datetime import datetime, date
from shutil import copy2, copytree, rmtree
from contextlib import closing, contextmanager
from re import search, sub as re_sub
from urllib2 import urlopen, HTTPError
from tempfile import mkdtemp
from json import load as json_load, dump as json_dump, loads
from subprocess import call as subproc_call
from smtplib import SMTP_SSL
from hashlib import sha256
from getpass import getpass
from collections import OrderedDict

from email.encoders import encode_base64
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText

from docutils.utils import SystemMessage
from docutils.core import publish_string

from libcchdo import LOG
from libcchdo.serve import SimpleHTTPServer
from libcchdo.bb import BB
from libcchdo.fns import file_extensions, guess_file_type, uniquify, get_editor
from libcchdo.datadir.util import mkdir_ensure, make_subdirs
from libcchdo.db.model import legacy
from libcchdo.db.model.legacy import QueueFile
from libcchdo.config import (
    get_legacy_datadir_host, 
    get_merger_initials, get_merger_name_first, get_merger_name_last,
    get_merger_name, get_merger_email)
from libcchdo.formats.google_wire import DefaultJSONSerializer
from libcchdo.datadir.dl import AFTP, SFTP
from libcchdo.datadir.filenames import (
    EXPOCODE_FILENAME, README_FILENAME, PROCESSING_EMAIL_FILENAME,
    UOW_CFG_FILENAME, FILE_MANIFEST_FILENAME, README_TEMPLATE_FILENAME)


def str_to_fs_slug(sss):
    """Convert a possibly evil string into a filesystem safe slug."""
    return re_sub('(/|\s)', '-', sss)


class DirName(object):
    online = 'online'
    original = 'originals'
    submission = 'submission'
    processing = 'processing'
    tgo = 'to_go_online'


class UOWDirName(DirName):
    online = '1.online'
    original = '2.original'
    submission = '3.submission'
    processing = '4.processing'
    tgo = '5.to_go_online'


def working_dir_path(basepath, person, title='working', dt=None, separator='_'):
    if not dt:
        dt = date.today()
    dirname = separator.join(
        [dt.strftime('%Y.%m.%d'), str_to_fs_slug(title), person])
    return os.path.join(basepath, dirname)


def populate_dir(dirpath, files, subdirs, dir_perms=0700, file_perms=0600):
    """Create subdirectories and files in directory."""
    for fname in files:
        fpath = os.path.join(dirpath, fname)
        try:
            os.chmod(fpath, file_perms)
        except OSError:
            pass
        with file(fpath, 'a'):
            os.utime(fpath, None)
            os.chmod(fpath, file_perms)
    make_subdirs(dirpath, subdirs, dir_perms)


def processing_subdir(name, processing_subdirs=False):
    """Generate subdirectory entry for populating."""
    if processing_subdirs:
        return [name, ['exchange', 'woce', 'netcdf']]
    else:
        return name


def populate_working_dir(dirpath, dir_perms=0770, file_perms=0660,
                         processing_subdirs=False):
    files = [README_FILENAME]
    subdirs = [
        'submission',
        'to_go_online',
        'originals',
    ]
    subdirs.append(processing_subdir('processing', processing_subdirs))
    populate_dir(dirpath, files, subdirs, dir_perms, file_perms)


def mkdir_working(basepath, person, title='working', dt=None, separator='_',
                  processing_subdirs=False):
    """Create a working directory for data versioning.

    processing_subdirs - (optional) whether to generate subdirectories in the
        processing directory.

    """
    dirpath = working_dir_path(basepath, person, title, dt, separator)
    dir_perms = 0770
    mkdir_ensure(dirpath, dir_perms)
    populate_working_dir(dirpath, dir_perms)
    return dirpath


def write_readme_template(template_path):
    """Write the readme template to the given path."""
    template = get_email_template()
    with open(template_path, 'w') as fff:
        fff.write(template.encode('utf8'))


def mkdir_uow(basepath, title, summary, ids, separator='_',
              processing_subdirs=False, dl_originals=True):
    """Create a Unit of Work directory for data work.

    This directory includes the currently online files, submission files, and
    places to put processing, and final files.

    processing_subdirs - (optional) whether to generate subdirectories in the
        processing directory.
    dl_originals - (optional) whether to download the originals directory.

    """
    from libcchdo.datadir.readme import ProcessingReadme

    # Check that all files referenced have the same cruise.
    qfis = _legacy_as_received_infos(*ids)
    expocodes = uniquify([qf['expocode'] for qf in qfis])
    if len(expocodes) > 1:
        LOG.warn(
            u'As-received files do not have the same cruise.\n{0}'.format(
            ', '.join(expocodes)))
        expocode = expocodes[0]
        LOG.info(u'Picked the first cruise as the UOW cruise: {0}'.format(
            expocode))
    elif len(expocodes) == 1:
        expocode = expocodes[0]
    else:
        LOG.error(
            u'None of the as-received files are attached to a cruise. This '
            'must be corrected in the database.')
        return

    dirname = separator.join(
        ['uow', expocode, str_to_fs_slug(title), '-'.join(map(str, ids))])
    dirpath = os.path.join(basepath, dirname)

    dir_perms = 0770
    file_perms = 0660

    mkdir_ensure(dirpath, dir_perms)

    files = [README_FILENAME]
    subdirs = [
        UOWDirName.online,
        UOWDirName.original,
        UOWDirName.submission,
        UOWDirName.tgo,
    ]
    subdirs.append(processing_subdir(UOWDirName.processing, processing_subdirs))
    populate_dir(dirpath, files, subdirs, dir_perms, file_perms)

    sftp_host = get_legacy_datadir_host()
    sftp = SFTP()
    sftp.connect(sftp_host)
    aftp = AFTP(sftp)

    qfis = fetch_as_received(
        aftp, os.path.join(dirpath, UOWDirName.submission), *ids)
    fetch_online(aftp, os.path.join(dirpath, UOWDirName.online), expocode)
    if dl_originals:
        fetch_originals(
            aftp, os.path.join(dirpath, UOWDirName.original), expocode)

    # Write UOW configuration
    uow_cfg = OrderedDict([
        ['alias', ''],
        ['expocode', expocode],
        ['data_types_summary', ''],
        ['params', ''],
        ['title', title],
        ['q_infos', qfis],
        ['summary', summary],
        ['conversions', []],
        ['conversions_checked', False],
    ])
    write_uow_cfg(os.path.join(dirpath, UOW_CFG_FILENAME), uow_cfg)

    write_readme_template(os.path.join(dirpath, README_TEMPLATE_FILENAME))

    # Generate a preliminary readme file using the UOW configuration.
    readme = ProcessingReadme(dirpath)
    with open(os.path.join(dirpath, README_FILENAME), 'w') as fff:
        fff.write(unicode(readme))

    return dirpath


def write_uow_cfg(path, uow_cfg):
    """Write out a UOW configuration file given the dictionary."""
    try:
        with open(path, 'w') as fff:
            json_dump(uow_cfg, fff, cls=DefaultJSONSerializer, indent=2)
    except IOError, e:
        LOG.error(u'Unable to write {0}'.format(UOW_CFG_FILENAME))
        LOG.info(
            u'You can write your own using this dict {0!r}'.format(uow_cfg))


def read_uow_cfg(path):
    with open(path) as fff:
        return json_load(fff)


def write_file_manifest(uow_dir, online_files, tgo_files):
    manifest_path = os.path.join(uow_dir, FILE_MANIFEST_FILENAME)
    with open(manifest_path, 'w') as fff:
        fff.write('# online\n')
        fff.write('\n'.join(online_files) + '\n')
        fff.write('# to go online\n')
        fff.write('\n'.join(tgo_files) + '\n')
        fff.write(MANIFEST_INSTRUCTIONS)
    return manifest_path


def read_file_manifest(uow_dir):
    file_manifest = []
    with open(os.path.join(uow_dir, FILE_MANIFEST_FILENAME)) as fff:
        for line in fff:
            if line.startswith('#'):
                continue
            file_manifest.append(line.strip())
    return file_manifest


MANIFEST_INSTRUCTIONS = """\
# File manifest for commit
#
# This should be a list of the files that belong in the cruise directory after
# the commit. Please delete any file names that should be removed.
#
# online = files currently online
# to go online = new files that were added
#
# If you remove everything, the commit will be aborted.
# To start over, delete this file and re-run commit.
"""


def regenerate_file_manifest(uow_dir, online_files, tgo_files):
    manifest_path = write_file_manifest(uow_dir, online_files, tgo_files)
    subproc_call([get_editor(), manifest_path])
    return read_file_manifest(uow_dir)


def uow_copy(uow_dir, uow_subdir, work_dir, work_subdir, filename=None):
    """Copy from UOW sub-directory to work sub-directory.

    If filename is specified, copy only that file, otherwise copy the entire
    tree.

    """
    if filename is None:
        copytree(os.path.join(uow_dir, uow_subdir),
                 os.path.join(work_dir, work_subdir))
    else:
        copy2(os.path.join(uow_dir, uow_subdir, filename),
              os.path.join(work_dir, work_subdir))


IGNORED_FILES_CHECKSUM = ['.DS_Store']


def checksum_dir(dir_path, recurse=True):
    """Return a tuple of the checksums.

    The first checksum is of the entire directory.
    The second is a dictionary mapping each file to its own checksum.

    Each file is added to the checksum as well.

    """
    checksum = sha256()
    file_sums = {}
    for path, names, fnames in os.walk(dir_path):
        if not recurse:
            del names[:]
        for fname in fnames:
            if fname in IGNORED_FILES_CHECKSUM:
                continue
            fpath = os.path.join(path, fname)
            sumpath = fpath.replace(dir_path, '')
            checksum.update(sumpath)
            if os.path.isdir(fpath):
                continue
            fsum = sha256()
            with open(fpath) as fff:
                for lll in fff:
                    checksum.update(lll)
                    fsum.update(lll)
            file_sums[fname] = fsum.digest()
    return (checksum.digest(), file_sums)


def checksum_diff_summary(sumsa, sumsb):
    """Log a summary of what is different using given file checksums."""
    filesa = set(sumsa.keys())
    filesb = set(sumsb.keys())

    if len(filesa) < len(filesb):
        LOG.info(u'Files were added after last fetch:\n{0}'.format(
            list(filesb - filesa)))
    elif len(filesa) > len(filesb):
        LOG.info(u'Files were removed after last fetch:\n{0}'.format(
            list(filesa - filesb)))
    else:
        files_changed = []
        for fname in sorted(list(filesa)):
            if sumsa[fname] != sumsb[fname]:
                files_changed.append(fname)
        LOG.info(u'File contents were changed for:\n{0}'.format(files_changed))
    LOG.info(u'If the changes do not affect data, you may choose to delete '
        '{0} and re-fetch the UOW before trying to commit again.'.format(
        UOWDirName.online))


@contextmanager
def tempdir(*args, **kwargs):
    """Generate a temporary directory and automatically clean up."""
    temp_dir = mkdtemp(*args, **kwargs)
    try:
        yield temp_dir
    finally:
        rmtree(temp_dir)


def check_online_checksums(aftp, uow_dir, expocode):
    """A crude check to make sure no online files have changed since UOW fetch.

    This is performed before comitting a UOW in case of multiple mergers working
    on the same cruise.

    """
    fetch_dir = os.path.join(uow_dir, UOWDirName.online)
    fetch_checksum, fetch_file_checksums = checksum_dir(fetch_dir)
    with tempdir() as temp_dir:
        fetch_online(aftp, temp_dir, expocode)
        current_checksum, current_file_checksums = checksum_dir(temp_dir)
        if fetch_checksum != current_checksum:
            checksum_diff_summary(fetch_file_checksums, current_file_checksums)
            raise ValueError(
                u'Cruise online files have changed since the last UOW fetch!')


def _copy_uow_dirs_into_work_dir(uow_dir, work_dir, dir_perms):
    """Copy a UOW's processing contents into a working directory.

    This only handles the subdirectories and leaves the readme file for later.

    """
    uow_copy(uow_dir, UOWDirName.processing, work_dir, DirName.processing)
    uow_copy(uow_dir, UOWDirName.submission, work_dir, DirName.submission)
    uow_copy(uow_dir, UOWDirName.tgo, work_dir, DirName.tgo)


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


def dryrun_log_info(msg, dryrun=True):
    """Log info message with dryrun in front if this is a dryrun."""
    if dryrun:
        LOG.info(u'{0} {1}'.format(DRYRUN_PREFACE, msg))
    else:
        LOG.info(msg)


def uow_commit(uow_dir, person=None, confirm_html=True, dryrun=True):
    """Commit a UOW directory to the cruise data history.

    write 00_README.txt header, submissions, parameter list, conversion,
    directories, and updated manifest

    """
    from libcchdo.datadir.readme import ProcessingReadme
    dryrun_log_info(u'Comitting UOW directory {0}'.format(uow_dir), dryrun)

    dir_perms = 0775

    # pre-flight checks
    # Make sure merger likes readme rendering
    readme_path = os.path.join(uow_dir, README_FILENAME)
    if not is_processing_readme_render_ok(
            readme_path, confirm_html=confirm_html):
        LOG.error(u'README is not valid reST or merger rejected. Stop.')
        return

    # Make sure there is a configuration
    try:
        readme = ProcessingReadme(uow_dir)
        uow_cfg = readme.uow_cfg
    except IOError:
        LOG.error(
            u'Cannot continue without {0}. (Are you sure {1} is a UOW '
            'directory?)'.format(UOW_CFG_FILENAME, uow_dir))
        return
    except ValueError, err:
        LOG.error(
            u'Unable to read {0}. The JSON is invalid. Abort.\n{1!r}'.format(
            UOW_CFG_FILENAME, err))
        return
    try:
        expocode = uow_cfg['expocode']
    except KeyError:
        LOG.error(u'UOW configuration is missing "expocode". Abort.')
        return
    try:
        title = uow_cfg['title']
    except KeyError:
        LOG.error(u'UOW configuration is missing "title". Abort.')
        return

    # Get remote directory
    sftp_host = get_legacy_datadir_host()
    sftp = SFTP()
    sftp.connect(sftp_host)
    aftp = AFTP(sftp, dryrun=dryrun)

    with _legacy_cruise_directory(expocode) as doc:
        cruise_dir = doc.FileName
    cruise_original_dir = os.path.join(cruise_dir, 'original')

    # Ensure cruise original directory exists...
    try:
        aftp.mkdir(cruise_original_dir, dir_perms)
    except (IOError, OSError), e:
        pass

    # Abort if online files are different from fetched online files
    try:
        saved_dryrun = aftp.dryrun
        aftp.dryrun = False
        check_online_checksums(aftp, uow_dir, expocode)
        aftp.dryrun = saved_dryrun
    except ValueError, e:
        LOG.error(u'{0} Abort.'.format(e))
        return

    uow_readme_path = os.path.join(uow_dir, README_FILENAME)
    finalized_readme_path = uow_readme_path

    # Create the working directory locally in prep to be uploaded
    with tempdir(dir='/tmp') as temp_dir:
        if person is None:
            initials = get_merger_initials()
        else:
            initials = person
        work_dir = working_dir_path(temp_dir, initials, title=title)
        working_dir_name = os.path.basename(work_dir)
        # Abort if work directory already exists
        if working_dir_name in aftp.listdir(cruise_original_dir):
            LOG.error(u'Work directory {work_dir} already exists in '
                      '{host}{path}. Abort.'.format(
                work_dir=work_dir, host=sftp_host, path=cruise_original_dir))
            return
        mkdir_ensure(work_dir, dir_perms)

        # Copy UOW contents into the local working directory
        _copy_uow_dirs_into_work_dir(uow_dir, work_dir, dir_perms)
        try:
            file_sets = _get_file_manifest(uow_dir, work_dir)
            (new_files, removed_files, overwritten_files, unchanged_files,
             missing_tgo_files) = _copy_uow_online_into_work_dir(
                uow_dir, work_dir, dir_perms, *file_sets)
            updated_files = new_files | overwritten_files
        except ValueError, e:
            LOG.error(e)
            return

        # Calculate remote path
        remote_path = os.path.join(cruise_original_dir, working_dir_name)

        # Finalize the readme file. This means adding the conversion,
        # directories, and updated manifests
        work_readme_path = os.path.join(work_dir, README_FILENAME)

        try:
            finalize_sections = u'\n'.join(
                readme.finalize_sections(
                    remote_path, cruise_dir, list(updated_files)))
        except ValueError, err:
            LOG.error(u'{0} Abort.'.format(err))
            return
        LOG.debug(u'{0} final sections:\n{1}'.format(
            README_FILENAME, finalize_sections))
        finalized_readme_path = os.path.join(uow_dir, '00_README.finalized.txt')

        with open(uow_readme_path) as iii:
            with open(work_readme_path, 'w') as ooo:
                for line in iii:
                    if line.startswith('.. -UOW-'):
                        continue
                    ooo.write(line)
                ooo.write(finalize_sections)
        copy2(work_readme_path, finalized_readme_path)

        # All green. Go!
        LOG.info(u'Committing to {0}:{1}'.format(sftp_host, remote_path))

        # upload the working directory
        aftp.up_dir(work_dir, remote_path)

    # Update the online files. It is ok to overwrite/delete at this point as
    # those affected have already been written to originals.
    for fname in removed_files:
        aftp.remove(os.path.join(cruise_dir, fname))
    LOG.info('unchanged:')
    for fname in unchanged_files:
        aftp.up(
            os.path.join(uow_dir, UOWDirName.online, fname),
            os.path.join(cruise_dir, fname))
    LOG.info('new:')
    for fname in updated_files:
        aftp.up(
            os.path.join(uow_dir, UOWDirName.tgo, fname),
            os.path.join(cruise_dir, fname))

    # Flight complete
    dryrun_log_info(
        u'Data file commit completed successfully. Writing history and '
       'notifications.', dryrun)

    # Post-flight
    add_processing_note(
        finalized_readme_path,
        os.path.join(uow_dir, PROCESSING_EMAIL_FILENAME),
        uow_cfg, dryrun)

    dryrun_log_info(u'UOW commit completed successfully.', dryrun)


def copy_replaced(filename, curr_date, separator='_'):
    """Move a replaced file to its special name.

    """
    dirname, filename = os.path.split(filename)
    dirname = os.path.join(os.getcwd(), dirname)
    file_type = guess_file_type(filename)
    if file_type is None:
        LOG.error(
            u'File {0} does not have a recognizable file extension.'.format(
            filename))
        return 1

    exts = file_extensions[file_type]
    sorted_exts = sorted(
        zip(exts, map(len, exts)), key=lambda x: x[1], reverse=True)
    exts = [x[0] for x in sorted_exts]

    basename = filename
    extension = None
    for ext in exts:
        if filename.endswith(ext):
            basename = filename[:-len(ext)]
            extension = ext

    replaced_str = separator.join(
        ['', 'rplcd', curr_date.strftime('%Y%m%d'), ''])
    extra_extension = extension.split('.')[0]

    new_name = os.path.relpath(os.path.join(dirname, 'original', ''.join(
        [basename, extra_extension, replaced_str, extension])))

    print filename, '->', new_name
    accepted = raw_input('copy? (y/[n]) ')
    if accepted == 'y':
        try:
            copy2(filename, new_name)
        except OSError, e:
            LOG.error(u'Could not move file: {0}'.format(e))
            return 1


def is_cruise_dir(path):
    """Determine if the given path is a cruise directory.

    Basically, if an 'ExpoCode' is present.

    """
    return EXPOCODE_FILENAME in os.listdir(path)


def is_working_dir(path):
    """Determine if the given path is a working directory.

    Basically, if an '00_README.txt' is present.

    """
    return README_FILENAME in os.listdir(path)


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


def _queuefile_info(qf):
    return {
        'filename': qf.unprocessed_input,
        'submitted_by': qf.contact,
        'date': qf.date_received,
        'data_type': qf.parameters,
        'q_id': qf.id,
        'submission_id': qf.submission_id,
        'expocode': qf.expocode,
    }


def _legacy_as_received_unmerged():
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


def _legacy_as_received(*ids):
    with closing(legacy.session()) as sesh:
        qfs = sesh.query(QueueFile).filter(QueueFile.id.in_(ids)).all()
        for qf in qfs:
            yield qf
    

def _legacy_as_received_infos(*ids):
    qfis = []
    for qf in _legacy_as_received(*ids):
        qfis.append(_queuefile_info(qf))
    return qfis


def as_received_infos(*ids):
    return _legacy_as_received_infos(*ids)


def as_received_unmerged_list():
    """Return a list of dictionaries representing files that are not merged.

    """
    return _legacy_as_received_unmerged()


def copy_chunked(iii, ooo, chunk=2 ** 10):
    """Copy file-like to file-like in chunks."""
    data = iii.read(chunk)
    while data:
        ooo.write(data)
        data = iii.read(chunk)


def download_url(url, path):
    with open(path, 'w') as ooo:
        with closing(urlopen(url)) as fff:
            copy_chunked(fff, ooo)
    LOG.info(u'downloaded {0}'.format(url))


def _legacy_fetch_as_received(aftp, local_path, ids):
    """Download the as-received files into path.

    """
    qf_info = []
    for qf in _legacy_as_received(*ids):
        if qf.is_merged():
            LOG.info(u'QueueFile {0} is marked already merged'.format(qf.id))
        elif qf.is_hidden():
            LOG.info(u'QueueFile {0} is marked hidden'.format(qf.id))
        path = qf.unprocessed_input
        filename = os.path.basename(path)

        submission_subdir = os.path.join(local_path, str(qf.id))
        mkdir_ensure(submission_subdir, 0775)
        submission_path = os.path.join(submission_subdir, filename)

        with aftp.dl(path) as fff:
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


def _check_working_dir(working_dir):
    if not is_working_dir(working_dir):
        raise ValueError(
            u'Not a working directory {0!r}'.format(
            working_dir))


def fetch_as_received(aftp, path, *ids):
    """Copy the referenced as-received files into the directory.

    """
    return _legacy_fetch_as_received(aftp, path, ids)


IGNORED_FILES = ['Queue', 'original']


@contextmanager
def _legacy_cruise_directory(expocode):
    with closing(legacy.session()) as sesh:
        q_docs = sesh.query(legacy.Document).\
            filter(legacy.Document.ExpoCode == expocode).\
            filter(legacy.Document.FileType == 'Directory')
        num_docs = q_docs.count()
        if num_docs < 1:
            LOG.error(u'{0} does not have a directory entry.'.format(expocode))
            raise ValueError()
        elif num_docs > 1:
            LOG.error(
                u'{0} has more than one directory entry.'.format(expocode))
            raise ValueError()
        yield q_docs.first()
            

def _legacy_fetch_online(aftp, path, expocode):
    """Download the cruise's online files into path.

    """
    try:
        with _legacy_cruise_directory(expocode) as doc:
            cruise_dir = doc.FileName

        for fname in aftp.listdir(cruise_dir):
            online_path = os.path.join(cruise_dir, fname)
            local_path = os.path.join(path, fname)
            try:
                if aftp.isdir(online_path):
                    continue
                with aftp.dl(online_path) as fff:
                    if not fff:
                        LOG.error(u'Could not download {0}'.format(online_path))
			continue
                    with open(local_path, 'w') as ooo:
                        copy_chunked(fff, ooo)
            except HTTPError, e:
                os.unlink(local_path)
                if fname in IGNORED_FILES:
                    continue
                LOG.error(u'Could not download {0}:\n{1!r}'.format(fname, e))
    except IOError, err:
        LOG.error(u'Unable to list cruise directory {0}'.format(cruise_dir))
    except ValueError:
        pass


def fetch_online(aftp, path, expocode):
    """Copy the referenced cruise's current datafiles into the directory.

    """
    return _legacy_fetch_online(aftp, path, expocode)


def _legacy_fetch_originals(aftp, path, expocode):
    """Download the cruise's original files into path."""
    try:
        with _legacy_cruise_directory(expocode) as doc:
            cruise_dir = doc.FileName
    except ValueError:
        return
    originals_dir = os.path.join(cruise_dir, 'original')
    LOG.info(u'Downloading {0}'.format(originals_dir))

    try:
        aftp.dl_dir(originals_dir, path)
    except IOError:
        LOG.error(u'Unable to download originals directory {0}'.format(path))


def fetch_originals(aftp, path, expocode):
    """Copy the referenced cruise's original datafiles into the directory.

    """
    return _legacy_fetch_originals(aftp, path, expocode)


def get_email_template():
    resp, content = BB.api(
        'GET', '/repositories/ghdc/cchdo/wiki/data_curation_email_templates')
    wiki = loads(content)['data']
    # TODO cut out the template from all the rest
    return wiki


def is_processing_readme_render_ok(readme_path, confirm_html=True):
    """Ensure that the readme file passes the reST compiler and inspection.

    Arguments:
    confirm_html -- (optional) if set, will attempt to display the rendered page
        to the user and ask for go ahead.

    """
    with open(readme_path) as fff:
        readme = fff.read()

    try:
        output = publish_string(readme, writer_name='html')
    except SystemMessage, e:
        LOG.error(u'{0} failed test'.format(README_FILENAME))
        return False

    if confirm_html:
        server = SimpleHTTPServer()
        server.register('/', output)
        server.open_browser()
        accepted = None
        while accepted not in ('y', 'n'):
            server.httpd.handle_request()
            accepted = raw_input('Was the HTML output acceptable? (y/n) ')
            accepted = accepted.lower()
        return accepted == 'y'
    return True


PROCESSING_EMAIL_TEMPLATE = """\
Dear CCHDO,

This is an automated message.

The cruise page for http://cchdo.ucsd.edu/cruise/{expo} was updated by {merger}.

This update includes:

{sub_plural}
{submission_summary}

{q_plural} {q_ids} have been marked as merged.

A history note ({note_id}) has been made for the attached processing notes.

"""


DRYRUN_PREFACE = 'DRYRUN'


def summarize_submission(q_info):
    return '{0}: {1} {2} {3} {4}'.format(
        q_info['submission_id'], q_info['filename'], q_info['submitted_by'],
        q_info['date'], q_info['data_type'], )


def parse_readme(readme, uow_cfg):
    """Parse out salient information from readme file for processing email."""
    title = None
    merger = None
    for line in readme.split('\n'):
        if not title and search('processing', line):
            title = line
        elif not merger and search('^\w\s\w+$', line):
            merger = line

    subject = title
    if not merger:
        merger = 'unknown'
    matches = search('([A-Za-z0-9_\/]+)\s+processing', title)
    expocode = 'unknown'
    if matches:
        expocode = matches.group(1)

    if expocode == 'unknown':
        expocode = uow_cfg.get('expocode', 'unknown')

    if merger == 'unknown':
        merger = get_merger_name()
    return title, merger, subject, expocode


def processing_email(readme, email_path, uow_cfg, note_id, q_ids, dryrun=True):
    """Send processing completed notification email."""
    if dryrun:
        recipients = [get_merger_email()]
    else:
        recipients = ['cchdo@googlegroups.com']

    title, merger, subject, expocode = parse_readme(readme, uow_cfg)

    q_infos = uow_cfg['q_infos']
    sub_ids = uniquify([x['submission_id'] for x in q_infos])
    sub_plural = 'Submissions'
    q_plural = 'Queue entries'

    if len(sub_ids) == 1:
        sub_plural = 'Submission'
    if len(q_ids) == 1:
        q_plural = 'Queue entry'

    submission_summary = '\n'.join(map(summarize_submission, q_infos))

    body = PROCESSING_EMAIL_TEMPLATE.format(
        expo=expocode, merger=merger, sub_plural=sub_plural,
        submission_summary=submission_summary, q_plural=q_plural,
        q_ids=', '.join(map(str, q_ids)), note_id=note_id)

    email = MIMEMultipart()
    email['From'] = get_merger_email()
    email['To'] = ', '.join(recipients)
    if dryrun:
        email['Subject'] = '{0} {1}'.format(DRYRUN_PREFACE, subject)
    else:
        email['Subject'] = subject

    email.attach(MIMEText(body))

    attachment = MIMEBase('text', 'plain')
    attachment.set_payload(readme)
    encode_base64(attachment)
    attachment.add_header(
        'Content-Disposition',
        'attachment; filename="{0}"'.format(README_FILENAME))
    email.attach(attachment)

    email_str = email.as_string()
    s = SMTP_SSL('smtp.ucsd.edu')
    smtp_pass = ''
    while not smtp_pass:
        smtp_pass = getpass(
            u'Please enter your UCSD email password to send notification '
            'email to {0}: '.format(email['To']))
    try:
        s.login(get_merger_email(), smtp_pass)
        s.sendmail(email['From'], email['To'], email_str)
    except Exception, err:
        with open(email_path, 'w') as fff:
            fff.write(email_str)
        LOG.info(u'Wrote email to {0} to send later.'.format(email_path))
        raise err
    s.quit()


def processing_history(session, readme, uow_cfg):
    """Add history note for the given processing notes."""
    expocode = uow_cfg['expocode']
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
    event.Data_Type = uow_cfg['title']
    event.Action = 'Website Update'
    event.Date_Entered = datetime.now().date()
    try:
        event.Summary = uow_cfg['summary']
    except KeyError:
        LOG.error(u'Please add a summary key to the UOW configuration.')
        LOG.info(u'Typical entries contain file formats updated e.g.\n'
            'Exchange, NetCDF, WOCE files online\n'
            'Exchange & NetCDF files updated\n'
        )
        raise ValueError(u'Missing summary in UOW configuration')
    event.Note = readme

    session.add(event)
    session.flush()
    return event.ID


def mark_merged(session, uow_cfg):
    q_infos = uow_cfg['q_infos']
    q_ids = uniquify([x['q_id'] for x in q_infos])
    for qid in q_ids:
        qf = session.query(legacy.QueueFile).\
            filter(legacy.QueueFile.id == qid).first()
        if qf.is_merged():
            LOG.warn(u'QueueFile {0} is already merged.'.format(qf.id))
    qf.date_merged = date.today()
    qf.set_merged()
    return q_ids


def add_processing_note(readme_path, email_path, uow_cfg, dryrun=True):
    """Record processing history note.

    The current way to do this is to save a history note with the 00_README.txt
    contents, as well as email the CCHDO community.

    """
    try:
        with open(readme_path) as fff:
            readme = fff.read()
    except IOError:
        LOG.error(u'Cannot continue without {0}'.format(README_FILENAME))
        return

    with closing(legacy.session()) as session:
        note_id = processing_history(session, readme, uow_cfg)
        q_ids = mark_merged(session, uow_cfg)

        try:
            processing_email(readme, email_path, uow_cfg, note_id, q_ids, dryrun)
            LOG.info(u'Sent processing email.')
            email_ok = True
        except Exception, err:
            LOG.error(u'Could not send email: {0!r}'.format(err))
            email_ok = False

        if dryrun:
            LOG.info(
                u'{0} rolled back history note and merged statuses'.format(
                DRYRUN_PREFACE))
            session.rollback()
        elif not email_ok:
            LOG.info(u'rolled back history note and merged statuses')
            session.rollback()
        else:
            session.commit()
