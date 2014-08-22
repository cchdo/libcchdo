"""Utilities for operating on the CCHDO data directory"""

import os
from re import search, sub as re_sub, IGNORECASE
from datetime import date
from contextlib import contextmanager
from hashlib import sha256
from shutil import copy2, copytree, rmtree
from email.encoders import encode_base64
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from smtplib import SMTP_SSL
from getpass import getpass
from tempfile import mkdtemp
from subprocess import call as subproc_call
from logging import getLogger


log = getLogger(__name__)


from libcchdo.fns import get_editor, uniquify
from libcchdo.config import (
    get_merger_email, is_env_production, get_merger_initials, get_merger_smtp,
    get_cchdo_email)
from libcchdo.datadir.filenames import (
    README_FILENAME, EXPOCODE_FILENAME, FILE_MANIFEST_FILENAME)


def intersection(self, o):
    return [x for x in self if x in o]


def all_in(a, b):
    return intersection(a, b) == a


DIRECTORIES_TO_TRY = ['.', '/data', '/Volumes/DataArchive/data']


MAIN_DATA_DIRECTORIES = ['co2clivar', 'onetime', 'repeat']


def find_data_directory():
    """Find the data directory.

    Find the data directory by checking for the existence of
    MAIN_DATA_DIRECTORIES as subdirectories and sets it as the cwd.

    """

    for direc in DIRECTORIES_TO_TRY:
        log.debug('Checking for data directory %s' % direc)
        os.chdir(direc)
        if is_data_dir(direc):
            log.info('Selected data directory %s' % direc)
            return direc
    log.error(
        'Unable to find data directory with subdirectories: {0!r}'.format(
        MAIN_DATA_DIRECTORIES))
    return None


def cd_to_data_directory():
    """Change current directory to the data directory."""
    datadir = find_data_directory()
    if datadir:
        os.chdir(datadir)
    else:
        raise EnvironmentError('No data directory found.')


datafile_extensions = ['su.txt', 'hy.txt', 'hy1.csv', 'ct.zip', 'ct1.zip',
                       'nc_hyd.zip', 'nc_ctd.zip']


def has_data_files(path):
    """Return whether the given path has datafiles in it."""
    def filename_has_any_extensions(filename, extensions):
        return any(filename.endswith(ext) for ext in extensions)

    return any(filename_has_any_extensions(fname, datafile_extensions)
        for fname in os.listdir(path))
    

def is_data_dir(path):
    """Determine if the given path is a data directory.

    """
    return all_in(MAIN_DATA_DIRECTORIES, os.listdir(path))


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


allowable_oceans = ['arctic', 'atlantic', 'pacific', 'indian', 'southern']


_blacklisted_dirnames = [
    'WORK', 'work', 'TMP', 'tmp',
    'original', 'ORIG', 'ORIGINAL',
    'Queue',
    '.svn',
    'CTD', 'BOT',
    'old', 'prnt_dir'
]


_blacklisted_dirname_regexps = ['temp', 'tmp', 'holder', 'removed', 'moved']


def do_for_cruise_directories(operation):
  """Call operation for every cruise directory.

  Traverses the cwd as the CCHDO data directory and calls
  operation(fullpath_to_dir, dirs, files) for each cruise directory.
  """
  # TODO deprecated
  from libcchdo.datadir.store import check_is_legacy
  if not check_is_legacy():
      raise NotImplementedError(
          u'Directory traversal not available for pycchdo.')

  cd_to_data_directory()

  # Traverse the data directory to find real data directories to operate on
  for root, dirs, files in os.walk('.', topdown=True):
      # Filter out unwanted directories if still traveling down tree
      for dir in dirs[:]:
          if dir in _blacklisted_dirnames:
              dirs.remove(dir)
          else:
              for black in _blacklisted_dirname_regexps:
                  if search(black, dir, IGNORECASE):
                      dirs.remove(dir)
                      break
      # Only operate if this is a cruise directory
      if is_cruise_dir(root):
          operation(root, dirs, files)


def mkdir_ensure(path, mode=0777):
    try:
        os.mkdir(path, mode)
        os.chmod(path, mode)
    except OSError:
        makedirs_ensure(path, mode)


def makedirs_ensure(path, mode=0777):
    try:
        os.makedirs(path, mode)
        os.chmod(path, mode)
    except OSError:
        pass


def copy(src, dst, mode=0664):
    copy2(src, dst)

    dst_path = dst
    if os.path.isdir(dst):
        dst_path = os.path.join(dst, os.path.basename(src))
    os.chmod(dst_path, mode)


@contextmanager
def tempdir(*args, **kwargs):
    """Generate a temporary directory and automatically clean up."""
    temp_dir = mkdtemp(*args, **kwargs)
    try:
        yield temp_dir
    finally:
        rmtree(temp_dir)


def _make_subdir(root, dirname, perms):
    subroot = os.path.join(root, dirname)
    mkdir_ensure(subroot, perms)
    os.chmod(subroot, perms)


def make_subdirs(root, subdirs, perms):
    for subdir in subdirs:
        if type(subdir) is list:
            subdir, subdirs = subdir[0], subdir[1]
            _make_subdir(root, subdir, perms)
            make_subdirs(os.path.join(root, subdir), subdirs, perms)
        else:
            _make_subdir(root, subdir, perms)


def full_datadir_path(cruisedir):
    """Expand a relative cruise directory into the full path leading with /data.

    """
    cruisedir = os.path.abspath(cruisedir)
    return cruisedir[cruisedir.find('/data'):]


DRYRUN_PREFACE = 'DRYRUN'


def dryrun_log_info(msg, dryrun=True):
    """Log info message with dryrun in front if this is a dryrun."""
    if dryrun:
        log.info(u'{0} {1}'.format(DRYRUN_PREFACE, msg))
    else:
        log.info(msg)


class ReadmeEmail(object):
    """"Readme email."""
    def __init__(self, dryrun=True):
        self._email = MIMEMultipart()
        self._email['From'] = get_cchdo_email()
        self.dryrun = dryrun
        if self.dryrun:
            recipients = [get_merger_email()]
        else:
            if not is_env_production():
                log.warn(u'Environment is not production environment! '
                         'Switched email recipients to merger.')
                recipients = [get_merger_email()]
            else:
                recipients = ['cchdo@googlegroups.com']
        self._email['To'] = ', '.join(recipients)

    def set_subject(self, subject):
        if self.dryrun:
            self._email['Subject'] = '{0} {1}'.format(DRYRUN_PREFACE, subject)
        else:
            self._email['Subject'] = subject

    @classmethod
    def generate_body(cls):
        return ''

    def set_body(self, body):
        self._email.attach(MIMEText(body))

    def attach_readme(self, readme_text):
        """Attach readme text as attachment."""
        attachment = MIMEBase('text', 'plain')
        attachment.set_payload(readme_text)
        encode_base64(attachment)
        attachment.add_header(
            'Content-Disposition',
            'attachment; filename="{0}"'.format(README_FILENAME))
        self._email.attach(attachment)

    def send(self, email_path=None):
        """Send the email."""
        email_str = self._email.as_string()
        send_email(
            email_str, self._email['From'], self._email['To'], email_path)


def send_email(email_str, from_addr, to_addr, email_path=None):
    """Attempt to send email using SMTP server.

    email_path - the path to write the email to in case of failure

    """
    smtp = SMTP_SSL(get_merger_smtp())
    try:
        smtp.sendmail(from_addr, to_addr, email_str)
        log.info(u'Sent email from {0} to {1}'.format(from_addr, to_addr))
    except (KeyboardInterrupt, Exception), err:
        log.error(u'Unable to send email.')
        if email_path is not None:
            with open(email_path, 'w') as fff:
                fff.write(email_str)
            log.info(u'Wrote email to {0} to send manually. '
                     'Use hydro datadir email {0}.'.format(email_path))
        raise err
    finally:
        smtp.quit()


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
        log.info(u'Files were added after last fetch:\n{0}'.format(
            list(filesb - filesa)))
    elif len(filesa) > len(filesb):
        log.info(u'Files were removed after last fetch:\n{0}'.format(
            list(filesa - filesb)))
    else:
        if filesa != filesb:
            log.info(
                u'Different files available:\n{0}'.format(filesa ^ filesb))
        else:
            files_changed = []
            for fname in sorted(list(filesa)):
                if sumsa[fname] != sumsb[fname]:
                    files_changed.append(fname)
            log.info(
                u'File contents were changed for:\n{0}'.format(files_changed))
    log.info(u'If the changes do not affect data, you may choose to delete '
        '{0} and re-fetch the UOW before trying to commit again.'.format(
        UOWDirName.online))


def copy_chunked(iii, ooo, chunk=2 ** 10):
    """Copy file-like to file-like in chunks."""
    data = iii.read(chunk)
    while data:
        ooo.write(data)
        data = iii.read(chunk)


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


PERM_STAFF_ONLY_DIR = 0770
PERM_STAFF_ONLY_FILE = 0660


def _chmod_for_work_dir(path):
    """Recursively change modes of UOW subdir for inclusion in working dir."""
    if os.path.isdir(path):
        os.chmod(path, PERM_STAFF_ONLY_DIR)
        for fname in os.listdir(path):
            _chmod_for_work_dir(os.path.join(path, fname))
    else:
        os.chmod(path, PERM_STAFF_ONLY_FILE)


def uow_copy(uow_dir, uow_subdir, work_dir, work_subdir, filename=None):
    """Copy from UOW sub-directory to work sub-directory.

    If filename is specified, copy only that file, otherwise copy the entire
    tree.

    """
    uow_path = os.path.join(uow_dir, uow_subdir)
    work_path = os.path.join(work_dir, work_subdir)
    if filename is None:
        copytree(uow_path, work_path)
        _chmod_for_work_dir(work_path)
    else:
        copy2(os.path.join(uow_path, filename), work_path)
        _chmod_for_work_dir(os.path.join(work_path, filename))


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


def str_to_fs_slug(sss):
    """Convert a possibly evil string into a filesystem safe slug."""
    return re_sub(r'(/|\s)', '-', sss)


def working_dir_name(person=None, title='working', dtime=None, separator='_'):
    if not person:
        person = get_merger_initials()
    if not dtime:
        dtime = date.today()
    dirname = separator.join(
        [dtime.strftime('%Y.%m.%d'), str_to_fs_slug(title), person])
    return dirname


def is_uowdir_effectively_empty(path, subpath):
    """Return whether the directory at path/subpath is effectively empty.

    If a directory only contains .DS_Store or other such files, it is "empty".

    """
    path = os.path.join(path, subpath)
    files = os.listdir(path)
    if files:
        return len(set(files) - set(IGNORED_FILES_CHECKSUM)) == 0
    else:
        return True


def q_from_uow_cfg(uow_cfg):
    """Retrieve the unique queue file infos and ids from the UOW configuration.

    """
    q_infos = uow_cfg.get('q_infos', [])
    q_ids = uniquify([x['q_id'] for x in q_infos])
    return q_infos, q_ids


