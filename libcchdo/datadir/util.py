"""Utilities for operating on the CCHDO data directory"""

import re
import os
from shutil import copy2
from email.encoders import encode_base64
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from smtplib import SMTP_SSL
from getpass import getpass

from libcchdo import LOG
from libcchdo.config import get_merger_email, is_env_production
from libcchdo.datadir.filenames import README_FILENAME, EXPOCODE_FILENAME


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
        LOG.debug('Checking for data directory %s' % direc)
        os.chdir(direc)
        if is_data_dir(direc):
            LOG.info('Selected data directory %s' % direc)
            return direc
    LOG.error(
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
    return (all_in(MAIN_DATA_DIRECTORIES, os.listdir(path)) or
            has_data_files(path))


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
  cd_to_data_directory()

  # Traverse the data directory to find real data directories to operate on
  for root, dirs, files in os.walk('.', topdown=True):
      # Filter out unwanted directories if still traveling down tree
      for dir in dirs[:]:
          if dir in _blacklisted_dirnames:
              dirs.remove(dir)
          else:
              for black in _blacklisted_dirname_regexps:
                  if re.search(black, dir, re.IGNORECASE):
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


class ReadmeEmail(object):
    """"Readme email."""
    def __init__(self, dryrun=True):
        self._email = MIMEMultipart()
        self._email['From'] = get_merger_email()
        self.dryrun = dryrun
        if self.dryrun:
            recipients = [get_merger_email()]
        else:
            if not is_env_production():
                LOG.warn(u'Environment is not production environment! '
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

    def generate_body(self):
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

    def send(self, email_path):
        """Send the email."""
        email_str = self._email.as_string()
        send_email(
            email_str, self._email['From'], self._email['To'], email_path)


def send_email(email_str, from_addr, to_addr, email_path):
    smtp = SMTP_SSL('smtp.ucsd.edu')
    try:
        smtp_pass = ''
        while not smtp_pass:
            smtp_pass = getpass(
                u'Please enter your UCSD email password to send '
                'notification email to {0}: '.format(to_addr))
        smtp.login(get_merger_email(), smtp_pass)
        smtp.sendmail(from_addr, to_addr, email_str)
        LOG.info(u'Sent email.')
    except (KeyboardInterrupt, Exception), err:
        LOG.error(u'Unable to send email.')
        with open(email_path, 'w') as fff:
            fff.write(email_str)
        LOG.info(u'Wrote email to {0} to send manually. '
                 'Use hydro datadir email {0}.'.format(email_path))
        raise err
    smtp.quit()
