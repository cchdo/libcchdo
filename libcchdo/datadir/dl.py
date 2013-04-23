"""Abstraction of data directory.

Allow for filesystem access transparently through either FTP or SFTP.

"""
import sys
from errno import EEXIST
from tempfile import NamedTemporaryFile
from datetime import datetime
from os import (
    errno, getuid, lstat, listdir, readlink, mkdir, chmod, chown, utime, unlink,
    link)
from os import (getcwd, chdir, geteuid, getegid, seteuid, setegid)
import os.path
from contextlib import contextmanager
from threading import current_thread
import shutil
import stat

from paramiko import SSHException, SSHClient, AutoAddPolicy

from libcchdo import LOG


@contextmanager
def pushd(dir):
    cwd = getcwd()
    chdir(dir)
    try:
        yield
    except Exception, e:
        LOG.error('Error in pushd')
        LOG.error(e)
    finally:
        chdir(cwd)


@contextmanager
def lock(lock=None):
    name = current_thread().name
    if lock:
        LOG.debug(u'{0} requested\t{1}'.format(name, lock))
        lock.acquire()
        LOG.debug(u'{0} acquired\t{1}'.format(name, lock))
        try:
            yield
        finally:
            lock.release()
            LOG.debug(u'{0} released\t{1}'.format(name, lock))
    else:
        yield


@contextmanager
def su(uid=0, gid=0, su_lock=None):
    """Temporarily switch effective uid and gid to provided values."""
    if getuid() is not 0:
        yield
        return
        
    with lock(su_lock):
        try:
            seuid = geteuid()
            segid = getegid()
            if uid != 0 and seuid != 0:
                seteuid(0)
            setegid(gid)
            seteuid(uid)
        except OSError, e:
            LOG.error(
                u'You must run this program as root because file permissions '
                'need to be set.')
            sys.exit(1)
        try:
            yield
        except Exception, e:
            LOG.error(u'Error while su(%s, %s)' % (uid, gid))
            raise e
        finally:
            if uid != 0:
                seteuid(0)
            setegid(segid)
            seteuid(seuid)


class SSH(object):
    def __init__(self):
        self.ssh = SSHClient()

    def __enter__(self):
        return self.ssh

    def __exit__(self, exc_type, exc_value, traceback):
        if self.ssh:
            self.ssh.close()
        return True

    def connect(self, host, username=None, known_hosts=None, key_file=None):
        LOG.info(u"Connecting via SSH to {0}".format(host))

        if known_hosts is None:
            self.ssh.set_missing_host_key_policy(AutoAddPolicy())
        else:
            try:
                self.ssh.load_host_keys(known_hosts)
            except IOError, e:
                LOG.error(u'Could not load host keys.')
                raise e
        try:
            self.ssh.connect(
                host, username=username, key_filename=key_file)
        except IOError, e:
            LOG.error(u'Need key file {0}'.format(key_file))
            LOG.info(
                "Please generate an SSH key and put the public key in the "
                "remote user's authorized keys. Remember that this will allow "
                "anyone with the generated private key to log in as that user "
                "so BE CAREFUL.")
            raise e


class SFTP(SSH):
    def __init__(self):
        super(SFTP, self).__init__()

    def __enter__(self):
        super(SFTP, self).__enter__()
        return self.sftp

    def __exit__(self, exc_type, exc_value, traceback):
        if self.sftp:
            self.sftp.close()
        return super(SFTP, self).__exit__(exc_type, exc_value, traceback)

    def connect(self, host, username=None, known_hosts=None, key_file=None):
        super(SFTP, self).connect(
            host, username, known_hosts, key_file)
        self.sftp = self.ssh.open_sftp()
        

class AFTP(object):
    """Encapsulate the mechanics of downloading.

    """

    def __init__(self, ssh_sftp, dryrun=False, dl_gid=None, local_rewriter=None,
                 su_lock=None):
        """Create an abstract FTP interface.

        sftp = SFTP()
        sftp.connect('host')
        aftp = AFTP(sftp)
        
        """
        self.set_ssh_sftp(ssh_sftp)
        self.dryrun = dryrun
        self.dl_gid = dl_gid
        self.local_rewriter = local_rewriter
        self.su_lock = su_lock

    def __copy__(self):
        return AFTP(
            not self.dryrun, (self.ssh, self.sftp), self.dl_gid,
            self.local_rewriter, self.su_lock)

    def set_ssh_sftp(self, ssh_sftp):
        self.ssh = ssh_sftp.ssh
        self.sftp = ssh_sftp.sftp

    @contextmanager
    def sftp_dl(self, filepath):
        """Download a filepath from the remote server."""
        sftp = self.sftp
        temp = NamedTemporaryFile(delete=False)
        downloaded = temp
        try:
            LOG.info('downloading %s' % filepath)
            if self.dryrun:
                LOG.info('Skipped.')
                downloaded = None
            else:
                sftp.get(filepath, temp.name)
        except IOError, e:
            LOG.warn("Unable to locate file on remote %s: %s" % (filepath, e))
            downloaded = None

        try:
            yield downloaded
        finally:
            try:
                unlink(temp.name)
            except OSError, e:
                LOG.error('Unable to unlink tempfile: %s' % e)

    @contextmanager
    def local_dl(self, filepath):
        """Download a filepath from the local filesystem.

        Arguments:
        hardlink - whether to hard link the file instead of copying

        """
        rewritten_path = self.local_rewriter(filepath)
        LOG.debug(u'rewrite {0} to {1}'.format(filepath, rewritten_path))
        filepath = rewritten_path

        if self.dryrun:
            LOG.info('dryrun downloading {0}'.format(filepath))
            yield None
        else:
            LOG.info('downloading {0}'.format(filepath))
            try:
                with su(su_lock=self.su_lock):
                    downloaded = open(filepath, 'rb')
            except IOError, e:
                LOG.warn(u"Unable to locate file on local {0}:\n{1!r}".format(
                    filepath, e))
                downloaded = None
            try:
                yield downloaded
            finally:
                if downloaded:
                    downloaded.close()

    @contextmanager
    def dl(self, file_path):
        if self.local_rewriter:
            if not self.su_lock:
                LOG.error(
                    u'Unable to find su lock when copying file. Cannot '
                    'continue without risk. Skipping.')
                yield None
                return
            with self.local_dl(file_path) as x:
                yield x
        else:
            with self.sftp_dl(file_path) as x:
                yield x

    def set_stat(self, stat, path):
        try:
            chmod(path, stat.st_mode)
            if self.dl_gid is not None:
                chown(path, stat.st_uid, self.dl_gid)
            utime(path, (stat.st_atime, stat.st_mtime))
        except (OSError, IOError), e:
            LOG.error(u'unable to chmod {0!r}:\n{1!r}'.format(path, e))

    def _dl_dir(self, remotedir, localdir, copy):
        msg = u'dl dir\n\t{0}\n\t{1}'.format(remotedir, localdir)
        if self.dryrun:
            LOG.info(u'dryrun {0}'.format(msg))
        else:
            LOG.info(msg)

        if not self.dryrun:
            remote_dir_stat = self.lstat(remotedir)
            try:
                with su(su_lock=self.su_lock):
                    mkdir(localdir)
                    self.set_stat(remote_dir_stat, localdir)
            except OSError, e:
                if e.errno != EEXIST:
                    LOG.debug(e.errno)
                    LOG.error(u'Unable to create directory {0}:\n{1!r}'.format(
                        os.path.basename(remotedir), e))
                    return

        try:
            listing = self.listdir(remotedir)
        except OSError, e:
            LOG.error(
                u'Unable to list directory {0}: {1!r}'.format(remotedir, e))
            return
        for entry in listing:
            remote_path = os.path.join(remotedir, entry)
            local_path = os.path.join(localdir, entry)

            try:
                remote_stat = self.lstat(remote_path)
            except OSError, e:
                LOG.error(u'Unable to get stat for {0}'.format(remote_path))
                continue

            if stat.S_ISDIR(remote_stat.st_mode):
                self._dl_dir(remote_path, local_path, copy)
            elif not self.dryrun:
                LOG.info(u'dl {0}'.format(remote_path))
                try:
                    copy(remote_path, local_path)
                    with su(su_lock=self.su_lock):
                        self.set_stat(remote_stat, local_path)
                except IOError, e:
                    LOG.warning('unable to copy %s (%s)' % (remote_path, e))

    def sftp_copy_dir(self, remote_path, local_path):
        self.sftp.get(remote_path, local_path)

    def sftp_dl_dir(self, sftp, remotedir, localdir):
        LOG.info(u'sftp copying {0}'.format(remotedir))
        self._dl_dir(remotedir, localdir, self.sftp_copy_dir)

    def local_copy_dir(self, remote_path, local_path, hardlink=False):
        with su(su_lock=self.su_lock):
            if hardlink:
                link(remote_path, local_path)
            else:
                shutil.copy2(remote_path, local_path)

    def local_dl_dir(self, remotedir, localdir, hardlink=False):
        """Download a directory from the local filesystem

        Arguments:
        hardlink - whether to hard link the files in the directory instead of
            copying

        """
        LOG.info(u'locally copying {0}'.format(remotedir))
        self._dl_dir(remotedir, localdir, self.local_copy_dir)

    def dl_dir(self, remote_dir_path, local_dir_path):
#        if not self.su_lock:
#            LOG.error(
#                u'Unable to find su lock when copying directory. Cannot '
#                'continue without risk. Skipping.')
#            return
        if self.local_rewriter:
            self.local_dl_dir(
                self.local_rewriter(remote_dir_path), local_dir_path)
        else:
            self.sftp_dl_dir(self.sftp, remote_dir_path, local_dir_path)

    def lstat(self, path):
        if self.local_rewriter:
            with su(su_lock=self.su_lock):
                return lstat(self.local_rewriter(path))
        else:
            return self.sftp.lstat(path)

    def mtime(self, path):
        return datetime.fromtimestamp(self.lstat(path).st_mtime)

    def listdir(self, dir_path):
        if self.local_rewriter:
            return listdir(self.local_rewriter(dir_path))
        else:
            return self.sftp.listdir(dir_path)

    def readlink(self, path):
        if self.local_rewriter:
            return readlink(self.local_rewriter(path))
        else:
            return self.sftp.readlink(path)
