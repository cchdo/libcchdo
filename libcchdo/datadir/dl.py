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
from shutil import copy2, copytree
import stat
from stat import S_ISDIR
from errno import ENOENT

from paramiko import SSHException, SSHClient, AutoAddPolicy

from libcchdo import LOG
from libcchdo.datadir.util import mkdir_ensure


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
        #LOG.debug(u'{0} requested\t{1}'.format(name, lock))
        lock.acquire()
        #LOG.debug(u'{0} acquired\t{1}'.format(name, lock))
        try:
            yield
        finally:
            lock.release()
            #LOG.debug(u'{0} released\t{1}'.format(name, lock))
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
            self.ssh_sftp, self.dryrun, self.dl_gid, self.local_rewriter,
            self.su_lock)

    def set_ssh_sftp(self, ssh_sftp):
        self.ssh_sftp = ssh_sftp
        if ssh_sftp is not None:
            self.ssh = ssh_sftp.ssh
            self.sftp = ssh_sftp.sftp

    @contextmanager
    def sftp_dl(self, filepath):
        """Download a filepath from the remote server."""
        sftp = self.sftp
        temp = NamedTemporaryFile(delete=False)
        downloaded = temp
        try:
            if self.dryrun:
                LOG.info('dryrun downloading %s' % filepath)
                downloaded = None
            else:
                LOG.info('downloading %s' % filepath)
                sftp.get(filepath, temp.name)
        except IOError, e:
            LOG.warn(
                u'Unable to locate file on remote {0}\n{1!r}'.format(
                filepath, e))
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

    def sftp_up(self, local_file_path, filepath):
        """Upload a filepath to the remote server."""
        sftp = self.sftp
        if self.dryrun:
            LOG.info(u'dryrun uploading {0} to {1}'.format(
                local_file_path, filepath))
        else:
            LOG.info(u'uploading {0} {1}'.format(local_file_path, filepath))
            sftp.put(local_file_path, filepath)

    def local_up(self, local_file_path, filepath):
        """Upload a filepath to the local filesystem.

        Arguments:
        hardlink - whether to hard link the file instead of copying

        """
        rewritten_path = self.local_rewriter(filepath)
        LOG.debug(u'rewrite {0} to {1}'.format(filepath, rewritten_path))
        filepath = rewritten_path

        if self.dryrun:
            LOG.info('dryrun uploading {0}'.format(filepath))
            return
        else:
            LOG.info('uploading {0}'.format(filepath))
            with su(su_lock=self.su_lock):
                copy2(local_file_path, filepath)
    
    def up(self, local_file_path, file_path, suppress_errors=True):
        if self.local_rewriter:
            try:
                self.local_up(local_file_path, file_path)
            except (OSError, IOError), err:
                LOG.warn(u"Unable to copy to file on local {0}:\n{1!r}".format(
                    file_path, err))
                if not suppress_errors:
                    raise err
        else:
            try:
                self.sftp_up(local_file_path, file_path)
            except (OSError, IOError), err:
                LOG.warn(u'Unable to copy file to remote {0}:\n{1!r}'.format(
                    file_path, err))
                if not suppress_errors:
                    raise err

    def local_up_dir(self, local_dir_path, dir_path):
        """Upload the local_dir_path tree contents into dir_path.

        dir_path should not already exist.

        """
        if self.dryrun:
            LOG.info(u'dryrun upload dir {0} to {1}'.format(
                local_dir_path, dir_path))
        else:
            LOG.info(u'upload dir {0} to {1}'.format(
                local_dir_path, dir_path))
            copytree(local_dir_path, dir_path)

    def sftp_up_dir(self, local_dir_path, dir_path):
        """Upload the local_dir_path contents into the remote dir_path.

        dir_path should not already exist.

        """
        if self.dryrun:
            LOG.info(u'dryrun upload dir {0} to {1}'.format(
                local_dir_path, dir_path))
        else:
            LOG.info(u'upload dir {0} to {1}'.format(
                local_dir_path, dir_path))
            self.sftp.mkdir(dir_path)
            dir_lstat = os.lstat(local_dir_path)
            self.sftp.chmod(dir_path, dir_lstat.st_mode)
            for path, names, fnames in os.walk(local_dir_path):
                for name in names:
                    fpath = os.path.join(path, name)
                    rfpath = fpath.replace(local_dir_path, dir_path)
                    self.sftp.mkdir(rfpath)
                    dir_lstat = os.lstat(fpath)
                    self.sftp.chmod(rfpath, dir_lstat.st_mode)
                for fname in fnames:
                    fpath = os.path.join(path, fname)
                    f_lstat = os.lstat(fpath)
                    rfpath = fpath.replace(local_dir_path, dir_path)
                    if os.path.isdir(fpath):
                        continue
                    else:
                        LOG.debug(u'uploading {0} to {1}'.format(fpath, rfpath))
                        self.sftp.put(fpath, rfpath)
                        self.sftp.chmod(rfpath, f_lstat.st_mode)

    def up_dir(self, local_dir_path, dir_path):
        """Upload directory."""
        if self.local_rewriter:
            self.local_up_dir(local_dir_path, dir_path)
        else:
            self.sftp_up_dir(local_dir_path, dir_path)

    def local_remove(self, path):
        if self.dryrun:
            LOG.info(u'dryrun remove {0}'.format(path))
        else:
            LOG.info(u'remove {0}'.format(path))
            os.unlink(path)

    def sftp_remove(self, path):
        if self.dryrun:
            LOG.info(u'dryrun remove {0}'.format(path))
        else:
            LOG.info(u'remove {0}'.format(path))
            self.sftp.remove(path)

    def remove(self, path):
        """Delete the file at the remote path."""
        if self.local_rewriter:
            self.local_remove(path)
        else:
            self.sftp_remove(path)

    def local_mkdir(self, remote_path, mode=0777):
        mkdir(remote_path, mode)

    def sftp_mkdir(self, remote_path, mode=0777):
        LOG.debug('making directory {0}'.format(remote_path))
        self.sftp.mkdir(remote_path, mode)

    def mkdir(self, remote_path, mode=0777):
        if self.local_rewriter:
            self.local_mkdir(remote_path, mode)
        else:
            self.sftp_mkdir(remote_path, mode)
        
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
                copy2(remote_path, local_path)

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

    def isdir(self, path):
        if self.local_rewriter:
            return os.path.isdir(self.local_rewriter(path))
        else:
            return S_ISDIR(self.sftp.lstat(path).st_mode)

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
