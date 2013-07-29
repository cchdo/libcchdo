"""Get the version number using git.

http://dcreager.net/2010/02/10/setuptools-git-version-numbers/

"""
from os import chdir, getcwd
from os.path import dirname, abspath, join
from subprocess import check_output, PIPE, CalledProcessError


_here = dirname(abspath(__file__))


_release_version_path = join(_here, 'RELEASE-VERSION.txt')


def get_git_describe():
    """Return the current version from git.

    If the source file is not in a git working directory, return None.

    """
    save = getcwd()
    try:
        chdir(_here)
        return check_output(
            ['git', 'describe', '--always'], stderr=PIPE).rstrip()
    except CalledProcessError:
        return None
    finally:
        chdir(save)


def read_release_version():
    try:
        with open(_release_version_path) as fff:
            return fff.read()
    except (OSError, IOError):
        return None


def write_release_version(version):
    with open(_release_version_path, 'w') as fff:
        return fff.write(version)


def get_git_version():
    release_version = read_release_version()

    version = get_git_describe()

    if version is None:
        version = release_version

    if version is None:
        raise ValueError(u'Cannot find version number')

    if version != release_version:
        write_release_version(version)

    return version


__version__ = get_git_version()
