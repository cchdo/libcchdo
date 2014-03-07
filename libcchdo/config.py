"""Configuration files.

Editing
=======
A shortcut to edit the current most precedent configuration file is

    $ hydro misc edit_cfg

File order and precedence
=========================
When loading the library, the configuration files will be loaded to obtain
credentials and file locations. The following files will be read, in order,
for the configuration::

/usr/local/etc/libcchdo/libcchdo.cfg
current_working_directory/libcchdo.cfg
~/libcchdo.cfg

The contents of later files will overwrite the contents of earlier files.

Environment
===========
The library can be put into different environments. This causes the
configuration to load a different set of files. The naming convention for these
files is ```environment_libcchdo.cfg```.

In order to change the environment, set the environment variable
```LIBCCHDO_ENV```. When the library is not using the default environment, it
will preface each run with "Running in <your-environment> environment."

A good use of this feature is to have a development environment.

    $ export LIBCCHDO_ENV=dev
    $ hydro
    $ unset LIBCCHDO_ENV

"""
from ConfigParser import (
    SafeConfigParser, Error as ConfigError, NoSectionError,
    DuplicateSectionError, NoOptionError)
from datetime import date
from sys import stdout, stderr
import os
from getpass import getpass

from libcchdo.log import LOG
from libcchdo.util import memoize


_CONFIG_DIR = '.%s' % __package__


# This environment name is the *actual* production environment.
ENVIRONMENT_ENV_VARIABLE = 'LIBCCHDO_ENV'
PRODUCTION_ENVIRONMENT = 'prod'


@memoize
def get_libenv():
    """Return the current operating environment.

    Order of precedence::
    * Environment variable LIBCCHDO_ENV

    Values::
    1. prod - production
    2. dev - development
    3. test - test

    """
    env = os.environ.get(ENVIRONMENT_ENV_VARIABLE, PRODUCTION_ENVIRONMENT)
    if env != PRODUCTION_ENVIRONMENT:
        LOG.info('Running in {0} environment.'.format(env))
    return env


def is_env_production():
    """Return whether the current env is the *actual* production env."""
    return get_libenv() == PRODUCTION_ENVIRONMENT


def _config_filename():
    """Return the configuration file name to be used.

    This changes with the environment name. If the environment name is the 
    *actual* production environment, the name is libcchdo.cfg, otherwise it is
    envname_libcchdo.cfg.

    """
    if is_env_production():
        return '{0}.cfg'.format(__package__)
    env = get_libenv()
    return '{0}_{1}.cfg'.format(env, __package__)


_CONFIG_PATHS = [
    os.path.join('/usr/local/etc', __package__, _config_filename()),
    os.path.join(os.getcwd(), _CONFIG_DIR,  _config_filename()),
    os.path.expanduser(os.path.join('~', _CONFIG_DIR, _config_filename())),
]


_DEFAULT_CONFIG_PATH_INDEX = -1


_CONFIG = SafeConfigParser()
_parsed_files = _CONFIG.read(_CONFIG_PATHS)


def get_config_path():
    if _parsed_files:
        return _parsed_files[-1]
    else:
        config_path = _CONFIG_PATHS[_DEFAULT_CONFIG_PATH_INDEX]
        for path in _CONFIG_PATHS:
            if os.path.exists(path):
                config_path = path
        return os.path.realpath(config_path)


def get_config_dir():
    return os.path.dirname(get_config_path())


def _save_config():
    config_path = get_config_path()

    if not os.path.isdir(os.path.dirname(config_path)):
        try:
            os.makedirs(os.path.dirname(config_path))
        except os.error:
            LOG.error('Unable to write configuration file: %s' % config_path)
            return

    with open(config_path, 'wb') as config_file:
        _CONFIG.write(config_file)


try:
    _CONFIG.get('db', 'cache')
except ConfigError, e:
    if isinstance(e, NoSectionError):
        _CONFIG.add_section('db')
    _CONFIG.set('db', 'cache', os.path.join(get_config_dir(), 'cchdo_data.db'))
    _save_config()


def set_option(section, option, value, batch=False):
    try:
        _CONFIG.add_section(section)
    except DuplicateSectionError:
        pass

    _CONFIG.set(section, option, value)
    if not batch:
        _save_config()


def get_option(section, option, default_function=None):
    """Retrieve the value for section/option.

    If the value is not specified, run default_function to obtain the value and
    save it.

    """
    try:
        return _CONFIG.get(section, option)
    except NoOptionError, e:
        if not default_function:
            raise e
    except NoSectionError, e:
        if not default_function:
            raise e

    val = default_function()
    set_option(section, option, val)
    return val


def _storage_notice():
    return u'Your answer will be saved in {0}.'.format(get_config_path())


def get_input(prompt=None):
    """Get user input.

    If stdout is not a tty, then print the message to stderr and get input
    without writing to stdout.

    """
    if stdout.isatty():
        return raw_input(prompt)
    else:
        if prompt:
            stderr.write(prompt)
        return raw_input()


def get_db_credentials_cchdo():
    def input_cchdo_db_host():
        LOG.info(_storage_notice())
        return get_input(u'Which host is the legacy CCHDO database on? ')

    cfg_db = 'db_cred'
    cfg_host = 'cchdo/host'
    db_host = get_option(cfg_db, cfg_host, input_cchdo_db_host)

    def input_cchdo_db_name():
        LOG.info(_storage_notice())
        return get_input(u'What is the name of the legacy CCHDO database? ')

    cfg_db = 'db_cred'
    cfg_name = 'cchdo/name'
    db_name = get_option(cfg_db, cfg_name, input_cchdo_db_name)

    def input_cchdo_username():
        LOG.info(_storage_notice())
        return get_input(
            u'What is your username for the database {}/{}? '.format(
                db_host, db_name))

    cfg_db = 'db_cred'
    cfg_username = 'cchdo/username'
    username = get_option(cfg_db, cfg_username, input_cchdo_username)

    # Passwords will not be saved.
    cfg_password = 'cchdo/password'
    try:
        password = get_option(cfg_db, cfg_password)
    except ConfigError:
        LOG.info(
            u'To avoid this question, put your password in plain text as '
            '[{0}] {1} in {2}'.format(cfg_db, cfg_password, get_config_path()))
        try:
            password = getpass(
                u'Password for {}@{}/{}: '.format(username, db_host, db_name))
        except EOFError:
            password = None
    return (username, password, db_host, db_name)


def get_website_domain():
    def input_website_domain():
        return get_input(
            u'What is the website domain? {0} '.format(_storage_notice()))
    return get_option('website', 'domain', input_website_domain)


def get_legacy_datadir_host():
    def input_hostname():
        return get_input(
            u'What host does the datadir live on? {0} '.format(
            _storage_notice()))
    return get_option('datadir', 'hostname', input_hostname)
    

def get_merger_division():
    def input_division():
        def get():
            return (get_input('What division do you work for [CCH]? %s ' % \
                              _storage_notice()) or 'CCH').upper()
        input = get()
        while len(input) != 3:
            print 'Your division identifier must be three characters: '
            input = get()
        return input
    return get_option('Merger', 'division', input_division)


def get_merger_institution():
    def input_institution():
        def get():
            return (get_input('What institution do you work for [SIO]? %s ' % \
                              _storage_notice()) or 'SIO').upper()
        input = get()
        while len(input) != 3:
            print 'Your institution identifier must be three characters: '
            input = get()
        return input
    return get_option('Merger', 'institution', input_institution)


def get_merger_smtp():
    def input_smtp():
        return get_input('What is your SMTP server [smtp.ucsd.edu]? %s ' % \
                          _storage_notice()) or 'smtp.ucsd.edu'
    return get_option('Merger', 'smtp', input_smtp)


def get_merger_email():
    def input_email():
        return get_input('What is your email? %s ' % \
                          _storage_notice())
    return get_option('Merger', 'email', input_email)


def get_merger_name_first():
    def input_name_first():
        return get_input('What is your first name (or rough equivalent)? %s ' % \
                          _storage_notice())
    return get_option('Merger', 'name_first', input_name_first)


def get_merger_name_last():
    def input_name_last():
        return get_input('What is your last name (or rough equivalent)? %s ' % \
                          _storage_notice())
    return get_option('Merger', 'name_last', input_name_last)


def get_merger_name():
    def input_name():
        return get_input('What is your name? %s ' % \
                          _storage_notice())
    return get_option('Merger', 'name', input_name)


def get_merger_initials():
    def input_initials():
        return get_input('What are your initials? %s ' % \
                          _storage_notice()).upper()
    return get_option('Merger', 'initials', input_initials)


def get_datadir_hostname():
    def input_datadir_hostname():
        return get_input('What is the host of the data directory? %s ' % _storage_notice())
    return get_option('datadir', 'hostname', input_datadir_hostname)


def get_datadir_root():
    def input_datadir_root():
        return get_input('Where is the root of the data directory? %s ' % _storage_notice())
    return get_option('datadir', 'root', input_datadir_root)


def stamp():
    return '%(date)8s%(institution)3s%(division)3s%(initials)3s' % \
        {'date': date.today().strftime('%Y%m%d'),
         'institution': get_merger_institution(),
         'division': get_merger_division(),
         'initials': get_merger_initials(),
        }
