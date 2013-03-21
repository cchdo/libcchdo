import ConfigParser
from ConfigParser import Error
import datetime
import os
import getpass


from . import LOG


_CONFIG_DIR = '.%s' % __package__


_CONFIG_FILE_NAME = '%s.cfg' % __package__


_LEGACY_CONFIG_FILE_NAME = '.%s.cfg' % __package__


_CONFIG_PATHS = [
    os.path.join(os.getcwd(), _CONFIG_DIR,  _CONFIG_FILE_NAME),
    os.path.join(os.getcwd(), _CONFIG_DIR,  _LEGACY_CONFIG_FILE_NAME),
    os.path.expanduser(os.path.join('~', _CONFIG_DIR, _CONFIG_FILE_NAME)),
    os.path.expanduser(os.path.join('~', _CONFIG_DIR, _LEGACY_CONFIG_FILE_NAME)),
]


_DEFAULT_CONFIG_PATH_INDEX = -2


_CONFIG = ConfigParser.SafeConfigParser()
_parsed_files = _CONFIG.read(_CONFIG_PATHS)


def get_config_path():
    if _parsed_files:
        return _parsed_files[0]
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
except ConfigParser.Error, e:
    if isinstance(e, ConfigParser.NoSectionError):
        _CONFIG.add_section('db')
    dir = os.path.dirname(get_config_path())
    _CONFIG.set('db', 'cache', os.path.join(dir, 'cchdo_data.db'))
    _save_config()


def set_option(section, option, value, batch=False):
    try:
        _CONFIG.add_section(section)
    except ConfigParser.DuplicateSectionError:
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
    except ConfigParser.NoOptionError, e:
        if not default_function:
            raise e
    except ConfigParser.NoSectionError, e:
        if not default_function:
            raise e

    val = default_function()
    set_option(section, option, val)
    return val


_STORAGE_NOTICE = \
    "Your answer will be saved in %s." % _CONFIG_PATHS[-1]


def get_db_credentials_cchdo():
    def input_cchdo_db_host():
        LOG.info(_STORAGE_NOTICE)
        return raw_input(u'Which host is the legacy CCHDO database on? ')

    cfg_db = 'db_cred'
    cfg_host = 'cchdo/host'
    db_host = get_option(cfg_db, cfg_host, input_cchdo_db_host)

    def input_cchdo_db_name():
        LOG.info(_STORAGE_NOTICE)
        return raw_input(u'What is the name of the legacy CCHDO database? ')

    cfg_db = 'db_cred'
    cfg_name = 'cchdo/name'
    db_name = get_option(cfg_db, cfg_name, input_cchdo_db_name)

    def input_cchdo_username():
        LOG.info(_STORAGE_NOTICE)
        return raw_input(
            u'What is your username for the database {}/{}? '.format(
                db_host, db_name))

    cfg_db = 'db_cred'
    cfg_username = 'cchdo/username'
    username = get_option(cfg_db, cfg_username, input_cchdo_username)

    # Passwords will not be saved.
    cfg_password = 'cchdo/password'
    try:
        password = get_option(cfg_db, cfg_password)
    except ConfigParser.Error:
        LOG.info(
            u'To avoid this question, put your password in plain text as '
            '[{0}] {1} in {2}'.format(cfg_db, cfg_password, _CONFIG_PATHS[-1]))
        try:
            password = getpass.getpass(
                u'Password for {}@{}/{}: '.format(username, db_host, db_name))
        except EOFError:
            password = None
    return (username, password, db_host, db_name)
    

def get_merger_division():
    def input_division():
        def get():
            return (raw_input('What division do you work for [CCH]? %s ' % \
                              _STORAGE_NOTICE) or 'CCH').upper()
        input = get()
        while len(input) != 3:
            print 'Your division identifier must be three characters: '
            input = get()
        return input
    return get_option('Merger', 'division', input_division)


def get_merger_institution():
    def input_institution():
        def get():
            return (raw_input('What institution do you work for [SIO]? %s ' % \
                              _STORAGE_NOTICE) or 'SIO').upper()
        input = get()
        while len(input) != 3:
            print 'Your institution identifier must be three characters: '
            input = get()
        return input
    return get_option('Merger', 'institution', input_institution)


def get_merger_initials():
    def input_initials():
        return raw_input('What are your initials? %s ' % \
                          _STORAGE_NOTICE).upper()
    return get_option('Merger', 'initials', input_initials)


def stamp():
    return '%(date)8s%(institution)3s%(division)3s%(initials)3s' % \
        {'date': datetime.date.today().strftime('%Y%m%d'),
         'institution': get_merger_institution(),
         'division': get_merger_division(),
         'initials': get_merger_initials(),
        }
