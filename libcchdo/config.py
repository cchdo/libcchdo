import ConfigParser
import datetime
import os
import getpass


from . import LOG


_CONFIG_DIR = '.%s' % __package__


_CONFIG_FILE_NAME = '.%s.cfg' % __package__


_CONFIG_PATHS = [
    os.path.join(os.getcwd(), _CONFIG_DIR,  _CONFIG_FILE_NAME),
    os.path.expanduser(os.path.join('~', _CONFIG_DIR, _CONFIG_FILE_NAME)),
]


_CONFIG = ConfigParser.SafeConfigParser()
_CONFIG.read(_CONFIG_PATHS)


def _get_config_path():
    config_path = _CONFIG_PATHS[-1]
    for path in _CONFIG_PATHS:
        if os.exists(path):
            config_path = path
    return os.path.realpath(config_path)


def _save_config():
    config_path = _get_config_path()

    if not os.path.isdir(os.path.dirname(config_path)):
        try:
            os.makedirs(os.path.dirname(config_path))
        except error:
            LOG.error('Unable to write configuration file: %s' % config_path)
            return

    with open(config_path, 'wb') as config_file:
        _CONFIG.write(config_file)


try:
    _CONFIG.get('db', 'cache')
except ConfigParser.Error, e:
    if isinstance(e, ConfigParser.NoSectionError):
        _CONFIG.add_section('db')
    dir = os.path.dirname(_get_config_path())
    _CONFIG.set('db', 'cache', os.path.join(dir, 'cchdo_data.db'))
    _save_config()


def get_option(section, option, default_function=None):
    try:
        return _CONFIG.get(section, option)
    except ConfigParser.NoOptionError, e:
        if not default_function:
            raise e
    except ConfigParser.NoSectionError, e:
        if not default_function:
            raise e

    val = default_function()

    try:
        _CONFIG.add_section(section)
    except ConfigParser.DuplicateSectionError:
        pass
    _CONFIG.set(section, option, val)

    _save_config()
    return val


_STORAGE_NOTICE = \
    "(Your answer will be saved in %s for future use)" % _CONFIG_PATHS[-1]


def get_db_credentials_cchdo():
    def input_cchdo_username():
        return raw_input(('What is your username for the database '
                          'cchdo.ucsd.edu/cchdo? %s ') % _STORAGE_NOTICE)

    username = get_option('db_cred', 'cchdo/cchdo_user', input_cchdo_username)
    # Passwords will not be saved.
    try:
        password = get_option('db_cred', 'cchdo/cchdo_pass')
    except ConfigParser.Error:
        password = getpass.getpass(
            ('Password for database %s@cchdo.ucsd.edu/cchdo (to avoid this '
             'question, put your password in plain text as cchdo/cchdo_pass '
             'under [db_cred] in %s):') % (username, _CONFIG_PATHS[-1]))
    return (username, password)
    

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
    return '%(date)8s%(division)3s%(institution)3s%(initials)3s' % \
        {'date': datetime.date.today().strftime('%Y%m%d'),
         'institution': get_merger_institution(),
         'division': get_merger_division(),
         'initials': get_merger_initials(),
        }
