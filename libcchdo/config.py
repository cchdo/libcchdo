import ConfigParser
import datetime
import os


from . import LOG


_CONFIG_DIR = '.%s' % os.path.split(os.getcwd())[1]


_CONFIG_FILE_NAME = '.%s.cfg' % __package__


_CONFIG_PATHS = [
    os.path.join(os.getcwd(), _CONFIG_DIR,  _CONFIG_FILE_NAME),
    os.path.expanduser(os.path.join('~', _CONFIG_DIR, _CONFIG_FILE_NAME)),
]


_CONFIG = ConfigParser.SafeConfigParser()
_CONFIG.read(_CONFIG_PATHS)


def save_config():
    config_path = os.path.realpath(_CONFIG_PATHS[-1])

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
    _CONFIG.set('db', 'cache',
                os.path.expanduser(os.path.join('~', _CONFIG_DIR,
                                                'cchdo_data.db')))
    save_config()


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

    save_config()
    return val
    

def get_merger_division():
    def input_division():
        return (raw_input(('What division do you work for [CCH]? (Your answer '
                           'will be saved in %s for future use) ') % \
                           _CONFIG_PATHS[-1]) or 'CCH').upper()
    return get_option('Merger', 'division', input_division)


def get_merger_institution():
    def input_institution():
        return (raw_input(('What institution do you work for [SIO]? (Your answer '
                           'will be saved in %s for future use) ') % \
                           _CONFIG_PATHS[-1]) or 'SIO').upper()
    return get_option('Merger', 'institution', input_institution)


def get_merger_initials():
    def input_initials():
        return raw_input(('What are your initials? (Your answer will be '
                          'saved in %s for future use) ') % \
                          _CONFIG_PATHS[-1]).upper()
    return get_option('Merger', 'initials', input_initials)


def stamp():
    return '%(date)8s%(division)3s%(institution)3s%(initials)3s' % \
        {'date': datetime.datetime.now().strftime('%Y%m%d'),
         'institution': get_merger_institution(),
         'division': get_merger_division(),
         'initials': get_merger_initials(),
        }
