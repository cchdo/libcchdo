import ConfigParser
import datetime
import os


_CONFIG_FILE_NAME = '.libcchdo.cfg'
_CONFIG_PATHS = [
    os.path.join(os.getcwd(), _CONFIG_FILE_NAME),
    os.path.join(os.path.expanduser('~'), _CONFIG_FILE_NAME),
]
_CONFIG = ConfigParser.SafeConfigParser()
_CONFIG.read(_CONFIG_PATHS)


def get_option(section, option, default_function):
    try:
        return _CONFIG.get(section, option)
    except ConfigParser.NoOptionError:
        pass
    except ConfigParser.NoSectionError:
        pass

    val = default_function()

    try:
        _CONFIG.add_section(section)
    except ConfigParser.DuplicateSectionError:
        pass
    _CONFIG.set(section, option, val)
    with open(_CONFIG_PATHS[-1], 'wb') as config_file:
        _CONFIG.write(config_file)
    return val
    

def get_merger_institution():
    return get_option('Merger', 'institution', lambda x: 'SIOCCHDO')


def get_merger_initials():
    def input_initials():
        return raw_input(('What are your initials? (Your answer will be '
                          'saved in %s for future use) ') % \
                          _CONFIG_PATHS[-1]).upper()
    return get_option('Merger', 'initials', input_initials)


def stamp():
    return '%(date)8s%(institution)s%(initials)s' % \
        {'date': datetime.datetime.now().strftime('%Y%m%d'),
         'institution': get_merger_institution(),
         'initials': get_merger_initials(),
        }
