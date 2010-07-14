""" libcchdo.db.parameters
Ways to get parameters from databases.
"""


import sys

import connect


def init_from_postgresql(self, parameter_name):
    connection = connect.cchdotest()
    cursor = connection.cursor()
    select = ','.join(
        ('parameters.name', 'format', 'description', 'units', 'bound_lower',
         'bound_upper', 'units.mnemonic_woce', 'parameters_orders.order',))
    cursor.execute(
        ('SELECT %s FROM parameters '
         'INNER JOIN parameters_aliases ON '
         'parameters.id = parameters_aliases.parameter_id '
         'LEFT JOIN parameters_orders ON '
         'parameters.id = parameters_orders.parameter_id '
         'LEFT JOIN units ON parameters.units = units.id '
         "WHERE parameters_aliases.name = '%s' "
         'LIMIT 1') % (select, parameter_name,))
    row = cursor.fetchone()
    if row:
        self.full_name = row[0]
        self.format = row[1].strip() if row[1] else '11s'
        self.description = row[2]
        self.units = row[3]
        self.bound_lower = row[4]
        self.bound_upper = row[5]
        self.units_mnemonic = row[6]
        self.woce_mnemonic = parameter_name
        self.display_order = row[7] or -9999
        self.aliases = []
    else:
        connection.close()
        raise NameError(
             "'%s' is not in CCHDO's parameter list." % parameter_name)
    connection.close()


KNOWN_PARAMETERS = {
    'EXPOCODE': {'name': 'ExpoCode',
                 'format': '11s',
                 'description': 'ExpoCode',
                 'units': '',
                 'bound_lower': '',
                 'bound_upper': '',
                 'unit_mnemonic': '',
                 'display_order': 1,
                 'aliases': [],
                },
    'SECT_ID': {'name': 'Section ID',
                'format': '11s',
                'description': 'Section ID',
                'units': '',
                'bound_lower': '',
                'bound_upper': '',
                'unit_mnemonic': '',
                'display_order': 2,
                'aliases': [],
               },
# The CTD details are included because the database does not have descriptions.
    'CTDPRS': {'name': 'Pressure',
               'format': '8.1f',
               'description': 'CTD pressure',
               'units': 'decibar',
               'bound_lower': '0',
               'bound_upper': '11000',
               'unit_mnemonic': 'DBAR',
               'display_order': 6,
               'aliases': [],
              },
    'CTDTMP': {'name': 'Temperature',
               'format': '8.4f',
               'description': 'CTD temperature',
               'units': 'ITS90',
               'bound_lower': '-2',
               'bound_upper': '35',
               'unit_mnemonic': 'ITS-90',
               'display_order': 7,
               'aliases': [],
              },
    'CTDOXY': {'name': 'Oxygen',
               'format': '8.1f',
               'description': 'CTD oxygen',
               'units': u'\xb5mol/kg',
               'bound_lower': '0',
               'bound_upper': '500',
               'unit_mnemonic': 'UMOL/KG',
               'display_order': 8,
               'aliases': [],
              },
    'CTDSAL': {'name': 'Salinity',
               'format': '8.4f',
               'description': 'CTD salinity',
               'units': 'PSS-78',
               'bound_lower': '0',
               'bound_upper': '42',
               'unit_mnemonic': 'PSS-78',
               'display_order': 9,
               'aliases': [],
              },
    'CTDETIME': {'name': 'etime',
                 'format': 's',
                 'description': 'etime',
                 'units': '',
                 'bound_lower': '',
                 'bound_upper': '',
                 'unit_mnemonic': '',
                 'display_order': sys.maxint - 99999,
                 'aliases': [],
                },
    'CTDNOBS': {'name': 'nobs', # XXX
               'format': 's',
               'description': 'Number of observations',
               'units': '',
               'bound_lower': '',
               'bound_upper': '',
               'unit_mnemonic': '',
               'display_order': sys.maxint - 99999,
               'aliases': ['NUMBER'], # XXX
              },
    'TRANSM': {'name': 'transmissometer',
               'format': 's',
               'description': 'Transmissometer',
               'units': '',
               'bound_lower': '',
               'bound_upper': '',
               'unit_mnemonic': '',
               'display_order': sys.maxint - 99999,
               'aliases': [],
              },
    'FLUORM': {'name': 'fluorometer',
               'format': 's',
               'description': 'Fluorometer',
               'units': '',
               'bound_lower': '',
               'bound_upper': '',
               'unit_mnemonic': '',
               'display_order': sys.maxint - 99999,
               'aliases': [],
              },
}



def init_from_mysql(self, parameter_name):
    def initialize_self_from_known_parameters(this, parameter_name):
        info = KNOWN_PARAMETERS[parameter_name]
        this.full_name = info['name']
        this.format = info['format']
        this.description = info['description']
        this.units = info['units']
        this.bound_lower = info['bound_lower']
        this.bound_upper = info['bound_upper']
        this.units_mnemonic = info['unit_mnemonic']
        this.woce_mnemonic = parameter_name
        this.display_order = info['display_order']
        this.aliases = info['aliases']
    if parameter_name in KNOWN_PARAMETERS:
        initialize_self_from_known_parameters(self, parameter_name)
        return
    else: # try to use aliases
        for known_parameter in KNOWN_PARAMETERS:
            if parameter_name in KNOWN_PARAMETERS[known_parameter]["aliases"]:
                initialize_self_from_known_parameters(self, known_parameter)
                return
    connection = connect.cchdo()
    cursor = connection.cursor()
    def wrap_column(s):
        return '`%s`' % s
    select = ','.join(map(wrap_column,
                          ('FullName', 'RubyPrecision', 'Description',
                           'Units', 'Range', 'Unit_Mnemonic', 'Alias',)))
    cursor.execute(
        ('SELECT %s FROM parameter_descriptions '
         "WHERE Parameter LIKE '%s' LIMIT 1") % (select, parameter_name,))
    row = cursor.fetchone()
    if row:
        self.full_name = row[0]
        self.format = row[1].strip() if row[1] else '11s'
        self.description = row[2] or ''
        self.units = row[3]
        self.bound_lower = row[4].split(',')[0] if row[4] else None
        self.bound_upper = row[4].split(',')[1] if row[4] else None
        self.units_mnemonic = row[5]
        self.woce_mnemonic = parameter_name
        self.display_order = -9999
        self.aliases = row[6].split(',') if row[6] else []
        connection.close()
    else:
        connection.close()
        raise NameError(
             "'%s' is not in CCHDO's parameter list." % parameter_name)
