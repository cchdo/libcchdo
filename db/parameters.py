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
        self.display_order = row[7] or sys.maxint
        self.aliases = []
    else:
        connection.close()
        raise NameError(
             "'%s' is not in CCHDO's parameter list." % parameter_name)
    connection.close()


OVERRIDE_PARAMETERS = {
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
## The CTD details are included because the database does not have descriptions.
#    'CTDETIME': {'name': 'etime',
#                 'format': 's',
#                 'description': 'etime',
#                 'units': '',
#                 'bound_lower': '',
#                 'bound_upper': '',
#                 'unit_mnemonic': '',
#                 'display_order': sys.maxint,
#                 'aliases': [],
#                },
    'CTDNOBS': {'name': 'nobs', # XXX
               'format': 's',
               'description': 'Number of observations',
               'units': '',
               'bound_lower': '',
               'bound_upper': '',
               'unit_mnemonic': '',
               'display_order': sys.maxint,
               'aliases': ['NUMBER'], # XXX
              },
#    'TRANSM': {'name': 'transmissometer',
#               'format': 's',
#               'description': 'Transmissometer',
#               'units': '',
#               'bound_lower': '',
#               'bound_upper': '',
#               'unit_mnemonic': '',
#               'display_order': sys.maxint,
#               'aliases': [],
#              },
#    'FLUORM': {'name': 'fluorometer',
#               'format': 's',
#               'description': 'Fluorometer',
#               'units': '',
#               'bound_lower': '',
#               'bound_upper': '',
#               'unit_mnemonic': '',
#               'display_order': sys.maxint,
#               'aliases': [],
#              },
}


# Initialize parameter display orders
def mysql_parameter_order_to_array(order):
    return filter(None, map(lambda x: None if x.endswith('_FLAG_W') else x, 
                               map(lambda x: x.strip(), order.split(','))))

_conn = connect.cchdo()
_cur = _conn.cursor()
_cur.execute(("SELECT parameters FROM parameter_groups WHERE "
              "`group` = 'CCHDO Primary Parameters'"))
_row = _cur.fetchone()
_parameters = mysql_parameter_order_to_array(_row[0])
_cur.execute(("SELECT parameters FROM parameter_groups WHERE "
              "`group` = 'CCHDO Secondary Parameters'"))
_row = _cur.fetchone()
_parameters += mysql_parameter_order_to_array(_row[0])
_cur.execute(("SELECT parameters FROM parameter_groups WHERE "
              "`group` = 'CCHDO Tertiary Parameters'"))
_row = _cur.fetchone()
_parameters += mysql_parameter_order_to_array(_row[0])
_cur.close()
_conn.close()

MYSQL_PARAMETER_DISPLAY_ORDERS = dict(map(lambda x: x[::-1], enumerate(_parameters)))


def init_from_mysql(self, parameter_name):
    def initialize_self_from_known_parameters(this, parameter_name):
        info = OVERRIDE_PARAMETERS[parameter_name]
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
    if parameter_name in OVERRIDE_PARAMETERS:
        initialize_self_from_known_parameters(self, parameter_name)
        return
    else: # try to use aliases
        for known_parameter in OVERRIDE_PARAMETERS:
            if parameter_name in OVERRIDE_PARAMETERS[known_parameter]["aliases"]:
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
        try:
            self.display_order = MYSQL_PARAMETER_DISPLAY_ORDERS[
                                     parameter_name]
        except KeyError:
            self.display_order = sys.maxint
        self.aliases = row[6].split(',') if row[6] else []
        connection.close()
    else:
        connection.close()
        raise NameError(
             "'%s' is not in CCHDO's parameter list." % parameter_name)
