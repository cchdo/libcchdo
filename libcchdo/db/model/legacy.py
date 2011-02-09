import sys

import sqlalchemy as S
import sqlalchemy.orm
import sqlalchemy.ext.declarative

from ... import memoize
from ... import LOG
from ...db import connect
from ...db import Enum

Base = S.ext.declarative.declarative_base()
metadata = Base.metadata


@memoize
def session():
    return connect.session(connect.cchdo())


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
    'CTDNOBS': {'name': 'CTD Num OBS', # XXX
               'format': '5s',
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


class ParameterGroup(Base):
    __tablename__ = 'parameter_groups'

    id = S.Column(S.Integer, autoincrement=True, primary_key=True, nullable=False)
    group = S.Column(S.String)
    parameters = S.Column(S.String)

    def __init__(self):
        pass


def _mysql_parameter_order_to_array(order):
    return filter(None, map(lambda x: None if x.endswith('_FLAG_W') else x, 
                               map(lambda x: x.strip(), order.split(','))))


_sesh = connect.session(connect.cchdo())
_query = _sesh.query(ParameterGroup)

_primary = _query.filter(ParameterGroup.group == \
                        'CCHDO Primary Parameters').first()
_parameters = _mysql_parameter_order_to_array(_primary.parameters)

_secondary = _query.filter(ParameterGroup.group == \
                           'CCHDO Secondary Parameters').first()
_parameters += _mysql_parameter_order_to_array(_secondary.parameters)

_tertiary = _query.filter(ParameterGroup.group == \
                          'CCHDO Tertiary Parameters').first()
_parameters += _mysql_parameter_order_to_array(_tertiary.parameters)
_sesh.close()


MYSQL_PARAMETER_DISPLAY_ORDERS = dict(map(lambda x: x[::-1], enumerate(_parameters)))


class Parameter(Base):
    __tablename__ = 'parameter_descriptions'

    id = S.Column(S.Integer, autoincrement=True, primary_key=True)
    name = S.Column('Parameter', S.String(255))
    full_name = S.Column('FullName', S.String(255))
    description = S.Column('Description', S.String(255))
    units = S.Column('Units', S.String(255))
    range = S.Column('Range', S.String(255))
    alias = S.Column('Alias', S.String(255))
    group = S.Column('Group', S.String(255))
    unit_mnemonic = S.Column('Unit_Mnemonic', S.String(255))
    precision = S.Column('Precision', S.String(255), default='')
    ruby_precision = S.Column('RubyPrecision', S.String(255), default=None)
    private = S.Column('Private', S.Integer(11), default=0)
    unit_mnemonic_woce = S.Column('WoceUnitMnemonic', S.String, nullable=False)
    added_by = S.Column('AddedBy', S.String)
    notes = S.Column('Notes', S.String)

    def __init__(self, name):
        self.name = name

    @classmethod
    def find_known(cls, parameter_name):

        def init_from_known_parameters(self, parameter_name):
            info = OVERRIDE_PARAMETERS[parameter_name]
            self.name = parameter_name
            self.full_name = info['name']
            self.ruby_precision = info['format']
            self.description = info['description']
            self.bound_lower = info['bound_lower']
            self.bound_upper = info['bound_upper']
            self.units = info['units']
            self.units_mnemonic = info['unit_mnemonic']
            self.woce_mnemonic = parameter_name
            self.aliases = info['aliases']
            self.display_order = info['display_order']

        parameter = Parameter(parameter_name)

        if parameter_name in OVERRIDE_PARAMETERS:
            init_from_known_parameters(parameter, parameter_name)
            return parameter
        else: # try to use aliases
            for known_parameter, param in OVERRIDE_PARAMETERS.items():
                if parameter_name in param['aliases']:
                    init_from_known_parameters(parameter, known_parameter)
                    return parameter
            raise EnvironmentError(
                "Parameter '%s' is not known in legacy database." % \
                parameter_name)


class Contact(Base):
    __tablename__ = 'contacts'

    id = S.Column(S.Integer(11), primary_key=True)
    LastName = S.Column(S.String)
    FirstName = S.Column(S.String)
    Institute = S.Column(S.String)
    Address = S.Column(S.String)
    telephone = S.Column(S.String)
    fax = S.Column(S.String)
    email = S.Column(S.String)
    title = S.Column(S.String)

    def __init__(self):
        pass


class ParameterStatus(Base):
    __tablename__ = 'parameter_status'

    expocode = S.Column('ExpoCode', S.Integer(11), primary_key=True)
    parameter_id = S.Column(S.ForeignKey('parameter_descriptions.id'),
                            primary_key=True)
    pi_id = S.Column(S.ForeignKey('contacts.id'), nullable=True)
    status = S.Column(Enum([u'PRELIMINARY', u'NON-PRELIMINARY']),
                      default=u'PRELIMINARY')

    parameter = S.orm.relation(Parameter)
    pi = S.orm.relation(Contact)

    def __init__(self, expocode, parameter, status, pi=None):
        self.expocode = expocode
        self.parameter = parameter
        self.status = status
        if pi:
            self.pi = pi


def find_parameter(name):
    sesh = session()
    legacy_parameter = sesh.query(Parameter).filter(
        Parameter.name == name).first()

    if not legacy_parameter:
        # Try aliases
        LOG.warn(
            "No legacy parameter found for '%s'. Falling back to aliases." % \
            name)
        legacy_parameter = sesh.query(Parameter).filter(
            Parameter.alias.like('%%%s%%' % name)).first()
        
        if not legacy_parameter:
            # Try known overrides
            LOG.warn(
                ("No legacy parameter found for '%s'. Falling back on known "
                 "override parameters.") % name)
            try:
                legacy_parameter = Parameter.find_known(name)
            except EnvironmentError:
                return None
    else:
        try:
            legacy_parameter.display_order = \
                MYSQL_PARAMETER_DISPLAY_ORDERS[legacy_parameter.name]
        except:
            legacy_parameter.display_order = sys.maxint

    return legacy_parameter


