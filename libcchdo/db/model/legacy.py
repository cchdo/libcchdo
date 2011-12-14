import sys

import sqlalchemy as S
import sqlalchemy.orm
import sqlalchemy.ext.declarative
from geoalchemy import GeometryColumn, LineString

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


class BottleDB(Base):
    __tablename__ = 'bottle_dbs'

    id = S.Column(S.Integer, autoincrement=True, primary_key=True, nullable=False)
    ExpoCode = S.Column(S.String)
    Parameters = S.Column(S.String)
    Parameter_Persistance = S.Column(S.String)
    Bottle_Code = S.Column(S.String)
    Location = S.Column(S.String)
    Entries = S.Column(S.Integer)
    Stations = S.Column(S.Integer)


class Codes(Base):
    """ Codes used by CruiseParameterInfos """
    __tablename__ = 'codes'

    Code = S.Column(S.Integer, primary_key=True)
    Status = S.Column(S.String, primary_key=True)


# Initialize parameter display orders


class ParameterGroup(Base):
    __tablename__ = 'parameter_groups'

    id = S.Column(S.Integer, autoincrement=True, primary_key=True, nullable=False)
    group = S.Column(S.Unicode)
    parameters = S.Column(S.Unicode)

    @property
    def ordered_parameters(self):
        return _mysql_parameter_order_to_array(self.parameters)


def _mysql_parameter_order_to_array(order):
    return filter(None, map(lambda x: None if x.endswith('_FLAG_W') else x, 
                               map(lambda x: x.strip(), order.split(','))))


_sesh = connect.session(connect.cchdo())
_query = _sesh.query(ParameterGroup)

_primary = _query.filter(ParameterGroup.group == \
                        u'CCHDO Primary Parameters').first()
_parameters = _mysql_parameter_order_to_array(_primary.parameters)

_secondary = _query.filter(ParameterGroup.group == \
                           u'CCHDO Secondary Parameters').first()
_parameters += _mysql_parameter_order_to_array(_secondary.parameters)

_tertiary = _query.filter(ParameterGroup.group == \
                          u'CCHDO Tertiary Parameters').first()
_parameters += _mysql_parameter_order_to_array(_tertiary.parameters)
_sesh.close()


MYSQL_PARAMETER_DISPLAY_ORDERS = dict(map(lambda x: x[::-1], enumerate(_parameters)))


class Parameter(Base):
    __tablename__ = 'parameter_descriptions'

    id = S.Column(S.Integer, autoincrement=True, primary_key=True)
    name = S.Column('Parameter', S.String(255))
    full_name = S.Column('FullName', S.Unicode)
    description = S.Column('Description', S.Unicode)
    units = S.Column('Units', S.Unicode)
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


class User(Base):
    __tablename__ = 'users'

    id = S.Column(S.Integer, primary_key=True)
    username = S.Column(S.String)
    password_salt = S.Column(S.String)
    password_hash = S.Column(S.String)


class ArgoFile(Base):
    __tablename__ = 'argo_files'

    id = S.Column(S.Integer(11), primary_key=True)
    user_id = S.Column(S.ForeignKey('users.id'))
    expocode = S.Column('ExpoCode', S.String)
    description = S.Column(S.String)
    display = S.Column(S.Boolean)
    size = S.Column(S.Integer)
    filename = S.Column(S.String)
    content_type = S.Column(S.Integer)
    created_at = S.Column(S.DateTime)

    user = S.orm.relation(User)


class ArgoDownload(Base):
    __tablename__ = 'argo_downloads'

    file_id = S.Column(S.ForeignKey('argo_files.id'), primary_key=True)
    created_at = S.Column(S.TIMESTAMP, primary_key=True)
    ip = S.Column(S.String, primary_key=True)

    file = S.orm.relation(ArgoFile, backref='downloads')


class ContactsCruise(Base):
    __tablename__ = 'contacts_cruises'

    cruise_id = S.Column(S.Integer, S.ForeignKey('cruises.id'), primary_key=True)
    contact_id = S.Column(S.Integer, S.ForeignKey('contacts.id'), primary_key=True)
    function = S.Column(S.String)

    contact = S.orm.relationship('Contact', backref='contacts_cruises')


class CollectionsCruise(Base):
    __tablename__ = 'collections_cruises'

    cruise_id = S.Column(S.Integer, S.ForeignKey('cruises.id'), primary_key=True)
    collection_id = S.Column(S.Integer, S.ForeignKey('collections.id'), primary_key=True)

    collection = S.orm.relationship('Collection', backref='collections_cruises')



class TrackLine(Base):
    __tablename__ = 'track_lines'

    id = S.Column(S.Integer(11), primary_key=True)
    ExpoCode = S.Column(S.String)
    Track = GeometryColumn(LineString(2))
    Basins = S.Column(S.String)


class Cruise(Base):
    __tablename__ = 'cruises'

    id = S.Column(S.Integer(11), primary_key=True)
    ExpoCode = S.Column(S.String)
    Line = S.Column(S.String)
    Country = S.Column(S.String)
    Chief_Scientist = S.Column(S.String)
    Begin_Date = S.Column(S.Date)
    EndDate = S.Column(S.Date)
    Ship_Name = S.Column(S.String)
    Alias = S.Column(S.String)
    Group = S.Column(S.String)
    Program = S.Column(S.String)
    link = S.Column(S.String)

    contacts_cruises = S.orm.relationship('ContactsCruise', backref='cruise')
    collections_cruises = S.orm.relationship('CollectionsCruise', backref='cruise')


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


class Event(Base):
    __tablename__ = 'events'

    ID = S.Column(S.Integer(255), primary_key=True)
    ExpoCode = S.Column(S.String)
    First_Name = S.Column(S.String)
    LastName = S.Column(S.String)
    Data_Type = S.Column(S.String)
    Action = S.Column(S.String)
    Date_Entered = S.Column(S.Date)
    Summary = S.Column(S.String)
    Note = S.Column(S.String)


class Document(Base):
    __tablename__ = 'documents'

    id = S.Column(S.Integer, primary_key=True)
    Size = S.Column(S.String)
    FileType = S.Column(S.String)
    FileName = S.Column(S.String)
    ExpoCode = S.Column(S.String)
    Files = S.Column(S.String)
    LastModified = S.Column(S.DateTime)
    Modified = S.Column(S.String)
    Stamp = S.Column(S.String)
    Preliminary = S.Column(S.Integer)


class Collection(Base):
    __tablename__ = 'collections'

    id = S.Column(S.Integer(11), primary_key=True)
    Name = S.Column(S.String)


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


class CruiseParameterInfo(Base):
    __tablename__ = 'parameters'

    _PARAMETERS = [
    'THETA', 'SILCAT', 'SALNTY', 'PHSPHT', 'OXYGEN', 'NO2+NO3', 'HELIUM',
    'DELC14', 'CTDTMP', 'CTDSAL', 'CTDPRS', 'CFC113', 'CFC-12', 'CFC-11',
    'CCL4', 'TCARBN', 'REVTMP', 'PCO2', 'NITRIT', 'NITRAT', 'CTDRAW', 'ALKALI',
    'O18O16', 'MCHFRM', 'DELHE3', 'CTDOXY', 'REVPRS', 'PH', 'DELC13', 'PPHYTN',
    'CHLORA', 'CH4', 'AZOTE', 'ARGON', 'NEON', 'PCO2TMP', 'IODIDE', 'IODATE',
    'NH4', 'RA-228', 'RA-226', 'KR-85', 'POC', 'PON', 'TDN', 'DOC', 'AR-39',
    'BACT', 'ARAB', 'MAN', 'BRDU', 'RHAM', 'GLU', 'DCNS', 'FUC', 'PRO', 'PEUK',
    'SYN', 'BTLNBR', 'AOU', 'TOC', 'CASTNO', 'DEPTH', 'Halocarbons', 'I-129',
    'BARIUM', 'DON', 'SF6', 'NI', 'CU', 'CALCIUM', 'PHSPER', 'NTRIER', 'NTRAER',
    'DELHE4', 'N2O', 'DMS', 'TRITUM', 'PHTEMP', ]

    id = S.Column(S.Integer(11), primary_key=True)
    ExpoCode = S.Column(S.String)


for cpi in CruiseParameterInfo._PARAMETERS:
    setattr(CruiseParameterInfo, cpi, S.Column(S.String))
    setattr(CruiseParameterInfo, cpi + '_PI', S.Column(S.String))
    setattr(CruiseParameterInfo, cpi + '_Date', S.Column(S.String))


class QueueFile(Base):
    __tablename__ = 'queue_files'

    id = S.Column(S.Integer(11), primary_key=True)
    Name = S.Column(S.String)
    date_received = S.Column('DateRecieved', S.Date)
    date_merged = S.Column('DateMerged', S.Date)
    expocode = S.Column('ExpoCode', S.String)
    merged = S.Column('Merged', S.Integer)
    contact = S.Column('Contact', S.String)
    processed_input = S.Column('ProcessedInput', S.String)
    notes = S.Column('Notes', S.String)
    unprocessed_input = S.Column('UnprocessedInput', S.String)
    parameters = S.Column('Parameters', S.String)
    action = S.Column('Action', S.String)
    cchdo_contact = S.Column('CCHDOContact', S.String)
    merge_notes = S.Column(S.String)
    hidden = S.Column(S.Integer)
    documentation = S.Column(S.Integer)


class Submission(Base):
    __tablename__ = 'submissions'

    id = S.Column(S.Integer(11), primary_key=True)
    name = S.Column(S.String)
    institute = S.Column(S.String)
    country = S.Column('Country', S.String)
    email = S.Column(S.String)
    public = S.Column(S.String)
    expocode = S.Column('ExpoCode', S.String)
    ship_name = S.Column('Ship_Name', S.String)
    line = S.Column('Line', S.String)
    cruise_date = S.Column(S.Date)
    action = S.Column(S.String)
    notes = S.Column(S.String)
    file = S.Column(S.String)
    assigned = S.Column(S.Integer)
    assimilated = S.Column(S.Integer)
    submission_date = S.Column(S.Date)
    ip = S.Column(S.String)
    user_agent = S.Column(S.String)


class OldSubmission(Base):
    __tablename__ = 'old_submissions'

    id = S.Column(S.Integer(11), primary_key=True)
    Date = S.Column(S.Date)
    Stamp = S.Column(S.String)
    Name = S.Column(S.String)
    Line = S.Column(S.String)
    Filename = S.Column(S.String)
    Filetype = S.Column(S.String)
    Location = S.Column(S.String)
    Folder = S.Column(S.String)
    created_at = S.Column(S.DateTime)
    updated_at = S.Column(S.DateTime)


class SpatialGroup(Base):
    __tablename__ = 'spatial_groups'

    id = S.Column(S.Integer, primary_key=True)
    area = S.Column(S.String)
    expocode = S.Column('ExpoCode', S.String)

    atlantic = S.Column(S.types.BINARY)
    arctic = S.Column(S.types.BINARY)
    pacific = S.Column(S.types.BINARY)
    indian = S.Column(S.types.BINARY)
    southern = S.Column(S.types.BINARY)


class Internal(Base):
    __tablename__ = 'internal'

    Line = S.Column(S.String, primary_key=True)
    File = S.Column(S.String, primary_key=True)
    expocode = S.Column('ExpoCode', S.String, primary_key=True)
    Basin = S.Column(S.String, primary_key=True)


class UnusedTrack(Base):
    __tablename__ = 'unused_tracks'

    id = S.Column(S.Integer(11), primary_key=True)
    expocode = S.Column('ExpoCode', S.String)
    filename = S.Column('FileName', S.String)
    Basin = S.Column(S.String)
    Track = S.Column(S.String)


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


