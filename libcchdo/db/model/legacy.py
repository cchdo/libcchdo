import sys

import sqlalchemy as S
from sqlalchemy import (
    Column, Integer, String, Boolean, Unicode, DateTime, Date, ForeignKey
    )
import sqlalchemy.orm
from sqlalchemy.orm import relationship, relation, column_property, backref
from sqlalchemy.types import BINARY
import sqlalchemy.ext.declarative
from geoalchemy import GeometryColumn, LineString

from libcchdo.log import LOG
from libcchdo.db import connect, Enum


Base = S.ext.declarative.declarative_base()
metadata = Base.metadata


def session():
    return connect.session(connect.cchdo())


def str_list_add(str_list, item, separator=','):
    """Add item to a string representing a list."""
    if str_list is None:
        strs = []
    else:
        strs = str_list.split(separator)
    if item not in strs:
        return separator.join(strs + [item])
    return str_list


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


# This will get filled in the first time it is used
MYSQL_PARAMETER_DISPLAY_ORDERS = None


class ArcticAssignment(Base):
    __tablename__ = 'arctic_assignments'

    id = Column(Integer, primary_key=True)
    expocode = Column('ExpoCode', String(30))
    project = Column(String)
    current_status = Column(String)
    cchdo_contact = Column(String)
    data_contact = Column(String)
    action = Column(String)
    parameter = Column(String)
    history = Column(String)
    last_changed = Column('LastChanged', Date)
    notes = Column(String)
    priority = Column(Integer)
    deadline = Column(Date)
    manager = Column(String(255))
    complete = Column(Integer)
    task_group = Column(String(255))
    visible = Column(Integer)


class BottleDB(Base):
    __tablename__ = 'bottle_dbs'

    id = Column(Integer, autoincrement=True, primary_key=True, nullable=False)
    ExpoCode = Column(String)
    Parameters = Column(String)
    Parameter_Persistance = Column(String)
    Bottle_Code = Column(String)
    Location = Column(String)
    Entries = Column(Integer)
    Stations = Column(Integer)


class Codes(Base):
    """ Codes used by CruiseParameterInfos """
    __tablename__ = 'z_codes'

    Code = Column(Integer, primary_key=True)
    Status = Column(String, primary_key=True)


class CruiseGroup(Base):
    __tablename__ = 'cruise_groups'
    id = Column(Integer, primary_key=True)
    #name = Column('Group', String)
    spatial = Column('Spatial', String)
    cruises = Column('Cruises', String)
    group = Column('Group', String)
    subgroups = Column('SubGroups', String)


# Initialize parameter display orders


class ParameterGroup(Base):
    __tablename__ = 'parameter_groups'

    id = Column(Integer, autoincrement=True, primary_key=True, nullable=False)
    group = Column(Unicode)
    parameters = Column(Unicode)

    @property
    def ordered_parameters(self):
        return _mysql_parameter_order_to_array(self.parameters)


def _mysql_parameter_order_to_array(order):
    return filter(None, map(lambda x: None if x.endswith('_FLAG_W') else x, 
                               map(lambda x: x.strip(), order.split(','))))


def mysql_parameter_display_orders(session):
    global MYSQL_PARAMETER_DISPLAY_ORDERS

    if MYSQL_PARAMETER_DISPLAY_ORDERS is not None:
        return MYSQL_PARAMETER_DISPLAY_ORDERS

    query = session.query(ParameterGroup)

    groups = [
        u'CCHDO Primary Parameters',
        u'CCHDO Secondary Parameters',
        u'CCHDO Tertiary Parameters',
        ]

    parameters = []
    for group in groups:
        pgroup = query.filter(ParameterGroup.group == group).first()
        parameters += _mysql_parameter_order_to_array(pgroup.parameters)

    parameter_display_orders = dict(
        map(lambda x: x[::-1], enumerate(parameters)))

    MYSQL_PARAMETER_DISPLAY_ORDERS = parameter_display_orders

    return parameter_display_orders


def _to_unicode(x):
    if x is None:
        return None
    try:
        return unicode(x, 'raw_unicode_escape')
    except TypeError:
        return x


class Parameter(Base):
    __tablename__ = 'parameter_descriptions'

    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column('Parameter', Unicode)
    full_name_ = Column('FullName', String(255))
    description_ = Column('Description', String(255))
    units_ = Column('Units', String(255))
    range = Column('Range', Unicode)
    alias = Column('Alias', Unicode)
    group = Column('Group', Unicode)
    unit_mnemonic = Column('Unit_Mnemonic', Unicode)
    precision = Column('Precision', Unicode, default=u'')
    ruby_precision = Column('RubyPrecision', Unicode, default=None)
    private = Column('Private', Integer, default=0)
    unit_mnemonic_woce = Column('WoceUnitMnemonic', String, nullable=False)
    added_by = Column('AddedBy', String)
    notes = Column('Notes', String)

    def __init__(self, name):
        self.name = name

    @property
    def full_name(self):
        return _to_unicode(self.full_name_)

    @full_name.setter
    def full_name(self, x):
        self.full_name_ = x
    
    @full_name.deleter
    def full_name(self):
        del self.full_name_

    @property
    def description(self):
        return _to_unicode(self.description_)

    @description.setter
    def description(self, x):
        self.description_ = x

    @description.deleter
    def description(self):
        del self.description_

    @property
    def units(self):
        try:
            return _to_unicode(self.units_)
        except TypeError:
            return self.units_

    @units.setter
    def units(self, x):
        self.units_ = x

    @units.deleter
    def units(self):
        del self.units_

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

    id = Column(Integer, primary_key=True)
    username = Column(String)
    password_salt = Column(String)
    password_hash = Column(String)


class ArgoFile(Base):
    __tablename__ = 'argo_files'

    id = Column(Integer, primary_key=True)
    user_id = Column(ForeignKey('users.id'))
    expocode = Column('ExpoCode', String)
    description = Column(String)
    display = Column(Boolean)
    size = Column(Integer)
    filename = Column(String)
    content_type = Column(Integer)
    created_at = Column(DateTime)

    user = relation(User)


class ArgoSubmission(Base):
    __tablename__ = 'argo_submissions'

    id = Column(Integer, primary_key=True)
    user_id = Column('user', ForeignKey('users.id'))
    expocode = Column('ExpoCode', String(30))
    filename = Column(String)
    location = Column(String)
    display = Column(Integer)
    link = Column(Integer)
    datetime_added = Column(DateTime)
    note = Column(String)

    user = relation(User)


class ArgoDownload(Base):
    __tablename__ = 'argo_downloads'

    file_id = Column(ForeignKey('argo_files.id'), primary_key=True)
    created_at = Column(S.TIMESTAMP, primary_key=True)
    ip = Column(String, primary_key=True)

    file = relation(ArgoFile, backref='downloads')


class ContactsCruise(Base):
    __tablename__ = 'contacts_cruises'

    cruise_id = Column(Integer, ForeignKey('cruises.id'), primary_key=True)
    contact_id = Column(Integer, ForeignKey('contacts.id'), primary_key=True)
    function = Column(String)
    institution = Column(String)

    contact = relationship('Contact', backref='contacts_cruises')


class CollectionsCruise(Base):
    __tablename__ = 'collections_cruises'

    cruise_id = Column(Integer, ForeignKey('cruises.id'), primary_key=True)
    collection_id = Column(Integer, ForeignKey('collections.id'), primary_key=True)

    collection = relationship('Collection', backref='collections_cruises')



class TrackLine(Base):
    __tablename__ = 'track_lines'

    id = Column(Integer, primary_key=True)
    ExpoCode = Column(String)
    Track = GeometryColumn(LineString(2))
    Basins = Column(String)


class Cruise(Base):
    __tablename__ = 'cruises'

    id = Column(Integer, primary_key=True)
    ExpoCode = Column(String)
    Line = Column(String)
    Country = Column(String)
    Begin_Date = Column(Date)
    EndDate = Column(Date)
    Ship_Name = Column(String)
    Alias = Column(String)
    Group = Column(String)
    Program = Column(String)
    link = Column(String)

    contacts_cruises = relationship('ContactsCruise', backref='cruise')
    collections_cruises = relationship('CollectionsCruise', backref='cruise')


class Contact(Base):
    __tablename__ = 'contacts'

    id = Column(Integer, primary_key=True)
    LastName = Column(String)
    FirstName = Column(String)
    Institute = Column(String)
    Address = Column(String)
    telephone = Column(String)
    fax = Column(String)
    email = Column(String)
    title = Column(String)


class Event(Base):
    __tablename__ = 'events'

    ID = Column(Integer, primary_key=True)
    ExpoCode = Column(String)
    First_Name = Column(String)
    LastName = Column(String)
    Data_Type = Column(String)
    Action = Column(String)
    Date_Entered = Column(Date)
    Summary = Column(String)
    Note = Column(String)


class Document(Base):
    __tablename__ = 'documents'

    id = Column(Integer, primary_key=True)
    Size = Column(String)
    FileType = Column(String)
    FileName = Column(String)
    ExpoCode = Column(String)
    Files = Column(String)
    LastModified = Column(DateTime)
    Modified = Column(String)
    Stamp = Column(String)
    Preliminary = Column(Integer)

    def files(self):
        return filter(lambda x: x, self.Files.split('\n'))


class Collection(Base):
    __tablename__ = 'collections'

    id = Column(Integer, primary_key=True)
    Name = Column(String)


class ParameterStatus(Base):
    __tablename__ = 'y_parameter_status'

    expocode = Column('ExpoCode', Integer, primary_key=True)
    parameter_id = Column(ForeignKey('parameter_descriptions.id'),
                            primary_key=True)
    pi_id = Column(ForeignKey('contacts.id'), nullable=True)
    status = Column(Enum([u'PRELIMINARY', u'NON-PRELIMINARY']),
                      default=u'PRELIMINARY')

    parameter = relation(Parameter)
    pi = relation(Contact)

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

    id = Column(Integer, primary_key=True)
    ExpoCode = Column(String)


for cpi in CruiseParameterInfo._PARAMETERS:
    setattr(CruiseParameterInfo, cpi, Column(String))
    setattr(CruiseParameterInfo, cpi + '_PI', Column(String))
    setattr(CruiseParameterInfo, cpi + '_Date', Column(String))


class QueueFile(Base):
    __tablename__ = 'queue_files'

    id = Column(Integer, primary_key=True)
    Name = Column(String)
    date_received = Column('DateRecieved', Date)
    date_merged = Column('DateMerged', Date)
    expocode = Column('ExpoCode', String)
    merged = Column('Merged', Integer)
    contact = Column('Contact', String)
    processed_input = Column('ProcessedInput', String)
    notes = Column('Notes', String)
    unprocessed_input = Column('UnprocessedInput', String)
    parameters = Column('Parameters', String)
    action = Column('Action', String)
    cchdo_contact = Column('CCHDOContact', String)
    merge_notes = Column(String)
    hidden = Column(Integer)
    documentation = Column(Integer)
    submission_id = Column(Integer, ForeignKey('submissions.id'))
    submission = relationship('Submission', backref=backref(
        'queue_file', uselist=False))
    
    def is_unmerged(self):
        """Return the unmerged status.

        """
        return self.merged == 0
    
    def is_hidden(self):
        """Return the hidden status.

        """
        return self.merged == 2
    
    def is_merged(self):
        """Return the merge status.

        """
        return self.merged == 1

    def set_merged(self):
        self.merged = 1

    def set_hidden(self):
        self.merged = 2

    def set_unmerged(self):
        self.merged = 0


class Submission(Base):
    __tablename__ = 'submissions'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    institute = Column(String)
    country = Column('Country', String)
    email = Column(String)
    public = Column(String)
    expocode = Column('ExpoCode', String)
    ship_name = Column('Ship_Name', String)
    line = Column('Line', String)
    cruise_date = Column(Date)
    action = Column(String)
    notes = Column(String)
    file = Column(String)
    assigned = Column(Integer)
    assimilated = Column(Integer)
    submission_date = Column(Date)
    ip = Column(String)
    user_agent = Column(String)


class OldSubmission(Base):
    __tablename__ = 'old_submissions'

    id = Column(Integer, primary_key=True)
    Date = Column(Date)
    Stamp = Column(String)
    Name = Column(String)
    Line = Column(String)
    Filename = Column(String)
    Filetype = Column(String)
    Location = Column(String)
    Folder = Column(String)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


class SpatialGroup(Base):
    __tablename__ = 'spatial_groups'

    id = Column(Integer, primary_key=True)
    area = Column(String)
    expocode = Column('ExpoCode', String)

    atlantic = Column(BINARY)
    arctic = Column(BINARY)
    pacific = Column(BINARY)
    indian = Column(BINARY)
    southern = Column(BINARY)


class Internal(Base):
    __tablename__ = 'internal'

    id = Column(Integer, primary_key=True)
    Line = Column(String)
    File = Column(String)
    expocode = Column('ExpoCode', String)
    Basin = Column(String)


class NewTrack(Base):
    __tablename__ = 'z_new_tracks'

    id = Column(Integer, primary_key=True)
    expocode = Column('ExpoCode', String)
    filename = Column('FileName', String)
    basin = Column('Basin', String)
    track = Column('Track', String)


class SupportFile(Base):
    __tablename__ = 'support_files'

    name = Column('Name', String)
    date_received = Column('DateRecieved', String)
    date_merged = Column('DateMerged', String)
    expocode = Column('ExpoCode', String)
    merged = Column('Merged', String)
    contact = Column('Contact', String)
    processed_input = Column('ProcessedInput', String)
    notes = Column('Notes', String)
    unprocessed_input = Column('UnprocessedInput', String)
    contact = Column('Contact', String)
    id = Column(Integer, primary_key=True)
    parameters = Column('Parameters', String)
    action = Column('Action', String)
    description = Column('Description', String)


class UnusedTrack(Base):
    __tablename__ = 'z_unused_tracks'

    id = Column(Integer, primary_key=True)
    expocode = Column('ExpoCode', String)
    filename = Column('FileName', String)
    Basin = Column(String)
    Track = Column(String)


def find_parameter(session, name):
    legacy_parameter = session.query(Parameter).filter(
        Parameter.name == name).first()
    if not legacy_parameter:
        # Try aliases
        LOG.warn(
            "No legacy parameter found for '%s'. Falling back to aliases." % \
            name)
        legacy_parameter = session.query(Parameter).filter(
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
                mysql_parameter_display_orders(session)[legacy_parameter.name]
        except:
            legacy_parameter.display_order = sys.maxint

    return legacy_parameter


