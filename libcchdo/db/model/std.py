import sys
import os.path
from contextlib import contextmanager, closing
from threading import Lock

import sqlalchemy as S
import sqlalchemy.orm
import sqlalchemy.ext.declarative
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql.expression import exists

from libcchdo import config, check_cache
from libcchdo.log import LOG
from libcchdo.fns import _decimal
from libcchdo.db import connect
from legacy import session as LegacySession


Base = S.ext.declarative.declarative_base()


_cache_checked = False
_cache_checking = Lock()


_global_session = None


def session(no_global=False):
    global _global_session
    if check_cache:
        _ensure_database_cache()
    if _global_session and not no_global:
        return _global_session
    session = connect.session(connect.cchdo_data())
    if not session:
        raise ValueError("Unable to connect to local cache db cchdo_data")
    if not _global_session:
        _global_session = session
    return session


def _populate_library_database_parameters(std_session):
    LOG.info("Populating database with parameters.")

    # TODO have multiple strategies to download parameter information
    # a JSON API will be provided by pycchdo so use that when it becomes
    # available

    with closing(LegacySession()) as legacy_session:
        from libcchdo.db.model.convert import all_parameters as cvt_all_params
        return cvt_all_params(legacy_session, std_session)


def _regenerate_database_cache(std_session):
    Base.metadata.drop_all(std_session.get_bind())
    Base.metadata.create_all(std_session.get_bind())
    _populate_library_database_parameters(std_session)
            

def _ensure_database_cache():
    """Initialize the database cache if it is not present.

    WARNING: Do not call this while importing ...db.model.std. There will be
    a circular dependency as ...db.model.convert needs this module defined.

    """
    global _cache_checked, _cache_checking
    if not _cache_checking.acquire(False):
        return
    if _cache_checked:
        _cache_checking.release()
        return

    with closing(session()) as std_session:
        db_cache_path = config.get_option('db', 'cache')
        if not os.path.isfile(db_cache_path):
            LOG.info(u"Generating cache (%s)." % db_cache_path)
            Base.metadata.create_all(std_session.get_bind())

        try:
            if not std_session.query(Parameter).count():
                _populate_library_database_parameters(std_session)
        except OperationalError, e:
            LOG.info(
                u'Database operational error possibly due to schema change.'
                'Regenerating cache. Original error: %s' % e)
            _regenerate_database_cache(std_session)
        std_session.commit()

    _cache_checked = True
    _cache_checking.release()


class Country(Base):
    __tablename__ = 'countries'

    iso3166 = S.Column(S.String(2), primary_key=True)
    name = S.Column(S.String(255))

    def __init__(self, iso3166, name=None):
        self.iso3166 = iso3166
        if name:
            self.name = name

    def __repr__(self):
        return "<Country(%r, %r)>" % (self.iso3166, self.name)


class Institution(Base):
    __tablename__ = 'institutions'

    id = S.Column(S.Integer, primary_key=True)
    name = S.Column(S.String)
    country_id = S.Column(S.ForeignKey('countries.iso3166'))

    country = S.orm.relation(Country, backref=S.orm.backref('institutions',
                                                            lazy='dynamic'))

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "<Institution(%r)>" % self.name


class Contact(Base):
    __tablename__ = 'contacts'

    id = S.Column(S.Integer, primary_key=True)
    name = S.Column(S.String(255))
    institution_id = S.Column(S.ForeignKey('institutions.id'))

    institution = S.orm.relation(Institution,
                                 backref=S.orm.backref('contacts',
                                                       lazy='dynamic'))

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "<Contact(%r)>" % self.name


class Ship(Base):
    __tablename__ = 'ships'

    id = S.Column(S.Integer, primary_key=True)
    name = S.Column(S.String(20))
    code_NODC = S.Column(S.String(6))
    country_id = S.Column(S.ForeignKey('countries.iso3166'))

    country = S.orm.relation(Country,
                             backref=S.orm.backref('ships', lazy='dynamic'))

    def __init__(self, name, code_NODC=None):
        self.name = name
        if code_NODC:
            self.code_NODC = ncode_NODC

    def __repr__(self):
        return "<Ship(%r, %r)>" % (self.name, self.code_NODC)


cruises_pis = S.Table('cruises_pis', Base.metadata,
    S.Column('pi_id', S.ForeignKey('contacts.id', ondelete='CASCADE')),
    S.Column('cruise_id', S.ForeignKey('cruises.id', ondelete='CASCADE')),
)


class CruiseAlias(Base):
    __tablename__ = 'cruise_aliases'

    name = S.Column(S.String, primary_key=True)
    cruise_id = S.Column(S.ForeignKey('cruises.id'))

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "<CruiseAlias(%r)>" % self.name


class Project(Base):
    __tablename__ = 'projects'

    id = S.Column(S.Integer, primary_key=True)
    name = S.Column(S.String)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "<Project(%r)>" % self.name


cruises_projects = S.Table('cruises_projects', Base.metadata,
     S.Column('cruise_id', S.ForeignKey('cruises.id', ondelete='CASCADE')),
     S.Column('project_id', S.ForeignKey('projects.id', ondelete='CASCADE')),
)


class Port(Base):
    __tablename__ = 'ports'

    id = S.Column(S.Integer, primary_key=True)
    name = S.Column(S.String(255))

    def __init__(self):
        pass

    def __repr__(self):
        return "<Port(%r)>" % self.name


class Cruise(Base):
    __tablename__ = 'cruises'

    id = S.Column(S.Integer, primary_key=True)
    expocode = S.Column(S.String(11))
    ship_id = S.Column(S.ForeignKey('ships.id'))
    start_date = S.Column(S.Integer) # TODO
    end_date = S.Column(S.Integer) # TODO
    start_port = S.Column(S.ForeignKey('ports.id'))
    end_port_id = S.Column(S.ForeignKey('ports.id'))
    country_id = S.Column(S.ForeignKey('countries.iso3166'))

    ship = S.orm.relation(
        Ship, backref=S.orm.backref('cruises', order_by=id, lazy='dynamic'))
    pis = S.orm.relation(Contact, secondary=cruises_pis,
                         backref=S.orm.backref('cruises', lazy='dynamic'))
    projects = S.orm.relation(Project, secondary=cruises_projects,
                              backref=S.orm.backref('cruises', lazy='dynamic'))

    def __init__(self, expocode):
        self.expocode = expocode

    def __repr__(self):
        return "<Cruise(%r, %r)>" % (self.expocode, self.casts)


class Unit(Base):
    __tablename__ = 'units'

    id = S.Column(S.Integer, primary_key=True)
    name = S.Column(S.Unicode)
    mnemonic = S.Column(S.String(8))

    def __init__(self, name, mnemonic=None):
        self.name = name
        self.mnemonic = mnemonic

    def __eq__(self, other):
        try:
            return self.name == other.name and self.mnemonic == other.mnemonic
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return u"<Unit(%r, %r)>" % (self.name, self.mnemonic)

    @classmethod
    def find_by_name(cls, name):
        return session().query(Unit).filter(Unit.name == name).first()


S.Index('units_name', Unit.name, unique=True)


class ParameterAlias(Base):
    __tablename__ = 'parameter_aliases'

    parameter_id = S.Column(S.ForeignKey('parameters.id', ondelete='CASCADE'))
    name = S.Column(S.String(255), primary_key=True)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "<ParameterAlias(%r)>" % self.name


class Parameter(Base):
    __tablename__ = 'parameters'

    id = S.Column(S.Integer, primary_key=True)
    name = S.Column(S.Unicode)
    full_name = S.Column(S.Unicode)
    name_netcdf = S.Column(S.Unicode)
    description = S.Column(S.Unicode)
    format = S.Column(S.String(10))
    unit_id = S.Column(S.ForeignKey('units.id'))
    bound_lower = S.Column(S.Numeric)
    bound_upper = S.Column(S.Numeric)
    display_order = S.Column(S.Integer)

    units = S.orm.relation(Unit)
    aliases = S.orm.relation(
        ParameterAlias, backref=S.orm.backref('parameter'), lazy='immediate')

    def mnemonic_woce(self):
        return self.name.encode('ascii', 'replace')

    def __init__(self, name, full_name=None, format=None, units=None,
                 bound_lower=None, bound_upper=None, display_order=None):
        self.name = name
        if full_name:
            self.full_name = full_name
        if format:
            self.format = format
        if units:
            self.units = units
        if bound_lower:
            self.bound_lower = bound_lower
        if bound_upper:
            self.bound_upper = bound_upper
        if display_order:
            self.display_order = display_order

    def __ne__(self, other):
        return not (self == other)

    def __eq__(self, other):
        if self is None:
            return False
        if other is None:
            return False
        return self.name == other.name

    def is_in_range(self, x):
        x = _decimal(x)
        if self.bound_lower is not None:
            if x < _decimal(self.bound_lower):
                return False
        if self.bound_upper is not None:
            if x > _decimal(self.bound_upper):
                return False
        return True

    def __repr__(self):
        return u"<Parameter(%r, %r, %r, %r, %r)>" % (
            self.name, self.format, self.units, self.aliases,
            self.display_order)


S.Index('parameters_name_netcdf', Parameter.name_netcdf, unique=True)


class Cast(Base):
    __tablename__ = 'casts'

    id = S.Column(S.Integer, primary_key=True)
    cruise_id = S.Column(S.ForeignKey('cruises.id', ondelete='CASCADE'))
    name = S.Column(S.String(10))
    station = S.Column(S.String(10))

    cruise = S.orm.relation(Cruise,
                            backref=S.orm.backref('casts', lazy='dynamic'))

    def __init__(self, cruise, name, station):
        self.cruise = cruise
        self.name = name
        self.station = station

    def __repr__(self):
        return "<Cast(%r, %r)>" % (self.name, self.station)


class Location(Base):
    __tablename__ = 'locations'
    
    id = S.Column(S.Integer, primary_key=True)
    datetime = S.Column(S.DateTime)
    latitude = S.Column(S.Numeric)
    longitude = S.Column(S.Numeric)
    bottom_depth = S.Column(S.Integer)

    def __init__(self, datetime, latitude, longitude, bottom_depth):
        self.datetime = datetime
        self.latitude = latitude
        self.longitude = longitude
        self.bottom_depth = bottom_depth

    def __repr__(self):
        return "<Location(%r, %r, %r, %r)>" % \
            (self.datetime, self.latitude, self.longitude, self.bottom_depth)


S.Index('locations_uniq',
        Location.datetime, Location.latitude, Location.longitude,
        Location.bottom_depth,
        unique=True)


class CTD(Base):
    __tablename__ = 'ctds'

    id = S.Column(S.Integer, primary_key=True)
    cast_id = S.Column(S.ForeignKey('casts.id', ondelete='CASCADE'))
    location_id = S.Column(S.ForeignKey('locations.id'))
    instrument_id = S.Column(S.Integer)

    cast = S.orm.relation(Cast, backref=S.orm.backref('ctds', lazy='dynamic'))
    location = S.orm.relation(Location, backref=S.orm.backref('ctds', lazy='dynamic'))

    def __init__(self, cast, location, instrument_id):
        self.cast = cast
        self.location = location
        self.instrument_id = instrument_id

    def __repr__(self):
        return "<CTD(%r, %r, %r)>" % (self.cast, self.location,
                                            self.instrument_id)


class DataCTD(Base):
    __tablename__ = 'data_ctds'

    ctd_id = S.Column(S.ForeignKey('ctds.id', ondelete='CASCADE'),
                      primary_key=True)
    parameter_id = S.Column(S.ForeignKey('parameters.id', ondelete='CASCADE'),
                            primary_key=True)
    value = S.Column(S.Numeric)
    flag_woce = S.Column(S.Integer)
    flag_igoss = S.Column(S.Integer)

    ctd = S.orm.relation(CTD,
                         backref=S.orm.backref('data_ctd', lazy='dynamic'))
    parameter = S.orm.relation(Parameter,
                               backref=S.orm.backref('data', lazy='dynamic'))

    def __init__(self, ctd, parameter, value, flag_woce=None, flag_igoss=None):
        self.ctd = ctd
        self.parameter = parameter
        self.value = value
        if flag_woce:
            self.flag_woce = flag_woce
        if flag_igoss:
            self.flag_igoss = flag_igoss

    def __repr__(self):
        return "<DataCTD(%r, %r, %r, %r, %r)>" % \
            (self.ctd, self.parameter, self.value, self.flag_woce, self.flag_igoss)


class Bottle(Base):
    __tablename__ = 'bottles'

    id = S.Column(S.Integer, primary_key=True)
    cast_id = S.Column(S.ForeignKey('casts.id', ondelete='CASCADE'))
    location_id = S.Column(S.ForeignKey('locations.id'))
    name = S.Column(S.String(10))
    sample = S.Column(S.String(10))
    flag_woce = S.Column(S.Integer)
    flag_igoss = S.Column(S.Integer)
    latitude = S.Column(S.Numeric)
    longitude = S.Column(S.Numeric)

    cast = S.orm.relation(Cast, backref=S.orm.backref('bottles',
                                                      lazy='dynamic'))
    location = S.orm.relation(Location,
                              backref=S.orm.backref('bottles', lazy='dynamic'))

    def __init__(self, cast, location, name, sample=None,
                 flag_woce=None, flag_igoss=None):
        self.cast = cast
        self.location = location
        self.name = name
        if sample:
            self.sample = sample
        if flag_woce:
            self.flag_woce = flag_woce
        if flag_igoss:
            self.flag_igoss = flag_igoss

    def __repr__(self):
        return "<Bottle(%r, %r, %r, %r, %r, %r)>" % \
            (self.cast, self.location, self.name, self.sample, self.flag_woce,
             self.flag_igoss)


class DataBottle(Base):
    __tablename__ = 'data_bottles'

    bottle_id = S.Column(S.ForeignKey('bottles.id', ondelete='CASCADE'),
                         primary_key=True)
    parameter_id = S.Column(S.ForeignKey('parameters.id', ondelete='CASCADE'),
                            primary_key=True)
    value = S.Column(S.Numeric)
    flag_woce = S.Column(S.Integer)
    flag_igoss = S.Column(S.Integer)

    bottle = S.orm.relation(Bottle,
                            backref=S.orm.backref('data', lazy='dynamic'))
    parameter = S.orm.relation(
        Parameter, backref=S.orm.backref('data_bottle', lazy='dynamic'))

    def __init__(self, bottle, parameter, value, flag_woce=None, flag_igoss=None):
        self.bottle = bottle
        self.parameter = parameter
        self.value = value
        if flag_woce:
            self.flag_woce = flag_woce
        if flag_igoss:
            self.flag_igoss = flag_igoss

    def __repr__(self):
        return "<DataBottle(%r, %r, %r, %r, %r)>" % \
            (self.bottle, self.parameter, self.value,
             self.flag_woce, self.flag_igoss)


def make_contrived_parameter(name, format=None, units=None, bound_lower=None,
                             bound_upper=None, display_order=sys.maxint):
    return Parameter(
        name,
        full_name=name,
        format=format or '%11s', 
        units=Unit(units, units) if units else None,
        bound_lower=bound_lower,
        bound_upper=bound_upper,
        display_order=display_order)


def find_by_mnemonic(name):
    parameter = session().query(Parameter).\
        filter(Parameter.name == name).first()
    if not parameter:
        alias = session().query(ParameterAlias).filter(
            ParameterAlias.name == name).first()
        if alias:
            parameter = alias.parameter
            LOG.info("%s is an alias for %s" % (name, parameter))
        else:
            parameter = None
            LOG.warn("%s is not a recognized parameter" % name)
    return parameter
