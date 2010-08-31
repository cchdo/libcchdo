import sys
import os.path

import sqlalchemy as S
import sqlalchemy.orm
import sqlalchemy.ext.declarative

import libcchdo
import convert


Base = S.ext.declarative.declarative_base()
_metadata = Base.metadata


@libcchdo.memoize
def session():
    return libcchdo.db.connect.session(libcchdo.db.connect.cchdo_data())


def create_all():
    _metadata.create_all(libcchdo.db.connect.cchdo_data())


class Country(Base):
    __tablename__ = 'countries'

    iso3166 = S.Column(S.String(2), primary_key=True)
    name = S.Column(S.String(255))

    def __init__(self, iso3166, name=None):
        self.iso3166 = iso3166
        if name:
            self.name = name

    def __repr__(self):
        return "<Country('%s', '%s')>" % (self.iso3166, self.name)


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
        return "<Institution('%s')>" % self.name


class Contact(Base):
    __tablename__ = 'contacts'

    id = S.Column(S.Integer, primary_key=True)
    name = S.Column(S.String(255)) # TODO
    institution_id = S.Column(S.ForeignKey('institutions.id'))

    institution = S.orm.relation(Institution,
                                 backref=S.orm.backref('contacts',
                                                       lazy='dynamic'))

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "<Contact('%s')>" % self.name


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
        return "<Ship('%s', '%s')>" % (self.name, self.code_NODC)


cruises_pis = S.Table('cruises_pis', _metadata,
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
        return "<CruiseAlias('%s')>" % self.name


class Project(Base):
    __tablename__ = 'projects'

    id = S.Column(S.Integer, primary_key=True)
    name = S.Column(S.String)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "<Project('%s')>" % self.name


cruises_projects = S.Table('cruises_projects', _metadata,
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
        return "<Port('%s')>" % self.name


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
        return "<Cruise('%s', '%s')>" % (self.expocode, self.casts)


class Unit(Base):
    __tablename__ = 'units'

    id = S.Column(S.Integer, primary_key=True)
    name = S.Column(S.String(255))
    mnemonic = S.Column(S.String(8))

    def __init__(self, name, mnemonic=None):
        self.name = name
        self.mnemonic = mnemonic

    def __repr__(self):
        return "<Unit('%s', '%s')>" % (self.name, self.mnemonic)


class ParameterAlias(Base):
    __tablename__ = 'parameter_aliases'

    parameter_id = S.Column(S.ForeignKey('parameters.id', ondelete='CASCADE'))
    name = S.Column(S.String(255), primary_key=True)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "<ParameterAlias('%s')>" % self.name


class Parameter(Base):
    __tablename__ = 'parameters'

    id = S.Column(S.Integer, primary_key=True)
    name = S.Column(S.String(255))
    full_name = S.Column(S.String(255))
    format = S.Column(S.String(10))
    unit_id = S.Column(S.ForeignKey('units.id'))
    bound_lower = S.Column(S.Numeric)
    bound_upper = S.Column(S.Numeric)
    display_order = S.Column(S.Integer(10))

    units = S.orm.relation(Unit)
    aliases = S.orm.relation(
        ParameterAlias, backref=S.orm.backref('parameter', lazy='dynamic'))

    def mnemonic_woce(self):
        return self.name

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

    def __eq__(self, other):
        return self.name == other.name

    def __repr__(self):
        return "<Parameter('%s', '%s', '%s', '%s')>" % (self.name, self.format, self.units, self.aliases)


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
        return "<Cast('%s', '%s')>" % (self.name, self.station)


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
        return "<Location('%s', '%s', '%s', '%s')>" % \
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
        return "<CTD('%s', '%s', '%s')>" % (self.cast, self.location,
                                            self.instrument_id)


class DataCTD(Base):
    __tablename__ = 'data_ctds'

    ctd_id = S.Column(S.ForeignKey('ctds.id', ondelete='CASCADE'),
                      primary_key=True)
    parameter_id = S.Column(S.ForeignKey('parameters.id', ondelete='CASCADE'),
                            primary_key=True)
    value = S.Column(S.Numeric)
    flag_woce = S.Column(S.Integer(10))
    flag_igoss = S.Column(S.Integer(10))

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
        return "<DataCTD('%s', '%s', '%s', '%s', '%s')>" % \
            (self.ctd, self.parameter, self.value, self.flag_woce, self.flag_igoss)


class Bottle(Base):
    __tablename__ = 'bottles'

    id = S.Column(S.Integer, primary_key=True)
    cast_id = S.Column(S.ForeignKey('casts.id', ondelete='CASCADE'))
    location_id = S.Column(S.ForeignKey('locations.id'))
    name = S.Column(S.String(10))
    sample = S.Column(S.String(10))
    flag_woce = S.Column(S.Integer(10))
    flag_igoss = S.Column(S.Integer(10))
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
        return "<Bottle('%s', '%s', '%s', '%s', '%s', '%s')>" % \
            (self.cast, self.location, self.name, self.sample, self.flag_woce,
             self.flag_igoss)


class DataBottle(Base):
    __tablename__ = 'data_bottles'

    bottle_id = S.Column(S.ForeignKey('bottles.id', ondelete='CASCADE'),
                         primary_key=True)
    parameter_id = S.Column(S.ForeignKey('parameters.id', ondelete='CASCADE'),
                            primary_key=True)
    value = S.Column(S.Numeric)
    flag_woce = S.Column(S.Integer(10))
    flag_igoss = S.Column(S.Integer(10))

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
        return "<DataBottle('%s', '%s', '%s', '%s', '%s')>" % \
            (self.bottle, self.parameter, self.value,
             self.flag_woce, self.flag_igoss)


# Initialize the database file if it is not present

library_db_file_path = os.path.join(libcchdo._get_library_abspath(), 
    'db', libcchdo.db.connect._DB_LIBRARY_FILE)

if not os.path.isfile(library_db_file_path):
    libcchdo.warn(("The library's database file (%s) is not present. Auto-"
                   "generation is taking place.") % library_db_file_path)
    create_all()

    std_session = session()
    std_session.add_all(convert.all_parameters())
    std_session.commit()
    std_session.close()
