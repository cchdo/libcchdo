import sys

import sqlalchemy as S
import sqlalchemy.orm
import sqlalchemy.ext.declarative

import libcchdo
import libcchdo.db.connect

Base = S.ext.declarative.declarative_base()
_metadata = Base.metadata


class Contact(Base):
    __tablename__ = 'contacts'

    id = S.Column(S.Integer, primary_key=True)
    name = S.Column(S.String(255)) # TODO

    def __init__(self):
        pass


class Ship(Base):
    __tablename__ = 'ships'

    id = S.Column(S.Integer, primary_key=True)
    name = S.Column(S.String(20))
    code_NODC = S.Column(S.String(6))
    country_id = S.Column(S.ForeignKey('countries.iso3166'))

    def __init__(self):
        pass


cruises_pis = S.Table('cruises_pis', _metadata,
    S.Column('pi_id', S.ForeignKey('contacts.id')),
    S.Column('cruise_id', S.ForeignKey('cruises.id')),
)


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
        Ship, backref=S.orm.backref(__tablename__, order_by=id))
    pis = S.orm.relation(Contact, secondary=cruises_pis, backref=__tablename__)

    def __init__(self, expocode):
        self.expocode = expocode


class Country(Base):
    __tablename__ = 'countries'

    iso3166 = S.Column(S.String(2), primary_key=True)
    name = S.Column(S.String(255))

    def __init__(self):
        pass


class Port(Base):
    __tablename__ = 'ports'

    id = S.Column(S.Integer, primary_key=True)
    name = S.Column(S.String(255))

    def __init__(self):
        pass


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

    parameter_id = S.Column(S.ForeignKey('parameters.id'))
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
    aliases = S.orm.relation(ParameterAlias, backref='parameter')

    def mnemonic_woce(self):
        return self.name

    def __init__(self, name, fullname=None, format=None, bound_lower=None,
                 bound_upper=None, display_order=None):
        self.name = name

    def __eq__(self, other):
        return self.name == other.name


@libcchdo.memoize
def session():
    return libcchdo.db.connect.session(libcchdo.db.connect.cchdo_data())


def create_all():
    _metadata.create_all(libcchdo.db.connect.cchdo_data())


