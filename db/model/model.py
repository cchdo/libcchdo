#!/usr/bin/env python

import sqlalchemy as S
import sqlalchemy.orm
import sqlalchemy.ext.declarative

Base = S.ext.declarative.declarative_base()
metadata = Base.metadata


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
    country_id = S.Column(None, S.ForeignKey('countries.iso3166'))

    def __init__(self):
        pass


cruises_pis = S.Table('cruises_pis', metadata,
    S.Column('pi_id', None, S.ForeignKey('contacts.id')),
    S.Column('cruise_id', None, S.ForeignKey('cruises.id')),
)


class Cruise(Base):
    __tablename__ = 'cruises'

    id = S.Column(S.Integer, primary_key=True)
    expocode = S.Column(S.String(11))
    ship_id = S.Column(None, S.ForeignKey('ships.id'))
    start_date = S.Column(S.Integer) # TODO
    end_date = S.Column(S.Integer) # TODO
    start_port = S.Column(None, S.ForeignKey('ports.id'))
    end_port_id = S.Column(None, S.ForeignKey('ports.id'))
    country_id = S.Column(None, S.ForeignKey('countries.iso3166'))

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

engine = S.create_engine('sqlite:///:memory:', echo=True)
metadata.create_all(engine)
