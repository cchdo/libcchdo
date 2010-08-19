import sqlalchemy as S
import sys

import libcchdo
import libcchdo.db.model.std as STD

@libcchdo.memoize
def find_or_create_project(session, project):
    print 'FINDORCREATE PROJECT', project
    project = session.query(STD.Project).filter(
        STD.Project.name==project).first()

    if not project:
        project = STD.Project(project)

    return project


@libcchdo.memoize
def find_or_create_cruise(session, expocode):
    print 'FINDORCREATE CRUISE', expocode
    cruise = session.query(STD.Cruise).filter(
        STD.Cruise.expocode==expocode).first()

    if not cruise:
        cruise = STD.Cruise(expocode)
        session.add(cruise)

    return cruise


@libcchdo.memoize
def find_or_create_cast(session, cruise, castno, station):
    print 'FINDORCREATE CAST', castno, station
    cast = cruise.casts.filter(
        S.and_(STD.Cast.name==str(castno),
               STD.Cast.station==str(station))).first()

    if not cast:
        cast = STD.Cast(cruise, castno, station)

    if cast not in cruise.casts:
        cruise.casts.append(cast)

    return cast


@libcchdo.memoize
def find_or_create_location(session, latitude, longitude, bottom_depth, datetime):
    print 'FINDORCREATE LOCATION', latitude, longitude, bottom_depth, datetime
    location = session.query(STD.Location).filter(
        S.and_(STD.Location.latitude == latitude,
               STD.Location.longitude == longitude,
               STD.Location.bottom_depth == bottom_depth,
               STD.Location.datetime == datetime)).first()

    if not location:
        location = STD.Location(
            datetime, latitude, longitude, bottom_depth)

    return location


def convert(datafile):
    '''Return an array of cruises in the datafile with the data represented 
       in objects as a bottle.'''

    # TODO use the std parameters?
    print [(x.parameter, x.parameter.id) for x in datafile.columns.values()]

    cruises = {}
    session = STD.session()

    columns = datafile.sorted_columns()
    data = datafile.columns

    parameters = map(lambda x: x.parameter.name, columns)
    metadata_parameters = set(('EXPOCODE', 'SECT_ID', 'STNNBR', 'CASTNO',
                               'LATITUDE', 'LONGITUDE', 'DEPTH', '_DATETIME',
                               'SAMPNO', 'BTLNBR', ))
    data_parameters = [x for x in parameters if x not in metadata_parameters]
    data_columns = [data[x] for x in parameters]
    data_flagged_woce = [x.is_flagged_woce() for x in data_columns]
    data_flagged_igoss = [x.is_flagged_igoss() for x in data_columns]
    data_data = zip(data_parameters, data_columns,
                    data_flagged_woce, data_flagged_igoss)

    expocode_col = data['EXPOCODE']
    sectid_col = data['SECT_ID']
    stnnbr_col = data['STNNBR']
    castno_col = data['CASTNO']
    latitude_col = data['LATITUDE']
    longitude_col = data['LONGITUDE']
    depth_col = data['DEPTH']
    datetime_col = data['_DATETIME']
    sampno_col = data['SAMPNO']
    btl_col = data['BTLNBR']

    for i in range(len(datafile)):
        expocode = expocode_col[i]
        try:
            cruise = cruises[expocode]
        except:
            cruise = find_or_create_cruise(session, expocode)
            cruises[expocode] = cruise

        project = find_or_create_project(session, sectid_col[i])
        if project not in cruise.projects:
            cruise.projects.append(project)

        cast = find_or_create_cast(session, cruise, castno_col[i], stnnbr_col[i])

        location = find_or_create_location(
            session, latitude_col[i], longitude_col[i], depth_col[i],
            datetime_col[i])

        # Find or create Bottle (most likely create because bottle files
        # are bottle per line)

        try:
            sampno = sampno_col[i]
        except:
            sampno = None
        btlnbr = btl_col[i]
        btl_flag_woce = None
        btl_flag_igoss = None
        if btl_col.is_flagged_woce():
            btl_flag_woce = btl_col.flags_woce[i]
        if btl_col.is_flagged_igoss():
            btl_flag_igoss = btl_col.flags_igoss[i]

        bottle = cast.bottles.filter(
            S.and_(STD.Bottle.location_id == location.id,
                   STD.Bottle.name == str(btlnbr))).first()

        if not bottle:
            bottle = STD.Bottle(cast, location, btlnbr, sampno,
                                btl_flag_woce, btl_flag_igoss)
            cast.bottles.append(bottle)

        print >> sys.stderr, '_',
        data_bottles = []
        for param, column, flagged_woce, flagged_igoss in data_data:
            data_bottles.append({
                'bottle_id': bottle.id,
                'parameter_id': column.parameter.id,
                'value': column.values[i],
                'flag_woce': column.flags_woce[i] if flagged_woce else None,
                'flag_igoss': column.flags_woce[i] if flagged_igoss else None
            })
        session.execute(STD.DataBottle.__table__.insert(), data_bottles)
#            STD.DataBottle(bottle, column.parameter, column.values[i],
#                           column.flags_woce[i] if flagged_woce else None,
#                           column.flags_woce[i] if flagged_igoss else None)
        print >> sys.stderr, '.',

        if i % 30 == 0:
            print i 

    session.commit()

    return cruises

