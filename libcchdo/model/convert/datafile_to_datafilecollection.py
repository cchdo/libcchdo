import copy


from .. import datafile


def split_bottle(file):
    """ Split apart the bottle exchange file into a data file collection based
        on station cast. Each cast is a new 'file'.
    """
    coll = datafile.DataFileCollection()

    file_parameters = file.parameter_mnemonics_woce()

    current_file = copy.copy(file)

    expocodes = file['EXPOCODE']
    stations = file['STNNBR']
    casts = file['CASTNO']

    expocode = expocodes[0]
    station = stations[0]
    cast = casts[0]
    for i in range(len(file)):
        # Check if this row is a new measurement location
        if expocodes[i] != expocode or \
           stations[i] != station or \
           casts[i] != cast:
            current_file.check_and_replace_parameters()
            coll.append(current_file)
            current_file = copy.copy(file)
        expocode = expocodes[i]
        station = stations[i]
        cast = casts[i]

        # Put the current row in the current file
        for p in file_parameters:
            source_col = file[p]
            value = source_col[i]
            try:
                flag_woce = source_col.flags_woce[i]
            except IndexError:
                flag_woce = None
            try:
                flag_igoss = source_col.flags_igoss[i]
            except IndexError:
                flag_igoss = None
            current_file[p].append(value, flag_woce, flag_igoss)

    current_file.check_and_replace_parameters()
    coll.append(current_file)

    return coll
