from copy import copy


from libcchdo.model.datafile import DataFileCollection


def split_on_cast(dfile):
    """Split a DataFile that has multiple casts into a DataFileCollection.

    Splits are done based on station cast. Each cast is a new 'file'.

    """
    coll = DataFileCollection()

    file_parameters = dfile.parameter_mnemonics_woce()

    current_file = copy(dfile)

    expocodes = dfile['EXPOCODE']
    stations = dfile['STNNBR']
    casts = dfile['CASTNO']

    expocode = expocodes[0]
    station = stations[0]
    cast = casts[0]
    for i in range(len(dfile)):
        # Check if this row is a new measurement location
        if expocodes[i] != expocode or \
           stations[i] != station or \
           casts[i] != cast:
            current_file.check_and_replace_parameters()
            coll.append(current_file)
            current_file = copy(dfile)
        expocode = expocodes[i]
        station = stations[i]
        cast = casts[i]

        # Put the current row in the current dfile
        for p in file_parameters:
            source_col = dfile[p]
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
