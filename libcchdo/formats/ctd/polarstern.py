# -*- coding: utf-8 -*-
""" A second attempt at making a Polarstern CTD reader, mostly because the
first version did not make sense at all.

This attempt tries to be a bit more straightforward with the way it reads the
Polarstern files. It will use better function abstraction to try to make things
fit into the libcchdo.model.datafile.DataFile, and it will not be full of magic
dictionaries.
"""
from datetime import datetime
from copy import copy
import re

from libcchdo import LOG
from libcchdo.model.datafile import DataFileCollection


preamble = """\
Datafile read from Polarstern CTD format. Please verify before use.
Original data acquired from CD
Reference website: http://www.awi.de/en/research/research_divisions/climate_science/observational_oceanography

"""


class PolarsternCTDParameter (object):
    """ Parameter magic dictionary.
    FIXME this should end up in a database somewhere.
    """
    Equivalents = {
            u"Event": {
            "name": {"polarstern": "Event", "cchdo": "_EVENT", },
            "units": {"polarstern": "", "cchdo": "", },
            },
            u"Date/Time": {
            "name": {"polarstern": "Date/Time", "cchdo": "_DATETIME", },
            "units": {"polarstern": "", "cchdo": "", },
            },
            u"Depth water": {
            "name": {"polarstern": "DEPTH, water", "cchdo": None, },
            "units": {"polarstern": "m", "cchdo": None, },
            },
            u"Latitude": {
            "name": {"polarstern": "LATITUDE", "cchdo": "LATITUDE", },
            "units": {"polarstern": None, "cchdo": "", },
            },
            u"Longitude": {
            "name": {"polarstern": "LONGITUDE", "cchdo": "LONGITUDE", },
            "units": {"polarstern": None, "cchdo": "", },
            },
            u"Elevation": {
            "name": {"polarstern": "Elevation of event", "cchdo": None, },
            "units": {"polarstern": "m", "cchdo": None, },
            },
            u"Press": {
            "name": {"polarstern": "Pressure, water", "cchdo": "CTDPRS", },
            "units": {"polarstern": "dbar", "cchdo": "DBAR", },
            },
            u"Temp": {
            "name": {"polarstern": "Temperature, water", "cchdo": "CTDTMP", },
            "units": {"polarstern": "deg C", "cchdo": "ITS-90", },
            },
            u"Cond": {
            "name": {"polarstern": "Conductivity", "cchdo": None, },
            "units": {"polarstern": "mS/cm", "cchdo": None, },
            },
            u"Sal": {
            "name": {"polarstern": "Salinity", "cchdo": "CTDSAL", },
            "units": {"polarstern": None, "cchdo": "PSS-78", },
            },
            u"Tpot": {
            "name": {"polarstern": "Temperature, water, potential", "cchdo": None, },
            "units": {"polarstern": "deg C", "cchdo": None, },
            },
            u"Sigma-theta": {
            "name": {"polarstern": "Density, sigma-theta", "cchdo": None, },
            "units": {"polarstern": "kg/m**3", "cchdo": None, },
            },
            u"O2": {
            "name": {"polarstern": "Oxygen", "cchdo": "CTDOXY", },
            "units": {"polarstern": "µmol/l", "cchdo": "UMOL/L", },
            },
            u"O2 sat": {
            "name": {"polarstern": "Oxygen, saturation", "cchdo": None, },
            "units": {"polarstern": None, "cchdo": None, },
            },
            u"Attenuation": {
            "name": {"polarstern": "Attenuation", "cchdo": None, },
            "units": {"polarstern": "%", "cchdo": None, },
            },
            u"Atten": {
            "name": {"polarstern": "Attenuation", "cchdo": None, },
            "units": {"polarstern": "%", "cchdo": None, },
            },
            u"NOBS": {
            "name": {"polarstern": "Number of observations", "cchdo": "CTDNOBS", },
            "units": {"polarstern": "#", "cchdo": None, },
            },
            u"Fluorometer": {
            "name": {"polarstern": "Fluorometer", "cchdo": "FLUOR", },
            "units": {"polarstern": "arbitrary units", "cchdo": None, },
            },
            u"Chl fluores": {
            "name": {"polarstern": "Chlorophyll fluorescence raw data", "cchdo": "FLUOR", },
            "units": {"polarstern": "V", "cchdo": "MG/M^3", },
            },
            u"ys-fl": {
            "name": {"polarstern": "Yellow substance fluorescence raw data", "cchdo": None, },
            "units": {"polarstern": "V", "cchdo": None, },
            },
    }

    @staticmethod
    def get(abbreviated_name=None, ):
        """ Get the CCHDO equivalent of a Polarstern CTD parameter by looking up
        the abbreviated parameter name.

        Parameter:
            abbreviated_name (str) - Abbreviated Polarstern CTD parameter name.

        Returns:
            tuple<str, str> containing the CCHDO parameter name and units that
                    are equivalent to the specified parameter, or (None, None)
                    if no equivalent is found or required.
        """
        if abbreviated_name is None or \
                type(abbreviated_name) not in (str, unicode):
            raise TypeError, ("expected abbreviated Polarstern CTD "
                    "parameter name but got %r" % abbreviated_name)
        n = abbreviated_name
        if n not in PolarsternCTDParameter.Equivalents:
            raise KeyError, ("abbreviated Polarstern CTD parameter "
                    "name %r is not recognized" % abbreviated_name)
        return (PolarsternCTDParameter.Equivalents[n]["name"]["cchdo"],
                PolarsternCTDParameter.Equivalents[n]["units"]["cchdo"], )


class InvalidFileFormatException (Exception):
    """ Exception class used to indicate that a given file is invalid."""
    pass


def _parse_data_description(lines):
    """Parse the DATA DESCRIPTION from a Polarstern CTD file.

    A valid DATA DESCRIPTION header follows this context-free grammar:

    data_description ::= data_description_header
                         NEWLINE
                         optional_metadata_lines
                         data_description_end ;
    data_description_header ::= "/* DATA DESCRIPTION:" ;
    optional_metadata_lines ::=
                              | metadata_lines ;
    metadata_lines ::= optional_metadata_name
                       ':'
                       TAB
                       metadata_value
                       NEWLINE ;
    optional_metadata_name ::=
                             | TEXT ;
    metadata_value ::= TEXT ;
    data_description_end ::= "*/"
                             NEWLINE;

    Each metadata category has its own format and needs to be parsed. This
    function only deals with splitting the category from the value.

    Parameters:
        lines - lines containing a Data description block without the leading
        and trailing comment delimiters.

    Returns:
        TODO describe!

    """
    metadata_name = None
    metadata = {}

    for line in lines:
        # Break the line into metadata category and value(s).
        tokens = line.split('\t', 1)

        # Continuations of previously selected metadata are denoted by having a
        # line start with a tab. If the metadata name exists, switch to it.
        if tokens[0]:
            metadata_name = tokens[0].rstrip(u':')
            if metadata_name not in metadata:
                metadata[metadata_name] = []

        # Escape the metadata value to protect from strange characters.
        try:
            value_unicode = unicode(tokens[1], 'raw_unicode_escape')
        except IndexError, err:
            LOG.error(u'Not enough tokens: {0!r}'.format(tokens))
            raise err
        except TypeError, err:
            value_unicode = unicode(tokens[1])

        # Multiple values for metadata are separated by asterisks. This is where
        # the syntax gets handled.
        if u' * ' not in value_unicode:
            # There's only one thing to record here. Just add it.
            metadata[metadata_name].append(value_unicode)

        elif 'Parameter' in metadata_name:
            # Parameters get special treatment to make sure that subvalues
            # end up together in the same dict.
            tokens = value_unicode.split(u' * ')
            parameter = {}
            parameter[u'name'] = tokens[0]
            # Go through the subvalues.
            for item in tokens[1:]:
                if u': ' not in item:
                    # Not labeled as any particular subvalue. It's probably
                    # Geocode, which is a method.
                    parameter[u'METHOD'] = item
                else:
                    # Labeled subvalue. Easy to parse.
                    name, value = item.split(u': ', 1)
                    parameter[name] = value
            metadata[metadata_name].append(parameter)

        else:
            # There are multiple things in this metadata category. We'll need
            # to split them up.
            tokens = value_unicode.split(u' * ')

            # Split each item into subitem-value pairs.
            values = {}
            for item in tokens:
                if u': ' not in item:
                    # Not labeled. Add it to the back.
                    try:
                        values[''].extend(item)
                    except KeyError:
                        values[''] = [item]
                else:
                    # Labeled. Put it into the dictionary.
                    name, value = item.split(u': ', 1)
                    values[name] = value

            # Append the values on this line
            metadata[metadata_name].append(values)

    # Merge dictionary for certain categories 
    SINGLE_ITEM_CATEGORIES = ['Coverage']
    for category in SINGLE_ITEM_CATEGORIES:
        items = metadata[category] 
        new_item = items[0]
        for item in items[1:]:
            new_item.update(item)
        metadata[category] = new_item

    # Flatten out the single-item metadata categories.
    for item in metadata:
        if len(metadata[item]) == 1:
            metadata[item] = metadata[item][0]
    return metadata


def _read_data_description(f):
    """Read the DATA DESCRIPTION from a Polarstern CTD file.

    Paramters:
        f (file-like) - an input stream containing a Polarstern CTD. The stream
                MUST be positioned such that a valid DATA DESCRITPION header
                (as defined above) is immediately available.
    Side effects:
        (f) is advanced by as many lines as necessary to read 

    """
    lines = []

    # Enforce having a DATA DESCRIPTION at the top of the file.
    line = f.readline().rstrip()
    if not re.match(r'^/\* DATA DESCRIPTION:\s*$', line):
        raise InvalidFileFormatException('Missing DATA DESCRIPTION header')

    original_header = ['#' + line, ]

    # Read the DATA DESCRIPTION header.
    while True:

        # Read the next line and find out what metadata it is.
        line = f.readline().rstrip()
        if not line:
            # EOF was encountered, causing readline() to return an empty string.
            # This means thte DATA DESCRIPTION header is malformed.
            raise InvalidFileFormatException(('Unexpected end of file '
                    'while reading DATA DESCRIPTION header'))

        original_header.append('#' + unicode(line, 'raw_unicode_escape'))

        # Reached a valid end of the DATA_DESCRIPTION header.
        if r'*/' in line:
            original_header.append('')
            break

        lines.append(line)

    # Enforce format for the end of the DATA DESCRIPTION.
    if not re.match(r'^\*/\s*$', line):
        raise InvalidFileFormatException('Malformed end of DATA DESCRIPTION')

    metadata = _parse_data_description(lines)
    metadata[u'_header'] = u'\n'.join(original_header)
    return metadata


def _parse_datetime(dtstr):
    """Parse Pangea DATE/TIME string that is ISO formatted."""
    return datetime.strptime(dtstr, '%Y-%m-%dT%H:%M:%S')


def _parse_metadata(df, metadata, ):
    """ Try to parse metadata obtained from the DATA DESCRIPTION header and
    shove it into a DataFile object.

    Parameters:
        df (libcchdo.model.datafile.DataFile) - The DataFile to which metadata
                shall be added.
        metadata (dict<str, ...>) - The return value of _read_data_description()
                with the Polarstern CTD file that is to be loaded into df.
    """
    globals_from_metadata = {}

    # EXPOCODE. Since there isn't really an EXPOCODE for Polarstern stuff by
    # default, we'll leave it as None.
    globals_from_metadata['EXPOCODE'] = None

    # SECT_ID. We'll use the campaign.
    globals_from_metadata['SECT_ID'] = metadata[u'Event(s)'][0][u'CAMPAIGN']
    if u'\x28' in globals_from_metadata['SECT_ID']:
        globals_from_metadata['SECT_ID'] = \
                globals_from_metadata['SECT_ID'].split(u'\x28')[0]

    # STNNBR and CASTNO.
    globals_from_metadata['STNNBR'], globals_from_metadata['CASTNO'] = \
            metadata[u'Event(s)'][0][''][0].split(u'/')

    # DATE and TIME.
    dt = _parse_datetime(metadata['Event(s)'][0][u'DATE/TIME'])
    globals_from_metadata['_DATETIME'] = dt

    # LATITUDE and LONGITUDE.
    globals_from_metadata['LATITUDE'] = metadata[u'Event(s)'][0][u'LATITUDE']
    globals_from_metadata['LONGITUDE'] = metadata[u'Event(s)'][0][u'LONGITUDE']

    # DEPTH.
    globals_from_metadata['DEPTH'] = float(
            metadata[u'Coverage'][u'MAXIMUM DEPTH, water'].split()[0])

    # Drop the original header in there, just in case.
    globals_from_metadata['header'] = metadata[u'_header']

    # TODO We still need to twist the arms off of quite a bit of this metadata.
    # These are the essentials, so we can move on for now.

    df.globals.update(globals_from_metadata)


def _parse_parameters(df, parameters, metadata=None):
    """ Convert the tab-separated list of abbreviated parameters used to head
    the data in a Polarstern CTD file into actual libcchdo.db.model.*.Parameter
    objects, and construct the libcchdo.model.datafile.Column objects for the
    libcchdo.model.datafile.DataFile that will hold the Polarstern CTD data.

    Parameters:
        df (libcchdo.model.datafile.DataFile) - The DataFile into which the
                parameter structure will be created.
        parameters (list<str>) - The abbreviated parameter names used before
                the actual data in a Polarstern CTD file.
        metadata (dict<str, ...>) - The return value of _read_data_description()
                used to associate the abbreviated parameter names with their
                full names, and their canonical equivalents.
    """

    def _parse_meta_parameter(metaparameter, ):
        """ Attempt to convert a Polarstern CTD parameter into its CCHDO
        equivalent, if any.

        Parameter:
            metaparameter (dict<unicode, unicode>) - Polarstern CTD metadata for
                    parameters, received via _read_data_description().

        Returns:
            None - if no CCHDO equivalent is available for the parameter, or if
                    the CCHDO does not record the parameter;
            a 2-tuple containing the CCHDO parameter name and units that are
                    equivalent to the parameter.
        """
        # The regular expressions that will break down a Polarstern CTD
        # parameter listing into its formal (full) name, units, and abbreviated
        # name.
        formalname_re = r'([0-9A-Za-z(),\-\/ ]+)'
        opt_units_re = r'(?:\[(.+)\])?'
        abbrname_re = r'\((.+)\)'

        # Break the parameter listing into parts. If this cannot be done, either
        # the regular expressions are inadequate or the file is invalid.
        metaname = metaparameter[u'name']
        full_re = formalname_re + '\s+' + opt_units_re + '\s*' + abbrname_re
        match = re.match(full_re, metaname)
        if not match:
            raise InvalidFileFormatException, ("%r not recognized by "
                    "_parse_meta_parameter() (check the regexes)" % metaname)

        # Extract the parts of the parameter listing.
        formal_name = match.group(1)
        units = match.group(2)
        abbr_name = match.group(3)

        # Try to convert the parameter listing based on its abbreviated name.
        # This will probably flake out within the static function.
        param = PolarsternCTDParameter.get(abbr_name)

        # PolarsternCTDParameter will give either a 2-tuple containing useful
        # stuff or (None, None). Convert the latter (invalid) case into actual
        # None to indicate that no equivalent is available.
        return param if any(param) else None

    # LATITUDE (Latitude) and LONGITUDE (Longitude) might or might not show up
    # in the data. Remove them from the parameter metadata if they do not.
    if 'Latitude' not in parameters and 'Longitude' not in parameters:
        actual_parameters = filter(lambda metaparam:
                'Latitude' not in metaparam[u'name'] and
                'Longitude' not in metaparam[u'name'],
                metadata[u'Parameter(s)'])
    else:
        actual_parameters = metadata[u'Parameter(s)']

    # Try to convert Polarstern CTD parameters into CCHDO parameters.
    # This produces a list containing Nones and tuples of canonical parameter
    # names and their corresponding canonical units.
    parameters = [_parse_meta_parameter(metaparameter)
            for metaparameter in actual_parameters]

    # Construct order-preserved lists of column names and units from the output
    # of the _parse_meta_parameter() calls.
    cols = [p[0] for p in parameters if p]
    units = [p[1] for p in parameters if p]

    # Construct the columns from the pairs.
    df.create_columns(cols, units, False)

    # Return the conversion, including the None no-equivalent indicators, for
    # _load_data() to use.
    return parameters


def _load_data(df, data, parameters, ):
    """ Try to read Polarstern CTD data into an initialized DataFile.

    This basically dumps the data from a list of lists of floats into the column
    structures provided by the DataFile. There's no sanity checks, no unit
    conversions, etc.

    Parameters:
        df (libcchdo.model.datafile.DataFile) - the DataFile into which data
                shall be read.
        data (list<list<float>>) - CTD data, parameter-major.
        parameters (list<tuple<str, str> or None>) - paramter map that dictates
                which data goes into which columns.
    """
    if len(data) != len(parameters):
        # The number of parameters is not the same as the width of the data
        # lines. Something is wrong.
        raise InvalidFileFormatException, ("length mismatch "
                "(params=%d data=%d)" % (len(parameters), len(data)))

    # Dump the data into the DataFile.
    for param, col in zip(parameters, data):
        if param is None:
            continue
        for item in col:
            df.columns[param[0]].append(item)


def split(dfile, expocode):
    """Split a Pangea DataFile into a DataFileCollection.

    """
    lines = [line[1:] for line in dfile.globals['header'].split('\n')[1:-2]]
    metadata = _parse_data_description(lines)
    event_metas = {}
    for meta in metadata['Event(s)']:
        event_metas[meta[''][0].split()[0]] = meta

    dfc = DataFileCollection()
    cur_event = None
    cur_file = None
    for rowi in range(len(dfile)):
        event = dfile['_EVENT'][rowi]
        if event != cur_event:
            cur_event = event
            cur_file = copy(dfile)

            try:
                event_meta = event_metas[event]
            except KeyError:
                LOG.error(
                    u'Unable to get event metadata for event {0}'.format(event))

            sect, stncast = event.split('/')
            stn, cast = stncast.split('-')
            cur_file.globals['SECT_ID'] = sect
            cur_file.globals['STNNBR'] = stn
            cur_file.globals['CASTNO'] = cast
            try:
                cur_file.globals['DEPTH'] = int(
                    float(event_meta['ELEVATION'][1:-2]))
            except KeyError:
                pass
            try:
                cur_file.globals['LATITUDE'] = float(event_meta['LATITUDE'])
            except KeyError:
                pass
            try:
                cur_file.globals['LONGITUDE'] = float(event_meta['LONGITUDE'])
            except KeyError:
                pass
            try:
                cur_file.globals['_DATETIME'] = _parse_datetime(
                    event_meta['DATE/TIME'])
            except KeyError:
                pass
            cur_file.globals['EXPOCODE'] = expocode

            del cur_file.columns['LATITUDE']
            del cur_file.columns['LONGITUDE']
            del cur_file.columns['_DATETIME']
            del cur_file.columns['_EVENT']

            dfc.append(cur_file)

        for key, col in cur_file.columns.items():
            try:
                source_col = dfile[key]
            except KeyError:
                LOG.error(u'Missing column {0}'.format(key))
                continue
            try:
                flag_woce = source_col.flags_woce[rowi]
            except (KeyError, IndexError):
                flag_woce = None
            try:
                flag_igoss = source_col.flags_igoss[rowi]
            except (KeyError, IndexError):
                flag_igoss = None
            col.append(source_col.values[rowi], flag_woce, flag_igoss)
    return dfc


def _str_to_float(sss):
    try:
        return float(sss)
    except ValueError:
        return None


def read(df, f, toascii=False, first_two_cols_event_dt=True):
    """Read a Polarstern CTD file.

    Parameters:
        df (libcchdo.model.datafile.DataFile) - datafile object into which we
                shall read.
        f (file-like) - input stream containing a Polarstern CTD.
    """

    # Read through the DATA DESCRIPTION header.
    metadata = _read_data_description(f)

    # Read through the parameters line.
    # There should be some special processing, but it's not important right now.
    parameters = f.readline().split('\t')

    # Read through the data and dump it into a list of lists of floats.
    data = [[] for param in parameters]
    for line in f:
        tokens = line.rstrip().split('\t')

        if first_two_cols_event_dt:
            first_two = tokens[:2]
        else:
            first_two = [_str_to_float(x) for x in tokens[:2]]
        tokens = first_two + [_str_to_float(x) for x in tokens[2:]]

        if len(tokens) != len(data):
            # The number of parameters listed in the header is not the same as
            # that of the line we just read in. Something is wrong.
            raise InvalidFileFormatException(
                    'mismatched parameters (expected %d, had %d)' %
                    (len(data), len(tokens)))

        for i in range(len(data)):
            data[i].append(tokens[i])

    # Parse the file.
    # This is where the majority of problems should occur.
    _parse_metadata(df, metadata)
    parameter_map = _parse_parameters(df, parameters, metadata)
    _load_data(df, data, parameter_map)

    if toascii:
        # The conservative policy is to strip all the characters that are not
        # in range(128), but it has the unfortunate side effect of mangling
        # things like people's names (u"J\xfcri" with a u-umlaut >>> "Jri").
        def sanitize(unicodestr):
            # This actually replaces the characters with '?' for less badness.
            # So we get u"J\xfcri" >>> "J?ri".
            return ''.join([c if ord(c) in range(128) else '?'
                    for c in unicodestr])

        for item in df.globals:
            # Sanitize the value if necessary.
            if type(df.globals[item]) in (unicode, str, ) and \
                    any([ord(c) not in range(128) for c in df.globals[item]]):
                sanitized_value = sanitize(df.globals[item])
                df.globals[item] = sanitized_value

            # Sanitize the header just in case.
            if any([ord(c) not in range(128) for c in item]):
                sanitized_item = sanitize(item)
                df.globals[sanitized_item] = df.globals[item]
                del df.globals[item]

    # Check and canonicalize the parameters.
    # Not sure if this is going to screw things up, but we'll do it to be safe.
    df.check_and_replace_parameters()

    return df
