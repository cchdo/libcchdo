''' Generic MATLAB representation of bottle data.

The output of write() follows the format:

required struct {
  required "datafile": array<struct> [
    repeated struct {
      required "STNNBR": int or string;
      required "CASTNO": int or string;
      required ("SAMPNO" or "BTLNBR"): int or string;
      repeated ParameterName: struct {
        required "values": array<float>;
        optional "flags_woce": array<int>;
        optional "flags_igoss": array<int>;
      }
    }
  ]
}

'''

from datetime import datetime
import scipy.io

from libcchdo.formats.matlab.util import convert_value


def write(self, handle, ):
    all_profiles = {}

    # Find out which attributes are part of the identifier.
    # Valid identifiers MUST include STNNBR and CASTNO, and either one or
    # both of SAMPNO and BTLNBR.
    identifier_template = []
    for identifier_component in ('STNNBR', 'CASTNO', 'SAMPNO', 'BTLNBR', ):
        if self[identifier_component]:
            identifier_template.append(identifier_component)
            self[identifier_component].values = map(
                    lambda x: int(x) if type(x) is float else x,
                    self[identifier_component].values)

    # Clean up the DataFile, just in case.
    self.check_and_replace_parameters()

    # Define some helper functions for dealing with bottle entries by
    # identifier.
    id = lambda i: (lambda component: self.columns[component][i])
    identifier_for_line = lambda i: tuple(map(id(i), identifier_template))

    # Grab all the unique identifiers in the bottle file and add them to.
    # the set of all profiles. We're indexing profile information by
    # identifier until the very end, when we'll add them back in.
    for i in range(len(self)):
        identifier = identifier_for_line(i)
        if identifier not in all_profiles:
            all_profiles[identifier] = {}

    # Extract column data into the profiles set.
    for i in range(len(self)):
        identifier = identifier_for_line(i)
        for column in self.columns:
            if column in identifier_template:
                # Ignore identifier columns. They will be added at the end.
                continue
            if column not in all_profiles[identifier]:
                all_profiles[identifier][column] = []
            all_profiles[identifier][column].append(
                    convert_value(str(column), self.columns[column][i]))

    # Clean up the profiles set by removing empty parameters.
    # Also perform datetime-to-str conversion.
    for identifier in all_profiles:
        empty_parameters = []
        for parameter in all_profiles[identifier]:
            values = all_profiles[identifier][parameter]
            if not any(values) or len(values) == 0:
                # Empty parameter. Mark it for deletion.
                empty_parameters.append(parameter)
                continue
            if type(values[0]) is datetime:
                # Format as ISO 8601 date-time string.
                all_profiles[identifier][parameter] = map(
                        lambda dt: dt.strftime('%Y-%m-%dT%H:%M'),
                        all_profiles[identifier][parameter])
                continue
        # Delete empty parameters.
        for parameter in empty_parameters:
            del all_profiles[identifier][parameter]

    # Add the identifiers back into the profiles.
    for identifier in all_profiles:
        identifier_as_dict = dict(zip(identifier_template,
                map(convert_value, identifier_template, identifier)))
        all_profiles[identifier].update(identifier_as_dict)

    scipy.io.savemat(handle, {"datafile": all_profiles.values(), })
