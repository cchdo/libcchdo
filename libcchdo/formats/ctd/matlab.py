''' Generic MATLAB representation of CTD data.

The output of write() follows the format:

required struct {
  required "datafile": struct {
    repeated GlobalAttribute: int or string;
    required "data": struct {
      repeated Parameter: struct {
        required "units": string;
        required "values": array<float>;
        optional "flags_woce": array<int>;
        optional "flags_igoss": array<int>;
      }
    }
  }
}

The output of [libcchdo] write() can be read into MATLAB using [matlab] load().
Shown below is a sample usage for a small test file. (NB. everything below this
line is a representation of the MATLAB interface.)

>> load('example_ct1.mat')
>> datafile

datafile =

         DATE: '19900101'
       STNNBR: 1
      SECT_ID: 'X00'
        stamp: '20100929SIOCCHXXX'
    LONGITUDE: -117.9101
       header: ''
        DEPTH: 420
         TIME: '1234'
     LATITUDE: 33.0001
       CASTNO: 1
         data: [1x1 struct]
     EXPOCODE: '99XX19800101'

>> datafile.data

ans =

    CTDTMP: [1x1 struct]
    CTDOXY: [1x1 struct]
    CTDPRS: [1x1 struct]
    CTDSAL: [1x1 struct]

>> datafile.data.CTDSAL

ans =

         units: 'PSS-78'
        values: [10x1 double]
    flags_woce: [10x1 int32]

>> datafile.data.CTDSAL.values

ans =

    34.8624
    34.9033
    33.0523
    33.9304
    33.2329
    33.6106
    33.0252
    34.5983
    34.5734
    33.8689

>> datafile.data.CTDSAL.flags_woce

ans =

    2
    2
    2
    2
    2
    2
    2
    2
    2
    2
'''

import numpy
import scipy.io

from libcchdo.formats.matlab.util import convert_value
from libcchdo.log import LOG


def write(self, handle, ):
    """Write an exchange CTD file as MATLAB struct.

    Parameters:
        self (libcchdo.model.datafile.DataFile) - the exchange CTD, loaded into
                libcchdo.
        handle (str | <file>) - the (filename | file handle) to which to save
                the resulting MATLAB data structures.

    """

    # scipy.io converts dictionaries into MATLAB structs, subject to the
    # constraint that the keys must be <str>s.
    struct = {'data': {}}

    # Add the globals to the struct. Convert numeric things to numbers, but
    # try to be smart about it.
    for globl, value in self.globals.items():
        field = str(globl)
        val = convert_value(field, value)
        if val is not None:
            struct[field] = val

    # Add the parameters to the struct. Each parameter gets its own substruct
    # in the file's struct, containing the values and the flags (if any).
    for param, col in self.columns.items():
        field = str(param)
        column = {}

        # Copy the units into the parameter struct.
        column['units'] = str(col.parameter.units.mnemonic)

        # Copy the values into the parameter struct.
        column['values'] = numpy.asarray(map(float, col.values))

        # Copy any flags that may be present into the parameter struct.
        if any(col.flags_woce):
            column['flags_woce'] = numpy.asarray(col.flags_woce, dtype=int)
        if any(col.flags_igoss):
            column['flags_igoss'] = numpy.asarray(col.flags_igoss, dtype=int)

        # Add the parameter struct to the file struct.
        struct['data'][field] = column

    # Write the file struct to the handle.
    scipy.io.savemat(handle, {"datafile": struct})
