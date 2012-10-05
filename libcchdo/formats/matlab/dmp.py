from libcchdo.formats.matlab import dimes


def read(self, handle):
    """Read DIMES DMP format.

date: in yyyymmdd format

dvno: DMP dive number

lat: latitude

lon: longitude

time: 24-hr time of profile start

ep1: epsilon from micro-shear channel 1

ep2: epsilon from micro-shear channel 2

epl: best estimate of epsilon (either ep1 or ep2, or the mean)

prs: pressure at 1 dbar increments

sal: poor estimate of salinity

tem: quality estimate of temperature

"""
    self.globals['header'] = """\
Salinity estimates are of poor quality owing to the use of an un-pumped SBE4
cell. The cell flushes over a 50 to 100-dbar profiling distance. We have found
that salinity gradients estimated over these scales are decent for use in an N2
estimate. We do not recommend the use of these salinities for estimates that are
sensitive to fine scale density, such as Thorpe scales. Temperature estimates,
from an un-pumped SBE3 cell, seem to be of very good quality (as judged against
simultaneous pumped SBE 911 data). Any detailed questions on the data or
analysis can be directed to lous@whoi.edu
"""
    global_params = {
        'date': 'DATE',
        'time': 'TIME',
        'dvno': 'CASTNO',
        'lon': 'LONGITUDE',
        'lat': 'LATITUDE',
        }
    vertical_params = ['ep1', 'ep2', 'epl', 'prs', 'sal', 'tem']
    dimes.read(self, handle, global_params, vertical_params)
