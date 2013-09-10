"""
ndive: deployment number

time: at the start of the deployment, encoded with the Matlab function datenum.m

lat: Latitude (launch point for the deployment; decimal degrees)

lon: Longitude (decimal degrees)

pgrid: decibars - all data are binned to 0.5 dbar interval - the pgrid values
indicate the center of each bin

tave: temperature (ITS-90) derived from the pressure-protected thermometer of
the HRPii CTD instrument and pre-cruise laboratory calibration information

s_ave: salinity (pss) derived from acquired P, T and C observations,
subsequently adjusted empirically to agree with the wire-lowered CTD data. CTD T
and S data quality are rather poor owing in part to the very slow-responding
temperature sensor fitted to the HRPii on this cruise.

mav_e: zonal absolute velocity (m/s) derived from the MAVS acoustic travel time
current meter and flux gate compass data applied to a "point-mass model" of the
HRPii response to depth-varying horizontal currents. Depth-independent offsets
were applied to the relative velocity data output by the model to be consistent
with the deployment and surfacing positions of the HRP (as determined by its
on-board GPS receiver). Apart from dives 18 and 20 (when no accelerometer data
were logged), accelerometer data were used to remove the velocity signal at
vertical scales comparable to the length of the HRPii caused by its pendulum
motion while falling. An excess of finescale velocity variability on vertical
scales corresponding to the spin rate of the HRPii (in the sense of the HRPii
spin) is seen in most of the final profiles. This signal is considered spurious
but no satisfactory method for identifying and extracting the error velocity was
discovered, and so the data are reported as derived.

mav_n: meridional absolute velocity (m/s), as above

mav_relup: relative vertical velocity past the HRPii as derived from the MAVS
ACM (m/s).  Positive values indicate flow upwards relative to the HRPii.

dzdtave: time-rate-of-change of HRPii depth (m/s) derived from the measured
pressure.  Positive values correspond to the instrument sinking with time.
Estimates of the ocean's vertical velocity may be derived by subtracting dzdtave
from mav_relup.

mav_vortzrel: the relative circulation around the 4 paths of the MAVS ACM (1/s)

spinrate: the spin rate of the HRPii (1/s) Estimates of the ocean relative
vorticity within the sampling volume of the MAVS ACM may be had by differencing
mav_vortzrel and spinrate.

u_ef : zonal velocity estimated by the electric field current sensor on the
HRPii (m/s; derived by L. Rainville). Ground faults in the HRPii electronics
played havoc with the EF sensor data during the cruise. Most of the reported
velocity estimates are based on data from up-going profiles.

v_ef: meridional velocity from the EF sensor

u_adcp: absolute zonal velocity (m/s) from the R/V Thompson OS75 ADCP system
averaged over the duration of the HRPii dive.

v_adcp: absolute meridional velocity (m/s) from the ADCP (as above)

ep1: turbulent kinetic energy dissipation rate (W/kg) derived from shear probe
data of channel 1.

ep2: turbulent kinetic energy dissipation rate (W/kg) derived from shear probe
data of channel 2.

eps: "best" turbulent kinetic energy dissipation rate (W/kg) estimates for the
dive based on an evaluation of ep1 and ep2 data quality (evaluation done by L.
St. Laurent). In a couple of cases when both probes looked good, eps is the
average of the two probe series.

"""
import math
from datetime import datetime
from contextlib import contextmanager
import tempfile

from libcchdo.util import memoize
from libcchdo import fns, LOG
from libcchdo.formats import netcdf as nc
from libcchdo.formats.matlab import dimes
from libcchdo.formats.netcdf_oceansites import write_columns, OSVar, ParamToOS


def read(self, handle):
    """Read DIMES HRP2 format."""
    self.globals['header'] = ""

    global_params = {
        'ndive': 'STNNBR',
        'time': 'TIME',
        'lon': 'LONGITUDE',
        'lat': 'LATITUDE',
        }
    vertical_params = ['ep1', 'ep2', 'epl']
    dimes.read(self, handle, global_params, vertical_params)
    self.globals['CASTNO'] = 1
    self.globals['TIME'] = fns.ordinal_datetime_to_datetime(self.globals['TIME'])


# TODO refactor
@contextmanager
def nc_dataset_to_stream(stream, *args, **kwargs):
    """Creates a DataSet and writes it out to the stream when closed."""
    # netcdf library wants to write its own files.
    tmp = tempfile.NamedTemporaryFile()
    nc_file = nc.Dataset(tmp.name, 'w', *args, **kwargs)
    try:
        yield nc_file
    finally:
        nc_file.close()
        stream.write(tmp.read())
        tmp.close()
    

@memoize
def converter():
    param_to_os = ParamToOS()
    param_to_os.register_osvars(
        OSVar('pgrid', 'pgrid', 'pgrid', ''), 
        OSVar('tave', 'tave', 'tave', ''),
        OSVar('s_ave', 's_ave', 's_ave', ''),
        OSVar('spinrate', 'spinrate', 'spinrate', ''),
        OSVar('dzdtave', 'dzdtave', 'dzdtave', ''),
        OSVar('mav_relup', 'mav_relup', 'mav_relup', ''),
        OSVar('mav_vortzrel', 'mav_vortzrel', 'mav_vortzrel', ''),
        OSVar('mav_n', 'nav_n', 'nav_n', ''),
        OSVar('mav_e', 'mav_e', 'mav_e', ''),
        OSVar('ep1', 'ep1', 'ep1', ''),
        OSVar('ep2', 'ep2', 'ep2', ''),
        OSVar('epl', 'epl', 'epl', ''),
        OSVar('u_ef', 'u_ef', 'u_ef', ''),
        OSVar('v_ef', 'v_ef', 'v_ef', ''),
        OSVar('u_adcp', 'u_adcp', 'u_adcp', ''),
        OSVar('v_adcp', 'v_adcp', 'v_adcp', ''),
    )
    return param_to_os


def write(self, handle):
    """Write DIMES microstructure COARDS compliant file."""
    self.globals['EXPOCODE'] = 'expocode'
    self.globals['STNNBR'] = 1
    self.globals['DEPTH'] = 0
    self.globals['_DATETIME'] = datetime.utcnow()
    with nc_dataset_to_stream(handle, format='NETCDF4') as nc_file:
        nc_file.Conventions = 'CF-1.6'
        nc_file.netcdf_version = '4'
        nc_file.title = 'title'
        nc_file.history = ''.join(["data collected\n",
                           datetime.utcnow().isoformat(),
                           "Z date file translated/written"])
        nc_file.institution = 'institution'
        nc_file.source = 'HRP2'
        nc_file.references = 'references'
        nc_file.comment = 'comment'

        nc_file.data_type = 'DIMES HRP2'
        nc_file.format_version = '0.1-alpha'

        nc_file.wmo_platform_code = ''
        nc_file.date_update = fns.strftime_iso(datetime.utcnow())
        nc_file.data_mode = 'D'
        nc_file.quality_control_indicator = '1'
        nc_file.quality_index = 'B'
        nc_file.naming_authority = 'OceanSITES'
        nc_file.cdm_data_type = 'Station'
        nc_file.geospatial_lat_min = str(self.globals['LATITUDE'])
        nc_file.geospatial_lat_max = str(self.globals['LATITUDE'])
        nc_file.geospatial_lon_min = str(self.globals['LONGITUDE'])
        nc_file.geospatial_lon_max = str(self.globals['LONGITUDE'])
        nc_file.geospatial_vertical_min = 0
        nc_file.geospatial_vertical_max = int(self.globals['DEPTH'])
        nc_file.geospatial_vertical_positive = 'down'
        nc_file.author = 'CCHDO (Scripps Institution of Oceanography)'
        nc_file.data_assembly_center = 'CCHDO'
        nc_file.distribution_statement = (
            'Follows CLIVAR (Climate Varibility and Predictability) '
            'standards, cf. http://www.clivar.org/data/data_policy.php. '
            'Data available free of charge. User assumes all risk for use of '
            'data. User must display citation in any publication or product '
            'using data. User must contact PI prior to any commercial use of '
            'data.')
        nc_file.citation = (
            'These data were collected and made freely available by the OceanSITES '
            'project and the national programs that contribute to it.')
        nc_file.update_interval = 'void'
        nc_file.qc_manual = ''
        nc_file.time_coverage_start = fns.strftime_iso(self.globals['_DATETIME'])
        nc_file.time_coverage_end = fns.strftime_iso(self.globals['_DATETIME'])

        nc_file.createDimension('TIME')
        try:
            nc_file.createDimension('DEPTH', len(self))
        except RuntimeError:
            raise AttributeError("There is no data to be written.")
        nc_file.createDimension('LATITUDE', 1)
        nc_file.createDimension('LONGITUDE', 1)
        nc_file.createDimension('POSITION', 1)

        # OceanSITES coordinate variables
        var_time = nc_file.createVariable(
            'TIME', 'd', ('TIME',), fill_value=999999.0)
        var_time.long_name = 'time'
        var_time.standard_name = 'time'
        var_time.units = 'days since 1950-01-01T00:00:00Z'
        var_time.valid_min = 0.0
        var_time.valid_max = 90000.0
        var_time.QC_indicator = 7 # Matthias Lankhorst
        var_time.QC_procedure = 5 # Matthias Lankhorst
        # 1/24 assuming a typical cast lasts one hour Matthias Lankhorst
        var_time.uncertainty = 0.0417
        var_time.axis = 'T'

        var_latitude = nc_file.createVariable(
            'LATITUDE', 'f', ('LATITUDE',), fill_value=99999.0)
        var_latitude.long_name = 'Latitude of each location'
        var_latitude.standard_name = 'latitude'
        var_latitude.units = 'degrees_north'
        var_latitude.valid_min = -90.0
        var_latitude.valid_max = 90.0
        var_latitude.QC_indicator = 7 # Matthias Lankhorst
        var_latitude.QC_procedure = 5 # Matthias Lankhorst
        var_latitude.uncertainty = 0.0045 # Matthias Lankhorst
        var_latitude.axis = 'Y'
        var_latitude.reference = 'WGS84'
        var_latitude.coordinate_reference_frame = 'urn:ogc:crs:EPSG::4326'

        var_longitude = nc_file.createVariable(
            'LONGITUDE', 'f', ('LONGITUDE',), fill_value=99999.0)
        var_longitude.long_name = 'Longitude of each location'
        var_longitude.standard_name = 'longitude'
        var_longitude.units = 'degrees_east'
        var_longitude.valid_min = -180.0
        var_longitude.valid_max = 180.0
        var_longitude.QC_indicator = 7 # Matthias Lankhorst
        var_longitude.QC_procedure = 5 # Matthias Lankhorst
        # Matthias Lankhorst
        var_longitude.uncertainty = 0.0045 / math.cos(
            float(self.globals['LATITUDE']))
        var_longitude.axis = 'X'
        var_longitude.reference = 'WGS84'
        var_longitude.coordinate_reference_frame = 'urn:ogc:crs:EPSG::4326'

        var_depth = nc_file.createVariable(
            'DEPTH', 'f', ('DEPTH',), fill_value=-99999.0)
        var_depth.long_name = 'Depth of each measurement'
        var_depth.standard_name = 'depth'
        var_depth.units = 'meters'
        var_depth.valid_min = 0.0
        var_depth.valid_max = 12000.0
        # Subject: OceanSITES: more on QC flags, uncertainty, depth
        # Interpolated from latitude and pressure.
        var_depth.QC_indicator = 8
        var_depth.QC_procedure = 2 # See above
        var_depth.uncertainty = 1.0 # A decibar
        var_depth.positive = 'down'
        var_depth.axis = 'Z'
        var_depth.reference = 'sea_level' # TODO is this right?
        var_depth.coordinate_reference_frame = 'urn:ogc:crs:EPSG::5113'

        since_1950 = self.globals['_DATETIME'] - datetime(1950, 1, 1)
        var_time[:] = [since_1950.days + since_1950.seconds/86400.0]
        var_latitude[:] = [self.globals['LATITUDE']]
        var_longitude[:] = [self.globals['LONGITUDE']]

        write_columns(self, nc_file, converter())

        nc_file.title = ('HRP2 ExpoCode=%s Station=%s Cast=%s') % \
            (self.globals['EXPOCODE'], self.globals['STNNBR'],
             self.globals['CASTNO'])

        nc_file.id = 'HRP2 {0} {1} {2}'.format(
            self.globals['EXPOCODE'], self.globals['STNNBR'],
            self.globals['CASTNO'])

        nc.check_variable_ranges(nc_file)
