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
from libcchdo.formats.netcdf_oceansites import (
    write_columns, OSVar, ParamToOS, OS_TEXT)


DEFAULT_CFG = {
    'expocode': 'UNKNOWN',
    'pi': 'Unknown',
    'parameter_mapping': {
        'pgrid': 'PRESSURE',
        'tave': 'TEMPERATURE',
        's_ave': 'PSAL',
        'epl': 'EPSILON',
        'chi-t': 'CHI-T',
        'chi-c': 'CHI-C',
    },
    'data_type': 'HRP2',
}


def check_cfg(cfg):
    """Apply this check to the configuration before reading/writing."""
    assert cfg['data_type'] in ['HRP2', 'DMS']
    assert isinstance(cfg['expocode'], basestring)
    assert isinstance(cfg['pi'], basestring)
    assert type(cfg['parameter_mapping']) == dict


def read(self, handle, cfg=DEFAULT_CFG):
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

    self.globals['_DATETIME'] = fns.ordinal_datetime_to_datetime(
        self.globals['TIME'])
    self.globals['EXPOCODE'] = cfg['expocode']

    # TODO
    self.globals['DEPTH'] = 0


def standard_osvar(short_name, standard_name, units, uncertainty=''):
    return OSVar(short_name, standard_name, standard_name, units, uncertainty)


@memoize
def converter(cfg):
    param_to_os = ParamToOS()
    param_to_os.register_osvars(
        standard_osvar('PRESSURE', 'sea_water_pressure', 'dbar'),
        standard_osvar('TEMPERATURE',
            'sea_water_temperature', 'degrees_Celsius'),
        standard_osvar('PSAL', 'sea_water_salinity', '1e-3'),
        standard_osvar('EPSILON',
            'ocean_turbulent_kinetic_energy_dissipation_rate', 'W kg^-1'),
        standard_osvar('CHI-T',
            'ocean_dissipation_rate_of_thermal_variance_from_microtemperature',
            'C^2 s^-1'),
        standard_osvar('CHI-C',
            'ocean_dissipation_rate_of_thermal_variance_from_microconductivity',
            'C^2 s^-1'),
    )

    # Map matlabe variable names into CF names
    for matlabp, cfp in cfg['parameter_mapping'].items():
        param_to_os._register(matlabp, cfp)

    return param_to_os


def decimal_days_since(dtime, epoch=datetime(1950, 1, 1)):
    """Return the decimal days since an epoch.

    Arguments:
        dtime - the time to convert to decimal days since epoch.
        since - (optional) the epoch. Defaults to 1950-01-01

    """
    delta = dtime - epoch
    return delta.days + delta.seconds / 86400.0


def write(self, handle, cfg=DEFAULT_CFG):
    """Write DIMES microstructure COARDS compliant file."""
    with nc.nc_dataset_to_stream(handle, format='NETCDF3_CLASSIC') as nc_file:
        nc_file.Conventions = 'CF-1.6'
        nc_file.netcdf_version = '3'
        nc_file.history = ''.join([
            "data collected\n", datetime.utcnow().isoformat(),
            "Z date file translated/written"])
        nc_file.source = cfg['data_type']
        # TODO README file inclusion or reference? details quality, instruments
        # used, TS used pumped instruments etc
        # TODO will supply DOIs
        nc_file.references = 'references'

        nc_file.data_type = cfg['data_type']

        nc_file.format_version = '0.1-beta'

        nc_file.date_update = fns.strftime_iso(datetime.utcnow())
        nc_file.geospatial_lat_min = str(self.globals['LATITUDE'])
        nc_file.geospatial_lat_max = str(self.globals['LATITUDE'])
        nc_file.geospatial_lon_min = str(self.globals['LONGITUDE'])
        nc_file.geospatial_lon_max = str(self.globals['LONGITUDE'])
        nc_file.geospatial_vertical_min = 0
        nc_file.geospatial_vertical_max = int(self.globals['DEPTH'])
        nc_file.geospatial_vertical_positive = 'down'
        nc_file.author = cfg['pi']
        nc_file.data_assembly_center = 'CCHDO'
        # TODO
        nc_file.distribution_statement = (
            'HRP distribution statement. TBD.')
        # TODO
        nc_file.citation = (
            'Citation statement. TBD.')
        nc_file.update_interval = 'void'
        nc_file.time_coverage_start = fns.strftime_iso(self.globals['_DATETIME'])
        nc_file.time_coverage_end = fns.strftime_iso(self.globals['_DATETIME'])

        nc_file.dive_number = int(self.globals['STNNBR'])

        nc_file.createDimension('TIME')
        nc_file.createDimension('BOTTOM_DEPTH', 1)
        try:
            nc_file.createDimension('DEPTH', len(self))
        except RuntimeError:
            raise AttributeError("There is no data to be written.")
        nc_file.createDimension('LATITUDE', 1)
        nc_file.createDimension('LONGITUDE', 1)

        var_time = nc_file.createVariable(
            'TIME', 'd', ('TIME',), fill_value=999999.0)
        var_time.long_name = 'time'
        var_time.standard_name = 'time'
        var_time.units = 'days since 1950-01-01T00:00:00Z'
        var_time.valid_min = 0.0
        var_time.valid_max = 90000.0
        var_time.axis = 'T'

        var_latitude = nc_file.createVariable(
            'LATITUDE', 'f', ('LATITUDE',), fill_value=99999.0)
        var_latitude.long_name = 'Latitude of each location'
        var_latitude.standard_name = 'latitude'
        var_latitude.units = 'degrees_north'
        var_latitude.valid_min = -90.0
        var_latitude.valid_max = 90.0
        var_latitude.axis = 'Y'

        var_longitude = nc_file.createVariable(
            'LONGITUDE', 'f', ('LONGITUDE',), fill_value=99999.0)
        var_longitude.long_name = 'Longitude of each location'
        var_longitude.standard_name = 'longitude'
        var_longitude.units = 'degrees_east'
        var_longitude.valid_min = -180.0
        var_longitude.valid_max = 180.0
        var_longitude.axis = 'X'

        if self.globals.get('DEPTH'):
            var_bot_depth = nc_file.createVariable(
                'BOT_DEPTH', 'f', ('BOTTOM_DEPTH',), fill_value=-99999.0)
            var_bot_depth.long_name = 'Sea floor depth below sea level'
            var_bot_depth.standard_name = 'sea_floor_depth_below_sea_level'
            var_bot_depth.units = 'meters'
            var_bot_depth.valid_min = 0.0
            var_bot_depth.valid_max = 12000.0
            var_bot_depth.axis = 'Z'
            var_bot_depth = float(self.globals['DEPTH'])

        var_depth = nc_file.createVariable(
            'DEPTH', 'f', ('DEPTH',), fill_value=-99999.0)
        var_depth.long_name = 'Depth of each measurement'
        var_depth.standard_name = 'depth'
        var_depth.units = 'meters'
        var_depth.valid_min = 0.0
        var_depth.valid_max = 12000.0
        var_depth.axis = 'Z'

        # Write variables
        var_time[:] = [decimal_days_since(self.globals['_DATETIME'])]
        var_latitude[:] = [self.globals['LATITUDE']]
        var_longitude[:] = [self.globals['LONGITUDE']]

        cvt = converter(cfg)
        pres = None
        salt = None
        temp = None
        for column in self.sorted_columns():
            pname = column.parameter.name
            try:
                name, variable = cvt.convert(pname)
            except KeyError:
                LOG.warn(
                    u'Parameter name {0!r} is not mapped to an OceanSITES '
                    'variable. Skipping.'.format(pname))
                continue
            if name == 'PRESSURE':
                pres = self[pname]
            elif name == 'PSAL':
                salt = self[pname]
            elif name == 'TEMPERATURE':
                temp = self[pname]
            if pres and salt and temp:
                break
        depth_method, depths = self.calculate_depths(
            pres=pres, salt=salt, temp=temp)
        if depth_method == 'unesco1983':
            var_depth.comment = OS_TEXT['DEPTH_CALCULATED_UNESCO_1983']
        elif depth_method == 'sverdrup':
            var_depth.comment = OS_TEXT['DEPTH_CALCULATED_SVERDRUP']
        var_depth[:] = depths

        write_columns(self, nc_file, cvt)

        nc_file.title = '{0} ExpoCode={1} Dive={2}'.format(
            cfg['data_type'], self.globals['EXPOCODE'], self.globals['STNNBR'])
        nc_file.id = '{0} {1} {2}'.format(
            cfg['data_type'], self.globals['EXPOCODE'], self.globals['STNNBR'])

        nc.check_variable_ranges(nc_file)
