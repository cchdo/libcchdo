from libcchdo.formats.matlab import dimes


def read(self, handle):
    """Read DIMES HRP2 format.

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
    self.globals['header'] = ""

    global_params = {
        'ndive': 'CASTNO',
        'time': 'TIME',
        'lon': 'LONGITUDE',
        'lat': 'LATITUDE',
        }
    vertical_params = ['ep1', 'ep2', 'epl']
    dimes.read(self, handle, global_params, vertical_params)
