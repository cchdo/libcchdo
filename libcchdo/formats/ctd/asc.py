import sys
import re
import datetime
from libcchdo.fns import Decimal
from libcchdo.fns import ddm_to_dd

def read(self, f, expo=None):

    if expo:
        self.globals["EXPOCODE"] = expo

    self.globals["CASTNO"] = ""
    self.globals["SECT_ID"] = ""
    l = f.readline()
    while "===" not in l:
        if "Lat" in l:
            l = l.split('=')
            ctoks = l[1].strip().split(' ')
            self.globals["LATITUDE"] = ddm_to_dd(ctoks)
        elif "Lon" in l:
            l = l.split('=')
            ctoks = l[1].strip().split(' ')
            self.globals["LONGITUDE"] = ddm_to_dd(ctoks)
        elif "depth" in l:
            l = l.split(':')
            self.globals["DEPTH"] = l[1].strip()
        elif "UTC" in l:
            l = l.split('=')
            dt = datetime.datetime.strptime(l[1].strip(), '%H:%M:%S')
            self.globals["TIME"] = dt.strftime('%H%M')
        elif "Station" in l:
            l = l.split(':')
            self.globals['STNNBR'] = l[1].strip()
        else:
            try:
                dt = datetime.datetime.strptime(l.strip(), '%b %d %Y')
                self.globals["DATE"] = dt.strftime('%Y%m%d')
            except ValueError:
                pass

        l = f.readline()

    if "===" in l:
        l = f.readline()
    else:
        raise ValueError

    params = re.split('\s+', l)
    params = [p for p in params if p.strip()]
    for i, param in enumerate(params):
        if 'Temp' in param:
            params[i] = "CTDTMP"
        if 'Sal' in param:
            params[i] = "CTDSAL"
        if "Oxy" in param:
            params[i] = "CTDOXY"
        if "Pres" in param:
            params[i] = "CTDPRS"

    l = f.readline()
    units = re.findall('(?<=\[)[\/ \w]*(?=\])', l)
    for i, unit in enumerate(units):
        if 'db' in unit:
            units[i] = "DBAR"

    l = f.readline()
    if "---" in l:
        l = f.readline()
    else:
        raise ValueError

    self.create_columns(params, units, None)

    while l:
        values = [v for v in re.split('\s+', l) if v.split()]

        for column, value in zip(params, values):
            col = self.columns[column]
            if 'NaN' in value:
                col.append(None, flag_woce=9)
            elif column is not "CTDPRS":
                col.append(Decimal(value), flag_woce=2)
            else:
                col.append(Decimal(value))

        l = f.readline()

    self.check_and_replace_parameters()
    
