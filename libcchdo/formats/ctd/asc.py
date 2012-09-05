import sys
import datetime

def read(self, f):
    f = f[0]
    l = f.readline()
    while "===" not in l:
        if "Lat" in l:
            l = l.split('=')
            self.globals["LATITUDE"] = l[1].strip()
        if "Lon" in l:
            l = l.split('=')
            self.globals["LONGITUDE"] = l[1].strip()
        if "depth" in l:
            l = l.split(':')
            self.globals["DEPTH"] = l[1].strip()
        if "UTC" in l:
            l = l.split('=')
            self.globals["TIME"] = l[1].strip()
        l = f.readline()
