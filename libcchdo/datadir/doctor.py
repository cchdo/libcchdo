from libcchdo.datadir import util
import sys
import os.path
import os
import re

def checkCDF_ex_and_woce():
	util.do_for_cruise_directories(checkCDF)
	

def checkCDF(path, dirs, cfiles):
	woce=False
	ex=False
	for fname in cfiles:
		if fname.endswith("nc_ctd.zip"):
			return
		if fname.endswith("ct1.zip"):
			ex=True
		if fname.endswith("ct.zip"):
			woce=True
	if ex or woce:
		print "ERROR: no NETCDF CTD; yes WOCE or Exchange CTD in", path


def checkbot_ex_and_woce():
	util.do_for_cruise_directories(checkbotex)
	
	

def checkbotex(path, dirs, cfiles):
	woce=False
	for fname in cfiles:
		if fname.endswith("hy1.csv"):
			return
		if fname.endswith("hy.txt") or fname.endswith(".sea"):
			woce=True
	if woce:
		print "ERROR: no Exchange bottle; yes WOCE bottle in", path
def checkCTD_ex_and_woce():
	util.do_for_cruise_directories(checkCTDex)
	
	

def checkCTDex(path, dirs, cfiles):
	woce=False
	for fname in cfiles:
		if fname.endswith("ct1.zip"):
			return
		if fname.endswith("ct.zip"):
			woce=True
	if woce:
		print "ERROR: no Exchange CTD; yes WOCE CTD in", path
