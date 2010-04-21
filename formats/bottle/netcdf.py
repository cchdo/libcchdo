'''libcchdo.formats.bottle.netcdf'''

from format import format

class netcdf(format):
    def read(self, handle):
        """How to read a Bottle NetCDF file."""
        raise NotImplementedError
    def write(self, handle):
        """How to write a Bottle NetCDF file."""
        # This time, the handle is actually a path to a tempdir to give to the
        # NetCDF library to write in.
        expocode = self.columns['EXPOCODE'][0]
        station = self.columns['STNNBR'][0].rjust(5, '0')
        cast = self.columns['CASTNO'][0].rjust(5, '0')
        filename = '_'.join(expocode, station, cast, 'hy1')+'.nc'
        fullpath = handle+'/'+filename
    
        nc_file = NetCDFFile(fullpath, 'w')
    
        # Write dimension variables
        dim_bottle = nc_file.createDimension('bottle', len(self))
        dim_time = nc_file.createDimension('time', 1)
        dim_lat = nc_file.createDimension('latitude', 1)
        dim_lng = nc_file.createDimension('longitude', 1)
        dims_variable = (dim_bottle, dim_time, dim_lat, dim_lng)
        dims_static = (dim_time, dim_lat, dim_lng)
    
        dim_string = nc_file.createDimension('string_dimension', 10)
        dims_string = (dim_string, dim_time)
    
        # Sometimes, there's no WOCE Section associated with a certain STNNBR
        # and CASTNO. In that case, let the user known it's an UNKNOWN section
        sect = self.columns['SECT_ID'][0]
        if sect is '':
            sect = 'UNKNOWN'
        setattr(nc_file, 'EXPOCODE', expocode)
        setattr(nc_file, 'Conventions', 'COARDS/WOCE')
        setattr(nc_file, 'WOCE_VERSION', '3.0')
        setattr(nc_file, 'WOCE_ID', sect)
        setattr(nc_file, 'DATA_TYPE', 'Bottle')
        setattr(nc_file, 'STATION_NUMBER', station)
        setattr(nc_file, 'CAST_NUMBER', cast)
        setattr(nc_file, 'BOTTOM_DEPTH_METERS',
                max(self.columns['DEPTH'].values))
        setattr(nc_file, 'BOTTLE_NUMBERS',
                ' '.join(self.columns['BTLNBR'].values))
        if self.columns['BTLNBR'].is_flagged_woce():
            setattr(nc_file, 'BOTTLE_QUALITY_CODES',
                    ' '.join(self.columns['BTLNBR'].flags_woce))
        now = date(1970, 1, 1).now()
        setattr(nc_file, 'Creation_Time', str(now))
        header_filter = compile('BOTTLE|db_to_exbot|jjward|(Previous stamp)')
        header = '# Previous stamp: '+self.stamp+"\n"+"\n".join(
            filter(lambda x: not header_filter.match(x),
                   self.header.split("\n")))
        setattr(nc_file, 'ORIGINAL_HEADER', header)
        setattr(nc_file, 'WOCE_BOTTLE_FLAG_DESCRIPTION', 
            ':'.join([
            ':',
            '1 = Bottle information unavailable.',
            '2 = No problems noted.',
            '3 = Leaking.',
            '4 = Did not trip correctly.',
            '5 = Not reported.',
            ('6 = Significant discrepancy in measured values between Gerard '
             'and Niskin bottles.'),
            '7 = Unknown problem.',
            ('8 = Pair did not trip correctly. Note that the Niskin bottle '
             'can trip at an unplanned depth while the Gerard trips '
             'correctly and vice versa.'),
            '9 = Samples not drawn from this bottle.',
            "\n"]))
        setattr(nc_file, 'WOCE_WATER_SAMPLE_FLAG_DESCRIPTION', 
            ':'.join([
            ':',
            ('1 = Sample for this measurement was drawn from water bottle '
             'but analysis not received.'),
            '2 = Acceptable measurement.',
            '3 = Questionable measurement.',
            '4 = Bad measurement.',
            '5 = Not reported.',
            '6 = Mean of replicate measurements.',
            '7 = Manual chromatographic peak measurement.',
            '8 = Irregular digital chromatographic peak integration.',
            '9 = Sample not drawn for this measurement from this bottle.',
            "\n"]))
        ncvar = {}
        ncflagvar = {}
        for param, column in iter(self.columns):
            parameter = column.parameter
            parameter_name = parameter.mnemonic
            # continue if STATIC_PARAMETERS_PER_CAST.include parameter_name
            # TODO
        var_time = nc_file.createVariable('time', 'f', dims_static)
        setattr(var_time, 'long_name', 'time')
        setattr(var_time, 'units', 'minutes since 1980-01-01 00:00:00')
        setattr(var_time, 'data_min', 0)
        setattr(var_time, 'data_max', 0)
        setattr(var_time, 'C_format', '%10d')
    
        var_latitude = nc_file.createVariable('latitude', 'f', dims_static)
        setattr(var_latitude, 'long_name', 'latitude')
        setattr(var_latitude, 'units', 'degrees_N')
        setattr(var_latitude, 'data_min', 0)
        setattr(var_latitude, 'data_max', 0)
        setattr(var_latitude, 'C_format', '%9.4f')
    
        var_longitude = nc_file.createVariable('longitude', 'f', dims_static)
        setattr(var_longitude, 'long_name', 'longitude')
        setattr(var_longitude, 'units', 'degrees_E')
        setattr(var_longitude, 'data_min', 0)
        setattr(var_longitude, 'data_max', 0)
        setattr(var_longitude, 'C_format', '%9.4f')
    
        var_woce_date = nc_file.createVariable('woce_date', 'i', dims_static)
        setattr(var_woce_date, 'long_name', 'WOCE date')
        setattr(var_woce_date, 'units', 'yyyymdd UTC')
        setattr(var_woce_date, 'data_min', 0)#long min
        setattr(var_woce_date, 'data_max', 0)#long max
        setattr(var_woce_date, 'C_format', '%8d')
        
        var_woce_time = nc_file.createVariable('woce_time', 'i', dims_static)
        setattr(var_woce_time, 'long_name', 'WOCE time')
        setattr(var_woce_time, 'units', 'hhmm UTC')
        setattr(var_woce_time, 'data_min', 0)#long min
        setattr(var_woce_time, 'data_max', 0)#long max
        setattr(var_woce_time, 'C_format', '%4d')
        
        # Hydrographic specific
        
        var_station = nc_file.createVariable('station', 'c', dims_string)
        setattr(var_station, 'long_name', 'STATION')
        setattr(var_station, 'units', 'unspecified')
        setattr(var_station, 'C_format', '%s')
        
        var_cast = nc_file.createVariable('cast', 'c', dims_string)
        setattr(var_cast, 'long_name', 'CAST')
        setattr(var_cast, 'units', 'unspecified')
        setattr(var_cast, 'C_format', '%s')
    
        # Write out pairs TODO
    
        datetime = self.columns['DATE'][0]+self.columns['TIME']
        time_from_epoch = datetime # TODO
        cchdo_epoch_offset = datetime.date(1980, 01, 01)
        var_time[:] = (time_from_epoch - cchdo_epoch_offset)
    
        nc_file.close()
