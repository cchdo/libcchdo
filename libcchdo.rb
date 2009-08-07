# Sketch: proposal for libcchdo
#
# If this is a library, it may be used in pieces of code that don't want output:
#   Raise exceptions for all errors
#     no prompting
#     no writing to STDOUT/STDERR
#     leave error handling to the user-they can prompt/write as much as they please

require 'rubygems'
require 'active_record'
require 'zip/zip'

#ActiveRecord::Base.establish_connection(:adapter => 'postgresql', :host => 'cchdo.ucsd.edu', :database => 'cchdo',
#                                        :username => 'libcchdo', :password => '')
#
#class Parameter < ActiveRecord::Base; end

# Global methods

# Picks the correct file type to read a file with based on its extension
def open_cchdo_file(filename)
  raise "#{filename} does not exist" unless FileTest.file? filename
  if filename =~ /zip$/: 
    datafile = DataFileCollection.new
  else
    datafile = DataFile.new
  end
  case filename
    when /su\.txt$/: datafile.read_SUM_WOCE(filename)
    when /hy\.txt$/: datafile.read_Bottle_WOCE(filename)
    when /hy1\.csv$/: datafile.read_Bottle_Exchange(filename)
    when /nc_hyd.zip$/: datafile.read_Bottle_netCDF(filename)
    when /ct\.zip$/: datafile.read_CTDZip_WOCE(filename)
    when /ct1\.zip$/: datafile.read_CTDZip_Exchange(filename)
    when /nc_ctd\.zip$/: datafile.read_CTDZip_netCDF(filename)
  else
    raise "Unrecognized file type for #{filename}"
  end
  datafile
end

# DataFiles and Model

# A column in a DataFile. Each column has an associated parameter
class Column
  attr_reader :parameter, :values, :flags_woce, :flags_igoss
  
  def initialize(parameter_name)
    @parameter = parameter_name # Not really but I have no db right now.
    # TODO check units and warn if they don't match!
    @values = []
    @flags_woce = []
    @flags_igoss = []
  end
  
  def get(index)
    @values[index]
  end
  
  def set(index, value, flag_woce=nil, flag_igoss=nil)
    @values[index] = value
    @flags_woce[index] = flag_woce if flag_woce
    @flags_igoss[index] = flag_igoss if flag_igoss
  end

  def [](index)
    get(index)
  end
  
  def []=(index, value)
    @values[index] = value
  end
  
  def length
    @values.length
  end
  
  # Determines if any of the values are flagged, whether by WOCE or by IGOSS.
  def flagged?
    flagged_woce? or flagged_igoss?
  end
  
  # Determines if any of the values are flagged with WOCE codes.
  def flagged_woce?
    not (@flags_woce.nil? or @flags_woce.empty?)
  end
  
  # Determines if any of the values are flagged with IGOSS codes.
  def flagged_igoss?
    not (@flags_igoss.nil? or @flags_igoss.empty?)
  end
  
  # Compare Columns based on their display order.
  def <=>(other)
    @parameter.display_order - other.parameter.display_order
  end
end

# An abstraction of a datafile. The number of lines that were tracked in 
# hydro_lib is almost never used and implicit in the number of data rows; it 
# was left out. Stamp, header, and footer of the data file should be stored.
# The filename is useless once it has been read in and only necessary to open 
# a handle; thus, allow the user to use any kind of IO stream they want.
#
# All parsers should recognize when a value is clearly invalid (-999(.[09]+)) and
# represent it as nil. Range errors should raise errors instead.
#
# Possible other methods:
#   values_at(headers, range) to get a block of data
#   fake_merge(datafile) to describe conflicts that a merge would cause
class DataFile
  attr_reader :columns, :stamp, :header, :footer
  
  # Read the db into the internal representation. TODO decide on how to specify
  # what to read from the db.
  def read_db
    columns.each_pair do |header, column| # This depends on the schema
      # TODO write the column to the db
    end
  end
  
  # Write the internal representation to the db
  def write_db
    # TODO
  end

  def column_headers
    columns.keys
  end
  
  def expocodes
    columns['EXPOCODE'].uniq
  end
  
  def length
    columns.values.first.length
  end
  
  # The precisions of all the columns in their display order.
  def precisions
    columns.values.sort.collect {|column| column.parameter.precision}
  end
  
  def to_hash
    columns.to_a.inject(Hash.new) do |hash, param, column|
      hash[param] = column.values
      hash["#{param}_FLAG_W"] = column.flags_woce if column.flags_woce
      hash["#{param}_FLAG_I"] = column.flags_igoss if column.flags_igoss
    end
  end
end

# Collections of DataFiles into one.
class DataFileCollection
  attr_reader :files

  # Left for the specific DataFile to implement
  def merge(datafile)
    raise NotImplementedError
  end
  
  # Left for the specific DataFile to implement
  def split
    raise NotImplementedError
  end
  
  def stamps
    files.values.collect {|datafile| datafile.stamp}
  end
end

# Definitions of IO methods

# Raise exceptions on all illegal parts of the file, including but not limited to unknown
# parameters, mismatched parameters, units, or data, nonsense, etc. instead of listing them
# in an array. The user can then handle the error to gather a list at the end or handle 
# right away.
# Store global data as duplicates in columns? like Bottles? or as a hash of globals like CTDS?
# At the end of the IO method, all data is stored away in the columns in proper formats, e.g.
# data values are floats, ExpoCodes are strings, flags are integers, etc.
class DataFile # IO methods
  def read_Bottle_Exchange(handle)
    raise NotImplementedError
  end
  def write_Bottle_Exchange(handle)
    raise NotImplementedError
  end

  def read_Bottle_WOCE(handle)
    raise NotImplementedError
  end
  def write_Bottle_WOCE(handle)
    raise NotImplementedError
  end

  def read_Bottle_netCDF(handle)
    raise NotImplementedError
  end
  def write_Bottle_netCDF(handle)
    raise NotImplementedError
  end
  
  def read_CTD_Exchange(handle)
    raise NotImplementedError
  end
  def write_CTD_Exchange(handle)
    raise NotImplementedError
  end

  def read_CTD_WOCE(handle)
    raise NotImplementedError
  end
  def write_CTD_WOCE(handle)
    raise NotImplementedError
  end

  def read_CTD_netCDF(handle)
    raise NotImplementedError
  end
  def write_CTD_netCDF(handle)
    raise NotImplementedError
  end
end

module ZipCollection
  def foreach(&block)
    Zip::ZipFile.foreach(ARGF.filename) do |entry|
      block(entry)
    end
  end
end

class DataFileCollection # IO methods
  include ZipCollection

  def read_CTDZip_Exchange(handle)
    foreach do |entry|
      ctd_filename = entry.to_s
      next if ctd_filename.include? 'txt'
      file_stream = entry.get_input_stream
      @files[ctd_filename] = datafile = DataFile.new
      datafile.read_CTD_Exchange(file_stream)
      merge(datafile)
    end    
  end
  def write_CTDZip_Exchange(handle)
    raise NotImplementedError
  end
  
  def read_CTDZip_WOCE(handle)
    raise NotImplementedError
  end
  def write_CTDZip_WOCE(handle)
    raise NotImplementedError
  end
  
  def read_CTDZip_netCDF(handle)
    raise NotImplementedError
  end
  def write_CTDZip_netCDF(handle)
    raise NotImplementedError
  end
end

# Regions TODO maybe break this out into different parts of the library?

# A 4 dimensional location in the earth's ocean's history.
class Location < ActiveRecord::Base
  attr_accessor :coordinate, :datetime, :depth
  def initialize(coordinate, datetime=nil, depth=nil)
    @coordinate = coordinate
    @datetime = datetime
    @depth = depth
    # TODO nil axis magnitudes should be matched as a wildcard
  end
end

# A set of Locations defining a multidimensional polygon Region.
class Region
  attr_reader :name, :locations
  
  def initialize(name, *locations)
    @name = name
    @locations = locations
  end

  # Determines if the given Location is in a defined Region
  def include?(location)
    raise NotImplementedError 
  end
end

$BASINS = $REGIONS = {
  :Pacific => Region.new('Pacific', Location.new([1.111, 2.222], nil, nil), Location.new([-1.111, -2.222], nil, nil)),
  :East_Pacific => Region.new('East Pacific', Location.new([0, 0]), Location.new([1, 1]), Location.new([3, 3]))
  # TODO define the rest of the basins...maybe define bounds for other groupings
}
