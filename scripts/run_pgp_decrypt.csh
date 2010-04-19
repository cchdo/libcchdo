#!/bin/csh
#
# Finds all subdirectories with PGP encrypted files and decrypts them.
#
# 2009-08-12 S. Diggs
#

set location = `find . -name .passwd -print`
set script="`pwd`/pgp_decrypt.csh"

echo $location
foreach x ($location)
  pushd `dirname $x`
  $script
  popd
end

