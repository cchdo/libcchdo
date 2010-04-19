#!/bin/csh
#
# --> decrypts CCHDO's PGP encrypted datafiles from way back when using .passwd files in the directories
#
# 2009.08.12: M Shen and S. Diggs
#-----
set command = "pgp +batchmode -z"
set files = "*.asc"
setenv PGPPASS `cat .passwd`

echo "key is $PGPPASS"
foreach x ($files)
  echo '***'   " ===> now processing $x"
  $command $PGPPASS $x
end

