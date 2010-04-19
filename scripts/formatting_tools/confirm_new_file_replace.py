#!/usr/bin/env python

# Confirm new file replace
#
# Takes a suffix that will determine which files are the "new" files. Then
# renames all old files to have the suffix `.confirmed_old' and all the new
# files will renamed to their original filenames.
#
# Example
# -------
# Suppose you would like to confirm that all `.new_expocode' files should 
# become the real files. 
#
# ./confirm_new_file_replace.py .new_expocode
# 
# will do the trick.
#
# Blame
# -----
# 2009-07-31 myshen   Intial coding.

from sys import argv, exit
from os import listdir, rename

if len(argv) is not 2:
  print 'Usage:', argv[0], 'suffix'
  exit()

suffix = argv[1]
old_files = map(lambda file: file[:-len(suffix)],
                filter(lambda file: file.endswith(suffix), listdir('.')))
confirmed = '.confirmed_old'

for file in old_files:
  rename(file, file+confirmed)
  rename(file+suffix, file)
  print file, '->', file+confirmed, 'and', file+suffix, '->', file
