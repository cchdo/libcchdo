#!/usr/bin/env python

# Universal ExpoCode replacement
#
# Does ExpoCode replacing for all types of CCHDO files. This accounts for
# header shifting and stuff like that on positionally based formats e.g. WOCE
#
# Blame
# -----
# 2009-07-31 myshen   Initial coding.

from __future__ import with_statement
from sys import argv, exit

if len(argv) < 4:
  print 'Usage: ', argv[0], 'old_expocode replacement_expocode file [files...]'
  exit()

expocode, replacement = argv[1:3]
files = argv[3:]

def is_SUM_WOCE(filename):
  return filename.endswith('su.txt')
def is_Bottle_WOCE(filename):
  return filename.endswith('hy.txt')
def is_Bottle_Exchange(filename):
  return filename.endswith('hy1.csv')
def is_CTD_WOCE(filename):
  return filename.endswith('.ctd')
# etc...

suffix = '.new_expocode'

def file_replace_every(infilename, outfilename, old, new):
  print 'Simple line by line replacement will do.'
  with open(infilename, 'r') as infile:
    with open(outfilename, 'w') as outfile:
      for line in infile:
        outfile.write(line.replace(old, new))

for file in files:
  print 'Replacing', expocode, 'with', replacement, 'for the header of', file

  # Only need special treatment of positional files for SUM and WOCE CTD
  if is_SUM_WOCE(file) or is_CTD_WOCE(file):
    shift = len(replacement) - len(expocode)
    if shift > 0:
      if is_SUM_WOCE(file):
        # We can easily make space for the header ExpoCode but need to shift
        # THE WHOLE FILE right. Fun.
        with open(file, 'r') as infile:
          with open(file+suffix, 'w') as outfile:
            # For the first line, do a line replace. Then do shifts until the
            # header delimiter line (-xn---) and then do line replace.
            outfile.write(infile.readline().replace(expocode, replacement))
            is_past_header = False
            for line in infile:
              print line
              if is_past_header:
                outfile.write(line.replace(expocode, replacement))
              else:
                if line[0:3] == '---':
                  is_past_header = True
                first_space = line.find(' ')
                outfile.write(line[:first_space]+(' ' * shift)+
                              line[first_space:])
      else:
        # In a WOCE CTD there is only the header ExpoCode to expand for.
        # Check if there is enough space to expand to the left and not run into
        # the previous token.
        with open(file, 'r') as infile:
          line = infile.readline()
          index = line.find(expocode)
          if line[index-shift-1:index] == ' ' * (shift+1):
            file_replace_every(file, file+suffix,
                               (' ' * shift) + expocode,
                               replacement)
          else:
            print 'Not enough space in the file to put the new ExpoCode!'
            exit()
    else:
      # The new expocode is shorter. This means we don't need to shift, but just
      # (l/r)just the new one to the older ones length and do a line replace.
      if is_SUM_WOCE(file):
        file_replace_every(file, file+suffix, expocode,
                           replacement.ljust(len(expocode), ' '))
      else:
        file_replace_every(file, file+suffix, expocode,
                           replacement.rjust(len(expocode), ' '))
  else:
    if is_Bottle_Exchange(file):
      bottle_file_expocode_len = 11
      extra_length = 11-len(expocode)
      file_replace_every(file, file+suffix, (' '*extra_length)+expocode,
                         replacement.rjust(bottle_file_expocode_len, ' '))
    else:
      file_replace_every(file, file+suffix, expocode, replacement)
