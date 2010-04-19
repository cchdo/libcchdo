#!/bin/bash

# initialize the paths first from the user's .bashrc file
. ~/.bashrc

datafile=${1}
output_file="${datafile}.ps"

pscoast -R000/360/80S/85N -JM5i -B30g30 -K -P -G >| ${output_file}
psxy ${datafile}  -K  -R -JM5i -O -Sc.08 -G255/000/000 >> ${output_file}

gs ${output_file}
