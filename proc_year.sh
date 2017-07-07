#!/bin/bash
# Script to be run overnight that will spin through a year's rawacf info..

echo Enter a year whose data you would like processed...
read year

for month in `seq 1 12`;
do
    echo $year-$month
#    ./parse.py -y $year -m $month
#    export archive=$year-$month
#    mkdir $archive
#    cp *.log $archive/
#    cp bad*.txt $archive/
#    cp superdarntimes.sqlite $archive/
done 
