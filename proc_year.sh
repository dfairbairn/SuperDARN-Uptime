#!/bin/bash
# Script to be run over several days that will spin through a year's rawacf info

echo Enter a year whose data you would like processed...
read YEAR 

for MONTH in `seq 1 12`;
do
    echo $YEAR-$MONTH
    ./parse.py -y $YEAR -m $MONTH
    export archive=data/$YEAR-$MONTH
    mkdir $archive
    mv *.log $archive/
    mv bad*.txt $archive/
    cp superdarntimes.sqlite $archive/
done 

mv superdarntimes.sqlite "$YEAR"data.sqlite
