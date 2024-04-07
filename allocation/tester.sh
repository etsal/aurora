#!/usr/bin/env bash

OBJSIZE=12
DISKSIZE=16
HOT=1
TXN=10000000
MIN=16
MAX=16

# Range over a variable
for i in $(seq 1 1 6);
do
    echo "./main -x "$TXN" -o "$OBJSIZE" -s "$DISKSIZE" -e "$i" -l "$MIN" -m "$MAX" -c"
    ./main -x "$TXN" -o "$OBJSIZE" -s "$DISKSIZE" -e "$i" -l "$MIN" -m "$MAX"
    wait
done

python3 graph.py *.csv

