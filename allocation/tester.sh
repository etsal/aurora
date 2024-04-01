#!/usr/bin/env bash

OBJSIZE=112
DISKSIZE=128
HOT=8
TXN=50000000
MIN=2
MAX=8

# Range over a variable
for i in $(seq 8 8 96);
do
    ./main -x "$TXN" -o "$OBJSIZE" -s "$DISKSIZE" -e "$i" -l "$MIN" -m "$MAX" -c
    ./main -x "$TXN" -o "$OBJSIZE" -s "$DISKSIZE" -e "$i" -l "$MIN" -m "$MAX"
done

python3 graph.py *.csv

