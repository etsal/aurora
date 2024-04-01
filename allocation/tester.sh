#!/usr/bin/env bash

OBJSIZE=28
DISKSIZE=32
HOT=2
TXN=20000000
MIN=2
MAX=8

# Range over a variable
for i in $(seq 2 2 24);
do
    ./main -x "$TXN" -o "$OBJSIZE" -s "$DISKSIZE" -e "$i" -l "$MIN" -m "$MAX" -c
    ./main -x "$TXN" -o "$OBJSIZE" -s "$DISKSIZE" -e "$i" -l "$MIN" -m "$MAX"
done

python3 graph.py *.csv

