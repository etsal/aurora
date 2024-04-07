#!/usr/bin/env bash

OBJSIZE=12
DISKSIZE=16
TXN=20000000
MIN=1
MAX=16

# Range over a variable
for i in $(seq 1 1 1);
do
    ./main -x "$TXN" -o "$OBJSIZE" -s "$DISKSIZE" -l "$MIN" -m "$MAX" -c
    ./main -x "$TXN" -o "$OBJSIZE" -s "$DISKSIZE" -l "$MIN" -m "$MAX"
done

python3 graph.py *.csv

