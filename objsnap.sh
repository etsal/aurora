#!/usr/bin/env bash

if [ ! -z $1 ]; then
	DISK=$1
fi

if [ -z $DISK ]; then
	echo "Disk not specified, please initialize the DISK variable or pass it as an argument"
	exit 1
fi

THREADS=""
if [ -z $2 ]; then
	THREADS="1"
	echo "Thread number not specified, defaulting to $THREADS"
else
	THREADS="$2"
fi

DSS=""
if [ -z $3 ]; then
	DSS=$(( 1 ))
	BYTES=$(( $DSS * 4096 ))
	echo "Dirty set size not specified, defaulting to $DSS 4KiB blocks ($BYTES bytes)"
else
	DSS="$3"
fi

NUMCKPT=""
if [ -z $3 ]; then
	NUMCKPT=$(( 200 * 1000 ))
	echo "Number of checkpoints not specified, defaulting to $NUMCKPT"
else
	DSS="$3"
fi

kldunload objsnap > /dev/null 2> /dev/null
kldload objsnap

stat -x /dev/objsnap

./tools/new_objsnap/new_objsnap /dev/$DISK $THREADS $DSS $NUMCKPT

kldunload objsnap
