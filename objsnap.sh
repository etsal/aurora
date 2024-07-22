#!/usr/bin/env bash

if [ ! -z $1 ]; then
	DISK=$1
fi

if [ -z $DISK ]; then
	echo "Disk not specified, please initialize the DISK variable or pass it as an argument"
	exit 1
fi

kldload objsnap/objsnap.ko

stat -x /dev/objsnap

./tools/new_objsnap/new_objsnap $DISK $2

kldunload objsnap
