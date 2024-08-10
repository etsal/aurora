#!/usr/bin/env bash

if [ ! -z $1 ]; then
	DISK=$1
fi

if [ -z $DISK ]; then
	echo "Disk not specified, please initialize the DISK variable or pass it as an argument"
	exit 1
fi


zpool destroy test 2> /dev/null
umount /testmnt 2> /dev/null
kldunload objsnap > /dev/null 2> /dev/null

kldload objsnap/objsnap.ko

stat -x /dev/objsnap

./tools/new_objsnap/new_objsnap $DISK $2 $3 $4

kldunload objsnap
