#!/usr/bin/env bash
zpool destroy test 2> /dev/null
umount /testmnt 2> /dev/null
kldunload objsnap > /dev/null 2> /dev/null

kldload objsnap/objsnap.ko

stat -x /dev/objsnap

sysctl -f conf.sys 2> /dev/null

./tools/new_objsnap/new_objsnap "$@"

kldunload objsnap
