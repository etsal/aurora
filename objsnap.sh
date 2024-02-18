#!/usr/bin/env bash

kldload objsnap/objsnap.ko

stat -x /dev/objsnap

echo "INTING OSNAP"

./tools/new_objsnap/new_objsnap

echo "DONE"

kldunload objsnap