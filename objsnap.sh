#!/usr/bin/env bash

kldload objsnap/objsnap.ko

stat -x /dev/objsnap

./tools/new_objsnap/new_objsnap $DISK

kldunload objsnap
