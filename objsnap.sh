#!/usr/bin/env bash

kldload objsnap/objsnap.ko

stat -x /dev/objsnap

./tools/new_objsnap/new_objsnap /dev/nvd0

kldunload objsnap
