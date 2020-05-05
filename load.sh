#!/bin/sh

gstripe destroy st0
gstripe create -s 1048576 -v st0 vtbd1 vtbd2 vtbd3
#gstripe create -s 65536 -v st0 nvd0 nvd1 nvd2 nvd3
gstripe destroy st0
#gstripe create -s 65536 -v st0 nvd0 nvd1 nvd2 nvd3
gstripe create -s 1048576 -v st0 vtbd1 vtbd2 vtbd3

DRIVE=/dev/stripe/st0

#DRIVE=/dev/vtbd1

./tools/newosd/newosd $DRIVE

kldload slos/slos.ko

mount -rw -t slsfs $DRIVE /testmnt

#fio trace/test.fio

#echo "hello" > /testmnt/hello
#kldload kmod/sls.ko
#filebench -f trace/varmail.f &2> /dev/null
##sleep 10
#filebench -f trace/randomrw.f &2> /dev/null

#kldunload sls
#umount /testmnt
#kldunload slos

#mount -rw -t slsfs /dev/vtbd1 /testmnt


#mkdir -p /testmnt/dingdong/hello/2/
