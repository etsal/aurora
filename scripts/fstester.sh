#!/usr/bin/env bash

DISK="/dev/nvd0"
ARGS="--name=random_write_fsync --filename=/testmnt/test --rw=randwrite --bs=4k --runtime=60 --group_reporting --new_group"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
run() {

	fio $ARGS --direct=1 --iodepth=1 --fsync=1 --numjobs=$1 --size=10G --output=/tmp/run.out --output-format=json &
	sleep 20
	iostat -d nvd0 1 20 > /tmp/gstat.out
	wait
	throughput_mbs=$(cat /tmp/gstat.out | tail -n 19 | awk '{ sum += $3; n++ } END { if (n > 0) print sum / n; }')
	iops=$($SCRIPT_DIR/jsparse.py /tmp/run.out jobs 0 write iops)
	lat_ns=$($SCRIPT_DIR/jsparse.py /tmp/run.out jobs 0 write clat_ns mean)
	lat_99=$($SCRIPT_DIR/jsparse.py /tmp/run.out jobs 0 write clat_ns percentile string:99.000000)
	iokbytes=$($SCRIPT_DIR/jsparse.py /tmp/run.out jobs 0 write clat_ns percentile string:99.000000)
	goodput_kib=$($SCRIPT_DIR/jsparse.py /tmp/run.out jobs 0 write bw)
	goodput_mibs=$(expr $goodput_kib / 1024)
	echo "$3, $i, $iops, $lat_ns, $lat_99, $goodput_mibs, $throughput_mbs" >> "$2"
}

test_zfs() {
	for i in $(seq 1 $1) 
	do
		zpool create "test" $DISK
		zfs create "test/test"
		zfs set recordsize=4K "test/test"
		zfs set mountpoint=/testmnt test/test
		zfs set sync=disabled test/test
		zfs set compression=off test/test
		touch "/testmnt/test"
		truncate -s 0 "/testmnt/test"

		run "$i" "$2" "zfs"

		umount /testmnt
		zpool destroy "test"
	done
}

test_ffs() {
	for i in $(seq 1 $1) 
	do
		newfs -b 4096 "$DISK"
		mount "$DISK" /testmnt
		touch "/testmnt/test"
		truncate -s 0 "/testmnt/test"

		run "$i" "$2" "ffs"

		umount /testmnt
	done
}

test_objsnap() {
	CKPT=1000000
	for i in $(seq 1 $1) 
	do
		$SCRIPT_DIR/../objsnap.sh /dev/nvd0 $i 1 $(expr $CKPT / $i) | tail -n -1 > backingfile &
		sleep 10
		iostat -d nvd0 1 10 > /tmp/gstat.out
		throughput_mbs=$(cat /tmp/gstat.out | tail -n 9 | awk '{ sum += $3; n++ } END { if (n > 0) print sum / n; }')

		exec 3< backingfile
		rm backingfile

		wait
		echo "$(cat <&3), $throughput_mbs" >> "$2"
	done
}


OUT="out"
truncate -s 0 "$OUT"
echo "fs, num_threads, iops, lat_ns, lat_99_ns, goodput_kibs, throughput_mibs," >> "$OUT"
test_zfs 24 "$OUT"
test_ffs 24 "$OUT"
test_objsnap 24 "$OUT"
