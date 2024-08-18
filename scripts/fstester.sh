#!/usr/bin/env bash

DISK="/dev/nvd0"
ARGS="--name=random_write_fsync --filename=/testmnt/test --rw=randwrite --bs=4k --runtime=60 --group_reporting --new_group"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
run() {
	iostat -d nvd0 1 > /tmp/gstat.out &
	sleep 2
	IOSTAT_PID=$!
	fio $ARGS --iodepth=1 --fsync=1 --numjobs=$1 --size=10G --output=/tmp/run.out --output-format=json &
	PID=$!
	wait $PID
	sleep 5
	kill -SIGINT $IOSTAT_PID
	lines=$(wc -l /tmp/gstat.out)
	grep -v 'nvd0' /tmp/gstat.out > /tmp/temp
	grep -v 'tps' /tmp/temp > /tmp/gstat.out
	throughput_mib=$(cat /tmp/gstat.out | tail -n +2 | awk '{ sum += $3} END { print sum }')
	disk_iops=$(cat /tmp/gstat.out | tail -n +2 | awk '{ sum += $2 } END { print sum }')
	iops=$($SCRIPT_DIR/jsparse.py /tmp/run.out jobs 0 write iops)
	lat_ns=$($SCRIPT_DIR/jsparse.py /tmp/run.out jobs 0 write clat_ns mean)
	lat_99=$($SCRIPT_DIR/jsparse.py /tmp/run.out jobs 0 write clat_ns percentile string:99.000000)
	iokbytes=$($SCRIPT_DIR/jsparse.py /tmp/run.out jobs 0 write clat_ns percentile string:99.000000)
	goodput_kib=$($SCRIPT_DIR/jsparse.py /tmp/run.out jobs 0 write io_kbytes)
	goodput_mib=$(expr $goodput_kib / 1024)
	echo "$3, $i, $iops, $lat_ns, $lat_99, $goodput_mib, $throughput_mib, $disk_iops" >> "$2"
}

test_zfs() {
	for i in $(seq 1 $1) 
	do
		zpool create "test" $DISK
		zfs create "test/test"
		zfs set recordsize=4K "test/test"
		zfs set mountpoint=/testmnt test/test
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
	for i in $(seq 1 4 $1) 
	do
		iostat -hd nvd0 1 > /tmp/gstat.out &
		IOSTAT_PID=$!
		sleep 2
		$SCRIPT_DIR/../objsnap.sh /dev/nvd0 $i 1 60 | tail -n -1 > backingfile &
		PID=$!	
		wait $PID
		kill -INT $IOSTAT_PID
		grep -v 'nvd0' /tmp/gstat.out > /tmp/temp
		grep -v 'tps' /tmp/temp > /tmp/gstat.out
		lines=$(wc -l /tmp/gstat.out | awk '{ print $1}')
		throughput_mbs=$(cat /tmp/gstat.out | tail -n +2 | awk '{ sum += $3 } END { print sum }')
		disk_iops=$(cat /tmp/gstat.out | tail -n +2 | awk '{ sum += $2 } END { print sum }')
		rm /tmp/temp

		exec 3< backingfile
		rm backingfile

		echo "$(cat <&3), $throughput_mbs, $disk_iops" >> "$2"
	done
}


OUT="out"
THREADS=24
truncate -s 0 "$OUT"
echo "fs,num_threads,iops,lat_ns,lat_99_ns,goodput_mib,throughput_mib,disk_iops" >> "$OUT"
#test_objsnap $THREADS "$OUT"
test_zfs $THREADS "$OUT"
test_ffs $THREADS "$OUT"
