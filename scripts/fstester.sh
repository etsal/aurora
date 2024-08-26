#!/usr/bin/env bash

DISK="/dev/nvd0"
ARGS="--name=random_write_fsync --filename=/testmnt/test --rw=randwrite --runtime=60 --group_reporting --new_group"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
run() {
	iostat -d nvd0 1 > /tmp/gstat.out &
	sleep 2
	IOSTAT_PID=$!
	fio $ARGS --iodepth=1 --fsync=1 --numjobs=$1 --size=10G --output=/tmp/run.out --output-format=json --bs="$4" &
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
	echo "$3, $1, $4, $iops, $lat_ns, $lat_99, $goodput_mib, $throughput_mib, $disk_iops" >> "$2"
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

		run "$i" "$2" "zfs" "4096"

		umount /testmnt
		zpool destroy "test"
	done
}

test_ffs_journal() {
	for i in $(seq 1 $1) 
	do
		newfs -b 4096 "$DISK"
		tunefs -j enable "$DISK"
		tunefs -n enable "$DISK"
		tunefs -p "$DISK"
		mount "$DISK" /testmnt
		touch "/testmnt/test"
		truncate -s 0 "/testmnt/test"

		run "$i" "$2" "ffs+journal" "4096"

		umount /testmnt
	done
}

test_ffs() {
	for i in $(seq 1 $1) 
	do
		newfs -b 4096 "$DISK"
		tunefs -n enable "$DISK"
		tunefs -j disable "$DISK"
		tunefs -p "$DISK"
		mount "$DISK" /testmnt
		touch "/testmnt/test"
		truncate -s 0 "/testmnt/test"

		run "$i" "$2" "ffs+su" "4096"

		umount /testmnt
	done
}

test_ffs_bs() {
	for i in $(seq 1 $1) 
	do
		newfs "$DISK"
		tunefs -n enable "$DISK"
		tunefs -j disable "$DISK"
		tunefs -p "$DISK"
		mount "$DISK" /testmnt
		touch "/testmnt/test"
		truncate -s 0 "/testmnt/test"

		run "$i" "$2" "ffs+su+defaultbs" "4096"

		umount /testmnt
	done
}

run_once_objsnap() {
	iostat -hd nvd0 1 > /tmp/gstat.out &
	IOSTAT=$(pgrep iostat)
	sleep 2
	$SCRIPT_DIR/../objsnap.sh /dev/nvd0 $1 "$3" 60 | tail -n -1 > backingfile &
	WAITFOR=$!
	sleep 5
	PID=$(pgrep objsnap)
	sleep 20
	cpu=$(top -p "$PID" -Hb | tail -n +7 | awk '{ print $10 }' | sed 's/.$//' | awk '{ sum += $1; n++ } END { print sum / n;}')
	wait "$WAITFOR"
	kill -INT "$IOSTAT"
	grep -v 'nvd0' /tmp/gstat.out > /tmp/temp
	grep -v 'tps' /tmp/temp > /tmp/gstat.out
	lines=$(wc -l /tmp/gstat.out | awk '{ print $1}')
	throughput_mbs=$(cat /tmp/gstat.out | tail -n +2 | awk '{ sum += $3 } END { print sum }')
	disk_iops=$(cat /tmp/gstat.out | tail -n +2 | awk '{ sum += $2 } END { print sum }')
	rm /tmp/temp

	exec 3< backingfile
	rm backingfile
	echo "$(cat <&3), $throughput_mbs, $disk_iops, $cpu" >> $2
}

test_objsnap() {
	for i in $(seq 1 $1) 
	do
		run_once_objsnap "$i" "$2" "$3"
	done
}

test_zfs_dirtyset() {
	for i in $(seq 1 $3) 
	do
		zpool create "test" $DISK
		zfs create "test/test"
		zfs set recordsize=4K "test/test"
		zfs set mountpoint=/testmnt test/test
		zfs set compression=off test/test
		touch "/testmnt/test"
		truncate -s 0 "/testmnt/test"

		run $1 "$2" "zfs" "$((4096 * $i))"

		umount /testmnt
		zpool destroy "test"
	done
}

test_objsnap_dirtyset() {
	for i in $(seq 1 $3) 
	do
		run_once_objsnap "$1" "$2" "$i"
	done
}


OUT="out"
DIRTYSETOUT="dirtyset"
THREADS=24
MAXDIRTYSET=8
#truncate -s 0 "$OUT"
#echo "fs,num_threads,dirty_size,iops,lat_ns,lat_99_ns,goodput_mib,throughput_mib,disk_iops,avgcpu" >> "$OUT"
#test_objsnap $THREADS "$OUT" "1"
#test_ffs_journal $THREADS "$OUT"
#test_ffs_bs $THREADS "$OUT"
#test_ffs $THREADS "$OUT"
#test_zfs $THREADS "$OUT"

truncate -s 0 "$DIRTYSETOUT"
echo "fs,num_threads,dirty_size,iops,lat_ns,lat_99_ns,goodput_mib,throughput_mib,disk_iops,avgcpu" >> "$DIRTYSETOUT"
test_objsnap_dirtyset $THREADS "$DIRTYSETOUT" $MAXDIRTYSET
test_zfs_dirtyset $THREADS "$DIRTYSETOUT" $MAXDIRTYSET
