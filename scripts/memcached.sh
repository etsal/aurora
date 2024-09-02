MEMCACHED_DIR="/root/memcached/"
MEMCACHED_ARGS="-R 10000 -m 16384 -u root -o no_lru_crawler,no_lru_maintainer -C -c 4096"
RUNNER_IP="192.168.2.132"
MEMCACHED_IP="192.168.2.131"
MEMTIER_ARGS="-d 512 -n 5000 -t 12 -c 64 -s $MEMCACHED_IP -p 11211 -4 -P memcache_text"
OBJSNAP="/root/ryan/objsnap/objsnap.ko"
MEMCACHED_OBJSNAP_ARGS="-e $MEMCACHED_DIR/dummyfile"
KEY="/root/.ssh/sky5"
USER="ryan"

MEMCACHE_PID=""
startup() {
	kldload $OBJSNAP
	$MEMCACHED_DIR/memcached $1 $MEMCACHED_ARGS > /tmp/serverout 2> /tmp/serverout  &
	MEMCACHE_PID=$!
}

stop_mc() {
	kill -INT $MEMCACHE_PID 2> /dev/null > /dev/null
	kldunload $OBJSNAP 2> /dev/null > /dev/null
}

runner_go() {
	READ="$2"
	WRITE="$3"
	ssh -i "$KEY" $USER@$RUNNER_IP "memtier_benchmark --ratio=$WRITE:$READ $MEMTIER_ARGS" 2> /dev/null > /tmp/results
	ops=$(cat /tmp/results | grep -A 7 "ALL STATS" | tail -n 4 | awk '{print $2}' | tail -n 1)
	set_lat=$(cat /tmp/results | grep -A 7 "ALL STATS" | tail -n 4 | awk '{print $5}' | tail -n 1)
	echo "$1,$2,$3,$ops,$set_lat"
}

build() {
	cur=$PWD
	cd $MEMCACHED_DIR
	make clean 2> /dev/null > /dev/null
	make CFLAGS="$1" -j 8  2> /dev/null > /dev/null
	cd $cur
}


TOP=8
run_objsnap() {
	build "-DOBJSNAP=1"
	for i in $(seq 1 $TOP)
	do
		startup "$MEMCACHED_OBJSNAP_ARGS"
		sleep 5
		runner_go "objsnap" $TOP $i
		sleep 5
		stop_mc
	done
}

run_base() {
	build ""
	for i in $(seq 1 $TOP)
	do
		startup ""
		sleep 5
		runner_go "base" $TOP $i
		sleep 5
		stop_mc
	done
}

stop_mc
echo "type,read,write,ops,set_lat_ms"
run_objsnap
run_base


