#!/bin/sh

OID=1050

. aurora
aursetup

TESTDIR=$PWD

cd $MNT

"$TESTDIR/sasfork/sasfork" "$MNT" 1000 &
PID=$!
echo $PID

slsctl partadd slos -o $OID -t 100 -d -i
slsctl attach -o $OID -p $PID
slsctl checkpoint -o $OID -r

wait $PID
CODE=$?

sysctl aurora

cd $TESTDIR
if [ $CODE -ne 0 ];
then
    echo "Test SAS returned code $CODE"
    aurteardown
    exit 1
fi

cd $TESTDIR
aurteardown
