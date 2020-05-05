set $dir=/testmnt
set $filesize=1g
set $iosize=64k
set $nthreads=1
set $workingset=0
set $directio=0

define file name=largefile1,path=$dir,size=$filesize,prealloc,reuse,paralloc
define file name=largefile2,path=$dir,size=$filesize,prealloc,reuse,paralloc
define file name=largefile3,path=$dir,size=$filesize,prealloc,reuse,paralloc
define file name=largefile4,path=$dir,size=$filesize,prealloc,reuse,paralloc

define process name=rand-rw,instances=1
{
  thread name=thwr1,memsize=5m,instances=$nthreads
  {
    flowop write name=rw1,filename=largefile1,iosize=$iosize,random,workingset=$workingset,directio=$directio
  }
  thread name=thwr2,memsize=5m,instances=$nthreads
  {
    flowop write name=rw2,filename=largefile2,iosize=$iosize,random,workingset=$workingset,directio=$directio
  }

  thread name=thwr3,memsize=5m,instances=$nthreads
  {
    flowop write name=rw3,filename=largefile3,iosize=$iosize,random,workingset=$workingset,directio=$directio
  }

  thread name=thwr4,memsize=5m,instances=$nthreads
  {
    flowop write name=rw4,filename=largefile4,iosize=$iosize,random,workingset=$workingset,directio=$directio
  }

}

run 10

echo "Random RW Version 3.0 personality successfully loaded"
