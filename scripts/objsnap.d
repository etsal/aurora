#!/usr/sbin/dtrace -s

#pragma D option quiet
self uint64_t start, current;

fbt::allocate_txn_block:entry
{
	self->allocate = timestamp;
}

fbt::allocate_txn_block:return
{
	@tavg["allocation"] = avg(timestamp - self->allocate);
}

objsnap:::chunk_launder_start
{
	self->launder = timestamp;
}

objsnap:::chunk_launder_finish
{
	@tavg["launder"] = avg(timestamp - self->launder);
}

fbt::objsnap_io:entry
{
	self->firstio = timestamp;
}

fbt::objsnap_io:return
{
	@tavg["firstio"] = avg(timestamp - self->firstio);
}

fbt::objsnap_wal_log:entry
{
	self->wal = timestamp;
}

fbt::objsnap_wal_log:return
{
	@tavg["wal"] = avg(timestamp - self->wal);
}

fbt::ca_gc:entry
{
	self->gc = timestamp;
}

fbt::ca_gc:return
{
	@tavg["gc"] = avg(timestamp - self->gc);
}


END
{
    printa(@tavg);
}
