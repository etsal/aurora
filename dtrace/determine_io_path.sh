#!/usr/sbin/dtrace -s

int i;

BEGIN
{
	i = 0;
}

fbt::nvd_strategy:entry
{
	if (i % 137) {
		stack();
	}
	i += 1;
}

END
{
}
