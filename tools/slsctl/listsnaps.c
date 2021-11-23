#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/queue.h>
#include <sys/sbuf.h>

#include <fcntl.h>
#include <getopt.h>
#include <slos.h>
#include <slsfs.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

void
listsnaps_usage(void)
{
	printf("Usage: slsctl listsnaps -m <mount_dir> [-l]");
}

void
mountsnap_usage(void)
{
	printf(
	    "Usage: slsctl mountsnap -m <mount_dir> -i <int>\nIndex of -1 mounts latest, all other snapshots are mounted read-only");
}

static void
print_snap(struct slsfs_getsnapinfo *inf)
{
	struct slos_sb *sb = &inf->snap_sb;
	if (sb->sb_epoch == EPOCH_INVAL || sb->sb_root.offset == 0 ||
	    sb->sb_allocoffset.offset == 0 || sb->sb_allocsize.offset == 0) {
		return;
	}
	printf("Snap %lu - %d/100\n", inf->snap_sb.sb_epoch, sb->sb_index);
	printf("Locations:\n");
	printf("\tInodes Root: %lu\n", sb->sb_root.offset);
	printf("\tAllocator Offset Tree: %lu\n", sb->sb_allocoffset.offset);
	printf("\tAllocator Size Tree: %lu\n", sb->sb_allocsize.offset);
}

int
mountsnap_main(int argc, char *argv[])
{
	int opt;
	char mountdir[255];
	int mountgiven = 0;
	int index = 0;
	struct slsfs_getsnapinfo info;
	while ((opt = getopt(argc, argv, "m:i:")) != -1) {
		switch (opt) {
		case 'm':
			mountgiven = 1;
			strcpy(mountdir, optarg);
			break;
		case 'i':
			index = strtol(optarg, NULL, 10);
			break;
		default:
			listsnaps_usage();
			return 0;
		}
	}

	if (!mountgiven) {
		mountsnap_usage();
		return (0);
	}

	int fd = open(mountdir, O_RDONLY);
	info.index = index;
	return ioctl(fd, SLSFS_MOUNT_SNAP, &info);
}

int
modulo(int x, int N)
{
	return (x % N + N) % N;
}

int
listsnaps_main(int argc, char *argv[])
{
	int opt;
	char mountdir[255];
	int mountgiven = 0;
	int show_last_only = false;
	struct slsfs_getsnapinfo info;
	while ((opt = getopt(argc, argv, "m:l")) != -1) {
		switch (opt) {
		case 'm':
			mountgiven = 1;
			strcpy(mountdir, optarg);
			break;
		case 'l':
			show_last_only = true;
			break;
		default:
			listsnaps_usage();
			return 0;
		}
	}

	if (!mountgiven) {
		listsnaps_usage();
		return (0);
	}

	int fd = open(mountdir, O_RDONLY);
	int i = 0;
	for (; (i < NUMSBS) && (info.snap_sb.sb_epoch != EPOCH_INVAL); i++) {
		info.index = i;
		info.snap_sb.sb_epoch = EPOCH_INVAL;
		ioctl(fd, SLSFS_GET_SNAP, &info);
		if (!show_last_only)
			print_snap(&info);
	}

	if (show_last_only) {
		/*
		 * We must use true modulo here in case we decrement to a
		 * negative number
		 */
		info.index = modulo(i - 1, NUMSBS);
		ioctl(fd, SLSFS_GET_SNAP, &info);
		print_snap(&info);
		return info.index;
	}
	return (0);
}
