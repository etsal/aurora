#include <stdio.h>

#include <objsnap.h>

int main()
{
    int error = objsnap_init("/dev/nvme0");
    if (error) {
        printf("Error with objsnap init\n");
        return -1;
    }

    printf("Init good!\n");
}