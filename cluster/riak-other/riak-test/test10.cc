#include <stdio.h>
#include <fcntl.h>
#include <vector>
#include <algorithm>
#include <cr_class/cr_class.h>
#include <cr_class/cr_file.h>
#include <cr_class/cr_fixedlinearstorage.h>

int main(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <filename> <file_pos>\n", argv[0]);
        exit(1);
    }

    std::string filename = argv[1];
    off_t file_pos = atoll(argv[2]);

    int fd = CR_File::open(filename.c_str(), O_RDONLY, 0644);
    if (fd < 0) {
        DPRINTF("Open file[%s] failed[%s]\n", filename.c_str(), CR_Class_NS::strerrno(errno));
        return errno;
    }

    if (CR_File::lseek(fd, file_pos, SEEK_SET) < 0) {
        DPRINTF("Seek file to %ld failed[%s]\n", (long)file_pos, CR_Class_NS::strerrno(errno));
        return errno;
    }

    CR_FixedLinearStorage fls;

    int fret = fls.LoadFromFileDescriptor(fd);
    if (fret) {
        DPRINTF("%s at load\n", CR_Class_NS::strerrno(fret));
        return fret;
    }

    return 0;
}
