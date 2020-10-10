#include <cr_class/cr_file.h>
#include <cr_class/cr_class.h>
#include <errno.h>

CR_Locked<int> CR_File::retry_count(3);

int
CR_File::mkdir(const char *pathname, mode_t mode)
{
    int ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::mkdir(pathname, mode);
        if (ret == 0)
            break;
        if (errno == EEXIST)
            break;
        if (pathname[0] == '\0')
            break;
        DPRINTF("[%d]mkdir(\"%s\", %04o), %s\n",
          retry, pathname, mode, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

int
CR_File::open(const char *pathname, int flags, mode_t mode)
{
    int ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::open(pathname, flags, mode);
        if (ret >= 0)
            break;
        if (pathname[0] == '\0')
            break;
        DPRINTF("[%d]open(\"%s\", 0x%08X, %04o), %s\n",
          retry, pathname, flags, mode, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    if (ret >= 0)
        DPRINTFX(20, "open(\"%s\", 0x%08X, %04o) == %d\n", pathname, flags, mode, ret);

    return ret;
}

int
CR_File::close(int fd)
{
    int ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::close(fd);
        if (ret == 0)
            break;
        if (errno == EBADF)
            break;
        DPRINTF("[%d]close(%d), %s\n",
          retry, fd, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    if (ret == 0)
        DPRINTFX(20, "close(%d) == 0\n", fd);

    return ret;
}

int
CR_File::fsync(int fd)
{
    int ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::fsync(fd);
        if (ret == 0)
            break;
        if (errno == EBADF)
            break;
        DPRINTF("[%d]fsync(%d), %s\n",
          retry, fd, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

off_t
CR_File::lseek(int fd, off_t offset, int whence)
{
    off_t ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::lseek(fd, offset, whence);
        if (ret >= 0)
            break;
        if (errno == EBADF)
            break;
        DPRINTF("[%d]lseek(%d, %lld, %d), %s\n",
          retry, fd, (long long)offset, whence, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

ssize_t
CR_File::read(int fd, void *buf, size_t count)
{
    ssize_t ret = 0;
    int retry = CR_File::retry_count.value();
    off_t cur_fpos = CR_File::lseek(fd, 0, SEEK_CUR);

    do {
        ret = ::read(fd, buf, count);
        if (ret >= 0)
            break;
        if (errno == EBADF)
            break;
        if (cur_fpos < 0)
            break;
        int cur_errno = errno;
        if (CR_File::lseek(fd, cur_fpos, SEEK_SET) < 0) {
            errno = cur_errno;
            break;
        }
        DPRINTF("[%d]read(%d, %p, %lld), %s\n",
          retry, fd, buf, (long long)count, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

ssize_t
CR_File::write(int fd, const void *buf, size_t count)
{
    ssize_t ret = 0;
    int retry = CR_File::retry_count.value();
    off_t cur_fpos = CR_File::lseek(fd, 0, SEEK_CUR);

    do {
        ret = ::write(fd, buf, count);
        if (ret >= 0)
            break;
        if (errno == EBADF)
            break;
        if (cur_fpos < 0)
            break;
        int cur_errno = errno;
        if (CR_File::lseek(fd, cur_fpos, SEEK_SET) < 0) {
            errno = cur_errno;
            break;
        }
        DPRINTF("[%d]write(%d, %p, %lld), %s\n",
          retry, fd, buf, (long long)count, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

int
CR_File::unlink(const char *pathname)
{
    int ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::unlink(pathname);
        if (ret == 0)
            break;
        if (pathname[0] == '\0')
            break;
        DPRINTF("[%d]unlink(\"%s\"), %s\n",
          retry, pathname, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

int
CR_File::rmdir(const char *pathname)
{
    int ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::rmdir(pathname);
        if (ret == 0)
            break;
        if (pathname[0] == '\0')
            break;
        DPRINTF("[%d]rmdir(\"%s\"), %s\n",
          retry, pathname, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

int
CR_File::rename(const char *oldpath, const char *newpath)
{
    int ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::rename(oldpath, newpath);
        if (ret == 0)
            break;
        if ((oldpath[0] == '\0') || (newpath[0] == '\0'))
            break;
        DPRINTF("[%d]rename(\"%s\", \"%s\"), %s\n",
          retry, oldpath, newpath, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

int
CR_File::link(const char *oldpath, const char *newpath)
{
    int ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::link(oldpath, newpath);
        if (ret == 0)
            break;
        if ((oldpath[0] == '\0') || (newpath[0] == '\0'))
            break;
        DPRINTF("[%d]link(\"%s\", \"%s\"), %s\n",
          retry, oldpath, newpath, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

int
CR_File::symlink(const char *oldpath, const char *newpath)
{
    int ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::symlink(oldpath, newpath);
        if (ret == 0)
            break;
        if ((oldpath[0] == '\0') || (newpath[0] == '\0'))
            break;
        DPRINTF("[%d]symlink(\"%s\", \"%s\"), %s\n",
          retry, oldpath, newpath, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

int
CR_File::chown(const char *path, uid_t owner, gid_t group)
{
    int ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::chown(path, owner, group);
        if (ret == 0)
            break;
        if (path[0] == '\0')
            break;
        DPRINTF("[%d]chown(\"%s\", %d, %d), %s\n",
          retry, path, owner, group, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

int
CR_File::fchown(int fd, uid_t owner, gid_t group)
{
    int ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::fchown(fd, owner, group);
        if (ret == 0)
            break;
        if (errno == EBADF)
            break;
        DPRINTF("[%d]fchown(%d, %d, %d), %s\n",
          retry, fd, owner, group, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

int
CR_File::lchown(const char *path, uid_t owner, gid_t group)
{
    int ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::lchown(path, owner, group);
        if (ret == 0)
            break;
        if (path[0] == '\0')
            break;
        DPRINTF("[%d]lchown(\"%s\", %d, %d), %s\n",
          retry, path, owner, group, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

int
CR_File::chmod(const char *path, mode_t mode)
{
    int ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::chmod(path, mode);
        if (ret == 0)
            break;
        if (path[0] == '\0')
            break;
        DPRINTF("[%d]chmod(\"%s\", %04o), %s\n",
          retry, path, mode, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

int
CR_File::fchmod(int fd, mode_t mode)
{
    int ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::fchmod(fd, mode);
        if (ret == 0)
            break;
        if (errno == EBADF)
            break;
        DPRINTF("[%d]fchmod(%d, %04o), %s\n",
          retry, fd, mode, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

int
CR_File::flock(int fd, int operation)
{
    int ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::flock(fd, operation);
        if (ret == 0)
            break;
        if (errno == EBADF)
            break;
        DPRINTF("[%d]flock(%d, %d), %s\n",
          retry, fd, operation, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

int
CR_File::lockf(int fd, int cmd, off_t len)
{
    int ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::lockf(fd, cmd, len);
        if (ret == 0)
            break;
        if (errno == EBADF)
            break;
        DPRINTF("[%d]lockf(%d, %d, %lld), %s\n",
          retry, fd, cmd, (long long)len, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

int
CR_File::truncate(const char *path, off_t length)
{
    int ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::truncate(path, length);
        if (ret == 0)
            break;
        DPRINTF("[%d]truncate(\"%s\", %lld), %s\n",
          retry, path, (long long)length, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

int
CR_File::ftruncate(int fd, off_t length)
{
    int ret = 0;
    int retry = CR_File::retry_count.value();

    do {
        ret = ::ftruncate(fd, length);
        if (ret == 0)
            break;
        if (errno == EBADF)
            break;
        DPRINTF("[%d]ftruncate(%d, %lld), %s\n",
          retry, fd, (long long)length, CR_Class_NS::strerrno(errno));
        retry--;
    } while (retry >= 0);

    return ret;
}

off_t
CR_File::fgetsize(int fd)
{
    off_t ret = -1;

    off_t cur_pos = CR_File::lseek(fd, 0, SEEK_CUR);
    if (cur_pos >= 0) {
        ret = CR_File::lseek(fd, 0, SEEK_END);
        CR_File::lseek(fd, cur_pos, SEEK_SET);
    }

    return ret;
}

int
CR_File::openlock(const char *pathname, int open_flags, mode_t open_mode, bool shared_lock)
{
    int lock_mode = LOCK_EX;
    int fd = CR_File::open(pathname, open_flags, open_mode);

    if (fd < 0)
        return -1;

    if (shared_lock)
        lock_mode = LOCK_SH;

    if (CR_File::flock(fd, lock_mode) < 0) {
        CR_File::close(fd);
        return -1;
    }

    return fd;
}
