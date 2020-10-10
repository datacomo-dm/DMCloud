#ifndef __H_CR_FILE_H__
#define __H_CR_FILE_H__

#include <cr_class/cr_locked.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>

namespace CR_File {
	extern CR_Locked<int> retry_count;

	int mkdir(const char *pathname, mode_t mode);
	int open(const char *pathname, int flags, mode_t mode=0600);
	int close(int fd);
	int fsync(int fd);
	off_t lseek(int fd, off_t offset, int whence);
	ssize_t read(int fd, void *buf, size_t count);
	ssize_t write(int fd, const void *buf, size_t count);
	int unlink(const char *pathname);
	int rmdir(const char *pathname);
	int rename(const char *oldpath, const char *newpath);
	int link(const char *oldpath, const char *newpath);
	int symlink(const char *oldpath, const char *newpath);
	int chown(const char *path, uid_t owner, gid_t group);
	int fchown(int fd, uid_t owner, gid_t group);
	int lchown(const char *path, uid_t owner, gid_t group);
	int chmod(const char *path, mode_t mode);
	int fchmod(int fd, mode_t mode);
	int flock(int fd, int operation);
	int lockf(int fd, int cmd, off_t len);
	int truncate(const char *path, off_t length);
	int ftruncate(int fd, off_t length);

	off_t fgetsize(int fd);
	int openlock(const char *pathname, int open_flags, mode_t open_mode=0600, bool shared_lock=false);
};

#endif /* __H_CR_FILE_H__ */
