#ifndef _IBFile_H_
#define _IBFile_H_

#ifdef __GNUC__
#include <sys/file.h>
#else
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <io.h>
#include <stdlib.h>
#endif

#include <assert.h>
#include <string>
#include <limits.h>
#include <iostream>
using namespace std;

#define ib_read         read
#define ib_open         open
#define ib_write        write
#define ib_close        close
#define ib_seek         lseek
#define ib_tell(fd)     lseek(fd, 0, SEEK_CUR);
typedef mode_t          ib_pmode;
#define O_BINARY        0
#define O_SEQUENTIAL    0
#define O_RANDOM        0

typedef off_t           ib_off_t;

#define ib_umask        S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP

#ifndef UINT64_MAX
#define UINT64_MAX ULONG_LONG_MAX
#endif

#define IBFILE_OPEN_NO_TIMEOUT UINT64_MAX
#define IBFILE_OPEN_SLEEP_STEP 100

#ifndef _uint64_
typedef long long int _uint64;
typedef long long int uint64;
#define _uint64_
#endif

/* A portable wrapper class for low level file I/O.
  Note - Most of the functions throws exception if it fails. So caller must 
handle
  exception appropriately.
*/

namespace ib_rsi{

    class IBFile{
    	int          fd;
    public:

    	IBFile() { fd = -1; };
	IBFile(int _fd) { this->fd = _fd; }
    	virtual ~IBFile() { if (fd != -1) Close(); }
    	int	GetFileId() { return fd; }

    	int Open(std::string const& file, int flags, ib_pmode mode, uint64 timeout = IBFILE_OPEN_NO_TIMEOUT); //timeout in miniseconds
    	uint Read(void* buf, uint count);
    	uint Write(const void* buf, uint count , bool auto_exception = false);
    	ib_off_t Seek(ib_off_t pos, int whence);
    	ib_off_t Tell();
       
    	// O_CREAT|O_RDWR|O_LARGEFILE|O_BINARY
    	int OpenCreate(std::string const& file);
    	// O_CREAT|O_WRONLY|O_TRUNC|O_LARGEFILE|O_BINARY
    	int OpenCreateEmpty(std::string const& file, int create = 0, uint64 timeout = 0) ;
    	// O_CREAT|O_RDONLY|O_LARGEFILE|O_BINARY
    	int OpenCreateReadOnly(std::string const& file);
    	// O_RDONLY|O_LARGEFILE|O_BINARY
    	int OpenReadOnly(std::string const& file, int create = 0, uint64 timeout = 0);
    	// O_RDWR|O_LARGEFILE|O_BINARY
    	int OpenReadWrite(std::string const& file, int create = 0, uint64 timeout = 0);
    	// O_WRONLY|O_LARGEFILE|O_APPEND|O_BINARY
    	int OpenWriteAppend(std::string const& file, int create = 0, uint64 timeout = 0 );

    	// Variant for Read and Write calls
    	// Throws exception if can't read exact number of bytes
    	uint ReadExact(void* buf, uint count);
    	// Throws exception if can't read exact number of bytes
    	uint WriteExact(const void* buf, uint count);

    	bool IsOpen() const;
    	int  Close();

    };
}

#endif //_IBFile_H_

