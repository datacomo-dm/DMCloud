#include <unistd.h>
#include "IBFile.h"

#if defined( __FreeBSD__ )
static int const O_LARGEFILE = 0;
#endif

using namespace std;
using namespace ib_rsi;
//#define O_NONBLOCK 1

int IBFile::Open(std::string const& file, int flags, ib_pmode mode, uint64 timeout)
{
	assert(file.length());
	int retry_times = 0;
	while(retry_times<10){
		fd = ib_open(file.c_str(), flags, mode);
		if(-1 == fd){
			sleep(1);
			retry_times++;
			std::cerr <<  "--->open file: " << file << " return -1,retry_times: " << retry_times << std::endl;
		}else{
			break;
		}
	}
	return fd;
}

uint IBFile::Read(void* buf, uint count){
	uint read_bytes;
	assert(fd != -1);
	read_bytes = (uint)ib_read(fd, buf, count);
	return read_bytes;
}

uint IBFile::Write(const void* buf, uint count,bool auto_exception){
	uint writen_bytes;
	assert(fd != -1);
	writen_bytes = ib_write(fd, buf, count);
	return writen_bytes;
}

int IBFile::OpenCreate(std::string const& file){
	return Open(file, O_CREAT|O_RDWR|O_LARGEFILE|O_BINARY, ib_umask);
};

int IBFile::OpenCreateEmpty(std::string const& file, int create, uint64 timeout){
	return Open(file, O_CREAT|O_RDWR|O_TRUNC|O_LARGEFILE|O_BINARY, ib_umask);
};

int IBFile::OpenCreateReadOnly(std::string const& file){
	return Open(file, O_CREAT|O_RDONLY|O_LARGEFILE|O_BINARY, ib_umask);
};

int IBFile::OpenReadOnly(std::string const& file, int create, uint64 timeout){
	return Open(file, O_RDONLY|O_LARGEFILE|O_BINARY, ib_umask);
};

int IBFile::OpenReadWrite(std::string const& file, int create, uint64 timeout){
	return Open(file, O_RDWR|O_LARGEFILE|O_BINARY, ib_umask);
};

int IBFile::OpenWriteAppend(std::string const& file, int create, uint64 timeout){
	return Open(file, O_WRONLY|O_LARGEFILE|O_APPEND|O_BINARY, ib_umask, timeout);
};


uint IBFile::ReadExact(void* buf, uint count){
	uint read_bytes=0,rb=0;
	while( read_bytes < count ) {
		rb = Read((char *)buf+read_bytes, count-read_bytes);
		if(rb == 0) break;
		read_bytes += rb;
	}
	return read_bytes;
}

uint IBFile::WriteExact(const void* buf, uint count){
	uint writen_bytes;
	writen_bytes = Write(buf, count);
	return writen_bytes;
}

ib_off_t IBFile::Seek(ib_off_t pos, int whence){
	ib_off_t new_pos = -1;
	assert(fd != -1);

	new_pos = ib_seek(fd, pos, whence);
	return new_pos;
}

ib_off_t IBFile::Tell(){
	ib_off_t pos;
	assert(fd != -1);
	pos = ib_tell(fd);
	return pos;
}

bool IBFile::IsOpen() const{
	return (fd != -1 ? true: false);
}

int IBFile::Close(){
	int bh_err = 0;
	if (fd != -1) {
		bh_err = ib_close(fd);
		fd = -1;
	}
	return bh_err;
}

