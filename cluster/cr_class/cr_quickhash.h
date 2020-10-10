#ifndef __H_CR_QUICKHASH_H__
#define __H_CR_QUICKHASH_H__

#include <cr_class/cr_addon.h>
#include <unistd.h>
#include <string>

namespace CR_QuickHash {
	typedef uint32_t (*hash_func_t)(const void *p, size_t size, uint32_t hash);

	uint32_t CRC32Hash(const void *p, size_t size, uint32_t hash=0);
	uint32_t SDBMHash(const void *p, size_t size, uint32_t hash=0);
	uint32_t RSHash(const void *p, size_t size, uint32_t hash=0);
	uint32_t PJWHash(const void *p, size_t size, uint32_t hash=0);
	uint32_t ELFHash(const void *p, size_t size, uint32_t hash=0);
	uint32_t BKDRHash(const void *p, size_t size, uint32_t hash=0);
	uint32_t APHash(const void *p, size_t size, uint32_t hash=0);

	uint64_t CRC64Hash(const void *p, size_t size, uint64_t hash=0);

	std::string md5raw(const void *buf, const size_t length);
	std::string md5raw(const std::string &str);
	std::string md5(const void *buf, const size_t length);
	std::string md5(const std::string &str);

	std::string sha1raw(const void *buf, const size_t length);
	std::string sha1raw(const std::string &str);
	std::string sha1(const void *buf, const size_t length);
	std::string sha1(const std::string &str);

	std::string sha256raw(const void *buf, const size_t length);
	std::string sha256raw(const std::string &str);
	std::string sha256(const void *buf, const size_t length);
	std::string sha256(const std::string &str);

	std::string sha384raw(const void *buf, const size_t length);
	std::string sha384raw(const std::string &str);
	std::string sha384(const void *buf, const size_t length);
	std::string sha384(const std::string &str);

	std::string sha512raw(const void *buf, const size_t length);
	std::string sha512raw(const std::string &str);
	std::string sha512(const void *buf, const size_t length);
	std::string sha512(const std::string &str);
};

#endif /* __H_CR_QUICKHASH_H__ */
