#ifndef __H_CR_COMPRESS_H__
#define __H_CR_COMPRESS_H__

#include <stdint.h>
#include <string>

namespace CR_Compress {
	// Data format :
	// +---------------------------------------------------------------+
	// | CompressType | flags  | OriginSize | CompressedData |   CRC   |
	// +---------------------------------------------------------------+
	// |    1 byte    | 1 byte |  6 bytes   |    nn bytes    | 8 bytes |
	// +---------------------------------------------------------------+
	//  OriginSize = htobe64(OriginData.size())
	//  CompressType : same as enum CompressType
	//  CRC = htobe64(crc64(&CompressType, 1 + 7 + nn)

	typedef enum {
		CT_NONE		= 'N',
		CT_NONE_VER_0	= 0,
		CT_LZ4		= '4',
		CT_LZ4_VER_0	= 1,
		CT_SNAPPY	= 'S',
		CT_SNAPPY_VER_0	= 2,
		CT_ZLIB		= 'Z',
		CT_ZLIB_VER_0	= 3,
		CT_LZMA		= 'L',
		CT_LZMA_VER_0	= 5,
		CT_AUTO		= 127
	} CompressType;

	uint64_t compress_bound(const uint64_t src_size, const int compress_type=CT_AUTO);
	uint64_t decompress_size(const void *src_p);

	void *compress(const void *src_p, const uint64_t src_size, void *dst_p, uint64_t *dst_size_p,
		const int compress_type=CT_AUTO, const int preset=1);
	int64_t compress(const void *src_p, const uint64_t src_size, void *dst_p, uint64_t dst_max_size,
		const int compress_type=CT_AUTO, const int preset=1);
	int compress(const void *src_p, const uint64_t src_size, std::string &dst,
		const int compress_type=CT_AUTO, const int preset=1);
	int compress(const std::string &src, std::string &dst,
		const int compress_type=CT_AUTO, const int preset=1);
	std::string compress(const void *src_p, const uint64_t src_size,
		const int compress_type=CT_AUTO, const int preset=1);
	std::string compress(const std::string &src, const int compress_type=CT_AUTO, const int preset=1);

	void *decompress(const void *src_p, const uint64_t src_size, void *dst_p, uint64_t *dst_size_p);
	int64_t decompress(const void *src_p, const uint64_t src_size, void *dst_p, uint64_t dst_max_size);
	int decompress(const void *src_p, const uint64_t src_size, std::string &dst);
	int decompress(const std::string &src, std::string &dst);
	std::string decompress(const void *src_p, const uint64_t src_size);
	std::string decompress(const std::string &src);
} // namespace CR_Compress

#endif /* __H_CR_COMPRESS_H__ */
