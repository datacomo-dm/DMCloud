#ifndef IBCOMPRESS_DEF_H
#define IBCOMPRESS_DEF_H

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <string>

#ifndef ibc_malloc
#define ibc_malloc malloc
#endif

#ifndef ibc_free
#define ibc_free free
#endif

#ifndef uint_
typedef unsigned int uint;
#define uint_
#endif

#ifndef uchar_
typedef unsigned char uchar;
#define uchar_
#endif

#ifndef _int64_
#ifdef __GNUC__
typedef long long int _int64;
#endif
#define _int64_
#endif

#ifndef _uint64
#ifdef __GNUC__
typedef unsigned long long _uint64;
#else
typedef unsigned __int64 _uint64;
#endif
#endif

#ifndef _ValueType_def
#define _ValueType_def
enum ValueType { UCHAR = 1, USHORT = 2, UINT = 4, UINT64 = 8 };
#endif

/*
    压缩后数据格式:
    [null_buf_size:2 bytes]
    [null_buf:null_buf_size bytes]
    [tmp_cb_len:4 bytes]
    [maxv: 8 bytes]
    [tmp_comp_buffer:tmp_cb_len bytes]
    --------------->
    [null长度: 2字节]
    [null 数据包:null 长度字节]
    [压缩后数据包长度<含自身和最大值长度>:4字节]
    [最大值:8字节]
    [压缩后的数据包内容:压缩后数据包长度字节]
*/
// packn compress
// success: 0
// failed: other
extern "C" int ib_packn_compress(const uint * nulls,	/*: nulls 存储缓存      */
				 const int no_nulls,	/*: null 个数           */
				 const void *data_full,	/*: 完整数据            */
				 const int no_obj,	/*: 数据包中对象数      */
				 const _uint64 maxv,	/*: 数据中的最大值      */
				 std::string & compressed_str,	/*: 压缩后的缓存        */
				 uchar & optimal_mode);	/*: 压缩状态选项        */

extern "C" int ib_packn_compress_zlib(const uint * nulls,	/*: nulls 存储缓存      */
				      const int no_nulls,	/*: null 个数           */
				      const void *data_full,	/*: 完整数据            */
				      const int no_obj,	/*: 数据包中对象数      */
				      const _uint64 maxv,	/*: 数据中的最大值      */
				      std::string & compressed_str,	/*: 压缩后的缓存        */
				      uchar & optimal_mode);	/*: 压缩状态选项        */

extern "C" int ib_packn_compress_snappy(const uint * nulls,	/*: nulls 存储缓存      */
					const int no_nulls,	/*: null 个数           */
					const void *data_full,	/*: 完整数据            */
					const int no_obj,	/*: 数据包中对象数      */
					const _uint64 maxv,	/*: 数据中的最大值      */
					std::string & compressed_str,	/*: 压缩后的缓存        */
					uchar & optimal_mode);	/*: 压缩状态选项        */

extern "C" int ib_packn_compress_lz4(const uint * nulls,	/*: nulls 存储缓存      */
				     const int no_nulls,	/*: null 个数           */
				     const void *data_full,	/*: 完整数据            */
				     const int no_obj,	/*: 数据包中对象数      */
				     const _uint64 maxv,	/*: 数据中的最大值      */
				     std::string & compressed_str,	/*: 压缩后的缓存        */
				     uchar & optimal_mode);	/*: 压缩状态选项        */

extern "C" int ib_packn_compress_auto(const uint * nulls,
				      const int no_nulls,
				      const void *data_full,
				      const int no_obj,
				      const _uint64 maxv,
				      std::string & compressed_str,
				      uchar & optimal_mode,
				      const int compress_type);
/*
    压缩后数据格式:
    [null_buf_size:2 bytes]
    [null_buf:null_buf_size bytes]
    [tmp_cb_len:4 bytes]
    [maxv: 8 bytes]
    [tmp_comp_buffer:tmp_cb_len bytes]
    --------------->
    [null长度: 2字节]
    [null 数据包:null 长度字节]
    [压缩后数据包长度<含自身和最大值长度>:4字节]
    [最大值:8字节]
    [压缩后的数据包内容:压缩后数据包长度字节]
*/
// packn decompress
// success: 0
// failed: other
extern "C" int ib_packn_decompress(const uchar * compressed_buf,	/*: 压缩后的缓存        */
				   const uint compressed_buf_len,	/*: 压缩后的缓存长度    */
				   const uchar optimal_mode,	/*: 压缩状态选项        */
				   uint * nulls,	/*: nulls 存储缓存      */
				   const int no_nulls,	/*: null 个数           */
				   void *data_full,	/*: 完整数据            */
				   const int no_obj);	/*: 数据包中对象数      */

extern "C" int ib_packn_decompress_zlib(const uchar * compressed_buf,	/*: 压缩后的缓存        */
					const uint compressed_buf_len,	/*: 压缩后的缓存长度    */
					const uchar optimal_mode,	/*: 压缩状态选项        */
					uint * nulls,	/*: nulls 存储缓存      */
					const int no_nulls,	/*: null 个数           */
					void *data_full,	/*: 完整数据            */
					const int no_obj);	/*: 数据包中对象数      */

extern "C" int ib_packn_decompress_snappy(const uchar * compressed_buf,	/*: 压缩后的缓存        */
					  const uint compressed_buf_len,	/*: 压缩后的缓存长度    */
					  const uchar optimal_mode,	/*: 压缩状态选项        */
					  uint * nulls,	/*: nulls 存储缓存      */
					  const int no_nulls,	/*: null 个数           */
					  void *data_full,	/*: 完整数据            */
					  const int no_obj);	/*: 数据包中对象数      */

extern "C" int ib_packn_decompress_lz4(const uchar * compressed_buf,	/*: 压缩后的缓存        */
				       const uint compressed_buf_len,	/*: 压缩后的缓存长度    */
				       const uchar optimal_mode,	/*: 压缩状态选项        */
				       uint * nulls,	/*: nulls 存储缓存      */
				       const int no_nulls,	/*: null 个数           */
				       void *data_full,	/*: 完整数据            */
				       const int no_obj);	/*: 数据包中对象数      */

extern "C" int ib_packn_decompress_auto(const uchar * compressed_buf,
					const uint compressed_buf_len,
					const uchar optimal_mode,
					uint * nulls,
					const int no_nulls,
					void *data_full, const int no_obj);

/*
    压缩后数据格式:
    [null_buf_size:2 bytes]
    [null_buf:null_buf_size bytes]
    [comp_len_buf_size:4 bytes]
    [maxv: 8 bytes]
    [comp_len_buf:comp_len_buf_size bytes]
    [dlen:4 bytes]
    [comp_buf: dlen bytes]
    ------------->
    [压缩后nulls长度:2 bytes]
    [压缩后nulls缓存:压缩后nulls长度 bytes]
    [长度压缩后的长度<含自身和最大长度>:4 bytes]
    [最大长度: 8 bytes]
    [长度压缩后的缓存:长度压缩后的长度<含自身和最大长度> bytes]
    [数据压缩后长度:4 bytes]
    [数据压缩后缓存: 数据压缩后长度 bytes]
*/
// packs compress
// success: 0
// failed: other
extern "C" int ib_packs_compress(const uint * nulls,	/*: nulls 存储缓存      */
				 const int no_nulls,	/*: null 个数           */
				 const void *data,	/*: 完整数据            */
				 const uint data_full_byte_size,	/*: 完整数据的长度      */
				 const int no_obj,	/*: 数据包中对象数      */
				 const void *lens,	/*: 数据长度数组        */
				 const short len_mode,	/*: 长度模式            */
				 std::string & compressed_str,	/*: 压缩后的缓存        */
				 uchar & optimal_mode);	/*: 压缩状态选项        */

extern "C" int ib_packs_compress_zlib(const uint * nulls,	/*: nulls 存储缓存      */
				      const int no_nulls,	/*: null 个数           */
				      const void *data,	/*: 完整数据            */
				      const uint data_full_byte_size,	/*: 完整数据的长度      */
				      const int no_obj,	/*: 数据包中对象数      */
				      const void *lens,	/*: 数据长度数组        */
				      const short len_mode,	/*: 长度模式            */
				      std::string & compressed_str,	/*: 压缩后的缓存        */
				      uchar & optimal_mode);	/*: 压缩状态选项        */

extern "C" int ib_packs_compress_snappy(const uint * nulls,	/*: nulls 存储缓存      */
					const int no_nulls,	/*: null 个数           */
					const void *data,	/*: 完整数据            */
					const uint data_full_byte_size,	/*: 完整数据的长度      */
					const int no_obj,	/*: 数据包中对象数      */
					const void *lens,	/*: 数据长度数组        */
					const short len_mode,	/*: 长度模式            */
					std::string & compressed_str,	/*: 压缩后的缓存        */
					uchar & optimal_mode);	/*: 压缩状态选项        */

extern "C" int ib_packs_compress_lz4(const uint * nulls,	/*: nulls 存储缓存      */
				     const int no_nulls,	/*: null 个数           */
				     const void *data,	/*: 完整数据            */
				     const uint data_full_byte_size,	/*: 完整数据的长度      */
				     const int no_obj,	/*: 数据包中对象数      */
				     const void *lens,	/*: 数据长度数组        */
				     const short len_mode,	/*: 长度模式            */
				     std::string & compressed_str,	/*: 压缩后的缓存        */
				     uchar & optimal_mode);	/*: 压缩状态选项        */

extern "C" int ib_packs_compress_auto(const uint * nulls,
				      const int no_nulls,
				      const void *data,
				      const uint data_full_byte_size,
				      const int no_obj,
				      const void *lens,
				      const short len_mode,
				      std::string & compressed_str,
				      uchar & optimal_mode,
				      const int compress_type);

/*
    压缩后数据格式:
    [null_buf_size:2 bytes]
    [null_buf:null_buf_size bytes]
    [comp_len_buf_size:4 bytes]
    [maxv: 8 bytes]
    [comp_len_buf:comp_len_buf_size bytes]
    [dlen:4 bytes]
    [comp_buf: dlen bytes]
    ------------->
    [压缩后nulls长度:2 bytes]
    [压缩后nulls缓存:压缩后nulls长度 bytes]
    [长度压缩后的长度<含自身和最大长度>:4 bytes]
    [最大长度: 8 bytes]
    [长度压缩后的缓存:长度压缩后的长度<含自身和最大长度> bytes]
    [数据压缩后长度:4 bytes]
    [数据压缩后缓存: 数据压缩后长度 bytes]
*/
// packs decompress
// success: 0
// failed: other
extern "C" int ib_packs_decompress(const uchar * compressed_buf,	/*: 压缩后的缓存        */
				   const int compressed_buf_len,	/*: 压缩后的缓存长度    */
				   const int no_obj,	/*: 数据包中对象数      */
				   const uchar optimal_mode,	/*: 压缩状态选项        */
				   void *data,	/*: 完整数据缓冲        */
				   const uint data_len,	/*: 完整的数据缓冲区长度 */
				   const short len_mode,	/*: 长度模式            */
				   void *lens,	/*: 数据长度            */
				   uint * nulls,	/*: nulls 存储缓存      */
				   const int no_nulls);	/*: null 个数           */

extern "C" int ib_packs_decompress_zlib(const uchar * compressed_buf,	/*: 压缩后的缓存        */
					const int compressed_buf_len,	/*: 压缩后的缓存长度    */
					const int no_obj,	/*: 数据包中对象数      */
					const uchar optimal_mode,	/*: 压缩状态选项        */
					void *data,	/*: 完整数据缓冲        */
					const uint data_len,	/*: 完整的数据缓冲区长度 */
					const short len_mode,	/*: 长度模式            */
					void *lens,	/*: 数据长度            */
					uint * nulls,	/*: nulls 存储缓存      */
					const int no_nulls);	/*: null 个数           */

extern "C" int ib_packs_decompress_snappy(const uchar * compressed_buf,	/*: 压缩后的缓存        */
					  const int compressed_buf_len,	/*: 压缩后的缓存长度    */
					  const int no_obj,	/*: 数据包中对象数      */
					  const uchar optimal_mode,	/*: 压缩状态选项        */
					  void *data,	/*: 完整数据缓冲        */
					  const uint data_len,	/*: 完整的数据缓冲区长度 */
					  const short len_mode,	/*: 长度模式            */
					  void *lens,	/*: 数据长度            */
					  uint * nulls,	/*: nulls 存储缓存      */
					  const int no_nulls);	/*: null 个数           */

extern "C" int ib_packs_decompress_lz4(const uchar * compressed_buf,	/*: 压缩后的缓存        */
				       const int compressed_buf_len,	/*: 压缩后的缓存长度    */
				       const int no_obj,	/*: 数据包中对象数      */
				       const uchar optimal_mode,	/*: 压缩状态选项        */
				       void *data,	/*: 完整数据缓冲        */
				       const uint data_len,	/*: 完整的数据缓冲区长度 */
				       const short len_mode,	/*: 长度模式            */
				       void *lens,	/*: 数据长度            */
				       uint * nulls,	/*: nulls 存储缓存      */
				       const int no_nulls);	/*: null 个数           */

extern "C" int ib_packs_decompress_auto(const uchar * compressed_buf,
					const int compressed_buf_len,
					const int no_obj,
					const uchar optimal_mode,
					void *data,
					const uint data_len,
					const short len_mode,
					void *lens,
					uint * nulls, const int no_nulls);

// get retcode message
extern "C" void ib_cprs_message(const int retcode, char *retmsg, int msglen);

// 设置和判断null值的换成
namespace ib_cprs_assist {
	// 判断某一个元素是否为空，对应:AttrPack::IsNull
	bool IsNull(const uint * nulls, const int no_nulls, const int no_obj,
		    const int i);
	// 设置packn的null值，对应AttrPackN::SetNull(int i),nulls大小为8k字节
	void SetPacknNull(uint * nulls, int &no_nulls, const int i);
	// 设置packs的null值，对应AttrPackS::SetNull(int i),nulls大小为8k字节
	void SetPacksNull(uint * nulls, int &no_nulls, const int i);
}

namespace ib_packs_compressmode_assist { // packs 压缩类型设置
	bool IsModeNullsCompressed(const uchar optimal_mode);
	bool IsModeDataCompressed(const uchar optimal_mode);
	bool IsModeCompressionApplied(const uchar optimal_mode);
	bool IsModeNoCompression(const uchar optimal_mode);
	bool IsModeValid(const uchar optimal_mode);

	void ResetModeNoCompression(uchar & optimal_mode);
	void SetModeDataCompressed(uchar & optimal_mode);
	void SetModeNullsCompressed(uchar & optimal_mode);
	void ResetModeNullsCompressed(uchar & optimal_mode);
	void ResetModeDataCompressed(uchar & optimal_mode);
}

namespace ib_packn_compressmode_assist // packn 压缩类型设置
{
	bool IsModeNullsCompressed(const uchar optimal_mode);
	bool IsModeDataCompressed(const uchar optimal_mode);
	bool IsModeCompressionApplied(const uchar optimal_mode);
	bool IsModeNoCompression(const uchar optimal_mode);
	bool IsModeValid(const uchar optimal_mode);

	void ResetModeNoCompression(uchar & optimal_mode);
	void SetModeDataCompressed(uchar & optimal_mode);
	void SetModeNullsCompressed(uchar & optimal_mode);
	void ResetModeNullsCompressed(uchar & optimal_mode);
	void ResetModeDataCompressed(uchar & optimal_mode);
}

namespace ib_pack_assist {
	extern const int bin_sums[256];

	bool ShouldNotCompress(const int no_obj);
	int CalculateBinSum(unsigned int n);

	int ZlibCode2IBCode(const int zlib_code);
	int SnappyCode2IBCode(const int snappy_code);
	int CompressCode2IBCode(const int compress_code);
}

namespace ib_packn_assist {
	ValueType GetValueType(const _uint64 maxv);
	_int64 GetVal64(const void *data_full, const ValueType value_type,
			const int n);
	void SetVal64(void *data_full, const ValueType value_type,
		      const uint no_obj, const uint n,
		      const _uint64 & val_code);
}

namespace ib_packs_assist {
	int GetSize(const void *lens, const short len_mode, int ono);
	void SetSize(void *lens, const short len_mode, const int ono,
		     uint size);
}
#endif
