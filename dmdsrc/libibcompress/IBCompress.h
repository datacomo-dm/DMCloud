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
    ѹ�������ݸ�ʽ:
    [null_buf_size:2 bytes]
    [null_buf:null_buf_size bytes]
    [tmp_cb_len:4 bytes]
    [maxv: 8 bytes]
    [tmp_comp_buffer:tmp_cb_len bytes]
    --------------->
    [null����: 2�ֽ�]
    [null ���ݰ�:null �����ֽ�]
    [ѹ�������ݰ�����<����������ֵ����>:4�ֽ�]
    [���ֵ:8�ֽ�]
    [ѹ��������ݰ�����:ѹ�������ݰ������ֽ�]
*/
// packn compress
// success: 0
// failed: other
extern "C" int ib_packn_compress(const uint * nulls,	/*: nulls �洢����      */
				 const int no_nulls,	/*: null ����           */
				 const void *data_full,	/*: ��������            */
				 const int no_obj,	/*: ���ݰ��ж�����      */
				 const _uint64 maxv,	/*: �����е����ֵ      */
				 std::string & compressed_str,	/*: ѹ����Ļ���        */
				 uchar & optimal_mode);	/*: ѹ��״̬ѡ��        */

extern "C" int ib_packn_compress_zlib(const uint * nulls,	/*: nulls �洢����      */
				      const int no_nulls,	/*: null ����           */
				      const void *data_full,	/*: ��������            */
				      const int no_obj,	/*: ���ݰ��ж�����      */
				      const _uint64 maxv,	/*: �����е����ֵ      */
				      std::string & compressed_str,	/*: ѹ����Ļ���        */
				      uchar & optimal_mode);	/*: ѹ��״̬ѡ��        */

extern "C" int ib_packn_compress_snappy(const uint * nulls,	/*: nulls �洢����      */
					const int no_nulls,	/*: null ����           */
					const void *data_full,	/*: ��������            */
					const int no_obj,	/*: ���ݰ��ж�����      */
					const _uint64 maxv,	/*: �����е����ֵ      */
					std::string & compressed_str,	/*: ѹ����Ļ���        */
					uchar & optimal_mode);	/*: ѹ��״̬ѡ��        */

extern "C" int ib_packn_compress_lz4(const uint * nulls,	/*: nulls �洢����      */
				     const int no_nulls,	/*: null ����           */
				     const void *data_full,	/*: ��������            */
				     const int no_obj,	/*: ���ݰ��ж�����      */
				     const _uint64 maxv,	/*: �����е����ֵ      */
				     std::string & compressed_str,	/*: ѹ����Ļ���        */
				     uchar & optimal_mode);	/*: ѹ��״̬ѡ��        */

extern "C" int ib_packn_compress_auto(const uint * nulls,
				      const int no_nulls,
				      const void *data_full,
				      const int no_obj,
				      const _uint64 maxv,
				      std::string & compressed_str,
				      uchar & optimal_mode,
				      const int compress_type);
/*
    ѹ�������ݸ�ʽ:
    [null_buf_size:2 bytes]
    [null_buf:null_buf_size bytes]
    [tmp_cb_len:4 bytes]
    [maxv: 8 bytes]
    [tmp_comp_buffer:tmp_cb_len bytes]
    --------------->
    [null����: 2�ֽ�]
    [null ���ݰ�:null �����ֽ�]
    [ѹ�������ݰ�����<����������ֵ����>:4�ֽ�]
    [���ֵ:8�ֽ�]
    [ѹ��������ݰ�����:ѹ�������ݰ������ֽ�]
*/
// packn decompress
// success: 0
// failed: other
extern "C" int ib_packn_decompress(const uchar * compressed_buf,	/*: ѹ����Ļ���        */
				   const uint compressed_buf_len,	/*: ѹ����Ļ��泤��    */
				   const uchar optimal_mode,	/*: ѹ��״̬ѡ��        */
				   uint * nulls,	/*: nulls �洢����      */
				   const int no_nulls,	/*: null ����           */
				   void *data_full,	/*: ��������            */
				   const int no_obj);	/*: ���ݰ��ж�����      */

extern "C" int ib_packn_decompress_zlib(const uchar * compressed_buf,	/*: ѹ����Ļ���        */
					const uint compressed_buf_len,	/*: ѹ����Ļ��泤��    */
					const uchar optimal_mode,	/*: ѹ��״̬ѡ��        */
					uint * nulls,	/*: nulls �洢����      */
					const int no_nulls,	/*: null ����           */
					void *data_full,	/*: ��������            */
					const int no_obj);	/*: ���ݰ��ж�����      */

extern "C" int ib_packn_decompress_snappy(const uchar * compressed_buf,	/*: ѹ����Ļ���        */
					  const uint compressed_buf_len,	/*: ѹ����Ļ��泤��    */
					  const uchar optimal_mode,	/*: ѹ��״̬ѡ��        */
					  uint * nulls,	/*: nulls �洢����      */
					  const int no_nulls,	/*: null ����           */
					  void *data_full,	/*: ��������            */
					  const int no_obj);	/*: ���ݰ��ж�����      */

extern "C" int ib_packn_decompress_lz4(const uchar * compressed_buf,	/*: ѹ����Ļ���        */
				       const uint compressed_buf_len,	/*: ѹ����Ļ��泤��    */
				       const uchar optimal_mode,	/*: ѹ��״̬ѡ��        */
				       uint * nulls,	/*: nulls �洢����      */
				       const int no_nulls,	/*: null ����           */
				       void *data_full,	/*: ��������            */
				       const int no_obj);	/*: ���ݰ��ж�����      */

extern "C" int ib_packn_decompress_auto(const uchar * compressed_buf,
					const uint compressed_buf_len,
					const uchar optimal_mode,
					uint * nulls,
					const int no_nulls,
					void *data_full, const int no_obj);

/*
    ѹ�������ݸ�ʽ:
    [null_buf_size:2 bytes]
    [null_buf:null_buf_size bytes]
    [comp_len_buf_size:4 bytes]
    [maxv: 8 bytes]
    [comp_len_buf:comp_len_buf_size bytes]
    [dlen:4 bytes]
    [comp_buf: dlen bytes]
    ------------->
    [ѹ����nulls����:2 bytes]
    [ѹ����nulls����:ѹ����nulls���� bytes]
    [����ѹ����ĳ���<���������󳤶�>:4 bytes]
    [��󳤶�: 8 bytes]
    [����ѹ����Ļ���:����ѹ����ĳ���<���������󳤶�> bytes]
    [����ѹ���󳤶�:4 bytes]
    [����ѹ���󻺴�: ����ѹ���󳤶� bytes]
*/
// packs compress
// success: 0
// failed: other
extern "C" int ib_packs_compress(const uint * nulls,	/*: nulls �洢����      */
				 const int no_nulls,	/*: null ����           */
				 const void *data,	/*: ��������            */
				 const uint data_full_byte_size,	/*: �������ݵĳ���      */
				 const int no_obj,	/*: ���ݰ��ж�����      */
				 const void *lens,	/*: ���ݳ�������        */
				 const short len_mode,	/*: ����ģʽ            */
				 std::string & compressed_str,	/*: ѹ����Ļ���        */
				 uchar & optimal_mode);	/*: ѹ��״̬ѡ��        */

extern "C" int ib_packs_compress_zlib(const uint * nulls,	/*: nulls �洢����      */
				      const int no_nulls,	/*: null ����           */
				      const void *data,	/*: ��������            */
				      const uint data_full_byte_size,	/*: �������ݵĳ���      */
				      const int no_obj,	/*: ���ݰ��ж�����      */
				      const void *lens,	/*: ���ݳ�������        */
				      const short len_mode,	/*: ����ģʽ            */
				      std::string & compressed_str,	/*: ѹ����Ļ���        */
				      uchar & optimal_mode);	/*: ѹ��״̬ѡ��        */

extern "C" int ib_packs_compress_snappy(const uint * nulls,	/*: nulls �洢����      */
					const int no_nulls,	/*: null ����           */
					const void *data,	/*: ��������            */
					const uint data_full_byte_size,	/*: �������ݵĳ���      */
					const int no_obj,	/*: ���ݰ��ж�����      */
					const void *lens,	/*: ���ݳ�������        */
					const short len_mode,	/*: ����ģʽ            */
					std::string & compressed_str,	/*: ѹ����Ļ���        */
					uchar & optimal_mode);	/*: ѹ��״̬ѡ��        */

extern "C" int ib_packs_compress_lz4(const uint * nulls,	/*: nulls �洢����      */
				     const int no_nulls,	/*: null ����           */
				     const void *data,	/*: ��������            */
				     const uint data_full_byte_size,	/*: �������ݵĳ���      */
				     const int no_obj,	/*: ���ݰ��ж�����      */
				     const void *lens,	/*: ���ݳ�������        */
				     const short len_mode,	/*: ����ģʽ            */
				     std::string & compressed_str,	/*: ѹ����Ļ���        */
				     uchar & optimal_mode);	/*: ѹ��״̬ѡ��        */

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
    ѹ�������ݸ�ʽ:
    [null_buf_size:2 bytes]
    [null_buf:null_buf_size bytes]
    [comp_len_buf_size:4 bytes]
    [maxv: 8 bytes]
    [comp_len_buf:comp_len_buf_size bytes]
    [dlen:4 bytes]
    [comp_buf: dlen bytes]
    ------------->
    [ѹ����nulls����:2 bytes]
    [ѹ����nulls����:ѹ����nulls���� bytes]
    [����ѹ����ĳ���<���������󳤶�>:4 bytes]
    [��󳤶�: 8 bytes]
    [����ѹ����Ļ���:����ѹ����ĳ���<���������󳤶�> bytes]
    [����ѹ���󳤶�:4 bytes]
    [����ѹ���󻺴�: ����ѹ���󳤶� bytes]
*/
// packs decompress
// success: 0
// failed: other
extern "C" int ib_packs_decompress(const uchar * compressed_buf,	/*: ѹ����Ļ���        */
				   const int compressed_buf_len,	/*: ѹ����Ļ��泤��    */
				   const int no_obj,	/*: ���ݰ��ж�����      */
				   const uchar optimal_mode,	/*: ѹ��״̬ѡ��        */
				   void *data,	/*: �������ݻ���        */
				   const uint data_len,	/*: ���������ݻ��������� */
				   const short len_mode,	/*: ����ģʽ            */
				   void *lens,	/*: ���ݳ���            */
				   uint * nulls,	/*: nulls �洢����      */
				   const int no_nulls);	/*: null ����           */

extern "C" int ib_packs_decompress_zlib(const uchar * compressed_buf,	/*: ѹ����Ļ���        */
					const int compressed_buf_len,	/*: ѹ����Ļ��泤��    */
					const int no_obj,	/*: ���ݰ��ж�����      */
					const uchar optimal_mode,	/*: ѹ��״̬ѡ��        */
					void *data,	/*: �������ݻ���        */
					const uint data_len,	/*: ���������ݻ��������� */
					const short len_mode,	/*: ����ģʽ            */
					void *lens,	/*: ���ݳ���            */
					uint * nulls,	/*: nulls �洢����      */
					const int no_nulls);	/*: null ����           */

extern "C" int ib_packs_decompress_snappy(const uchar * compressed_buf,	/*: ѹ����Ļ���        */
					  const int compressed_buf_len,	/*: ѹ����Ļ��泤��    */
					  const int no_obj,	/*: ���ݰ��ж�����      */
					  const uchar optimal_mode,	/*: ѹ��״̬ѡ��        */
					  void *data,	/*: �������ݻ���        */
					  const uint data_len,	/*: ���������ݻ��������� */
					  const short len_mode,	/*: ����ģʽ            */
					  void *lens,	/*: ���ݳ���            */
					  uint * nulls,	/*: nulls �洢����      */
					  const int no_nulls);	/*: null ����           */

extern "C" int ib_packs_decompress_lz4(const uchar * compressed_buf,	/*: ѹ����Ļ���        */
				       const int compressed_buf_len,	/*: ѹ����Ļ��泤��    */
				       const int no_obj,	/*: ���ݰ��ж�����      */
				       const uchar optimal_mode,	/*: ѹ��״̬ѡ��        */
				       void *data,	/*: �������ݻ���        */
				       const uint data_len,	/*: ���������ݻ��������� */
				       const short len_mode,	/*: ����ģʽ            */
				       void *lens,	/*: ���ݳ���            */
				       uint * nulls,	/*: nulls �洢����      */
				       const int no_nulls);	/*: null ����           */

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

// ���ú��ж�nullֵ�Ļ���
namespace ib_cprs_assist {
	// �ж�ĳһ��Ԫ���Ƿ�Ϊ�գ���Ӧ:AttrPack::IsNull
	bool IsNull(const uint * nulls, const int no_nulls, const int no_obj,
		    const int i);
	// ����packn��nullֵ����ӦAttrPackN::SetNull(int i),nulls��СΪ8k�ֽ�
	void SetPacknNull(uint * nulls, int &no_nulls, const int i);
	// ����packs��nullֵ����ӦAttrPackS::SetNull(int i),nulls��СΪ8k�ֽ�
	void SetPacksNull(uint * nulls, int &no_nulls, const int i);
}

namespace ib_packs_compressmode_assist { // packs ѹ����������
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

namespace ib_packn_compressmode_assist // packn ѹ����������
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
