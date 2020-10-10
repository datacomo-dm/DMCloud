#include "IBCompress.h"
#include <assert.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_memguard.h>

#include "common/CommonDefinitions.h"	// uint ,uchar, ...
#include "common/bhassert.h"	// BHASSERT
#include "system/fet.h"		// MEASURE_FET

#include "NumCompressor.h"
#include "BitstreamCompressor.h"
#include "TextCompressor.h"

namespace ib_packn_assist {
	template < typename etype >
	    int RemoveNullsAndCompress(NumCompressor < etype > &nc,
				       uchar * tmp_comp_buffer,
				       uint & tmp_cb_len,
				       _uint64 maxv,
				       int no_nulls,
				       uint * nulls, int no_obj,
				       void *data_full) {
		CR_MemGuard < etype > tmp_data;
		if (no_nulls > 0) {
			tmp_data = CR_MemGuard < etype > (no_obj - no_nulls);
			for (uint i = 0, d = 0; i < no_obj; i++) {
				if (!ib_cprs_assist::
				    IsNull(nulls, no_nulls, no_obj, i))
					tmp_data[d++] =
					    ((etype *) (data_full))[i];
		}} else
			 tmp_data =
			    CR_MemGuard < etype > ((etype *) data_full, false);
		int retcode = CPRS_SUCCESS;
		retcode =
		    nc.Compress((char *)tmp_comp_buffer, tmp_cb_len,
				tmp_data.get(), no_obj - no_nulls,
				(etype) (maxv));
		return retcode;
	}

	template < typename etype >
	    int DecompressAndInsertNulls(NumCompressor < etype > &nc,
					 uint * &cur_buf,
					 const int no_nulls,
					 const uint * nulls,
					 const int no_obj, void *data_full) {
		int retcode = CPRS_SUCCESS;
		retcode =
		    nc.Decompress((etype *) data_full, (char *)((cur_buf + 3)),
				  *cur_buf, no_obj - no_nulls,
				  (etype) * (_uint64 *) ((cur_buf + 1)));
		if (CPRS_SUCCESS != retcode) {
			return retcode;
		}
		etype *d = ((etype *) (data_full)) + no_obj - 1;
		etype *s = ((etype *) (data_full)) + no_obj - no_nulls - 1;
		for (int i = no_obj - 1; d > s; i--) {
			if (ib_cprs_assist::IsNull(nulls, no_nulls, no_obj, i)) {
				--d;
			} else {
				*(d--) = *(s--);
			}
		}
		return retcode;
	}
}

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
int ib_packn_compress(const uint * nulls,	/*: nulls 存储缓存      */
		      const int no_nulls,	/*: null 个数           */
		      const void *data_full,	/*: 完整数据            */
		      const int no_obj,	/*: 数据包中对象数      */
		      const _uint64 maxv,	/*: 数据中的最大值      */
		      std::string & compressed_str,	/*: 压缩后的缓存        */
		      uchar & optimal_mode)
{				/*: 压缩状态选项        */
	uint buffer_size = 0;
	CR_MemGuard < uchar > tmp_comp_buffer;

	uint tmp_cb_len = 0;
	int retcode = CPRS_SUCCESS;

	CR_MemGuard < uchar > comp_null_buf;
	uint null_buf_size = 0;

	ValueType value_type = ib_packn_assist::GetValueType(maxv);
	optimal_mode = 0x0;

	compressed_str.clear();

	// const int no_obj,void* data_full
	//>> 1. compress data
	if (maxv != 0) {
		if (value_type == UCHAR) {
			NumCompressor < uchar >
			    nc(ib_pack_assist::ShouldNotCompress(no_obj));
			tmp_cb_len = (no_obj - no_nulls) * sizeof(uchar) + 20;
			if (tmp_cb_len)
				tmp_comp_buffer =
				    CR_MemGuard < uchar > (tmp_cb_len);
			ib_packn_assist::RemoveNullsAndCompress(nc,
								tmp_comp_buffer.
								get(),
								(uint &)
								tmp_cb_len,
								(_uint64) maxv,
								(int)no_nulls,
								(uint *) nulls,
								(int)no_obj,
								(void *)
								data_full);
		} else if (value_type == USHORT) {
			NumCompressor < ushort >
			    nc(ib_pack_assist::ShouldNotCompress(no_obj));
			tmp_cb_len = (no_obj - no_nulls) * sizeof(ushort) + 20;
			if (tmp_cb_len)
				tmp_comp_buffer =
				    CR_MemGuard < uchar > (tmp_cb_len);
			retcode =
			    ib_packn_assist::RemoveNullsAndCompress(nc,
								    tmp_comp_buffer.
								    get(),
								    (uint &)
								    tmp_cb_len,
								    (_uint64)
								    maxv,
								    (int)
								    no_nulls,
								    (uint *)
								    nulls,
								    (int)no_obj,
								    (void *)
								    data_full);
		} else if (value_type == UINT) {
			NumCompressor < uint >
			    nc(ib_pack_assist::ShouldNotCompress(no_obj));
			tmp_cb_len = (no_obj - no_nulls) * sizeof(uint) + 20;
			if (tmp_cb_len)
				tmp_comp_buffer =
				    CR_MemGuard < uchar > (tmp_cb_len);
			retcode =
			    ib_packn_assist::RemoveNullsAndCompress(nc,
								    tmp_comp_buffer.
								    get(),
								    (uint &)
								    tmp_cb_len,
								    (_uint64)
								    maxv,
								    (int)
								    no_nulls,
								    (uint *)
								    nulls,
								    (int)no_obj,
								    (void *)
								    data_full);
		} else {
			NumCompressor < _uint64 >
			    nc(ib_pack_assist::ShouldNotCompress(no_obj));
			tmp_cb_len = (no_obj - no_nulls) * sizeof(_uint64) + 20;
			if (tmp_cb_len)
				tmp_comp_buffer =
				    CR_MemGuard < uchar > (tmp_cb_len);
			retcode =
			    ib_packn_assist::RemoveNullsAndCompress(nc,
								    tmp_comp_buffer.
								    get(),
								    (uint &)
								    tmp_cb_len,
								    (_uint64)
								    maxv,
								    (int)
								    no_nulls,
								    (uint *)
								    nulls,
								    (int)no_obj,
								    (void *)
								    data_full);
		}

		if (CPRS_SUCCESS != retcode) {
			DPRINTF("compress data error,retcode[%d].\n", retcode);
			goto finish;
		}

		ib_packn_compressmode_assist::
		    SetModeDataCompressed(optimal_mode);
		buffer_size += tmp_cb_len;
	}
	buffer_size += 12;

	//>> 2. compress nulls
	null_buf_size = ((no_obj + 7) / 8);
	if (no_nulls > 0) {
		if (ib_pack_assist::ShouldNotCompress(no_obj)) {
			comp_null_buf =
			    CR_MemGuard < uchar > ((uchar *) nulls, false);
			null_buf_size = ((no_obj + 7) / 8);

			ib_packn_compressmode_assist::ResetModeNullsCompressed
			    (optimal_mode);
		} else {
			comp_null_buf =
			    CR_MemGuard < uchar > (null_buf_size + 2);
			uint cnbl = null_buf_size + 1;
			comp_null_buf[cnbl] = 0xBA;
			BitstreamCompressor bsc;
			retcode =
			    bsc.Compress((char *)comp_null_buf.get(),
					 null_buf_size, (char *)nulls, no_obj,
					 no_nulls);
			if (CPRS_SUCCESS == retcode) {
				ib_packn_compressmode_assist::
				    SetModeNullsCompressed(optimal_mode);
				if (comp_null_buf[cnbl] != 0xBA)
					retcode = CPRS_ERR_COR;
			}
		}

		if ((CPRS_SUCCESS != retcode) || (null_buf_size > ((no_obj + 7) / 8))) {
			retcode = CPRS_SUCCESS;
			comp_null_buf = CR_MemGuard < uchar > ((uchar *) nulls, false);
			null_buf_size = ((no_obj + 7) / 8);
			ib_packn_compressmode_assist::
			    ResetModeNullsCompressed(optimal_mode);
		}

		buffer_size += null_buf_size + 2;
	}
	//>> 3. copy combination to compressed_null buff
	if (no_nulls > 0) {
		ushort null_buf_size_short = null_buf_size;
		compressed_str.append((const char *)(&null_buf_size_short),
				      sizeof(null_buf_size_short));
		compressed_str.append((const char *)comp_null_buf.get(),
				      null_buf_size);
	}
	//>> 4. copy maxv and compressed data len
	compressed_str.append((const char *)(&tmp_cb_len), sizeof(tmp_cb_len));
	compressed_str.append((const char *)(&maxv), sizeof(maxv));
	compressed_str.append((const char *)tmp_comp_buffer.get(), tmp_cb_len);

 finish:

	return retcode;
}

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
int ib_packn_decompress(const uchar * compressed_buf,	/*: 压缩后的缓存        */
			const uint compressed_buf_len,	/*: 压缩后的缓存长度    */
			const uchar optimal_mode,	/*: 压缩状态选项        */
			uint * nulls,	/*: nulls 存储缓存,8K   */
			const int no_nulls,	/*: null 个数           */
			void *data_full,	/*: 完整数据            */
			const int no_obj)
{				/*: 数据包中对象数      */
	int retcode = CPRS_SUCCESS;

	if (ib_packn_compressmode_assist::IsModeNoCompression(optimal_mode)) {
		DPRINTF("ib_compressmode_assist::IsModeNoCompression .\n");
		return retcode;
	}

	uint *cur_buf = (uint *) compressed_buf;

	//>> 1. decompress nulls
	if (no_nulls > 0) {
		uint null_buf_size = 0;

		if (no_obj < 65536) {
			memset(nulls, 0, 8192);
		}

		null_buf_size = (*(ushort *) cur_buf);
		if (null_buf_size > 8192) {
			retcode = CPRS_ERR_PAR;
			DPRINTF("Unexpected bytes found in data pack .\n");
			goto finish;
		}
		if (!ib_packn_compressmode_assist::IsModeNullsCompressed(optimal_mode)) {	// no nulls compression
			memcpy(nulls, (uchar *) cur_buf + 2, null_buf_size);
		} else {
			BitstreamCompressor bsc;
			retcode =
			    bsc.Decompress((char *)nulls, null_buf_size,
					   (char *)cur_buf + 2, no_obj,
					   no_nulls);
			if (CPRS_SUCCESS != retcode) {
				DPRINTF("decompress nulls error,recode[%d].\n",
					retcode);
				goto finish;
			}
#if defined(_DEBUG) || (defined(__GNUC__) && !defined(NDEBUG))
			// For tests:
			uint nulls_counted = 0;
			for (uint i = 0; i < 2048; i++) {
				nulls_counted +=
				    ib_pack_assist::CalculateBinSum(nulls[i]);
			}

			if (no_nulls != nulls_counted) {
				DPRINTF
				    ("uncompressed wrong number of nulls.\n");
				retcode = CPRS_ERR_COR;
				goto finish;
			}
#endif
		}
		// skip the cmp_null_len and cmp_len
		cur_buf = (uint *) ((uchar *) cur_buf + null_buf_size + 2);
	}
	//>> 2. depress data and insert nulls
	if (!ib_packn_compressmode_assist::IsModeValid(optimal_mode)) {
		DPRINTF("Unexpected byte in data pack\n");
		retcode = CPRS_ERR_PAR;
		goto finish;
	} else {
		_uint64 maxv = *(_uint64 *) (cur_buf + 1);
		ValueType value_type = ib_packn_assist::GetValueType(maxv);
		// 压缩过的数据，且最大值不为0
		if (ib_packn_compressmode_assist::
		    IsModeDataCompressed(optimal_mode)
		    && maxv != (_uint64) 0) {
			if (value_type == UCHAR) {
				NumCompressor < uchar > nc;
				retcode =
				    ib_packn_assist::
				    DecompressAndInsertNulls(nc, cur_buf,
							     no_nulls, nulls,
							     no_obj, data_full);
			} else if (value_type == USHORT) {
				NumCompressor < ushort > nc;
				retcode =
				    ib_packn_assist::
				    DecompressAndInsertNulls(nc, cur_buf,
							     no_nulls, nulls,
							     no_obj, data_full);
			} else if (value_type == UINT) {
				NumCompressor < uint > nc;
				retcode =
				    ib_packn_assist::
				    DecompressAndInsertNulls(nc, cur_buf,
							     no_nulls, nulls,
							     no_obj, data_full);
			} else {
				NumCompressor < _uint64 > nc;
				retcode =
				    ib_packn_assist::
				    DecompressAndInsertNulls(nc, cur_buf,
							     no_nulls, nulls,
							     no_obj, data_full);
			}

			if (CPRS_SUCCESS != retcode) {
				DPRINTF
				    ("DecompressAndInsertNulls data error,retcode[%d].\n",
				     retcode);
				goto finish;
			}
		} else {
			// 没有压缩过的数据，不为空的直接设置为0
			for (uint o = 0; o < no_obj; o++) {
				if (!ib_cprs_assist::
				    IsNull(nulls, no_nulls, no_obj, (int)o)) {
					ib_packn_assist::SetVal64(data_full,
								  value_type,
								  no_obj, o, 0);
				}
			}
		}
	}

 finish:
	return retcode;
}

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
int ib_packs_compress(const uint * nulls,	/*: nulls 存储缓存      */
		      const int no_nulls,	/*: null 个数           */
		      const void *data,	/*: 完整数据            */
		      const uint data_full_byte_size,	/*: 完整数据的长度      */
		      const int no_obj,	/*: 数据包中对象数      */
		      const void *lens,	/*: 数据长度数组        */
		      const short len_mode,	/*: 长度模式            */
		      std::string & compressed_str,	/*: 压缩后的缓存        */
		      uchar & optimal_mode)
{				/*: 压缩状态选项        */
	int retcode = CPRS_SUCCESS;

	CR_MemGuard < uchar > new_compressed_buf;
	uchar *p = NULL;
	int dlen = 0;
	CR_MemGuard < uchar > comp_buf;
	TextCompressor tc;
	int zlo = 0;
	optimal_mode = 0x0;

	// get maxv
	int onn = 0;
	uint maxv = 0;
	uint cv = 0;

	// compress len
	CR_MemGuard < uint > comp_len_buf;
	NumCompressor < uint > nc(ib_pack_assist::ShouldNotCompress(no_obj));
	CR_MemGuard < uint > nc_buffer;

	size_t compressed_buf_len = 0;

	compressed_str.clear();

	//>> 1. Compress nulls:
	uint comp_len_buf_size = 0, comp_null_buf_size = 0, comp_buf_size = 0;
	CR_MemGuard < uchar > comp_null_buf;
	if (no_nulls > 0) {
		if (ib_pack_assist::ShouldNotCompress(no_obj)) {
			comp_null_buf =
			    CR_MemGuard < uchar > ((uchar *) nulls, false);
			comp_null_buf_size = ((no_obj + 7) / 8);
			ib_packs_compressmode_assist::ResetModeNullsCompressed
			    (optimal_mode);
		} else {
			comp_null_buf_size = ((no_obj + 7) / 8);
			comp_null_buf =
			    CR_MemGuard < uchar > (comp_null_buf_size + 2);

			uint cnbl = comp_null_buf_size + 1;
			comp_null_buf[cnbl] = 0xBA;	// just checking - buffer overrun
			BitstreamCompressor bsc;

			retcode =
			    bsc.Compress((char *)comp_null_buf.get(),
					 comp_null_buf_size, (char *)nulls,
					 no_obj, no_nulls);
			if (CPRS_SUCCESS == retcode) {
				ib_packs_compressmode_assist::
				    SetModeNullsCompressed(optimal_mode);
				if (comp_null_buf[cnbl] != 0xBA)
					retcode = CPRS_ERR_COR;
			}
		}

		if ((CPRS_SUCCESS != retcode) || (comp_null_buf_size > ((no_obj + 7) / 8))) {
			retcode = CPRS_SUCCESS;
			comp_null_buf = CR_MemGuard < uchar > ((uchar *) nulls, false);
			comp_null_buf_size = ((no_obj + 7) / 8);
			ib_packn_compressmode_assist::
			    ResetModeNullsCompressed(optimal_mode);
		}
	}
	//>> 2. compress len
	nc_buffer = CR_MemGuard < uint > (1 << 16);

	//>> 2.1 get max len
	for (uint o = 0; o < no_obj; o++) {
		if (!ib_cprs_assist::IsNull(nulls, no_nulls, no_obj, o)) {
			cv = ib_packs_assist::GetSize(lens, len_mode, o);
			*(nc_buffer.get() + onn++) = cv;
			if (cv > maxv)
				maxv = cv;
		}
	}

	//>> 2.2 compress len
	if (maxv != 0) {
		comp_len_buf_size = onn * sizeof(uint) + 28;
		comp_len_buf = CR_MemGuard < uint > (comp_len_buf_size / 4);
		uint tmp_comp_len_buf_size = comp_len_buf_size - 8;
		retcode =
		    nc.Compress((char *)(comp_len_buf.get() + 2),
				tmp_comp_len_buf_size, nc_buffer.get(), onn,
				maxv);
		if (CPRS_SUCCESS != retcode) {
			DPRINTF("compress lens error,retcode[%d].\n", retcode);
			goto finish;
		}
		comp_len_buf_size = tmp_comp_len_buf_size + 8;
	} else {
		comp_len_buf_size = 8;
		comp_len_buf = CR_MemGuard < uint > (2);
	}

	// set compressed lens buff size and max length
	*comp_len_buf.get() = comp_len_buf_size;
	*(comp_len_buf.get() + 1) = maxv;	// the max length

	//>> 3. compress data
	for (uint obj = 0; obj < no_obj; obj++) {
		if (!ib_cprs_assist::IsNull(nulls, no_nulls, no_obj, obj)
		    && ib_packs_assist::GetSize(lens, len_mode, obj) == 0) {
			zlo++;
		}
	}

	dlen = (int)data_full_byte_size + 10;
	comp_buf = CR_MemGuard < uchar > (dlen);

	if (data_full_byte_size) {
		int objs = (no_obj - no_nulls) - zlo;

		CR_MemGuard < char *>tmp_index(objs);
		CR_MemGuard < ushort > tmp_len(objs);

		int nid = 0;
		uint index_pos = 0;
		//>> 3.1 get index array and len array
		for (int id = 0; id < (int)no_obj; id++) {
			if (!ib_cprs_assist::IsNull(nulls, no_nulls, no_obj, id)
			    && ib_packs_assist::GetSize(lens, len_mode,
							id) != 0) {
				tmp_index[nid] = (char *)data + index_pos;	// 获取指定的index位置
				tmp_len[nid] =
				    ib_packs_assist::GetSize(lens, len_mode,
							     id);
				index_pos += tmp_len[nid];
				nid++;
			}
		}

		retcode =
		    tc.Compress((char *)comp_buf.get(), (int &)dlen,
				tmp_index.get(), (ushort *) tmp_len.get(),
				(int)objs,
				ib_pack_assist::
				ShouldNotCompress(no_obj) ? 0 : TextCompressor::
				VER);

		if (CPRS_SUCCESS != retcode) {
			DPRINTF("compress data error,retcode[%d].\n", retcode);
			goto finish;
		}
	} else {
		dlen = 0;
	}

	ib_packs_compressmode_assist::SetModeDataCompressed(optimal_mode);

	//>> 4 copy combination compressed data
	//>> 4.1 null_size + null_size_len(2 bytes) + len_size + len_size_len(4 bytes) + compressed_data_len
	comp_buf_size =
	    (comp_null_buf_size >
	     0 ? 2 + comp_null_buf_size : 0) + comp_len_buf_size + 4 + dlen;

	new_compressed_buf = CR_MemGuard < uchar > (comp_buf_size);
	p = new_compressed_buf.get();

	//>> 4.2 copy nulls
	if (no_nulls > 0) {
		*((ushort *) p) = (ushort) comp_null_buf_size;
		p += 2;
		compressed_buf_len += 2;
		memcpy(p, comp_null_buf.get(), comp_null_buf_size);
		p += comp_null_buf_size;
		compressed_buf_len += comp_null_buf_size;
	}
	//>> 4.3 copy lens
	if (comp_len_buf_size) {
		memcpy(p, comp_len_buf.get(), comp_len_buf_size);
		compressed_buf_len += comp_len_buf_size;
	}
	p += comp_len_buf_size;

	//>> 4.4 copy data
	*((int *)p) = dlen;
	p += sizeof(int);
	compressed_buf_len += sizeof(int);
	if (dlen) {
		memcpy(p, comp_buf.get(), dlen);
		compressed_buf_len += dlen;
	}
	compressed_str.assign((const char *)new_compressed_buf.get(),
			      compressed_buf_len);

 finish:

	return retcode;
}

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
int ib_packs_decompress(const uchar * compressed_buf,	/*: 压缩后的缓存        */
			const int compressed_buf_len,	/*: 压缩后的缓存长度    */
			const int no_obj,	/*: 数据包中对象数      */
			const uchar optimal_mode,	/*: 压缩状态选项        */
			void *data,	/*: 完整数据缓冲        */
			const uint data_len,	/*: 完整的数据缓冲区长度 */
			const short len_mode,	/*: 长度模式            */
			void *lens,	/*: 数据长度            */
			uint * nulls,	/*: nulls 存储缓存      */
			const int no_nulls)
{				/*: null 个数           */
	int retcode = CPRS_SUCCESS;

	if (ib_packs_compressmode_assist::IsModeNoCompression(optimal_mode)) {
		DPRINTF
		    ("ib_packs_compressmode_assist::IsModeNoCompression .\n");
		return retcode;
	}

	CR_MemGuard < char >p_new_data(data_len);

	uint length_pos = 0;

	uint comp_len_buf_size = 0;

	CR_MemGuard < char *>tmp_index(no_obj);

	int i;
	uint *cur_buf = (uint *) compressed_buf;

	//>> 1. decompress nulls
	char (*FREE_PLACE) (reinterpret_cast < char *>(-1));

	uint null_buf_size = 0;
	if (no_nulls > 0) {
		null_buf_size = (*(ushort *) cur_buf);
		if (!ib_packs_compressmode_assist::
		    IsModeNullsCompressed(optimal_mode)) {
			// flat null encoding
			memcpy(nulls, (uchar *) cur_buf + 2, null_buf_size);
		} else {
			BitstreamCompressor bsc;
			retcode =
			    bsc.Decompress((char *)nulls, null_buf_size,
					   (char *)cur_buf + 2, no_obj,
					   no_nulls);
			if (CPRS_SUCCESS != retcode) {
				DPRINTF("decompress nulls error,recode[%d].\n",
					retcode);
				goto finish;
			}
#if defined(_DEBUG) || (defined(__GNUC__) && !defined(NDEBUG))
			// For tests:
			uint nulls_counted = 0;
			for (i = 0; i < 2048; i++) {
				nulls_counted +=
				    ib_pack_assist::CalculateBinSum(nulls[i]);
			}
			if (no_nulls != nulls_counted) {
				DPRINTF
				    ("uncompressed wrong number of nulls.\n");
				retcode = CPRS_ERR_COR;
				goto finish;
			}
#endif
		}

		// skip the cmp_null_len and cmp_nulls
		cur_buf = (uint *) ((uchar *) cur_buf + null_buf_size + 2);

		for (i = 0; i < (int)no_obj; i++) {
			if (ib_cprs_assist::IsNull(nulls, no_nulls, no_obj, i)) {
				tmp_index[i] = NULL;	// i.e. nulls
			} else {
				tmp_index[i] = FREE_PLACE;	// special value: an object awaiting decoding
			}
		}
	} else {
		for (i = 0; i < (int)no_obj; i++) {
			tmp_index[i] = FREE_PLACE;
		}
	}

	//>> 2. decompress buf len and buf
	if (optimal_mode == 0) {
		DPRINTF("Error: Compression format no longer supported.\n");
		retcode = CPRS_ERR_PAR;
		goto finish;
	} else {
		//>> 2.1 : decompress len buf
		comp_len_buf_size = *cur_buf;
		if ((_uint64) * (cur_buf + 1) != 0) {	// maxv is not zeor
			NumCompressor < uint > nc;
			// malloc 65536 * sizeof(uint),to store the len
			CR_MemGuard < uint > cn_ptr(1 << 16);
			retcode =
			    nc.Decompress(cn_ptr.get(), (char *)(cur_buf + 2),
					  comp_len_buf_size - 8,
					  no_obj - no_nulls,
					  *(uint *) (cur_buf + 1));

			if (CPRS_SUCCESS != retcode) {
				DPRINTF("decompress lens error,retcode[%d].\n",
					retcode);
				goto finish;
			}

			int oid = 0;
			for (uint o = 0; o < no_obj; o++) {
				if (!ib_cprs_assist::
				    IsNull(nulls, no_nulls, no_obj, int (o))) {
					ib_packs_assist::SetSize(lens, len_mode,
								 o,
								 (uint)
								 cn_ptr[oid++]);
				}
			}
		} else {	// max len is zero
			for (uint o = 0; o < no_obj; o++) {
				if (!ib_cprs_assist::
				    IsNull(nulls, no_nulls, no_obj, int (o))) {
					ib_packs_assist::SetSize(lens, len_mode,
								 o, 0);
				}
			}
		}

		//>> 2.2 decompress data
		cur_buf = (uint *) ((char *)(cur_buf) + comp_len_buf_size);	// compresseddata len
		TextCompressor tc;
		int dlen = *(int *)cur_buf;
		cur_buf += 1;
		int zlo = 0;
		for (uint obj = 0; obj < no_obj; obj++) {
			if (!ib_cprs_assist::
			    IsNull(nulls, no_nulls, no_obj, obj)
			    && ib_packs_assist::GetSize(lens, len_mode,
							obj) == 0) {
				zlo++;
			}
		}
		int objs = no_obj - no_nulls - zlo;

		if (objs) {
			CR_MemGuard < ushort > tmp_len(objs);

			for (uint tmp_id = 0, id = 0; id < no_obj; id++) {
				if (!ib_cprs_assist::
				    IsNull(nulls, no_nulls, no_obj, id)
				    && ib_packs_assist::GetSize(lens, len_mode,
								id) != 0) {
					tmp_len[tmp_id++] =
					    ib_packs_assist::GetSize(lens,
								     len_mode,
								     id);
				}
			}

			retcode =
			    tc.Decompress((char *)data, data_len,
					  (char *)cur_buf, dlen,
					  tmp_index.get(), tmp_len.get(), objs);

			if (CPRS_SUCCESS != retcode) {
				DPRINTF("decompress data error,retcode[%d].\n",
					retcode);
				goto finish;
			}
		}
	}

	// 重新构建data
	for (uint tmp_id = 0, id = 0; id < no_obj; id++) {
		int _len = ib_packs_assist::GetSize(lens, len_mode, int (id));
		if (!ib_cprs_assist::IsNull(nulls, no_nulls, no_obj, int (id))
		    && _len != 0) {
			assert((length_pos + _len) <= data_len);
			if ((length_pos + _len) > data_len) {
				retcode = CPRS_ERR_OTH;
				DPRINTF("decompress data error,retcode[%d].\n",
					retcode);
				goto finish;
			} else {
				// 重新构造data数组
				memcpy(p_new_data.get() + length_pos,
				       (uchar *) tmp_index[tmp_id++], _len);
				length_pos += _len;
			}
		}
		if (optimal_mode == 0) {
			tmp_id++;
		}
	}

	memcpy(data, p_new_data.get(), data_len);

 finish:

	return retcode;
}
