#include "IBCompress.h"
#include <assert.h>
#include <errno.h>
#include <zlib.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_memguard.h>
#include <cr_class/cr_compress.h>

#include "common/CommonDefinitions.h"	// uint ,uchar, ...
#include "common/bhassert.h"	// BHASSERT

int
 ib_pack_assist::CompressCode2IBCode(const int compress_code)
{
	switch (compress_code) {
	case 0:
		return CPRS_SUCCESS;
	case EMSGSIZE:
	case ENOBUFS:
		return CPRS_ERR_BUF;
	case EFAULT:
	case EINVAL:
		return CPRS_ERR_PAR;
	case EBADMSG:
		return CPRS_ERR_SUM;
	case ENOTSUP:
		return CPRS_ERR_VER;
	case ENOMSG:
		return CPRS_ERR_COR;
	case ENOMEM:
		return CPRS_ERR_MEM;
	default:
		return CPRS_ERR_OTH;
	};
}

int
ib_packn_compress_auto(const uint * nulls,
		       const int no_nulls,
		       const void *data_full,
		       const int no_obj,
		       const _uint64 maxv,
		       std::string & compressed_str,
		       uchar & optimal_mode, const int compress_type)
{
	int retcode;

	uint data_buf_size = 0;
	uint null_buf_size = 0;

	std::string compressed_data;
	std::string compressed_null;

	ValueType value_type = ib_packn_assist::GetValueType(maxv);

	optimal_mode = 0x0;

	compressed_str.clear();

	// const int no_obj,void* data_full
	//>> 1. compress data
	if (maxv != 0) {
		retcode =
		    ib_pack_assist::CompressCode2IBCode(CR_Compress::compress
							(data_full,
							 no_obj * value_type,
							 compressed_data,
							 compress_type));

		if (CPRS_SUCCESS != retcode) {
			DPRINTF("compress data error,retcode[%d].\n", retcode);
			goto finish;
		}

		ib_packn_compressmode_assist::SetModeDataCompressed
		    (optimal_mode);
		data_buf_size = compressed_data.size();
	}
	//>> 2. compress nulls
	null_buf_size = ((no_obj + 7) / 8);
	if (no_nulls > 0) {
		if (ib_pack_assist::ShouldNotCompress(no_obj)) {
			compressed_null.assign((const char *)nulls,
					       null_buf_size);
			ib_packn_compressmode_assist::ResetModeNullsCompressed
			    (optimal_mode);
		} else {
			retcode =
			    ib_pack_assist::
			    CompressCode2IBCode(CR_Compress::compress
						(nulls, null_buf_size,
						 compressed_null,
						 compress_type));
			if ((CPRS_SUCCESS == retcode)
			    && (compressed_null.size() < 8192)) {
				null_buf_size = compressed_null.size();
				ib_packn_compressmode_assist::
				    SetModeNullsCompressed(optimal_mode);
			} else {
				compressed_null.assign((const char *)nulls,
						       null_buf_size);
				ib_packn_compressmode_assist::ResetModeNullsCompressed
				    (optimal_mode);
			}
		}
	}
	//>> 3. copy combination to compressed_null buff
	if (no_nulls > 0) {
		ushort null_buf_size_short = null_buf_size;
		compressed_str.append((const char *)(&null_buf_size_short),
				      sizeof(null_buf_size_short));
		compressed_str.append(compressed_null);
	}
	//>> 4. copy maxv and compressed data len
	compressed_str.append((const char *)(&data_buf_size),
			      sizeof(data_buf_size));
	compressed_str.append((const char *)(&maxv), sizeof(maxv));
	compressed_str.append(compressed_data);

 finish:

	return retcode;
}

int
ib_packn_decompress_auto(const uchar * compressed_buf,
			 const uint compressed_buf_len,
			 const uchar optimal_mode,
			 uint * nulls,
			 const int no_nulls, void *data_full, const int no_obj)
{
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
			uint64_t nulls_size_local = (no_obj + 7) / 8;
			int rt = CR_Compress::decompress((char *)cur_buf + 2,
							 null_buf_size,
							 nulls,
							 nulls_size_local
							 );
			retcode = ib_pack_assist::CompressCode2IBCode(rt);
			if (CPRS_SUCCESS != retcode) {
				DPRINTF("decompress nulls error,recode[%d].\n",
					retcode);
				goto finish;
			}
			cur_buf =
			    (uint *) ((uchar *) cur_buf + null_buf_size + 2);
		}

		//>> 2. depress data and insert nulls
		if (!ib_packn_compressmode_assist::IsModeValid(optimal_mode)) {
			DPRINTF("Unexpected byte in data pack.\n");
			retcode = CPRS_ERR_PAR;
			goto finish;
		} else {
			_uint64 maxv = *(_uint64 *) (cur_buf + 1);
			ValueType value_type =
			    ib_packn_assist::GetValueType(maxv);
			// 压缩过的数据，且最大值不为0
			if (ib_packn_compressmode_assist::IsModeDataCompressed
			    (optimal_mode) && maxv != (_uint64) 0) {
				uint64_t data_full_size_local =
				    no_obj * value_type;
				int rt =
				    CR_Compress::decompress((char *)cur_buf +
							    12, *cur_buf,
							    data_full,
							    data_full_size_local);
				retcode =
				    ib_pack_assist::CompressCode2IBCode(rt);
				if (CPRS_SUCCESS != retcode) {
					DPRINTF
					    ("DecompressAndInsertNulls data error,retcode[%d].\n",
					     retcode);
					goto finish;
				}
			} else {
				// 没有压缩过的数据，不为空的直接设置为0
				for (uint o = 0; o < no_obj; o++) {
					if (!ib_cprs_assist::IsNull
					    (nulls, no_nulls, no_obj, (int)o)) {
						ib_packn_assist::SetVal64
						    (data_full, value_type,
						     no_obj, o, 0);
					}
				}
			}
		}
	}
 finish:
	return retcode;
}

int ib_packs_compress_auto(const uint * nulls,
			   const int no_nulls,
			   const void *data,
			   const uint data_full_byte_size,
			   const int no_obj,
			   const void *lens,
			   const short len_mode,
			   std::string & compressed_str,
			   uchar & optimal_mode, const int compress_type)
{
	int retcode = CPRS_SUCCESS;

	CR_MemGuard < uchar > new_compressed_buf;
	uchar *p = NULL;
	int dlen = 0;
	CR_MemGuard < uchar > comp_buf;
	int zlo = 0;
	optimal_mode = 0x0;

	// get maxv
	int onn = 0;
	uint maxv = 0;
	uint cv = 0;

	// compress len
	CR_MemGuard < uint > comp_len_buf;
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

			uLongf _tmp_comp_null_buf_size = comp_null_buf_size;
			int rt = compress2((Bytef *) comp_null_buf.get(),
					   &_tmp_comp_null_buf_size,
					   (Bytef *) nulls,
					   ((no_obj + 7) / 8), Z_BEST_SPEED);
			retcode = ib_pack_assist::ZlibCode2IBCode(rt);

			if (comp_null_buf[cnbl] != 0xBA) {
				DPRINTF
				    ("compress null error,compressed buff overflowed ,retcode[%d].\n",
				     retcode);
				retcode = CPRS_ERR_COR;
				goto finish;
			}

			if (CPRS_ERR_BUF == retcode) {
				comp_null_buf =
				    CR_MemGuard < uchar >
				    ((uchar *) nulls, false);
				comp_null_buf_size = ((no_obj + 7) / 8);
				ib_packs_compressmode_assist::
				    ResetModeNullsCompressed(optimal_mode);
			} else if (CPRS_SUCCESS == retcode) {
				comp_null_buf_size = _tmp_comp_null_buf_size;
				ib_packs_compressmode_assist::
				    SetModeNullsCompressed(optimal_mode);
			} else {
				DPRINTF
				    ("compress null error,retcode[%d].\n",
				     retcode);
				goto finish;
			}
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
		comp_len_buf_size = onn * sizeof(uint) + 128 + 8;
		comp_len_buf = CR_MemGuard < uint > (comp_len_buf_size / 4);
		uLongf _tmp_comp_len_buf_size = comp_len_buf_size - 8;
		int rt = compress2((Bytef *) (comp_len_buf.get() + 2),
				   &_tmp_comp_len_buf_size,
				   (Bytef *) nc_buffer.get(),
				   onn * sizeof(uint),
				   Z_BEST_SPEED);
		retcode = ib_pack_assist::ZlibCode2IBCode(rt);
		if (CPRS_SUCCESS != retcode) {
			DPRINTF("compress lens error,retcode[%d].\n", retcode);
			goto finish;
		}
		comp_len_buf_size = _tmp_comp_len_buf_size + 8;
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

	dlen = (int)data_full_byte_size + 128;
	comp_buf = CR_MemGuard < uchar > (dlen);

	if (data_full_byte_size) {
		int objs = (no_obj - no_nulls) - zlo;

		CR_MemGuard < char *>tmp_index(objs);
		CR_MemGuard < ushort > tmp_len(objs);

		int nid = 0;
		uint index_pos = 0;
		uLong _combination_pos = 0;
		//>> 3.1 get index array and len array
		for (int id = 0; id < (int)no_obj; id++) {
			if (!ib_cprs_assist::IsNull(nulls, no_nulls, no_obj, id)
			    && ib_packs_assist::GetSize(lens, len_mode,
							id) != 0) {
				tmp_index[nid] = (char *)data + index_pos;	// 获取指定的index位置
				tmp_len[nid] =
				    ib_packs_assist::GetSize(lens,
							     len_mode, id);
				index_pos += tmp_len[nid];
				nid++;
			}
		}

		CR_MemGuard < char >tmp_combination_index(index_pos + 1);
		tmp_combination_index[index_pos] = '\0';

		for (int _i = 0; _i < nid; _i++) {
			// merge the combination_index data
			memcpy((tmp_combination_index.get() +
				_combination_pos), tmp_index[_i], tmp_len[_i]);
			_combination_pos += tmp_len[_i];
		}

		uLongf _tmp_dlen = dlen;
		int rt = compress2((Bytef *) comp_buf.get(), &_tmp_dlen,
				   (Bytef *) tmp_combination_index.get(),
				   index_pos,
				   Z_BEST_SPEED);
		retcode = ib_pack_assist::ZlibCode2IBCode(rt);

		if (CPRS_SUCCESS != retcode) {
			DPRINTF("compress data error,retcode[%d].\n", retcode);
			goto finish;
		}

		dlen = _tmp_dlen;
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

int ib_packs_decompress_auto(const uchar * compressed_buf,
			     const int compressed_buf_len,
			     const int no_obj,
			     const uchar optimal_mode,
			     void *data,
			     const uint data_len,
			     const short len_mode,
			     void *lens, uint * nulls, const int no_nulls)
{
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
		if (!ib_packs_compressmode_assist::IsModeNullsCompressed
		    (optimal_mode)) {
			// flat null encoding
			memcpy(nulls, (uchar *) cur_buf + 2, null_buf_size);
		} else {
			uLongf _tmp_null_buf_size = null_buf_size;
			int rt = uncompress((Bytef *) nulls,
					    (uLongf *) & _tmp_null_buf_size,
					    (Bytef *) (cur_buf + 2),
					    (uLong) (no_nulls * sizeof(char)));
			retcode = ib_pack_assist::ZlibCode2IBCode(rt);
			if (CPRS_SUCCESS != retcode) {
				DPRINTF
				    ("decompress nulls error,recode[%d].\n",
				     retcode);
				goto finish;
			}
			null_buf_size = _tmp_null_buf_size;
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
			// malloc 65536 * sizeof(uint),to store the len
			uLongf _tmp_len = (1 << 16) * sizeof(uint);
			CR_MemGuard < uint > cn_ptr(1 << 16);
			int rt = uncompress((Bytef *) cn_ptr.get(),
					    &_tmp_len,
					    (Bytef *) (cur_buf + 2),
					    (uLong) ((comp_len_buf_size -
						      8) * sizeof(char)));
			retcode = ib_pack_assist::ZlibCode2IBCode(rt);
			if (CPRS_SUCCESS != retcode) {
				DPRINTF
				    ("decompress lens error,retcode[%d].\n",
				     retcode);
				goto finish;
			}
			int oid = 0;
			for (uint o = 0; o < no_obj; o++) {
				if (!ib_cprs_assist::IsNull
				    (nulls, no_nulls, no_obj, int (o))) {
					ib_packs_assist::SetSize(lens,
								 len_mode,
								 o, (uint)
								 cn_ptr[oid++]);
				}
			}
		} else {	// max len is zero
			for (uint o = 0; o < no_obj; o++) {
				if (!ib_cprs_assist::IsNull
				    (nulls, no_nulls, no_obj, int (o))) {
					ib_packs_assist::SetSize(lens,
								 len_mode,
								 o, 0);
				}
			}
		}

		//>> 2.2 decompress data
		cur_buf = (uint *) ((char *)(cur_buf) + comp_len_buf_size);	// compresseddata len
		int dlen = *(int *)cur_buf;
		cur_buf += 1;
		int zlo = 0;
		for (uint obj = 0; obj < no_obj; obj++) {
			if (!ib_cprs_assist::IsNull
			    (nulls, no_nulls, no_obj, obj)
			    && ib_packs_assist::GetSize(lens, len_mode,
							obj) == 0) {
				zlo++;
			}
		}
		int objs = no_obj - no_nulls - zlo;

		if (objs) {
			CR_MemGuard < ushort > tmp_len(objs);

			for (uint tmp_id = 0, id = 0; id < no_obj; id++) {
				if (!ib_cprs_assist::IsNull
				    (nulls, no_nulls, no_obj, id)
				    && ib_packs_assist::GetSize(lens, len_mode,
								id) != 0) {
					tmp_len[tmp_id++] =
					    ib_packs_assist::GetSize(lens,
								     len_mode,
								     id);
				}
			}

			uLongf _tmp_data_full_byte_size = data_len;
			int rt = uncompress((Bytef *) data,
					    &_tmp_data_full_byte_size,
					    (Bytef *) cur_buf, dlen);

			retcode = ib_pack_assist::ZlibCode2IBCode(rt);
			if (CPRS_SUCCESS != retcode) {
				DPRINTF
				    ("decompress data error,retcode[%d].\n",
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
				DPRINTF
				    ("decompress data error,retcode[%d].\n",
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
