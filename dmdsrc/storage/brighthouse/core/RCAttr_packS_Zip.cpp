/* Copyright (C)  2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free  Software Foundation.

This program is distributed in the hope that  it will be useful, but
WITHOUT ANY WARRANTY; without even  the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License version 2.0 for more details.

You should have received a  copy of the GNU General Public License
version 2.0  along with this  program; if not, write to the Free
Software Foundation,  Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA  */

#include "RCAttr.h"
#include "RCAttrPack.h"
#include "bintools.h"
#include "compress/BitstreamCompressor.h"
#include "compress/TextCompressor.h"
#include "compress/PartDict.h"
#include "compress/NumCompressor.h"
#include "system/TextUtils.h"
#include "system/IBStream.h"
#include "WinTools.h"
#include "tools.h"
#include "ValueSet.h"
#include "system/MemoryManagement/MMGuard.h"
#include "zlib.h"

AttrPackS_Zip::AttrPackS_Zip(PackCoordinate pc, AttributeType attr_type, int inserting_mode, bool no_compression, DataCache* owner)
	:	AttrPackS(pc, attr_type, inserting_mode, no_compression, owner)
{
}

/*
AttrPackS_Zip::AttrPackS_Zip(const AttrPackS_Zip &aps)
	:ver(aps.ver), previous_no_obj(aps.previous_no_obj), data(0), index(0), lens(0), decomposer_id(aps.decomposer_id), use_already_set_decomposer(aps.use_already_set_decomposer), outliers(aps.outliers)
{
	try {
		assert(!is_only_compressed);
		max_no_obj = aps.max_no_obj;
		//data_id = aps.data_id;
		data_id = 0;
		data_full_byte_size = aps.data_full_byte_size;
		len_mode = aps.len_mode;
		binding = false;
		last_copied = no_obj - 1;
		optimal_mode = aps.optimal_mode;

		lens = alloc(len_mode * no_obj * sizeof(char), BLOCK_UNCOMPRESSED);
		memcpy(lens, aps.lens, len_mode * no_obj * sizeof(char));

		index = (uchar**)alloc(no_obj * sizeof(uchar*), BLOCK_UNCOMPRESSED);
		data = 0;

		//int ds = (int) rc_msize(aps.data) / sizeof(char*);
		if(data_full_byte_size) {
			data_id = 1;
			data = (uchar**)alloc(sizeof(uchar*), BLOCK_UNCOMPRESSED);
			*data = 0;
			int sum_size = 0;
			for (uint i = 0; i < aps.no_obj; i++) {
				sum_size += aps.GetSize(i);
			}
			if(sum_size > 0) {
				data[0] = (uchar*) alloc(sum_size, BLOCK_UNCOMPRESSED);
			} else
				data[0] = 0;
			sum_size = 0;
			for (uint i = 0; i < aps.no_obj; i++) {
				int size = aps.GetSize(i);
				if(!aps.IsNull(i) && size != 0) {
					memcpy(data[0] + sum_size, (uchar*)aps.index[i], size);
					index[i] = data[0] + sum_size;
					sum_size += size;
				} else
					index[i] = 0;
			}
		}
	} catch (...) {
		Destroy();
		throw;
	}
}
*/
std::auto_ptr<AttrPack> AttrPackS_Zip::Clone() const
{
	return std::auto_ptr<AttrPack>(new AttrPackS_Zip(*this));
}

void AttrPackS_Zip::Uncompress(DomainInjectionManager& dim)
{
	switch (ver) {
	case 0:
		UncompressOld();
		break;
	case 8:
		Uncompress8(dim);
		break;
	default:
		rclog << lock << "ERROR: wrong version of data pack format" << unlock;
		BHASSERT(0, "ERROR: wrong version of data pack format");
		break;
	}
}

void AttrPackS_Zip::Uncompress8(DomainInjectionManager& dim)
{
    //>> Begin :add by liujs, compressOld and compress8 are the same
	UncompressOld();
	return ;
	//<< End: add by liujs

	DomainInjectionDecomposer& decomposer = dim.Get(decomposer_id);	
	AllocNullsBuffer();
	uint *cur_buf = (uint*) compressed_buf;
	// decode nulls
	uint null_buf_size = 0;
	if(no_nulls > 0) 
	{
		null_buf_size = (*(ushort*) cur_buf);
		if(!IsModeNullsCompressed()) // flat null encoding
			memcpy(nulls, (char*) cur_buf + 2, null_buf_size);
		else 
		{
			BitstreamCompressor bsc;
			bsc.Decompress((char*) nulls, null_buf_size, (char*) cur_buf + 2, no_obj, no_nulls);
         	// For tests:
			uint nulls_counted = 0;
			for(uint i = 0; i < 2048; i++)
				nulls_counted += CalculateBinSum(nulls[i]);
			if(no_nulls != nulls_counted)
				rclog << lock << "Error: AttrPackT::Uncompress uncompressed wrong number of nulls." << unlock;
		}
		cur_buf = (uint*) ((char*) cur_buf + null_buf_size + 2);
	}

	char*	block_type = (char*) cur_buf;
	cur_buf = (uint*) ((char*) cur_buf + no_groups);  // one compression type so far so skip
	uint* block_lengths = cur_buf;						// lengths of blocks
	cur_buf += no_groups;

	//NumCompressor<ushort> nc;
	//TextCompressor tc;
	
	std::vector<boost::shared_ptr<DataBlock> > blocks;

	char* cur_bufc = (char*) cur_buf;	
	for (int g=0; g<no_groups; g++) {
		int bl_nobj = *(int*) cur_bufc;
		boost::shared_ptr<DataBlock> block;
		switch(*block_type) {
			case BLOCK_BINARY	:	
				block = boost::shared_ptr<DataBlock>(new BinaryDataBlock(bl_nobj));
				break;
			case BLOCK_NUMERIC_UCHAR	:
				block = boost::shared_ptr<DataBlock>(new NumericDataBlock<uchar>(bl_nobj));				
				break;
			case BLOCK_NUMERIC_USHORT	:
				block = boost::shared_ptr<DataBlock>(new NumericDataBlock<ushort>(bl_nobj));				
				break;
			case BLOCK_NUMERIC_UINT	:
				block = boost::shared_ptr<DataBlock>(new NumericDataBlock<uint>(bl_nobj));				
				break;
			case BLOCK_NUMERIC_UINT64	:
				block = boost::shared_ptr<DataBlock>(new NumericDataBlock<uint64>(bl_nobj));				
				break;
			case BLOCK_STRING	:
				block = boost::shared_ptr<DataBlock>(new StringDataBlock(bl_nobj));				
				break;
			default:
				BHASSERT(0, "Wrong data block type in decomposed data pack");
				break;
		}
		block->Decompress((char*) cur_bufc, *block_lengths);
		blocks.push_back(block);
		cur_bufc += (*block_lengths);
		block_lengths++;
		block_type++;
	}

	//data_full_byte_size = decomposer.GetComposedSize(blocks);
	AllocBuffersButNulls();

	StringDataBlock block_out(no_obj - no_nulls);
	decomposer.Compose(blocks, block_out, *((char**)(data)), data_full_byte_size, outliers);
	
	uint oid = 0;
	for(uint o = 0; o < no_obj; o++) 		
		if (!IsNull(o)) {
			index[o] = (uchar*) block_out.GetIndex(oid);
			SetSize(o, block_out.GetLens(oid));
			oid++;
		} else {
			SetSize(o, 0);
			index[o] = 0;
			//lens[o] = 0;
		}	

	dealloc(compressed_buf);
	compressed_buf=0;
	comp_buf_size=0;
}

void AttrPackS_Zip::UncompressOld()
{
#ifdef FUNCTIONS_EXECUTION_TIMES
	FETOperator feto("AttrPackS_Zip::Uncompress()");
	FETOperator feto1(string("aS[") + boost::lexical_cast<string>(pc_column( GetPackCoordinate())) + "].Compress(...)");
	NotifyDataPackDecompression(GetPackCoordinate());
#endif
	if(IsModeNoCompression())
		return;

	is_only_compressed = false;
	if(optimal_mode == 0 && ATI::IsBinType(attr_type)) {
		rclog << lock << "Error: Compression format no longer supported." << unlock;
		return;
	}

	AllocBuffers();
	MMGuard<char*> tmp_index((char**)alloc(no_obj * sizeof(char*), BLOCK_TEMPORARY), *this);
	////////////////////////////////////////////////////////
	int i; //,j,obj_317,no_of_rep,rle_bits;

	uint *cur_buf = (uint*) compressed_buf;

	///////////////////////////////////////////////////////
	// decode nulls
	char (*FREE_PLACE)(reinterpret_cast<char*> (-1));

	uint null_buf_size = 0;
	if(no_nulls > 0) 
	{
		null_buf_size = (*(ushort*) cur_buf);
		if(!IsModeNullsCompressed()) // flat null encoding
		{
			memcpy(nulls, (char*) cur_buf + 2, null_buf_size);
		}
		else 
		{
		    //>> Begin : modify by liujs
		    #if 0
			BitstreamCompressor bsc;
			bsc.Decompress((char*) nulls, null_buf_size, (char*) cur_buf + 2, no_obj, no_nulls);
            #else
            int rt = 0;
			uLongf _tmp_null_buf_size = null_buf_size;
			rt = uncompress((Bytef*)nulls,(uLongf*)&_tmp_null_buf_size,(Bytef*)(cur_buf + 2),(uLong)(no_nulls * sizeof(char)));
			if(Z_OK != rt)
			{
                rclog << lock << "ERROR: uncompress return "<< rt << " . "<< unlock;
				BHASSERT(0, "ERROR: uncompress return != Z_OK .");
			}
			#endif
            null_buf_size = _tmp_null_buf_size;
			
			//<< End: modify by liujs
			// For tests:
			uint nulls_counted = 0;
			for(i = 0; i < 2048; i++)
				nulls_counted += CalculateBinSum(nulls[i]);
			if(no_nulls != nulls_counted)
				rclog << lock << "Error: AttrPackT::Uncompress uncompressed wrong number of nulls." << unlock;
		}
		cur_buf = (uint*) ((char*) cur_buf + null_buf_size + 2);
		for(i = 0; i < (int) no_obj; i++) 
		{
			if(IsNull(i))
				tmp_index[i] = NULL; // i.e. nulls
			else
				tmp_index[i] = FREE_PLACE; // special value: an object awaiting decoding
		}
	} 
	else
	{
		for(i = 0; i < (int) no_obj; i++)
		{
			tmp_index[i] = FREE_PLACE;
		}
	}
	
	///////////////////////////////////////////////////////
	//	<null_buf_size><nulls><lens><char_lib_size><huffmann_size><huffmann><rle_bits><obj>...<obj>
	if(optimal_mode == 0) 
	{
		rclog << lock << "Error: Compression format no longer supported." << unlock;
	} 
	else 
	{
		comp_len_buf_size = *cur_buf;
		if((_uint64) * (cur_buf + 1) != 0)
		{
			NumCompressor<uint> nc;
			MMGuard<uint> cn_ptr((uint*) alloc((1 << 16) * sizeof(uint), BLOCK_TEMPORARY), *this);
			
            //>> Begin : modify by liujs
			#if 0
			nc.Decompress(cn_ptr.get(), (char*) (cur_buf + 2), comp_len_buf_size - 8, no_obj - no_nulls, *(uint*) (cur_buf + 1));
            #else
			int len = (1<<16)*sizeof(uint);
			int rt = 0;
			uLongf _tmp_len = len;
			rt = uncompress((Bytef*)cn_ptr.get(),(uLongf*)&_tmp_len,(Bytef*)(cur_buf + 2),(uLong)((comp_len_buf_size - 8)*sizeof(char)));
			if(Z_OK != rt)
			{
				rclog << lock << "ERROR: uncompress return "<< rt << " . "<< unlock;
				BHASSERT(0, "ERROR: uncompress return != Z_OK .");
			}
			len = _tmp_len;
			#endif
			//<< End: modify by liujs
			
			int oid = 0;
			for(uint o = 0; o < no_obj; o++)
			{
				if(!IsNull(int(o)))
					SetSize(o, (uint) cn_ptr[oid++]);
			} 
		}
		else 
		{
			for(uint o = 0; o < no_obj; o++)
				if(!IsNull(int(o)))
					SetSize(o, 0);
		}

		cur_buf = (uint*) ((char*) (cur_buf) + comp_len_buf_size);
		TextCompressor tc;
		int dlen = *(int*) cur_buf;
		cur_buf += 1;
		int zlo = 0;
		for(uint obj = 0; obj < no_obj; obj++)
		{
			if(!IsNull(obj) && GetSize(obj) == 0)
			{
				zlo++;
			}
		}
		int objs = no_obj - no_nulls - zlo;

		if(objs) 
		{
			MMGuard<ushort> tmp_len((ushort*) alloc(objs * sizeof(ushort), BLOCK_TEMPORARY), *this);
			for(uint tmp_id = 0, id = 0; id < no_obj; id++)
			{
				if(!IsNull(id) && GetSize(id) != 0)
				{
					tmp_len[tmp_id++] = GetSize(id);
				}
			}
			#if 0
			tc.Decompress((char*)*data, data_full_byte_size, (char*) cur_buf, dlen, tmp_index.get(), tmp_len.get(), objs);
            #else
			int rt = 0;
			uLongf _tmp_data_full_byte_size= data_full_byte_size;
			rt = uncompress((Bytef*)*data,(uLongf*)&_tmp_data_full_byte_size,(Bytef*)cur_buf,dlen);
			if(Z_OK != rt)
			{
				rclog << lock << "ERROR: uncompress return "<< rt << " . "<< unlock;
				BHASSERT(0, "ERROR: uncompress return != Z_OK .");
			}
            data_full_byte_size = _tmp_data_full_byte_size;
			#endif
		}
	}

	for(uint tmp_id = 0, id = 0; id < no_obj; id++) {
		if(!IsNull(id) && GetSize(id) != 0)
			index[id] = (uchar*) tmp_index[tmp_id++];
		else {
			SetSize(id, 0);
			index[id] = 0;
		}
		if(optimal_mode == 0)
			tmp_id++;
	}

	dealloc(compressed_buf);
	compressed_buf=0;
	comp_buf_size=0;
#ifdef FUNCTIONS_EXECUTION_TIMES
	SizeOfUncompressedDP += (1 + rc_msize(*data) + rc_msize(nulls) + rc_msize(lens) + rc_msize(index));
#endif
}

CompressionStatistics AttrPackS_Zip::Compress(DomainInjectionManager& dim)
{
	if (!use_already_set_decomposer) {
		if (dim.HasCurrent()) {
			ver = 8;
			decomposer_id = dim.GetCurrentId();
		} else {
			ver = 0;
			decomposer_id = 0;
		}
	}

	switch (ver) {
		case 0:
			return CompressOld();
		case 8:
			return Compress8(dim.Get(decomposer_id));
		default:
			rclog << lock << "ERROR: wrong version of data pack format" << unlock;
			BHASSERT(0, "ERROR: wrong version of data pack format");
			break;
	}
	return CompressionStatistics();
}

CompressionStatistics AttrPackS_Zip::Compress8(DomainInjectionDecomposer& decomposer)
{
	MEASURE_FET("AttrPackS_Zip::Compress()");
#ifdef FUNCTIONS_EXECUTION_TIMES
	std::stringstream s1;
	s1 << "aS[" << pc_column( GetPackCoordinate() ) << "].Compress(...)";
	FETOperator fet1(s1.str());
#endif

	//SystemInfo si;
	//uint nopf_start = SystemInfo::NoPageFaults();
	//uint nopf_end = 0;

	//////////////////////////////////////////////////////////////////////////////////////////////////
	// Compress nulls:
	comp_len_buf_size = comp_null_buf_size = comp_buf_size = 0;	
	MMGuard<uchar> comp_null_buf;
	if(no_nulls > 0) 
	{
		if(ShouldNotCompress()) 
		{
			comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
			comp_null_buf_size = ((no_obj + 7) / 8);
			ResetModeNullsCompressed();
		} 
		else 
		{
			comp_null_buf_size = ((no_obj + 7) / 8);
			comp_null_buf = MMGuard<uchar>((uchar*)alloc((comp_null_buf_size + 2) * sizeof(uchar), BLOCK_TEMPORARY), *this);

			uint cnbl = comp_null_buf_size + 1;
			comp_null_buf[cnbl] = 0xBA; // just checking - buffer overrun

			//>> Begin : modify by liujs
			#if 0
			BitstreamCompressor bsc;
			if(CPRS_ERR_BUF == bsc.Compress((char*) comp_null_buf.get(), comp_null_buf_size, (char*) nulls, no_obj, no_nulls)) 
			{
				if(comp_null_buf[cnbl] != 0xBA) 
				{
					rclog << lock << "ERROR: buffer overrun by BitstreamCompressor (T f)." << unlock;
					BHASSERT(0, "ERROR: buffer overrun by BitstreamCompressor (T f)!");
				}

				comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
				comp_null_buf_size = ((no_obj + 7) / 8);
				ResetModeNullsCompressed();
			} else 
			{
				SetModeNullsCompressed();
				if(comp_null_buf[cnbl] != 0xBA) 
				{
					rclog << lock << "ERROR: buffer overrun by BitstreamCompressor (T)." << unlock;
					BHASSERT(0, "ERROR: buffer overrun by BitstreamCompressor (T f)!");
				}
			}
			#else
            int rt = 0;
			uLongf _tmp_comp_null_buf_size= comp_null_buf_size;
			rt = compress2((Bytef*)comp_null_buf.get(),(uLong*)&_tmp_comp_null_buf_size,(Bytef*)nulls,no_nulls*sizeof(char),1);
            if( Z_OK != rt )
			{
				if(comp_null_buf[cnbl] != 0xBA) 
				{
					rclog << lock << "ERROR: compress2 return "<< rt << unlock;
					BHASSERT(0, "ERROR: compress2 return != Z_OK!");
				}

				comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
				comp_null_buf_size = ((no_obj + 7) / 8);
				ResetModeNullsCompressed();
			} 
			else 
			{
			    comp_null_buf_size = _tmp_comp_null_buf_size;
				SetModeNullsCompressed();
				if(comp_null_buf[cnbl] != 0xBA) 
				{
					rclog << lock << "ERROR: buffer overrun by compress2 ." << unlock;
					BHASSERT(0, "ERROR: buffer overrun by compress2 !");
				}
			}
			#endif
			//<< end : modify by liujs
		}
	}
	comp_buf_size += (comp_null_buf_size > 0 ? 2 + comp_null_buf_size : 0);


	//>> Begin: modify by liujs , DataBlock �����ڲ���ѹ����ѹ����Ϊ�ڲ�ʵ���㷨���������ԭ��ʽ
	//>> ԭ��ʽѹ����ѹ��: ���ַ��������жϳ��������ַ�����ѡ��ֵ���ַ����ֿ�ѹ����ѹ�����鷳!
	//>> �����޸ģ��������ϴ󣬹ʲ������޸ģ�ֱ���޸�
#if 0
	// Decomposition
	StringDataBlock wholeData(no_obj-no_nulls);

	// add non null object 
	for(uint o = 0; o < no_obj; o++)
		if(!IsNull(o))
			wholeData.Add(GetVal(o), GetSize(o));

	std::vector<boost::shared_ptr<DataBlock> > decomposedData;
	CompressionStatistics stats;
	stats.previous_no_obj = previous_no_obj;
	stats.new_no_outliers = stats.previous_no_outliers = outliers;

	// call function : Concatenator::Decompose
	decomposer.Decompose(wholeData, decomposedData, stats);
	outliers = stats.new_no_outliers;

	no_groups = ushort(decomposedData.size());
	comp_buf_size += 5 * no_groups; // types of blocks conversion + size of compressed block
	MMGuard<uint> comp_buf_size_block((uint*)alloc(no_groups * sizeof(uint), BLOCK_TEMPORARY), *this);

	// Compression
	//> ����ط�ʵ�ֱȽ�ϸ�ˣ���������������ѹ������ֵ���ַ����Ƿֿ�ѹ����
	for(int b = 0; b < no_groups; b++) {
		decomposedData[b]->Compress(comp_buf_size_block.get()[b]);
		comp_buf_size += comp_buf_size_block.get()[b];
		// TODO: what about setting comp_len_buf_size???
	}
    #else
    //>> Begin: add by liujs
    
	//////////////////////////////////////////////////////////////////////////////////////////////////
	// Compress len:
	MMGuard<uint> comp_len_buf;
	NumCompressor<uint> nc(ShouldNotCompress());
	MMGuard<uint> nc_buffer((uint*)alloc((1 << 16) * sizeof(uint), BLOCK_TEMPORARY), *this);

	int onn = 0;
	uint maxv = 0;
	uint cv = 0;
	for(uint o = 0; o < no_obj; o++) 
	{
		if(!IsNull(o)) 
		{
			cv = GetSize(o);
			*(nc_buffer.get() + onn++) = cv;
			if(cv > maxv)
				maxv = cv;
		}
	}

	if(maxv != 0) 
	{
		comp_len_buf_size = onn * sizeof(uint) + 128 + 8;
		comp_len_buf = MMGuard<uint>((uint*)alloc(comp_len_buf_size / 4 * sizeof(uint), BLOCK_TEMPORARY), *this);
		uLongf _tmp_comp_len_buf_size = comp_len_buf_size - 8;
		
		int rt = 0;
		rt = compress2((Bytef*)(comp_len_buf.get()+2), (uLongf *)&_tmp_comp_len_buf_size, (Bytef*) nc_buffer.get(), onn * sizeof(uint),1);
		if(Z_OK != rt)
		{
			rclog << lock << "ERROR: compress2 return = "<< rt <<" . "<< unlock;
			BHASSERT(0, "ERROR: compress2 return != Z_OK.");
		}
        
		comp_len_buf_size = _tmp_comp_len_buf_size + 8;
	} 
	else 
	{
		comp_len_buf_size = 8;
		comp_len_buf = MMGuard<uint>((uint*) alloc(sizeof(uint) * 2, BLOCK_TEMPORARY), *this);
	}

	*comp_len_buf.get() = comp_len_buf_size;
	*(comp_len_buf.get() + 1) = maxv;


	//////////////////////////////////////////////////////////////////////////////////////////////////
	// Compress non data:
	MMGuard<char*> tmp_index((char**)alloc(no_obj * sizeof(char*), BLOCK_TEMPORARY), *this);
	MMGuard<ushort> tmp_len((ushort*)alloc(no_obj * sizeof(ushort), BLOCK_TEMPORARY), *this);
	
	int nid = 0;
	
	uLong _combination_buf_len = 0; 	
	uLong _combination_pos = 0;
	
	int dlen = data_full_byte_size + 128;
	MMGuard<char> comp_buf((char*)alloc(dlen * sizeof(char), BLOCK_TEMPORARY), *this);
	if(data_full_byte_size) 
	{		

		for(int id = 0; id < (int) no_obj; id++)
		{
			if(!IsNull(id) && GetSize(id) != 0)
			{
				tmp_index[nid] = GetVal(id);
				tmp_len[nid++] = GetSize(id);
		
				_combination_buf_len += GetSize(id);
			}
		}

		// get natural data  buffer
		MMGuard<char*> tmp_combination_index((char**)alloc(_combination_buf_len + 1, BLOCK_TEMPORARY), *this);
		tmp_combination_index[_combination_buf_len] = '\0';
		
		for(int _i=0;_i<nid;_i++)
		{
			// merge the combination_index data
			memcpy((tmp_combination_index.get()+_combination_pos),tmp_index[_i],tmp_len[_i]);
			_combination_pos += tmp_len[_i];
		}
		
		int rt = 0;
		uLongf _tmp_dlen = dlen;
		rt = compress2((Bytef*)(comp_buf.get()),(uLongf*)&_tmp_dlen,(Bytef*)tmp_combination_index.get(),_combination_buf_len,1);
		if( Z_OK != rt );
		{ 
			rclog << lock << "ERROR: compress2 return = "<< rt <<" . "<< unlock;
			BHASSERT(0, "ERROR: compress2 return != Z_OK.");			   
		}		
		dlen = _tmp_dlen;
	}
	else
	{
        dlen = 0;
	}
	//<< End: add by liujs

	#endif
	// Allocate and fill the compressed buffer
	comp_buf_size = (comp_null_buf_size > 0 ? 2 + comp_null_buf_size : 0) + comp_len_buf_size + 4 + dlen;

	MMGuard<uchar> new_compressed_buf((uchar*) alloc(comp_buf_size * sizeof(uchar), BLOCK_COMPRESSED), *this);
	char* p = (char*)new_compressed_buf.get();

	////////////////////////////////	Nulls
	if(no_nulls > 0) {
		*((ushort*) p) = (ushort) comp_null_buf_size;
		p += 2;
		memcpy(p, comp_null_buf.get(), comp_null_buf_size);
		p += comp_null_buf_size;
	}

	////////////////////////////////	Informations about blocks
	//>> Begin: modify by lius
	//>> deal non null blocks
    #if 0
	for(int b = 0; b < no_groups; b++)
		*(p++) = decomposedData[b]->GetType();		// types of block's compression
	memcpy(p, comp_buf_size_block.get(), no_groups * sizeof(uint)); // compressed block sizes
	p += no_groups * sizeof(uint);
	
	for(int b = 0; b < no_groups; b++) {
		decomposedData[b]->StoreCompressedData(p, comp_buf_size_block.get()[b]);
		p += comp_buf_size_block.get()[b];
	}
    #else
	
    // Lens
	if(comp_len_buf_size)
		memcpy(p, comp_len_buf.get(), comp_len_buf_size);

	p += comp_len_buf_size;

    // non null object len
	*((int*) p) = dlen;
	p += sizeof(int);

	// non null object compressed buf
	if(dlen)
		memcpy(p, comp_buf.get(), dlen);
	
    //<< End: modify by liujs
	#endif	
    
	dealloc(compressed_buf);
	compressed_buf = new_compressed_buf.release();

	SetModeDataCompressed();
	compressed_up_to_date = true;

	CompressionStatistics stats;
	stats.previous_no_obj = previous_no_obj;
	stats.new_no_obj = NoObjs();
	return stats;
}

CompressionStatistics AttrPackS_Zip::CompressOld()
{
#ifdef FUNCTIONS_EXECUTION_TIMES
	std::stringstream s1;
	s1 << "aS[" << pc_column( GetPackCoordinate() ) << "].Compress(...)";
	FETOperator fet1(s1.str());
#endif

	//SystemInfo si;
	//uint nopf_start = SystemInfo::NoPageFaults();
	//uint nopf_end = 0;

	//////////////////////////////////////////////////////////////////////////////////////////////////
	// Compress nulls:

	comp_len_buf_size = comp_null_buf_size = comp_buf_size = 0;
	MMGuard<uchar> comp_null_buf;
	if(no_nulls > 0) 
	{
		if(ShouldNotCompress()) 
		{
			comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
			comp_null_buf_size = ((no_obj + 7) / 8);
			ResetModeNullsCompressed();
		} else {
			comp_null_buf_size = ((no_obj + 7) / 8);
			comp_null_buf = MMGuard<uchar>((uchar*)alloc((comp_null_buf_size + 2) * sizeof(uchar), BLOCK_TEMPORARY), *this);

			uint cnbl = comp_null_buf_size + 1;
			comp_null_buf[cnbl] = 0xBA; // just checking - buffer overrun

            //>> Begin: modify by liujs
            #if 0			
			BitstreamCompressor bsc;
			if(CPRS_ERR_BUF == bsc.Compress((char*) comp_null_buf.get(), comp_null_buf_size, (char*) nulls, no_obj, no_nulls)) {
				if(comp_null_buf[cnbl] != 0xBA) {
					rclog << lock << "ERROR: buffer overrun by BitstreamCompressor (T f)." << unlock;
					BHASSERT(0, "ERROR: buffer overrun by BitstreamCompressor (T f)!");
				}

				comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
				comp_null_buf_size = ((no_obj + 7) / 8);
				ResetModeNullsCompressed();
			} else {
				SetModeNullsCompressed();
				if(comp_null_buf[cnbl] != 0xBA) {
					rclog << lock << "ERROR: buffer overrun by BitstreamCompressor (T)." << unlock;
					BHASSERT(0, "ERROR: buffer overrun by BitstreamCompressor (T f)!");
				}
			}
		    #else
			uLongf _tmp_comp_null_buf_size = comp_null_buf_size;			
			if(Z_OK != compress2((Bytef*)comp_null_buf.get(), (uLongf *)&_tmp_comp_null_buf_size, (Bytef*) nulls, no_nulls * sizeof(char),1))
			{
				if(comp_null_buf[cnbl] != 0xBA) 
				{
					rclog << lock << "ERROR: buffer overrun by compress2 ." << unlock;
					BHASSERT(0, "ERROR: buffer overrun by compress2 .");
				}
			
				comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
				comp_null_buf_size = ((no_obj + 7) / 8);
				ResetModeNullsCompressed();
			}
			else 
			{
			    comp_null_buf_size=_tmp_comp_null_buf_size;
				SetModeNullsCompressed();
				if(comp_null_buf[cnbl] != 0xBA)
				{
					rclog << lock << "ERROR: buffer overrun by  compress2 (N)." << unlock;
					BHASSERT(0, "ERROR: buffer overrun by compress2 (N f).");
				}
			}
		    #endif
			//<< End: modify by liujs
		}		
	}


	//////////////////////////////////////////////////////////////////////////////////////////////////
	// Compress len:
	MMGuard<uint> comp_len_buf;

	NumCompressor<uint> nc(ShouldNotCompress());
	MMGuard<uint> nc_buffer((uint*)alloc((1 << 16) * sizeof(uint), BLOCK_TEMPORARY), *this);

	int onn = 0;
	uint maxv = 0;
	uint cv = 0;
	for(uint o = 0; o < no_obj; o++) {
		if(!IsNull(o)) {
			cv = GetSize(o);
			*(nc_buffer.get() + onn++) = cv;
			if(cv > maxv)
				maxv = cv;
		}
	}

	if(maxv != 0) 
	{
		comp_len_buf_size = onn * sizeof(uint) + 128 + 8;
		comp_len_buf = MMGuard<uint>((uint*)alloc(comp_len_buf_size / 4 * sizeof(uint), BLOCK_TEMPORARY), *this);

		//>> Begin: modify by liujs
		#if 0
		nc.Compress((char*)(comp_len_buf.get() + 2), tmp_comp_len_buf_size, nc_buffer.get(), onn, maxv);
		#else
		int rt = 0;		
		uLongf _tmp_comp_len_buf_size = comp_len_buf_size - 8;
		rt = compress2((Bytef*)(comp_len_buf.get()+2), (uLongf *)&_tmp_comp_len_buf_size, (Bytef*) nc_buffer.get(), onn * sizeof(uint),1);
		if(Z_OK != rt)
		{
			rclog << lock << "ERROR: compress2 return = "<< rt <<" . "<< unlock;
			BHASSERT(0, "ERROR: compress2 return != Z_OK.");
		}
		#endif
        //<< end: modify by liujs
        
		comp_len_buf_size = _tmp_comp_len_buf_size + 8; // 8 = sizeof(comp_len_buf_size) + sizeof(maxv)
	} 
	else 
	{
		comp_len_buf_size = 8;
		comp_len_buf = MMGuard<uint>((uint*) alloc(sizeof(uint) * 2, BLOCK_TEMPORARY), *this);
	}

	*comp_len_buf.get() = comp_len_buf_size;
	*(comp_len_buf.get() + 1) = maxv;


	//////////////////////////////////////////////////////////////////////////////////////////////////
	// Compress non nulls:
	//TextCompressor tc;
	int zlo = 0;
	for(uint obj = 0; obj < no_obj; obj++)
		if(!IsNull(obj) && GetSize(obj) == 0)
			zlo++;
	int dlen = data_full_byte_size + 128;
	MMGuard<char> comp_buf((char*)alloc(dlen * sizeof(char), BLOCK_TEMPORARY), *this);

	if(data_full_byte_size) 
	{
		int objs = (no_obj - no_nulls) - zlo;

		MMGuard<char*> tmp_index((char**)alloc(objs * sizeof(char*), BLOCK_TEMPORARY), *this);
		MMGuard<ushort> tmp_len((ushort*)alloc(objs * sizeof(ushort), BLOCK_TEMPORARY), *this);

		int nid = 0;

		//>> Begin:natural data len , add by liujs 
		uLong _combination_buf_len = 0;		
		uLong _combination_pos = 0;
		//<< End ,add by liujs
		
		for(int id = 0; id < (int) no_obj; id++) {
			if(!IsNull(id) && GetSize(id) != 0) {
				tmp_index[nid] = (char*) index[id];
				tmp_len[nid++] = GetSize(id);

				//>> Begin: natural data len,add by liujs
                _combination_buf_len += GetSize(id);
				//<< End: add by liujs
			}
		}

        //>> Begin : modify by liujs
        #if 0
		tc.Compress(comp_buf.get(), dlen, tmp_index.get(), tmp_len.get(), objs, ShouldNotCompress() ? 0 : TextCompressor::VER);
		#else		
		// get natural data  buffer
		MMGuard<char*> tmp_combination_index((char**)alloc(_combination_buf_len + 1, BLOCK_TEMPORARY), *this);
		tmp_combination_index[_combination_buf_len] = '\0';

		for(int _i=0;_i<nid;_i++)
		{
			// merge the combination_index data
			memcpy((tmp_combination_index.get()+_combination_pos),tmp_index[_i],tmp_len[_i]);
			_combination_pos += tmp_len[_i];
		}

		int rt = 0;
		uLongf _tmp_dlen = dlen;
		rt = compress2((Bytef*)(comp_buf.get()),(uLong*)&_tmp_dlen,(Bytef*)tmp_combination_index.get(),_combination_buf_len,1);
        if( Z_OK != rt );
        { 
	  		rclog << lock << "ERROR: compress2 return = "<< rt <<" . "<< unlock;
    		BHASSERT(0, "ERROR: compress2 return != Z_OK.");               
        }			
		dlen = _tmp_dlen;
		#endif
        //<< End:modify by liujs
	}
	else
	{
		dlen = 0;
	}

    // caculate all compressed buff size
	comp_buf_size = (comp_null_buf_size > 0 ? 2 + comp_null_buf_size : 0) + comp_len_buf_size + 4 + dlen;

	MMGuard<uchar> new_compressed_buf((uchar*) alloc(comp_buf_size * sizeof(uchar), BLOCK_COMPRESSED), *this);
	uchar* p = new_compressed_buf.get();

	// Nulls
	if(no_nulls > 0) {
		*((ushort*) p) = (ushort) comp_null_buf_size;
		p += 2;
		memcpy(p, comp_null_buf.get(), comp_null_buf_size);
		p += comp_null_buf_size;
	}

	// Lens
	if(comp_len_buf_size)
		memcpy(p, comp_len_buf.get(), comp_len_buf_size);

	p += comp_len_buf_size;

    // non null object len
	*((int*) p) = dlen;
	p += sizeof(int);

	// non null object compressed buf
	if(dlen)
		memcpy(p, comp_buf.get(), dlen);

	dealloc(compressed_buf);

	// get compressed buf
	compressed_buf = new_compressed_buf.release();

	SetModeDataCompressed();
	compressed_up_to_date = true;
	CompressionStatistics stats;
	stats.previous_no_obj = previous_no_obj;
	stats.new_no_obj = NoObjs();
	return stats;
}


