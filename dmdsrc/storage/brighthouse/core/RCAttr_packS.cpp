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
//#include <iostream>
//print protobuf debug string,for app linked with release version of protobuf

std::string PBDebugString(void *mess) {
	::google::protobuf::Message *msg=(::google::protobuf::Message *)mess;
	return msg->DebugString();
}

std::string PBShortDebugString(void *mess) {
	::google::protobuf::Message *msg=(::google::protobuf::Message *)mess;
	return msg->ShortDebugString();
}

AttrPackS::AttrPackS(PackCoordinate pc, AttributeType attr_type, int inserting_mode, bool no_compression, DataCache* owner)
	:	AttrPack(pc, attr_type, inserting_mode, no_compression, owner), ver(0), previous_no_obj(0), data(0), index(0), lens(0), decomposer_id(0), use_already_set_decomposer(false), outliers(0)
{
	Construct();
}

AttrPackS::AttrPackS(const AttrPackS &aps)
	:	AttrPack(aps), ver(aps.ver), previous_no_obj(aps.previous_no_obj), data(0), index(0), lens(0), decomposer_id(aps.decomposer_id), use_already_set_decomposer(aps.use_already_set_decomposer), outliers(aps.outliers)
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

std::auto_ptr<AttrPack> AttrPackS::Clone() const
{
	return std::auto_ptr<AttrPack>(new AttrPackS(*this));
}

void AttrPackS::TruncateObj( int new_objs)
{
	if(no_obj>new_objs && no_nulls>0) {
		for(int i = new_objs; i < no_obj; i++) {
			if (CR_Class_NS::BMP_BITTEST(this->nulls, this->nulls_bytes, i)) {
				// reset the i-th bit of the table
				CR_Class_NS::BMP_BITCLEAR(this->nulls, this->nulls_bytes, i);
				no_nulls--;
	   		}
	   	}
	}
	no_obj = new_objs;
}

void AttrPackS::Prepare(int no_nulls)
{
	Lock();
	Construct();
	this->no_obj = no_nulls;
	for(int i = 0; i < no_nulls; i++)
		SetNull(i);						// no_nulls set inside
	is_empty = false;
	Unlock();
}

void AttrPackS::LoadData(IBStream* fcurfile,Descriptor *pdesc,int64 local_min,void *pf,int _objs)
//void AttrPackS::LoadData(IBRiakDataPackQueryParam *query_param_p)
{
#ifdef FUNCTIONS_EXECUTION_TIMES
	FETOperator feto("AttrPackS::LoadData(...)");
	NotifyDataPackLoad(GetPackCoordinate());
#endif
	Lock();
	//packload perf monitor
	mytimer tm_loadn,tm_preprocess,tm_get,tm_postprocess;
	tm_loadn.Start();
	tm_preprocess.Start();
	
	//cr_result.clear();
	is_cq=false;is_rq=false;
	try {
		Construct();
		std::string *pparam=NULL;
		std::string params;
		IBRiakDataPackQueryParam param;
		param.set_pack_type(IBRiakDataPackQueryParam::PACK_STRING);
		if(pdesc &&_logical_coord.co.pack[4]!=0) {
			is_cq=true;
			// we only support BETWEEN now
			MIIterator mit;// for dummy iterator
			RCBString v1, v2;
			IBRiakDataPackQueryParam_ExpField &exp=*param.mutable_expression();
			switch(pdesc->op) {
			case O_NOT_BETWEEN:
			case O_BETWEEN:
				if (pdesc->op == O_NOT_BETWEEN)
					exp.set_op_type(IBRiakDataPackQueryParam_ExpOPType_EOP_NBT);
				else
					exp.set_op_type(IBRiakDataPackQueryParam_ExpOPType_EOP_BT);
				pdesc->val1.vc->GetValueString(v1, mit);
				pdesc->val2.vc->GetValueString(v2, mit);
				exp.add_value_list_str(v1);
				exp.add_value_list_str(v2);
				break;
			case O_NOT_LIKE:
			case O_LIKE:
				if (pdesc->op == O_NOT_LIKE)
					exp.set_op_type(IBRiakDataPackQueryParam_ExpOPType_EOP_NLK);
				else
					exp.set_op_type(IBRiakDataPackQueryParam_ExpOPType_EOP_LK);
				pdesc->val1.vc->GetValueString(v1, mit);
				exp.add_value_list_str(v1);
				break;
			case O_NOT_IN :
			case O_IN :
				if (pdesc->op == O_NOT_IN)
					exp.set_op_type(IBRiakDataPackQueryParam_ExpOPType_EOP_NIN);
				else
					exp.set_op_type(IBRiakDataPackQueryParam_ExpOPType_EOP_IN);
				{
					MultiValColumn* mvc = static_cast<MultiValColumn*>(pdesc->val1.vc);
					for(MultiValColumn::Iterator it = mvc->begin(mit), endit = mvc->end(mit); it != endit; ++it) {
						RCBString vcs = it->GetString();
						exp.add_value_list_str(vcs.Value(),vcs.size());
					}
				}
				break;
			case O_LESS :
			case O_LESS_EQ :
			case O_MORE :
			case O_MORE_EQ :
				pdesc->val1.vc->GetValueString(v1, mit);
				exp.add_value_list_str(v1);
				switch (pdesc->op) {
				case O_LESS :
					exp.set_op_type(IBRiakDataPackQueryParam_ExpOPType_EOP_LT);
					break;
				case O_LESS_EQ :
					exp.set_op_type(IBRiakDataPackQueryParam_ExpOPType_EOP_LE);
					break;
				case O_MORE :
					exp.set_op_type(IBRiakDataPackQueryParam_ExpOPType_EOP_GT);
					break;
				case O_MORE_EQ :
					exp.set_op_type(IBRiakDataPackQueryParam_ExpOPType_EOP_GE);
					break;
				}
				break;
			}
		}
		int mapin_len=(_objs==0||_objs==65535)?8192:(_objs/8+1);
		const char *mapin=(const char *)pf;
		if(pf) {
			param.set_first_row(0);
			param.set_last_row(_objs==0?(no_obj-1):_objs);
		}
		
		// maybe both range and condition query
		if(_logical_coord.co.pack[3]!=0) {
			is_rq=true;
			param.set_pack_type(IBRiakDataPackQueryParam_PackType::IBRiakDataPackQueryParam_PackType_PACK_STRING);
			param.set_first_row(LocalRangeSt());
			param.set_last_row(LocalRangeEd());

			first_obj=LocalRangeSt();
			if(pf) {
				int stbytes=first_obj/8;
				mapin+=stbytes;
				mapin_len=LocalRangeEd()/8-stbytes+1;
			}
		}
		if(is_rq || is_cq) {
			if(mapin && is_cq) {
				param.set_cond_rowmap_in(mapin,mapin_len);
			}
		}
		if(is_rq&& !is_cq && param.first_row()==0 && param.last_row()==no_obj-1) {
			param.clear_first_row();
			param.clear_last_row();
		}
		param.SerializeToString(&params);
		pparam=&params;
		msgRCAttr_packS msg_pack;
		std::string riak_value;

		no_nulls = 0;
		optimal_mode = 0;

		std::string _riak_key = this->GetRiakKey();
		//packload perf monitor
		tm_preprocess.Stop();
		tm_get.Start();
#ifndef NDEBUG
		int ret=_riak_cluster->Get(RIAK_CLUSTER_DP_BUCKET_NAME, _riak_key, riak_value, pparam,checkTraceRiakNode(table_number,attr_number,pack_no));
#else
		int ret=_riak_cluster->Get(RIAK_CLUSTER_DP_BUCKET_NAME, _riak_key, riak_value, pparam);
#endif
		//packload perf monitor
		tm_get.Stop();
		tm_postprocess.Start();
		
		if (ret != 0) {
			param.clear_cond_rowmap_in();
			DPRINTF("Error: PackS [%s] load failed, ret [%s], msg [%s], \nparam => %s\n, mapin_len = %d\n",
				_riak_key.c_str(), CR_Class_NS::strerrno(ret), CR_Class_NS::strerror(ret),
				param.DebugString().c_str(), (mapin)?(mapin_len):(0)
			);
			std::string errmsg;
			char errmsg_buf[256];
			snprintf(errmsg_buf, sizeof(errmsg_buf),
				"PackS [%s] load failed, ret [%s], msg [%s]",
				_riak_key.c_str(), CR_Class_NS::strerrno(ret), CR_Class_NS::strerror(ret)
			);
			errmsg = errmsg_buf;
			rclog << lock << errmsg << unlock;
			throw DatabaseRCException(errmsg);
		}

		if(RIAK_OPER_LOG>=2){
			DPRINTF("PackS::LoadData[T%08d-A%08d-P%08d], riakkey = %s , value_size = %d\n",
			    this->table_number, this->attr_number, this->pack_no,
			    _riak_key.c_str(), (int)(riak_value.size()));
		}

		if (!msg_pack.ParseFromString(riak_value)) {
			DPRINTF("Parse error at:%s, value_size=%d\n", _riak_key.c_str(), (int)(riak_value.size()));
			char errmsg_buf[128];
			snprintf(errmsg_buf,sizeof(errmsg_buf),"Parse error at:%s, value_size=%d\n", _riak_key.c_str(), (int)(riak_value.size()));
			throw DatabaseRCException(errmsg_buf);	
		}

		no_obj = msg_pack.no_obj();
		previous_no_obj = ++no_obj;
		no_nulls = msg_pack.no_nulls();
		previous_no_obj -= no_nulls;
		ver = msg_pack.ver();
		optimal_mode = msg_pack.optimal_mode();
		decomposer_id = msg_pack.decomposer_id();
		no_groups = msg_pack.no_groups();
		data_full_byte_size = msg_pack.data_full_byte_size();
		if(msg_pack.has_first_row() && msg_pack.first_row()<0) {
			if(pdesc==NULL) {
				char errmsg_buf[128];
				snprintf(errmsg_buf,sizeof(errmsg_buf),"Get a corrupt pack(key:%s),first_row=%d,attr_number=%d.\n", _riak_key.c_str(), msg_pack.first_row(),attr_number);
				throw DatabaseRCException(errmsg_buf);
			}
		}
		else
			LoadUncompressed(msg_pack);

		max_no_obj = no_obj;
		is_empty = false;
#ifdef FUNCTIONS_EXECUTION_TIMES
		NoBytesReadByDPs += total_size;
#endif
	} catch (DatabaseRCException&) {
		Unlock();
		throw;
	}
	//packload perf monitor
	tm_postprocess.Stop();
	tm_loadn.Stop();
	static int loadstatct=0;
	static double pack_load=0,pack_pre=0,pack_get=0,pack_post=0;
	pack_load+=tm_loadn.GetTime();
	pack_pre+=tm_preprocess.GetTime();
	pack_get+=tm_get.GetTime();
	pack_post+=tm_postprocess.GetTime();
	loadstatct++;
	if(loadstatct==200) {
		if(rccontrol.isOn() && RIAK_OPER_LOG>=1) {
			rclog << lock << "PackS Read stat for last 200 packs: t"
			<<F3S(pack_load)<<"s,b"
			<<F4MS(pack_pre)<<"ms,g"
			<<F3S(pack_get)<<"s,p"
			<<F4MS(pack_post)<<"ms."
			<<unlock;
		}
		loadstatct=0;
		pack_load=pack_pre=pack_get=pack_post=0;
	}

	Unlock();
}

void AttrPackS::Destroy()
{
	if(data) {
		int ds = (int) rc_msize(data) / sizeof(char*);
		for (int i = 0; i < ds && i < data_id; i++) {
			dealloc(data[i]);
			data[i] = 0;
		}
		dealloc(data);
		data = 0;
	}

	dealloc(index);
	index = 0;

	dealloc(lens);
	lens = 0;
	is_empty = true;

	dealloc(nulls);
	dealloc(compressed_buf);
	compressed_buf = 0;
	nulls = NULL;
	Instance()->AssertNoLeak(this);
	nulls_bytes=0;
	//BHASSERT(m_sizeAllocated == 0, "TrackableObject accounting failure");
}

AttrPackS::~AttrPackS()
{
	DestructionLock();
	Destroy();
}

void AttrPackS::Construct()
{
	data = 0;
	index = 0;
	lens = 0;
	max_no_obj = no_obj = 0;
	compressed_up_to_date = false;
	saved_up_to_date = false;
	no_groups = 1;

	if(attr_type == RC_BIN)
		len_mode = sizeof(uint);
	else
		len_mode = sizeof(ushort);
	//cout << "len_mode = " << len_mode << endl;

	last_copied = -1;
	binding = false;
	nulls = NULL;
	no_nulls = 0;
	nulls_bytes =0;
	data_full_byte_size = 0;
	compressed_buf = 0;
	data_id = 0;
	optimal_mode = 0;
	comp_buf_size = comp_null_buf_size = comp_len_buf_size = 0;
}

void AttrPackS::Expand(int no_obj)
{
	BHASSERT(!is_only_compressed, "The pack is compressed!");
	BHASSERT(this->no_obj + no_obj <= MAX_NO_OBJ, "Expression evaluation failed on MAX_NO_OBJ!");

	uint new_max_no_obj = this->no_obj + no_obj;
	index = (uchar**)rc_realloc(index, new_max_no_obj * sizeof(uchar*), BLOCK_UNCOMPRESSED);
	lens = rc_realloc(lens, len_mode * new_max_no_obj * sizeof(char), BLOCK_UNCOMPRESSED);

	memset((char*)lens + (this->no_obj * len_mode), 0, (no_obj * len_mode));

	int cs = data ? (int)rc_msize(data) : 0;
	data = (uchar**)rc_realloc(data, cs + (no_obj * sizeof(uchar*)), BLOCK_UNCOMPRESSED);

	memset((char*)data + cs, 0, no_obj * sizeof(uchar*));
	cs /= sizeof(uchar*);

	for(uint o = this->no_obj; o < new_max_no_obj; o++)
		index[o] = data[cs++];

	max_no_obj = new_max_no_obj;
	if(nulls && nulls_bytes!=8192) {
		uint8_t *newnulls = (uint8_t*) alloc(8192, BLOCK_UNCOMPRESSED);
		memset(newnulls, 0, 8192);
		memcpy(newnulls,nulls,nulls_bytes);
		dealloc(nulls);
		nulls=newnulls;
		nulls_bytes=8192;
	}
}

void AttrPackS::BindValue(bool null, uchar* value, uint size)
{
	is_empty = false;
	if(!binding) {
		binding = true;
		last_copied = (int) no_obj - 1;
	}

	if(null) {
		SetNull(no_obj);
		index[no_obj] = 0;
	} else if(size == 0) {
		SetSize(no_obj, 0);
		index[no_obj] = 0;
	} else {
		data_full_byte_size += size;
		SetSize(no_obj, size);
		index[no_obj] = value;
	}
	no_obj++;
	compressed_up_to_date = false;
}

void AttrPackS::CopyBinded()
{
	BHASSERT(!is_only_compressed, "The pack is compressed!");
	if(binding) {
		int size_sum = 0;
		for(uint i = last_copied + 1; i < no_obj; i++)
			size_sum += GetSize(i);
		if(size_sum > 0) {
			data[data_id] = (uchar*)alloc(size_sum, BLOCK_UNCOMPRESSED);
			size_sum = 0;
			for(uint i = last_copied + 1; i < no_obj; i++) {
				int size = GetSize(i);
				if(!IsNull(i) && size != 0) {
					uchar* value = (uchar*&)index[i];
					memcpy(data[data_id] + size_sum, value, size);
					index[i] = data[data_id] + size_sum;
					size_sum += size;
				} else
					index[i] = 0;
			}
		} else
			data[data_id] = 0;
		data_id++;
	}
	last_copied = no_obj -1;
	binding = false;
}

void AttrPackS::SetSize(int ono, uint size)
{
	BHASSERT(!is_only_compressed, "The pack is compressed!");
	if(len_mode == sizeof(ushort))
		((ushort*)lens)[ono-first_obj] = (ushort)size;
	else
		((uint*)lens)[ono-first_obj] = (uint)size;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////

void AttrPackS::Uncompress(DomainInjectionManager& dim)
{
	return;

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

void AttrPackS::AllocNullsBuffer()
{
	if(no_nulls > 0) {
		if(!nulls)
			nulls = (uint8_t*)alloc(8192, BLOCK_UNCOMPRESSED);
		if(no_obj < 65536)
			memset(nulls, 0, 8192);
		nulls_bytes=8192;
	}
}

void AttrPackS::AllocBuffersButNulls()
{
	if(data == NULL) {
		data = (uchar**) alloc(sizeof(uchar*), BLOCK_UNCOMPRESSED);
		*data = 0;
		if(data_full_byte_size) {
			*data = (uchar*) alloc(data_full_byte_size, BLOCK_UNCOMPRESSED);
			if(!*data)
				rclog << lock << "Error: out of memory (" << data_full_byte_size << " bytes failed). (40)" << unlock;
		}
		data_id = 1;
	}
	assert(!lens && !index);
	lens = alloc(len_mode * no_obj * sizeof(char), BLOCK_UNCOMPRESSED);
	index = (uchar**)alloc(no_obj * sizeof(uchar*), BLOCK_UNCOMPRESSED);
}

void AttrPackS::Uncompress8(DomainInjectionManager& dim)
{
	DomainInjectionDecomposer& decomposer = dim.Get(decomposer_id);	
	AllocNullsBuffer();
	uint *cur_buf = (uint*) compressed_buf;
	// decode nulls
	uint null_buf_size = 0;
	if(no_nulls > 0) {
		null_buf_size = (*(ushort*) cur_buf);
		if(!IsModeNullsCompressed()) // flat null encoding
			memcpy(nulls, (char*) cur_buf + 2, null_buf_size);
		else {
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

void AttrPackS::UncompressOld()
{
#ifdef FUNCTIONS_EXECUTION_TIMES
	FETOperator feto("AttrPackS::Uncompress()");
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
	if(no_nulls>0)
		nulls_bytes=8192;
	AllocBuffers();
	MMGuard<char*> tmp_index((char**)alloc(no_obj * sizeof(char*), BLOCK_TEMPORARY), *this);
	////////////////////////////////////////////////////////
	int i; //,j,obj_317,no_of_rep,rle_bits;
	uint *cur_buf = (uint*) compressed_buf;

	///////////////////////////////////////////////////////
	// decode nulls
	char (*FREE_PLACE)(reinterpret_cast<char*> (-1));

	uint null_buf_size = 0;
	if(no_nulls > 0) {
		null_buf_size = (*(ushort*) cur_buf);
		if(!IsModeNullsCompressed()) // flat null encoding
			memcpy(nulls, (char*) cur_buf + 2, null_buf_size);
		else {
			BitstreamCompressor bsc;
			bsc.Decompress((char*) nulls, null_buf_size, (char*) cur_buf + 2, no_obj, no_nulls);
			// For tests:
			uint nulls_counted = 0;
			for(i = 0; i < 2048; i++)
				nulls_counted += CalculateBinSum(nulls[i]);
			if(no_nulls != nulls_counted)
				rclog << lock << "Error: AttrPackT::Uncompress uncompressed wrong number of nulls." << unlock;
		}
		cur_buf = (uint*) ((char*) cur_buf + null_buf_size + 2);
		for(i = 0; i < (int) no_obj; i++) {
			if(IsNull(i))
				tmp_index[i] = NULL; // i.e. nulls
			else
				tmp_index[i] = FREE_PLACE; // special value: an object awaiting decoding
		}
	} else
		for(i = 0; i < (int) no_obj; i++)
			tmp_index[i] = FREE_PLACE;

	///////////////////////////////////////////////////////
	//	<null_buf_size><nulls><lens><char_lib_size><huffmann_size><huffmann><rle_bits><obj>...<obj>
	if(optimal_mode == 0) {
		rclog << lock << "Error: Compression format no longer supported." << unlock;
	} else {
		comp_len_buf_size = *cur_buf;
		if((_uint64) * (cur_buf + 1) != 0) {
			NumCompressor<uint> nc;
			MMGuard<uint> cn_ptr((uint*) alloc((1 << 16) * sizeof(uint), BLOCK_TEMPORARY), *this);
			nc.Decompress(cn_ptr.get(), (char*) (cur_buf + 2), comp_len_buf_size - 8, no_obj - no_nulls, *(uint*) (cur_buf + 1));

			int oid = 0;
			for(uint o = 0; o < no_obj; o++)
				if(!IsNull(int(o)))
					SetSize(o, (uint) cn_ptr[oid++]);
		} else {
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
			if(!IsNull(obj) && GetSize(obj) == 0)
				zlo++;
		int objs = no_obj - no_nulls - zlo;

		if(objs) {
			MMGuard<ushort> tmp_len((ushort*) alloc(objs * sizeof(ushort), BLOCK_TEMPORARY), *this);
			for(uint tmp_id = 0, id = 0; id < no_obj; id++)
				if(!IsNull(id) && GetSize(id) != 0)
					tmp_len[tmp_id++] = GetSize(id);
			tc.Decompress((char*)*data, data_full_byte_size, (char*) cur_buf, dlen, tmp_index.get(), tmp_len.get(), objs);
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

CompressionStatistics AttrPackS::Compress(DomainInjectionManager& dim)
{
	return CompressionStatistics();

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

CompressionStatistics AttrPackS::Compress8(DomainInjectionDecomposer& decomposer)
{
	MEASURE_FET("AttrPackS::Compress()");
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
	if(no_nulls > 0) {
		if(ShouldNotCompress()) {
			comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
			comp_null_buf_size = ((no_obj + 7) / 8);
			ResetModeNullsCompressed();
		} else {
			comp_null_buf_size = ((no_obj + 7) / 8);
			comp_null_buf = MMGuard<uchar>((uchar*)alloc((comp_null_buf_size + 2) * sizeof(uchar), BLOCK_TEMPORARY), *this);

			uint cnbl = comp_null_buf_size + 1;
			comp_null_buf[cnbl] = 0xBA; // just checking - buffer overrun
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
		}
	}
	comp_buf_size += (comp_null_buf_size > 0 ? 2 + comp_null_buf_size : 0);

	// Decomposition
	StringDataBlock wholeData(no_obj-no_nulls);
	for(uint o = 0; o < no_obj; o++)
		if(!IsNull(o))
			wholeData.Add(GetVal(o), GetSize(o));
	std::vector<boost::shared_ptr<DataBlock> > decomposedData;
	CompressionStatistics stats;
	stats.previous_no_obj = previous_no_obj;
	stats.new_no_outliers = stats.previous_no_outliers = outliers;
	decomposer.Decompose(wholeData, decomposedData, stats);
	outliers = stats.new_no_outliers;

	no_groups = ushort(decomposedData.size());
	comp_buf_size += 5 * no_groups; // types of blocks conversion + size of compressed block
	MMGuard<uint> comp_buf_size_block((uint*)alloc(no_groups * sizeof(uint), BLOCK_TEMPORARY), *this);

	// Compression
	for(int b = 0; b < no_groups; b++) {
		decomposedData[b]->Compress(comp_buf_size_block.get()[b]);
		comp_buf_size += comp_buf_size_block.get()[b];
		// TODO: what about setting comp_len_buf_size???
	}

	// Allocate and fill the compressed buffer
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
	for(int b = 0; b < no_groups; b++)
		*(p++) = decomposedData[b]->GetType();		// types of block's compression
	memcpy(p, comp_buf_size_block.get(), no_groups * sizeof(uint)); // compressed block sizes
	p += no_groups * sizeof(uint);
	
	for(int b = 0; b < no_groups; b++) {
		decomposedData[b]->StoreCompressedData(p, comp_buf_size_block.get()[b]);
		p += comp_buf_size_block.get()[b];
	}

	dealloc(compressed_buf);
	compressed_buf = new_compressed_buf.release();

	SetModeDataCompressed();
	compressed_up_to_date = true;
	return stats;
}

CompressionStatistics AttrPackS::CompressOld()
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
	if(no_nulls > 0) {
		if(ShouldNotCompress()) {
			comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
			comp_null_buf_size = ((no_obj + 7) / 8);
			ResetModeNullsCompressed();
		} else {
			comp_null_buf_size = ((no_obj + 7) / 8);
			comp_null_buf = MMGuard<uchar>((uchar*)alloc((comp_null_buf_size + 2) * sizeof(uchar), BLOCK_TEMPORARY), *this);

			uint cnbl = comp_null_buf_size + 1;
			comp_null_buf[cnbl] = 0xBA; // just checking - buffer overrun
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
		}
	}


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

	if(maxv != 0) {
		comp_len_buf_size = onn * sizeof(uint) + 28;
		comp_len_buf = MMGuard<uint>((uint*)alloc(comp_len_buf_size / 4 * sizeof(uint), BLOCK_TEMPORARY), *this);
		uint tmp_comp_len_buf_size = comp_len_buf_size - 8;
		nc.Compress((char*)(comp_len_buf.get() + 2), tmp_comp_len_buf_size, nc_buffer.get(), onn, maxv);
		comp_len_buf_size = tmp_comp_len_buf_size + 8;
	} else {
		comp_len_buf_size = 8;
		comp_len_buf = MMGuard<uint>((uint*) alloc(sizeof(uint) * 2, BLOCK_TEMPORARY), *this);
	}

	*comp_len_buf.get() = comp_len_buf_size;
	*(comp_len_buf.get() + 1) = maxv;

	TextCompressor tc;
	int zlo = 0;
	for(uint obj = 0; obj < no_obj; obj++)
		if(!IsNull(obj) && GetSize(obj) == 0)
			zlo++;
	int dlen = data_full_byte_size + 10;
	MMGuard<char> comp_buf((char*)alloc(dlen * sizeof(char), BLOCK_TEMPORARY), *this);

	if(data_full_byte_size) {
		int objs = (no_obj - no_nulls) - zlo;

		MMGuard<char*> tmp_index((char**)alloc(objs * sizeof(char*), BLOCK_TEMPORARY), *this);
		MMGuard<ushort> tmp_len((ushort*)alloc(objs * sizeof(ushort), BLOCK_TEMPORARY), *this);

		int nid = 0;
		for(int id = 0; id < (int) no_obj; id++) {
			if(!IsNull(id) && GetSize(id) != 0) {
				tmp_index[nid] = (char*) index[id];
				tmp_len[nid++] = GetSize(id);
			}
		}

		tc.Compress(comp_buf.get(), dlen, tmp_index.get(), tmp_len.get(), objs, ShouldNotCompress() ? 0 : TextCompressor::VER);

	} else {
		dlen = 0;
	}

	comp_buf_size = (comp_null_buf_size > 0 ? 2 + comp_null_buf_size : 0) + comp_len_buf_size + 4 + dlen;

	MMGuard<uchar> new_compressed_buf((uchar*) alloc(comp_buf_size * sizeof(uchar), BLOCK_COMPRESSED), *this);
	uchar* p = new_compressed_buf.get();

	////////////////////////////////Nulls
	if(no_nulls > 0) {
		*((ushort*) p) = (ushort) comp_null_buf_size;
		p += 2;
		memcpy(p, comp_null_buf.get(), comp_null_buf_size);
		p += comp_null_buf_size;
	}

	////////////////////////////////	Lens

	if(comp_len_buf_size)
		memcpy(p, comp_len_buf.get(), comp_len_buf_size);

	p += comp_len_buf_size;

	*((int*) p) = dlen;
	p += sizeof(int);
	if(dlen)
		memcpy(p, comp_buf.get(), dlen);

	dealloc(compressed_buf);
	compressed_buf = new_compressed_buf.release();

	SetModeDataCompressed();
	compressed_up_to_date = true;
	CompressionStatistics stats;
	stats.previous_no_obj = previous_no_obj;
	stats.new_no_obj = NoObjs();
	return stats;
}

void AttrPackS::StayCompressed()
{
	if(!compressed_up_to_date)
		saved_up_to_date = false;
	BHASSERT(compressed_buf!=NULL && compressed_up_to_date, "Compression not up-to-date in StayCompressed");


	if(data) {
		int ds = (int)rc_msize(data) / sizeof(char*);
		for(int i = 0; i < ds && i < data_id; i++)
			dealloc(data[i]);
		dealloc(data);
		data = 0;
	}

	dealloc(nulls);
	dealloc(index);
	dealloc(lens);
	index = 0;
	lens = 0;
	nulls = NULL;
	nulls_bytes=0;
	is_only_compressed = true;
}


int AttrPackS::Save(IBStream* fcurfile, DomainInjectionManager& dim)
{
	MEASURE_FET("AttrPackS::Save(...)");

        msgRCAttr_packS msg_pack;
        std::string riak_value;

	msg_pack.set_no_obj(no_obj - 1);
	msg_pack.set_no_nulls(no_nulls);
	msg_pack.set_optimal_mode(optimal_mode);
	msg_pack.set_decomposer_id(decomposer_id);
	msg_pack.set_no_groups(no_groups);
	msg_pack.set_data_full_byte_size(data_full_byte_size);
	msg_pack.set_ver(ver);
	std::string _riak_key = this->GetRiakKey();
	std::string riak_params = " ";

	SaveUncompressed(msg_pack);

	if (!msg_pack.SerializeToString(&riak_value)) {
		DPRINTF("Serialize error at: %s\n", _riak_key.c_str());
	}

	if(RIAK_OPER_LOG>=2){
		DPRINTF("PackS::Save, riakkey = %s , value_size = %d\n", _riak_key.c_str(), (int)(riak_value.size()));
	}

	int ret = 0;
	if(pAsyncRiakHandle == NULL){
		ret = _riak_cluster->Put(RIAK_CLUSTER_DP_BUCKET_NAME, _riak_key, riak_value, &riak_params);
	}
	else{
		ret = _riak_cluster->AsyncPut(pAsyncRiakHandle,RIAK_CLUSTER_DP_BUCKET_NAME, _riak_key, riak_value, &riak_params);
	}
	if (ret != 0) {
		std::string errmsg;
		char errmsg_buf[128];
		snprintf(errmsg_buf, sizeof(errmsg_buf), "Error: PackS [%s] save failed, ret [%d], msg [%s]",
			_riak_key.c_str(), ret, strerror(ret));
		errmsg = errmsg_buf;
		rclog << lock << errmsg << unlock;
		throw DatabaseRCException(errmsg);
	}

//	previous_size = TotalSaveSize();
	previous_size = riak_value.size();
	saved_up_to_date = true;
	previous_no_obj = no_obj-no_nulls;
	return 0;
}

uint AttrPackS::TotalSaveSize()
{
	//fix sasu-89
	return 1;
}

void AttrPackS::SaveUncompressed(msgRCAttr_packS &msg_pack)
{
	uint string_size = 0;

	for(uint i = 0; i < no_obj; i++) {
		if(!IsNull(i))
			string_size += GetSize(i);
	}

	if(data) {
		std::string tmp_data;
		for(uint i = 0; i < no_obj; i++) {
			if(!IsNull(i)) {
				tmp_data.append(GetVal(i), GetSize(i));
			}
		}
		msg_pack.set_data(tmp_data);
	}

	if(nulls) {
		msg_pack.set_nulls(this->nulls, this->nulls_bytes);
	}

	if(lens) {
		msg_pack.set_lens(lens, len_mode * no_obj);
	}
}


void AttrPackS::LoadUncompressed(msgRCAttr_packS &msg_pack)
{
	// cond_rowmap有可能在返回部分数据或者全部数据时设置
	if(msg_pack.has_cond_rowmap()) {
		cr_result->CreateBitmap(msg_pack.cond_rowmap().data(), msg_pack.cond_rowmap().size(),msg_pack.first_row(), msg_pack.last_row());
	}
	// case 1: return value list
	if(msg_pack.has_cond_data()) {
		// build nulls&& data
		const char* cond_data=msg_pack.cond_data().data();		
		int last_obj=0;
		int list_rows=msg_pack.cond_ret_line_no_size();
		if(list_rows>0) {
			first_obj=msg_pack.cond_ret_line_no().data()[0];
			last_obj=msg_pack.cond_ret_line_no().data()[list_rows-1];
		}
		else {
			first_obj=msg_pack.first_row();
			last_obj=msg_pack.last_row();
		}
		int range_rows=last_obj-first_obj+1;

		nulls_bytes = ((last_obj >> 3) - (first_obj >> 3) + 1);
		// alloc lens/index/nulls
		AllocBuffersLens(range_rows,nulls_bytes);
		AllocBuffersData(range_rows,msg_pack.cond_data().size());
		no_nulls=0;
		memset(nulls,0,nulls_bytes);
		int data_offset=0;
		const char *plens=msg_pack.cond_lens().data();
		memcpy((char *)*data,cond_data,msg_pack.cond_data().size());

		if (list_rows > 0) { // just return line_no , not return bit map	
			int prev_obj=first_obj-1; 
			
			cr_result->CreateList(msg_pack.cond_ret_line_no().data(), list_rows);	
			for(int i=0;i<list_rows;i++) {
				int obj=msg_pack.cond_ret_line_no(i);
				int obj_len=len_mode == sizeof(ushort)?((ushort *)plens)[i]:((int *)plens)[i];
				for(int j=prev_obj+1;j<obj;j++) {
					SetNull(j);
					index[j-first_obj]=NULL;
				}
				SetSize(obj,obj_len);
				index[obj-first_obj] = ((uchar*) (*data)) + data_offset;
				data_offset+=obj_len;
				prev_obj=obj;
			}
		} else { // just return the bit map
			const uint8_t *cond_rowmap = (const uint8_t *)msg_pack.cond_rowmap().data();
			size_t cond_rowmap_size = msg_pack.cond_rowmap().size();
			const uint16_t *obj_lens_p = (const uint16_t *)plens;
			for(int i=first_obj;i<=last_obj;i++) {
				if (CR_Class_NS::BMP_BITTEST(cond_rowmap, cond_rowmap_size, i, first_obj)) {
					SetSize(i, *obj_lens_p);
					index[i - first_obj] = ((uchar*) (*data)) + data_offset;
					data_offset += *obj_lens_p;
					obj_lens_p++;
				} else {
					SetNull(i);
					index[i - first_obj]=NULL;
				}
			}
		}
	}
	// case 2: return partial rows and set nulls_map
	else if(msg_pack.has_range_data()||msg_pack.has_range_nulls()) {
		first_obj=msg_pack.first_row();
		int last_obj=msg_pack.last_row();
		int range_rows=last_obj-first_obj+1;
		nulls_bytes=msg_pack.range_nulls().size();
		if(nulls_bytes > 0) {
			AllocBuffersLens(range_rows, nulls_bytes);
			memcpy(nulls, msg_pack.range_nulls().data(), nulls_bytes);
		} else {
			AllocBuffersLens(range_rows,0);
		}
		memcpy(lens, msg_pack.range_lens().data(),msg_pack.range_lens().size());
		AllocBuffersData(range_rows,msg_pack.range_data().size());
		memcpy(*data, msg_pack.range_data().data(),msg_pack.range_data().size());
		int sumlen=0;
		for(uint i = first_obj; i < first_obj+range_rows; i++) {
			if(!IsNull(i) && GetSize(i) != 0) {
				index[i-first_obj] = ((uchar*) (*data)) + sumlen;
				sumlen += GetSize(i);
			} else {
				index[i-first_obj] = 0;
			}
		}
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(sumlen==0 || 
			msg_pack.range_data().size()>0);
	}
	// case 3: return full package
	else if(msg_pack.has_data()) {
		first_obj=0;
		nulls_bytes=msg_pack.nulls().size();
		//allocate data/lens/nulls
		AllocBuffers();
		if(data) {
			memcpy(*data, msg_pack.data().data(), data_full_byte_size);
		}
		if(lens) {
			memcpy(lens, msg_pack.lens().data(), msg_pack.lens().size());
		}
		if(nulls_bytes>0) {
			memcpy(nulls, msg_pack.nulls().data(), nulls_bytes);
		}
		int sumlen = 0;
		for(uint i = 0; i < no_obj; i++) {
			if(!IsNull(i) && GetSize(i) != 0) {
				index[i] = ((uchar*) (*data)) + sumlen;
				sumlen += GetSize(i);
			} else {
				SetSize(i, 0);
				index[i] = 0;
			}
		}
		is_cq=false;
	}
	else { // get a empty pack
		//set all range values to null
		first_obj=msg_pack.first_row();
		int last_obj=msg_pack.last_row();
		int range_rows=last_obj-first_obj+1;
		nulls_bytes = ((last_obj >> 3) - (first_obj >> 3) + 1);
		if (nulls) {
			dealloc(nulls);
		}
		nulls = (uint8_t*)alloc(nulls_bytes, BLOCK_UNCOMPRESSED);
		memset(nulls, 0xff, nulls_bytes);
	}
}

void AttrPackS::AllocBuffersLens(int newobjs,int newnullsbytes)
{
	assert(!lens && !index);
	lens = alloc(len_mode * newobjs * sizeof(char), BLOCK_UNCOMPRESSED);
	memset(lens,0,len_mode * newobjs * sizeof(char));
	index = (uchar**)alloc(newobjs * sizeof(uchar*), BLOCK_UNCOMPRESSED);
	memset(index,0,newobjs * sizeof(uchar*));
	if(newnullsbytes > 0) {
		if (nulls) dealloc(nulls);
		nulls = (uint8_t*) alloc(newnullsbytes, BLOCK_UNCOMPRESSED);
		nulls_bytes = newnullsbytes;
		BHASSERT(((newobjs!=65536) || (newnullsbytes==8192)), "Get a Full package(S) but nulls not completed");
	}
	else {
		if(nulls) {
			dealloc(nulls);
			nulls=NULL;
		}
		nulls_bytes=0;
	}
}

void AttrPackS::AllocBuffersData(int newobjs,int newdatasize)
{
	if(data == NULL) {
		data = (uchar**) alloc(sizeof(uchar*), BLOCK_UNCOMPRESSED);
		*data = 0;
		if(newdatasize) {
			*data = (uchar*) alloc(newdatasize, BLOCK_UNCOMPRESSED);
			if(!*data)
				rclog << lock << "Error: out of memory (" << data_full_byte_size << " bytes failed). (40)" << unlock;
		}
		data_id = 1;
	}
}

//allocate for full pack
void AttrPackS::AllocBuffers()
{
	if(data == NULL) {
		data = (uchar**) alloc(sizeof(uchar*), BLOCK_UNCOMPRESSED);
		*data = 0;
		if(data_full_byte_size) {
			*data = (uchar*) alloc(data_full_byte_size, BLOCK_UNCOMPRESSED);
			if(!*data)
				rclog << lock << "Error: out of memory (" << data_full_byte_size << " bytes failed). (40)" << unlock;
		}
		data_id = 1;
	}
	assert(!lens && !index);
	lens = alloc(len_mode * no_obj * sizeof(char), BLOCK_UNCOMPRESSED);
	index = (uchar**)alloc(no_obj * sizeof(uchar*), BLOCK_UNCOMPRESSED);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(no_nulls==0||nulls_bytes>0);
	if(no_nulls > 0) {
		if(nulls)
			dealloc(nulls);
		nulls = (uint8_t*) alloc(nulls_bytes, BLOCK_UNCOMPRESSED);
	}
	else {
		nulls_bytes=0;
		if(nulls) {
			dealloc(nulls);
			nulls=NULL;
		}
	}
}
