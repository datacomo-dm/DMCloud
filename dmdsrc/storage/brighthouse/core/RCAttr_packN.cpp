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
#include "compress/NumCompressor.h"
#include "WinTools.h"
#include "system/MemoryManagement/MMGuard.h"
#include "RCAttr.h"
#include "vc/SingleColumn.h"

AttrPackN::AttrPackN(PackCoordinate pc, AttributeType attr_type, int inserting_mode, DataCache* owner)
	:	AttrPack(pc, attr_type, inserting_mode, false, owner), data_full(0)
{
	no_obj = 0;
	no_nulls = 0;
	nulls_bytes=0;
	max_val = 0;
	bit_rate = 0;
	value_type = UCHAR;
	comp_buf_size = 0;
	compressed_buf = 0;
	nulls = NULL;
	compressed_up_to_date = false;
	saved_up_to_date = false;
	optimal_mode = 0;
}


AttrPackN::AttrPackN(const AttrPackN &apn)
	:	AttrPack(apn)
{
	assert(!is_only_compressed);
	value_type = apn.value_type;
	bit_rate = apn.bit_rate;
	max_val = apn.max_val;
	optimal_mode = apn.optimal_mode;
	data_full = 0;
	if(no_obj && apn.data_full) {
		data_full = alloc(value_type * no_obj, BLOCK_UNCOMPRESSED);
		memcpy(data_full, apn.data_full, value_type * no_obj);
	}
}

std::auto_ptr<AttrPack> AttrPackN::Clone() const
{
	return std::auto_ptr<AttrPack>(new AttrPackN(*this) );
}

AttrPackN::ValueType AttrPackN::ChooseValueType(int bit_rate)
{
	if(bit_rate <= 8)
		return UCHAR;
	else if(bit_rate <= 16)
		return USHORT;
	else if(bit_rate <= 32)
		return UINT;
	else
		return UINT64;
}

void AttrPackN::Prepare(uint new_no_obj, _uint64 new_max_val)
{
	Lock();
	this->no_obj=new_no_obj;
	//no_nulls = new_no_obj;
	this->no_nulls = 0;
	this->nulls_bytes=0;
	this->max_val=new_max_val;
	bit_rate = GetBitLen(max_val);
	data_full = 0;
	value_type = UCHAR;
	if(bit_rate) {
		value_type = ChooseValueType(bit_rate);
		if(new_no_obj)
			data_full = alloc(value_type * new_no_obj, BLOCK_UNCOMPRESSED);
	}
	comp_buf_size=0;
	compressed_buf=NULL;
	nulls=NULL;
	compressed_up_to_date = false;
	saved_up_to_date = false;
	optimal_mode = 0;
	is_empty = false;
	nulls_bytes=0;
	Unlock();
}

void AttrPackN::Destroy()
{
	dealloc(data_full);
	data_full = 0;
	is_empty = true;
	dealloc(nulls);
	dealloc(compressed_buf);
	compressed_buf = 0;
	nulls = NULL;
	nulls_bytes=0;
}

AttrPackN::~AttrPackN()
{
	DestructionLock();
	Destroy();
}


////////////////////////////////////////////////////////////////////////////
//	Save format:
//
//	<total_byte_size>		- uint, the size of the data on disk (bytes), including header and dictionaries
//	<mode>					- uchar, see Compress method definition. Special values: 255 - nulls only, 254 - empty (0 objects)
//	<no_obj>				- ushort-1, i.e. "0" means 1 object
//	<no_nulls>				- ushort(undefined if nulls only)
//	<max_val>				- T_uint64, the maximal number to be encoded. E.g. 0 - only nulls and one more value.
//	<...data...>			- depending on compression mode

void AttrPackN::LoadData(IBStream* fcurfile,Descriptor *pdesc,int64 local_min,void *pf,int _objs)
{
#ifdef FUNCTIONS_EXECUTION_TIMES
	FETOperator feto("AttrPackN::LoadData(...)");
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
		uint buf_used;
		comp_buf_size = comp_null_buf_size = comp_len_buf_size = 0;
		compressed_buf = 0;
		nulls = NULL;
		data_full = 0;
		uint total_size;
		msgRCAttr_packN msg_pack;
		std::string riak_value;
		int ret;
		IBRiakDataPackQueryParam param;
		std::string _riak_key = this->GetRiakKey();
		std::string *pparam=NULL;
		std::string params;
		if (this->attr_type == RC_FLOAT) {
			param.set_pack_type(IBRiakDataPackQueryParam::PACK_FLOAT);
		} else {
			param.set_pack_type(IBRiakDataPackQueryParam::PACK_NUM_SIGNED);
		}
		if(pdesc &&_logical_coord.co.pack[4]!=0) {
			RCAttr* a0 = (RCAttr *) ((SingleColumn *)pdesc->attr.vc)->GetPhysical();
			bool isfloat=a0->Type().IsFloat();
			is_cq=true;
			// we only support BETWEEN now
			MIIterator mit;// for dummy iterator
			_int64 v1, v2;
			double dv1,dv2;
			IBRiakDataPackQueryParam_ExpField &exp=*param.mutable_expression();
			switch(pdesc->op) {
			case O_NOT_BETWEEN:
			case O_BETWEEN:
				if (pdesc->op == O_NOT_BETWEEN)
					exp.set_op_type(IBRiakDataPackQueryParam_ExpOPType_EOP_NBT);
				else
					exp.set_op_type(IBRiakDataPackQueryParam_ExpOPType_EOP_BT);
				if(isfloat) {
					dv1=pdesc->val1.vc->GetValueDouble(mit);
					dv2=pdesc->val2.vc->GetValueDouble(mit);
					exp.add_value_list_flt(dv1);
					exp.add_value_list_flt(dv2);
				}
				else
				{
					v1=pdesc->val1.vc->GetValueInt64(mit);
					v2=pdesc->val2.vc->GetValueInt64(mit);
					exp.add_value_list_num(v1);
					exp.add_value_list_num(v2);
				}
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
					if(isfloat) {
						dv1 = it->GetRCNum();
						exp.add_value_list_flt(dv1);
						//param.add_condition_in_float(dv1);
					}
					else {
						v1 = it->GetInt64();
						exp.add_value_list_num(v1);
						//param.add_condition_in_num_s(v1);
					}
				}
				}
				break;
			case O_LESS :
			case O_LESS_EQ :
			case O_MORE :
			case O_MORE_EQ :
				if(isfloat) {
					dv1=pdesc->val1.vc->GetValueDouble(mit);
					exp.add_value_list_flt(dv1);
				}
				else {
					v1=pdesc->val1.vc->GetValueInt64(mit);
					exp.add_value_list_num(v1);
				}
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
			param.set_pack_local_min_num(local_min);

		}
		int mapin_len=(_objs==0||_objs==65535)?8192:(_objs/8+1);
		const char *mapin=(const char *)pf;
		if(pf) {
			param.set_first_row(0);
			param.set_last_row(_objs==0?65535:_objs);
		}
		// maybe both cq&rq
		if(_logical_coord.co.pack[3]!=0) {
			is_rq=true;
			param.set_pack_type(IBRiakDataPackQueryParam_PackType::IBRiakDataPackQueryParam_PackType_PACK_NUM_SIGNED);
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
		//packload perf monitor
		tm_preprocess.Stop();
		tm_get.Start();
		#ifndef NDEBUG
		ret=_riak_cluster->Get(RIAK_CLUSTER_DP_BUCKET_NAME, _riak_key, riak_value, pparam,checkTraceRiakNode(table_number,attr_number,pack_no));
		#else
		ret=_riak_cluster->Get(RIAK_CLUSTER_DP_BUCKET_NAME, _riak_key, riak_value, pparam);
		#endif
		//packload perf monitor
		tm_get.Stop();
		tm_postprocess.Start();
		if (ret != 0) {
			param.clear_cond_rowmap_in();
			DPRINTF("Error: PackN [%s] load failed, ret [%s], msg [%s], \nparam => %s\n, mapin_len = %d\n",
				_riak_key.c_str(), CR_Class_NS::strerrno(ret), CR_Class_NS::strerror(ret),
				param.DebugString().c_str(), (mapin)?(mapin_len):(0)
			);
			std::string errmsg;
			char errmsg_buf[256];
			snprintf(errmsg_buf, sizeof(errmsg_buf),
				"PackN [%s] load failed, ret [%s], msg [%s]",
				_riak_key.c_str(), CR_Class_NS::strerrno(ret), CR_Class_NS::strerror(ret)
			);
			errmsg = errmsg_buf;
			rclog << lock << errmsg << unlock;
			throw DatabaseRCException(errmsg);
		}

		if(RIAK_OPER_LOG>=2){
			DPRINTF("PackN::LoadData[T%08d-A%08d-P%08d], riakkey = %s , value_size = %d\n",
			    this->table_number, this->attr_number, this->pack_no,
			    _riak_key.c_str(), (int)(riak_value.size()));
		}

		if (!msg_pack.ParseFromString(riak_value)) {
			DPRINTF("Parse error at:%s, value_size=%d\n", _riak_key.c_str(), (int)(riak_value.size()));
			char errmsg_buf[128];
			snprintf(errmsg_buf,sizeof(errmsg_buf),"Parse error at:%s, value_size=%d\n", _riak_key.c_str(), (int)(riak_value.size()));
			throw DatabaseRCException(errmsg_buf);
		}

		no_obj = msg_pack.no_obj() + 1;
		no_nulls = msg_pack.no_nulls();
		optimal_mode = msg_pack.optimal_mode();
		max_val = msg_pack.max_val();
		total_size = previous_size = msg_pack.total_size();

		bit_rate = CalculateBinSize(max_val);
		value_type = ChooseValueType(bit_rate);
		if(msg_pack.has_cond_rowmap()) {
			cr_result->CreateBitmap(msg_pack.cond_rowmap().data(), msg_pack.cond_rowmap().size(),msg_pack.first_row(), msg_pack.last_row());
		}
		if(msg_pack.has_first_row() && msg_pack.first_row()<0) {
			if(pdesc==NULL) {
				char errmsg_buf[128];
				snprintf(errmsg_buf,sizeof(errmsg_buf),"Get a corrupt pack(key:%s),first_row=%d,attr_number=%d.\n", _riak_key.c_str(), msg_pack.first_row(),attr_number);
				throw DatabaseRCException(errmsg_buf);
			}
		}		// cond_rowmap有可能在返回部分数据或者全部数据时设置
		// case 1: return value list
		else if(msg_pack.has_cond_data()) {
			// build nulls&& data
			int list_rows=msg_pack.cond_ret_line_no_size();
			const char* cond_data=msg_pack.cond_data().data();
			int cond_size=msg_pack.cond_data().size();
			int last_obj=0;
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
			data_full = alloc(value_type * range_rows, BLOCK_UNCOMPRESSED);
			memset(data_full,0,value_type * range_rows);
			if (list_rows > 0) {
				//no null row?
				if(list_rows!=range_rows) {
					nulls = (uint8_t*)alloc(nulls_bytes, BLOCK_UNCOMPRESSED);
					memset(nulls,0,nulls_bytes);
				}
				else {
					if(nulls) {
						dealloc(nulls);
						nulls=NULL;
					}
					nulls_bytes=0;
				}
				no_nulls=0;
				
				int prev_obj=first_obj-1;
				cr_result->CreateList(msg_pack.cond_ret_line_no().data(), list_rows);
				for(int i=0;i<list_rows;i++) {
					int obj=msg_pack.cond_ret_line_no(i);
					for(int j=prev_obj+1;j<obj;j++) 
						SetNull(j);
					memcpy((char *)data_full+(obj-first_obj)*value_type,cond_data,value_type);
					cond_data+=value_type;
					prev_obj=obj;
				}
			} else { // just return the bit map
				const uint8_t *cond_rowmap = (const uint8_t *)msg_pack.cond_rowmap().data();
				size_t cond_rowmap_size = msg_pack.cond_rowmap().size();
				nulls = (uint8_t*)alloc(nulls_bytes, BLOCK_UNCOMPRESSED);
				memset(nulls,0,nulls_bytes);
				no_nulls=0;
				for(int i=first_obj;i<=last_obj;i++) {
					if (CR_Class_NS::BMP_BITTEST(cond_rowmap, cond_rowmap_size, i, first_obj)) {
						memcpy((char *)data_full+(i-first_obj)*value_type,cond_data,value_type);
						cond_data+=value_type;
					} else {
						SetNull(i);
					}
				}
				assert((range_rows-no_nulls)*value_type==cond_size);
			}
		}
		// case 2: return partial rows and set nulls_map
		else if(msg_pack.has_range_data()||msg_pack.has_range_nulls()) {
			if(msg_pack.has_range_nulls()) {
				nulls_bytes=msg_pack.range_nulls().size();
				nulls = (uint8_t*)alloc(nulls_bytes, BLOCK_UNCOMPRESSED);
				memcpy(nulls, msg_pack.range_nulls().data(), nulls_bytes);
			}
			first_obj=msg_pack.first_row();
			if(msg_pack.has_range_data()) {
				data_full = alloc(msg_pack.range_data().size(), BLOCK_UNCOMPRESSED);
				memcpy(data_full,msg_pack.range_data().data(),msg_pack.range_data().size());
			}
		}
		// case 3: return full package
		else if(msg_pack.has_data()||msg_pack.has_nulls()) {
			if(no_nulls>0) {
				nulls_bytes=msg_pack.nulls().size();
				nulls = (uint8_t*)alloc(nulls_bytes, BLOCK_UNCOMPRESSED);
				memcpy(nulls, msg_pack.nulls().data(), nulls_bytes);
			}
			first_obj=0;
			if(msg_pack.has_data()) {
				data_full = alloc(msg_pack.data().size(), BLOCK_UNCOMPRESSED);
				memcpy(data_full,msg_pack.data().data(),msg_pack.data().size());
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
		compressed_up_to_date = false;
		saved_up_to_date = true;
		is_empty = false;
#ifdef FUNCTIONS_EXECUTION_TIMES
		NoBytesReadByDPs += total_size;
#endif
	} catch (RCException&) {
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
			rclog << lock << "PackN Read stat for last 200 packs: t"
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

int AttrPackN::Save(IBStream* fcurfile, DomainInjectionManager& dim)
{
	MEASURE_FET("AttrPackN::Save(...)");
	uint total_size = 0;
	msgRCAttr_packN msg_pack;
	std::string riak_value;
	int ret = 0;

	previous_size = total_size = TotalSaveSize();

	msg_pack.set_no_obj(no_obj - 1);
	msg_pack.set_no_nulls(no_nulls);
	msg_pack.set_optimal_mode(optimal_mode);
	msg_pack.set_max_val(max_val);
	msg_pack.set_total_size(total_size);

	if(nulls)
		msg_pack.set_nulls(nulls, this->nulls_bytes);

	if(data_full)
		msg_pack.set_data(data_full, value_type * no_obj);

	std::string riak_params = " ";
	std::string _riak_key = this->GetRiakKey();

	msg_pack.SerializeToString(&riak_value);

	if(RIAK_OPER_LOG>=2){
		DPRINTF("PackN::Save, riakkey = %s , value_size = %d\n", _riak_key.c_str(), (int)(riak_value.size()));
	}

	if(pAsyncRiakHandle == NULL){
		ret = _riak_cluster->Put(RIAK_CLUSTER_DP_BUCKET_NAME, _riak_key, riak_value, &riak_params);
	}
	else{
		ret = _riak_cluster->AsyncPut(pAsyncRiakHandle,RIAK_CLUSTER_DP_BUCKET_NAME, _riak_key, riak_value, &riak_params);
	}
	if (ret == 0) {
		saved_up_to_date = true;
	} else {
		std::string errmsg;
		char errmsg_buf[128];
		snprintf(errmsg_buf, sizeof(errmsg_buf), "Error: PackN [%s] save failed, ret [%d], msg [%s]",
			_riak_key.c_str(), ret, strerror(ret));
		errmsg = errmsg_buf;
		rclog << lock << errmsg << unlock;
		throw DatabaseRCException(errmsg);
	}

	return ret;
}

uint AttrPackN::TotalSaveSize()
{
	//fix sasu-89
	return 1;
}


////////////////////////////////////////////////////////////////////////////


void AttrPackN::SetVal64(uint n, const _uint64 &val_code)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!is_only_compressed);
	compressed_up_to_date = false;
	
	//BHASSERT_WITH_NO_PERFORMANCE_IMPACT(data_full && n < no_obj);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(n < no_obj);
	if(data_full) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(n <= no_obj);

		if(value_type == UINT64)		((_uint64*)data_full)[n-first_obj]	= _uint64(val_code);
		else if(value_type == UCHAR)	((uchar*)data_full) [n-first_obj]	= uchar(val_code);
		else if(value_type == USHORT)	((ushort*)data_full)[n-first_obj]	= ushort(val_code);
		else if(value_type == UINT)		((uint*)data_full)  [n-first_obj]	= uint(val_code);
	}
}

template <AttrPackN::ValueType VT>
void AttrPackN::AssignToAll(_int64 offset, void*& new_data_full)
{
	typedef typename ValueTypeChooser<VT>::Type Type;
	Type v = (Type)(offset);
	Type* values = (Type*)(new_data_full);
	InitValues(v, values);
}

template <typename T>
void AttrPackN::InitValues(T value, T*& values)
{
    for(uint i = 0; i < no_obj; i++) {
        if(!IsNull(i)) {
            values[i] = value;
        }
    }
}

void AttrPackN::InitValues(ValueType value_type, _int64 value, void*& values)
{
	switch(value_type) {
		case UCHAR :
			InitValues((uchar)value, (uchar*&)values);
			break;
		case USHORT :
			InitValues((ushort)value, (ushort*&)values);
			break;
		case UINT :
			InitValues((uint)value, (uint*&)values);
			break;
		case UINT64 :
			InitValues((_uint64)value, (_uint64*&)values);
			break;
		default :
			BHERROR("Unexpected Value Type");
	}
}


template <typename S, typename D>
void AttrPackN::CopyValues(D*& new_values, _int64 offset)
{
	S* values = (S*)(data_full);
    for(uint i = 0; i < no_obj; i++) {
        if(!IsNull(i))
            new_values[i] = D(values[i] + offset);
    }
}

void AttrPackN::CopyValues(ValueType value_type, ValueType new_value_type, void*& new_values, _int64 offset)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(new_value_type >= value_type);
	if(value_type == UCHAR) {
		if(new_value_type == UCHAR)
			CopyValues<uchar, uchar>((uchar*&)(data_full), offset);
		else if(new_value_type == USHORT)
			CopyValues<uchar, ushort>((ushort*&)new_values, offset);
		else if(new_value_type == UINT)
			CopyValues<uchar, uint>((uint*&)new_values, offset);
		else {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(new_value_type == UINT64);
			CopyValues<uchar, _uint64>((_uint64*&)new_values, offset);
		}
	} else if(value_type == USHORT) {
		if(new_value_type == USHORT)
			CopyValues<ushort, ushort>((ushort*&)new_values, offset);
		else if(new_value_type == UINT)
			CopyValues<ushort, uint>((uint*&)new_values, offset);
		else {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(new_value_type == UINT64);
			CopyValues<ushort, _uint64>((_uint64*&)new_values, offset);
		}
	} else if(value_type == UINT) {
		if(new_value_type == UINT)
			CopyValues<uint, uint>((uint*&)new_values, offset);
		else {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(new_value_type == UINT64);
			CopyValues<uint, _uint64>((_uint64*&)new_values, offset);
		}
	} else if(value_type == UINT64) {
		CopyValues<_uint64, _uint64>((_uint64*&)new_values, offset);
	}
}

void AttrPackN::Expand(uint new_no_obj, _uint64 new_max_val, _int64 offset)		// expand data by adding empty values (if necessary) or changing bit rate (if necessary)
{
	BHASSERT(!is_only_compressed, "The pack is compressed!");
	BHASSERT(new_no_obj >= no_obj, "Counter of new objects must be greater or equal to the counter total number of objects!");

	if(max_val <= new_max_val)
		max_val = new_max_val;
	else
		saved_up_to_date = false;

	int new_bit_rate = GetBitLen(max_val);
	ValueType new_value_type = ChooseValueType(new_bit_rate);

	if(new_bit_rate > 0 && (new_bit_rate > bit_rate || new_no_obj > no_obj || offset != 0)) {

		if(value_type != new_value_type) {

			MMGuard<void> new_data_full(alloc(new_value_type * new_no_obj, BLOCK_UNCOMPRESSED), *this);
			void* p(new_data_full.get());
			if(bit_rate == 0)
				InitValues(new_value_type, offset, p);
			else
				CopyValues(value_type, new_value_type, p, offset);

			dealloc(data_full);
			data_full = new_data_full.release();

		} else {
			if(new_value_type * new_no_obj)
				data_full = rc_realloc(data_full, new_value_type * new_no_obj, BLOCK_UNCOMPRESSED);
			else
				data_full = 0;

			if(bit_rate == 0)
				InitValues(value_type, offset, data_full);
			else
				CopyValues(value_type, new_value_type, data_full, offset);
		}
		bit_rate = new_bit_rate;
		value_type = new_value_type;
	}

	if(new_no_obj > no_obj) {
		no_obj = new_no_obj;
		compressed_up_to_date = false;
	}
	if(nulls && nulls_bytes!=8192) {
		uint8_t *newnulls = (uint8_t*) alloc(8192, BLOCK_UNCOMPRESSED);
		memset(newnulls, 0, 8192);
		memcpy(newnulls,nulls,nulls_bytes);
		dealloc(nulls);
		nulls=newnulls;
		nulls_bytes=8192;
	}
}

void AttrPackN::StayCompressed()	// Remove full data (conserve memory) and stay with compressed buffer only.
{
	if(!compressed_up_to_date)
		saved_up_to_date = false;
	BHASSERT(compressed_buf!=NULL && compressed_up_to_date, "Compression not up-to-date in StayCompressed");

	dealloc(data_full);
	dealloc(nulls);
	nulls_bytes=0;
	data_full	= NULL;
	nulls		= NULL;
	is_only_compressed = true;
}

///////////////////////////////////////////////////////////////////////////////////////
//
//	Compression protocol < OLD! Encoding for version 2.12 SP2 and earlier >:
//
//  General notation remark:	<val> - 2-level value in flat version or Huffmann version (depending on mode)
//								<jump> - flat 3-bit or Huffmann encoding of jump length (minimum: 1)
//								<diff> - flat 3-bit or Huffmann encoding of difference: 0 -> set NULL, 1 -> diff=0, 2 -> diff=1, 3 -> diff=-1, ... k -> diff=(k/2)*(-1)^(k%2)
//								<rle> - number of repetitions, encoded on "rle_bits" bits; value 0 means 1 repetition etc.
//  Compression modes:
//		 mode%2		= 0 for flat value encoding and 1 for Huff. val. enc.
//		(mode/2)%2	= 1 for RLE enabled, otherwise 0
//		(mode/4)%2	= 1 for jump mode, otherwise 0
//		(mode/8)%2	= 1 for combined mode, otherwise 0
//		(mode/16)%2	= 1 for compressed nulls, otherwise 0 (flat or no nulls at all)
//
//  Parameter "rle_bits": number of bits to encode number of repetitions
//
//  Encoding:
//	[x0000 - RLE off, jump off, combined off]
//	<nulls><val>...<val>
//
//	[x0010 - RLE on, jump off, combined off]
//	<nulls>
//	<rle_bits>					- uchar, RLE bit depth, 0 = no RLE compression
//	1<val>						- new value of attr.
//	0<rle>						- how many times we should repeat the last value; if rle_bits=0 then bit 0 indicates one repetition.
//
//	[x0110 - RLE on, jump on, combined off]
//	<nulls>
//	<rle_bits>					- uchar, RLE bit depth, 0 = no RLE compression
//	1<val>						- new value of attr.
//	0<rle><jump>				- get a value from position (current-jump-1)
//								  and repeat it (rle+1) times; if rle_bits=0 then this is just one repetition.
//
//	[x1000 - RLE off, jump off, combined on]
//	<nulls>
//	1<val>						- new value of attr.
//	0<diff>						- get the last nonzero value and add the encoded difference
//
//	[x1010 - RLE on, jump off, combined on]
//	<nulls>
//	<rle_bits>					- uchar, RLE bit depth, 0 = no RLE compression
//	11<val>						- new value of attr.
//	10<diff>					- get the last nonzero value and add the encoded difference
//  01<rle>						- get the last nonzero value, add 1 and put here, creating an increasing sequence of (rle+1) numbers (e.g. last=7 and code "01<4>" produce sequence "8,9,10,11,12")
//								  if rle_bits=0 then code "01" means the same as "10<+1>"
//  00<rle><diff>				- get the last nonzero value, add the encoded difference and repeat the result (rle+2) times
//								  if rle_bits=0 then code "00<diff>" means two identical values encoded by "diff"

template<typename etype> void AttrPackN::RemoveNullsAndCompress(NumCompressor<etype> &nc, char* tmp_comp_buffer, uint & tmp_cb_len, _uint64 & maxv)
{
    MMGuard<etype> tmp_data;
	if(no_nulls > 0) {
		tmp_data = MMGuard<etype>((etype*) (alloc((no_obj - no_nulls) * sizeof(etype), BLOCK_TEMPORARY)), *this);
		for(uint i = 0, d = 0; i < no_obj; i++) {
			if(!IsNull(i))
				tmp_data[d++] = ((etype*) (data_full))[i];
		}
	} else
		tmp_data = MMGuard<etype>((etype*)data_full, *this, false);

	nc.Compress(tmp_comp_buffer, tmp_cb_len, tmp_data.get(), no_obj - no_nulls, (etype) (maxv));
}


template<typename etype> void AttrPackN::DecompressAndInsertNulls(NumCompressor<etype> & nc, uint *& cur_buf)
{
    nc.Decompress(data_full, (char*)((cur_buf + 3)), *cur_buf, no_obj - no_nulls, (etype) * (_uint64*)((cur_buf + 1)));
    etype *d = ((etype*)(data_full)) + no_obj - 1;
    etype *s = ((etype*)(data_full)) + no_obj - no_nulls - 1;
    for(int i = no_obj - 1;d > s;i--){
        if(IsNull(i))
            --d;
        else
            *(d--) = *(s--);
    }
}

CompressionStatistics AttrPackN::Compress(DomainInjectionManager& dim)		// Create new optimal compressed buf. basing on full data.
{
	return CompressionStatistics();
	
	MEASURE_FET("AttrPackN::Compress()");
#ifdef FUNCTIONS_EXECUTION_TIMES
	std::stringstream s1;
	s1 << "aN[" << pc_column( GetPackCoordinate() ) << "].Compress(...)";
	FETOperator fet1(s1.str());
#endif

	/////////////////////////////////////////////////////////////////////////
	uint *cur_buf = NULL;
	uint buffer_size = 0;
	MMGuard<char> tmp_comp_buffer;

	uint tmp_cb_len = 0;
	SetModeDataCompressed();

	_uint64 maxv = 0;
	if(data_full) {		// else maxv remains 0
		_uint64 cv = 0;
		for(uint o = 0; o < no_obj; o++) {
			if(!IsNull(o)) {
				cv = (_uint64)GetVal64(o);
				if(cv > maxv)
					maxv = cv;
			}
		}
	}

	if(maxv != 0) {
		//BHASSERT(last_set + 1 == no_obj - no_nulls, "Expression evaluation failed!");

		if(value_type == UCHAR) {
			NumCompressor<uchar> nc(ShouldNotCompress());
			tmp_cb_len = (no_obj - no_nulls) * sizeof(uchar) + 20;
			if(tmp_cb_len)
				tmp_comp_buffer = MMGuard<char>((char*)alloc(tmp_cb_len * sizeof(char), BLOCK_TEMPORARY), *this);
			RemoveNullsAndCompress(nc, tmp_comp_buffer.get(), tmp_cb_len, maxv);
		} else if(value_type == USHORT) {
			NumCompressor<ushort> nc(ShouldNotCompress());
			tmp_cb_len = (no_obj - no_nulls) * sizeof(ushort) + 20;
			if(tmp_cb_len)
				tmp_comp_buffer = MMGuard<char>((char*)alloc(tmp_cb_len * sizeof(char), BLOCK_TEMPORARY), *this);
			RemoveNullsAndCompress(nc, tmp_comp_buffer.get(), tmp_cb_len, maxv);
		} else if(value_type == UINT) {
			NumCompressor<uint> nc(ShouldNotCompress());
			tmp_cb_len = (no_obj - no_nulls) * sizeof(uint) + 20;
			if(tmp_cb_len)
				tmp_comp_buffer = MMGuard<char>((char*)alloc(tmp_cb_len * sizeof(char), BLOCK_TEMPORARY), *this);
			RemoveNullsAndCompress(nc, tmp_comp_buffer.get(), tmp_cb_len, maxv);
		} else {
			NumCompressor<_uint64> nc(ShouldNotCompress());
			tmp_cb_len = (no_obj - no_nulls) * sizeof(_uint64) + 20;
			if(tmp_cb_len)
				tmp_comp_buffer = MMGuard<char>((char*)alloc(tmp_cb_len * sizeof(char), BLOCK_TEMPORARY), *this);
			RemoveNullsAndCompress(nc, tmp_comp_buffer.get(), tmp_cb_len, maxv);
		}
		buffer_size += tmp_cb_len;
	}
	buffer_size += 12;
	//delete [] nc_buffer;

	////////////////////////////////////////////////////////////////////////////////////
	// compress nulls
	uint null_buf_size = ((no_obj+7)/8);
	MMGuard<uchar> comp_null_buf;
	//IBHeapAutoDestructor del((void*&)comp_null_buf, *this);
	if(no_nulls > 0) {
		if(ShouldNotCompress()) {
			comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
			null_buf_size = ((no_obj + 7) / 8);
			ResetModeNullsCompressed();
		} else {
			comp_null_buf = MMGuard<uchar>((uchar*)alloc((null_buf_size + 2) * sizeof(char), BLOCK_TEMPORARY), *this);
			uint cnbl = null_buf_size + 1;
			comp_null_buf[cnbl] = 0xBA; // just checking - buffer overrun
			BitstreamCompressor bsc;
			if(CPRS_ERR_BUF == bsc.Compress((char*)comp_null_buf.get(), null_buf_size, (char*) nulls, no_obj, no_nulls)) {
				if(comp_null_buf[cnbl] != 0xBA) {
					rclog << lock << "ERROR: buffer overrun by BitstreamCompressor (N f)." << unlock;
					BHASSERT(0, "ERROR: buffer overrun by BitstreamCompressor (N f).");
				}

				comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
				null_buf_size = ((no_obj + 7) / 8);
				ResetModeNullsCompressed();
			} else {
				SetModeNullsCompressed();
				if(comp_null_buf[cnbl] != 0xBA) {
					rclog << lock << "ERROR: buffer overrun by BitstreamCompressor (N)." << unlock;
					BHASSERT(0, "ERROR: buffer overrun by BitstreamCompressor (N f).");
				}
			}
		}
		buffer_size += null_buf_size + 2;
	}
	dealloc(compressed_buf);
	compressed_buf = 0;
	compressed_buf= (uchar*)alloc(buffer_size*sizeof(uchar), BLOCK_COMPRESSED);

	if(!compressed_buf) rclog << lock << "Error: out of memory (" << buffer_size << " bytes failed). (29)" << unlock;
	memset(compressed_buf, 0, buffer_size);
	cur_buf = (uint*)compressed_buf;
	if(no_nulls > 0) {
		if(null_buf_size > 8192)
			throw DatabaseRCException("Unexpected bytes found (AttrPackN::Compress).");
#ifdef _MSC_VER
		__assume(null_buf_size <= 8192);
#endif
		*(ushort*) compressed_buf = (ushort) null_buf_size;
#pragma warning(suppress: 6385)
		memcpy(compressed_buf + 2, comp_null_buf.get(), null_buf_size);
		cur_buf = (uint*) (compressed_buf + null_buf_size + 2);
	}

	////////////////////////////////////////////////////////////////////////////////////

	*cur_buf = tmp_cb_len;
	*(_uint64*) (cur_buf + 1) = maxv;
	memcpy(cur_buf + 3, tmp_comp_buffer.get(), tmp_cb_len);
	comp_buf_size = buffer_size;
	compressed_up_to_date = true;

	CompressionStatistics stats;
	stats.new_no_obj = NoObjs();
	return stats;
}

void AttrPackN::TruncateObj( int new_objs)
{
	if(no_obj>new_objs && no_nulls>0) {
	   for(int i = new_objs; i < no_obj; i++) {
			int mask = (uint) (1) << (i & 31);
			if((nulls[i >> 5] & mask) == 1) {
				nulls[i >> 5] &= ~mask; // reset the i-th bit of the table
				no_nulls--;
	   		}
	   	}
	}
	no_obj = new_objs;
}


void AttrPackN::Uncompress(DomainInjectionManager& dim)		// Create full_data basing on compressed buf.
{
	return;

	MEASURE_FET("AttrPackN::Uncompress()");
#ifdef FUNCTIONS_EXECUTION_TIMES
	FETOperator feto1(string("aN[") + boost::lexical_cast<string>(pc_column( GetPackCoordinate())) + "].Compress(...)");
	NotifyDataPackDecompression(GetPackCoordinate());
#endif
	is_only_compressed = false;
	if(IsModeNoCompression())
		return;
	assert(compressed_buf);
	uint *cur_buf=(uint*)compressed_buf;
	if(data_full == NULL && bit_rate > 0 && value_type * no_obj)
		data_full = alloc(value_type * no_obj, BLOCK_UNCOMPRESSED);

//	int cur_val = -1, prev_val = -1;

//	int i_bit=0;		// buffer position
//	int last_val[]={-1,-1,-1,-1,-1,-1,-1,-1};

	///////////////////////////////////////////////////////////////
	// decompress nulls

	if(no_nulls > 0) {
		uint null_buf_size = 0;
		if(nulls == NULL) {
			nulls = (uint8_t*) alloc(8192, BLOCK_UNCOMPRESSED);
			nulls_bytes = 8192;
		}

		if(no_obj < 65536)
			memset(nulls, 0, 8192);
		null_buf_size = (*(ushort*) cur_buf);
		if(null_buf_size > 8192)
			throw DatabaseRCException("Unexpected bytes found in data pack (AttrPackN::Uncompress).");
		if(!IsModeNullsCompressed() ) // no nulls compression
			memcpy(nulls, (char*) cur_buf + 2, null_buf_size);
		else {
			BitstreamCompressor bsc;
			bsc.Decompress((char*) nulls, null_buf_size, (char*) cur_buf + 2, no_obj, no_nulls);
			// For tests:
#if defined(_DEBUG) || (defined(__GNUC__) && !defined(NDEBUG))
			uint nulls_counted = 0;
			for(uint i = 0; i < 2048; i++)
				nulls_counted += CalculateBinSum(nulls[i]);
			if(no_nulls != nulls_counted)
				throw DatabaseRCException("AttrPackN::Uncompress uncompressed wrong number of nulls.");
#endif
		}
		cur_buf = (uint*) ((char*) cur_buf + null_buf_size + 2);
	}

	////////////////////////////////////////////////////////////////////////////////////
	if(!IsModeValid()) {
		rclog << lock << "Unexpected byte in data pack (AttrPackN)." << unlock;
		throw DatabaseRCException("Unexpected byte in data pack (AttrPackN).");
	} else {
		if(IsModeDataCompressed() && bit_rate > 0 && *(_uint64*) (cur_buf + 1) != (_uint64) 0) {
			if(value_type == UCHAR) {
				NumCompressor<uchar> nc;
				DecompressAndInsertNulls(nc, cur_buf);
			} else if(value_type == USHORT) {
				NumCompressor<ushort> nc;
				DecompressAndInsertNulls(nc, cur_buf);
			} else if(value_type == UINT) {
				NumCompressor<uint> nc;
				DecompressAndInsertNulls(nc, cur_buf);
			} else {
				NumCompressor<_uint64> nc;
				DecompressAndInsertNulls(nc, cur_buf);
			}
		} else if(bit_rate > 0) {
			for(uint o = 0; o < no_obj; o++)
				if(!IsNull(int(o)))
					SetVal64(o, 0);
		}
	}

	compressed_up_to_date = true;	
	dealloc(compressed_buf);
	compressed_buf=0;
	comp_buf_size=0;

#ifdef FUNCTIONS_EXECUTION_TIMES
	SizeOfUncompressedDP += (rc_msize(data_full) + rc_msize(nulls));
#endif
}
