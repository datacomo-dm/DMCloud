/* Copyright (C) 2005-2008 Infobright Inc. */

#include <sstream>
#include <boost/shared_ptr.hpp>

#include "common/CommonDefinitions.h"
#include "core/RCAttrPack.h"
#include "core/tools.h"
#include "core/WinTools.h"
#include "RCAttrLoadBase.h"
#include "loader/NewValueSet.h"
#include "loader/BHLoader.h"
#include "core/DPN.h"
#include "core/DataPackImpl.h"
#include "types/RCDataTypes.h"
#include "edition/core/GlobalDataCache.h"
#include "edition/core/Transaction.h"
#include "loader/NewValuesSetBase.h"

using namespace std;
using namespace boost;

RCAttrLoadBase::RCAttrLoadBase(int a_num,int t_num,string const& a_path,int conn_mode,unsigned int s_id,bool loadapindex, DTCollation collation) throw(DatabaseRCException)
	: RCAttr(a_num, t_num, a_path, conn_mode, s_id, loadapindex,collation), illegal_nulls(false), packrow_size(MAX_PACK_ROW_SIZE), no_outliers(TransactionBase::NO_DECOMPOSITION)
{
}

RCAttrLoadBase::RCAttrLoadBase(int a_num, AttributeType a_type, int a_field_size, int a_dec_places, uint param, DTCollation collation, bool compress_lookups, string const& path_ )
	:	RCAttr(a_num, a_type, a_field_size, a_dec_places, param, collation, compress_lookups, path_), illegal_nulls(false), packrow_size(MAX_PACK_ROW_SIZE), no_outliers(TransactionBase::NO_DECOMPOSITION)
{
}

RCAttrLoadBase::~RCAttrLoadBase()
{
	LogWarnigs();
}
void RCAttrLoadBase::LoadPackIndexForLoader(const char *partname,int sessionid,bool ismerge) {
		if(ct.IsPackIndex()) {
			if(ldb_index!=NULL) delete ldb_index;
			ldb_index=new AttrIndex(attr_number,path.c_str());
			ldb_index->LoadHeader();
			ldb_index->LoadForUpdate(partname,sessionid,ismerge);
		}
}

// appendmode : 不需要参数控制文件,在最后一个分区上装入(insert sql mode)
void RCAttrLoadBase::LoadPartionForLoader(uint connid,int sessionid,bool appendmode) {
		//if(ct.IsPackIndex()) {
			if(partitioninfo!=NULL) delete partitioninfo;
			if(appendmode){ // bulid a virtual control file
				string ctlfile=path+"/loadinfo.ctl";
				if (DoesFileExist(ctlfile))
					RemoveFile( ctlfile);
				IBFile loadinfo;
				loadinfo.OpenCreate(ctlfile);
				loadinfo.WriteExact("LOADCTRL",8);
				loadinfo.WriteExact(&connid,sizeof(int));
				short len=0;
				loadinfo.WriteExact(&len,sizeof(short));
			}
			partitioninfo=new attr_partitions(attr_number,path.c_str());
			partitioninfo->SetSessionID(GetSessionId());
			if(!partitioninfo->checksession(connid)) {
				rclog << lock << "Internal error: load connection id not match: server got :" << connid << ",client request: " << partitioninfo->GetSaveID()<< unlock;
				throw DatabaseRCException("load connection id not match.");	
			}		
		//}
}
#define MAX_PACK_IN_RIAKPNODE 0x1000

int64 RCAttrLoadBase::RiakAllocPNode() {
		int64 newriaknode= _riak_cluster->KeySpaceAlloc(RIAK_PAT_NAME,table_number,attr_number);
		if(newriaknode<0) {
			rclog << lock << "Internal error: allocate packet id failed. get pnode" << newriaknode << ",tabid:" <<table_number<<",attr :"<<attr_number<< unlock;
			throw DatabaseRCException("allocate pnode from riak failed,RCAttrLoadBase::RiakAllocPNode");	
		}
		else  {
			rclog << lock << "Got pnode" << newriaknode << ",tabid:" <<table_number<<",attr :"<<attr_number<< unlock;
		}
		if(newriaknode%MAX_PACK_IN_RIAKPNODE!=0){
			rclog << lock << "Internal error: allocate packet id failed. get pnode" << newriaknode << ",tabid:" <<table_number<<",attr :"<<attr_number<< unlock;
			throw DatabaseRCException("allocate pnode from riak failed,RCAttrLoadBase::RiakAllocPNode");	
		}
		SetSaveFileLoc(GetCurSaveLocation(),(int)(newriaknode>>32));
		SetSavePosLoc(GetCurSaveLocation(),(int)(newriaknode&0xffffffff));
}

void RCAttr::ResetRiakPNode() {
		SetSaveFileLoc(GetCurSaveLocation(),-1);
		SetSavePosLoc(GetCurSaveLocation(),0);
}

void RCAttrLoadBase::LoadPackInfoForLoader()
{
	if(!GetPackInfoCollapsed())
		return;

	WaitForSaveThreads();
	BHASSERT(FileFormat()==10, "should be 'file_format==10'");
	string fn;
	if(GetDictOffset() != 0) {
		IBFile fattr;
		try {
			fattr.OpenReadOnly(fn = AttrFileName(file));
			int fin_size = int(fattr.Seek(0, SEEK_END));
			char* buf_ptr = new char[(fin_size - GetPackOffset()) + 1];
			fattr.Seek(GetPackOffset(), SEEK_SET);
			fattr.ReadExact(buf_ptr, fin_size - GetPackOffset());

			if(!Type().IsLookup()) {
				if(GetDictOffset() != 0 && (fin_size - GetDictOffset()))
					LoadDictionaries(buf_ptr);
			} else {
				if(process_type == ProcessType::BHLOADER)
					dic = GlobalDataCache::GetGlobalDataCache().GetObject<FTree> (FTreeCoordinate(table_number,
							attr_number), bind(&RCAttrLoadBase::LoadLookupDictFromFile, this));
				else
					dic = ConnectionInfoOnTLS->GetTransaction()->GetFTreeForUpdate(FTreeCoordinate(table_number,
							attr_number));

				if(!dic) {
					dic = boost::shared_ptr<FTree> (new FTree());
					LoadLookupDictFromFile();
					ConnectionInfoOnTLS->GetTransaction()->PutObject(FTreeCoordinate(table_number, attr_number), dic);
				}
			}

			delete[] buf_ptr;
		} catch (DatabaseRCException&) {
			rclog << lock << "Internal error: unable to load column dictionary from " << fn << ". " << GetErrorMessage(
					errno) << unlock;
			throw;
		}
	}

	if(NoPack() > 0) {
		int const ss(SplicedVector<DPN>::SPLICE_SIZE);
		packs_omitted = ((NoPack() - 1) / ss) * ss;
		// load start at last save pack:(align to ss)
		// dma-521 retore packs_ommitted
		//packs_omitted=(partitioninfo->getsavepartinfo(NULL).getsavepack()/ss)*ss;
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( ! ( packs_omitted % ss ) );
	} else
		packs_omitted = 0;
	if(NoPack()-packs_omitted > GetNoPackPtr()) {
		uint n_pack_ptrs = NoPack() - packs_omitted;
		if(n_pack_ptrs % SplicedVector<DPN>::SPLICE_SIZE) {
			n_pack_ptrs += SplicedVector<DPN>::SPLICE_SIZE;
			n_pack_ptrs -= (n_pack_ptrs % SplicedVector<DPN>::SPLICE_SIZE);
		}
		SetNoPackPtr(n_pack_ptrs);
		dpns.resize(GetNoPackPtr());
	}
	//设置装入位置
	int cur_save_loc = GetCurSaveLocation();
	partitioninfo->SetSessionID(GetSaveSessionId());
	attr_partinfo &savepart=partitioninfo->getsavepartinfo(NULL);

	std::vector<int64> rmfiles;
	//restore uncommitted data
	// rollback last uncommited transaction
	//如果是同一个会话,则不回退,否则回退上次状态并update sessionid
	bool appendmode=savepart.preload(GetSaveSessionId(),rmfiles,table_number);
	for(int i=0;i<rmfiles.size();i++) {
		//clear packfiles
		rclog << lock << "RCAttrLoadBase::LoadPackInfoForLoader()1.Find isolated PNode,has released :" << rmfiles[i]<<unlock;
	}
	if(savepart.isempty()||NoPack()<1 || savepart.getfiles()==0) {
		//新分区 
		ResetRiakPNode();
	}
	else  { 
		//不连续
		if(savepart.getsavepack()!=NoPack()-1) {
			//TODO: 检查是否需要截断文件,删除上次未提交的数据
			ResetRiakPNode();
		}
		else {
			//新装入紧接上次最后装入的分区,如何连续装入,
			// 找到未提交的起始点!
			//为什么不用ABS机制保存的位置�?因为每个分区有自己的存储位置
			// last pack there is not real data
			
			//如果已提交的最后一个包有pack_file(并且未满),则直接使用(未提交部分不会覆盖这个包的DPN数据(ABS)
			DPN &dpn(dpns[savepart.getlastpack()- packs_omitted]);
			if(dpn.pack_file>=0 && dpn.no_objs!=65535) {
				SetSaveFileLoc(cur_save_loc,dpn.pack_file);
				SetSavePosLoc(cur_save_loc,dpn.pack_addr);
			}
			else {
				//getsavefilepos() return -1,means a invalid pos
				if(savepart.getlastsavepos()==-1 || savepart.lastfile()==-1) 
					ResetRiakPNode();
				else  {
					int64 riakkey=savepart.lastfile()+(savepart.getlastsavepos()&0xfff)+1;
					SetSaveFileLoc(cur_save_loc,riakkey>>32);
					SetSavePosLoc(cur_save_loc,riakkey&0xffffffff);
				}
			}
		}
	}
	
	//ExpandDPNArrays();
	SetPackInfoCollapsed(false);
}

void RCAttrLoadBase::ExpandDPNArrays()
{
	if((NoPack() - packs_omitted + 1) > GetNoPackPtr()) {
		// Create new package table (enlarged by one) and copy old values
		uint old_no_pack_ptr = GetNoPackPtr();
		uint n_pack_ptrs = (NoPack() - packs_omitted + 1);
		if(n_pack_ptrs % SplicedVector<DPN>::SPLICE_SIZE) {
			n_pack_ptrs += SplicedVector<DPN>::SPLICE_SIZE;
			n_pack_ptrs -= (n_pack_ptrs % SplicedVector<DPN>::SPLICE_SIZE);
		}
		SetNoPackPtr(n_pack_ptrs);
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !( ((NoPack() - packs_omitted + 1) > GetNoPackPtr()) || ( n_pack_ptrs % SplicedVector<DPN>::SPLICE_SIZE ) ) );
		WaitForSaveThreads();

		dpns.resize(GetNoPackPtr());

		if(dpns.empty()) {
			rclog << lock << "Error: out of memory (" << GetNoPackPtr() - old_no_pack_ptr << " new packs failed). (25)"
					<< unlock;
			return;
		}
	}
}


DPN RCAttrLoadBase::LoadDataPackN(const DPN& source_dpn, NewValuesSetBase* nvs, _int64& load_min, _int64& load_max,
		int& load_nulls)
{
	DPN dpn(source_dpn);
	double& real_sum = *(double*) &dpn.sum_size;
	uint cur_nulls = dpn.no_nulls;
	uint cur_obj = dpn.GetNoObj();
	bool nulls_conferted = false;
	_int64 null_value = -1;
	int nonv = nvs->NoValues();
	bool is_real_type = ATI::IsRealType(TypeName());
	int load_obj = nvs->NoValues();

	if(!ATI::IsRealType(TypeName())) {
		load_min = PLUS_INF_64;
		load_max = MINUS_INF_64;
	} else {
		load_min = *(_int64*) &PLUS_INF_DBL;
		load_max = *(_int64*) &MINUS_INF_DBL;
	}

	for(int i = 0; i < nonv; i++) {
		if(nvs->IsNull(i)) {
			if(Type().GetNullsMode() == NO_NULLS) {
				nulls_conferted = true;
				illegal_nulls = true;
				_int64 v = 0;
				if(ATI::IsStringType(TypeName())) {
					if(null_value == -1)
						null_value = v = EncodeValue_T(ZERO_LENGTH_STRING, 1);
					else
						v = null_value;
				} else {
					null_value = v = 0;
				}
			} else
				load_nulls++;
		}
	}

	if(!is_real_type) {
		_int64 tmp_sum;
		nvs->GetIntStats(load_min, load_max, tmp_sum);
		dpn.sum_size += tmp_sum;
		if(nulls_conferted) {
			if(load_min > null_value)
				load_min = null_value;
			if(load_max < null_value)
				load_max = null_value;
		}
	} else {
		double tmp_sum;
		nvs->GetRealStats(*(double*) &load_min, *(double*) &load_max, tmp_sum);
		real_sum += tmp_sum;
		if(nulls_conferted) {
			if(*(double*) &load_min > *(double*) &null_value)
				*(double*) &load_min = *(double*) &null_value;
			if(*(double*) &load_max < *(double*) &null_value)
				*(double*) &load_max = *(double*) &null_value;
		}
	}

	if((cur_nulls + load_nulls) == 0 && load_min == load_max && (cur_obj == 0 || (dpn.local_min == load_min
			&& dpn.local_max == load_max))) {
		// no need to store any values - uniform package
		dpn.pack_mode = PACK_MODE_TRIVIAL;
		dpn.is_stored = false;
		dpn.pack_file = PF_NOT_KNOWN; // will not be used
		dpn.local_min = load_min;
		dpn.local_max = load_max;
		// sum is already calculated
	} else if(load_nulls == load_obj && (dpn.pack_file == PF_NO_OBJ || dpn.pack_file == PF_NULLS_ONLY)) {
		// no need to store any values - uniform package (nulls only)
		dpn.pack_mode = PACK_MODE_TRIVIAL;
		dpn.is_stored = false;
		dpn.pack_file = PF_NULLS_ONLY;
		dpn.local_min = PLUS_INF_64;
		dpn.local_max = MINUS_INF_64;
		dpn.sum_size = 0;
		// uniqueness status not changed
	} else {
		_uint64 new_max_val = 0;
		if(cur_obj == 0 || dpn.pack_mode == PACK_MODE_TRIVIAL || dpn.pack_mode == 3) {
			// new package (also in case of expanding so-far-uniform package)
			_uint64 uniform_so_far = 0;
			if(dpn.pack_file == PF_NULLS_ONLY) {
				uniform_so_far = (_uint64) NULL_VALUE_64;
				dpn.local_min = load_min;
				dpn.local_max = load_max;
			} else {
				if(ATI::IsRealType(TypeName())) {
					uniform_so_far = dpn.local_min; // fill with uniform-so-far
				} else if(dpn.local_min > load_min)
					uniform_so_far = _uint64(dpn.local_min - load_min);

				if(!ATI::IsRealType(TypeName())) {
					if(cur_obj == 0 || dpn.local_min > load_min)
						dpn.local_min = load_min;
					if(cur_obj == 0 || dpn.local_max < load_max)
						dpn.local_max = load_max;
				} else {
					if(cur_obj == 0 || *(double*) &(dpn.local_min) > *(double*) &(load_min))
						dpn.local_min = load_min;
					if(cur_obj == 0 || *(double*) &(dpn.local_max) < *(double*) &(load_max))
						dpn.local_max = load_max;
				}
			}

			BHASSERT(dpn.pack, "'dpn.pack' should not be null");
			if(ATI::IsRealType(TypeName())) {
				// full 64-bit scope
				((AttrPackN*) dpn.GetPackN())->Prepare(cur_obj + load_obj, _uint64(0xFFFFFFFFFFFFFFFFLL));
			} else {
				new_max_val = _uint64(dpn.local_max - dpn.local_min);
				((AttrPackN*) dpn.GetPackN())->Prepare(cur_obj + load_obj, new_max_val);
			}
			dpn.pack_file = PF_NOT_KNOWN;
			dpn.pack_mode = PACK_MODE_IN_MEMORY;
			dpn.is_stored = true;
			// Now fill the beginning of the table by so far uniform values (if there is any beginning)
			if(cur_obj > 0) {
				if(uniform_so_far != NULL_VALUE_64) {
					if((dpn.local_min != dpn.local_max) || (load_nulls > 0))
						for(int i = 0; i < (int) cur_obj; i++)
							((AttrPackN*) dpn.GetPackN())->SetVal64(i, uniform_so_far);
				} else {
					for(int i = 0; i < (int) cur_obj; i++) {
						dpn.GetPackN()->SetNull(i);
					}
				}
			}
		} else {
			// expand existing package
			//fix JIRA DMAPP-1100,packn rollbacked rows not reset while re-insert.
			((AttrPackN*) dpn.pack.get())->TruncateObj(cur_obj);
			if(ATI::IsRealType(TypeName())) {
				if(*(double*) &dpn.local_min > *(double*) &load_min)
					*(double*) &dpn.local_min = *(double*) &load_min;
				if(*(double*) &dpn.local_max < *(double*) &load_max)
					*(double*) &dpn.local_max = *(double*) &load_max;
				((AttrPackN*) dpn.GetPackN())->Expand(cur_obj + load_obj, _uint64(0xFFFFFFFFFFFFFFFFLL), 0);
			} else {
				_int64 offset = 0;
				if(dpn.local_min > load_min) {
					offset = dpn.local_min;
					offset -= load_min;
					dpn.local_min = load_min;
				}
				if(dpn.local_max < load_max)
					dpn.local_max = load_max;
				BHASSERT(dpn.pack, "'dpn.pack' should not be null");
				new_max_val = _uint64(dpn.local_max - dpn.local_min);
				((AttrPackN*) dpn.GetPackN())->Expand(cur_obj + load_obj, new_max_val, offset);
			}
		}

		_uint64 v = 0;
		int obj = 0;
		bool isnull;
		for(int i = 0; i < load_obj; i++) {
			isnull = nvs->IsNull(i);
			if(isnull && Type().GetNullsMode() == NO_NULLS) {
				isnull = false;
				v = null_value;
			} else
				v = *(_uint64*) nvs->GetDataBytesPointer(i);
			obj = i + cur_obj;
			if(dpn.local_min == dpn.local_max && !ATI::IsRealType(TypeName())) {// special case: no data stored except nulls
				if(isnull)
					dpn.pack->SetNull(obj);
			} else {
				if(!ATI::IsRealType(TypeName()))
					v -= dpn.local_min;
				if(isnull) {
					//((AttrPackN*)dpns[NoPack()-packs_omitted-1].pack)->SetVal64(obj, 0);
					((AttrPackN*) dpn.GetPackN())->SetNull(obj);
				} else
					((AttrPackN*) dpn.GetPackN())->SetVal64(obj, v);
			}
		}
	}
	return dpn;
}

bool RCAttrLoadBase::UpdateGlobalMinAndMaxForPackN(const DPN& dpn)
{
	return UpdateGlobalMinAndMaxForPackN(dpn.GetNoObj(), dpn.local_min, dpn.local_max, dpn.no_nulls);
}

bool RCAttrLoadBase::UpdateGlobalMinAndMaxForPackN(int no_obj, _int64 load_min, _int64 load_max, int load_nulls)
{
	bool is_exclusive = false;
	if(load_nulls != no_obj) {
		if(NoObj() == 0) {
			SetMinInt64(load_min);
			SetMaxInt64(load_max);
			is_exclusive = true;
		} else {
			if(!ATI::IsRealType(TypeName())) {
				is_exclusive = (GetMinInt64() > load_max || GetMaxInt64() < load_min);
				if(GetMinInt64() > load_min)
					SetMinInt64(load_min);
				if(GetMaxInt64() < load_max)
					SetMaxInt64(load_max);
			} else {
				_int64 a_min = GetMinInt64();
				_int64 a_max = GetMaxInt64();
				is_exclusive = (*(double*) (&a_min) > *(double*) (&load_max) || *(double*) (&a_max)
						< *(double*) (&load_min));
				if(*(double*) (&a_min) > *(double*) (&load_min))
					SetMinInt64(load_min);
				if(*(double*) (&a_max) < *(double*) (&load_max))
					SetMaxInt64(load_max); // 1-level statistics
			}
		}
	}
	return is_exclusive;
}

bool RCAttrLoadBase::HasRepetitions(DPN & new_dpn, const DPN & old_dpn, int load_obj, int load_nulls,
		NewValuesSetBase *nvs)
{
	bool has_repetition = false;
	std::map<int,int> nfblocks;
	Filter f_val(new_dpn.local_max - new_dpn.local_min + 1,nfblocks);
	f_val.Reset();
	if(new_dpn.local_min == new_dpn.local_max) {
		if(old_dpn.GetNoObj() + load_obj - new_dpn.no_nulls - load_nulls > 1) // more than one non-null uniform object?
			new_dpn.repetition_found = true;
	} else {
		_uint64 v = 0;
		for(int i = 0; i < load_obj; i++) {
			if(nvs->IsNull(i) == false) {
				BHASSERT_WITH_NO_PERFORMANCE_IMPACT(*(_int64*)nvs->GetDataBytesPointer(i) <= new_dpn.local_max);
				BHASSERT_WITH_NO_PERFORMANCE_IMPACT(*(_int64*)nvs->GetDataBytesPointer(i) >= new_dpn.local_min);
				v = *(_int64*) nvs->GetDataBytesPointer(i) - new_dpn.local_min;
				if(f_val.Get(v)) {
					has_repetition = true;
					break;
				}
				f_val.Set(v);
			}
		}
	}
	return has_repetition;
}

void RCAttrLoadBase::UpdateUniqueness(const DPN& old_dpn, DPN& new_dpn, _int64 load_min, _int64 load_max,
		int load_nulls, bool is_exclusive, NewValuesSetBase* nvs)
{
	int load_obj = nvs->NoValues();

	if((old_dpn.no_nulls + load_nulls) == 0 && load_min == load_max && (old_dpn.GetNoObj() == 0 || (old_dpn.local_min
			== load_min && old_dpn.local_max == load_max))) {
		if(IsUniqueUpdated() && IsUnique() && is_exclusive) {
			if(load_obj - load_nulls == 1)
				SetUnique(true); // only one new value, different than the previous
			else
				SetUnique(false); // at least two identical values found
		} else
			SetUniqueUpdated(false); // may be unique or not
	}

	if(IsUniqueUpdated() && IsUnique() && is_exclusive // there is a chance for uniqueness
			&& !ATI::IsRealType(TypeName()) // temporary limitations
			&& _int64(new_dpn.local_max - new_dpn.local_min) > 0 && _int64(new_dpn.local_max - new_dpn.local_min)
			< 8000000) {
		new_dpn.repetition_found = HasRepetitions(new_dpn, old_dpn, load_obj, load_nulls, nvs);
		if(new_dpn.repetition_found)
			SetUnique(false); // else remains true
	} else
		SetUniqueUpdated(false);
}

void RCAttrLoadBase::UpdateUniqueness(DPN& dpn, bool is_exclusive)
{

	if(dpn.no_nulls == 0 && dpn.local_min == dpn.local_max) {
		if(IsUniqueUpdated() && IsUnique() && is_exclusive) {
			if(dpn.GetNoObj() == 1)
				SetUnique(true); // only one new value, different than the previous
			else
				SetUnique(false); // at least two identical values found
		} else
			SetUniqueUpdated(false); // may be unique or not
	}

	if(IsUniqueUpdated() && IsUnique() && is_exclusive // there is a chance for uniqueness
			&& !ATI::IsRealType(TypeName()) // temporary limitations
			&& _int64(dpn.local_max - dpn.local_min) > 0 && _int64(dpn.local_max - dpn.local_min) < 8000000) {
		if(dpn.repetition_found)
			SetUnique(false); // else remains true
	} else
		SetUniqueUpdated(false);
}

void RCAttrLoadBase::InitKNsForUpdate()
{
	if(process_type == ProcessType::DATAPROCESSOR) {
		if(PackType() == PackN) {
			if(rsi_hist_update == NULL) {
				rsi_hist_update = new RSIndex_Hist();
				rsi_hist_update->SetID(RSIndexID(RSI_HIST, table_number, attr_number));
			}
		} else if(rsi_cmap_update == NULL && !RequiresUTFConversions(ct.GetCollation())) {
			rsi_cmap_update = new RSIndex_CMap();
			rsi_cmap_update->SetID(RSIndexID(RSI_CMAP, table_number, attr_number));
		}
	} else {
		if(PackType() == PackN) {
			if(rsi_manager && rsi_hist_update == NULL)
				rsi_hist_update = (RSIndex_Hist*) rsi_manager->GetIndexForUpdate(RSIndexID(RSI_HIST, table_number,
						attr_number), GetCurReadLocation());
		} else {
			if(rsi_manager && rsi_cmap_update == NULL && !RequiresUTFConversions(ct.GetCollation()))
				rsi_cmap_update = (RSIndex_CMap*) rsi_manager->GetIndexForUpdate(RSIndexID(RSI_CMAP, table_number,
						attr_number), GetCurReadLocation());
		}
	}
}

bool RCAttrLoadBase::PreparePackForLoad()
{
	bool new_pack_created = false;
	/* adjust partition loading */
	//int current_pack = partitioninfo->getsavepartinfo(NULL).getsavepack()-packs_omitted;
	attr_partinfo &savepart=partitioninfo->getsavepartinfo(NULL);
	int current_pack = savepart.getsavepack();
	//强制新建 pack
	//只有最后一个分区的增量装入使用继续填充数据块的策略
	bool force_create=(current_pack !=NoPack() - 1) || savepart.isempty();
	//if(NoPack() == 0 || dpns[NoPack() - packs_omitted - 1].GetNoObj() == GetPackrowSize()) {
	// check new partion
	if(NoPack() == 0 || force_create || dpns [current_pack - packs_omitted].GetNoObj() == GetPackrowSize() ) {
		CreateNewPackage();
		new_pack_created = true;
		if(force_create)
			rccontrol << lock << "Partition splitted while loading,partname :" << partitioninfo->getsavepartinfo(NULL).name()<<" packs:"<<NoPack()<< unlock;
	} else
		LockPackForUse(NoPack() - packs_omitted - 1);
		//LockPackForUse(current_pack-packs_omitted);
	return new_pack_created;
}

int RCAttrLoadBase::LoadData(NewValuesSetBase* nvs, bool copy_forced, bool force_saveing_pack, bool pack_already_prepared)
{
	MEASURE_FET(string("a[") + boost::lexical_cast<string>(attr_number) + "].LoadData(...)");

	InitKNsForUpdate();

	if(nvs->NoValues() == 0)
		return 0;
	LoadPackInfoForLoader();
	attr_partinfo &savepart=partitioninfo->getsavepartinfo(NULL);	
	// called at RCTableLoad::LoadData ,not here!
	// LoadPackIndexForLoader((short)GetSaveSessionId(),GetSaveSessionId());
	// save to new file?
	if(NoPack()>0 &&  (savepart.getsavepack()!=NoPack()-1  || savepart.isempty())) {
		// 
		//new partition on a none empty table,create new file!		
		ResetRiakPNode() ;
		//没有看到这个提示�!
		// TODO : dma-712:
		// 2013-06-28 16:23:25 New partition loading ,partname:20130110 save loc: 2,other save loc: 1,p:807NoPack:792
		
		rclog << lock << "New partition loading ,partname:" << partitioninfo->getsavepartinfo(NULL).name()
			<<" save loc: "<< GetSaveFileLoc(GetCurSaveLocation())<< ",other save loc: "<<GetSaveFileLoc(1-GetCurSaveLocation())<< ",p:"<<savepart.getsavepack()<<"NoPack:"<<NoPack()<<unlock;
		if(NoPack()<savepart.getsavepack()) {
			throw DatabaseRCException("nopack < part save pack,data partition maybe need to reload.");
		}
	}
	bool new_pack_created = pack_already_prepared ? true : PreparePackForLoad();
	
	

	//add partition support here:
	// find last loading pack,fill the pack until full
	//  create new pack ordered continue to total_pack of table
	int load_nulls = 0;
	int load_obj = nvs->NoValues();
	/* adjust partition loading */

	int current_pack = NoPack() - packs_omitted - 1;

	// dma-521 retore origin code
	//int current_pack = partitioninfo->getsavepartinfo(NULL).getsavepack()-packs_omitted;
	uint cur_obj = (uint) dpns[current_pack].GetNoObj();

	if(this->PackType() == PackN) {
		_int64 load_min, load_max;
		DPN new_dpn = LoadDataPackN(dpns[current_pack], nvs, load_min, load_max, load_nulls);
		SetNaturalSizeSaved(GetNaturalSizeSaved() + nvs->SumarizedSize()); //this is needed for LOOKUP column
		bool is_exclusive = UpdateGlobalMinAndMaxForPackN(nvs->NoValues(), load_min, load_max, load_nulls);
		UpdateUniqueness(dpns[current_pack], new_dpn, load_min, load_max, load_nulls, is_exclusive, nvs);
		dpns[current_pack] = new_dpn;
	} else if(PackType() == PackS) {
		DPN& dpn(dpns[current_pack]);
		for(int i = 0; i < nvs->NoValues(); i++) {
			if(nvs->IsNull(i)) {
				if(Type().GetNullsMode() == NO_NULLS) {
					illegal_nulls = true;
				} else
					load_nulls++;
			}
		}
		dpn.natural_save_size = uint(nvs->SumarizedSize());
		SetNaturalSizeSaved(GetNaturalSizeSaved() + nvs->SumarizedSize());
		AttrPackS*& packS = ((AttrPackS*&) dpn.pack);

		if(load_nulls == nvs->NoValues() && (dpn.pack_file == PF_NO_OBJ || dpn.pack_file == PF_NULLS_ONLY)) {
			// no need to store any values - uniform package
			dpn.pack_mode = PACK_MODE_TRIVIAL;
			dpn.is_stored = false;
			dpn.pack_file = PF_NULLS_ONLY;
			dpn.local_min = GetDomainInjectionManager().GetCurrentId();
			dpn.local_max = -1;
			dpn.sum_size = 0;
		} else {
			if(dpn.pack_file == PF_NULLS_ONLY)
				dpn.local_min = 0;
			BHASSERT(packS, "'packS' should not be null"); //pack object must exist
			SetUnique(false); // we will not check uniqueness for strings now
			SetUniqueUpdated(false);
			if(cur_obj == 0 || dpn.pack_mode == 0 || dpn.pack_mode == 3)// new package (also in case of expanding so-far-null package)
			{
				packS->Prepare(dpn.no_nulls); // data size: one byte for each null, plus more...
				dpn.pack_mode = PACK_MODE_IN_MEMORY;
				dpn.is_stored = true;
				dpn.pack_file = PF_NOT_KNOWN;
			}
			if(!new_pack_created && current_pack == 0 && dpn.local_min == 0 && dpn.local_max == -1
					&& GetPackOntologicalStatus(current_pack) != NULLS_ONLY) {
				RCDataTypePtr min_ptr;
				RCDataTypePtr max_ptr;
				GetMinMaxValuesPtrs(current_pack, min_ptr, max_ptr);
				if(!min_ptr->IsNull())
					strncpy((uchar*) (&dpn.local_min), *(RCBString*) (min_ptr.get()), sizeof(_int64));
				if(!max_ptr->IsNull())
					strncpy((uchar*) (&dpn.local_max), *(RCBString*) (min_ptr.get()), 
					(uint)min( (*(RCBString*)(min_ptr.get())).size(), sizeof(_int64)) );
			}

			RCBString min_s;
			RCBString max_s;
			ushort max_size = 0;
			nvs->GetStrStats(min_s, max_s, max_size);
			// JIRA: DMA-670 last pack of last loading not committed.
			if(packS->NoObjs()+nvs->NoValues()>MAX_NO_OBJ)
				packS->TruncateObj(MAX_NO_OBJ-nvs->NoValues());
			packS->Expand(nvs->NoValues());
			bool isnull = false;
			char const* v = 0;
			uint size = 0;
			RCBString zls(ZERO_LENGTH_STRING, 0);
			for(int i = 0; i < nvs->NoValues(); i++) {
				isnull = nvs->IsNull(i);
				if(isnull && Type().GetNullsMode() == NO_NULLS) {
					isnull = false;
					v = ZERO_LENGTH_STRING;
					size = 0;
					if(min_s.IsNull() || min_s > zls)
						min_s = zls;
					if(max_s.IsNull() || max_s < zls)
						max_s = zls;
				} else {
					v = nvs->GetDataBytesPointer(i);
					size = nvs->Size(i);
				}
				packS->BindValue(isnull, (uchar*) v, size);
			}

			if(new_pack_created || GetPackOntologicalStatus(current_pack) == NULLS_ONLY) {
				if(!min_s.IsNull())
					SetPackMin(current_pack, min_s);
				if(!max_s.IsNull())
					SetPackMax(current_pack, max_s);
				dpn.sum_size = max_size;
			} else {
				if(!min_s.IsNull() && !min_s.GreaterEqThanMinUTF(dpn.local_min, Type().GetCollation(), true))
					SetPackMin(current_pack, min_s);

				if(!max_s.IsNull() && !max_s.LessEqThanMaxUTF(dpn.local_max, Type().GetCollation(), true))
					SetPackMax(current_pack, max_s);

				if(dpn.sum_size < max_size)
					dpn.sum_size = max_size;
			}
			dpn.pack_mode = PACK_MODE_IN_MEMORY;
			dpn.is_stored = true;
		}
	}

	DPN& dpn(dpns[current_pack]);
	dpn.no_nulls += load_nulls;
	dpn.no_objs = ushort(cur_obj + load_obj - 1);
	// in case of un full pack loading as this buffer fetched end 
	partitioninfo->getsavepartinfo(NULL).setsaveobjs(dpn.no_objs+1);
	//SetNoObj(NoObj() + load_obj);
	//FIX DMA-495 DMA-494
	//SetNoObj(  ((_int64)current_pack<<16)+load_obj);
	//FIX DMA-507
	//SetNoObj(  ((_int64)(current_pack+packs_omitted)<<16)+load_obj);
	// FIX DMA-607: inserted record lost(one row once)
	SetNoObj(  ((_int64)(current_pack+packs_omitted)<<16)+dpn.no_objs+1);
	
	SetNoNulls(NoNulls() + load_nulls);

	if(GetPackrowSize() != MAX_PACK_ROW_SIZE && dpn.GetNoObj() == GetPackrowSize()) {
		SetNoObj(NoObj() + (MAX_PACK_ROW_SIZE - GetPackrowSize()));
	}

	if(PackType() == PackS && (copy_forced || dpn.GetNoObj() != GetPackrowSize()))
		((AttrPackS&) *dpn.pack).CopyBinded();

	if(dpn.GetNoObj() == GetPackrowSize() || force_saveing_pack)
		SavePack(current_pack);
	return 0;
}

void RCAttrLoadBase::SavePartitonPacksIndex(DMThreadData *tl) {
	//Write all index data.
	if(ct.IsPackIndex()) 
	{
		ldb_index->ClearMergeDB();
		if(!partitioninfo->IsLastLoad()) {
			apindex *pindex=ldb_index->GetIndex(GetLoadPartName(),GetSaveSessionId()); // _w db
			rclog << lock << "LOG: Write packindex begin "<<path<<" colid "<< attr_number <<unlock;
			pindex->WriteMap();
			rclog << lock << "LOG: Write packindex end "<<path<<" colid "<< attr_number <<unlock;		
		}else {
		    // packindex的合并过程:
		    // 1. fork 原来的数据库
		    // 2. merge 原来的数据库和当前正在写的数据库(_w)到新的数据库(_mrg)中
		    // ---> 3. 提交过程在mysqld中，将_mrg重命名操作
			rclog << lock << "LOG: Generate new db begin "<<path<<" colid "<< attr_number <<unlock;

            
    		mytimer _tm_save_packindex;             // 记录时间用
    		_tm_save_packindex.Restart();
            
            // fork packindex
            std::string dbname=apindex::GetDBPath(attr_number,GetLoadPartName(),path,false);
			std::string tmpreaddb=dbname+"_fork";
			apindex::ForkDB(dbname.c_str(),tmpreaddb.c_str());

            _tm_save_packindex.Stop();
            #if defined(_DEBUG) || (defined(__GNUC__) && !defined(NDEBUG))
			rclog << lock << "RCAttrLoadBase::SavePartitonPacksIndex fork "<<dbname <<" to "<<tmpreaddb << ", use time ["<<_tm_save_packindex.GetTime()<<"] (S) , "<<path<<" colid "<< attr_number <<unlock;            
            #endif
            
			apindex sindex(tmpreaddb,"");
			sindex.ReloadDB();
			char mergedbname[300];
			apindex::GetMergeIndexPath(dbname.c_str(),mergedbname);
			apindex *pindex=ldb_index->GetIndex(GetLoadPartName(),GetSaveSessionId()); // _w db
			rclog << lock << "MapSize:"<<pindex->GetMapSize()<<unlock;
			apindex mergeidx(mergedbname,"");

            _tm_save_packindex.Restart();            
            mergeidx.MergeFromHashMap(sindex,*pindex);

            _tm_save_packindex.Stop();
            #if defined(_DEBUG) || (defined(__GNUC__) && !defined(NDEBUG))
            rclog << lock << "RCAttrLoadBase::SavePartitonPacksIndex merge "<<tmpreaddb <<" & "<< pindex->GetPath()<< " to "<< mergedbname <<" , use time ["<<_tm_save_packindex.GetTime()<<"] (S) , "<<path<<" colid "<< attr_number <<unlock;                        
            #endif            
            
			mergeidx.ReloadDB();
			ldb_index->ClearTempDB(GetLoadPartName(),GetSaveSessionId());
			sindex.ClearDB(false);
			rclog << lock << "LOG: Generate new db end "<<path<<" colid "<< attr_number <<unlock;		
		}
	}
}

void RCAttrLoadBase::UnlockPackFromUse(unsigned pack_no)
{
	//TODO: [michal] Refactoring
	DPN& dpn(dpns[pack_no]);
	if(process_type == ProcessType::BHLOADER || process_type == ProcessType::DATAPROCESSOR) {
		//for(int i = 0; i < dpn.no_pack_locks; i++)	don't unlock to avoid a race with memory manager
		//	dpn.pack->Unlock();							the pack ought to be deleted on following reset so can be locked
		dpn.no_pack_locks = 0;
		dpn.pack.reset();
		if(dpn.pack_mode == PACK_MODE_IN_MEMORY)
			dpn.pack_mode = PACK_MODE_UNLOADED;
	} else if(dpn.pack) {
		PackCoordinate pack_coord(table_number, attr_number, pack_no + packs_omitted,0,0);
		for(int i = 0; i < dpn.no_pack_locks - 1; i++)
			dpn.pack->Unlock();
		TrackableObject::UnlockAndResetPack(dpn.pack);
		dpn.no_pack_locks = 0;
		if(dpn.pack_mode == PACK_MODE_IN_MEMORY)
			dpn.pack_mode = PACK_MODE_UNLOADED;
		else if(!ShouldExist(pack_no))
			ConnectionInfoOnTLS->GetTransaction()->DropLocalObject(pack_coord);
	}
}

void RCAttrLoadBase::LockPackForUse(unsigned pack_no)
{
	//TODO: [michal] Refactoring
	assert((int)pack_no < NoPack());
	PackCoordinate pack_coord(table_number, attr_number, pack_no + packs_omitted,0,0);
	DPN& dpn(dpns[pack_no]);
	if((process_type == ProcessType::BHLOADER || process_type == ProcessType::DATAPROCESSOR)) {
		if(!!dpn.pack) {
			dpn.pack->Lock();
			dpn.no_pack_locks++;
		} else {
			if(PackType() == PackS){
				if(Type().GetCompressType() == Compress_DEFAULT){
					dpn.pack = AttrPackPtr(new AttrPackS(pack_coord, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
				}
				else{
					dpn.pack = AttrPackPtr(new AttrPackS_Zip(pack_coord, TypeName(), GetInsertingMode(), GetNoCompression(), 0));	
				}	
			}
			else{
				if(Type().GetCompressType()== Compress_DEFAULT){
					dpn.pack = AttrPackPtr(new AttrPackN(pack_coord, TypeName(), GetInsertingMode(), 0));
				}
				else{
					dpn.pack = AttrPackPtr(new AttrPackN_Zip(pack_coord, TypeName(), GetInsertingMode(), 0));
				}
			}
			dpn.no_pack_locks = 1;
			LoadPack(pack_no);
		}
	} else {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(ConnectionInfoOnTLS.IsValid());
		PackAllocator pa(NULL, PackN); /* FIXME: Amok, this is only a dummy object */
		if(!dpn.pack) {
			dpn.pack = ConnectionInfoOnTLS->GetTransaction()->GetAttrPackForUpdate(pack_coord);
			if(!!dpn.pack && dpn.pack_mode == PACK_MODE_UNLOADED)
				dpn.pack_mode = PACK_MODE_IN_MEMORY;
		} else
			dpn.pack->Lock();

		if(!!dpn.pack) {
			dpn.no_pack_locks++;
		} else {
			if(PackType() == PackS){
				if(Type().GetCompressType() == Compress_DEFAULT){
					dpn.pack = AttrPackPtr(new AttrPackS(pack_coord, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
				}
				else{
					dpn.pack = AttrPackPtr(new AttrPackS_Zip(pack_coord, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
				}
			}
			else{
				if(Type().GetCompressType() == Compress_DEFAULT){
					dpn.pack = AttrPackPtr(new AttrPackN(pack_coord, TypeName(), GetInsertingMode(), 0));
				}
				else{
					dpn.pack = AttrPackPtr(new AttrPackN_Zip(pack_coord, TypeName(), GetInsertingMode(), 0));	
				}
			}
			dpn.no_pack_locks = 1;
			ConnectionInfoOnTLS->GetTransaction()->PutObject(pack_coord, dpn.pack);
			LoadPack(pack_no);
		}
	}
}

DPN& RCAttrLoadBase::CreateNewPackage()
{
	//TODO: [Michal] Refactoring
	MEASURE_FET("RCAttrLoadBase::CreateNewPackage()");
	ExpandDPNArrays();
	int pack(NoPack() - packs_omitted);
	DPN& dpn(dpns[pack]);
	if(PackType() == PackN) {
		dpn.local_min = PLUS_INF_64;
		dpn.local_max = MINUS_INF_64;

		BHASSERT(!dpn.pack, "'dpns[no_pack-packs_omitted].pack' should be null");
		if((process_type == ProcessType::BHLOADER || process_type == ProcessType::DATAPROCESSOR)) {
			PackCoordinate pc(table_number, attr_number, pack+packs_omitted,0,0);
			if(Type().GetCompressType() == Compress_DEFAULT){
				dpn.pack = AttrPackPtr(new AttrPackN(pc, TypeName(), GetInsertingMode(), 0));
			}
			else{
				dpn.pack = AttrPackPtr(new AttrPackN_Zip(pc, TypeName(), GetInsertingMode(), 0));
			}
			dpn.no_pack_locks = 1;
		} else {
			PackCoordinate pc(table_number, attr_number, NoPack(),0,0);
			if(Type().GetCompressType() == Compress_DEFAULT){
				dpn.pack = AttrPackPtr(new AttrPackN(pc, TypeName(), GetInsertingMode(), 0));
			}
			else{
				dpn.pack = AttrPackPtr(new AttrPackN_Zip(pc, TypeName(), GetInsertingMode(), 0));
			}
			dpn.no_pack_locks = 1;
			ConnectionInfoOnTLS->GetTransaction()->PutObject(pc, dpn.pack);
		}
	} else {
		dpn.local_min = 0;
		dpn.local_max = -1;

		if((process_type == ProcessType::BHLOADER || process_type == ProcessType::DATAPROCESSOR)) {
			PackCoordinate pc(table_number, attr_number, pack+packs_omitted,0,0);
			if(Type().GetCompressType()== Compress_DEFAULT){
				dpn.pack = AttrPackPtr(new AttrPackS(pc, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
			}
			else{
				dpn.pack = AttrPackPtr(new AttrPackS_Zip(pc, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
			}
			dpn.no_pack_locks = 1;
		} else {
			PackCoordinate pc(table_number, attr_number, NoPack(),0,0);
			if(Type().GetCompressType()== Compress_DEFAULT){
				dpn.pack = AttrPackPtr(new AttrPackS(pc, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
			}
			else{
				dpn.pack = AttrPackPtr(new AttrPackS_Zip(pc, TypeName(), GetInsertingMode(), GetNoCompression(), 0));
			}
			dpn.no_pack_locks = 1;
			ConnectionInfoOnTLS->GetTransaction()->PutObject(pc, dpn.pack);
		}
	}
	partitioninfo->getsavepartinfo(NULL).newpack(NoPack());
	SetNoPack(NoPack() + 1);
	return dpn;
}

void RCAttrLoadBase::LoadPack(uint n)
{
	BHASSERT(n < NoPack()-packs_omitted, "should be 'n < no_pack-packs_omitted'");
	if(dpns[n].pack_mode != PACK_MODE_UNLOADED || !ShouldExist(n))
		return;
	WaitForSaveThreads();
	LoadPackInherited(n);
}

void RCAttrLoadBase::LoadPackInherited(int n)
{
	DPN& dpn(dpns[n]);
	if(dpn.pack_file < 0)
		rclog << lock << "INTERNAL ERROR: attempting to open wrong file (LoadPack), dpns[" << n << "].pack_file="
				<< dpn.pack_file << unlock;
	{
		if(dpn.pack_mode != PACK_MODE_UNLOADED)
			return;

		BHASSERT(dpn.pack_mode == PACK_MODE_UNLOADED, "Invalid pack_mode!");
		dpn.pack->SetRiakPack(table_number,attr_number,dpn.GetRiakKey());
		dpn.pack->LoadData(NULL);
		dpn.pack->Uncompress(dom_inj_mngr);
		dpn.pack_mode = PACK_MODE_IN_MEMORY;
		dpn.is_stored = true;
	}
}

/* // delete by liujs
int RCAttrLoadBase::Save()
{
	MEASURE_FET("RCAttrLoadBase::Save()");
	if(current_state != 1) {
		rclog << lock << "Error: cannot save. It is read only session." << unlock;
		throw;
	}
	LoadPackInfo();
	WaitForSaveThreads();
	int npack = NoPack();
	for(uint i = 0; i < npack - packs_omitted; i++)
		SavePack(i);
	WaitForSaveThreads();
	if(rsi_hist_update || rsi_cmap_update)
		SaveRSI();
	BHASSERT(FileFormat()==10, "should be 'file_format==10'");
	SaveDPN();
	SaveHeader();
	return 0;
}
*/

void RCAttrLoadBase::SaveDPN()
{
	MEASURE_FET("RCAttrLoadBase::SaveDPN()");
	if(NoPack() == 0)
		return;
	const int buf_size = 10000;
	char buf[buf_size];
	IBFile fdpn;
	string fn(DPNFileName());

	try {
		fdpn.OpenCreate(fn);
		ushort buffer_pos = 0;
		int npack = NoPack();
		for(uint p = 0; p < npack - packs_omitted; p++) {
			if(buffer_pos + 37 > buf_size) {
				fdpn.Write(buf, buffer_pos);
				buffer_pos = 0;
			}
			if(p == 0)
				StoreDPN(npack - packs_omitted - 1, buf + buffer_pos); // last pack is saved first
			else
				StoreDPN(p - 1, buf + buffer_pos);
			if(p == 0) {
				// last pack is saved at the beginning of the file
				if(GetCurSaveLocation() == 1)
					fdpn.Seek(37, SEEK_CUR);
				fdpn.Write(buf, 37);
				// the rest is appended to the end of the file
				if(p < npack - packs_omitted - 1)
					fdpn.Seek((packs_omitted + 2) * 37, SEEK_SET);
			} else
				buffer_pos += 37;
		}
		if(buffer_pos > 0)
			fdpn.Write(buf, buffer_pos);
		fdpn.Close();
	} catch (DatabaseRCException&) {
		rclog << lock << "Internal error: unable to write data pack nodes to " << fn << ". " << GetErrorMessage(errno)
				<< unlock;
		throw;
	}
}

void RCAttrLoadBase::CompareAndSetCurrentMin(RCBString tstmp, RCBString & min, bool set)
{
	bool res;
	if(RequiresUTFConversions(Type().GetCollation())) {
		res = CollationStrCmp(Type().GetCollation(), tstmp, min) < 0;
	} else
		res = strcmp(tstmp, min) < 0;

	if(!set || res) {
		min = tstmp;
		min.MakePersistent();
		set = true;
	}
}

void RCAttrLoadBase::CompareAndSetCurrentMax(RCBString tstmp, RCBString & max)
{
	bool res;
	if(RequiresUTFConversions(Type().GetCollation())) {
		res = CollationStrCmp(Type().GetCollation(), tstmp, max) > 0;
	} else
		res = strcmp(tstmp, max) > 0;

	if(res) {
		max = tstmp;
		max.MakePersistent();
	}
}

uint RCAttrLoadBase::RoundUpTo8Bytes(RCBString& s)
{
#ifndef PURE_LIBRARY
	uint useful_len = 0;
	if(Type().GetCollation().collation->mbmaxlen > 1) {
		int next_char_len;
		while(true) {
			if(useful_len >= s.len)
				break;
			next_char_len = Type().GetCollation().collation->cset->mbcharlen(Type().GetCollation().collation,
					(uchar) s.val[useful_len + s.pos]);
			assert("wide character unrecognized" && next_char_len > 0);
			if(useful_len + next_char_len > 8)
				break;
			useful_len += next_char_len;
		}
	} else
		useful_len = s.len > 8 ? 8 : s.len;
	return useful_len;
#else
	BHERROR("NOT IMPLEMENTED");
	return 0;
#endif
}

void RCAttrLoadBase::SetPackMax(uint pack, RCBString& max_s)
{
	DPN const& dpn(dpns[pack]);
	if(RequiresUTFConversions(Type().GetCollation())) {
		int useful_len = RoundUpTo8Bytes(max_s);

		//deal with ...ae -> ...

		strncpy((uchar*) (&dpn.local_max), max_s, useful_len);
		if(useful_len < 8)
			((uchar*) &dpn.local_max)[useful_len] = 0;
	} else
		strncpy((uchar*) (&dpn.local_max), max_s, (uint)min(max_s.size(), sizeof(_int64)));
}

void RCAttrLoadBase::SetPackMin(uint pack, RCBString& min_s)
{
	DPN const& dpn(dpns[pack]);
	if(RequiresUTFConversions(Type().GetCollation())) {
		int useful_len = RoundUpTo8Bytes(min_s);

		//deal with ...ae -> ...

		strncpy((uchar*) (&dpn.local_min), min_s, useful_len);
		if(useful_len < 8)
			((uchar*) &dpn.local_min)[useful_len] = 0;
	} else
		strncpy((uchar*) (&dpn.local_min), min_s, sizeof(_int64));
}

RCBString RCAttrLoadBase::MinS(Filter* f)
{
#ifdef FUNCTIONS_EXECUTION_TIMES
	FETOperator feto("RCAttr::MinS(...)");
#endif
	if(f->IsEmpty() || !ATI::IsStringType(TypeName()) || NoObj() == 0 || NoObj() == NoNulls())
		return RCBString();
	RCBString min;
	bool set = false;
	if(f->NoBlocks() + packs_omitted != NoPack())
		throw DatabaseRCException("Data integrity error, query cannot be evaluated (MinS).");
	else {
		RCBString tstmp;
		LoadPackInfo();
		FilterOnesIterator it(f);
		while(it.IsValid()) {
			uint b = it.GetCurrPack();
        	if(b >= NoPack() - packs_omitted)
        		continue;
			DPN const& dpn( dpns[b] );
			if(PackType() == PackN &&
					(GetPackOntologicalStatus(b) == UNIFORM ||
					(GetPackOntologicalStatus(b) == UNIFORM_AND_NULLS && f->IsFull(b)))
			) {
				tstmp = DecodeValue_S(dpn.local_min);
			    CompareAndSetCurrentMin(tstmp, min, set);
				it.NextPack();
			} else if(!(dpn.pack_file == PF_NULLS_ONLY || dpn.pack_file == PF_NO_OBJ)) {
				while(it.IsValid() && b == it.GetCurrPack()) {
					int n = it.GetCurrInPack();
					if(PackType() == PackS && dpn.pack->IsNull(n) == 0) {
						int len = ((AttrPackS*)dpn.GetPackS())->GetSize(n);
						tstmp = len ? RCBString(((AttrPackS*)dpn.GetPackS())->GetVal(n), len) : RCBString("");
					    CompareAndSetCurrentMin(tstmp, min, set);
					}
					++it;
				}
			}
		}
	}
	return min;
}

RCBString RCAttrLoadBase::MaxS(Filter* f)
{
#ifdef FUNCTIONS_EXECUTION_TIMES
	FETOperator feto("RCAttr::MaxS(...)");
#endif
	if(f->IsEmpty() || !ATI::IsStringType(TypeName()) || NoObj() == 0 || NoObj() == NoNulls())
		return RCBString();

	RCBString max;
	if(f->NoBlocks() + packs_omitted != NoPack())
		throw DatabaseRCException("Data integrity error, query cannot be evaluated (MaxS).");
	else {
		RCBString tstmp;
		LoadPackInfo();
		FilterOnesIterator it(f);
		while(it.IsValid()) {
			uint b = it.GetCurrPack();
			if(b >= NoPack() - packs_omitted)
        		continue;
			DPN const& dpn( dpns[b] );
			if(PackType() == PackN &&
					(GetPackOntologicalStatus(b) == UNIFORM ||
					(GetPackOntologicalStatus(b) == UNIFORM_AND_NULLS && f->IsFull(b)))
			) {
				tstmp = DecodeValue_S(dpn.local_min);
				CompareAndSetCurrentMax(tstmp, max);
			} else if(!(dpn.pack_file == PF_NULLS_ONLY || dpn.pack_file == PF_NO_OBJ)) {
				while(it.IsValid() && b == it.GetCurrPack()) {
					int n = it.GetCurrInPack();
					if(PackType() == PackS && dpn.pack->IsNull(n) == 0) {
						int len = ((AttrPackS*)dpn.GetPackS())->GetSize(n);
						tstmp = len ? RCBString(((AttrPackS*)dpn.GetPackS())->GetVal(n), len) : RCBString("");
						CompareAndSetCurrentMax(tstmp, max);
					} else if(PackType() == PackN && !dpn.pack->IsNull(n)) {
						tstmp = RCBString(DecodeValue_S(((AttrPackN*) dpn.GetPackN())->GetVal64(n) + dpn.local_min));
						CompareAndSetCurrentMax(tstmp, max);
					}
					++it;
				}
			}
		}
	}
	return max;
}

RCDataTypePtr RCAttrLoadBase::GetMinValuePtr(int pack) // needed for loader?
{
	AttributeType a_type = TypeName();
	RCDataType* ret = 0;
	if(this->NoNulls() != NoObj() && NoObj() != 0) {
		LoadPackInfo();
		if(ATI::IsIntegerType(a_type))
			ret = new RCNum(dpns[pack].local_min);
		else if(ATI::IsDateTimeType(a_type))
			ret = new RCDateTime(dpns[pack].local_min, a_type);
		else if(ATI::IsRealType(a_type))
			ret = new RCNum(dpns[pack].local_min, 0, true);
		else if(a_type == RC_NUM)
			ret = new RCNum(dpns[pack].local_min, Type().GetScale());
		else {
			std::map<int,int> nfblocks;
			Filter f(NoObj() - (static_cast<_uint64> (packs_omitted) << 16),nfblocks);
			f.SetBlock(pack);
			ret = new RCBString(MinS(&f));
		}
	}
	return RCDataTypePtr(ret);
}

RCDataTypePtr RCAttrLoadBase::GetMaxValuePtr(int pack) // needed for loader?
{
	AttributeType a_type = TypeName();
	RCDataType* ret = 0;
	if(this->NoNulls() != NoObj() && NoObj() != 0) {
		LoadPackInfo();
		if(ATI::IsIntegerType(a_type))
			ret = new RCNum(dpns[pack].local_max);
		else if(ATI::IsDateTimeType(a_type))
			ret = new RCDateTime(dpns[pack].local_max, a_type);
		else if(ATI::IsRealType(a_type))
			ret = new RCNum(dpns[pack].local_max, 0, true);
		else if(a_type == RC_NUM)
			ret = new RCNum(dpns[pack].local_max, Type().GetScale());
		else {
			std::map<int,int> nfblocks;
			Filter f(NoObj() - (static_cast<_uint64> (packs_omitted) << 16),nfblocks);
			f.SetBlock(pack);
			ret = new RCBString(MaxS(&f));
		}
	}
	return RCDataTypePtr(ret);
}

void RCAttrLoadBase::GetMinMaxValuesPtrs(int pack, RCDataTypePtr& out_min, RCDataTypePtr& out_max) // needed for loader?
{
	out_min = GetMinValuePtr(pack);
	out_max = GetMaxValuePtr(pack);
}

void RCAttrLoadBase::LogWarnigs()
{
	if(illegal_nulls) {

		std::pair<std::string, std::string> db_and_table_names = RCEngine::GetDatabaseAndTableNamesFromFullPath(path);

		stringstream ss;
		ss << "WARNING: There was an attempt to insert NULL to " << db_and_table_names.first << "." << db_and_table_names.second << "." << this->Name()
				<< " column that is defined as NOT NULL.";
		rclog << lock << ss.str() << unlock;

		ss.str("");
		ss << "         NULLs were changed to ";

		if(ATI::IsStringType(TypeName()))
			ss << "empty string.";
		else if(ATI::IsNumericType(TypeName()))
			ss << RCNum(0, Type().GetScale(), ATI::IsRealType(TypeName()), TypeName()).ToRCString() << ".";
		else
			ss << RCDateTime(0, TypeName()).ToRCString() << ".";

		rclog << lock << ss.str() << unlock;
	}
}

void RCAttrLoadBase::UpdateRSI_Hist(int pack, int no_objs) // make the index up to date for the selected pack
{
	MEASURE_FET("RCAttrLoadBase::UpdateRSI_Hist(...)");
	if(PackType() != PackN || (rsi_manager == NULL && process_type != ProcessType::DATAPROCESSOR) || NoObj() == 0)
		return;
	// Note that GetIndxForUpdate will create a new index, if it does not exist.
	// Assuming that if UpdateRSI_Hist() is executed, then we really should have an index for this attr.
	// Allows updating the histogram in the random order.

	if(rsi_hist_update == NULL)
		rsi_hist_update = (RSIndex_Hist*) rsi_manager->GetIndexForUpdate(
				RSIndexID(RSI_HIST, table_number, attr_number), GetCurReadLocation());

	if(rsi_hist_update == NULL) {
		rccontrol << lock << "Warning: cannot access RSI_Hist" << unlock;
		return;
	}
	LoadPackInfo();
	if(rsi_hist_update->NoObj() == 0)
		rsi_hist_update->Create(NoObj(), (ATI::IsRealType(TypeName()) ? false : true)); // the new index
	else
		rsi_hist_update->Update(NoObj()); // safe also if no_obj is the same as a while ago

	rsi_hist_update->ClearPack(pack + packs_omitted); // invalidate the previous content of the pack
	_int64 pmin = dpns[pack].local_min;
	_int64 pmax = dpns[pack].local_max;
	BHASSERT(pmin != PLUS_INF_64, "should be 'pmin != PLUS_INF_64'");
	if(pmin == pmax || GetPackOntologicalStatus(pack) == NULLS_ONLY)
		return;

	//LockPackForUse(pack);
	//	if(dpns[pack].pack_mode != PACK_MODE_IN_MEMORY) LoadPack(pack);	// do not mark this pack access, as it is optional
	_uint64 obj_start = (_uint64(pack) << 16);
	_uint64 obj_stop = obj_start + no_objs;
	for(_uint64 obj = obj_start; obj < obj_stop; obj++)
		rsi_hist_update->PutValue(GetValueInt64(obj), pack + packs_omitted, pmin, pmax);
	//UnlockPackFromUse(pack);
	// NOTE: we must update and delete index after each load, because someone may use it in a query before commit.
	// Updating should be performed externally (after load) to avoid too frequent disk access.
}

void RCAttrLoadBase::UpdateRSI_CMap(int pack, int no_objs, bool new_prefix) // make the index up to date for the selected pack
{
	MEASURE_FET("RCAttrLoadBase::UpdateRSI_CMap(...)");
	if(PackType() != PackS || (rsi_manager == NULL && process_type != ProcessType::DATAPROCESSOR) || NoObj() == 0
			|| RequiresUTFConversions(Type().GetCollation()))
		return;
	if(rsi_cmap_update == NULL)
		rsi_cmap_update = (RSIndex_CMap*) rsi_manager->GetIndexForUpdate(
				RSIndexID(RSI_CMAP, table_number, attr_number), GetCurReadLocation());

	if(rsi_cmap_update == NULL) {
		rccontrol << lock << "Warning: cannot access RSI_Hist" << unlock;
		return;
	}
	LoadPackInfo();
	if(rsi_cmap_update->NoObj() == 0)
		rsi_cmap_update->Create(NoObj(), (Type().GetPrecision() > 64 ? 64 : Type().GetPrecision())); // the new index
	else
		rsi_cmap_update->Update(NoObj()); // safe also if no_obj is the same as a while ago
    // AS DMA-495 fixed,restore this line;
    //rsi_cmap_update->Update( (((_int64)pack)<<16)+no_objs);
	rsi_cmap_update->ClearPack(pack + packs_omitted); // invalidate the previous content of the pack

	if(GetPackOntologicalStatus(pack) == NULLS_ONLY)
		return;

	//LockPackForUse(pack);
	_uint64 obj_start = (_uint64(pack) << 16);
	_uint64 obj_stop = obj_start + no_objs;
	_uint64 obj = /*new_prefix ? 0 :*/obj_start;
	int prefix_len = (int) GetPrefix(pack).size();
	for(; obj < obj_stop; obj++)
		if(!IsNull(obj))
			rsi_cmap_update->PutValue(GetValueString(obj) += prefix_len, pack + packs_omitted); // false - because we don't want to see binary as hex
	// NOTE: we must update and delete index after each load, because someone may use it in a query before commit.
	// Updating should be performed externally (after load) to avoid too frequent disk access.
}

void RCAttrLoadBase::AddOutliers(int64 no_outliers)
{
	IBGuard guard(no_outliers_mutex);
	if(this->no_outliers == TransactionBase::NO_DECOMPOSITION)
		this->no_outliers = 0;
	this->no_outliers += no_outliers;
}

int64 RCAttrLoadBase::GetNoOutliers() const
{
	IBGuard guard(no_outliers_mutex);
	return no_outliers;
}


