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

#include "common/CommonDefinitions.h"
#include "core/RCAttrPack.h"
#include "core/tools.h"
#include "core/WinTools.h"
#include "RCAttr_load.h"
#include "loader/NewValueSet.h"
#include <boost/function.hpp>
#include <boost/bind.hpp>
using namespace std;

RCAttrLoad::RCAttrLoad(int a_num,int t_num, string const& a_path,int conn_mode,unsigned int s_id,bool loadapindex, DTCollation collation) throw(DatabaseRCException)
	: RCAttrLoadBase(a_num, t_num, a_path, conn_mode, s_id,loadapindex, collation )
{
}

RCAttrLoad::~RCAttrLoad()
{
	LogWarnigs();
}



int RCAttrLoad::Save(bool for_insert)
{
	if(current_state!=1)
	{
		rclog << lock << "Error: cannot save. It is read only session." << unlock;
		throw;
	}
	LoadPackInfo();

    if(!for_insert){ // 不是insert的时候,必须执行,insert的时候在RCAttrLoadBase::LoadData函数中的pack包的force_saveing_pack已经保存操作。
    	int npack=NoPack();

    	attr_partinfo &curpart=partitioninfo->getsavepartinfo(NULL);
    	//uint lastpack=partitioninfo->GetLastPack()-packs_omitted;
    	uint lastpack=curpart.getsavepack()-packs_omitted;
    	if(npack-packs_omitted>lastpack+1){ lastpack++;}
    	int last_virtual_pack = 0; 
    	for(uint i=lastpack;i<npack-packs_omitted;i++){
    		SavePack(i);
    		last_virtual_pack = i;	
    	}
	}
	
	if(rsi_hist_update || rsi_cmap_update)
		SaveRSI();
	BHASSERT(FileFormat()==10, "should be 'file_format==10'");
	SaveDPN();
	SaveHeader();
	return 0;
}


void RCAttrLoad::SavePackIndex(DMThreadData *pl,int n) {
	RCAttrLoad* rcattr = this;
	_uint64 obj_start = (_uint64(n) << 16);
	_uint64 obj_stop = obj_start + rcattr->dpns[n].GetNoObj();
	_uint64 obj = /*new_prefix ? 0 :*/obj_start;
	bool ispacks=PackType() == PackS;
	apindex *pindex=ldb_index->GetIndex(GetLoadPartName(),GetSaveSessionId());
	if(pindex->SetSavePack(n)) {
	  for(; obj < obj_stop; obj++)
		if(!IsNull(obj)) {
			if(ispacks) {
				RCBString str=GetNotNullValueString(obj);
				const char *putvalue=str.Value();
				int len=str.size();
				leveldb::Slice keyslice(putvalue,len);
				pindex->Put(keyslice,n+packs_omitted);
			}
			else
			 pindex->Put(GetNotNullValueInt64(obj),n+packs_omitted);
		}
	}
}

int RCAttrLoad::DoSavePack(int n)		// WARNING: assuming that all packs with lower numbers are already saved!
{
	if(current_state != 1) {
		rclog << lock << "Error: cannot save. Read only session." << unlock;
		throw;
	}
	LoadPackInfo();
	DPN &dpn(dpns[n]);
	RCAttrLoad* rcattr = this;

	if(dpn.pack && dpn.pack->UpToDate())
		return 2;
	// process packindex before empty_pack check in case of all value in pack identity to a same value(equals to min/max)
	// Update pack index
	// check dpns[n].pack->UpToDate() to bypass?
	// parallel process packindex fill data
	if(ct.IsPackIndex() && dpn.pack_mode!=PACK_MODE_EMPTY && dpn.pack_file != PF_NULLS_ONLY) { 
		pi_thread.LockOrCreate()->Start(boost::bind(&RCAttrLoad::SavePackIndex,this,_1,n)); // fix dma-715 //SavePackIndex(NULL,n);
	}

	if(!dpn.pack || dpn.pack->IsEmpty() || dpn.pack_mode == PACK_MODE_TRIVIAL || dpn.pack_mode == PACK_MODE_EMPTY) {
		if(PackType() == PackS && GetDomainInjectionManager().HasCurrent()){
			AddOutliers(0);
		}
		pi_thread.WaitAllEnd();
		return 0;
	}


	#if !PERFORMANCE_SASU_54
	CompressionStatistics stats = dpn.pack->Compress(rcattr->GetDomainInjectionManager());
	#else
	_my_timer_comp_pack_sum.Start();
	CompressionStatistics stats = dpn.pack->Compress(rcattr->GetDomainInjectionManager());
	_my_timer_comp_pack_sum.Stop();
	#endif

	if(rcattr->PackType() == PackS && rcattr->GetDomainInjectionManager().HasCurrent())
		rcattr->AddOutliers(stats.new_no_outliers - stats.previous_no_outliers);

	//////////////// Update RSI ////////////////
	#if !PERFORMANCE_SASU_54
	rcattr->UpdateRSI_Hist(n, dpn.GetNoObj());		// updates pack n in RSIndices (NOTE: it may be executed in a random order of packs),
	rcattr->UpdateRSI_CMap(n, dpn.GetNoObj());		// but does not save it to disk. Use SaveRSI to save changes (after the whole load)
	#else
	_my_timer_rsi_pack_sum.Start();
	rcattr->UpdateRSI_Hist(n, dpn.GetNoObj());		// updates pack n in RSIndices (NOTE: it may be executed in a random order of packs),
	rcattr->UpdateRSI_CMap(n, dpn.GetNoObj());		// but does not save it to disk. Use SaveRSI to save changes (after the whole load),
	_my_timer_rsi_pack_sum.Stop();
	#endif 	


	bool last_pack = false;
	// check pack_file is valid( append to last pack)
	if (dpn.pack_file != PF_NOT_KNOWN && dpn.pack_file != PF_NULLS_ONLY && dpn.pack_file != PF_NO_OBJ
			&& dpn.pack_file>=0) {
		last_pack = true;
	}
	
	if (last_pack) {
		//DMA-614 abs switch error ,no recover
		// 文件顺序编号(改造前不同会话按奇偶编号)
		if (rcattr->dpns[n].pack_file != GetSaveFileLoc(GetCurSaveLocation()) ) {
			// last pack, but from the wrong file (switching session)
			SetSaveFileLoc(1 - GetCurSaveLocation(), dpn.pack_file);
			SetSavePosLoc(1 - GetCurSaveLocation(), dpn.pack_addr);
			dpn.pack_file = GetSaveFileLoc(GetCurSaveLocation());
			dpn.pack_addr = GetSavePosLoc(GetCurSaveLocation());
		}
	}
	else {
		// new package
		// require allocate a new riak pnode (4096 packs)
		if(GetSaveFileLoc(GetCurSaveLocation())==-1) {
			RiakAllocPNode();
		}
		dpn.pack_file=GetSaveFileLoc(GetCurSaveLocation());
		dpn.pack_addr=GetSavePosLoc(GetCurSaveLocation());
	}
	////////////////////////////////////////////////////////
	// Now the package has its final location.
	//if(rcattr->dpns[n].pack_file >= rcattr->GetTotalPackFile()) // find the largest used file number
	//	rcattr->SetTotalPackFile(rcattr->dpns[n].pack_file + 1);
	BHASSERT(dpn.pack_file >= 0, "should be 'rcattr->dpns[n].pack_file >= 0'");
	uint pack_des = dpn.pack_addr;
	if(dpn.pack && !dpn.pack->IsEmpty())
		rcattr->SetSavePosLoc(rcattr->GetCurSaveLocation(), dpn.pack_addr
				+ dpn.pack->TotalSaveSize());
	// pnode pack limit is 4096
	if( (rcattr->GetSavePosLoc(rcattr->GetCurSaveLocation())&0xfff) == (rcattr->file_size_limit-1)) // more than 1.5 GB - start the next package from the new file
	{
		ResetRiakPNode();// reset here,allocate on demond.
		rclog << lock << "PNode full,prepared for create new one ,attr_number:"<<rcattr->attr_number<<unlock;
	}
	// update save parition
	if(partitioninfo) {
		attr_partinfo &curpart=partitioninfo->getsavepartinfo(NULL);
		int64 cur_fileid=(((int64)dpn.pack_file)<<32)+(dpn.pack_addr&0xfffff000);
		// has appended at CreateNewPacket
		//curpart.newpack(n+packs_omitted);
		curpart.setsavepos(n+packs_omitted,dpn.GetNoObj());
		if(cur_fileid>=0)
			curpart.check_or_append(cur_fileid);
		int cur_save_loc = rcattr->GetCurSaveLocation();
		curpart.setsavefilepos(rcattr->GetSavePosLoc(rcattr->GetCurSaveLocation())&0xfff);
	}
	////////////////////////////////////////////////////////
	string file_name;

	try
	{
		dpn.pack->SetRiakPack(rcattr->table_number,rcattr->attr_number,dpn.GetRiakKey());
		dpn.pack->SetAsyncRiakHandle(this->GetAsyncRiakHandle());
		
		#if !PERFORMANCE_SASU_54
		dpn.pack->Save(NULL, rcattr->GetDomainInjectionManager());
		#else		
		_my_timer_save_pack_sum.Start();
		_my_timer_save_pack.Restart();
		dpn.pack->Save(NULL, rcattr->GetDomainInjectionManager());
		_my_timer_save_pack.Stop();
		_my_timer_save_pack_sum.Stop();
		
		// --- 单个包处理时间输出
		DPRINTF("save pack<%d,%d,%d> use time %0.4f(s).\n",table_number,attr_number,n,_my_timer_save_pack.GetTime());
		#endif
		
		//fattr.Close();
	}
	catch(DatabaseRCException& e)
	{
		rclog << lock << "Error: can not save data pack to " << file_name << ". " << GetErrorMessage(errno) << unlock;
		throw;
	}

	pi_thread.WaitAllEnd();
	
	rcattr->UnlockPackFromUse(n);
	return 0;
}

