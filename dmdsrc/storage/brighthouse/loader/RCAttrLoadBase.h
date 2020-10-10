/* Copyright (C) 2005-2008 Infobright Inc. */

#ifndef LOADER_RCATTRLOADBASE_H_INCLUDED
#define LOADER_RCATTRLOADBASE_H_INCLUDED

#include <boost/optional.hpp>
#include <boost/any.hpp>
#include "common/mysql_gate.h"
#include "core/RCAttr.h"
#include "system/ib_system.h"



class NewValuesSetBase;

class RCAttrLoadBase : public RCAttr
{
public:
	RCAttrLoadBase(int a_num,int t_num,std::string const& a_path,int conn_mode=0,unsigned int s_id=0,bool loadapindex=true, DTCollation collation = DTCollation()) throw(DatabaseRCException);
	RCAttrLoadBase(int a_num, AttributeType a_type, int a_field_size, int a_dec_places, uint param, DTCollation collation, bool compress_lookups, std::string const& path_ );
	virtual ~RCAttrLoadBase();
	void LoadPackInfoForLoader();
	void LoadPackIndexForLoader(const char *partname,int sessionid,bool ismerge=false);
	void LoadPartionForLoader(uint connid,int sessionid,bool appendmode=false);
	void ExpandDPNArrays();
	void SaveDPN();
	void SavePartitonPacksIndex(DMThreadData *tl);
	/*!
	 * \param pack_already_prepared: false - prepare DP, true - new DP was prepared/created
	 */
	int LoadData(NewValuesSetBase* nvs, bool force_values_coping, bool force_saveing_pack = false, bool pack_already_prepared = false);
	bool PreparePackForLoad();

	void UnlockPackFromUse(unsigned pack_no);
	void LockPackForUse(unsigned pack_no);
	void LoadPack(uint n);
	virtual int Save(bool for_insert = false) = 0;
	DPN& CreateNewPackage();
    static DWORD WINAPI WINAPI SavePackThread(void *params);
    void UpdateRSI_Hist(int pack, int no_objs);
    void UpdateRSI_CMap(int pack, int no_objs, bool new_prefix = true);
    void threadJoin(int n);
    void threadCancel(int n);
    inline void WaitForSaveThreads() { DoWaitForSaveThreads(); }
    inline int SavePack(int n) { return DoSavePack(n); }

    void	InitKNsForUpdate();

	void SetPackrowSize(uint packrow_size) { this->packrow_size = packrow_size; }
	uint GetPackrowSize() const { return packrow_size; }

	int64	GetNoOutliers() const;
	void	AddOutliers(int64 no_outliers);
	int64 RiakAllocPNode();
	
protected:
	void LoadPackInherited(int n);
    virtual int DoSavePack(int n) =0;
    virtual void DoWaitForSaveThreads() =0;
    void LogWarnigs();
    RCDataTypePtr GetMinValuePtr(int pack);
    RCDataTypePtr GetMaxValuePtr(int pack);
    void GetMinMaxValuesPtrs(int pack, RCDataTypePtr & out_min, RCDataTypePtr & out_max);
    void SetPackMax(uint pack, RCBString& s);
    void SetPackMin(uint pack, RCBString& s);
    inline uint RoundUpTo8Bytes(RCBString& s);

    DPN		LoadDataPackN(const DPN& source_dpn, NewValuesSetBase* nvs, _int64& load_min, _int64& load_max, int& load_nulls);
    bool 	UpdateGlobalMinAndMaxForPackN(const DPN& dpn);
    bool	UpdateGlobalMinAndMaxForPackN(int no_obj, _int64 load_min, _int64 load_max, int load_nulls); // returns true if the new data lie outside of the current i_min-i_max
    void	UpdateUniqueness(const DPN& old_dpn, DPN& new_dpn, _int64 load_min, _int64 load_max, int load_nulls, bool is_exclusive, NewValuesSetBase* nvs);
    void	UpdateUniqueness(DPN& dpn, bool is_exclusive);
    bool 	HasRepetitions(DPN & new_dpn, const DPN & old_dpn, int load_obj, int load_nulls, NewValuesSetBase *nvs);


private:
    RCBString MinS(Filter *f);
    RCBString MaxS(Filter *f);
    inline void CompareAndSetCurrentMin(RCBString tstmp, RCBString & min, bool set);
    inline void CompareAndSetCurrentMax(RCBString tstmp, RCBString & min);

protected:
    bool illegal_nulls;
    uint packrow_size;

private:
    int64 no_outliers;
	
    mutable IBMutex no_outliers_mutex;
};

#endif /* not LOADER_RCATTRLOADBASE_H_INCLUDED */
