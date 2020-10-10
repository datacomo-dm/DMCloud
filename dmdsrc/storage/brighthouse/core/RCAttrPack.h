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

#ifndef RCATTRPACK_H_INCLUDED
#define RCATTRPACK_H_INCLUDED

#include "core/tools.h"
#include "types/RCDataTypes.h"
#include "CQTerm.h"
#include "ftree.h"
#include "Filter.h"
#include "system/fet.h"

#include "system/MemoryManagement/TrackableObject.h"
#include "system/IBStream.h"
#include "domaininject/DomainInjection.h"

#ifndef DEBUG
#define DEBUG
#endif

#include <snappy-c.h>
#include <cr_class/cr_class.h>
#include <riakdrv/riakcluster.h>
#include <riakdrv/riakdrv++.pb.h>
#include "msgIB.pb.h"
#include "Descriptor.h"
#include "vc/MultiValColumn.h"

extern RiakCluster *_riak_cluster;
extern int _riak_max_thread;
extern int _riak_connect_number;// RIAK_CONNECT_THREAD_NUMBER_ENV_NAME
extern int _riak_nodes;
extern pthread_once_t _riak_cluster_init_once;
extern void _riak_cluster_init();

extern std::string _get_riak_key(const int tbno,const int colno,const int64 packno);

bool riak_compress_value(std::string &riak_value);
bool riak_decompress_value(std::string &riak_value);

class RCAttr;
class DataCache;
template <class T>	class NumCompressor ;

enum AttrPackType {PackUnknown = 0, PackN, PackT, PackB, PackS};
enum PackMode {PM_TRIVIAL = 0, PM_UNLOADED = 1, PM_LOADED = 2, PM_NO_DATA = 3};

// table of modes:
//  0 - trivial data: all values are derivable from the statistics, or nulls only,
//		the pack physically doesn't exist, only statistics
//  1 - unloaded (on disc)
//  2 - loaded to memory
//  3 - no data yet, empty pack

//cr_list/cond_bitmap其中之一有效
//  不需要同时设置
struct  CRResult 
{
	ushort *cr_list;
	char *cond_bitmap;
	ushort no_list;
	ushort no_bmbytes;
	ushort start_row,end_row;
	CRResult() {
		cr_list=NULL;
		cond_bitmap=NULL;
		no_list=no_bmbytes=start_row=end_row=0;
	}
	~CRResult() {
		if(cr_list)
			delete []cr_list;
		if(cond_bitmap)
			delete []cond_bitmap;
	}

	int size() {
		if(cond_bitmap) return no_bmbytes;
		else if(cr_list) return no_list;
		return 0;
	}
	
	void CreateList(const uint *list,int rows) {
		if(cond_bitmap) delete [] cond_bitmap;
		cond_bitmap=NULL;
		if(cr_list) delete [] cr_list;
		cr_list=new ushort[rows];
		no_list=rows;
		for(int i=0;i<rows;i++) {
			cr_list[i]=list[i];
		}
	}
	void CreateList(const ushort *list,int rows) {
		if(cond_bitmap) delete [] cond_bitmap;
		cond_bitmap=NULL;
		if(cr_list) delete [] cr_list;
		cr_list=new ushort[rows];
		no_list=rows;
		memcpy(cr_list,list,rows*sizeof(ushort));
	}
	void CreateBitmap(const char *bm,int bmbytes,ushort _start_row,ushort _end_row) {
		if(cr_list) delete [] cr_list;
		cr_list=NULL;
		if(cond_bitmap) delete [] cond_bitmap;
		cond_bitmap=new char[bmbytes];
		memcpy(cond_bitmap,bm,bmbytes);
		no_bmbytes=bmbytes;
		start_row=_start_row;
		end_row=_end_row;
	}
};
#define LocalRangeSt() ((unsigned short)(((unsigned int )_logical_coord.co.pack[3])&0xffff))
#define LocalRangeEd() ((unsigned short)(((unsigned int )_logical_coord.co.pack[3])>>16))
//boost::share_ptr<CR_Result> 
//print protobuf debug string,for app linked with release version of protobuf
std::string PBDebugString(void *mess);
std::string PBShortDebugString(void *mess);
std::string *checkTraceRiakNode(int tabid,int attrnum,int packno);

class AttrPack : public TrackableObject
{
	friend class DataCache;
public:
	AttrPack(PackCoordinate pc, AttributeType attr_type, int inserting_mode, bool no_compression, DataCache* owner);
	AttrPack(const AttrPack &ap);
	virtual std::auto_ptr<AttrPack> Clone() const = 0;
	virtual void LoadData(IBStream* fcurfile,Descriptor *pdesc=NULL,int64 local_min=0,void *pf=NULL,int _objs=0) = 0;
	//virtual void LoadData(IBRiakDataPackQueryParam *query_param_p) = 0;
	CRResultPtr &GetCRResult() {return cr_result;}
	bool IsCQ() {return is_cq;}
	bool IsRQ() {return is_rq;}
	AttrPack* m_prev_pack;
	AttrPack* m_next_pack;

	bool IsDataPack() const { return true; };
	int Collapse() { return 0; };

	virtual AttrPackType GetPackType() const = 0;

	virtual void Uncompress(DomainInjectionManager& dim) = 0;			// Create full_data basing on compressed buf.
	virtual CompressionStatistics Compress(DomainInjectionManager& dim) = 0; // Create optimal compressed buf. basing on full data.
	virtual void StayCompressed() = 0;		// Remove full data (conserve memory) and stay with compressed buffer only.

	bool IsEmpty() const {return is_empty;}
	virtual bool IsCompressed() = 0;
	virtual int Save(IBStream* fcurfile, DomainInjectionManager& dim) = 0;		// save all pack data (parameters, dictionaries, compressed data)

	virtual bool UpToDate();					// return 1 iff there is no need to save
	virtual uint TotalSaveSize() = 0;		// number of bytes occupied by data and dictionaries on disk

	inline bool IsNull(int i) const
	{
		if (no_nulls == no_obj)
			return true;
		if (nulls_bytes == 0 || !nulls)
			return false;
		// save code;
		// int p=(i-(first_obj&0xfff8));
		// return ((nulls[p>>3]&((uint)(1)<<(p%8)))!=0);		// the i-th bit of the table
	// BMP_BITTEST is inline function now, and temporary use for consistent with riak-proxy code
		return CR_Class_NS::BMP_BITTEST(this->nulls, this->nulls_bytes, i, this->first_obj);
	}
	void SetNull(int ono);
	uint NoNulls()	{return no_nulls;}
	uint NoObjs()	{return no_obj;}

	//TrackableObject functionality
	TRACKABLEOBJECT_TYPE TrackableType() const {return TO_PACK;}
	uint PreviousSaveSize() {
	 //fix sasu-89
	 return 1;
	 //return previous_size;
	}

	PackCoordinate GetPackCoordinate() const { return _logical_coord.co.pack; }
	virtual void Release();
public:
	virtual ~AttrPack();
	inline std::string GetRiakKey() {
		return _get_riak_key(this->table_number, this->attr_number, this->riak_pack_no);
	}
	inline void SetRiakPack(const int _table_number,const int _attr_number,const int64 _riak_pack_no){
		// 删除过分区的表 _riak_pack_no[riak中存储的] 与 pack_no[dpn中存储的] 会不一致
		this->table_number = _table_number;
		this->attr_number = _attr_number;
		this->riak_pack_no = _riak_pack_no; 
	}
	inline void SetAsyncRiakHandle(void *pHandle){
		pAsyncRiakHandle = pHandle;
	}

	inline void* GetAsyncRiakHandle(){	
		return pAsyncRiakHandle;
	}
	
protected:
	virtual void Destroy() = 0;
	bool ShouldNotCompress();

protected:	
	int     table_number;
	int     attr_number;
	int		pack_no;
	int64     riak_pack_no;     // 在没有删除过分区的表，与pack_no相同，删除过后，Save和Load都是用该包号码
	void    *pAsyncRiakHandle;    // save 包的时候，异步调用handle
	uint	no_nulls;
	uint	nulls_bytes;
	uint	no_obj;
	bool	is_empty;

	bool  compressed_up_to_date;			// true means that flat data was encoded in "compressed_buf" and didn't change
	bool  saved_up_to_date;				// true means that the contents of table in memory didn't change (comparing to disk)

	uchar *compressed_buf;
	
	uint8_t* nulls;
	uint  comp_buf_size, comp_null_buf_size, comp_len_buf_size;

	bool is_only_compressed; // true means that compressed pack resides in a memory (uncompressed part is deleted)
	uint previous_size;
	int inserting_mode;
	bool no_compression;
	AttributeType attr_type;
	uint first_obj;
	// is condition-request
	bool is_cq,is_rq;// condition query or range query
	//std::vector<ushort> cr_result;
	CRResultPtr cr_result;
	//PackCoordinate pc;
};

//////////////////////////////////////////////////////////////////////////////////////////////////

class AttrPackN : public AttrPack
{
public:
    AttrPackN(PackCoordinate pc, AttributeType attr_type, int inserting_mode, DataCache* owner);
	AttrPackN(const AttrPackN &apn);
    virtual std::auto_ptr<AttrPack> Clone() const;
	void LoadData(IBStream *fcurfile,Descriptor *pdesc=NULL,int64 local_min=0,void *pf=NULL,int _objs=0);
    //virtual void LoadData(IBRiakDataPackQueryParam *query_param_p);
    void Prepare(uint new_no_obj, _uint64 new_max_val);
    void SetVal64(uint n, const _uint64 & val_code2);
    _int64 GetVal64(int n);
    double GetValD(int n);
    void Expand(uint new_no_obj, _uint64 new_max_val, _int64 offset = 0);
    virtual void Uncompress(DomainInjectionManager& dim);
    virtual CompressionStatistics Compress(DomainInjectionManager& dim);
    void StayCompressed();
    bool IsCompressed()	{ return is_only_compressed; }
    void TruncateObj( int new_objs);
	int Save(IBStream *fcurfile, DomainInjectionManager& dim);
    uint TotalSaveSize();

    virtual AttrPackType GetPackType() const { return PackN; }

public:
    virtual ~AttrPackN();
protected:
    void Destroy();

protected:
    enum ValueType{ UCHAR = 1, USHORT = 2, UINT = 4, UINT64 = 8};
    template<ValueType VT> class ValueTypeChooser
    {
		public:
        typedef typename boost::mpl::if_c<VT == UINT64, uint64,
			typename boost::mpl::if_c<VT == UINT, uint,
				typename boost::mpl::if_c<VT == USHORT,ushort,uchar> ::type> ::type>::type Type;
    };

    template <typename T> void SetValues(_int64 offset);
	void InitValues(ValueType new_value_type, _int64 offset, void*& new_data_full);
	void CopyValues(ValueType value_type, ValueType new_value_type, void*& new_data_full, _int64 offset);
	template <typename S, typename D> void CopyValues(D*& new_values, _int64 offset);
	template <ValueType VT> void AssignToAll(_int64 value, void*& new_data_full);
	template <typename T> void InitValues(T value, T*& values);
	static ValueType ChooseValueType(int bit_rate);
    template<typename etype> void DecompressAndInsertNulls(NumCompressor<etype> & nc, uint *& cur_buf);
    template<typename etype> void RemoveNullsAndCompress(NumCompressor<etype> &nc, char* tmp_comp_buffer, uint & tmp_cb_len, _uint64 & maxv);

protected:
    void*		data_full;
	ValueType	value_type;
	int			bit_rate;
	_uint64		max_val;
	uchar		optimal_mode; //see below

	bool IsModeNullsCompressed() const {return optimal_mode & 0x10;}
	//! Pack not existing physically, does not cover situation when min==max, then data_full is null
	bool IsModeDataCompressed() const {return optimal_mode & 0x20;}
	bool IsModeCompressionApplied() const {return IsModeDataCompressed() || IsModeNullsCompressed();}
	bool IsModeNoCompression() const {return optimal_mode & 0x40;}
//	void SetModeNoCompression() {optimal_mode = 0x40;}		//not used, replaced by CompressCopy inside the Compressor to keep old format
	void ResetModeNoCompression() {optimal_mode &= 0xBF;}
	void SetModeDataCompressed() {ResetModeNoCompression(); optimal_mode |= 0x20; }
	void SetModeNullsCompressed() {ResetModeNoCompression(); optimal_mode |= 0x10;}
	void ResetModeNullsCompressed() {optimal_mode &= 0xEF;}
	void ResetModeDataCompressed() {optimal_mode &= 0xDF;}
	bool IsModeValid() const {return ((optimal_mode & 0xCF) == 0) || IsModeNoCompression();}

};

inline _int64 AttrPackN::GetVal64(int n)
{
	assert(!is_only_compressed);
	if(data_full == NULL) return 0;
	if(value_type == UINT)			return (_int64)((uint*)data_full)[n-first_obj];
	else if(value_type == UINT64)	return ((_int64*)data_full)[n-first_obj];
	else if(value_type == USHORT)	return (_int64)((ushort*)data_full)[n-first_obj];
	return (_int64)((uchar*)data_full)[n-first_obj];
}

inline double AttrPackN::GetValD(int n)
{
	assert(!is_only_compressed);
	if(data_full == NULL) return 0;
	assert(value_type == UINT64);
	return ((double*)data_full)[n-first_obj];
}


//////////////////////////////////////////////////////////////////////////////////////////////////

class AttrPackS : public AttrPack
{
public:
	AttrPackS(PackCoordinate pc, AttributeType attr_type, int inserting_mode, bool no_compression, DataCache* owner);
	AttrPackS(const AttrPackS &aps);
	virtual std::auto_ptr<AttrPack> Clone() const;
	void LoadData(IBStream* fcurfile,Descriptor *pdesc=NULL,int64 local_min=0,void *pf=NULL,int _objs=0);
	//virtual void LoadData(IBRiakDataPackQueryParam *query_param_p);
	void TruncateObj( int new_objs);
	void Prepare(int no_nulls);
	void Expand(int no_obj);
	void BindValue(bool null, uchar* value = 0, uint size = 0);
	void CopyBinded();
	bool HasBinded() const { return binding; }
	//void AddValue(bool null, uchar* value = 0, uint size = 0);

	int		GetSize(int ono) const;
	char*	GetVal(int ono) { BHASSERT_WITH_NO_PERFORMANCE_IMPACT(ono < (int)no_obj); return (char*)index[ono-first_obj]; }

	virtual void Uncompress(DomainInjectionManager& dim);
	//void Uncompress8();
	virtual void Uncompress8(DomainInjectionManager& dim);
  	virtual void UncompressOld();

	virtual CompressionStatistics Compress(DomainInjectionManager& dim);
	virtual CompressionStatistics Compress8(DomainInjectionDecomposer& decomposer);
	virtual CompressionStatistics CompressOld();
	void StayCompressed();
	bool IsCompressed() { return is_only_compressed; /*(data == NULL);*/ };
	int Save(IBStream* fcurfile, DomainInjectionManager& dim);

	uint TotalSaveSize();

	virtual AttrPackType GetPackType() const { return PackS; }

	uint PreviousNoObj() const { return previous_no_obj; }
	bool IsDecomposed() const { return ver == 8; }
	void SetDecomposerID(uint decomposer_id) { this->decomposer_id = decomposer_id; use_already_set_decomposer = true; ver = decomposer_id != 0 ? 8 : 0;  }
	uint GetDecomposerID() const { return decomposer_id; }

	uint GetNoOutliers() const { return outliers; };

public:
    virtual ~AttrPackS();
protected:
    void Destroy();

protected:
	void Construct();
	void SetSize(int ono, uint size);

	uchar ver;
	uint 	previous_no_obj;
	uint	max_no_obj;

	uchar**	data;
	uchar**	index;
	void*	lens;

	int		data_id;
//	ushort	values_psbs;
	uint	data_full_byte_size;

	ushort	len_mode;						//RC_BIN - sizeof(ushort), otherwise - sizeof(ushort)
	//uchar*	value_t;

	bool	binding;
	int		last_copied;

//	uchar	predictor_hist[32];
	int		optimal_mode;
	uint decomposer_id;
	ushort	no_groups;	// for decomposition

	bool use_already_set_decomposer;

	uint outliers;

	bool IsModeNullsCompressed() const {return optimal_mode & 0x1;}
	//! Pack not existing physically, does not cover situation when min==max, then data_full is null
	bool IsModeDataCompressed() const {return optimal_mode & 0x2;}
	bool IsModeCompressionApplied() const {return IsModeDataCompressed() || IsModeNullsCompressed();}
	bool IsModeNoCompression() const {return optimal_mode & 0x4;}
//	void SetModeNoCompression() {optimal_mode = 0x4;} //not used, replaced by CompressCopy inside the Compressor to keep old format
	void SetModeDataCompressed() {ResetModeNoCompression() ; optimal_mode |= 0x2;}
	void SetModeNullsCompressed() {ResetModeNoCompression() ; optimal_mode |= 0x1;}
	void ResetModeNullsCompressed() {optimal_mode &= 0xFE;}
	void ResetModeDataCompressed() {optimal_mode &= 0xFD;}
	void ResetModeNoCompression() {optimal_mode &= 0xB;}
	bool IsModeValid() const {return ((optimal_mode & 0xFC) == 0) || IsModeNoCompression();}

//	void SaveUncompressed(uchar *h, IBStream* fcurfile);
	void SaveUncompressed(msgRCAttr_packS &msg_pack);
//	void LoadUncompressed(IBStream* fcurfile);
	void LoadUncompressed(msgRCAttr_packS &msg_pack);
	void AllocBuffers();
	void AllocBuffersData(int newobjs,int newdatasize);
	void AllocBuffersLens(int newobjs,int newnullsbytes);
	void AllocNullsBuffer();
	void AllocBuffersButNulls();

	void Decompose(uint& size, std::vector<uchar**>& indexes, std::vector<ushort*>& lengths, std::vector<uint>& maxes, std::vector<uint>& sumlens);
};

inline int AttrPackS::GetSize(int ono) const
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!is_only_compressed);
    if(ono >= (int)no_obj){ // try to fix dma-1193, -- add by liujs
        char msg[256];
        //_logical_coord.co.pack._coord
        sprintf(msg,"Error AttrPackS::GetSize got error {tabid:%d,attr:%id,pack:%d}[current_obj:%d,pack_obj:%d].",
               _logical_coord.co.pack[0] ,_logical_coord.co.pack[1],
               _logical_coord.co.pack[2],ono,no_obj);
        rclog <<lock <<msg<<unlock;
        throw msg;
    }    
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(ono < (int)no_obj);
	if(len_mode == sizeof(ushort))
		return ((ushort*)lens)[ono-first_obj];
	else
		return ((uint*)lens)[ono-first_obj];
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

// >> Begin: Add AttrPackN_zip,AttrPackS_zip for AttrPack compress / decompress  , add by liujs
// create from copying AttrPackN,AttrPackS

// AttrPackN zlib compress decompress 
class AttrPackN_Zip : public AttrPackN
{
public:
    AttrPackN_Zip(PackCoordinate pc, AttributeType attr_type, int inserting_mode, DataCache* owner);
	//AttrPackN_Zip(const AttrPackN_Zip &apn);
    virtual ~AttrPackN_Zip(){};
    virtual std::auto_ptr<AttrPack> Clone() const;
	
public:
	virtual void Uncompress(DomainInjectionManager& dim);
    virtual CompressionStatistics Compress(DomainInjectionManager& dim);

protected:
    template<typename etype> void _DecompressAndInsertNulls(etype e,uint *& cur_buf);
    template<typename etype> void _RemoveNullsAndCompress(etype e,char* tmp_comp_buffer, uint & tmp_cb_len, _uint64 & maxv);
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AttrPackS zip compress decompress 
class AttrPackS_Zip : public AttrPackS
{
public:
	AttrPackS_Zip(PackCoordinate pc, AttributeType attr_type, int inserting_mode, bool no_compression, DataCache* owner);
	//AttrPackS_Zip(const AttrPackS_Zip &aps);
	virtual std::auto_ptr<AttrPack> Clone() const;
    virtual ~AttrPackS_Zip(){};

public:
	virtual	void Uncompress(DomainInjectionManager& dim);
	virtual void Uncompress8(DomainInjectionManager& dim);
	virtual	void UncompressOld();
	virtual CompressionStatistics Compress(DomainInjectionManager& dim);
	virtual CompressionStatistics Compress8(DomainInjectionDecomposer& decomposer);
	virtual CompressionStatistics CompressOld();
};

// << End:add by liujs


#endif /* RCATTRPACK_H_INCLUDED */

