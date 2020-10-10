#ifndef _CORE_RSIHIST_H_
#define _CORE_RSIHIST_H_

#include "RSI_Framework.h"
#include "IBFile.h"
/////////////////////////////////////////////////////////////////////////////
// RSIndex_Hist    - histograms for packs
//
// Usage:
//
//	RSIndex_Hist h;
//	h.Load(fhandle);
//	if(h.NoObj()<current_no_obj) then only a part of it may be safely used, i.e.
//		only between 0 and (h.NoObj()>>16 - 1) packs are up to date.
//
//	result = h.IsValue( value,value,pack,min_pack,max_pack);	// check if a value (or interval) is present in a pack
//
// Updating:
//
//	h.ClearPack(pack);		// clear all packs which are changed
//	h.Update(new_no_obj);	// register new number of objects
//	for(all changed packs)
//		h.PutValue(value,pack,min_pack,max_pack);
//	h.Save(fhandle);
//

namespace ib_rsi{

    class RSIndex_Hist : public RSIndex,public IRSI_Hist
    {
    public:
    	RSIndex_Hist();									// create an empty index
    	virtual ~RSIndex_Hist();

    	void	Clear();								// make the index empty

    	_int64	NoObj()	{ return no_obj; }
    	bool	UpToDate(_int64 cur_no_obj, int pack);	// true, if this pack is up to date (i.e. either cur_no_obj==no_obj, or pack is not the last pack in the index)

    	// creation, update

    	void	Create(_int64 _no_obj, bool _fixed);		// create a new histogram based on _no_obj objects; _fixed is true for fixed precision, false for floating point
    	void	Update(_int64 _new_no_obj);				// enlarge buffers for the new number of objects;
    													// note that all of the last pack must be analyzed by PutValue(),
    													// because pack min and max might change. ClearPack may be needed.
    	void	ClearPack(int pack);					// reset the information about the specified pack
    													// and prepare it for update (i.e. analyzing again all values)
    	void	PutValue(_int64 v,
    					 int pack, _int64 pack_min, _int64 pack_max);		// set information that value v does exist in this pack

        void    AppendKNs(int no_new_packs);    //append new KNs

    	// reading histogram information
    	// Note: this function is thread-safe, i.e. it only reads the data
    	RSValue	IsValue(_int64 min_v, _int64 max_v,
    					int pack, _int64 pack_min, _int64 pack_max);
    					// Results:		RS_NONE - there is no objects having values between min_v and max_v (including)
    					//				RS_SOME - some objects from this pack do have values between min_v and max_v
    					//				RS_ALL	- all objects from this pack do have values between min_v and max_v

    	bool	Intersection(int pack, _int64 pack_min, _int64 pack_max, RSIndex_Hist *sec, int pack2, _int64 pack_min2, _int64 pack_max2);
    					//	Return true if there is any common value possible for two packs from two different columns (histograms).

    	int		NoOnes(int pack, int width);			// a number of ones in the pack (up to "width" bits)
    	bool	ExactMode(_int64 pack_min, _int64 pack_max)	{ return (_uint64(pack_max - pack_min) <= (32 * int_res - 2)); }
    	// if true, then the histogram provides exact information, not just interval-based

    	// Loading/saving

    	int		Load(IBFile* frs_index, int current_loc);			// fd - file descriptor, open for binary reading (the file can be newly created and empty - this shouldn't cause an error)
    	int		LoadLastOnly(IBFile* frs_index, int current_loc);	// read structure for update (do not create a full object)
    	int		Save(IBFile* frs_index, int current_loc);			// fd - file descriptor, open for binary writing, start from last pack

    	void	Display(uint pack);

    	void AppendKN(int pack, RSIndex_Hist*, uint no_values_to_add);	//append a given KN to KN list, extending the number of objects
    	bool GetFixed() { return fixed; };

    	ushort	GetRepLength() { return ushort(int_res * sizeof(int)); }
    	void	CopyRepresentation(void *buf, int pack);
    	
    protected:
    	char	*GetRepresentation(int pack) { assert(pack <= end_pack); return (char *)(&t[pack*int_res]); }

    private:
    	bool	IntervalTooLarge( _int64 pack_min, _int64 pack_max )		// ignore packs with extreme values
    	{ return (pack_min < -4611686018427387900LL && pack_max > 4611686018427387900LL ); }		// about 2^62

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private:
    	_int64	no_obj;				// timeliness signature: a number of objects for which the index is up-to-date
    	int		no_pack;			// number of packs
    	int		start_pack;			// number of the first pack stored in memory
    	int		end_pack;			// number of the last pack stored in memory
    	int		no_pack_declared;	// current size of the buffer (usually more than used, to avoid too frequent reallocations)
    	int		int_res;			// number of ints to describe one pack (default: 32 ints = 1024 bits)
    	unsigned int *t;			// buffer for values

    	bool	fixed;				// true for fixed precision, false for floats (in this case all _int64 values are treated as double)


    };

}
#endif  //_CORE_RSIHIST_H_

