#ifndef __RSI_FRAMEWORK_H
#define __RSI_FRAMEWORK_H

#include <stdio.h>
#include <ostream>
#include <vector>
#include <string>
#include <assert.h>
#include "IBFile.h"
#include "RSI_interface.h"

#define RSI_DEFFILE			"rsi_dir.rsd"
#define RSI_METADATAFILE	"metadata.rsd"
#define RSI_MANAGER_VERSION "3.2GA"
#define RSI_FLOCK_TRY_DEFR	50			// how many times to try to lock definition file when reading
#define RSI_FLOCK_WAIT_DEFR	500			// time delay [ms] between tries, for definition file when reading
#define RSI_FLOCK_TRY_DEFW	20			// how many times to try to lock definition file when writing
#define RSI_FLOCK_WAIT_DEFW	500			// time delay [ms] between tries, for definition file when writing
#define RSI_FLOCK_TRY_R		10			// how many times to try to lock .rsi file when reading
#define RSI_FLOCK_WAIT_R	500			// time delay [ms] between tries, when reading
#define RSI_FLOCK_TRY_W		100			// how many times to try to lock .rsi file when writing
#define RSI_FLOCK_WAIT_W	500			// time delay [ms] between tries, when writing
#define RSI_DELETE_TRY		20			// how many times to try to delete .rsi file
#define RSI_DELETE_WAIT		200			// time delay [ms] between tries
#define MAX_PACK_ROW_SIZE   0x10000LL

enum RSIndexType	{	RSI_TEST = 0,			// used for testing of RSI_Manager
						RSI_HIST = 1,			// histogram data on packs
						RSI_JPP = 2,			// Pack-pack joining index
						RSI_CMAP = 3			// character maps on packs
					};

namespace ib_rsi{

    const int bin_sums[256]={   0,  1,  1,  2,  1,  2,  2,  3,      // 00000***
                                1,  2,  2,  3,  2,  3,  3,  4,      // 00001***
                                1,  2,  2,  3,  2,  3,  3,  4,      // 00010***
                                2,  3,  3,  4,  3,  4,  4,  5,      // 00011***
                                1,  2,  2,  3,  2,  3,  3,  4,      // 00100***
                                2,  3,  3,  4,  3,  4,  4,  5,      // 00101***
                                2,  3,  3,  4,  3,  4,  4,  5,      // 00110***
                                3,  4,  4,  5,  4,  5,  5,  6,      // 00111***
                                1,  2,  2,  3,  2,  3,  3,  4,      // 01000***
                                2,  3,  3,  4,  3,  4,  4,  5,      // 01001***
                                2,  3,  3,  4,  3,  4,  4,  5,      // 01010***
                                3,  4,  4,  5,  4,  5,  5,  6,      // 01011***
                                2,  3,  3,  4,  3,  4,  4,  5,      // 01100***
                                3,  4,  4,  5,  4,  5,  5,  6,      // 01101***
                                3,  4,  4,  5,  4,  5,  5,  6,      // 01110***
                                4,  5,  5,  6,  5,  6,  6,  7,      // 01111***
                                1,  2,  2,  3,  2,  3,  3,  4,      // 10000***
                                2,  3,  3,  4,  3,  4,  4,  5,      // 10001***
                                2,  3,  3,  4,  3,  4,  4,  5,      // 10010***
                                3,  4,  4,  5,  4,  5,  5,  6,      // 10011***
                                2,  3,  3,  4,  3,  4,  4,  5,      // 10100***
                                3,  4,  4,  5,  4,  5,  5,  6,      // 10101***
                                3,  4,  4,  5,  4,  5,  5,  6,      // 10110***
                                4,  5,  5,  6,  5,  6,  6,  7,      // 10111***
                                2,  3,  3,  4,  3,  4,  4,  5,      // 11000***
                                3,  4,  4,  5,  4,  5,  5,  6,      // 11001***
                                3,  4,  4,  5,  4,  5,  5,  6,      // 11010***
                                4,  5,  5,  6,  5,  6,  6,  7,      // 11011***
                                3,  4,  4,  5,  4,  5,  5,  6,      // 11100***
                                4,  5,  5,  6,  5,  6,  6,  7,      // 11101***
                                4,  5,  5,  6,  5,  6,  6,  7,      // 11110***
                                5,  6,  6,  7,  6,  7,  7,  8};     // 11111***
    
    extern int CalculateBinSum(unsigned int n);


    class RSIndex;
    class KN
    {
    public:
    	KN() : rsi(0), pack(-1) {}
    	RSIndex* rsi;
    	int pack;
    };

    struct RSUsageStats
    {
    	RSUsageStats():all_packs(0),used_packs(0),pack_rsr(0),pack_rsi(0)
    	{}
    	_int64 all_packs;		// upper limit: all packs in a column / pair
    	_int64 used_packs;		// number of packs still to be (roughly) checked
    	_int64 pack_rsr;			// number of packs excluded because of rough set representation (statistics)
    	_int64 pack_rsi;			// number of packs excluded because of rough set indexes of various types
    	void Clear()
    	{
    		all_packs=used_packs=pack_rsi=pack_rsr=0;
    	}
    };

    struct RSIndexID			// index identifier: index type plus index-dependent parameters (can be extended when needed)
    {
    	RSIndexType type;

    	// table and column numbers: (tab,col) - 1st column, (tab2,col2) - 2nd column (if used; depends on 'type')
    	int tab, col, tab2, col2;

    	bool IsType1() const						{ return (type <= RSI_HIST || type == RSI_CMAP); }
    																						// one-column index
    	bool IsType2() const						{ return type == RSI_JPP;  }			// two-column index
    	bool IsCorrect()							{ return type <= RSI_CMAP && (tab>=0)&&(col>=0) &&
    														 (IsType1() || ((tab2>=0)&&(col2>=0))); }
    	void Check()								{ assert(IsCorrect()); }

    	RSIndexID()									{ type = (RSIndexType)-1; tab = col = tab2 = col2 = -1; }
    	RSIndexID(RSIndexType t, int tb, int c)		{ type = t; tab = tb; col = c; tab2 = col2 = -1; Check(); }
    	RSIndexID(RSIndexType t, int tb, int c, int tb2, int c2)
    												{ type = t; tab = tb; col = c; tab2 = tb2; col2 = c2; Check(); }
    	RSIndexID(std::string filename);

    	int Load(FILE* f);
    	int Save(FILE* f);

    	bool operator==(const RSIndexID& id) const;
    	bool operator< (const RSIndexID& id) const;
    	bool Contain(int t, int c = -1);			// does this ID contain column (t,c) or table (t)
    };

    std::ostream& operator<<(std::ostream& o, const RSIndexID& rsi);
    extern int NoObj2NoPacks(_int64 no_obj);

    //////////////////////////////////////////////////////////////
    /*
        rsi 对外调用接口类
    */
    class RSIndex
    {
    private:
    	RSIndexID id;
    	int readers;		// when positive: number of readers; -1: used by a writer; 0: currently not used (can be deleted)
    	bool valid;			// object is invalidated when index is deleted, so the object should be also deleted asap
 
    public:
    	///// From TrackableObject
    	virtual bool IsLocked()					{ return readers != 0; }
    	//virtual bool IsLockedByWrite()			{ return readers < 0; }

    	///// To be called by RSI_Manager. Assumption: mutual exclusion is provided by critical section in RSI_Manager
    	void SetID(RSIndexID id_)							{ id = id_; }
    	RSIndexID GetID()									{ return id; }

    	// Return value: positive for error, 0 when OK
    	RSIndex* GetForRead()				{ if(readers < 0) return NULL;	 	readers++; return this; }
    	RSIndex* GetForWrite()				{ if(readers != 0) return NULL;		readers = -1; return this; }
    	int ReleaseRead()					{ if(readers <= 0) return 1;		readers--; return 0; }
    	int ReleaseWrite()					{ if(readers >= 0) return 1;		readers = 0; return 0; }

    	bool IsValid()						{ return valid; }
    	void Invalidate()					{ valid = false; }

    	RSIndex()							{ readers = 0; valid = true; }
    	virtual ~RSIndex()					{}
        
    protected:
    	virtual char* GetRepresentation(int pack) = 0;
    };
}

#endif


