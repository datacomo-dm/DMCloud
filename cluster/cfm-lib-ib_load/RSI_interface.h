#ifndef RSI_INTERFACE_DEF_H
#define RSI_INTERFACE_DEF_H
#include <stdio.h>
#include <string>
#include <stdlib.h>
#include <assert.h>


#ifndef _int64_
typedef long long int _int64;
#define _int64_
#endif

#ifndef uint_
typedef unsigned int uint;
#define uint_
#endif

#ifndef uchar_
typedef unsigned char uchar;
#define uchar_
#endif

#ifndef ushort_
typedef unsigned short ushort;
#define ushort_
#endif

// enum type can be user defined
enum RSValue {	    RS_NONE = 0,				// the pack is empty
					RS_SOME = 1,				// the pack is suspected (but may be empty or full) (i.e. RS_SOME & RS_ALL = RS_SOME)
					RS_ALL	= 2,				// the pack is full
					RS_UNKNOWN = 3				// the pack was not checked yet (i.e. RS_UNKNOWN & RS_ALL = RS_ALL)
};


namespace ib_rsi{

    #define STRUCTURE_DESSTRUCTURE(class_name)\
        public:\
            class_name(){};\
            virtual ~ class_name(){};
            
    /*
        ����ӿڻ���
    */
    class IBFile;
    class  IRSIndex{
          STRUCTURE_DESSTRUCTURE(IRSIndex);
            
    	public: // file operation    	  	
            virtual int Load(IBFile* frs_index, int current_loc) = 0;       // fd - file descriptor, open for binary reading (the file can be newly created and empty - this shouldn't cause an error)
            virtual int LoadLastOnly(IBFile* frs_index, int current_loc) = 0;       // read structure for update (do not create a full object)
            virtual int Save(IBFile* frs_index, int current_loc) = 0;       // fd - file descriptor, open for binary writing
            
        public: // get pack rsi data             
            virtual ushort GetRepLength() = 0; // get each pack rsi size
            virtual void CopyRepresentation(void *buf, int pack) = 0;       // copy pack rsi info       

        public:            
            virtual void Clear() = 0;             // reset to empty index - to be called when Load() fails, to assure index validity
           	virtual _int64	NoObj()	 = 0;	        // get the obj_num
           	virtual bool	UpToDate(_int64 cur_no_obj, int pack) = 0;		// true, if this pack is up to date (i.e. either cur_no_obj==no_obj, or pack is not the last pack in the index)
           	virtual void	Update(_int64 _new_no_obj) = 0;		// enlarge buffers for the new number of objects;
           	virtual void	ClearPack(int pack) = 0;			// reset the information about the specified pack
            virtual void    AppendKNs(int no_new_packs) = 0;    // append new KNs
    };

    /*
        �� CMap�ⲿ���ýӿ�,ʹ��˵��:
        1. create_rsi_cmap �����ö���
        2. ͨ��Create��������rsi�İ���
        3. ͨ��PutValue��������������ֵ,�����д��rsi����
        4. ���pack������,����ͨ��AppendKNs����rsi��������
        5. �����е�����������ɺ�,����Saveֱ�ӱ���        
    */
    class RSIndex_CMap;
    class IRSI_CMap:public IRSIndex{
        STRUCTURE_DESSTRUCTURE(IRSI_CMap);
        
        public:
            virtual void	AppendKN(int pack, RSIndex_CMap*, uint no_values_to_add) = 0;	//append a given KN to KN table, extending the number of objects
           	virtual void	Create(_int64 _no_obj,int no_pos = 64) = 0;			// create a new histogram based on _no_obj objects;
           	virtual void	PutValue(const char *v, const size_t vlen, int pack) = 0; // set information that value v does exist in this pack
           	virtual RSValue IsValue(std::string min_v, std::string max_v, int pack) = 0; //get information that value v does exist in this pack,return {RS_NONE,RS_SOME,RS_ALL}
           	                // IsValue ��min_v��max_v��ȥ��pack_prefix���Ⱥ���ַ���
           	virtual RSValue	IsLike(std::string pattern, int pack, char escape_character) = 0; //get information that value v does exist in this pack,return {RS_NONE,RS_SOME,RS_ALL}
    };
    IRSI_CMap*  create_rsi_cmap();   // ����cmap����,��������ֱ��delete
    std::string rsi_cmap_filename(int tabid,int colnum);  // ����cmap�ļ�����
    
    /*
        �� Hist�ⲿ���ýӿ�,ʹ��˵��:
        1. create_rsi_cmap �����ö���
        2. ͨ��Create��������rsi�İ���
        3. ͨ��PutValue��������������ֵ,�����д��rsi����
        4. ���pack������,����ͨ��AppendKNs����rsi��������
        5. �����е�����������ɺ�,����Saveֱ�ӱ���    
    */
    class RSIndex_Hist;
    class IRSI_Hist:public IRSIndex{
        STRUCTURE_DESSTRUCTURE(IRSI_Hist);
        
        public:
            virtual void    AppendKN(int pack, RSIndex_Hist*, uint no_values_to_add) = 0;	//append a given KN to KN list, extending the number of objects
            virtual void	Create(_int64 _no_obj, bool _fixed) = 0;		// create a new histogram based on _no_obj objects; _fixed is true for fixed precision, false for floating point
            virtual void	PutValue(_int64 v,int pack, _int64 pack_min, _int64 pack_max) =0 ;		// set information that value v does exist in this pack ,return {RS_NONE,RS_SOME,RS_ALL}
            virtual RSValue	IsValue(_int64 min_v, _int64 max_v,int pack, _int64 pack_min, _int64 pack_max) = 0; // get information that value v does exist in this pack ,return {RS_NONE,RS_SOME,RS_ALL}
    };
    IRSI_Hist*  create_rsi_hist();  // ����hist����,��������ֱ��delete
    std::string rsi_hist_filename(int tabid,int colnum);  // ����hist�ļ�����
}

#endif

