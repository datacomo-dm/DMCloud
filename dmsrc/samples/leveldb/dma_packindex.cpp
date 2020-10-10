#include <stdio.h>
#include <string.h>
#include <string>
#include <vector>
#include <cr_class/cr_externalsort.h>
using namespace std;

void init_file_list(std::vector<std::string>& ldb_file_lst);
void printf_packno(const std::string _str_key,const std::string _str_value)
{
		printf("-------------------------------------------------------------\n");
		int _size = _str_value.size()/sizeof(int);
		printf("pack number : %d \n",_size);
		printf("key:%s\npackno list:",_str_key.c_str());
		const int* pvalue=(const int*)_str_value.c_str();
		for(int i =0;i<_size;i++){			
				printf("%d",*pvalue);
				
				if(i!=_size-1){
					 printf(",");
				}
				if( (i+1) %16 == 0){ 
					printf("\n");
				}
				
				pvalue++;			
		}
		printf("\n");
}

int main(int argn,char* argv[]){ 
    std::vector<std::string> ldb_file_lst;
    
    std::string str_scan_key(argv[1]);

    // 获取leveldb文件列表
    init_file_list(ldb_file_lst);

	  int merge_packidx_packno_init  = 0;
    if(1) { // 合并leveldb

        CR_ExternalSort::PopOnly pop_only;

        pop_only.LoadFiles(ldb_file_lst);

        int64_t _pack_no = 0;

        std::string _str_key = "";    // key 值
        int64_t _num_key = 0;

        std::string _str_value = "" ; // pack 的value值
        bool _str_value_tmp = false;

        std::string pki_last_key = CR_Class_NS::randstr(16); // fix DMA-1501 by hwei
        int last_key_first_packno = -1;  // 记录上一个key的第一个包号
         
	    int total_packs = 0;	 
    
        while (1) {
            int ret = pop_only.PopOne(_pack_no, _str_key, _str_value, _str_value_tmp);

            if (ret == EAGAIN) {// done;
                break;
            } else if (ret) { //
                char err_msg[1024];

                sprintf(err_msg,"ERROR: merge_packindex_data failed! Database may be corrupted");
                
                printf(err_msg);
                
                throw err_msg;
            }
            

            // fix DMA-1501 : we MUST reset last_key_first_packno after _str_key changed. by hwei
            if (pki_last_key != _str_key) {
                pki_last_key = _str_key;
                last_key_first_packno = -1;
            }
                        
            if(_str_key != str_scan_key){ // 只看满足条件的key
            	  continue;
            }            

            // memcpy后,在修改
            if(merge_packidx_packno_init >0 ) { // 需要调整包号
                int* _pack_no_start = (int*)(_str_value.c_str());
                int  _pack_size = _str_value.size()/sizeof(int); // by hwei, for DMA-1389
                int* _pack_no_end = _pack_no_start + _pack_size;
                while(_pack_no_start<_pack_no_end) {
                    *_pack_no_start += merge_packidx_packno_init;
                    _pack_no_start++;
                }
            }

            const int* p_current_key_packno = (int*)(_str_value.c_str());
            if(_str_value.size() == sizeof(int) &&  last_key_first_packno == *p_current_key_packno) {
            	  printf("----------------2 \n");
                continue;// fix dma-1419:相同的数据包跳过
            }            

			total_packs += _str_value.size()/sizeof(int); 

            if(true) {
                // pindex->Put(_str_key.c_str(),_str_value,bltsession);
                printf_packno(_str_key,_str_value);                
            } else {
                _num_key = CR_Class_NS::str2i64(_str_key,0);
                // pindex->Put(_num_key,_str_value,bltsession);
                char _a[100];
                sprintf(_a,"%ld",_num_key);
                std::string _b(_a);
                printf_packno(_b,_str_value);
            }
            
            last_key_first_packno = *p_current_key_packno;
        }

		printf("\npack sum:%d \n",total_packs);
    }
    
    return 1;
}

void init_file_list(std::vector<std::string>& ldb_file_lst)
{
		ldb_file_lst.clear();


 ldb_file_lst.push_back("/data/dt3/dma/data/S_318432_1_320833-N00000-A00016.pki");
 ldb_file_lst.push_back("/data/dt3/dma/data/S_318432_1_320833-N00001-A00016.pki");
 ldb_file_lst.push_back("/data/dt3/dma/data/S_318432_1_320833-N00002-A00016.pki");
 ldb_file_lst.push_back("/data/dt3/dma/data/S_318432_1_320833-N00003-A00016.pki");
 ldb_file_lst.push_back("/data/dt3/dma/data/S_318432_1_320833-N00004-A00016.pki");
 ldb_file_lst.push_back("/data/dt3/dma/data/S_318432_1_320833-N00005-A00016.pki");
 ldb_file_lst.push_back("/data/dt3/dma/data/S_318432_1_320833-N00006-A00016.pki");
 ldb_file_lst.push_back("/data/dt3/dma/data/S_318432_1_320833-N00007-A00016.pki");
 ldb_file_lst.push_back("/data/dt3/dma/data/S_318432_1_320833-N00008-A00016.pki");

	
		#ifdef _232526_1_290358
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_1_290358-N00000-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_1_290358-N00001-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_1_290358-N00002-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_1_290358-N00003-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_1_290358-N00004-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_1_290358-N00005-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_1_290358-N00006-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_1_290358-N00007-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_1_290358-N00008-A00016.pki");
		#endif
		
		#ifdef _232526_2_289089
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_2_289089-N00000-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_2_289089-N00001-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_2_289089-N00002-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_2_289089-N00003-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_2_289089-N00004-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_2_289089-N00005-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_2_289089-N00006-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_2_289089-N00007-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_2_289089-N00008-A00016.pki");
		#endif
		
		#ifdef _232526_3_286620
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_3_286620-N00000-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_3_286620-N00001-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_3_286620-N00002-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_3_286620-N00003-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_3_286620-N00004-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_3_286620-N00005-A00016.pki");
		//ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_3_286620-N00006-A00016.pki");
		//ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_3_286620-N00007-A00016.pki");
		//ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_3_286620-N00008-A00016.pki");
		#endif
		
		#ifdef _232526_4_287822
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_4_287822-N00000-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_4_287822-N00001-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_4_287822-N00002-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_4_287822-N00003-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_4_287822-N00004-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_4_287822-N00005-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_4_287822-N00006-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_4_287822-N00007-A00016.pki");
		ldb_file_lst.push_back("/data/dt3/dma/data/S_232526_4_287822-N00008-A00016.pki");
		#endif		
		
}
