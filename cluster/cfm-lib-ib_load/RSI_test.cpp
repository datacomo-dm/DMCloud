#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <string>
#include "IBFile.h"
#include "RSI_interface.h"
#include "AutoHandle.h"
#include "dt_common.h"

using namespace ib_rsi;
using namespace std;

#define PACK_ROWS  65536
typedef struct column_rsi_handle{
    int col_type; // 1: int,long 2:float 3:string -1:other
    void* col_handle;// 4:IRSI_CMap,-1:NULL,other:IRSI_Hist
    char rsi_file_name[256]; // rsi file name
    column_rsi_handle(){
        col_type = -1;
        col_handle = NULL;
        rsi_file_name[0]=0;
    }
}column_rsi_handle,*column_rsi_handle_ptr;   

int Start(void *ptr);

int main(int argn,char* argv[]){
    if(argn != 2){
        printf("RSI_test 输入参数错误,输入参数为: RSI_test rsi索引保存路径 \n");
        printf("example: RSI_test /tmp \n");
        return -1;
    }
    char* RSI_dir = argv[1];
    int nRetCode = 0;
    WOCIInit("RSI_test");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(false);
    nRetCode=wociMainEntrance(Start,true,(void*)RSI_dir,2);
    WOCIQuit(); 
    return 0;
}


typedef struct stru_pack_info{
    int pack_no;
    int start_row;
    int end_row;
    stru_pack_info(){
        pack_no = -1;
        start_row = -1;
        end_row = -1;
    }
}stru_pack_info,*stru_pack_info_ptr;

int Start(void *ptr){
    char* rsi_path =(char*)ptr;

    column_rsi_handle rsi_handle_array[500];

    //>> 1. connect db and get src data
    AutoHandle dts;
    printf("选择源数据库...\n");
    dts.SetHandle(BuildConn(0));

    char sqlst[2000];
    memset(sqlst,0,2000);
    
    getString("请输入查询语句(中间不要回车)",NULL,sqlst);
    int fetchn=getOption("提交行数",100000,1000,1000000);

    AutoMt mt(dts,fetchn);
    mt.FetchFirst(sqlst);
    int row_number = mt.Wait();
    assert(row_number >=0);

    
    int tabid=getOption("rsi tabid",100000,1,1000000);

    //>> 2.build rsi struct
    int column_number = mt.GetColumnNum();
    for(int i=0;i<column_number;i++){
        switch(mt.getColType(i)){
            case COLUMN_TYPE_BIGINT:
            case COLUMN_TYPE_DATE:
            case COLUMN_TYPE_INT:
                rsi_handle_array[i].col_type = 1;
                rsi_handle_array[i].col_handle = create_rsi_hist();
                sprintf(rsi_handle_array[i].rsi_file_name,"%s/%s",rsi_path,
                    rsi_hist_filename(tabid,i).c_str());

                assert(rsi_handle_array[i].col_handle);
                break;
                
            case COLUMN_TYPE_FLOAT:                
                rsi_handle_array[i].col_type = 2;
                rsi_handle_array[i].col_handle = create_rsi_hist();
                sprintf(rsi_handle_array[i].rsi_file_name,"%s/%s",rsi_path,
                    rsi_hist_filename(tabid,i).c_str());
                
                assert(rsi_handle_array[i].col_handle);
                break;
                
            case COLUMN_TYPE_CHAR:
                rsi_handle_array[i].col_type = 3;
                rsi_handle_array[i].col_handle = create_rsi_cmap();
                sprintf(rsi_handle_array[i].rsi_file_name,"%s/%s",rsi_path,
                    rsi_cmap_filename(tabid,i).c_str());
                
                assert(rsi_handle_array[i].col_handle);
                break;


            default: 
                assert(0); 
                break;
        }
    }

    //>> 3.build pack row info
    stru_pack_info pack_info_arrry[10000];
    int _pack_no = 0;
    for(int i=0;i<row_number;i++){
        if(i%PACK_ROWS == 0){            
            pack_info_arrry[_pack_no].pack_no = _pack_no;
            pack_info_arrry[_pack_no].start_row = i;
            if((row_number-i) >= PACK_ROWS){
                pack_info_arrry[_pack_no].end_row= i+PACK_ROWS-1;    
            }else{                
                pack_info_arrry[_pack_no].end_row= row_number-1;  
            }
            _pack_no++;
        }
    }

    //>> 4. build rsi struct
    for(int i=0;i<column_number;i++){
        if(rsi_handle_array[i].col_type == 1){
           ((IRSI_Hist*)(rsi_handle_array[i].col_handle))->Create(row_number,true);
        }else if(rsi_handle_array[i].col_type == 2){
           ((IRSI_Hist*)(rsi_handle_array[i].col_handle))->Create(row_number,false);
        }else if(rsi_handle_array[i].col_type == 3){
           ((IRSI_CMap*)(rsi_handle_array[i].col_handle))->Create(row_number);
        }else break;        
    }

    //>> 5. build rsi info 
    for(int i=0;i<column_number;i++){
        long    l_min=0,l_max=0;
        double  f_min=0,f_max=0.0;
        int pack = 0;         
        uint obj = 0;
        _int64 lv = 0;
        double dv = 0;
        std::string sv = "";
        switch(mt.getColType(i)){
            case COLUMN_TYPE_BIGINT:
            case COLUMN_TYPE_DATE:
                for(pack = 0;pack < _pack_no;pack++){
                    // get max,min value
                    for(obj = pack_info_arrry[pack].start_row;obj <pack_info_arrry[pack].end_row;obj++){                 
                        lv = *mt.PtrLong(i,obj);
                        if(l_min > lv ) l_min = lv ;
                        else if(l_max < lv) l_max = lv ;
                    }

                    // put value
                    for(obj = pack_info_arrry[pack].start_row;obj <pack_info_arrry[pack].end_row;obj++){                 
                        lv  = *mt.PtrLong(i,obj);
                        ((IRSI_Hist*)(rsi_handle_array[i].col_handle))->PutValue(lv,pack_info_arrry[pack].pack_no,l_min,l_max);
                    }
                }
                break;

            case COLUMN_TYPE_INT:
                for(pack = 0;pack < _pack_no;pack++){
                    // get max,min value
                    for(obj = pack_info_arrry[pack].start_row;obj <pack_info_arrry[pack].end_row;obj++){                 
                        lv = *mt.PtrInt(i,obj);
                        if(l_min > lv ) l_min = lv ;
                        else if(l_max < lv ) l_max = lv ;
                    }

                    // put value
                    for(obj = pack_info_arrry[pack].start_row;obj <pack_info_arrry[pack].end_row;obj++){                 
                        lv  = *mt.PtrInt(i,obj);
                        ((IRSI_Hist*)(rsi_handle_array[i].col_handle))->PutValue(lv,pack_info_arrry[pack].pack_no,l_min,l_max);
                    }
                }
                break;                
            case COLUMN_TYPE_FLOAT:                
                for(pack = 0;pack < _pack_no;pack++){
                    // get max,min value
                    for(obj = pack_info_arrry[pack].start_row;obj <pack_info_arrry[pack].end_row;obj++){                 
                        dv = *mt.PtrDouble(i,obj);
                        if(f_min > dv ) f_min = dv ;
                        else if(f_max < dv ) f_max = dv ;
                    }

                    // put value
                    for(obj = pack_info_arrry[pack].start_row;obj <pack_info_arrry[pack].end_row;obj++){                 
                        dv  = *mt.PtrDouble(i,obj);
                        ((IRSI_Hist*)(rsi_handle_array[i].col_handle))->PutValue(dv,pack_info_arrry[pack].pack_no,f_min,f_max);
                    }
                }
                break;
                
            case COLUMN_TYPE_CHAR:
                for(pack = 0;pack < _pack_no;pack++){
 
                    // put value
                    for(obj = pack_info_arrry[pack].start_row;obj <pack_info_arrry[pack].end_row;obj++){                 
                        sv = std::string(mt.PtrStr(i,obj));
                        ((IRSI_CMap*)(rsi_handle_array[i].col_handle))->PutValue(sv,pack_info_arrry[pack].pack_no);
                    }
                }
                break;
                
            default: 
                assert(0); 
                break;
        }        
    }

    //>> 6. Save rsi file
    for(int i=0;i<column_number;i++){
        if(rsi_handle_array[i].col_handle == NULL) break;

        printf("保存[%s].\n",rsi_handle_array[i].rsi_file_name);
        IBFile ib_file;
        ib_file.OpenCreate(rsi_handle_array[i].rsi_file_name);

        IRSIndex* p = (IRSIndex*)rsi_handle_array[i].col_handle;
        assert(p);
        p->Save(&ib_file,0);
        ib_file.Close();

        delete p;
    }

    printf("数据抽取语句[%s]对应[%d]列[%d]行的RSI索引文件,表ID[%d]生成到[%s]目录完成.\n",sqlst,column_number,row_number,tabid,rsi_path);

    return 0;
    
}
