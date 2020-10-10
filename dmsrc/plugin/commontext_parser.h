/* gprs_parser.h --

   This file is part of the DataMerger data extrator.

   Copyright (C) 2010 DataComo
   All Rights Reserved.
   
   Wang Guosheng
   <wangguosheng@datacomo.com>
   http://www.datacomo.com
   http://mobi5.cn
 */
#ifndef COMMONTXT_PARSER_H
#define COMMONTXT_PARSER_H
#include "dumpfile.h"
#include "column_parser.h"
    
class CommonTxtParser: public FileParser {
    char *fieldval ;
    char *linebak;
    int yyyy,dd,mm,hour;
    int ExtractNum(int offset,const char *info,int len) ;
    bool SwitchOpt(char *deststr,char *opt,int val,int len);
    bool SwitchOpt(char *deststr,char *opt,char *value) ;
    //���طֽ����ֶ�����,colidx����ֶ�������collen��Ź̶�����
    int parserfixlen(int memtab,const char *cols,int *fixflag,int *collen);
    int parsercolumn(int memtab,const char *cols,int *colsflag);
    //>> begin:fix DMA-604,��Ҫ�����п�ȵ���
    bool colsAdjustWidth[MAX_FILE_COLUMNS]; // �洢��ֵ��colspos���Ӧ
    //<< end:fix dma-604  


private: 
    //>> begin: fix dma-1086, http����
    Column_parser _column_parser_obj;            // http ������ѯ�ؼ��ֺ�����Ӧ��ʹ��
    #define HTTP_DOWNLOAD_LEN  512
    typedef struct stru_Http_translate{
        stru_Http_translate(){
            init();
        }
        void init(){
            _http_col = _key_col = _download_col = -1;
            _key_col_value[0] = 0;
            _download_col_value[0] = 0;
            _update_check = 0;
        }
        bool is_valid(){
            if(_http_col != -1){
                if(_key_col != -1 || _download_col != -1){
                    return true;
                }
            }
            return false;
        }
        int  get_column_count(){
            if(is_valid()){
                if(_key_col != -1 && _download_col != -1){
                    return 2;
                }else{
                    return 1;
                }
            }
        }
        int _http_col;                                 // http ������,http�У��ؼ����У������б����ǰ����˳������

        int _key_col;                                  // �ؼ�����
        char _key_col_value[HTTP_DOWNLOAD_LEN];        // ��Ч�Ĺؼ���
        
        int _download_col;                             // ������
        char _download_col_value[HTTP_DOWNLOAD_LEN];   // ��Ч��������

        int _update_check;               // �����жϴ�http�н���������[�ؼ��ֺ����������Ƿ��Ѿ����µ��ؼ��ֺ�������������,������ɺ�Ӧ��0,����ǰӦ�����е���Ч��]
    }stru_Http_translate,*stru_Http_translate_ptr;
    stru_Http_translate _st_http_translate_obj;
    //<< end: fix dma-1086

private:
    //>> begin: fix dma-794(���ʱ��),dma-941(����ļ�����)
    typedef struct stru_external{
        stru_external(){
            init();
        }
        void init(){
            _load_date_col_index = _load_filename_col_index = -1;
        }
        bool is_valid(){
            if(_load_filename_col_index != -1 || 
                _load_filename_col_index != -1)
                return true;
            else 
                return false;
        }
        int  get_column_count(){
            if(is_valid()){
                if(_load_filename_col_index != -1 && 
                    _load_date_col_index != -1){
                    return 2;
                }else{
                    return 1;
                }
            }
        }
        int _load_date_col_index;      // װ��ʱ��
        int _load_filename_col_index;   // װ�����ļ�����
    }stru_external,*stru_external_ptr;
    stru_external _st_external_obj;
    
    //>> end: fix dma-794,dma-941

private:
    // �ж������Ƿ�׼ȷ
    // _file_col_num: �ļ���ȷʵ���ڵ���
    // _header_col_num: files::header�ж������
    // _st_ext: ���ʱ�������ļ�������
    // _st_http: http ��������Ϣ
    // _msg: ������ʾ��Ϣ
    bool check_column_count(const int _file_col_num,const int _header_col_num,stru_external& _st_ext,stru_Http_translate& _st_http,std::string& msg);

    // �����������з��ֵĴ�������д���ļ���ɺ����ѹ������,fix dma-1159
    bool CompressErrData();
    
public :
    CommonTxtParser():FileParser(){
        linebak =new char[MAXLINELEN];
        fieldval =new char[MAXLINELEN];
        
        yyyy = dd = mm = hour = -1;
    }
    virtual ~CommonTxtParser() {
        delete []linebak;delete []fieldval;
    }
    // fill filename member,the wrapper check conflict concurrent process,
    // if one has privillege to process current file ,then invoke DoParse
    // ����ֵ��
    //  -1 ������������������״̬�Ѿ��������������޸�Ϊȡ��������ͣ
    //  0�������ļ�
    //  1��ȡ����Ч���ļ�
    virtual int GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly=false) ;
    // if end ,update task to next step(with lock)
    // critical function !
    // checked every file parser completed.
    // �����ǲ������ļ���Ҫ�����ʱ�򣬲��ܵ��������麯��
    virtual bool ExtractEnd(int _dbc) ;
    virtual int WriteErrorData(const char *line) ;
    virtual int GetFileSeq(const char *filename) ;
    
    // return value:
    //  -1: a error occurs.�����ڴ���ֶνṹ��ƥ��
    //  0:memory table has been filled full,and need to parse again
    //  1:file parse end
    //while parser file ended,this function record file process log and 
    // sequence of file in dp dictions tab.
    virtual int DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) ;
};
IFileParser *BuildParser(void);
#endif
