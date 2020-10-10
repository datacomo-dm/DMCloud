#include "dt_common.h"
#include "AutoHandle.h"
#include "commontext_parser.h"
#include <string.h>

#ifndef min
#define min(a,b) ((a)>(b)?(b):(a))
#endif
#ifndef max
#define max(a,b) ((a)>(b)?(a):(b))
#endif


extern "C" uLongCRC crc32(uLongCRC crc, const Bytef * buf, uInt len);

IFileParser *BuildParser(void)
{
    return new CommonTxtParser();
}
#ifndef TMP_LEN
#define TMP_LEN 100
#endif

#define ExtractDate(optionstr,value,len,func)   {\
    const char *opt=pWrapper->getOption(optionstr,"");\
    int offset=0;\
    /* value=-1;  // warning:delete 20130114, Fix bug DM-206 */  \
    if(strlen(opt)>0) {\
        assert(strlen(opt)<TMP_LEN);\
        if(opt[strlen(opt)-1]=='d') {\
            char _tmp[TMP_LEN];\
            strcpy(_tmp,opt);\
            _tmp[strlen(opt)-1] = 0;\
            offset=atoi(_tmp);\
            value=ExtractNum(offset,dbname,len);\
        } \
        else if(opt[strlen(opt)-1]=='t') { \
            char _tmp[TMP_LEN];\
            strcpy(_tmp,opt);\
            _tmp[strlen(opt)-1] = 0;\
            offset=atoi(_tmp);\
            value=ExtractNum(offset,tabname,len); \
        } \
        else if(opt[0]=='p') { \
            value=func(datapartdate);\
        } \
        else value=atoi(opt);\
    }\
}

int CommonTxtParser::ExtractNum(int offset,const char *info,int len)
{
    int value =0;
    char numchar[20];
    if(offset<0) {
        strncpy(numchar,info+strlen(info)+offset,len);
        numchar[len]=0;
        value=atoi(numchar);
    } else {
        strncpy(numchar,info+offset,len);
        numchar[len]=0;
        value=atoi(numchar);
    }
    return value;
}
bool CommonTxtParser::SwitchOpt(char *deststr,char *opt,int val,int len)
{
    char sourcestr[300];
    strcpy(sourcestr,deststr);
    char *strdest=strcasestr(sourcestr,opt);
    if(strdest) {
        char patt[20];
        sprintf(patt,"%%0%dd",len);
        if(val<0) ThrowWith("δ����files:%s����",opt+1);
        strncpy(deststr,sourcestr,strdest-sourcestr);
        deststr[strdest-sourcestr]=0;

        sprintf(deststr+strlen(deststr),patt,val);
        strcat(deststr,sourcestr+(strdest-sourcestr)+strlen(opt));
        return true;
    }
    return false;
}
bool CommonTxtParser::SwitchOpt(char *deststr,char *opt,char *value)
{
    char sourcestr[300];
    strcpy(sourcestr,deststr);
    char *strdest=strcasestr(sourcestr,opt);
    if(strdest) {
        //char patt[10];
        //sprintf(patt,"%%0%dd",len);
        if(!value) ThrowWith("����%s��ֵ��Ч",opt+1);
        strncpy(deststr,sourcestr,strdest-sourcestr);
        deststr[strdest-sourcestr]=0;
        strcat(deststr,value);
        strcat(deststr,sourcestr+(strdest-sourcestr)+strlen(opt));
        return true;
    }
    return false;
}


// fill filename member,the wrapper check conflict concurrent process,
// if one has privillege to process current file ,then invoke DoParse
// ����ֵ��
//  -1 ������������������״̬�Ѿ��������������޸�Ϊȡ��������ͣ
//  0�������ļ�
//  1��ȡ����Ч���ļ�
// checkonly : dma-111,ֻ�����û���ļ������ɼ�
int CommonTxtParser::GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly)
{
    AutoMt mt(dbc,10);
    mt.FetchAll("select databasename,tabname from dp.dp_table where tabid=%d",tabid);
    if(mt.Wait()<1) ThrowWith("Table id:%d not found.",tabid);
    char tabname[200],dbname[200];
    strcpy(dbname,mt.PtrStr(0,0));
    strcpy(tabname,mt.PtrStr(1,0));
    char datapartdate[30];
    mt.FetchAll("select begintime from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
    if(mt.Wait()<1) ThrowWith("Table id:%d,datapart:%d not found in dp.dp_datapart.",tabid,datapartid);
    memcpy(datapartdate,mt.PtrDate(0,0),7);
    ExtractDate("files:year",yyyy,4,wociGetYear);
    ExtractDate("files:month",mm,2,wociGetMonth);
    ExtractDate("files:day",dd,2,wociGetDay);
    ExtractDate("files:hour",hour,2,wociGetHour);

    //>> begin: fix dma-1298,��ڵ�������⴦��
    char _filename_tmp[256];
    char filepatt[300];
    filepatt[0] = 0;
    if(slaver_index > 0 ) {
        sprintf(_filename_tmp,"files:filename_%d",slaver_index);
        strcpy(filepatt,pWrapper->getOption(_filename_tmp,""));
        if(filepatt[0] == 0){ 
            lgprintf("����:�ɼ�����ctl�����ļ���[files::filename_%d]������,ʹ��Ĭ������ѡ��[files::filename]",slaver_index);
            strcpy(_filename_tmp,"files:filename");
            strcpy(filepatt,pWrapper->getOption(_filename_tmp,""));
        }
    } else {
        strcpy(_filename_tmp,"files:filename");
        strcpy(filepatt,pWrapper->getOption(_filename_tmp,""));
    }

    //>> end: fix dma-1298,��ڵ�������⴦��
    while(true) {
        if(SwitchOpt(filepatt,(char*)"$year",yyyy,4)) continue;
        if(SwitchOpt(filepatt,(char*)"$month",mm,2)) continue;
        if(SwitchOpt(filepatt,(char*)"$day",dd,2)) continue;
        if(SwitchOpt(filepatt,(char*)"$hour",hour,2)) continue;
        if(SwitchOpt(filepatt,(char*)"$table",tabname)) continue;
        if(SwitchOpt(filepatt,(char*)"$db",dbname)) continue;
        break;
    }

    // Begin:�����Ƿ�ɾ��Դ�ļ���Ĭ��ɾ��[1:ɾ����0:��ɾ��] 2012-11-02
    fm.SetDeleteFileFlag(pWrapper->getOption("files:delsrcdata",0));
    // End: �����Ƿ�ɾ��Դ�ļ���Ĭ��ɾ��[1:ɾ����0:��ɾ��]

begin_to_list_file_path: // fix : dma-1061

    //>>begin:fix dma-926 ��ȡ�������·����Ϣ,���ڴ���FILENAME�к���·�������
    char temp_path[300];
    if(strlen(pWrapper->getOption("ftp:path","")) > 0) { // ����ftp��·��
        if( (str_path_vector.empty()) || (current_path_index == -1) ) { // ����·�����ڴ���
            splite_path_info(pWrapper->getOption("ftp:path",""),str_path_vector);
            current_path_index = 0;
            assert(str_path_vector.size()>0);
        }

        if(strlen(fix_ftp_path)==0 || strlen(fix_filepatt) == 0) {
            splite_path_name(filepatt,temp_path,fix_filepatt);
            sprintf(fix_ftp_path,"%s/%s",str_path_vector[current_path_index].c_str(),temp_path);

            //>> begin: fix dma-1315:��ȡ��·�����ļ�������·��md5ֵ
            {
                unsigned char md[16] = {0}; // 16 bytes     ,���ܶ����char,��������sprintf������md5array�д�������
                MD5((unsigned char*)fix_ftp_path,strlen(fix_ftp_path),md);
                for(int i=0; i<16; i++) {
                    sprintf(fix_cur_pach_md5+strlen(fix_cur_pach_md5),"%02x",md[i]);
                }
            }
            //<< end: fix dma-1315:��ȡ��·�����ļ�������·��md5ֵ
        }
    }
    if(strlen(pWrapper->getOption("local:path","")) > 0) { // ������Ŀ¼��·��
        if((str_path_vector.empty()) || (current_path_index == -1)) { // ����·�����ڴ���
            splite_path_info(pWrapper->getOption("local:path",""),str_path_vector);
            current_path_index = 0;
            assert(str_path_vector.size()>0);
        }
        if(strlen(fix_localpath)==0 || strlen(fix_filepatt) == 0) {
            splite_path_name(filepatt,temp_path,fix_filepatt);
            sprintf(fix_localpath,"%s/%s",str_path_vector[current_path_index].c_str(),temp_path);

            //>> begin: fix dma-1315:��ȡ��·�����ļ�������·��md5ֵ
            {
                unsigned char md[16] = {0}; // 16 bytes     ,���ܶ����char,��������sprintf������md5array�д�������
                MD5((unsigned char*)fix_localpath,strlen(fix_localpath),md);
                for(int i=0; i<16; i++) {
                    sprintf(fix_cur_pach_md5+strlen(fix_cur_pach_md5),"%02x",md[i]);
                }
            }
            //<< end: fix dma-1315:��ȡ��·�����ļ�������·��md5ֵ
        }
    }
    //<<end:fix dma-926

    // if not set datetime ,set to now
    char now[10];
    wociGetCurDateTime(now);
    if(yyyy<0) yyyy=wociGetYear(now);
    if(mm<0) mm=wociGetMonth(now);
    if(dd<0) dd=wociGetDay(now);
    if(hour<0) hour=wociGetHour(now);
    char backuppath[300];
    sprintf(backuppath,"%s/%s.%s",pWrapper->getOption("files:backuppath","./"),dbname,tabname);
    sprintf(backuppath+strlen(backuppath),"/part%d",datapartid);
    xmkdir(backuppath);

    int rt= 0;

    // �ȵ�Դ����Ŀ¼�л�ȡ����
    rt = commonGetFile(backuppath,false,fix_filepatt,sp,tabid,datapartid,checkonly);
    if((rt==0) && frombackup) { // Դ����·���л�ȡ�����ļ�,������Ŀ¼�л�ȡ
        rt=commonGetFile(backuppath,true,fix_filepatt,sp,tabid,datapartid,checkonly);
    }

    if(rt == 0) { // û�л�ȡ���ļ�,����һ��Ŀ¼��ȡ�ļ�,
        if(current_path_index != (str_path_vector.size()-1)) { // û������Ŀ¼�������ļ����
            current_path_index += 1;
            fix_ftp_path[0]= 0;
            fix_localpath[0] = 0;
            fix_cur_pach_md5[0] = 0;
            fix_filepatt[0] = 0;
            goto begin_to_list_file_path;   // ������һ��Ŀ¼�е��ļ�
        }
        else{
            // fix dma-2006: add by liujs at 20160510
            lgprintf("��������ɨ�����[%d]��Ŀ¼������,������һ�ֱ���Ŀ¼......",str_path_vector.size());
            current_path_index = -1;
            str_path_vector.clear();
            fix_ftp_path[0]= 0;
            fix_localpath[0] = 0;
            fix_cur_pach_md5[0] = 0;
            fix_filepatt[0] = 0;
        }
    }
    return rt;
}
// if end ,update task to next step(with lock)
// critical function !
// checked every file parser completed.
// �����ǲ������ļ���Ҫ�����ʱ�򣬲��ܵ��������麯��
bool CommonTxtParser::ExtractEnd(int _dbc)
{
    wociSetEcho(false);
    AutoMt mt(_dbc,10);
    mt.FetchAll("select case status when 10 then 2 when 1 then 2 else status end status,max(fileseq) xseq,min(fileseq) mseq,count(*) ct,sum(recordnum) srn "
                " from dp.dp_filelog where tabid=%d and datapartid=%d and slaveridx=%d group by case status when 10 then 2 when 1 then 2 else status end ",
                tabid,datapartid,slaver_index);
    int rn = mt.Wait();
    int maxseq=0;
    int minseq=0;
    LONG64 filect=0;
    if(rn != 0) {
        if(mt.Wait()!=1) return false; //�ļ���û�д����ꡣ
        if(mt.GetDouble("status",0)!=2) return false;//�ļ���û�д����ꡣ

        maxseq=mt.GetInt("xseq",0);
        minseq=mt.GetInt("mseq",0);
        filect=mt.GetLong("ct",0);
        lgprintf("��С�ļ���:%d,����ļ���:%d,�ļ���:%ld.",minseq,maxseq,mt.GetLong("ct",0));
    } else {
        // TODO: nothing ,fix dma-700
    }
    //Ӧ�ƶ�Ҫ��ȥ����������Լ��

    // Begin: �����crc�������ļ���ţ�������У��
    if(pWrapper->getOption("files:crcsequence",0)==0 && rn > 0) {
        // �Ƿ�У�����
        if(pWrapper->getOption("files:sequencecheck",1)!=0) {
            if(minseq!=1 || maxseq-minseq+1!=mt.GetLong("ct",0)) {
                lgprintf("�ļ���Ų�������Ӧ�������1-%d,Ӧ����%d���ļ���ʵ����%d���ļ�....",
                         maxseq,maxseq,mt.GetLong("ct",0));
                return false;//�ļ�������
            }
        }
    }
    // End: �����crc�������ļ���ţ�������У��

    /* ��¼���ļ�����������֣�
    �ļ��Ѿ��ֽ⵽�ڴ�������ڴ��û������û��Ԥ����Ҳ��û��д��dp_middledatafile��
    ��˼�¼���˶��쳣

    */
    if(pWrapper->getOption("files:forceenddump",0)!=1) {
        mt.FetchAll("select timestampdiff(MINUTE,now(),date_add(date_add('%04d-%02d-%02d',interval 1 day),interval '%d:%d' hour_minute))",
                    yyyy,mm,dd,pWrapper->getOption("files:enddelay",600)/60,pWrapper->getOption("files:enddelay",600)%60);
        mt.Wait();
        LONG64 tm1=mt.GetLong(0,0);
        lgprintf("�����ֹʱ��:%ld.\n",tm1);
        if(mt.GetLong(0,0)>=0) return false;//��û���ӳ�ʱ��
        mt.FetchAll( "select timestampdiff(MINUTE,now(),date_add(max(endproctime),interval %d minute)) from dp.dp_filelog where tabid=%d and datapartid=%d and slaveridx=%d",
                     pWrapper->getOption("files:endcheckdelay",45),tabid,datapartid,slaver_index);
        if(mt.Wait()!=1) return false;
        tm1=mt.GetLong(0,0);
        lgprintf("�����ļ��ӳ�:%ld.\n",tm1);
        if(mt.GetLong(0,0)>=0) return false;//��û�����һ���ļ�����������ӳ�ʱ��
    }
    lgprintf("�ļ���ȡ��������%d���ļ�(���к�%d-%d)��",filect,minseq,maxseq);
    return true;
}

int CommonTxtParser::GetFileSeq(const char *filename)
{
    // Jira-49��add two control parameters to file load .
    char seqbuf[50];
    int backoff=pWrapper->getOption("files:sequencebackoffset",0);
    int seqlen=pWrapper->getOption("files:sequencelen",4);

    //Begin: �ļ����crc����
    if(pWrapper->getOption("files:crcsequence",0)==1) {
        int fileSeq = 0; // �ļ����
        fileSeq = crc32(0,(const Bytef *)filename,(uInt)strlen(filename));
        return fileSeq;
    }
    //End:�ļ����crc����

    if(backoff<0) backoff=0;
    const char *pseq=filename+strlen(filename)-backoff-1;
    while(!isdigit(*pseq) && pseq>filename) pseq--;
    // last digit should be included
    pseq++;
    if(pseq-filename<seqlen) seqlen=pseq-filename;
    strncpy(seqbuf,pseq-seqlen,seqlen);
    seqbuf[seqlen]=0;
    pseq=seqbuf;
    while(*pseq) {
        if(!isdigit(*pseq)) {
            strcpy((char *)pseq,pseq+1);
        } else pseq++;
    }
    return atoi(seqbuf);
}
int CommonTxtParser::parsercolumn(int memtab,const char *cols,int *colsflag)
{
    memset(colsflag,0,sizeof(int)*wociGetColumnNumber(memtab));
    if(!cols || strlen(cols)<1) return 0;
    int colct=0;
    int colarray[MAX_COLUMN];
    int cvtct=wociConvertColStrToInt(memtab,cols,colarray);
    for(int i=0; i<cvtct; i++)
        colsflag[colarray[i]]=1;
    return cvtct;
}
// fixflag collen use 0 base index
int CommonTxtParser::parserfixlen(int memtab,const char *cols,int *fixflag,int *collen)
{
    //split colname
    memset(fixflag,0,sizeof(int)*wociGetColumnNumber(memtab));
    memset(collen,0,sizeof(int)*wociGetColumnNumber(memtab));
    if(!cols || strlen(cols)<3) return 0;
    char colname[COLNAME_LEN];
    const char *pcol=cols;
    int cni=0;
    int colct=0;
    memset(fixflag,0,sizeof(int)*wociGetColumnNumber(memtab));
    memset(collen,0,sizeof(int)*wociGetColumnNumber(memtab));
    while(*pcol) {
        if(*pcol==':' || !pcol[1]) {
            colname[cni]=0;
            cni=0;
            int idx=wociGetColumnPosByName(memtab,colname);
            fixflag[idx]=1;
            collen[idx]=atoi(pcol+1);
            colct++;
            while(*pcol && *pcol!=',') pcol++;
            if(!pcol) break;
        } else colname[cni++]=*pcol;
        pcol++;
    }
    return colct;
}

// return value:
//  -1: a error occurs.�����ڴ���ֶνṹ��ƥ��
//  0:memory table has been filled full,and need to parse again
//  1:file parse end
//while parser file ended,this function record file process log and
// sequence of file in dp dictions tab.
int CommonTxtParser::WriteErrorData(const char *line)
{
    char errfile[300];
    strcpy(errfile,pWrapper->getOption("files:errordatapath",""));
    //δ���ô����ļ����·�����򲻱����������
    if(errfile[0]==0) return 0;
    if(errfile[strlen(errfile)-1]!='/') strcat(errfile,"/");
    strcat(errfile,basefilename.c_str());
    strcat(errfile,".err");
    FILE *err=fopen(errfile,"a+t");
    if(err==NULL) {
        lgprintf("��������ļ�'%s'ʧ�ܣ�����Ȩ��!",errfile);
    }
    //����Ҫ�ӻس�
    if(fputs(line,err)<1)
        lgprintf("��������ļ�'%s'ʧ�ܣ��������ļ�ϵͳ��!",errfile);
    fclose(err);
    return 1;
}

// �����������з��ֵĴ�������д���ļ���ɺ����ѹ������,fix dma-1159
bool CommonTxtParser::CompressErrData()
{
    char errfile[300];
    strcpy(errfile,pWrapper->getOption("files:errordatapath",""));
    //δ���ô����ļ����·�����򲻱����������
    if(errfile[0]==0) return false;
    if(errfile[strlen(errfile)-1]!='/') strcat(errfile,"/");
    strcat(errfile,basefilename.c_str());
    strcat(errfile,".err");
    gzipfile(errfile); // gzip file and delete src file
    lgprintf("gzip file %s .",errfile);
    return true;
}


// ����������һ���ļ��д��ڶ��������ݣ���Ҫ�������������ݽ���д������ļ������ǲ���Ҫ�����:$DATAMERGER_HOME/log/dump/*.log��
#define HLG_SKIP_PRINTF_LOG "HLG_SKIP_PRINTF_LOG"

// �ж������Ƿ�׼ȷ
// _file_col_num: �ļ���ȷʵ���ڵ���
// _header_col_num: files::header�ж������
// _st_ext: ���ʱ�������ļ�������
// _st_http: http ��������Ϣ
// _msg: ������ʾ��Ϣ
bool CommonTxtParser::check_column_count(const int _file_col_num,const int _header_col_num,stru_external& _st_ext,stru_Http_translate& _st_http,std::string& msg)
{
    int ct = _file_col_num;
    int colct = _header_col_num;
    char _msg[1024];

    if(_st_ext.is_valid()&&_st_http.is_valid()) {
        if(_st_ext.get_column_count() == 2 && _st_http.get_column_count() == 2 && (ct != colct-4)) {
            sprintf(_msg,"�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶ�(�������ʱ����,����ļ�����,HTTP�ؼ�����,HTTP������)��ʵ����%d��.",filename.c_str(),curoffset,colct-4,ct);
        } else if(_st_ext.get_column_count() == 2 && _st_http.get_column_count() == 1 && (ct != colct-3)) {
            if(_st_ext._load_date_col_index == -1) {
                sprintf(_msg,"�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶ�(��������ļ�����,HTTP�ؼ�����,HTTP������)��ʵ����%d��.",filename.c_str(),curoffset,colct-3,ct);
            } else if(_st_ext._load_filename_col_index == -1) {
                sprintf(_msg,"�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶ�(�������ʱ����,����ļ�����,HTTP�ؼ�����,HTTP������)��ʵ����%d��.",filename.c_str(),curoffset,colct-3,ct);
            } else {
                return true;
            }
        } else if(_st_ext.get_column_count() == 1 && _st_http.get_column_count() == 2 && (ct != colct-3)) {
            if(_st_http._key_col== -1) {
                sprintf(_msg,"�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶ�(��������ļ�����,���ʱ����,,HTTP������)��ʵ����%d��.",filename.c_str(),curoffset,colct-3,ct);
            } else if(_st_http._download_col== -1) {
                sprintf(_msg,"�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶ�(�����������ʱ����,����ļ�����,HTTP�ؼ�����)��ʵ����%d��.",filename.c_str(),curoffset,colct-3,ct);
            } else {
                return true;
            }
        } else {
            if(_st_ext._load_date_col_index != -1 && _st_http._key_col!= -1 && (ct != colct-2)) {
                sprintf(_msg,"�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶ�(�������ʱ����,HTTP�ؼ�����)��ʵ����%d��.",filename.c_str(),curoffset,colct-2,ct);
            } else if(_st_ext._load_date_col_index != -1 && _st_http._download_col!= -1 && (ct != colct-2)) {
                sprintf(_msg,"�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶ�(�������ʱ����,HTTP������)��ʵ����%d��.",filename.c_str(),curoffset,colct-2,ct);
            } else if(_st_ext._load_date_col_index == -1 && _st_http._download_col!= -1 && (ct != colct-2)) {
                sprintf(_msg,"�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶ�(��������ļ�������,HTTP������)��ʵ����%d��.",filename.c_str(),curoffset,colct-2,ct);
            } else if(_st_ext._load_date_col_index == -1 && _st_http._key_col!= -1 && (ct != colct-2)) {
                sprintf(_msg,"�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶ�(��������ļ�������,HTTP������)��ʵ����%d��.",filename.c_str(),curoffset,colct-2,ct);
            } else {
                return true;
            }
        }
    } else if(_st_ext.is_valid()) {
        if(_st_ext.get_column_count() == 2 && (ct != colct-2)) {
            sprintf(_msg,"�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶ�(�������ʱ����,����ļ�����)��ʵ����%d��.",filename.c_str(),curoffset,colct-2,ct);
        } else if(_st_ext.get_column_count() == 1 && _st_ext._load_date_col_index != -1 && (ct != colct-1)) {
            sprintf(_msg,"�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶ�(�������ʱ����)��ʵ����%d��.",filename.c_str(),curoffset,colct-1,ct);
        } else if(_st_ext.get_column_count() == 1 && _st_ext._load_filename_col_index != -1 && (ct != colct-1)) {
            sprintf(_msg,"�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶ�(��������ļ�������)��ʵ����%d��.",filename.c_str(),curoffset,colct-1,ct);
        } else {
            return true;
        }
    } else if(_st_http.is_valid()) {
        if(_st_http.get_column_count() == 2 && (ct != colct-2) ) {
            sprintf(_msg,"�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶ�(�������ʱ����,����ļ�����)��ʵ����%d��.",filename.c_str(),curoffset,colct-2,ct);
        } else if(_st_http.get_column_count()==1 && _st_http._key_col != -1 && (ct != colct-1)) {
            sprintf(_msg,"�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶ�(����HTTP�����ؼ�����)��ʵ����%d��.",filename.c_str(),curoffset,colct-1,ct);
        } else if(_st_http.get_column_count()==1 && _st_http._download_col != -1 && (ct != colct-1)) {
            sprintf(_msg,"�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶ�(����HTTP����������)��ʵ����%d��.",filename.c_str(),curoffset,colct-1,ct);
        } else {
            return true;
        }
    } else if(ct != colct) {
        sprintf(_msg,"�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶΣ�ʵ����%d��.",filename.c_str(),curoffset,colct,ct);
    } else {
        return true;
    }
    msg = _msg;
    return false;
}


  /****************************************************/
  // ��ȡһ������,fix DMA-2122,add by liujs
char* my_fgets(char *s, int n,  FILE *stream,int &read_number)
{
   int n_ori = n;
   read_number = 0;
   register int c;
   register char *cs;
   cs=s;
   while(--n>0 &&(c = getc(stream))!=EOF)
   if ((*cs++=  c) =='\n'){
         break;
   }
   *cs ='\0';
   
   read_number = (n_ori-n);     
   return (c == EOF && cs == s) ?NULL :s ;
}

void my_remove_zore(char* s,int n)
{
    char *dest = new char[n];
    int pos=0;
    for(int i=0;i<n;i++)
    {
       if(s[i]!='\0'){
            dest[pos++]=s[i];
       }
    }
    dest[pos]=0;

    strcpy(s,dest); // ������ȥ
    
    delete [] dest;
}
/********************************************************/



// ����
// 0: �ڴ��������ǰ�ļ�δ������
// 1: �ڴ��δ������ǰ�ļ��Ѵ�����
// 3: �ڴ��������ǰ�ļ��Ѵ�����
int CommonTxtParser::DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid)
{
    AutoMt mt(0,10);
    mt.SetHandle(memtab,true);
    //mt.Reset();
    char sepchar[10];
    if(fp==NULL) {
        fp=fopen(filename.c_str(),"rt");
        if(fp==NULL) ThrowWith("�ļ�'%s'��ʧ��!",filename.c_str());
        //����ֶ�����
        strcpy(line,pWrapper->getOption("files:header",""));
        if(strlen(line)<1) {
            if(fgets(line,MAXLINELEN,fp)==NULL) ThrowWith("���ļ�'%s'ʧ��!",filename.c_str());
            curoffset=1;  //����
        } else curoffset=0; //����
        int sl=strlen(line);   //��¼����
        if(line[sl-1]=='\n') line[sl-1]=0;
        if(line[0]=='*') {
            colct=wociGetColumnNumber(memtab);  //��ȡ�ֶ���
            for(int i=0; i<colct; i++)
                colspos[i]=i;
        } else colct=wociConvertColStrToInt(memtab,line,colspos); //��ȡ�ֶ���

        //>> begin: ������Ҫ���Ϊnull���ַ�������,add by liujs
        int _colct=mt.GetColumnNum();  //��ȡ�ֶ���
        nulls_colct = 0;
        if(_colct != colct) { // files::header ����* ���������е����
            for(int i=0; i<_colct; i++) {
                if(mt.getColType(i) == COLUMN_TYPE_CHAR && mt.GetColLen(i) >8) { // �ַ��������ȳ���8
                    bool need_set_null = true;
                    for(int j=0; j<colct; j++) {
                        if(colspos[j] == i) {    // ����Ҫ����null
                            need_set_null = false;
                            break;
                        }
                    }

                    if(need_set_null) {//  ��Ҫ����Ϊnull��
                        nulls_colspos[nulls_colct] = i;
                        nulls_colct++;
                    }
                }
            }
        }
        //<< end: ������Ҫ���Ϊnull���ַ�������


        //>> begin DMA-604 : ������Ҫ�����п�ȵ���,20130411
        strcpy(line,pWrapper->getOption("files:adjustcolumnswidth",""));
        if(strlen(line)<1) {
            for(int _i = 0; _i<colct; _i++) { // û����Ҫ�����п�ȵ���
                colsAdjustWidth[_i] = false;
            }
        } else {
            sl=strlen(line);
            if(line[sl-1]=='\n') line[sl-1]=0;


            // 1. �����ַ������ж���Ҫ�������
            if(line[0]=='*') {
                lgprintf("���е��ַ�������,����Ҫ�����п��,[files:adjustcolumnswidth=*]");
                for(int _i = 0 ; _i<colct; _i++) { // ֻ���ַ���������Ҫ����
                    if( COLUMN_TYPE_CHAR == mt.getColType(colspos[_i])) {
                        colsAdjustWidth[_i] = true;
                    } else {
                        colsAdjustWidth[_i] = false;
                    }
                }

            } else {// 2.  ��������Ҫ�������,fix dma-1325

                size_t  separater_pos = 0;
                std::string columnsNameInfo = std::string(line);
                // ���һ��","����β�������㴦�����һ��
                columnsNameInfo += ", ";
                const char separater_items=',';
                std::string columnItem;
                separater_pos = columnsNameInfo.find_first_of(separater_items);
                int colindex = 0;
                while (separater_pos != std::string::npos) {
                    columnItem = columnsNameInfo.substr(0,separater_pos);
                    columnsNameInfo = columnsNameInfo.substr(separater_pos+1,columnsNameInfo.size()-separater_pos-1);
                    separater_pos = columnsNameInfo.find_first_of(separater_items);

                    // �ж���������Ƿ���ڣ���������ھ���ʾ����
                    bool _exist = false;
                    for(int _i = 0; _i<colct; _i++) {
                        char _cn[256];
                        wociGetColumnName(memtab,colspos[_i],_cn);
                        if(strcasecmp(_cn,columnItem.c_str()) == 0) {
                            _exist = true;

                            // �ж��������Ƿ���ȷ����������Ͳ���ȷ���˳�
                            if(COLUMN_TYPE_CHAR != mt.getColType(colspos[_i])) {
                                ThrowWith("����[%s]�ֶ����Ͳ����ַ�����,��֧���Զ������п��.",columnItem.c_str());
                            }
                            colsAdjustWidth[_i] = true;
                            break;
                        } else {
                            colsAdjustWidth[_i] = false;
                        }
                    }

                    // û���ҵ�����
                    if(!_exist) {
                        ThrowWith("����[%s]�ֶβ�����Ч�ֶΣ���֧���Զ������п��.",columnItem.c_str());
                    }
                }
            }
        }
        //<< end: DMA-604 ������Ҫ�����п�ȵ���

        //>> begin: dma-1086 �����ؼ��ֺ�������
        {
            char _http_col[256];
            char _col_key[256];
            char _col_download[256];
            strcpy(_http_col,pWrapper->getOption("http_translate:http_col",""));
            if(strlen(_http_col)>1) {
                strcpy(_col_key,pWrapper->getOption("http_translate:key_col",""));
                strcpy(_col_download,pWrapper->getOption("http_translate:download_col",""));
                if(strcasecmp(_col_key,_col_download) == 0) {
                    ThrowWith("http_translate:key_col �� http_translate:download_col ����ָ��Ϊͬһ��.");
                }

                // ������Ч����
                int _col_key_len = strlen(_col_key);
                int _col_download_len = strlen(_col_download);

                _st_http_translate_obj.init();
                char _cn[256];

                if( _col_key_len > 1 || _col_download_len > 1) {
                    for(int _i = 0; _i<colct; _i++) {
                        if(mt.getColType(colspos[_i]) != COLUMN_TYPE_CHAR) continue;

                        wociGetColumnName(memtab,colspos[_i],_cn);

                        if(strcasecmp(_cn,_http_col) == 0) {
                            _st_http_translate_obj._http_col = colspos[_i];
                        } else if(_col_key_len>1 && strcasecmp(_cn,_col_key) == 0) {
                            _st_http_translate_obj._key_col = colspos[_i];
                        } else if(_col_download_len>1 && strcasecmp(_cn,_col_download) == 0) {
                            _st_http_translate_obj._download_col= colspos[_i];
                        }
                    }

                    if(_st_http_translate_obj._http_col == -1) {
                        ThrowWith("Http�в�����,��ȷ��http_translate:http_col�Ƿ���д��ȷ.");
                    }

                    if(_st_http_translate_obj._key_col == -1) {
                        ThrowWith("�ؼ����в�����,��ȷ��http_translate:key_col�Ƿ���д��ȷ.");
                    }

                    if(_st_http_translate_obj._download_col == -1) {
                        ThrowWith("�����в�����,��ȷ��http_translate:download_col�Ƿ���д��ȷ.");
                    }

                    // http �б�������عؼ����к�������������һ��,����http�б�������ǰ��
                    if(_col_key_len > 1) {
                        if(_st_http_translate_obj._http_col >=_st_http_translate_obj._key_col ||
                           (_st_http_translate_obj._key_col-_st_http_translate_obj._http_col) > _st_http_translate_obj.get_column_count()) {
                            ThrowWith("http �б�������عؼ����к�������������һ��,����http�б�������ǰ��");
                        }
                    }

                    if(_col_download_len>1) {
                        if(_st_http_translate_obj._http_col >=_st_http_translate_obj._download_col ||
                           (_st_http_translate_obj._download_col - _st_http_translate_obj._http_col) > _st_http_translate_obj.get_column_count()) {
                            ThrowWith("http �б�������عؼ����к�������������һ��,����http�б�������ǰ��");
                        }
                    }

                    // �������������չ�ļ�����
                    char _extern_name[2048];
                    strcpy(_extern_name,pWrapper->getOption("http_translate:download_pattern",""));
                    if(strlen(_extern_name)<1) {
                        ThrowWith("http �������ļ���չ��http_translate:download_pattern����Ϊ��.");
                    }
                    _column_parser_obj.set_download_extern_name(_extern_name);
                }
            }
        }
        //<< end: dma-1086 �����ؼ��ֺ�������

        //>> begin: fix dma-794(���ʱ��),dma-941(����ļ�����)
        {
            char _loaddatecol[256];
            char _loadfilecol[256];
            strcpy(_loaddatecol,pWrapper->getOption("external:loaddatecol",""));
            strcpy(_loadfilecol,pWrapper->getOption("external:loadfilecol",""));
            int _loaddatecol_len = strlen(_loaddatecol);
            int _loadfilecol_len = strlen(_loadfilecol);

            _st_external_obj.init();
            if(_loaddatecol_len>1 || _loadfilecol_len>1) {
                char _cn[256];
                for(int i=0; i<colct; i++) {
                    wociGetColumnName(mt,colspos[i],_cn);
                    if(mt.getColType(colspos[i]) == COLUMN_TYPE_DATE) {
                        if(strcasecmp(_cn,_loaddatecol) == 0) {
                            _st_external_obj._load_date_col_index = colspos[i]; // �ҵ���Ҫ����װ��ʱ���е�����
                        }
                    }

                    if(mt.getColType(colspos[i]) == COLUMN_TYPE_CHAR) {
                        if(strcasecmp(_cn,_loadfilecol) == 0) {
                            _st_external_obj._load_filename_col_index = colspos[i];// �ҵ���Ҫ����װ��ʱ���е�����
                        }
                    }
                }
            }
        }
        //<< end: fix dma-794(���ʱ��),dma-941(����ļ�����)

        filerows=0;
        errinfile=0;
        ignore_rows = 0;

        lgprintf("�ļ�'%s'��ʼ����.",filename.c_str());
    }

    /////////////////////////////////////chxl//////////////////
    int *pnszVarCol = new int[MAX_FILE_COLUMNS];
    int *pnszTypeLen = new int[MAX_FILE_COLUMNS];
    memset(pnszVarCol,0,sizeof(int)*MAX_FILE_COLUMNS);
    memset(pnszTypeLen,0,sizeof(int)*MAX_FILE_COLUMNS);

    int nWrNum = 1000000000;

    _wdbiGetVarCol(mt.handle, pnszVarCol, pnszTypeLen);
    char **pWrBf = new char* [MAX_FILE_COLUMNS];
    // get lowest rownum in var colums to fit in 10M buffer
    for(int t = 0; t<colct; t++) {
        if(pnszVarCol[colspos[t]]) {
            int nRow = (TEN_M_SIZE/pnszTypeLen[colspos[t]])-1;
            nWrNum = nWrNum > nRow?nRow:nWrNum;
        }
    }
    // allocate 10M buffer for each var column
    for(int t=0; t<colct; t++) {
        if(pnszVarCol[colspos[t]]) {
            pWrBf[colspos[t]] = new char[nWrNum * pnszTypeLen[colspos[t]]];
            memset(pWrBf[colspos[t]],0,nWrNum*pnszTypeLen[colspos[t]]);
        } else {
            pWrBf[colspos[t]] = NULL;
        }
    }
    //////////////////////////////////chxl//////////////////////
    int rows=mt.GetRows();
    int maxrows=mt.GetMaxRows();
    char colname[300];
    char ipaddr[20];

    char separator[8] = {0};
    char separator_tmp[8] = {0};
    int separator_len = 0;
    char enclosedby = pWrapper->getOption("files:enclosedby",'\x0');
    strcpy(separator_tmp, pWrapper->getOption("files:separator",""));

    //>> begin: fix dma-1191:֧�ֶ����Ʒָ���
    if(strncasecmp(separator_tmp,"0x",2)== 0) {
        separator[0] = atoi(separator_tmp+2);
        separator[1] = 0;
    } else {
        strcpy(separator,separator_tmp);
    }
    //<< end : fix dma-1191

    separator_len = strlen(separator);
    bool ischaracter = separator_len == 1;


    char escape=pWrapper->getOption("files:escape",'\x0');
    bool fixedmode=pWrapper->getOption("files:fixedmode",0)==1;
    int isfixed[MAX_COLUMN],fixlen[MAX_COLUMN];
    int fixcols=parserfixlen(memtab,pWrapper->getOption("files:fixlengthcols",""),isfixed,fixlen);
    int asciicvt[MAX_COLUMN];
    bool ipcheck=pWrapper->getOption("files:ipconvert",0)==1;
    int asccvt=parsercolumn(memtab,pWrapper->getOption("files:asciivalues",""),asciicvt);
    if(fixedmode) ThrowWith("�����ļ���ʽ�ݲ�֧��!");
    int valuelen[MAX_COLUMN];
    int recordfmtcheck=pWrapper->getOption("files:recordfmtcheck",1);
    int postoperculum = pWrapper->getOption("files:postoperculum",0)==1; //0��ʾĩβ�����ָ���
    bool ignoreLastColAnalyse = pWrapper->getOption("files:ignorelastcolanalyse",0) == 1;// �������һ��ת���ַ��ͷָ����ķ�����JIRA DMA-474
    bool addsecond = pWrapper->getOption("files:addsecond",0)==1;//[������������]��ʱ���ֶ��е�����в�0����,�������ɵ�ʱ��û���룬���磺{"2014-05-12 12:30","2014-05-12 13:04"}
    int appendlstcol = pWrapper->getOption("files:appendlstcol",0); // �������ݲ�ȫ��ʱ��,��Ҫ������n�е�����

    // ����������һ���ļ��д��ڶ��������ݣ���Ҫ�������������ݽ���д������ļ������ǲ���Ҫ�����:$DATAMERGER_HOME/log/dump/*.log��
    bool skip_printf_log = false;
    char *_p_env = getenv(HLG_SKIP_PRINTF_LOG);
    if(_p_env != NULL && 1 == atoi(_p_env)) {
        skip_printf_log = true;
    }else{
        char *_p_env2 = getenv("SKIP_PRINTF_LOG");
        if(_p_env2 != NULL && 1 == atoi(_p_env2)) {
            skip_printf_log = true;
        }        
    }

    //>> begin: fix dma-794
    char now[10];
    wociGetCurDateTime(now);
    //<< end: fix dma-794

    ////////////////////
    long nVarRow = 0;
    int nCol = 0;
    long nVarLevelRow = rows;;

    /////////////////
    // while(rows<maxrows && fgets(line,MAXLINELEN,fp)) {  // fix dma-2122,add 
    // by liujs
     int read_len = 0;
     while(rows<maxrows && my_fgets(line,MAXLINELEN,fp,read_len)) {  
        curoffset++; //�к�
        int sl=strlen(line); //��¼����
        //begin: fix dma-2122,add by liujs
        if(sl < read_len){
            // ȥ���м��0
            my_remove_zore(line,read_len);  // ���¹���������
        }    
        //end: fix dma-2122,add by liujs
        strcpy(linebak,line);

        //truncate \r\n seq:
        if(line[sl-1]=='\n' || line[sl-1]=='\r') line[--sl]=0;
        if(line[sl-1]=='\n' || line[sl-1]=='\r') line[--sl]=0;

        // >> begin : fix dma-1316
        for(int i=0; i<appendlstcol; i++) {
            strcat(line,separator);
        }
        sl = strlen(line);
        // >> end : fix dma-1316

        if(postoperculum && 0 == strncmp(line + (sl - separator_len), separator, separator_len)) {
            line[sl - separator_len]=0;
            sl -= separator_len;
        }

        if(line[0]==0) continue; //�������
        char *ptrs[MAX_FILE_COLUMNS];
        char *ps=line;   //ͷָ��
        char *psle=line + sl -1;  //βָ��
        memset(valuelen,0,colct*sizeof(int));
        ptrs[0]=line;
        bool enclose=false;
        int ct=0;  //ʵ���ֶ���
        //initial first column
        ptrs[ct]=ps;
        bool isthisfix=isfixed[colspos[ct]];
        bool skipline=false;
        //�����ֶβ���ת������
        while(*ps) {
            if(ct==colct) {
                // fix dma-1446:�Զ������ı��ļ��������,delete by liujs
                // ct++;break;
                ct--;
                break;       // ֱ����������
            }

            isthisfix=isfixed[colspos[ct]];
            if(isthisfix) {
                //������ʽ
                char *pse=ps+fixlen[colspos[ct]]-1;
                if(pse>psle) {
                    if(recordfmtcheck!=0) {
                        ThrowWith("�ļ�'%s'��%d�и�ʽ����,�����ֶ�(%d)�������еĳ���%d.",filename.c_str(),
                                  curoffset,ct+1,pse-psle);
                    }

                    if(!skip_printf_log) {
                        lgprintf("�ļ�'%s'��%d�и�ʽ����,�����ֶ�(%d)�������еĳ���%d.",filename.c_str(),
                                 curoffset,ct+1,pse-psle);
                    }
                    skipline=true;
                    errinfile++;
                    WriteErrorData(linebak);
                    break;
                }

                valuelen[colspos[ct]]=pse-ps+1;
                //�����ֶβ�֧�ַ�װ�ַ����ţ���
                ps+=fixlen[colspos[ct]];

                //DMA-152
                if(!ischaracter) {
                    /* string */
                    /*trim left separator*/
                    if(0 == strncmp(ps, separator, separator_len)) {
                        ps += separator_len;
                    }
                } else {
                    /* charactor */
                    if(*ps==separator[0] && separator[0]!=0) {
                        ps++;
                    }
                }
                ptrs[++ct]=ps;
            } else { //���������ת�壿
                //�䳤��ʽ
                if(*ps==enclosedby && enclosedby!=0) { // ��ǰ�ַ��Ƿ�װ��
                    ps++;
                    if(/*enclose && */ps[-2]==escape &&escape!=0) {
                        strcpy(ps-2,ps-1);
                        continue;
                    }
                    if(enclose) {
                        ps[-1]=0;
                        enclose=false;
                        continue;
                    }
                    ps[-1]=0;
                    ptrs[ct]=ps; //delete enclosed char and
                    enclose=true;
                }

                if(!ischaracter) { // ���ַ��ָ���
                    // �ָ������ַ��������
                    /* trim left separator */
                    if(0 == strncmp(ps, separator, separator_len)) {
                        ps += separator_len;
                        if(ps[-separator_len] == escape && escape!=0) {
                            strcpy(ps - separator_len, ps);
                            continue;
                        }
                        memset(ps-separator_len, 0x0, separator_len);
                        ptrs[++ct]=ps;
                    } else ps++;
                } // end if(!ischaracter)
                else {
                    // ���ַ��ָ���
                    if(*ps!=enclosedby) { // fix dma-1160
                        if(*ps++==separator[0] && !enclose) {
                            if(ps[-2]==escape && escape!=0) {
                                strcpy(ps-2,ps-1);
                                continue;
                            }
                            ps[-1]=0;
                            ptrs[++ct]=ps;
                        }
                    }
                }   // end if(ischaracter)
            }// end else

            //>> Begin: fix jira dma-474
            if(ignoreLastColAnalyse && (ct == colct-1)) {
                break;
            }
            //<< End: fix jira dma-474
        }// end while(*ps)

        if(skipline) {
            continue;
        }

        if(!isfixed[colspos[colct-1]]) ct++;
        //POSITION;PRINTF("ct:%d; colct:%d\n", ct, colct);

        std::string msg;
        bool bcheck = check_column_count(ct,colct,_st_external_obj,_st_http_translate_obj,msg);
        if(!bcheck) {
            if(recordfmtcheck!=0) {
                ThrowWith(msg.c_str());
            }

            if(!skip_printf_log) {
                lgprintf(msg.c_str());
            }

            errinfile++;
            WriteErrorData(linebak);
            continue;
        }

        //>> begin: fix dma-941
        // û��load_time��load_filename��Ҫ�����ʱ�򣬲���Ҫ������
        // ������load_time��load_filename��Ҫ�����ʱ�򣬾�Ҫ������˳��
        // ��Ϊ��ʽ�����е�����Ŀ��ʵ��ֵ������Ŀ��һ����ʵ��ֵ������Ŀ��Ҫ����!
        int fix_col_ptr_index = 0; // ʵ��ֵ������
        //<< end: fix dma-941

        _int64 _int_tmp = 0;
        for(int i=0; i<colct; i++) {
            nCol = colspos[i];

            //>> begin: dma-794
            if(colspos[i] == _st_external_obj._load_date_col_index) {
                if(mt.getColType(colspos[i]) != COLUMN_TYPE_DATE) {
                    ThrowWith("Set load date column ,Invalid column type :%d,id:%d",mt.getColType(colspos[i]),colspos[i]);
                    break;
                }
                mt.SetDate(colspos[i],rows,now);
                continue;
            }
            //<< end:dma-794


            //>> begin: dma-941
            if(colspos[i] == _st_external_obj._load_filename_col_index) {
                if(mt.getColType(colspos[i]) != COLUMN_TYPE_CHAR) {
                    ThrowWith("Set load filename column ,Invalid column type :%d,id:%d",mt.getColType(colspos[i]),colspos[i]);
                    break;
                }
                // ����ط���������SetStr
                // mt.SetStr(colspos[i],rows,fix_filename);
                if(pnszVarCol[nCol]) {
                    memcpy(pWrBf[nCol]+nVarRow*pnszTypeLen[nCol],fix_filename,strlen(fix_filename));
                } else { // fixed len type
                    strcpy(mt.PtrStrNoVar(colspos[i],rows),fix_filename);
                }

                continue;
            }
            //<< end: dma-941

            char *ptrf=ptrs[fix_col_ptr_index];  // ��ȡ�ļ���ʱ�������е�ֵ
            /* // �����ֶ���ʱ�����ε���
            if(isfixed[colspos[i]]) {
                //�����ֶ�����û�зָ���������ֶε�ֵ�п���δ�ضϣ���Ҫ��������
                memcpy(fieldval,ptrs[fix_col_ptr_index],valuelen[colspos[i]]);
                fieldval[valuelen[colspos[i]]]=0;
                ptrf=fieldval;
            }
            */
            char *trimp=ptrf;
            int olen=strlen(trimp)-1;
            while(trimp[olen]==' ') trimp[olen--]=0;
            while(*trimp==' ') trimp++;

            //>> begin: dma-1086 , http �н����ؼ��ֺ���������
            // http �б�������عؼ����к�������������һ��,����http�б�������ǰ��
            if(_st_http_translate_obj.is_valid() && colspos[i] == _st_http_translate_obj._http_col) {
                if(mt.getColType(colspos[i]) != COLUMN_TYPE_CHAR) {
                    ThrowWith("parser http column ,Invalid column type :%d,id:%d",mt.getColType(colspos[i]),colspos[i]);
                    break;
                }

                if(rows > 1) { // У��http�ؼ����к��������Ƿ������ȷ
                    if(_st_http_translate_obj._update_check != 0) {
                        ThrowWith("parser http column error,����http������,��Ҫ����[http�ؼ��֡����������� ��%d��,ֻ������%d��].",_st_http_translate_obj.get_column_count(),_st_http_translate_obj.get_column_count() - _st_http_translate_obj._update_check);
                        break;
                    }
                }

                _st_http_translate_obj._update_check = _st_http_translate_obj.get_column_count();

                if(_st_http_translate_obj._key_col != -1) { // ����http�л�ȡ�ؼ���,�洢��_st_http_translate_obj._key_col_value��
                    if(*trimp == 0) { // http ��Ϊ�յ����
                        _st_http_translate_obj._key_col_value[0] = 0;
                    } else {
                        std::string _keyword;
                        _column_parser_obj.get_keyword_from_http(trimp,_keyword);
                        if(_keyword.size()>HTTP_DOWNLOAD_LEN/2) {
                            lgprintf("��ȡ�õ�����Ĺؼ���:[%s]����[%d].",_keyword.c_str(),_keyword.size());
                        }
                        int valid_len = min(_keyword.size(),HTTP_DOWNLOAD_LEN/2);
                        strncpy(_st_http_translate_obj._key_col_value,_keyword.c_str(),valid_len);
                        _st_http_translate_obj._key_col_value[valid_len]=0;
                    }
                }

                if(_st_http_translate_obj._download_col != -1) { // ����http�л�ȡ����������,�洢��_st_http_translate_obj._download_col_value��
                    if(*trimp == 0) { // http ��Ϊ�յ����,�������������е�ֵΪ��
                        _st_http_translate_obj._download_col_value[0]=0;
                    } else {
                        std::string _download;
                        _column_parser_obj.get_download_from_http(trimp,_download);
                        if(_download.size()>HTTP_DOWNLOAD_LEN/2) {
                            lgprintf("��ȡ�õ��������������:[%s]����[%d].",_download.c_str(),_download.size());
                        }
                        int valid_len = min(_download.size(),HTTP_DOWNLOAD_LEN/2);
                        strncpy(_st_http_translate_obj._download_col_value,_download.c_str(),valid_len);
                        _st_http_translate_obj._download_col_value[valid_len]=0;
                    }
                }

            } else if(_st_http_translate_obj.is_valid() && colspos[i] == _st_http_translate_obj._key_col) {
                // ���ùؼ��ֵ�ֵ
                if(pnszVarCol[nCol]) {
                    memcpy(pWrBf[nCol]+nVarRow*pnszTypeLen[nCol],_st_http_translate_obj._key_col_value,strlen(_st_http_translate_obj._key_col_value));
                } else { // fixed len type
                    strcpy(mt.PtrStrNoVar(colspos[i],rows),_st_http_translate_obj._key_col_value);
                }
                _st_http_translate_obj._update_check--;

                continue;
            } else if(_st_http_translate_obj.is_valid() && colspos[i] == _st_http_translate_obj._download_col) {
                // �����������ֵ
                if(pnszVarCol[nCol]) {
                    memcpy(pWrBf[nCol]+nVarRow*pnszTypeLen[nCol],_st_http_translate_obj._download_col_value,strlen(_st_http_translate_obj._download_col_value));
                } else { // fixed len type
                    strcpy(mt.PtrStrNoVar(colspos[i],rows),_st_http_translate_obj._download_col_value);
                }
                _st_http_translate_obj._update_check--;

                continue;
            }
            //>> end: dma-1086

            if(*trimp==0) {
                wociSetNull(mt,colspos[i],rows,true);
                //mt.SetStr(colspos[i],rows,"");
                //continue;
            } else switch(mt.getColType(colspos[i])) {
                    case COLUMN_TYPE_CHAR   :
                        if(ipcheck) {
                            wociGetColumnName(mt,colspos[i],colname);
                            if(strcasecmp(colname+strlen(colname)-3,"_IP")==0 && strchr(ptrf,'.')==NULL) {
                                int ip=atoi(ptrs[fix_col_ptr_index]);
                                unsigned char *bip=(unsigned char*)&ip;
                                //TODO : this only work on little-endian system
                                sprintf(ipaddr,"%u.%u.%u.%u",bip[3],bip[2],bip[1],bip[0]);
                                if(ip==0) ipaddr[0]=0;
                                //strcpy(mt.PtrStr(colspos[i],rows),ipaddr);
                                if(pnszVarCol[nCol]) {
                                    memcpy(pWrBf[nCol]+nVarRow*pnszTypeLen[nCol],ipaddr,strlen(ipaddr));
                                } else {
                                    strcpy(mt.PtrStrNoVar(nCol,rows),ipaddr);
                                }
                            }
                        } else {
                            //ascii��ת��
                            if(asciicvt[colspos[i]]) {
                                if(strlen(trimp)>3) {
                                    if(recordfmtcheck!=0)
                                        ThrowWith("ASCII ת��ֻ֧��һλ���ļ�'%s'��%d�е�%d�ֶε�ֵ'%s'�޷�ת��",
                                                  filename.c_str(),curoffset,i+1,trimp);
                                    lgprintf  ("ASCII ת��ֻ֧��һλ���ļ�'%s'��%d�е�%d�ֶε�ֵ'%s'�޷�ת��",
                                               filename.c_str(),curoffset,i+1,trimp);
                                    errinfile++;
                                    WriteErrorData(linebak);
                                    skipline=true;
                                }
                                char chr[2];
                                chr[1]=0;
                                chr[0]=atoi(trimp);
                                if(pnszVarCol[nCol]) {
                                    memcpy(pWrBf[nCol]+nVarRow*pnszTypeLen[nCol],chr,2);
                                } else {
                                    strcpy(mt.PtrStrNoVar(colspos[i],rows),chr);
                                }
                            } else {
                                int _trimpLen = strlen(trimp);
                                if(colsAdjustWidth[i]) { // ���ȳ���Ŀ�����п�ȣ�����Ҫ�ضϴ����
                                    if( _trimpLen > mt.getColLen(colspos[i])-1) {
                                        // strncpy(mt.PtrStr(colspos[i],rows),trimp,(mt.getColLen(colspos[i])-1));
                                        //  mt.PtrStr(colspos[i],rows)[mt.getColLen(colspos[i])-1] = 0;
                                        if(pnszVarCol[nCol]) {
                                            memcpy(pWrBf[nCol]+nVarRow*pnszTypeLen[nCol],trimp,(mt.getColLen(colspos[i])-1));
                                        } else {
                                            strncpy(mt.PtrStrNoVar(colspos[i],rows),trimp,(mt.getColLen(colspos[i])-1));
                                            mt.PtrStrNoVar(colspos[i],rows)[mt.getColLen(colspos[i])-1] = 0;
                                        }

                                        if(!skip_printf_log) {
                                            lgprintf("�ļ�'%s'��%d�е�%d�ֶγ�������ĳ���(%d,%d),�ѽ�ȡ��Ч����(%d).",filename.c_str(),
                                                     curoffset,i+1,strlen(ptrs[fix_col_ptr_index]),mt.getColLen(colspos[i])-1,mt.getColLen(colspos[i])-1);
                                        }

                                        ignore_rows++;
                                    } else {
                                        if(pnszVarCol[nCol]) {
                                            memcpy(pWrBf[nCol]+nVarRow*pnszTypeLen[nCol],trimp,strlen(trimp));
                                        } else {
                                            strcpy(mt.PtrStrNoVar(colspos[i],rows),trimp);
                                        }
                                    }
                                } else { // ����Ҫ�����п�ȣ��ҳ��ȳ����п�ȵ����
                                    //BUG? char(1),getColLen got 2
                                    if( _trimpLen >mt.getColLen(colspos[i])-1) {
                                        if(recordfmtcheck!=0)
                                            ThrowWith("�ļ�'%s'��%d�е�%d�ֶγ�������ĳ���(%d,%d)��",filename.c_str(),
                                                      curoffset,i+1,strlen(ptrs[fix_col_ptr_index]),mt.getColLen(colspos[i])-1);

                                        if(!skip_printf_log) {
                                            lgprintf("�ļ�'%s'��%d�е�%d�ֶγ�������ĳ���(%d,%d)��",filename.c_str(),
                                                     curoffset,i+1,strlen(ptrs[fix_col_ptr_index]),mt.getColLen(colspos[i])-1);
                                        }

                                        errinfile++;
                                        WriteErrorData(linebak);

                                        skipline=true;
                                    } else {
                                        if(pnszVarCol[nCol]) {
                                            memcpy(pWrBf[nCol]+nVarRow*pnszTypeLen[nCol],trimp,strlen(trimp));
                                        } else {
                                            strcpy(mt.PtrStrNoVar(colspos[i],rows),trimp);
                                        }
                                    }
                                }
                            }
                        }
                        break;
                    case COLUMN_TYPE_FLOAT  :
                        *mt.PtrDouble(colspos[i],rows)=*trimp==0?0:atof(trimp);
                        break;
                    case COLUMN_TYPE_INT    :
                        _int_tmp = ((*trimp==0)?0:atol(trimp));
                        if(_int_tmp >= PLUS_INF_64 || _int_tmp <= MINUS_INF_64) { // fix dma-1164
                            if(recordfmtcheck!=0) {
                                ThrowWith("�ļ�'%s'��%d�е�%d�ֶδ������ֵ(%s)��",filename.c_str(),curoffset,i+1,trimp);
                            } else {
                                lgprintf("�ļ�'%s'��%d�е�%d�ֶδ������ֵ(%s)��",filename.c_str(),curoffset,i+1,trimp);
                                errinfile++;
                                WriteErrorData(linebak);
                                skipline=true;
                            }
                        }
                        *mt.PtrInt(colspos[i],rows)=_int_tmp;
                        break;
                    case COLUMN_TYPE_BIGINT :
                        _int_tmp = ((*trimp==0)?0:atol(trimp));
                        if(_int_tmp >= PLUS_INF_64 || _int_tmp <= MINUS_INF_64) { // fix dma-1164
                            if(recordfmtcheck!=0) {
                                ThrowWith("�ļ�'%s'��%d�е�%d�ֶδ������ֵ(%s)��",filename.c_str(),curoffset,i+1,trimp);
                            } else {
                                lgprintf("�ļ�'%s'��%d�е�%d�ֶδ������ֵ(%s)��",filename.c_str(),curoffset,i+1,trimp);
                                errinfile++;
                                WriteErrorData(linebak);
                                skipline=true;
                            }
                        }
                        *mt.PtrLong(colspos[i],rows)=_int_tmp;
                        break;
                    case COLUMN_TYPE_DATE   : {
                        if(*trimp==0) {
                            wociSetDateTime(mt.PtrDate(colspos[i],rows),1900,1,1,0,0,0);
                            break;
                        }
                        int of=0,y=0,m=0,d=0,hh=0,mm=0,ss=-1;
                        char *dptr=trimp;
                        while(*dptr==' ') dptr++;
                        if(*dptr<'0' || *dptr>'9') {
                            if(recordfmtcheck!=0)
                                ThrowWith("�ļ�'%s'��%d�е�%d�ֶδ��������(%s)��",filename.c_str(),
                                          curoffset,i+1,dptr);
                            lgprintf("�ļ�'%s'��%d�е�%d�ֶδ��������(%s)��",filename.c_str(),
                                     curoffset,i+1,dptr);
                            errinfile++;
                            WriteErrorData(linebak);
                            skipline=true;
                            break;
                        }
                        if(dptr[4]>='0' && dptr[4]<='9') {
                            //û�зָ���,���磺20121112121212
                            char r=dptr[4];
                            dptr[4]=0;
                            y=atoi(dptr);
                            dptr+=4;
                            *dptr=r;
                            r=dptr[2];
                            dptr[2]=0;
                            m=atoi(dptr);
                            dptr+=2;
                            *dptr=r;
                            r=dptr[2];
                            dptr[2]=0;
                            d=atoi(dptr);
                            dptr+=2;
                            *dptr=r;
                            r=dptr[2];
                            dptr[2]=0;
                            hh=atoi(dptr);
                            dptr+=2;
                            *dptr=r;
                            r=dptr[2];
                            dptr[2]=0;
                            mm=atoi(dptr);
                            dptr+=2;
                            *dptr=r;
                            r=dptr[2];
                            dptr[2]=0;
                            if(addsecond && strlen(dptr) == 12) { //201211121212
                                ss = 0 ;
                            } else {
                                ss=atoi(dptr);
                                dptr+=2;
                            }
                        } else {
                            // ���ڷָ��������磺2012-11-12 12:12:12,2012/12/12 12/12/12
                            y=atoi(dptr+of);
                            of+=5;
                            m=atoi(dptr+of);
                            of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
                            d=atoi(dptr+of);
                            of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
                            hh=atoi(dptr+of);
                            of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
                            mm=atoi(dptr+of);
                            of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
                            if(addsecond && strlen(dptr)==16) { // 2012-11-12 12:12
                                ss = 0;
                            } else {
                                ss=atoi(dptr+of);
                            }
                        }
                        if(y<1900 || m<1 || m>12 || d<1 || d>31 ||hh<0 || hh>60 || mm<0 || mm>60 ||ss<0 || ss>60) {
                            //>> begin: fix dma-895
                            if(recordfmtcheck!=0) {
                                ThrowWith("�ļ�'%s'��%d�е�%d�ֶδ��������(%s)��",filename.c_str(),curoffset,i+1,dptr);
                            } else {
                                lgprintf("�ļ�'%s'��%d�е�%d�ֶδ��������(%s)��",filename.c_str(),curoffset,i+1,dptr);
                                errinfile++;
                                WriteErrorData(linebak);
                                skipline=true;
                            }
                            //<< end: fix dma-895
                            break;
                        }
                        wociSetDateTime(mt.PtrDate(colspos[i],rows), y,m,d,hh,mm,ss);
                    }
                    break;
                    case COLUMN_TYPE_NUM    :
                    default :
                        ThrowWith("Invalid column type :%d,id:%d",mt.getColType(colspos[i]),colspos[i]);
                        break;

                } // end switch(mt.getColType(colspos[i])) {

            if(skipline) {
                _st_http_translate_obj._update_check = 0;
                break;
            }

            fix_col_ptr_index ++;
        }  //end for(int i=0;i<colct;i++)

        if(!skipline) {
            for(int _i = 0; _i<nulls_colct; _i++) {
                mt.SetStr(nulls_colspos[_i],rows,(char*)"");
            }
            filerows++;
            rows++;
            nVarRow++;

            if(nVarRow == nWrNum) {
                for(int i=0; i<colct; i++) {
                    if(pnszVarCol[colspos[i]]) {
                        mt.SetStr(colspos[i], nVarLevelRow, pWrBf[colspos[i]],nVarRow);
                        memset(pWrBf[colspos[i]],0,nWrNum*pnszTypeLen[colspos[i]]*sizeof(char));
                    }
                }
                nVarLevelRow = rows;
                nVarRow = 0;
                _wdbiSetMTRows(mt,rows);
            }
        }
    }// end while (Get Next Line)

    for(int i=0; i<colct; i++) {
        if(pnszVarCol[colspos[i]]) {
            mt.SetStr(colspos[i], nVarLevelRow, pWrBf[colspos[i]],nVarRow);
            delete[] pWrBf[colspos[i]];
        }
    }
    _wdbiSetMTRows(mt,rows);

    delete[] pnszTypeLen;
    delete[] pnszVarCol;
    delete[] pWrBf;

    if(feof(fp)!=0) {
        AutoStmt st(dbc);
        /***************�ļ����������¼*****************/
        //��ȡ�������ݴ洢λ��

        char errfilename[300];
        strcpy(errfilename,pWrapper->getOption("files:errordatapath","/tmp"));

        if(errfilename[0]!=0) {
            if(errfilename[strlen(errfilename)-1]!='/') strcat(errfilename,"/");
            strcat(errfilename,basefilename.c_str());
            strcat(errfilename,".err");
        } else {
            strcpy(errfilename,"NULL");
        }

        // �����ļ�Ϊ��ʱ����״̬
        st.Prepare("update dp.dp_filelog set status=1,recordnum=%d,endproctime=now(),errrows=%d,errfname='%s',ignorerows=%d where tabid=%d and datapartid=%d and fileseq=%d",filerows,errinfile,errfilename,ignore_rows,tabid,datapartid,fileseq);
        st.Execute(1);
        st.Wait();

        lgprintf("�ļ�'%s'�����������%d�м�¼���ļ�%d�У�����%d�С�",filename.c_str(),filerows,curoffset,errinfile);
        fclose(fp);
        fp=NULL;
        if(errinfile>0) {
            sp.log(tabid,datapartid,106,"�ļ�'%s'�����������%d�м�¼���ļ�%d�У�����%d�С���־�ļ�: %s ",filename.c_str(),filerows,curoffset,errinfile,_wdbiGetLogFile());

            // �Դ��������ļ�����ѹ������,fix dma-1159
            CompressErrData();
        }

        if( fm.SrcFileIsZipped() ||
            (pWrapper->getOption("files:keeppickfile",0)!=1 && pWrapper->getOption("files:skippickup",0)!=1) ||
            fm.GetIsBackupDir()
          ){
            lgprintf("ɾ���ļ�[%s]",filename.c_str());
            remove(filename.c_str());
        }


        //>> Begin: delete src file ,after parsed src file,DM-216
        fm.DeleteSrcFile();
        //<< End:

        curoffset=0;
    } else if(rows==maxrows) {
        return 0; // memtable is full and datafile not eof
    }
    if(rows==maxrows) {
        return 2;// memtable is full and datafile is eof
    }
    return 1;
}
