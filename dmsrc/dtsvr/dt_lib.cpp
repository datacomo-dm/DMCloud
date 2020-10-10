#include "dt_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include "dt_lib.h"
#include "zlib.h"
#include "dtio.h"
#include <unistd.h>
#include <endian.h>
#include <string>
#include <vector>
#include <glob.h>
#include <fcntl.h>
#include <queue>
//>> Begin: JIRA:DM 198 , modify by liujs
#include "IDumpfile.h"
#include "DumpFileWrapper.h"
//<< End:modify by liujs
// DMA-624: improve threadlist
#include "ThreadList.h"
#include <cr_class/cr_externalsort.h>
#include <cfm_drv/dm_ib_sort_load.h>
#include <cfm_drv/dm_ib_tableinfo.h>

DllExport bool wdbi_kill_in_progress;
#ifndef WIN32
#include <sys/wait.h>
#endif
// 2005/08/27�޸ģ�partid��Ч��sub
// Ϊ����wdbi�����������(dtadmin��������),TestColumn�ڴ�ʵ�֡�����λ���ǵ�wdbi��ʵ��
bool TestColumn(int mt,const char *colname)
{
    int colct=wociGetColumnNumber(mt);
    char tmp[300];
    for(int i=0; i<colct; i++) {
        wociGetColumnName(mt,i,tmp);
        if(STRICMP(colname,tmp)==0) return true;
    }
    return false;
}

// �ж�����Ƿ���RIAK�汾
bool CheckRiakVersion()
{
    bool b_riak_version = false;
    char* p_riak_host = NULL;
    p_riak_host = getenv("RIAK_HOSTNAME");
    if(p_riak_host != NULL && p_riak_host[0] !=0 && strlen(p_riak_host)>1) {
        b_riak_version = true;
    }
    return b_riak_version;
}

// �ж�����Ƿ�֧�ֲ���������
bool CheckParallelLoad()
{
    bool dp_parallel_load = false;
    char *p = getenv("DP_PARALLEL_LOAD");
    if(p != NULL && strcmp(p,"1") == 0) {
        dp_parallel_load = true;
    } else {
        dp_parallel_load = false;
    }
    return dp_parallel_load;
}


int CopyMySQLTable(const char *path,const char *sdn,const char *stn,
                   const char *ddn,const char *dtn)
{
    char srcf[300],dstf[300];
    // check destination directory
    sprintf(dstf,"%s%s",path,ddn);
    xmkdir(dstf);
    sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".frm");
    sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".frm");
    mCopyFile(srcf,dstf);
    sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MYD");
    sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".MYD");
    mCopyFile(srcf,dstf);
    sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MYI");
    sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".MYI");
    mCopyFile(srcf,dstf);

    sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".DTP");
    FILE *fsrc=fopen(srcf,"rb");
    if(fsrc!=NULL) {
        fclose(fsrc) ;
        sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".DTP");
        mCopyFile(srcf,dstf);
    }
    sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MRG");
    fsrc=fopen(srcf,"rb");
    if(fsrc!=NULL) {
        fclose(fsrc) ;
        sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".MRG");
        mCopyFile(srcf,dstf);
    }
    return 1;
}

int MoveMySQLTable(const char *path,const char *sdn,const char *stn,
                   const char *ddn,const char *dtn)
{
    char srcf[300],dstf[300];
    // check destination directory
    sprintf(dstf,"%s%s",path,ddn);
    xmkdir(dstf);
    sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".frm");
    sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".frm");
    rename(srcf,dstf);
    sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MYD");
    sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".MYD");
    rename(srcf,dstf);
    sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MYI");
    sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".MYI");
    rename(srcf,dstf);
    sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".DTP");
    FILE *fsrc=fopen(srcf,"rb");
    if(fsrc!=NULL) {
        fclose(fsrc) ;
        sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".DTP");
        rename(srcf,dstf);
    }
    sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MRG");
    fsrc=fopen(srcf,"rb");
    if(fsrc!=NULL) {
        fclose(fsrc) ;
        sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".MRG");
        rename(srcf,dstf);
    }

    return 1;
}


/*
int CreateMtFromFile(int maxrows,char *filename)
{
FILE *fp=fopen(filename,"rb");
if(!fp)
ThrowWith("CreateMt from file '%s' which could not open.",filename);
int cdlen=0,cdnum=0;
fread(&cdlen,sizeof(int),1,fp);
fread(&cdnum,sizeof(int),1,fp);
revInt(&cdnum);
revInt(&cdlen);
if(cdlen==0 || cdnum==0)
ThrowWith("Could not read columns info from file 's' !",filename);
char *pbf=new char[cdlen];
if(fread(pbf,1,cdlen,fp)!=cdlen) {
delete [] pbf;
ThrowWith("Could not read columns info from file 's' !",filename);
}
int mt=wociCreateMemTable();
wociImport(mt,NULL,0,pbf,cdnum,cdlen,maxrows,0);
delete []pbf;
return mt;
}
*/



/**************************************************************************************************
Function    : RemoveContinueSpace(char * sqlText)
DateTime    : 2013/1/20 22:11
Description : ɾ���ַ����������ظ��Ŀո�create by liujs
param       : sqlText[input/output]
**************************************************************************************************/
void RemoveContinueSpace(char string[])
{
    // trim right
    int len=strlen(string)-1;
    for (int i=len; i>0; i--) {
        if (string[i] == ' ' || string[i] == '\r' || string[i] == '\n' || string[i] == '\t' )  string[i] = 0;
        else  break;
    }

    // trim left
#ifndef MAX_STMT_LEN
#define MAX_STMT_LEN 20001
#endif
    char _dumpsql[MAX_STMT_LEN];
    strcpy(_dumpsql,string);
    char* p=_dumpsql;
    while (*p == ' ' || *p == '\r' || *p == '\n' || *p == '\t') {
        p++;
    }
    strcpy(string,p);

    // trim middle
    len=strlen(string);
    for (int i = 0; i <=len; ++i) {
        if (string[i]==' ') {
            if (string[i+1]==' ') {
                for (int j = i+1; j<=len; j++)
                    string[j]=string[j+1];
                i--;
                continue;
            }
        } else
            continue;
    }
}


DataDump::DataDump(int dtdbc,int maxmem,int _blocksize):fnmt(dtdbc,MAX_MIDDLE_FILE_NUM)
{
    this->dtdbc=dtdbc;
    //Create fnmt and build column structrues.
    //fnmt.FetchAll("select * from dt_middledatafile where rownum<1");
    fnmt.FetchAll("select * from dp.dp_middledatafile limit 3");
    fnmt.Wait();
    fnmt.Reset();
    memlimit=maxmem*(long)1024*1024;
    maxblockrn=0;
    blocksize=_blocksize*1024;

    //----- �ֲ�ʽ������Ӳ���
    p_stDMIBSortLoader = NULL;
    p_ltDMIBSortLoader = NULL;
    b_master_node = false;
    slaver_index = 0;
    dp_parallel_load_columns = 0;
    st_parallel_load_rows =0;
    parallel_load_obj_id=0;
    lt_parallel_load_rows =0;
    tb_packindex_arr.clear();
    if(dp_parallel_load) {
        init_sorter_cluster();
        st_session_interval = 0;
        push_line_nums= 0;
        last_stsessionid[0]=0;
        add_stsessions_to_ltsession = false;
        sortsampleid[0]=0;
        threadmtrows =0;
    }
}

int DataDump::init_sorter_cluster()
{
    char *sorter_hostname=NULL;
    char *sorter_port_str=NULL;
    int fret=0;

    /*
    LOAD_SORTED_DATA_DIR: �������ݴ洢·��

    ��mysqld�ڵ�����dpadmin��ʱ��
    export LOAD_SORTED_DATA_DIR=nfs://192.168.1.1:/app/dma/var/preparedir

    ����(mysqld ����������dpadmin��ʱ��):
    export LOAD_SORTED_DATA_DIR=/app/dma/var/preparedir
    */
    std::string hostname;
    char* p=getenv("LOAD_SORTED_DATA_DIR"); // fix DMA-1456, by hwei
    if(p==NULL) {
        ThrowWith("dpadmin�ֲ�ʽ��������ʱ��,��ȡ������������[LOAD_SORTED_DATA_DIR]����������,�޷���Ⱥ�ύ������������ݵ�·��,�쳣�˳�.");
    }
    return 1;
}

int DataDump::BuildMiddleFilePath(int _fid)
{
    int fid=_fid;
    sprintf(tmpfn,"%smddt_%d.dat",dp.tmppath[0],fid);
    sprintf(tmpidxfn,"%smdidx_%d.dat",dp.tmppath[0],fid);
    while(true) {
        int freem=GetFreeM(tmpfn);
        if(freem<1024) {
            lgprintf("Available space on hard disk('%s') less then 1G : %dM,waitting 5 minutes for free...",tmpfn,freem);
            mSleep(300000);
        } else break;
    }
    return fid;
}

void DataDump::debug_time()
{
    lgprintf("[debug_time] : sort time :%11.6f ",paraller_sorttm.GetTime());
}



// ��ȡ��id������Ŀ
int ParallelLoad::get_table_info(const char* pbasepth,const char* pdbname,const char* ptbname,int& tabid,int& colnum)
{
    char pth[300];
    // ---> /data/dt2/app/dma/var/zkb/test_continu_dump.bht/Table.ctb
    sprintf(pth,"%s/%s/%s.bht/Table.ctb",pbasepth,pdbname,ptbname);

    FILE  *pFile  = fopen(pth,"rb");
    if(!pFile) {
        ThrowWith("�ļ�%s��ʧ��!\n",pth);
    }
    // get colno
    char buf[500] = {0};
    int fsize = fread(buf,1,33,pFile);
    if (fsize < 33 || strncmp(buf, "RScT", 4) != 0 || buf[4] != '2') {
        ThrowWith("Bad format of table definition in %s\n",pth);
    }
    colnum=*((int*)(buf+25));

    // get tabno
    fseek(pFile,-4,SEEK_END);
    int _tabid = 0;
    fread(&_tabid,4,1,pFile);
    fclose(pFile);

    tabid = _tabid;

    return 0;
}

// ÿһ�������洢�İ��ķֶ���Ϣ
typedef struct stru_part_section {
    int packstart;
    int packend;
    int lastobjs;
    stru_part_section()
    {
        packstart = packend = lastobjs=0;
    }
} stru_part_section,*stru_part_section_ptr;
// �����ṹ�洢��Ϣ
typedef struct stru_part {
    char partname[300];
    int  namelen;

    int firstpack;
    int lastpack;
    int lastpackobj;
    int lastsavepack;
    int lastsavepackobj;
    int savesessionid;

    long lastfileid;
    int savepos;
    int lastsavepos;

    // �����ڲ��Ĳ��ְ���λ��
    std::vector<stru_part_section>    part_section_list;

    // ap�汾�ķ������ļ��б�
    std::vector<long> part_file_list;

    // ����riak�е�pnode�б�
    std::vector<uint64_t> riak_pnode_list;

    stru_part()
    {
        partname[0] = 0;
        namelen = 0;
        lastfileid=0;
        savepos = lastsavepos=-1;
        lastsavepack = lastsavepackobj = savesessionid=0;
        firstpack = lastpack = lastpackobj = 0;
        part_section_list.clear();
        part_file_list.clear();
        riak_pnode_list.clear();
    }
} stru_part,*stru_part_ptr;

int ParallelLoad::get_part_pack_info(const char* pbasepth,const char* pdbname,const char* ptbname,
                                     const char* partname,int& pack_no,int& lstpackobj)
{
    int ret = -1;
    char path[256];
    sprintf(path,"%s/%s/%s.bht",pbasepth,pdbname,ptbname);

    char filename[300];
    sprintf(filename,"%s/PART_%05d.ctl",path,0);

    bool version15=true;

    FILE *fp = NULL;
    fp = fopen(filename,"rb");
    int attrid;
    uint16_t parts;
    std::vector<stru_part>  st_part_list;

    version15=true;
    char buf[20];
    fread(buf,1,8,fp);
    if(memcmp(buf,"PARTIF15",8)!=0) {
        if(memcmp(buf,"PARTINFO",8)!=0) {
            ThrowWith("�ļ���ʽ����,'%s'.",filename);
        }
        version15=false;
    }

    fread(&attrid,sizeof(int),1,fp);
    fread(&parts,sizeof(short),1,fp);
    long total_objs=0;

    // RIAK �� AP�汾��PART_XXXXX.ctl�ļ���ʽ��һ��
    // fileid ���Ȳ�һ��
    // RIAK:8�ֽ�
    // AP:4�ֽ�
    bool b_riak_version = CheckRiakVersion();

    for(int i=0; i<parts; i++) { // ���������ȡ��st_part_list
        stru_part _st_part;
        fread(buf,1,8,fp);
        if(memcmp(buf,"PARTPARA",8)!=0) {
            lgprintf("%d������ʽ����'%s'.",i+1,filename);
            throw -1;
        }
        fread(&_st_part.namelen,sizeof(short),1,fp);
        fread(_st_part.partname,_st_part.namelen,1,fp);
        _st_part.partname[_st_part.namelen]=0;
        fread(&_st_part.firstpack,sizeof(uint),1,fp);
        fread(&_st_part.lastpack,sizeof(uint),1,fp);
        fread(&_st_part.lastpackobj,sizeof(uint),1,fp);
        fread(&_st_part.lastsavepack,sizeof(uint),1,fp);
        fread(&_st_part.lastsavepackobj,sizeof(uint),1,fp);
        fread(&_st_part.savesessionid,sizeof(uint),1,fp);
        fread(&attrid,sizeof(uint),1,fp);
        if(version15) {
            if(b_riak_version) {
                fread(&_st_part.lastfileid,sizeof(long),1,fp);
            } else {
                fread(&_st_part.lastfileid,sizeof(int),1,fp);
            }
            fread(&_st_part.savepos,sizeof(int),1,fp);
            fread(&_st_part.lastsavepos,sizeof(int),1,fp);
        }

        // ap�汾�ķ������ļ��б�
        int vectsize;
        fread(&vectsize,sizeof(uint),1,fp);
        for(int j=0; j<vectsize; j++) {
            long fileid=0;
            if(b_riak_version) {
                fread(&fileid,sizeof(long),1,fp);
            } else {
                fread(&fileid,sizeof(uint),1,fp);
            }
            _st_part.part_file_list.push_back(fileid);
        }

        // part section ��Ϣ��ȡ���ڴ�
        int packsize;
        fread(&packsize,sizeof(uint),1,fp);
        for(int j=0; j<packsize; j++) {
            stru_part_section st_part_section;
            fread(&st_part_section.packstart,sizeof(uint),1,fp);
            fread(&st_part_section.packend,sizeof(uint),1,fp);
            fread(&st_part_section.lastobjs,sizeof(uint),1,fp);

            _st_part.part_section_list.push_back(st_part_section);
        } //  for(int j=0;j<packsize;j++) {

        st_part_list.push_back(_st_part);
    } // end for(int i=0;i<parts;i++)
    fclose(fp);
    fp = NULL;

    // ���Ҷ�Ӧ���������һ��packno
    for( int i=0; i<st_part_list.size(); i++) {
        if(strcasecmp(st_part_list[i].partname,partname) == 0) { // �ҵ���Ӧ����
            ret = 1;
            int section_no = st_part_list[i].part_section_list.size();
            if(section_no > 0) {
                pack_no = st_part_list[i].part_section_list[section_no-1].packend;
                lstpackobj = st_part_list[i].part_section_list[section_no-1].lastobjs;
            } else {
                pack_no =0 ;
                lstpackobj = 0;
            }
            break;
        }
    }

    return ret;
}

// ��ȡ����Ŀ��������
int ParallelLoad::get_table_pack_obj_info(const char* pbasepth,const char* pdbname,const char* ptbname,
        int& pack_no,int64& no_obj)
{

    char path[256];
    sprintf(path,"%s/%s/%s.bht",pbasepth,pdbname,ptbname);
    std::string attr_file = get_attr_file_name(path,0);

    FILE  *pFile  = fopen(attr_file.c_str(),"rb");
    if(!pFile) {
        lgprintf("�ļ�%s��ʧ��!\n",attr_file.c_str());
        return -__LINE__;
    }

    const int CLMD_HEADER_LEN = 46;
    char read_buf[CLMD_HEADER_LEN+1];
    int ret = fread(read_buf,CLMD_HEADER_LEN,1,pFile);
    if(ret != 1) {
        lgprintf("�ļ�%s��ȡʧ��!\n",attr_file.c_str());
        return -__LINE__;
    }

    /*    if ((unsigned char)read_buf[8] < 128) {
            col_type = (int)read_buf[8];
        } else {
            col_type = (int)((unsigned char)read_buf[8]-128);
        }*/

    no_obj = *((int64*)(read_buf + 9));
    pack_no = *((int*)(read_buf + 30));

    fclose(pFile);
    pFile = NULL;

    return 0;
}

// path: $DATAMERGER_HOME/var/dbname/tbname.bht
std::string ParallelLoad::get_dpn_file_name(const char* path,const int attr_number)
{
    char fnm[] = { "TA00000DPN.ctb" };
    fnm[6]=(char)('0'+attr_number%10);
    fnm[5]=(char)('0'+(attr_number/10)%10);
    fnm[4]=(char)('0'+(attr_number/100)%10);
    fnm[3]=(char)('0'+(attr_number/1000)%10);
    fnm[2]=(char)('0'+(attr_number/10000)%10);
    string filename(path);
    filename += "/";
    filename +=fnm;
    return filename;
}




/******************************************
 * ABSwitch implementation
 ******************************************/
typedef enum ABSwtich_tag { ABS_A_STATE, ABS_B_STATE }  ABSwitch;

#define INVALID_TRANSACTION_ID   0xFFFFFFFF

#define AB_SWITCH_FILE_NAME   "ab_switch"

class ABSwitcher
{
public:
    ABSwitch GetState(std::string const& path);

    static const char* SwitchName(ABSwitch value );

private:
    void GenerateName(std::string& name, std::string const& path);

};
/******************************************
 * ABSwitcher implementation
 ******************************************/

ABSwitch ABSwitcher::GetState(std::string const&  path)
{
    std::string name;
    GenerateName(name, path);
    struct stat st;
    if(stat(name.c_str(),&st)) { // �ɹ�����0
        return ABS_A_STATE;
    }
    return ABS_B_STATE;
}

void ABSwitcher::GenerateName(std::string& name, std::string const& path)
{
    name = path;
    name += "/";
    name += AB_SWITCH_FILE_NAME;
}

/*static*/
const char* ABSwitcher::SwitchName( ABSwitch value )
{
    if (value == ABS_A_STATE) return "ABS_A_STATE";
    return "ABS_B_STATE";
}

// ftype {0:read,1:write}
// path: $DATAMERGER_HOME/var/dbname/tbname.bht
std::string ParallelLoad::get_attr_file_name(const std::string path,const int attr_number)
{
    char fnm[] = { "TA00000.ctb" };

    bool oppositeName =false;
    int ftype = 0;

    if(ftype == 1) {
        fnm[1] = 'S'; // save file:   TS000...
    } else {
        ABSwitcher absw;
        ABSwitch cur_ab_switch_state = absw.GetState(path);

        if(oppositeName) {
            if(cur_ab_switch_state == ABS_A_STATE)
                fnm[1] = 'B';
        } else {
            if(cur_ab_switch_state == ABS_B_STATE)
                fnm[1] = 'B';
        }
    }

    fnm[6]=(char)('0'+attr_number%10);
    fnm[5]=(char)('0'+(attr_number/10)%10);
    fnm[4]=(char)('0'+(attr_number/100)%10);
    fnm[3]=(char)('0'+(attr_number/1000)%10);
    fnm[2]=(char)('0'+(attr_number/10000)%10);
    string filename(path);
    filename +="/";
    filename += fnm;
    return filename;
}


std::string DataDump::generate_session_id(SysAdmin &sp,int tabid,int datapartid)
{
    char load_session_id[32];
    sprintf(load_session_id,"_%d_%d_%d",tabid,datapartid,sp.GetSeqID("session_id"));
    return std::string(load_session_id);
}

void commit_sort_data_thread(ThreadList *tl)
{
    /*
    void *params[5];
    params[0]=p_ltDMIBSortLoader;
    params[1]=&load_param;
    params[2]=&loadrows;
    */
    void **params=tl->GetParams();
    DMIBSortLoad* _ltDMIBSortLoader=(DMIBSortLoad*)params[0];
    std::string *_load_param = (std::string*)params[1];
    long _loadrows = *(long*)params[2];
    std::string errmsg;
    int ret = _ltDMIBSortLoader->Finish(*_load_param,&errmsg);
    if(ret < 0) {
        ThrowWith("MASTER�ڵ�,������Ⱥ���ύ���Ự����ʧ��,������Ϣ[%s],�������[%s].",errmsg.c_str(),CR_Class_NS::strerrno(errno));
        tl->SetReturnValue(-1);
    }
    lgprintf("MASTER�ڵ�,��ֲ�ʽ����Ⱥ�ύ[%ld]�еĳ��Ự���ݳɹ�,Finish = %d.",_loadrows,ret);
    tl->SetReturnValue(0);
}



int DataDump::update_commit_info_to_db(SysAdmin &sp,int tabid,int datapartid,unsigned long commit_rows,bool b_lt_session)
{
    int    ret = 0;
    AutoStmt datapartext_mt(sp.GetDTS());
    if(b_lt_session) {

        if(b_master_node) {
            datapartext_mt.Prepare("update dp.dp_datapartext set "
                                   " sortendtime=now(),rownums=%ld,commitstatus=%d "
                                   " where tabid =%d and datapartid =%d and sessionid='%s' and sessiontype=%d ",
                                   commit_rows,PL_LOAD_WAIT,tabid,datapartid,ltsessionid,LONG_SESSION_TYPE);

            datapartext_mt.Execute(1);
            ret = datapartext_mt.Wait();
        } else {
            // fix dma-1402: slaver�ڵ㲻�ܸ���dp.dp_datapartext.commitstatus״̬
            // ���Ự��slaver�ڵ㲻�ø��¼�¼��,masterͳһ����
            datapartext_mt.Prepare("update dp.dp_datapartext set finishslavercnt=finishslavercnt+1,"
                                   " sortendtime=now() "
                                   " where tabid =%d and datapartid =%d and sessionid='%s' and sessiontype=%d ",
                                   tabid,datapartid,ltsessionid,LONG_SESSION_TYPE);

            datapartext_mt.Execute(1);
            ret = datapartext_mt.Wait();
        }

    } else {

        if(b_master_node) {
            datapartext_mt.Prepare("update dp.dp_datapartext set "
                                   " sortendtime=now(),rownums=rownums+%ld,commitstatus=%d "
                                   " where tabid =%d and datapartid =%d and sessionid='%s' and sessiontype=%d ",
                                   commit_rows,PL_LOAD_WAIT,tabid,datapartid,stsessionid,SHORT_SESSION_TYPE);

            datapartext_mt.Execute(1);
            ret = datapartext_mt.Wait();
        } else {
            // fix dma-1402: slaver�ڵ㲻�ܸ���dp.dp_datapartext.commitstatus״̬
            datapartext_mt.Prepare("update dp.dp_datapartext set finishslavercnt=finishslavercnt+1,"
                                   " sortendtime=now(),rownums=rownums+%ld "
                                   " where tabid =%d and datapartid =%d and sessionid='%s' and sessiontype=%d ",
                                   commit_rows,tabid,datapartid,stsessionid,SHORT_SESSION_TYPE);

            datapartext_mt.Execute(1);
            ret = datapartext_mt.Wait();
        }
    }
    return ret;
}


// ����launchmaster��
int DataDump::reset_part_launch_master_info(SysAdmin &sp,int tabid,int datapartid)
{
    int ret = 0;
    AutoStmt ast(sp.GetDTS());
    ast.Prepare("update dp.dp_datapart set launchmaster=0 where tabid= %d and datapartid = %d",
                tabid,datapartid);

    ast.Execute(1);
    ret = ast.Wait();

    return ret;
}


// �ֲ�ʽ������ɺ��ύ�Ź���
int DataDump::commit_sort_data_process(SysAdmin &sp,int tabid,int datapartid,bool cmt_st_session,
                                       bool cmt_lt_session,bool part_is_finish,bool filesmode)
{
    if(!dp_parallel_load) return 0;

    // ======= �ύ����������� ======== SASU-318
    // 1. �ж��Ƿ�Ϊmaster�ڵ�
    // 2.  slaver �ڵ�:
    // 2.1 ����������ɽӿ�
    // 2.2 ����: dp.dp_datapartext.finishslavercnt,rownums
    // ------------------------------------------------------
    // 2. master �ڵ�:
    // 2.1 �ȴ�����slaver�ڵ��������
    // 2.2 ��ȡ���ݿ�,����,��������,�÷��������һ�����ļ�¼������Ϣ
    // 2.3 ���������ύ����Ĳ���
    // 2.4 ����������ɽӿ�
    // 2.5 ����: dp.dp_datapartext.launchstarttime,dp.dp_datapartext.launchendtime,rownumber
    // 2.6 ��������״̬Ϊ3
    // ---------------------------------
    lgprintf("enter commit_sort_data_process ,tabid(%d),datapartid(%d),slaver_index(%d),stsessionid(%s),ltsessionid(%s).",tabid,datapartid,slaver_index,stsessionid,ltsessionid);

    std::string errmsg;
    int ret = 0;
    int launchcnt = 0;
    int finishcnt = 0;
    if(b_master_node) { // master�ڵ�

        // 2.1 �ж��Ƿ����е�slaver�ڵ㶼�Ѿ�������,����ȴ�
        int64 loadrows = 0;
        int cluster_status = 0;
        while(1) {
            if(is_all_slaver_node_finish(sp.GetDTS(),tabid,datapartid,stsessionid,
                                         launchcnt,finishcnt,loadrows,cluster_status)) {

                if(cluster_status == CS_ERROR) {
                    ThrowWith("MASTER�ڵ�,�ȴ�slaver�ڵ���ɹ����з��ּ�Ⱥ��ǰ�Ự[%s]����,�˳�.",stsessionid);
                }

                break;
            }
            lgprintf("Master�ڵ�,�ȴ������ύ�ڵ����,����ڵ���[%d]����ɽڵ�[%d],�ȴ�30��.",launchcnt,finishcnt);
            sleep(30);

            if(cluster_status == CS_ERROR) {
                ThrowWith("MASTER�ڵ�,�ȴ�slaver�ڵ���ɹ����з��ּ�Ⱥ��ǰ�Ự[%s]����,�˳�.",stsessionid);
            }
        }

        loadrows += st_parallel_load_rows; // master�ڵ������ݻ�û�и��½���dp.dp_datapartext����

        // master �ڵ����ü�Ⱥ״̬Ϊ��ס,��ʱ����slaver�ڵ����
        set_cluster_status(sp.GetDTS(),tabid,datapartid,stsessionid,CS_MASTER_BUSY);

        // 2.1x �ȴ��ñ�����������Ѿ������������ύ��,���ǻ�û��װ������ݽ���װ��
        //     dp.dp_datapartext.commitstatus in(0,1) and tabid = %d
        //     fix DMA-1347
        while(1) {
            int need_load_sessions = 0;
            int64 need_load_rows = 0;
            if(is_all_sorted_data_has_loaded(sp.GetDTS(),tabid,need_load_sessions,need_load_rows)) {
                break;
            }
            lgprintf("Master�ڵ�,�ȴ���(%d)�ϲ�װ�����ݻỰ��(%d)��¼��(%ld),�ȴ�30��.",tabid,need_load_sessions,need_load_rows);
            sleep(30);
        }

        // 2.2 ��ȡ���ݿ�,����,��������,�÷��������һ�����ļ�¼������Ϣ
        int64 startloadrow=0;
        int startpackno=0;
        int startpackobj = 0;
        get_commit_sort_info(sp,tabid,datapartid,startloadrow,
                             startpackno,startpackobj);

        lgprintf("��(%d),����(%d)׼���ύ����,��ʼ����(%d),��ʼ����¼��(%d),����ϲ���һ�������λ��(%d).",
                 tabid,datapartid,startpackno,startpackobj,startloadrow);

        // 2.3 ���������ύ����Ĳ���
        std::string st_load_param;
        std::string lt_load_param;
        {
            st_load_param = CR_Class_NS::str_setparam(st_load_param,
                            LIBIBLOAD_PN_TABLE_INFO,tbl_info_t_str);
            lt_load_param = CR_Class_NS::str_setparam(lt_load_param,
                            LIBIBLOAD_PN_TABLE_INFO,tbl_info_t_str);

            // ÿһ�����ļ�¼��
            unsigned int block_lines = 65536;
            st_load_param = CR_Class_NS::str_setparam_u64(st_load_param,
                            DMIBSORTLOAD_PN_BLOCK_LINES,block_lines);
            lt_load_param = CR_Class_NS::str_setparam_u64(lt_load_param,
                            DMIBSORTLOAD_PN_BLOCK_LINES,block_lines);

            // ��һ�е��к�(�µķ������߲������ķ�����Ҫ���ж��뿼��)
            // ��һ������Ҫ��������ʼ���λ��(���ڵ����λ��)
            st_load_param = CR_Class_NS::str_setparam_i64(st_load_param,
                            DMIBSORTLOAD_PN_LINEID_BEGIN,startloadrow);
            lt_load_param = CR_Class_NS::str_setparam_i64(lt_load_param,
                            DMIBSORTLOAD_PN_LINEID_BEGIN,0);


            // �������ı��
            st_load_param = CR_Class_NS::str_setparam_u64arr(st_load_param,
                            LIBIBLOAD_PN_PACKINDEX_INFO,tb_packindex_arr);
            lt_load_param = CR_Class_NS::str_setparam_u64arr(lt_load_param,
                            LIBIBLOAD_PN_PACKINDEX_INFO,tb_packindex_arr);


            st_load_param = CR_Class_NS::str_setparam_bool(st_load_param,
                            LIBIBLOAD_PN_PACKINDEX_MULTIMODE,true);
            lt_load_param = CR_Class_NS::str_setparam_bool(lt_load_param,
                            LIBIBLOAD_PN_PACKINDEX_MULTIMODE,true);

            /*
            LOAD_SORTED_DATA_DIR: �������ݴ洢·��

            ��mysqld�ڵ�����dpadmin��ʱ��
            export LOAD_SORTED_DATA_DIR=nfs://192.168.1.1:/app/dma/var/preparedir

            ����(mysqld ����������dpadmin��ʱ��):
            export LOAD_SORTED_DATA_DIR=/app/dma/var/preparedir
            */
            std::string hostname;
            char* p=getenv("LOAD_SORTED_DATA_DIR"); // fix DMA-1456, by hwei
            if(p==NULL) {
                ThrowWith("MASTER�ڵ�,��ȡ������������[LOAD_SORTED_DATA_DIR]����������,�޷���Ⱥ�ύ������������ݵ�·��,�쳣�˳�.");
            } else {
                hostname=std::string(p);
            }

            lgprintf("����ڵ���������,ת������ύ����·��Ϊ:%s. ",hostname.c_str());

            st_load_param = CR_Class_NS::str_setparam(st_load_param,LIBIBLOAD_PN_SAVE_PATHNAME,hostname);

            lt_load_param = CR_Class_NS::str_setparam(lt_load_param,LIBIBLOAD_PN_SAVE_PATHNAME,hostname);


            // �ַ����ͣ�ָ�� riak �����ַ����������ݶ����� RiakCluster::Rebuild �� conn_str ����һ��
            /*
            // �ò����ڴ�����������ʱ���������
            char* q=getenv(RIAK_HOSTNAME_ENV_NAME);
            if(NULL == q || strlen(q) <=1){
                lgprintf("��������:RIAK_HOSTNAMEδ����,�������Ϊ�ֲ�ʽ����AP�汾.");
            }else{
                lgprintf("��������:RIAK_HOSTNAME=[%s],�������Ϊ�ֲ�ʽ����Cloud�汾.",q);

                std::string _connect_riak_str = std::string(q);
                st_load_param = CR_Class_NS::str_setparam(st_load_param,LIBIBLOAD_PN_RIAK_CONNECT_STR,_connect_riak_str);
                lt_load_param = CR_Class_NS::str_setparam(lt_load_param,LIBIBLOAD_PN_RIAK_CONNECT_STR,_connect_riak_str);

                // LIBIBLOAD_PN_RIAK_BUCKET �ַ����ͣ�ָ�����ڱ������ݵ� riak bucket ���ƣ�Ŀǰ�汾 CDM ָ��Ϊ "IB_DATAPACKS"
                _connect_riak_str = std::string("IB_DATAPACKS");
                st_load_param = CR_Class_NS::str_setparam(st_load_param,LIBIBLOAD_PN_RIAK_BUCKET,_connect_riak_str);
                lt_load_param = CR_Class_NS::str_setparam(lt_load_param,LIBIBLOAD_PN_RIAK_BUCKET,_connect_riak_str);
            }
            */

            // 2.4 ����������ɽӿ�

            // 2.4.1 �̻Ự�����ύ
            if(cmt_st_session) {
                if(strcmp(last_stsessionid,stsessionid) != 0) {
                    ret = p_stDMIBSortLoader->Finish(st_load_param,&errmsg);
                    if(ret < 0) {
                        ThrowWith("MASTER�ڵ�,������Ⱥ���ύ�̻Ự����ʧ��,������Ϣ[%s],�������[%s].",errmsg.c_str(),CR_Class_NS::strerrno(errno));
                    }
                    lgprintf("MASTER�ڵ�,��ֲ�ʽ����Ⱥ�ύ[%ld]�еĶ̻Ự[%s]���ݳɹ�,Finish = %d.",loadrows,stsessionid,ret);
                } else {
                    lgprintf("MASTER�ڵ�,��ֲ�ʽ����Ⱥ�ύ�ỰID[%s]��ͬ.������finish����.",stsessionid);
                }
            } else {
                lgprintf("MASTER�ڵ�,������ֲ�ʽ����Ⱥ�ύ[%ld]�еĶ̻Ự����.",loadrows);
            }
            // 2.4.2 ���Ự�����ύ,�̻Ự���ݽ��
            if(p_ltDMIBSortLoader != NULL && st_session_interval>0) {

                lgprintf("MASTER�ڵ�,׼����ֲ�ʽ����Ⱥ�ύ�̻Ự[%s]��[%ld]�г��Ự���ݳɹ�.",stsessionid,loadrows);
                CR_FixedLinearStorage tmp_fls(65536);
                tmp_fls.SetTag(CR_Class_NS::str_setparam("",DMIBSORTLOAD_PN_QUERY_CMD, LIBSORT_PV_APPEND_PART_FILES));
                tmp_fls.PushBack(loadrows,std::string(stsessionid),NULL);
                ret = p_ltDMIBSortLoader->PushLines(tmp_fls);

                if(ret != 0) {
                    ThrowWith("MASTER�ڵ�,������Ⱥ���᳤�Ự����ʧ��,������Ϣ[%s],�������[%s].",errmsg.c_str(),CR_Class_NS::strerrno(errno));
                }
                lgprintf("MASTER�ڵ�,��ֲ�ʽ����Ⱥ�ύ[%ld]�еĳ��Ự[%s]���ݳɹ�.",loadrows,stsessionid);

                lt_parallel_load_rows+=loadrows;

                // ���³��Ự��¼��,fix dma-1479
                {
                    AutoStmt st(sp.GetDTS());
                    lgprintf("���ñ�(%d),����(%d),���Ự('%s')�ļ�¼��Ϊ(%ld).",tabid,datapartid,ltsessionid,lt_parallel_load_rows);
                    st.DirectExecute("update dp.dp_datapartext set rownums = %ld  where tabid = %d and datapartid=%d and sessionid='%s'",
                                     lt_parallel_load_rows,tabid,datapartid,ltsessionid);
                }
            }


            // 2.5 ����: dp.dp_datapartext���:
            // sortendtime,rownums,commitstatus

            // 2.5.1 �̻Ự�ϸ���
            if(strcmp(last_stsessionid,stsessionid) != 0) {
                update_commit_info_to_db(sp,tabid,datapartid,st_parallel_load_rows,false);
            }

        }

        // 2.5.2 ���Ự�ϸ���

        // �ȴ����Ự�������ύ���
        if(cmt_lt_session && p_ltDMIBSortLoader != NULL && st_session_interval>0) {// ֻ�г��Ự������ڵ�ʱ��Ž����ύ

            ret = p_ltDMIBSortLoader->Finish(lt_load_param,&errmsg);
            if(ret < 0) {
                ThrowWith("MASTER�ڵ�,������Ⱥ���ύ���Ự����ʧ��,������Ϣ[%s],�������[%s].",errmsg.c_str(),CR_Class_NS::strerrno(errno));
            }
            lgprintf("MASTER�ڵ�,��ֲ�ʽ����Ⱥ�ύ[%ld]�еĳ��Ự[%s]���ݳɹ�,Finish=%d.",lt_parallel_load_rows,ltsessionid,ret);

            update_commit_info_to_db(sp,tabid,datapartid,lt_parallel_load_rows,true);
        }


        // 2.6 �ύ�ļ�״̬
        if(filesmode) {
            // �����ļ�״̬,Ϊ�Ѿ��ύ���״̬
            // �ļ�״̬���ύֻ����master�ڵ������
            AutoStmt st(dtdbc);
            char slaver_index_arry[128]; // �ϲ���slaver_index,����:'0','1','2'...
            slaver_index_arry[0] = 0;
            for(int i=0; i<=launchcnt; i++) {
                sprintf(slaver_index_arry+strlen(slaver_index_arry),"'%d'",i);
                if(i != launchcnt) {
                    strcat(slaver_index_arry,",");
                }
            }
            lgprintf("�ύ��(%d),����(%d),�ڵ�(%s)���ļ���״̬[10--->2].",tabid,datapartid,slaver_index_arry);
            if(strlen(slaver_index_arry)>0) {
                st.DirectExecute("update dp.dp_filelog set status=2 where tabid=%d and datapartid=%d and slaveridx in (%s) and status=10"
                                 ,tabid,datapartid,slaver_index_arry);
            }
        }

        // 2.7 ���������ɺ�ֱ�ӽ�����״̬�޸ĳ�3(�������)����
        if(part_is_finish) {
            sp.UpdateTaskStatus(LOADING,tabid,datapartid);

            reset_part_launch_master_info(sp,tabid,datapartid);

        }

        st_parallel_load_rows = 0;

        // �ָ����ü�Ⱥ״̬
        set_cluster_status(sp.GetDTS(),tabid,datapartid,stsessionid,CS_OK);

    } else { // slaver �ڵ�

        // 2 ����������ɽӿ�
        // 2.1 �̻Ự�����ύ
        std::string load_param="";
        if(strcmp(last_stsessionid,stsessionid) != 0) {
            ret = p_stDMIBSortLoader->Finish(load_param,&errmsg);
            if(ret <0) { // fix dma-2094
                ThrowWith("SLAVER�ڵ�[%d],������Ⱥ���ύ�̻Ự����ʧ��,������Ϣ[%s],�������[%s].",slaver_index,errmsg.c_str(),CR_Class_NS::strerrno(errno));
            }
            lgprintf("SLAVER�ڵ�[%d],��ֲ�ʽ����Ⱥ�ύ[%ld]�еĶ̻Ự[%s]���ݳɹ�,Finish=%d.",slaver_index,st_parallel_load_rows,stsessionid,ret);

        } else {
            lgprintf("SLAVER�ڵ�[%d],��ֲ�ʽ����Ⱥ�ύ�ỰID[%s]��ͬ.������finish����.",slaver_index,stsessionid);
        }

        // 2.2 ȫ�ֻỰ�����ύ
        if(cmt_lt_session && p_ltDMIBSortLoader != NULL && st_session_interval>0) {// ֻ�г��Ự������ڵ�ʱ��Ž����ύ
            ret = p_ltDMIBSortLoader->Finish(load_param,&errmsg);
            if(ret < 0) {
                ThrowWith("SLAVER�ڵ�,������Ⱥ���ύ���Ự����ʧ��,������Ϣ[%s],�������[%s].",errmsg.c_str(),CR_Class_NS::strerrno(errno));
            }
            lgprintf("SLAVER�ڵ�,��ֲ�ʽ����Ⱥ�ύ[%ld]�еĳ��Ự[%s]���ݳɹ�,Finish=%d.",lt_parallel_load_rows,ltsessionid,ret);
        }

        if(strcmp(last_stsessionid,stsessionid) != 0) {
            // �̻Ự�ϵĸ���
            update_commit_info_to_db(sp,tabid,datapartid,st_parallel_load_rows,false);
        }
        if(cmt_lt_session) { // ���Ự�ϸ��¼�¼���(0)��finish��
            update_commit_info_to_db(sp,tabid,datapartid,0,cmt_lt_session);
        }
        st_parallel_load_rows= 0;

    }

    strcpy(last_stsessionid,stsessionid);

    if(part_is_finish) {
        lgprintf("��������tabid(%d),datapartid(%d)�����������,�ȴ�װ������.",tabid,datapartid);
    }

    lgprintf("leave commit_sort_data_process ,tabid(%d),datapartid(%d),slaver_index(%d).",tabid,datapartid,slaver_index);


    return 0;
}


// û�вɼ������ݵ�ʱ��,����һ���յ�DMIBSortLoad����,�����ύ��ʱ���ʧ��
int DataDump::CreateEmptyDMIBSortLoad(SysAdmin &sp,int tabid,int partid,AutoMt *pCur)
{
    if(dp_parallel_load) {

        int row_str_len = wociGetRowLen(*pCur);
        dp_parallel_load_columns = pCur->GetColumnNum();
        row_str_len += dp_parallel_load_columns*256;

        // ��ֵ���͵���ʱ����
        uint64_t numeric_val_tmp=0;
        uint32_t uint32_val_tmp=0;
        uint16_t uint16_val_tmp= 0;

        // ======= ��������װ������========= SASU-318
        // 1. ���ӵ�����Ⱥ
        // 2. ͨ���Ѿ����ӵ�����Ⱥ,��������������
        // 3. �ڴ����õ�handle��������,���еķ�װ�����ݣ�������װ�õ�����ͨ��PushRow��������Ⱥ
        // 4. ������������Ҫ��������ݶ�������ɺ�,�ȴ��������,���ú��� DataDump::commit_sort_data
        // ---------------------------------

        // 1. ���ӵ�����Ⱥ
        // 2. ͨ���Ѿ����ӵ�����Ⱥ,��������������
        if(p_stDMIBSortLoader== NULL) { // �÷�����һ�������ʱ��,�Ż����ִ��

            int ret = 0;

            // ��ȡ������������
            std::vector<std::string> packidx_column_list;
            get_packindex_column_list(sp,tabid,packidx_column_list);
            tb_packindex_arr.clear();

            // ��ȡzipѹ����
            std::vector<std::string> zipcompress_column_list;
            get_zipcompress_column_list(sp,tabid,zipcompress_column_list);

            std::vector<std::string> snappycompress_column_list;
            get_snappycompress_column_list(sp,tabid,snappycompress_column_list);

            std::vector<std::string> lz4compress_column_list;
            get_lz4compress_column_list(sp,tabid,lz4compress_column_list);



            // 1. ���ӵ�����Ⱥ,�Ѿ��ڹ��캯���е���
            // init_riak_cluster_for_sort()ʵ��

            // 2. ͨ���Ѿ����ӵ�����Ⱥ,��������������
            table_info.clear();

            // ��������Ŀ
            uint16_t col_numbers = dp_parallel_load_columns;

            std::string _col_name="";
            int _tmp_col_len = 0;
            for(int i = 0; i<col_numbers ; i++) {

                char _tmp_col_name[256];
                wociGetColumnName(*pCur,i,_tmp_col_name);
                _col_name = std::string(_tmp_col_name);

                // �жϸ����Ƿ��ǰ�����
                if(column_is_packindex(packidx_column_list,_col_name)) {
                    tb_packindex_arr.push_back(i);
                }

                // �жϸ����Ƿ���zipѹ��
                std::string str_compress_mode = std::string("");
                if(column_is_zipcompress(zipcompress_column_list,_col_name)) {
                    str_compress_mode =  std::string("compressmode=zlib");
                } else if(column_is_snappycompress(snappycompress_column_list,_col_name)) {
                    str_compress_mode =  std::string("compressmode=snappy");
                } else if(column_is_lz4compress(lz4compress_column_list,_col_name)) {
                    str_compress_mode =  std::string("compressmode=lz4");
                }


                switch(pCur->getColType(i)) {
                    case COLUMN_TYPE_CHAR:
                        _tmp_col_len = pCur->getColLen(i) -1; // fix DMA-1383:mysqld �˵ĳ����ǲ���0��β��,��Ҫ��1
                        table_info.push_back(_col_name,DM_IB_TABLEINFO_CT_VARCHAR,_tmp_col_len,0,str_compress_mode);
                        break;
                    case COLUMN_TYPE_FLOAT:
                        table_info.push_back(_col_name, DM_IB_TABLEINFO_CT_DOUBLE, 8,0,str_compress_mode);
                        break;
                    case COLUMN_TYPE_INT:
                        table_info.push_back(_col_name, DM_IB_TABLEINFO_CT_INT, 4,0,str_compress_mode);
                        break;
                    case COLUMN_TYPE_BIGINT:

                        table_info.push_back(_col_name, DM_IB_TABLEINFO_CT_BIGINT, 8,0,str_compress_mode);
                        break;
                    case COLUMN_TYPE_DATE:
                        table_info.push_back(_col_name, DM_IB_TABLEINFO_CT_DATETIME, 8,0,str_compress_mode);
                        break;
                    default:
                        ThrowWith("Invalid column type :%d,id:%d",pCur->getColType(i),i);
                }
            }
            table_info.SerializeToString(tbl_info_t_str);
            row_data.BindTableInfo(table_info); //�ж���󶨵� table_info ����

            // �������̻Ự����
            return create_DMIBSortLoader(sp,tabid,partid,true);
        }
    }

    return 0;

}


typedef std::queue<CR_FixedLinearStorage*> CR_FixedLinearStorage_queue;
void PushThread(ThreadList *tl)
{
    /*
    void* params[10];
    params[0] = (void*)&fixed_linear_storage_queue_run_flag;
    params[1] = (void*)&fixed_linear_storage_queue_mutex;
    params[2] = (void*)&fixed_linear_storage_queue;
    params[3] = (void*)p_stDMIBSortLoader;
    params[4] = (void*)stsessionid;
    params[5] = NULL;
    pushThread.SetReturnValue(0);
    pushThread.Start(params,10,PushThread);
    */
    void **params=tl->GetParams();
    bool& _fixed_linear_storage_queue_run_flag=*(bool *)params[0];
    Mutex *_fixed_linear_storage_queue_mutex =(Mutex*)params[1];
    CR_FixedLinearStorage_queue* _fixed_linear_storage_queue = (CR_FixedLinearStorage_queue*)params[2];
    DMIBSortLoad* _stDMIBSortLoader=( DMIBSortLoad* )params[3];
    char* _stsessionid=(char*)params[4];
    int ret = 0;
    while(1) {
        _fixed_linear_storage_queue_mutex->Lock();
        if(!_fixed_linear_storage_queue_run_flag) {

            while(!_fixed_linear_storage_queue->empty()) {
                std::string errmsg = "";
                CR_FixedLinearStorage* pobj =  _fixed_linear_storage_queue->front();
                lgprintf("PushThread [%s] DMIBSortLoader->PushLines[%p] A start",_stsessionid,pobj);
                ret = _stDMIBSortLoader->PushLines(*pobj, &errmsg);
                lgprintf("PushThread [%s] DMIBSortLoader->PushLines[%p] A end",_stsessionid,pobj);
                if(ret) {
                    // delete by liujs : the following code exist crash while invoking  CR_FixedLinearStorage::~CR_FixedLinearStorage/clear , more detail DMA-1873
                    if(pobj != NULL) {
                        // pobj->Clear();
                        // DPRINTF("delete pobj=[%p]\n",pobj);
                        delete pobj;
                        pobj=NULL;
                    }
                    _fixed_linear_storage_queue->pop();
                    _fixed_linear_storage_queue_mutex->Unlock();
                    tl->SetReturnValue(1); // error
                    lgprintf("PushThreadA������Ⱥ[%s]���������ʧ��,������Ϣ[%s],�������[%d].",_stsessionid,errmsg.c_str(),ret);
                    return ;
                }
                // pobj->Clear();
                delete pobj;
                pobj=NULL;
                _fixed_linear_storage_queue->pop();
            }
            _fixed_linear_storage_queue_mutex->Unlock();
            break;
        }
        _fixed_linear_storage_queue_mutex->Unlock();

        _fixed_linear_storage_queue_mutex->Lock();
        bool lock_in_while =false;
        int  idle_cnt = 0;
        while(!_fixed_linear_storage_queue->empty()) {

            if(!lock_in_while) {
                _fixed_linear_storage_queue_mutex->Unlock();
                lock_in_while = true;
            }

            std::string errmsg = "";
            _fixed_linear_storage_queue_mutex->Lock();
            CR_FixedLinearStorage* pobj =  _fixed_linear_storage_queue->front();
            //_fixed_linear_storage_queue_mutex->Unlock();

            lgprintf("PushThread [%s] DMIBSortLoader->PushLines[%p] B start",_stsessionid,pobj);
            ret = _stDMIBSortLoader->PushLines(*pobj, &errmsg);
            lgprintf("PushThread [%s] DMIBSortLoader->PushLines[%p] B end",_stsessionid,pobj);
            if(ret) {
                // delete by liujs : the following code exist crash while invoking  CR_FixedLinearStorage::~CR_FixedLinearStorage/clear , more detail DMA-1873
                if(pobj != NULL) {
                    // pobj->Clear();
                    DPRINTF("delete pobj=[%p]\n",pobj);
                    delete pobj;
                    pobj = NULL;
                }

                //_fixed_linear_storage_queue_mutex->Lock();
                _fixed_linear_storage_queue->pop();
                _fixed_linear_storage_queue_mutex->Unlock();

                tl->SetReturnValue(2);  // error
                lgprintf("PushThreadB������Ⱥ[%s]���������ʧ��,������Ϣ[%s],�������[%d].",_stsessionid,errmsg.c_str(),ret);
                return ;
            }
            // pobj->Clear();
            delete pobj;
            pobj=NULL;

            //_fixed_linear_storage_queue_mutex->Lock();
            _fixed_linear_storage_queue->pop();
            _fixed_linear_storage_queue_mutex->Unlock();
        }

        if(!lock_in_while) {
            _fixed_linear_storage_queue_mutex->Unlock();
            if(idle_cnt++>1000) {
                sleep(1);
                idle_cnt=0;
            }
        }

    }
    lgprintf("Exit Pushthread.");
    tl->SetReturnValue(0);
}


#define max(a,b) ((a)>(b)?(a):(b))
void DataDump::ProcBlock(SysAdmin &sp,int tabid,int partid,AutoMt *pCur,int idx,AutoMt &blockmt,int _fid,bool filesmode)
{
    if(dp_parallel_load) { // ��������װ������

        mytimer  ProcBlockAll;
        ProcBlockAll.Clear();
        ProcBlockAll.Start();

        mytimer  ProcBlockPush;
        ProcBlockPush.Clear();

        int row_str_len = wociGetRowLen(*pCur);
        dp_parallel_load_columns = pCur->GetColumnNum();
        row_str_len += dp_parallel_load_columns*256;

        // ��ֵ���͵���ʱ����
        uint64_t numeric_val_tmp=0;
        uint32_t uint32_val_tmp=0;
        uint16_t uint16_val_tmp= 0;

        // ======= ��������װ������========= SASU-318
        // 1. ���ӵ�����Ⱥ
        // 2. ͨ���Ѿ����ӵ�����Ⱥ,��������������
        // 3. �ڴ����õ�handle��������,���еķ�װ�����ݣ�������װ�õ�����ͨ��PushRow��������Ⱥ
        // 4. ������������Ҫ��������ݶ�������ɺ�,�ȴ��������,���ú��� DataDump::commit_sort_data
        // ---------------------------------

        // 1. ���ӵ�����Ⱥ
        // 2. ͨ���Ѿ����ӵ�����Ⱥ,��������������
        if(p_stDMIBSortLoader== NULL) { // �÷�����һ�������ʱ��,�Ż����ִ��
            int ret = 0;

            // ��ȡ������������
            std::vector<std::string> packidx_column_list;
            get_packindex_column_list(sp,tabid,packidx_column_list);
            tb_packindex_arr.clear();

            // ��ȡzipѹ����
            std::vector<std::string> zipcompress_column_list;
            get_zipcompress_column_list(sp,tabid,zipcompress_column_list);

            std::vector<std::string> snappycompress_column_list;
            get_snappycompress_column_list(sp,tabid,snappycompress_column_list);

            std::vector<std::string> lz4compress_column_list;
            get_lz4compress_column_list(sp,tabid,lz4compress_column_list);


            // 1. ���ӵ�����Ⱥ,�Ѿ��ڹ��캯���е���
            // init_riak_cluster_for_sort()ʵ��

            // 2. ͨ���Ѿ����ӵ�����Ⱥ,��������������
            table_info.clear();
            // ��������Ŀ
            uint16_t col_numbers = dp_parallel_load_columns;

            std::string _col_name="";
            int _tmp_col_len = 0;
            for(int i = 0; i<col_numbers ; i++) {

                char _tmp_col_name[256];
                wociGetColumnName(*pCur,i,_tmp_col_name);
                _col_name = std::string(_tmp_col_name);

                // �жϸ����Ƿ��ǰ�����
                if(column_is_packindex(packidx_column_list,_col_name)) {
                    tb_packindex_arr.push_back(i);
                }

                // �жϸ����Ƿ���zipѹ��
                std::string str_compress_mode = std::string("");
                if(column_is_zipcompress(zipcompress_column_list,_col_name)) {
                    str_compress_mode =  std::string("compressmode=zlib");
                } else if(column_is_snappycompress(snappycompress_column_list,_col_name)) {
                    str_compress_mode =  std::string("compressmode=snappy");
                } else if(column_is_lz4compress(lz4compress_column_list,_col_name)) {
                    str_compress_mode =  std::string("compressmode=lz4");
                }

                switch(pCur->getColType(i)) {
                    case COLUMN_TYPE_CHAR:
                        _tmp_col_len = pCur->getColLen(i) -1; // fix DMA-1383:mysqld �˵ĳ����ǲ���0��β��,��Ҫ��1
                        table_info.push_back(_col_name,DM_IB_TABLEINFO_CT_VARCHAR,_tmp_col_len,0,str_compress_mode);
                        break;
                    case COLUMN_TYPE_FLOAT:
                        table_info.push_back(_col_name, DM_IB_TABLEINFO_CT_DOUBLE, 8,0,str_compress_mode);
                        break;
                    case COLUMN_TYPE_INT:
                        table_info.push_back(_col_name, DM_IB_TABLEINFO_CT_INT, 4,0,str_compress_mode);
                        break;
                    case COLUMN_TYPE_BIGINT:
                        table_info.push_back(_col_name, DM_IB_TABLEINFO_CT_BIGINT, 8,0,str_compress_mode);
                        break;
                    case COLUMN_TYPE_DATE:
                        table_info.push_back(_col_name, DM_IB_TABLEINFO_CT_DATETIME, 8,0,str_compress_mode);
                        break;
                    default:
                        ThrowWith("Invalid column type :%d,id:%d",pCur->getColType(i),i);
                }
            }
            table_info.SerializeToString(tbl_info_t_str);

            row_data.BindTableInfo(table_info); //�ж���󶨵� table_info ����

            // �������̻Ự����
            if(b_master_node) {
                lgprintf("MASTER�ڵ�,�������̻ỰID(%s),���ü�Ⱥ״̬Ϊ:CS_MASTER_INIT",stsessionid);
                set_cluster_status(sp.GetDTS(),tabid, datapartid,stsessionid,CS_MASTER_INIT);

                if(st_session_interval>0) {
                    lgprintf("MASTER�ڵ�,���������ỰID(%s),���ü�Ⱥ״̬Ϊ:CS_MASTER_INIT",ltsessionid);
                    set_cluster_status(sp.GetDTS(),tabid, datapartid,ltsessionid,CS_MASTER_INIT);
                }
            }
            int _ret = create_DMIBSortLoader(sp,tabid,partid,true);

            if(_ret == -1) {

                if(!b_master_node) {
                    // �̻Ự�Լ���
                    decrease_slaver_lanuch_count(sp,tabid,datapartid,false);
                    if(st_session_interval>0) {
                        // ���Ự�Լ���
                        decrease_slaver_lanuch_count(sp,tabid,datapartid,true);
                    }

                    ThrowWith("Slaver�ڵ�DataDump::create_DMIBSortLoader��������ʧ��,tabid(%d),datapartid(%d),�Ự(%s)׼���˳�!",
                              tabid,datapartid,stsessionid);
                } else {

                    ThrowWith("Master�ڵ�DataDump::create_DMIBSortLoader��������ʧ��,tabid(%d),datapartid(%d),�Ự(%s)׼���˳�!",
                              tabid,datapartid,stsessionid);

                }
            } else if (_ret == 0) {
                if(b_master_node) {
                    lgprintf("MASTER�ڵ�,�̻ỰID(%s)��������������,���ü�Ⱥ״̬Ϊ:CS_OK",stsessionid);
                    set_cluster_status(sp.GetDTS(),  tabid, datapartid,stsessionid,CS_OK);

                    if(st_session_interval>0) {
                        lgprintf("MASTER�ڵ�,���ỰID(%s)��������������,���ü�Ⱥ״̬Ϊ:CS_OK",ltsessionid);
                        set_cluster_status(sp.GetDTS(),  tabid, datapartid,ltsessionid,CS_OK);
                    }
                }
            }

            // ���������еĶ���
            int cur=*pCur;
            char *idxcolsname=dp.idxp[idx].idxcolsname;
            lgprintf("�����ֶ���[%s].",idxcolsname);
            wociConvertColStrToInt2(cur,idxcolsname,sort_key_col_info.idxcol,sort_key_col_info.partlen);
        }

        // 3. �ڴ����õ�handle��������,���еķ�װ�����ݣ�������װ�õ�����ͨ��PushRow��������Ⱥ
        const  int64 CR_FixedLinearStorage_size = 1024*1024*128;


        push_line_nums = 0;
#ifndef THREAD_PUSH
        CR_FixedLinearStorage lines(CR_FixedLinearStorage_size);

#else
        CR_FixedLinearStorage *plines = NULL;

        CR_FixedLinearStorage *lines = new CR_FixedLinearStorage(CR_FixedLinearStorage_size);
        plines = lines;
        CR_FixedLinearStorage_queue fixed_linear_storage_queue;
        Mutex fixed_linear_storage_queue_mutex;
        bool fixed_linear_storage_queue_run_flag = true;
        ThreadList pushThread;
        {
            void* params[10];
            params[0] = (void*)&fixed_linear_storage_queue_run_flag;
            params[1] = (void*)&fixed_linear_storage_queue_mutex;
            params[2] = (void*)&fixed_linear_storage_queue;
            params[3] = (void*)p_stDMIBSortLoader;
            params[4] = (void*)stsessionid;
            params[5] = NULL;
            pushThread.SetReturnValue(0);
            pushThread.Start(params,10,PushThread);
        }
#endif
        int ret = 0;
        try {
            // try to fix dma-1988/dma-1928 by liujs
            if(pCur->GetRows() > 0) {
                lgprintf("׼����ֲ�ʽ����Ⱥ[%s]����[%d]��[%d]�е�����...",stsessionid,pCur->GetRows(),pCur->GetColumnNum());

                // �����keyֵ
                char *p_mem_sort_key = new char[row_str_len];
                int p_mem_sort_key_pos = 0;

                // null ֵ��key
                std::string null_key = "";
                null_key.assign(1,(char)0);

                // ��null��ֵ��key,�ַ�����flag
                std::string non_null_key = "";
                non_null_key.assign(1,(char)1);
                char _tmp[2000];
                _tmp[sizeof(_tmp)-1] = 0;
                char sz_date[200];
                sz_date[sizeof(sz_date)-1] = 0;

                for(int idx_row=0; idx_row < pCur->GetRows(); idx_row++) {
                    p_mem_sort_key_pos =0;

                    for(int _i = 0; _i<(sizeof(sort_key_col_info.idxcol)/sizeof(int)); _i++) {

                        if(sort_key_col_info.idxcol[_i] == -1) { // �Ѿ�����
                            break;
                        }
                        // 3.1> ���������õ�key,����֮��ֱ���ַ�������
                        // a> ���е������KEYʹ���ַ�������ֱ�Ӻϲ�����
                        // b> �����NULL����,�����ַ�null_key[0]����
                        // c> �������NULL����non_null_key[1]����

                        int idx_col = sort_key_col_info.idxcol[_i];

                        // ���������е�NULLֵ: null flag[0],non-null[1]
                        if(wociIsNull(*pCur, idx_col, idx_row)) {
                            memcpy(p_mem_sort_key+p_mem_sort_key_pos,null_key.data(),null_key.length());
                            p_mem_sort_key_pos+=null_key.length();
                            break;  // ��ǰ�д������
                        } else {
                            memcpy(p_mem_sort_key+p_mem_sort_key_pos,non_null_key.data(),non_null_key.length());
                            p_mem_sort_key_pos += non_null_key.length();
                        }

                        // ��NULLֵ����,���ö�Ӧ��ֵ,���ɶ�Ӧ������KEY
                        char *_p;
                        switch(pCur->getColType(idx_col)) {

                            case COLUMN_TYPE_CHAR:

                                _p = (char*)pCur->PtrStr(idx_col,idx_row);
                                if(sort_key_col_info.partlen[_i] == 0) { // �����ȡ
                                    memcpy(p_mem_sort_key+p_mem_sort_key_pos,_p,strlen(_p));
                                    p_mem_sort_key_pos += strlen(_p);
                                } else if(sort_key_col_info.partlen[_i] < 0) { // ��β����ȡ
                                    int _len = strlen(_p);
                                    if( _len + sort_key_col_info.partlen[_i] < 0) {
                                        memcpy(p_mem_sort_key+p_mem_sort_key_pos,_p,strlen(_p));
                                        p_mem_sort_key_pos += strlen(_p);
                                    } else {
                                        _len = strlen(_p)+sort_key_col_info.partlen[_i];
                                        strcpy(_tmp,_p+_len);
                                        memcpy(p_mem_sort_key+p_mem_sort_key_pos,_tmp,strlen(_tmp));
                                        p_mem_sort_key_pos += strlen(_tmp);

                                    }
                                } else { // ��ͷ��ȡ
                                    int _len = strlen(_p);
                                    if(_len > sort_key_col_info.partlen[_i]) {
                                        strcpy(_tmp,_p);
                                        _tmp[sort_key_col_info.partlen[_i]] = 0;
                                        memcpy(p_mem_sort_key+p_mem_sort_key_pos,_tmp,strlen(_tmp));
                                        p_mem_sort_key_pos += strlen(_tmp);

                                    } else {

                                        memcpy(p_mem_sort_key+p_mem_sort_key_pos,_p,strlen(_p));
                                        p_mem_sort_key_pos += strlen(_p);

                                    }
                                }
                                break;

                            case COLUMN_TYPE_FLOAT:
                                numeric_val_tmp =  htobe64(CR_Class_NS::double2u64(*pCur->PtrDouble(idx_col,idx_row)));
                                memcpy(p_mem_sort_key+p_mem_sort_key_pos,(char*)&numeric_val_tmp,sizeof(numeric_val_tmp));
                                p_mem_sort_key_pos += sizeof(numeric_val_tmp);

                                break;

                            case COLUMN_TYPE_INT:
                                // wociGetColumnName(*pCur,idx_col,_tmp);
                                // sort_key += CR_Class_NS::i642str(pCur->GetLong(_tmp,idx_row));
                                uint32_val_tmp = htobe32(CR_Class_NS::i322u32(*pCur->PtrInt(idx_col,idx_row)));

                                memcpy(p_mem_sort_key+p_mem_sort_key_pos,(char*)&uint32_val_tmp,sizeof(uint32_val_tmp));
                                p_mem_sort_key_pos += sizeof(uint32_val_tmp);

                                break;

                            case COLUMN_TYPE_BIGINT:
                                // wociGetColumnName(*pCur,idx_col,_tmp);
                                // sort_key += CR_Class_NS::i642str(pCur->GetLong(_tmp,idx_row));
                                numeric_val_tmp = htobe64(CR_Class_NS::i642u64(*pCur->PtrLong(idx_col,idx_row)));
                                memcpy(p_mem_sort_key+p_mem_sort_key_pos,(char*)&numeric_val_tmp,sizeof(numeric_val_tmp));
                                p_mem_sort_key_pos += sizeof(numeric_val_tmp);

                                break;

                            case COLUMN_TYPE_DATE: {
                                pCur->GetDate(idx_col,idx_row,sz_date);
                                if(sort_key_col_info.partlen[_i] <= 0) { // �����ȡ
                                    sprintf(_tmp,"%04d%02d%02d%02d%02d%02d",
                                            wociGetYear(sz_date),
                                            wociGetMonth(sz_date),
                                            wociGetDay(sz_date),
                                            wociGetHour(sz_date),
                                            wociGetMin(sz_date),
                                            wociGetSec(sz_date));
                                } else { // ��ͷ����ȡ
                                    // yyyy-mm-dd hh:mi:ss ֧�ֽ�ȡ��: ��,��,��,ʱ,��
                                    switch(sort_key_col_info.partlen[_i]) {
                                        case 4: // year
                                            sprintf(_tmp,"%04d",wociGetYear(sz_date));
                                            break;
                                        case 7: // month
                                            sprintf(_tmp,"%04d%02d",wociGetYear(sz_date),wociGetMonth(sz_date));
                                            break;
                                        case 10: // day
                                            sprintf(_tmp,"%04d%02d%02d",wociGetYear(sz_date),wociGetMonth(sz_date),wociGetDay(sz_date));
                                            break;
                                        case 3: // hour
                                            sprintf(_tmp,"%04d%02d%02d%02d",wociGetYear(sz_date),wociGetMonth(sz_date),wociGetDay(sz_date),wociGetMonth(sz_date));
                                            break;
                                        case 16: // minute
                                            sprintf(_tmp,"%04d%02d%02d%02d%02d",wociGetYear(sz_date),wociGetMonth(sz_date),wociGetDay(sz_date),wociGetMonth(sz_date),wociGetMin(sz_date));
                                            break;
                                        default:
                                            sprintf(_tmp,"%04d%02d%02d%02d%02d%02d",
                                                    wociGetYear(sz_date),
                                                    wociGetMonth(sz_date),
                                                    wociGetDay(sz_date),
                                                    wociGetHour(sz_date),
                                                    wociGetMin(sz_date),
                                                    wociGetSec(sz_date));
                                            break;
                                    }
                                }
                            }

                            memcpy(p_mem_sort_key+p_mem_sort_key_pos,_tmp,strlen(_tmp));
                            p_mem_sort_key_pos += strlen(_tmp);

                            break;
                            default:
                                ThrowWith("Invalid column type :%d,id:%d",pCur->getColType(idx_col),idx_col);
                        }
                    }

                    // 3.2> �������´������������Ϣ
                    row_data.Clear();
                    int fret = 0;
                    for(int idx_col=0; idx_col < dp_parallel_load_columns; idx_col++) {

                        // ��������NULLλͼ���ֺ����ݲ��ֹ���
                        int _len = 0;
                        if(wociIsNull(*pCur, idx_col, idx_row)) { // ����NULL bit
                            fret = row_data.SetNull(idx_col);
                        } else {
                            switch(pCur->getColType(idx_col)) {
                                case COLUMN_TYPE_CHAR:
                                    fret = row_data.SetString(idx_col,pCur->PtrStr(idx_col,idx_row),
                                                              strlen(pCur->PtrStr(idx_col,idx_row)));
                                    break;

                                case COLUMN_TYPE_FLOAT:
                                    fret = row_data.SetDouble(idx_col,*pCur->PtrDouble(idx_col,idx_row));
                                    break;

                                case COLUMN_TYPE_INT:
                                    fret = row_data.SetInt64(idx_col,*pCur->PtrInt(idx_col,idx_row));
                                    break;

                                case COLUMN_TYPE_BIGINT:
                                    fret = row_data.SetInt64(idx_col,*pCur->PtrLong(idx_col,idx_row));
                                    break;

                                case COLUMN_TYPE_DATE:
                                    pCur->GetDate(idx_col,idx_row,sz_date);

#if 0
                                    sprintf(_tmp,"%04d%02d%02d%02d%02d%02d",
                                            wociGetYear(sz_date),
                                            wociGetMonth(sz_date),
                                            wociGetDay(sz_date),
                                            wociGetHour(sz_date),
                                            wociGetMin(sz_date),
                                            wociGetSec(sz_date));

                                    fret = row_data.SetDateTime(idx_col,_tmp);
#else
                                    DM_IB_RowData_NS::ib_datetime_t ib_datetime_tmp;
                                    ib_datetime_tmp.union_v.int_v = 0;
                                    ib_datetime_tmp.fields.year = wociGetYear(sz_date);
                                    ib_datetime_tmp.fields.month = wociGetMonth(sz_date);
                                    ib_datetime_tmp.fields.day = wociGetDay(sz_date);
                                    ib_datetime_tmp.fields.hour = wociGetHour(sz_date);
                                    ib_datetime_tmp.fields.minute = wociGetMin(sz_date);
                                    ib_datetime_tmp.fields.second = wociGetSec(sz_date);
                                    fret = row_data.SetDateTime(idx_col,ib_datetime_tmp.union_v.int_v);
#endif

                                    break;

                                default:
                                    ThrowWith("Invalid column type :%d,id:%d",pCur->getColType(idx_col),idx_col);
                            }
                        }

                        if(fret) {
                            lgprintf("ִ�з��ش���[%s].",CR_Class_NS::strerrno(fret));
                            ThrowWith("DM_IB_RowData::setdata row[%d] col[%d] return %d \n",idx_row,idx_col,fret);
                        }
                    }

                    // ��ȡ���ݺͳ���
                    const void *row_data_data;
                    size_t row_data_size;
                    fret = row_data.Data(row_data_data, row_data_size);
                    if (fret) {
                        lgprintf("ִ�з��ش���[%s].",CR_Class_NS::strerrno(fret));
                        ThrowWith("DM_IB_RowData::Data row[%d] return %d \n",idx_row,fret);
                    }

                    st_parallel_load_rows++;
                    parallel_load_obj_id++;

                    // 3.3> ��ֲ�ʽ����Ⱥ���ͺ�������
                    if(1) {
                        std::string errmsg="";

                        // ͨ���ڵ�ID(��8bit)+��¼��(��56bit)����
                        unsigned long _push_obj_id = generate_obj_id();

                        ProcBlockPush.Start();
#ifndef THREAD_PUSH
                        ret = lines.PushBack(_push_obj_id, p_mem_sort_key,p_mem_sort_key_pos,row_data_data,row_data_size);
#else
                        ret = plines->PushBack(_push_obj_id,p_mem_sort_key,p_mem_sort_key_pos,row_data_data,row_data_size);
#endif
                        if (ret == ENOBUFS) {

#ifndef THREAD_PUSH
                            // �̻Ựid���ύ����
                            //lgprintf("tabid(%d),datapartid(%d),slaver_index(%d),ProcBlock DMIBSortLoader->PushLines start.",tabid,datapartid,slaver_index);
                            ret = p_stDMIBSortLoader->PushLines(lines, &errmsg);
                            //lgprintf("tabid(%d),datapartid(%d),slaver_index(%d),ProcBlock DMIBSortLoader->PushLines end.",tabid,datapartid,slaver_index);
                            if(ret) {
                                lgprintf("ִ�з��ش���[%s].",CR_Class_NS::strerrno(ret));
                                ThrowWith("������Ⱥ[%s]���������ʧ��,������Ϣ[%s],�������[%d].",stsessionid,errmsg.c_str(),ret);
                            }
                            lines.Clear();
#else
                            fixed_linear_storage_queue_mutex.Lock();
                            fixed_linear_storage_queue.push(plines);
                            fixed_linear_storage_queue_mutex.Unlock();
#endif
                            //lgprintf("tabid(%d),datapartid(%d),slaver_index(%d),CR_FixedLinearStorage::PushBack(%ld) ��¼����(%d) return ENOBUFS,�л���һƬCR_FixedLinearStorage.",tabid,datapartid,slaver_index,CR_FixedLinearStorage_size,push_line_nums);
                            push_line_nums=0;
                            // ����һƬ֮��,��Ҫ����һ���µ�ID
                            parallel_load_obj_id++;
                            _push_obj_id = generate_obj_id();

#ifdef THREAD_PUSH
                            CR_FixedLinearStorage *lines = new CR_FixedLinearStorage(CR_FixedLinearStorage_size);
                            plines= lines;
                            ret = plines->PushBack(_push_obj_id,p_mem_sort_key,p_mem_sort_key_pos,row_data_data,row_data_size);
#else
                            ret = lines.PushBack(_push_obj_id,p_mem_sort_key,p_mem_sort_key_pos,row_data_data,row_data_size);
#endif
                            if(ret !=0) {
                                lgprintf("ִ�з��ش���[%s].",CR_Class_NS::strerrno(ret));
                                ThrowWith("������Ⱥ[%s]���������ʧ��,push_obj_id[%ld].",stsessionid,_push_obj_id);
                            }

                            continue;
                        } else if(ret) {
                            lgprintf("ִ�з��ش���[%s].",CR_Class_NS::strerrno(ret));
                            ThrowWith("������Ⱥ[%s]���������ʧ��,������Ϣ[%s],�������[%d].",stsessionid,errmsg.c_str(),ret);
                        }

                        push_line_nums++;

                        ProcBlockPush.Stop();
                    }
                }

                delete [] p_mem_sort_key;
                p_mem_sort_key = NULL;

                lgprintf("tabid(%d),datapartid(%d),slaver_index(%d),�����ֲ�ʽ����Ⱥ[%s]����[%d]��[%d]�е�����.",tabid,datapartid,slaver_index,stsessionid,pCur->GetRows(),pCur->GetColumnNum());
            } else {
#ifdef THREAD_PUSH
                delete lines;
                lines = NULL;
#endif
            }
        } catch(const char* emsg) {
            // fix dma-1988/dma-1928 by liujs
            fixed_linear_storage_queue_mutex.Lock();
            fixed_linear_storage_queue_run_flag = false;
            fixed_linear_storage_queue_mutex.Unlock();
            ThrowWith(emsg);
        } catch(...) {
            // fix dma-1988/dma-1928 by liujs
            fixed_linear_storage_queue_mutex.Lock();
            fixed_linear_storage_queue_run_flag = false;
            fixed_linear_storage_queue_mutex.Unlock();
            ThrowWith("tabid(%d),datapartid(%d),slaver_index(%d),�����ֲ�ʽ����Ⱥ[%s]��������ʧ��,�׳��쳣.",tabid,datapartid,slaver_index,stsessionid);
        }

        // 4. ������������Ҫ��������ݶ�������ɺ�,�ȴ��������,���ú��� DataDump::commit_sort_data
        if(pCur->GetRows()>0) {
            std::string errmsg="";
            // �̻Ự�����ύ

            ProcBlockPush.Start();
#ifndef THREAD_PUSH
            lgprintf("tabid(%d),datapartid(%d),slaver_index(%d),ProcBlock DMIBSortLoader->PushLines start.",tabid,datapartid,slaver_index);
            ret = p_stDMIBSortLoader->PushLines(lines, &errmsg);
            lgprintf("tabid(%d),datapartid(%d),slaver_index(%d),ProcBlock DMIBSortLoader->PushLines start.",tabid,datapartid,slaver_index);
#else
            fixed_linear_storage_queue_mutex.Lock();
            fixed_linear_storage_queue.push(plines);
            fixed_linear_storage_queue_run_flag = false;
            fixed_linear_storage_queue_mutex.Unlock();

            pushThread.WaitAll();
            ret = pushThread.GetReturnValue();
#endif
            if(ret != 0) {
                // ���ü�Ⱥ״̬δ����

#ifdef THREAD_PUSH
                // �����ڴ�
                fixed_linear_storage_queue_mutex.Lock();
                lgprintf("Before clear [%s] fixed_linear_storage_queue size:%d.",stsessionid,fixed_linear_storage_queue.size());

                // delete by liujs : the following code exist crash while invoking  CR_FixedLinearStorage::~CR_FixedLinearStorage/clear , more detail DMA-1873
                while(!fixed_linear_storage_queue.empty()) {
                    CR_FixedLinearStorage* pobj =  fixed_linear_storage_queue.front();
                    if(pobj!=NULL) {
                        // pobj->Clear();
                        DPRINTF("delete pobj=[%p]\n",pobj);
                        delete pobj;
                        pobj=NULL;
                    }
                    fixed_linear_storage_queue.pop();
                }


                lgprintf("After clear [%s] fixed_linear_storage_queue size:%d.",stsessionid,fixed_linear_storage_queue.size());
                fixed_linear_storage_queue_mutex.Unlock();
#endif

                set_cluster_status(sp.GetDTS(),tabid,datapartid,stsessionid,CS_ERROR);
                ThrowWith("������Ⱥ[%s]���������ʧ��,������Ϣ[%s],�������[%d].",stsessionid,errmsg.c_str(),ret);
            }

            ProcBlockPush.Stop();
        } else {
#ifdef THREAD_PUSH
            fixed_linear_storage_queue_mutex.Lock();
            fixed_linear_storage_queue_run_flag = false;
            fixed_linear_storage_queue_mutex.Unlock();
            pushThread.WaitAll();
            ret = pushThread.GetReturnValue();
            if(ret !=0) {

                // �����ڴ�
                fixed_linear_storage_queue_mutex.Lock();

                lgprintf("Before clear [%s] fixed_linear_storage_queue size:%d.",stsessionid,fixed_linear_storage_queue.size());

                // delete by liujs : the following code exist crash while invoking  CR_FixedLinearStorage::~CR_FixedLinearStorage/clear , more detail DMA-1873
                while(!fixed_linear_storage_queue.empty()) {
                    CR_FixedLinearStorage* pobj =  fixed_linear_storage_queue.front();
                    if(pobj!=NULL) {
                        DPRINTF("delete pobj=[%p]\n",pobj);
                        // pobj->Clear();
                        delete pobj;
                        pobj=NULL;
                    }
                    fixed_linear_storage_queue.pop();
                }

                lgprintf("After clear [%s] fixed_linear_storage_queue size:%d.",stsessionid,fixed_linear_storage_queue.size());
                fixed_linear_storage_queue_mutex.Unlock();

                // ���ü�Ⱥ״̬δ����
                set_cluster_status(sp.GetDTS(),tabid,datapartid,stsessionid,CS_ERROR);
                ThrowWith("������Ⱥ[%s]���������ʧ��,�������[%d].",stsessionid,ret);
            }
#endif
        }


        ProcBlockAll.Stop();
        double tm_end = ProcBlockAll.GetTime();
        ProcBlockAll.Clear();
        lgprintf("tabid(%d),datapartid(%d),slaver_index(%d),stsessionid(%s),DataDump::ProcBlock ����%d������,�ۼ�ʹ��ʱ��(%0.4f)��(��ȷ����,����sleep�Ⱥ�ʱ).",tabid,datapartid,slaver_index,stsessionid,pCur->GetRows(),tm_end);

        tm_end = ProcBlockPush.GetTime();
        lgprintf("tabid(%d),datapartid(%d),slaver_index(%d),stsessionid(%s),DataDump::ProcBlock ��Ⱥ����%d������,�ۼ�ʹ��ʱ��(%0.4f)��(��ȷ����,����sleep�Ⱥ�ʱ).",tabid,datapartid,slaver_index,stsessionid,pCur->GetRows(),tm_end);
        ProcBlockPush.Clear();

    } else { // ��ԭ����,�����зֲ�ʽ����
        freeinfo1("Start ProcBlock");
        int fid=BuildMiddleFilePath(_fid);
        blockmt.Reset();
        int cur=*pCur;
        char *idxcolsname=dp.idxp[idx].idxcolsname;
        int *ikptr=NULL;
        int strow=-1;
        int subrn=0;
        int blockrn=0;
        if(maxblockrn<MIN_BLOCKRN) {
            sp.log(dp.tabid,partid,DUMP_DST_TABLE_DATA_BLOCK_SIZE_ERROR,"��%d��Ŀ�������ݿ��С(%d)�����ʣ�����Ϊ%d.",dp.tabid,maxblockrn,MIN_BLOCKRN);
            ThrowWith("Ŀ�������ݿ��С(%d)�����ʣ�����Ϊ%d.",maxblockrn,MIN_BLOCKRN);
        }
        if(maxblockrn>MAX_BLOCKRN) {
            sp.log(dp.tabid,partid,DUMP_DST_TABLE_DATA_BLOCK_SIZE_ERROR,"��%d��Ŀ�������ݿ��С(%d)�����ʣ����ܳ���%d.",dp.tabid,maxblockrn,MAX_BLOCKRN);
            ThrowWith("Ŀ�������ݿ��С(%d)�����ʣ����ܳ���%d.",maxblockrn,MAX_BLOCKRN);
        }
        AutoMt idxdt(0,10);
        wociCopyColumnDefine(idxdt,cur,idxcolsname);
        wociAddColumn(idxdt,"idx_blockoffset","",COLUMN_TYPE_LONG,0,0);
        wociAddColumn(idxdt,"idx_startrow","",COLUMN_TYPE_INT,0,0);
        wociAddColumn(idxdt,"idx_rownum","",COLUMN_TYPE_INT,0,0);
        wociAddColumn(idxdt,"idx_fid","",COLUMN_TYPE_INT,0,0);
        int idxrnlmt=max(FIX_MAXINDEXRN/wociGetRowLen(idxdt),2);
        idxdt.SetMaxRows(idxrnlmt);
        idxdt.Build();
        freeinfo1("After Build indxdt mt");
        void *idxptr[20];
        int pidxc1[10];
        bool pkmode=false;
        sorttm.Start();
        int cn1=wociConvertColStrToInt(cur,idxcolsname,pidxc1);
        //����PKģʽ��ȫ������ͨģʽ����
        //if(cn1==1 && wociGetColumnType(cur,pidxc1[0])==COLUMN_TYPE_INT)
        //  pkmode=true;
        if(!pkmode) {
            wociSetSortColumn(cur,idxcolsname);
            wociSortHeap(cur);
        } else {
            wociSetIKByName(cur,idxcolsname);
            wociOrderByIK(cur);
            wociGetIntAddrByName(cur,idxcolsname,0,&ikptr);
        }
        sorttm.Stop();
        long idx_blockoffset=0;
        int idx_store_size=0,idx_startrow=0,idx_rownum=0;
        int idxc=cn1;
        idxptr[idxc++]=&idx_blockoffset;
        //  idxptr[idxc++]=&idx_store_size;
        idxptr[idxc++]=&idx_startrow;
        idxptr[idxc++]=&idx_rownum;
        idxptr[idxc++]=&fid;
        idxptr[idxc]=NULL;
        try {
            dt_file df;
            df.Open(tmpfn,1,fid);
            idx_blockoffset=df.WriteHeader(cur);
            dt_file di;
            di.Open(tmpidxfn,1);
            di.WriteHeader(idxdt,wociGetMemtableRows(idxdt));
            idxdt.Reset();
            int totidxrn=0;
            int rn=wociGetMemtableRows(cur);
            adjtm.Start();
            for(int i=0; i<rn; i++) {
                int thisrow=pkmode?wociGetRawrnByIK(cur,i):wociGetRawrnBySort(cur,i);
                //int thisrow=wociGetRawrnByIK(cur,i);
                if(strow==-1) {
                    strow=thisrow;
                    idx_startrow=blockrn;
                }
                //�ӿ�ָ�
                else if(pkmode?(ikptr[strow]!=ikptr[thisrow]):
                        (wociCompareSortRow(cur,strow,thisrow)!=0) ) {
                    //if(ikptr[strow]!=ikptr[thisrow]) {
                    for(int c=0; c<cn1; c++) {
                        if(pCur->isVarStrCol(pidxc1[c])) {
                            int nLen = 0;
                            idxptr[c] =(void *)pCur->GetVarLenStrAddr(pidxc1[c],strow,&nLen);
                        } else {
                            idxptr[c]=pCur->PtrVoidNoVar(pidxc1[c],strow);
                        }
                    }
                    idx_rownum=blockrn-idx_startrow;
                    wociInsertRows(idxdt,idxptr,NULL,1);
                    totidxrn++;
                    int irn=wociGetMemtableRows(idxdt);
                    if(irn>idxrnlmt-2) {
                        int *pbo=idxdt.PtrInt("idx_blockoffset",0);
                        int pos=irn-1;
                        while(pos>=0 && pbo[pos]==idx_blockoffset) pos--;
                        if(pos>0) {
                            di.WriteMt(idxdt,COMPRESSLEVEL,pos+1,false);
                            if(pos+1<irn)
                                wociCopyRowsTo(idxdt,idxdt,0,pos+1,irn-pos-1);
                            else wociReset(idxdt);
                        } else {
                            sp.log(dp.tabid,partid,DUMP_INDEX_BLOCK_SIZE_ERROR,"��%d,����Ԥ�������,������%d,�����ֶ�'%s',�������������鳤��%d.",dp.tabid,partid,idxcolsname,MAX_BLOCKRN);
                            ThrowWith("����Ԥ�������,������%d,�����ֶ�'%s',�������������鳤��%d.",partid,idxcolsname,MAX_BLOCKRN);
                        }
                    }
                    strow=thisrow;
                    idx_startrow=blockrn;
                }
                //blockmt.QuickCopyFrom(pcur,blockrn,thisrow);
                wociCopyRowsTo(cur,blockmt,-1,thisrow,1);
                blockrn++;//=wociGetMemtableRows(blockmt);
                //����ӿ�ķָ�
                if(blockrn>=maxblockrn) {
                    adjtm.Stop();
                    fiotm.Start();
                    for(int c=0; c<cn1; c++) {
                        if(pCur->isVarStrCol(pidxc1[c])) {
                            int nLen = 0;
                            idxptr[c] =(void *)pCur->GetVarLenStrAddr(pidxc1[c],strow,&nLen);
                        } else {
                            idxptr[c]=pCur->PtrVoidNoVar(pidxc1[c],strow);
                        }
                    }
                    idx_rownum=blockrn-idx_startrow;
                    wociInsertRows(idxdt,idxptr,NULL,1);
                    totidxrn++;
                    int irn=wociGetMemtableRows(idxdt);
                    if(irn>idxrnlmt-2) {
                        int *pbo=idxdt.PtrInt("idx_blockoffset",0);
                        int pos=irn-1;
                        while(pos>=0 && pbo[pos]==idx_blockoffset) pos--;
                        if(pos>0) {
                            di.WriteMt(idxdt,COMPRESSLEVEL,pos+1,false);
                            if(pos+1<irn)
                                wociCopyRowsTo(idxdt,idxdt,0,pos+1,irn-pos-1);
                            else wociReset(idxdt);
                        } else {
                            sp.log(dp.tabid,partid,DUMP_INDEX_BLOCK_SIZE_ERROR,"��%d,����Ԥ�������,������%d,�����ֶ�'%s',�������������鳤��%d.",dp.tabid,partid,idxcolsname,MAX_BLOCKRN);
                            ThrowWith("����Ԥ�������,������%d,�����ֶ�'%s',�������������鳤��%d.",partid,idxcolsname,MAX_BLOCKRN);
                        }
                    }
                    idx_blockoffset=df.WriteMt(blockmt,COMPRESSLEVEL,0,false);
                    idx_startrow=0;
                    strow=-1;
                    blockrn=0;
                    blockmt.Reset();
                    fiotm.Stop();
                    adjtm.Start();
                }
            }
            adjtm.Stop();
            //�������Ŀ�����
            if(blockrn) {
                for(int c=0; c<cn1; c++) {
                    if(pCur->isVarStrCol(pidxc1[c])) {
                        int nLen = 0;
                        idxptr[c] =(void *)pCur->GetVarLenStrAddr(pidxc1[c],strow,&nLen);
                    } else {
                        idxptr[c]=pCur->PtrVoidNoVar(pidxc1[c],strow);
                    }
                }
                idx_rownum=blockrn-idx_startrow;
                wociInsertRows(idxdt,idxptr,NULL,1);
                totidxrn++;
                idx_blockoffset=df.WriteMt(blockmt,COMPRESSLEVEL,0,false);
                idx_startrow=0;
                strow=-1;
                blockrn=0;
                blockmt.Reset();
            }

            //������������
            {
                di.WriteMt(idxdt,COMPRESSLEVEL,0,false);
                di.SetFileHeader(totidxrn,NULL);
            }
            //�����ļ�����
            {
                void *ptr[20];
                ptr[0]=&fid;
                ptr[1]=&partid;
                ptr[2]=&dp.tabid;
                int rn=df.GetRowNum();
                long fl=df.GetLastOffset();
                ptr[3]=&rn;
                ptr[4]=&fl;
                char now[10];
                wociGetCurDateTime(now);
                ptr[5]=tmpfn;
                ptr[6]=tmpidxfn;
                ptr[7]=now;
                int state=filesmode?0:1;
                ptr[8]=&state;
                char nuldt[10];
                memset(nuldt,0,10);
                ptr[9]=now;//nuldt;
                ptr[10]=&dp.idxp[idx].idxid;
                ptr[11]=dp.idxp[idx].idxcolsname;
                ptr[12]=dp.idxp[idx].idxreusecols;
                int blevel=0;
                ptr[13]=&blevel;
                ptr[14]=NULL;
                wociInsertRows(fnmt,ptr,NULL,1);
            }
        } catch(...) {
            unlink(tmpidxfn);
            unlink(tmpfn);
            throw;
        }
        freeinfo1("End ProcBlock");
    }
}

/* �����ӷ����Ĺ���������ʵ�� */
// Jira DMA102
//����ֵ
// 0: û������
// 1:��������
// 2: �����쳣
// Jira 124: �Զ����������ڵı�
int MiddleDataLoader::AutoAddPartition(SysAdmin &sp)
{
    /* �����Ҫ�Զ����ӵķ��� */
    printf("����Զ����ӷ���...\n");
    int ret=0;
    try {
        if(!wociTestTable(sp.GetDTS(),"dp.dp_notify_ext")) return 0;
        AutoMt mt(sp.GetDTS(),10);
        mt.FetchAll("select * from dp.dp_notify_ext limit 100");
        int extrn=mt.Wait();
        if(extrn<1) return 0;
        for(int ern=0; ern<extrn;) {
            AutoHandle extdb;
            extdb.SetHandle(sp.BuildSrcDBCByID(mt.GetInt("dbsysid",ern)));
            AutoMt newpart(extdb,10);
            newpart.FetchFirst("select * from %s where imp_status=0",mt.PtrStr("tabname",ern));
            if(newpart.Wait()<1) {
                ern++;
                continue;
            }
            AutoStmt st(extdb);
            if(strlen(newpart.PtrStr("NEW_PARTFULLNAME",0))<1) {
                if(st.DirectExecute("update %s set imp_status=11 where new_tabname_indm='%s' and new_dbname_indm='%s'"
                                    ,mt.PtrStr("tabname",ern),newpart.PtrStr("NEW_TABNAME_INDM",0),newpart.PtrStr("NEW_DBNAME_INDM",0))!=1)
                    ern++;
                lgprintf("�½�����NEW_PARTFULLNAME�ֶβ���Ϊ�գ�");
                ret=2;
                continue;
            }

            AutoMt tabinfo(sp.GetDTS(),10);
            tabinfo.FetchAll("select * from dp.dp_table where lower(tabname)=lower('%s') and lower(databasename)=lower('%s')"
                             ,newpart.PtrStr("NEW_TABNAME_INDM",0),newpart.PtrStr("NEW_DBNAME_INDM",0));
            if(tabinfo.Wait()<1) {
                lgprintf("���ӷ����ı�û�ж��� %s.%s ,��ʼ���� ... ",newpart.PtrStr("NEW_DBNAME_INDM",0),newpart.PtrStr("NEW_TABNAME_INDM",0));
                if(st.DirectExecute("update %s set imp_status=4 where new_tabname_indm='%s' and new_dbname_indm='%s' and NEW_PARTFULLNAME='%s'"
                                    ,mt.PtrStr("tabname",ern),newpart.PtrStr("NEW_TABNAME_INDM",0),newpart.PtrStr("NEW_DBNAME_INDM",0),newpart.PtrStr("NEW_PARTFULLNAME",0))!=1)  {
                    lgprintf("�½������쳣,���������������ڴ���");
                    ret=2;
                    continue;
                }
                int tabid=CreateLike(
                              (const char *)newpart.PtrStr("BASE_DMDBNAME",0),(const char *)newpart.PtrStr("BASE_DMTABNAME",0),
                              (const char *)newpart.PtrStr("NEW_DBNAME",0),(const char *)newpart.PtrStr("NEW_TABNAME",0),
                              (const char *)newpart.PtrStr("NEW_DBNAME_INDM",0),(const char *)newpart.PtrStr("NEW_TABNAME_INDM",0),
                              "now",true);
                AutoStmt stdm(sp.GetDTS());
                //���·�����Ϣ
                if(stdm.DirectExecute("update dp.dp_datapart set set partfullname='%s', extsql='select * from %s.%s %s' where tabid=%d and datapartid=1",
                                      newpart.PtrStr("NEW_PARTFULLNAME",0),
                                      newpart.PtrStr("NEW_DBNAME",0),newpart.PtrStr("NEW_TABNAME",0),newpart.PtrStr("NEW_PARTFULLNAME",0),tabid)!=1) {
                    if(st.DirectExecute("update %s set imp_status=11 where new_tabname_indm='%s' and new_dbname_indm='%s' and NEW_PARTFULLNAME='%s'"
                                        ,mt.PtrStr("tabname",ern),newpart.PtrStr("NEW_TABNAME_INDM",0),newpart.PtrStr("NEW_DBNAME_INDM",0),newpart.PtrStr("NEW_PARTFULLNAME",0))!=1)
                        ern++;
                    lgprintf("�½������쳣,���·�����Ϣ(dp.dp_datapart.partfullname)ʧ��,��%d",tabid);
                    ret=2;
                    continue;
                }
                //lgprintf("���ӷ����ı�û�ж��� %s.%s ",newpart.PtrStr("NEW_DBNAME_INDM",0),newpart.PtrStr("NEW_TABNAME_INDM",0));
                continue;
            }
            int tabid=tabinfo.GetInt("tabid",0);
            tabinfo.FetchFirst("select * from dp.dp_datapart where tabid=%d and lower(extsql) like lower(' %%%s%% ')",
                               tabid,newpart.PtrStr("NEW_PARTFULLNAME",0));
            if(tabinfo.Wait()>0) {
                if(st.DirectExecute("update %s set imp_status=11 where new_tabname_indm='%s' and new_dbname_indm='%s'"
                                    ,mt.PtrStr("tabname",ern),newpart.PtrStr("NEW_TABNAME_INDM",0),newpart.PtrStr("NEW_DBNAME_INDM",0))!=1)
                    ern++;
                lgprintf("���ӵķ����Ѿ����� %s.%s (%d)",newpart.PtrStr("NEW_DBNAME_INDM",0),newpart.PtrStr("NEW_TABNAME_INDM",0),tabid);
                ret=2;
                continue;
            }
            tabinfo.FetchFirst("select max(datapartid) mdp,max(tmppathid) tmppathid from dp.dp_datapart where tabid=%d ",tabid);
            if(tabinfo.Wait()<1) {
                if(st.DirectExecute("update %s set imp_status=11 where new_tabname_indm='%s' and new_dbname_indm='%s'"
                                    ,mt.PtrStr("tabname",ern),newpart.PtrStr("NEW_TABNAME_INDM",0),newpart.PtrStr("NEW_DBNAME_INDM",0))!=1)
                    ern++;
                lgprintf("���ӵķ���û��Ԥ������� %s.%s (%d)",newpart.PtrStr("NEW_DBNAME_INDM",0),newpart.PtrStr("NEW_TABNAME_INDM",0),tabid);
                ret=2;
                continue;
            }
            int datapartid=tabinfo.GetInt("mdp",0)+1;
            int tmppathid=tabinfo.GetInt("tmppathid",0);
            if(st.DirectExecute("update %s set imp_status=4 where new_tabname_indm='%s' and new_dbname_indm='%s' and NEW_PARTFULLNAME='%s'"
                                ,mt.PtrStr("tabname",ern),newpart.PtrStr("NEW_TABNAME_INDM",0),newpart.PtrStr("NEW_DBNAME_INDM",0),newpart.PtrStr("NEW_PARTFULLNAME",0))!=1)  {
                lgprintf("�½������쳣,���������������ڴ���");
                ret=2;
                continue;
            }
            AutoStmt stdm(sp.GetDTS());
            stdm.DirectExecute("insert into dp.dp_datapart(datapartid,begintime,status,partfullname,partdesc,tabid,tmppathid,compflag,srcsysid,extsql) "
                               " values (%d,now(),0,'%s','%s',%d,%d,1,%d,'select * from %s.%s %s')",
                               datapartid,newpart.PtrStr("NEW_PARTFULLNAME",0),newpart.PtrStr("NEW_PARTFULLNAME",0),tabid,tmppathid,newpart.GetInt("DBSID_INDM",0),
                               newpart.PtrStr("NEW_DBNAME",0),newpart.PtrStr("NEW_TABNAME",0),newpart.PtrStr("NEW_PARTFULLNAME",0));
            lgprintf("���½���������%d ",tabid);
            ret=1;
        } // end for
    } // end try
    catch(...) {
        lgprintf("�½������쳣��");
        return 2;
    }
    return ret;
}

//����ֵ
//0 :û����Ҫ����ı�
//1 :��⵽��Ҫ׷�����ݵı�
//2 :û�м�⵽��Ҫ׷�����ݵı�
// DMA-112
int DataDump::CheckAddFiles(SysAdmin &sp)
{
    // Move to MiddleDataLoader and call by dt_check.cpp
    //int addp=AutoAddPartition(sp);
    AutoMt mdf(sp.GetDTS(),MAX_DST_DATAFILENUM);
    mdf.FetchAll("select count(1) ct from dp.dp_datapart "
                 "where status=5 and lower(extsql) like 'load %%' and begintime<now() and endtime>now() %s "
                 " order by blevel,touchtime,tabid,datapartid ",sp.GetNormalTaskDesc ());
    mdf.Wait();
    long ct=mdf.GetLong("ct",0);
    if(ct>0) {
        printf("��%d������Ҫ��������ļ���¼.\n",ct);
        mdf.FetchAll("select * from dp.dp_datapart "
                     "where status=5 and lower(extsql) like 'load %%' and begintime<now() and endtime>now() %s "
                     " order by blevel,touchtime,tabid,datapartid limit 2",sp.GetNormalTaskDesc ());
        mdf.Wait();
        char libname[300];
        char dumpsqlTemp[250];
        memset(dumpsqlTemp,0,250);
        strcpy(dumpsqlTemp,mdf.PtrStr("extsql",0));
        char *dumpsql=dumpsqlTemp;
        //>> Begin:fix jira dma-470,dma-471,dma-472,20130121
        RemoveContinueSpace(dumpsql);
        //<< End

        int tabid=mdf.GetInt("tabid",0);
        int datapartid=mdf.GetInt("datapartid",0);
        if(strcasestr(dumpsql,"load data from files")!=NULL) {
            char *plib=strcasestr(dumpsql,"using ");
            if(!plib) {
                sp.log(tabid,datapartid,DUMP_FILE_ERROR,"��%d,����%d,�ļ���������ȱ��using�ؼ���.",tabid,datapartid);
                ThrowWith("�ļ���������ȱ��using�ؼ���.");
            }
            plib+=strlen("using ");
            strcpy(libname,plib);
            plib=libname;
            //end by blank or null
            while(*plib) {
                if(*plib==' ') {
                    *plib=0;
                    break;
                } else plib++;
            }

            //>> Begin: DM-201 , modify by liujs
            IDumpFileWrapper *dfw;
            IFileParser *fparser;
            //<< End: modify by liujs
            dfw=new DumpFileWrapper(libname);
            fparser=dfw->getParser();
            fparser->SetTable(dtdbc,tabid,datapartid,dfw);
            int gfstate=fparser->GetFile(false,sp,tabid,datapartid,true);
            delete dfw;
            AutoStmt st(dtdbc);
            if(gfstate!=0) {
                st.DirectExecute("update dp.dp_datapart set status=73,blevel=0 where tabid=%d and datapartid=%d",tabid,datapartid);
                lgprintf("��%d��Ҫ׷�����ݡ�",tabid);
                sp.log(tabid,datapartid,DUMP_BEGIN_DUMPING_NOTIFY,"��%d,����%d,��ʼ׷������",tabid,datapartid);
                return 1;
            }
            //�´β�������������ͬһ������ѭ��
            if(ct>1)
                st.DirectExecute("update dp.dp_datapart set blevel=blevel+1 where tabid=%d and datapartid=%d",
                                 tabid,datapartid);
            return 2;
        } else if(strcasestr(dumpsql,"load data")!=NULL) {
            sp.log(tabid,datapartid,DUMP_FILE_ERROR,"��%d,����%d,��ʽ����'%s',��ʹ��'load data from files using xxx\n%s.",tabid,datapartid,dumpsql);
            ThrowWith("��ʽ����'%s',��ʹ��'load data from files using xxx\n%s.",dumpsql);
        }
    }
    return 0;
}

void PickThread(ThreadList *tl)
{
    void **params=tl->GetParams();
    SysAdmin &plugin_sp=*(SysAdmin *)params[0];
    IFileParser *fparser=(IFileParser *)params[1];
    bool &filehold=*(bool *)params[2];
    int &datapartid=*(int*)params[3];
    TradeOffMt &dtmt=*(TradeOffMt *)params[4];
    bool withbackup=(bool) params[5];
    int &tabid=*(int*)params[6];
    int &uncheckbct=*(int *)params[7];
    int &uncheckfct=*(int *)params[8];

    bool &_dp_parallel_load = *(bool*)params[9];

    char* _stsessionid=(char*)params[10];
    char* _ltsessionid=(char*)params[11];
    int &_slaver_index=*(int *)params[12];
    Mutex *_session_lock=(Mutex*)params[13];
    mytimer2 *_sorttm=(mytimer2*)params[14];
    int &_st_session_interval=*(int *)params[15];
    bool &_extractend = *(bool*)params[16];
    long &_threadmtrows =*(long*)params[17];

    if(_dp_parallel_load) {
        // ������ܻ���лỰ����
        AutoLock ak(_session_lock);
        fparser->SetSession(_slaver_index);
    }

    //Get file and parser
    //����ϴε��ļ�δ������(�ڴ����),���������ļ��������ֽ����
    while(true) {
        if(!filehold) {
            int gfstate=0;

            // ��ȡ�ļ�
            gfstate = fparser->GetFile(withbackup,plugin_sp,tabid,datapartid);

            if(gfstate==0) {

                //û��������Ҫ����
                if(fparser->ExtractEnd(plugin_sp.GetDTS())) {
                    if(_dp_parallel_load) {
                        _extractend = true;
                    }

                    if(_dp_parallel_load) {
                        lgprintf("��(%d),����(%d),slavercnt(%d) ��ȡ���ݽ���,�˳�PickThread.",tabid,datapartid,_slaver_index);
                    } else {
                        lgprintf("��(%d),����(%d,slavercnt(%d) ��ȡ���ݽ���,�˳�PickThread.",tabid,datapartid);
                    }
                    break;
                } else {
                    // ��ȡ�����ļ���,��ô��ʱ�Ͳ��õ���
                    if(_dp_parallel_load && _slaver_index>0) { // slaver �ڵ㲻�õ�
                        if(_st_session_interval>0 && _sorttm->GetTime() > _st_session_interval) {
                            _extractend = true;
                            lgprintf("��(%d),����(%d)��ȡ�����ļ�,�̻Ựʱ�䵽�뿪PickThread.",tabid,datapartid);
                            break;
                        }
                    }

                    if( _dp_parallel_load && _slaver_index == 0) { // master �ڵ�
                        if(fparser->GetForceEndDump() == 1) { // ֻ�вɼ�һ�����ݵ�ʱ��,���˳�
                            if(_st_session_interval>0 && _sorttm->GetTime() > 1*_st_session_interval) {
                                _extractend = true;  // master �ڵ㲻Ҫ�ȴ�
                                lgprintf("��(%d),����(%d)��ȡ�����ļ�,�̻Ựʱ�䵽�뿪PickThread A.",tabid,datapartid);
                                break;
                            }
                        } else {
                            if(_st_session_interval>0 && _sorttm->GetTime() > 1*_st_session_interval) {
                                _extractend = false; // master �ڵ�Ҫ�ȴ�
                                lgprintf("��(%d),����(%d)��ȡ�����ļ�,�̻Ựʱ�䵽�뿪PickThread B.",tabid,datapartid);
                                break;
                            }
                        }
                    }
                }

                if(_dp_parallel_load) {
                    _extractend = false;
                }
                mySleep(fparser->GetIdleSeconds());

                //>> begin: fix dma-1326: �����ļ��������,�ж��Ƿ��ύʱ��,������ύʱ���˾��ύ�ļ�
                if(_dp_parallel_load ) {
                    if(_sorttm->GetTime() > _st_session_interval &&
                       dtmt.Next()->GetRows() > 0) {
                        lgprintf("��(%d),����(%d)��ȡ�����ļ�,�̻Ựʱ�䵽�뿪PickThread.",tabid,datapartid);
                        break;
                    }
                }
                //<< end: fix dma-1326

                continue;
            } else if(gfstate==2) //�ļ����󣬵�Ӧ�ú���
                continue;

            uncheckfct++;
        }
        int frt=fparser->DoParse(*dtmt.Next(),plugin_sp,tabid,datapartid);
        filehold=false;
        if(frt==-1) {
            plugin_sp.log(tabid,datapartid,DUMP_FILE_ERROR,"��%d,����%d,�ļ��������.",tabid,datapartid);
            ThrowWith("�ļ��������.");
            break;
        } else if(frt==0) {
            //memory table is full,and file is not eof
            lgprintf("��(%d),����(%d)�ڴ���������ļ�������,�̻Ựʱ�䵽�뿪PickThread.",tabid,datapartid);
            filehold=true;
            break;
        } else if(frt==2) {
            //memory table is full and file is eof
            lgprintf("��(%d),����(%d)�ڴ���������ļ��������,�̻Ựʱ�䵽�뿪PickThread.",tabid,datapartid);
            break;
        }

        if(_dp_parallel_load) {
            AutoLock ak(_session_lock);
            _threadmtrows = dtmt.Next()->GetRows();
        }

        //>> begin: fix dma-1326: �����ļ��������,�ж��Ƿ��ύʱ��,������ύʱ���˾��ύ�ļ�
        if(_dp_parallel_load && frt== 1) {
            if(_st_session_interval>0 && _sorttm->GetTime() > _st_session_interval &&
               dtmt.Next()->GetRows() > 0) {
                lgprintf("��(%d),����(%d)�ڴ��δ�������ļ��������,�̻Ựʱ�䵽�뿪PickThread.",tabid,datapartid);
                break;
            }
        }
        //<< end: fix dma-1326

        double fillrate=(double)dtmt.Next()->GetRows()/dtmt.Next()->GetMaxRows();
        // 6��MTδ�ύ���ڴ�������볬��90%, ����9��MTδ�ύ
        if(!filehold && (uncheckbct>8 || (uncheckbct>5 && fillrate>0.9) )) {
            lgprintf("��(%d),����(%d)�ڴ��δ�������ļ��������,�̻Ựʱ�䵽�뿪PickThread.",tabid,datapartid);
            break;
        }
    }
    tl->SetReturnValue(dtmt.Next()->GetRows());
}


bool DataDump::is_all_slaver_node_finish(int dbc,int tabid,int datapartid,
        const char* st_session_id,
        int& launchcnt,int &finishcnt,
        int64& loadrows,int &clusterstatus)
{
    // �ж��Ƿ����е�slaver�ڵ㶼�Ѿ��������
    AutoMt mt(dbc,1000);
    mt.FetchFirst("select launchslavercnt,finishslavercnt,rownums,clusterstatus from dp.dp_datapartext where "
                  " tabid = %d and datapartid =%d and sessionid='%s'",
                  tabid,datapartid,st_session_id);

    int ret = mt.Wait();
    if(ret >0) {
        launchcnt = mt.GetInt("launchslavercnt",0);
        finishcnt = mt.GetInt("finishslavercnt",0);
        loadrows = mt.GetLong("rownums",0);
        clusterstatus = mt.GetInt("clusterstatus",0);

        return finishcnt == launchcnt;
    } else {

        lgprintf("�ָ�����tabid(%d),datapartid(%d)״̬Ϊ���µ���(0).",tabid,datapartid);
        AutoStmt st(dbc);
        st.DirectExecute("update dp.dp_datapart set status=0,launchmaster=0 where tabid=%d and datapartid=%d and status=1 and launchmaster=1",tabid,datapartid);

        ThrowWith("���������������̿��������������̳�ͻB,��%d(%d).",tabid,datapartid);
    }
    return false;
}


// ���ü�Ⱥ״̬
void DataDump::set_cluster_status(int dbc,int tabid,int datapartid,const char* session_id,const int cluster_status)
{
    AutoStmt st(dbc);
    lgprintf("���ñ�(%d),����(%d),�Ự(%s)�ļ�Ⱥ״̬Ϊ(%d).",tabid,datapartid,session_id,cluster_status);
    st.DirectExecute("update dp.dp_datapartext set clusterstatus = %d where tabid = %d and datapartid=%d and sessionid='%s'",
                     cluster_status,tabid,datapartid,session_id);
}

// ��ȡ��Ⱥ״̬
int  DataDump::get_cluster_status(int dbc,int tabid,int datapartid,const char* session_id)
{
    int clusterstatus = -1;
    AutoMt mt(dbc,1000);
    mt.FetchFirst("select clusterstatus from dp.dp_datapartext where "
                  " tabid = %d and datapartid =%d and sessionid='%s'",
                  tabid,datapartid,session_id);

    int ret = mt.Wait();
    assert(ret >0);
    if(ret >0) {
        clusterstatus = mt.GetInt("clusterstatus",0);
    }

    return clusterstatus;
}



bool DataDump::is_all_sorted_data_has_loaded(int dbc,int tabid,int & need_load_session,int64& loadrows)
{
    // �жϸñ��Ƿ�����������ɵ����ݶ��Ѿ��ύ�ϲ�װ��
    // select tabid,count(1) as  sessionnum,sum(rownums) as rownums from dp.dp_datapartext where tabid = 150634 and commitstatus in(0,1)
    AutoMt mt(dbc,1000);
    mt.FetchFirst("select tabid,count(1) as sessionnum,sum(rownums) as rownums from dp.dp_datapartext where tabid = %d and commitstatus in(0,1)",
                  tabid);

    int ret = mt.Wait();
    assert(ret >0);

    need_load_session = mt.GetLong("sessionnum",0);
    if(need_load_session >0) {
        loadrows = mt.GetLong("rownums",0);
    }

    return (need_load_session == 0) ? true:false;
}

// ��ȡ��İ������ֶ�����
int DataDump::get_packindex_column_list(SysAdmin &sp,int tabid,std::vector<std::string> & packidx_column_list)
{
    packidx_column_list.clear();

    AutoMt mdf(sp.GetDTS(),1000);
    mdf.FetchFirst("select a.* from dp.dp_column_info a,dp.dp_field_expand b where b.expand_id=a.expand_id and b.expand_name='packindex' and a.table_id=%d",tabid);
    int ret = mdf.Wait();
    if(ret == 0) {
        return 0;
    } else {
        for(int i=0; i<ret; i++) {
            packidx_column_list.push_back(std::string(mdf.PtrStr("column_name",i)));
        }
    }
    return packidx_column_list.size();
}

bool DataDump::column_is_packindex(const std::vector<std::string>  packidx_column_list,const std::string column_name)
{
    for(int i=0; i<packidx_column_list.size(); i++) {
        if(strcasecmp(packidx_column_list[i].c_str(),column_name.c_str()) == 0) {
            return true;
        }
    }
    return false;
}

int DataDump::get_snappycompress_column_list(SysAdmin &sp,int tabid,std::vector<std::string> & snappy_column_list)
{
    snappy_column_list.clear();

    AutoMt mdf(sp.GetDTS(),1000);
    mdf.FetchFirst("select a.* from dp.dp_column_info a,dp.dp_field_expand b where b.expand_id=a.expand_id and b.expand_name='snappycompression' and a.table_id=%d",tabid);
    int ret = mdf.Wait();
    if(ret == 0) {
        return 0;
    } else {
        for(int i=0; i<ret; i++) {
            snappy_column_list.push_back(std::string(mdf.PtrStr("column_name",i)));
        }
    }
    return snappy_column_list.size();

}

bool DataDump::column_is_snappycompress(const std::vector<std::string>  snappy_column_list,const std::string column_name)
{

    for(int i=0; i<snappy_column_list.size(); i++) {
        if(strcasecmp(snappy_column_list[i].c_str(),column_name.c_str()) == 0) {
            return true;
        }
    }
    return false;
}

int DataDump::get_lz4compress_column_list(SysAdmin &sp,int tabid,std::vector<std::string> & lz4_column_list)
{
    lz4_column_list.clear();

    AutoMt mdf(sp.GetDTS(),1000);
    mdf.FetchFirst("select a.* from dp.dp_column_info a,dp.dp_field_expand b where b.expand_id=a.expand_id and b.expand_name='lz4compression' and a.table_id=%d",tabid);
    int ret = mdf.Wait();
    if(ret == 0) {
        return 0;
    } else {
        for(int i=0; i<ret; i++) {
            lz4_column_list.push_back(std::string(mdf.PtrStr("column_name",i)));
        }
    }
    return lz4_column_list.size();

}

bool DataDump::column_is_lz4compress(const std::vector<std::string>  lz4_column_list,const std::string column_name)
{

    for(int i=0; i<lz4_column_list.size(); i++) {
        if(strcasecmp(lz4_column_list[i].c_str(),column_name.c_str()) == 0) {
            return true;
        }
    }
    return false;
}


// ��ȡ��İ������ֶ�����
int DataDump::get_zipcompress_column_list(SysAdmin &sp,int tabid,std::vector<std::string> & zip_column_list)
{
    zip_column_list.clear();

    AutoMt mdf(sp.GetDTS(),1000);
    mdf.FetchFirst("select a.* from dp.dp_column_info a,dp.dp_field_expand b where b.expand_id=a.expand_id and b.expand_name='zipcompression' and a.table_id=%d",tabid);
    int ret = mdf.Wait();
    if(ret == 0) {
        return 0;
    } else {
        for(int i=0; i<ret; i++) {
            zip_column_list.push_back(std::string(mdf.PtrStr("column_name",i)));
        }
    }
    return zip_column_list.size();
}

bool DataDump::column_is_zipcompress(const std::vector<std::string>  zip_column_list,const std::string column_name)
{
    for(int i=0; i<zip_column_list.size(); i++) {
        if(strcasecmp(zip_column_list[i].c_str(),column_name.c_str()) == 0) {
            return true;
        }
    }
    return false;
}

// ���¶̻Ựid,slaver�ڵ����
int DataDump::update_new_short_session(SysAdmin &sp,int tabid,int datapartid,bool switch_stsession)
{

    // ��ȡ��ǰ�Ự�̻ỰID
    /*
    select stsessionid from dp.dp_datapart a,dp.dp_datapartext b where a.stsessionid=b.sessionid and b.sessiontype=1 and b.commitstatus=-1;
    */
    AutoMt mdf(sp.GetDTS());
    if(switch_stsession) {

        mdf.FetchFirst("select stsessionid,ltsessionid,clusterstatus,launchslavercnt,finishslavercnt from dp.dp_datapart a,dp.dp_datapartext b where a.stsessionid=b.sessionid and  "
                       " a.tabid = b.tabid and a.datapartid = b.datapartid and "
                       /* ����Ҫ�ȴ����е�slaver�ڵ㴦��ɺ����ͳһ���¶̻Ựid */
                       " ((b.launchslavercnt = 0 and b.finishslavercnt = 0) "
                       /* ������slaver��С��a.slavercnt �����*/
                       /* TODO: ���slaverû�����ݲɼ�,�ͻᵼ�¸������ͽ�����????*/
                       /** ������лỰid�ظ����ж�,����Ѿ��ظ��ˣ��ͼ����ȴ� **/
                       " or (b.launchslavercnt >= b.finishslavercnt and b.launchslavercnt <= a.slavercnt) ) and "
                       " b.sessiontype=%d and b.commitstatus=%d and a.tabid=%d and a.datapartid=%d ",
                       SHORT_SESSION_TYPE,PL_LOAD_DEFAULT,tabid,datapartid);

    } else {

        mdf.FetchFirst("select stsessionid,ltsessionid,clusterstatus,launchslavercnt,finishslavercnt from dp.dp_datapart a,dp.dp_datapartext b where a.stsessionid=b.sessionid and  "
                       " a.tabid = b.tabid and a.datapartid = b.datapartid and "
                       " b.launchslavercnt < a.slavercnt and "
                       " b.sessiontype=%d and b.commitstatus=%d and a.tabid=%d and a.datapartid=%d ",
                       SHORT_SESSION_TYPE,PL_LOAD_DEFAULT,tabid,datapartid);
    }

    int ret = mdf.Wait();
    if(ret >0) {
        // �жϼ�Ⱥ״̬
        int cluster_status = mdf.GetInt("clusterstatus",0);
        if(cluster_status == CS_ERROR) {
            ThrowWith("Slaver�ڵ�,�ȴ���ȡ�̻ỰID�����з��ּ�Ⱥ��ǰ�Ự[%s]����,�˳�.",mdf.PtrStr("stsessionid",0));
        } else if(cluster_status == CS_MASTER_BUSY) {
            lgprintf("slaver�ڵ�[%d]����master�������ύ���ݹ�����,�����ȴ�.",slaver_index);
            return -4;
        } else if(cluster_status == CS_MASTER_INIT) {
            lgprintf("slaver�ڵ�[%d]����master�����ڼ�Ⱥ�������������,�����ȴ�.",slaver_index);
            return -5;
        }

        // �������һ��slaver�����ļ��Ѿ��ɼ����,���������ڵ�(master,slaver)���ڴ���������
        // ��ʱ����ֱ�ӻ�ȡ�ỰIDʹ��
        if(strcmp(stsessionid,mdf.PtrStr("stsessionid",0)) == 0) {
            lgprintf("slaver�ڵ�[%d]�Ựid[%s]���Ѿ�ʹ�ù���������,�����ȴ�.",slaver_index,stsessionid);
            return -2;
        }

        AutoLock lk(&sessionid_lock);
        strcpy(stsessionid,mdf.PtrStr("stsessionid",0));
        if(st_session_interval>0) {
            strcpy(ltsessionid,mdf.PtrStr("ltsessionid",0));
        }
        lgprintf("�ֲ�ʽ����slaver�ڵ�[%d]�̻ỰID[%s]���³ɹ�.",slaver_index,stsessionid);
        lgprintf("�ֲ�ʽ����slaver�ڵ�[%d]���ỰID[%s]���³ɹ�.",slaver_index,ltsessionid);
        return 0;
    } else {

        lgprintf("slaver�ڵ�[%d]û�з����п��õĶ̻ỰID,���򷵻�.",slaver_index);

        // ���ж϶̻Ự�Ƿ����ύ״̬,����Ǽ����ȴ�
        mdf.FetchFirst("select clusterstatus,launchslavercnt,finishslavercnt from dp.dp_datapartext  "
                       " where tabid='%d' and datapartid='%d' and sessiontype='%d' and commitstatus = '%d' ",
                       tabid,datapartid,SHORT_SESSION_TYPE,PL_LOAD_DEFAULT);
        ret = mdf.Wait();
        if(ret >0) {
            int cluster_status = mdf.GetInt("clusterstatus",0);
            if(cluster_status == CS_ERROR) {
                ThrowWith("Slaver�ڵ�,�ȴ���ȡ�̻ỰID�����з��ּ�Ⱥ��ǰ�Ự[%s]����,�˳�.",mdf.PtrStr("stsessionid",0));
            } else if(cluster_status == CS_MASTER_BUSY) {
                lgprintf("slaver�ڵ�[%d]����master�������ύ���ݹ�����,�����ȴ�.",slaver_index);
                return -4;
            }
        }

        return -3;
    }
}


// �����ݿ�ɼ�����,���������������ع�����
int DataDump::rollback_sort_session(SysAdmin &sp,const int tabid,const int datapartid,const int origin_status,const bool filesmode)
{

    // �����ݿ�ɼ����ݣ������˳�ǰ��ֱ�ӽ�����״̬�޸ĳ����µ�����
    // ��ִ��drop���ݿ����֪ͨ����Ⱥ��ֹ���е�����Ự(���̻Ự����)��
    AutoStmt st(sp.GetDTS());

    // 1>. �޸�����״̬
    if(!b_master_node) { // slaver���������

        if(p_stDMIBSortLoader!=NULL ) {

            if(e_extract_run_status == ENUM_SLAVER_WAIT_RUN_SESSION || e_extract_run_status == ENUM_RUN_CUR_SESSION) { // fix dma-1998 : slaver ����Ҫ�����

                // ���¶̻Ự�ڵ�����
                lgprintf("Slaver�ڵ�:���ö̻Ự[%s]��finishslavercnt=finishslavercnt+1.",stsessionid);

                st.DirectExecute("update dp.dp_datapartext set finishslavercnt=finishslavercnt+1 "
                                 " where tabid =%d and datapartid =%d and sessionid='%s' and sessiontype=%d ",
                                 tabid,datapartid,stsessionid,SHORT_SESSION_TYPE);
            }


            if(e_extract_run_status == ENUM_RUN_CUR_SESSION) {
                lgprintf("Slaver�ڵ�:����EmergencyStop��tabid(%d),partid(%d)��Ӧ����Ķ̻Ự(%s)����������.",tabid,datapartid,stsessionid);
                // DMIBSortLoad_NS::EmergencyStop(p_stDMIBSortLoader->GetJobID());
            }

        } else {
            // ������Ⱥ����ʧ��,launchslavercnt�Ѿ��Լ�����,��������ڲɼ����¶̻Ự�ڵ�����

            lgprintf("Slaver�ڵ�:��⵽p_stDMIBSortLoaderΪNULL,����EmergencyStop��tabid(%d),partid(%d)��Ӧ����Ķ̻Ự(%s)����������.",tabid,datapartid,stsessionid);
        }

        // ���ü�ȺΪ������״̬
        if(e_extract_run_status == ENUM_RUN_CUR_SESSION ) {
            set_cluster_status(sp.GetDTS(),tabid,datapartid,stsessionid,CS_ERROR);
        } else {
            lgprintf("���ݳ�ȡ����״̬Ϊ:ENUM_WAIT_NEW_SESSION,�������ü�Ⱥ״̬Ϊ����.");
        }
    }

    // 2>. �����
    if(b_master_node) {

        // �����е�slaver�ڵ���ֹ��,����
        while(1) {
            int launchcnt = 0;
            int finishcnt = 0;
            int64 loadrows = 0;
            int cluster_status = 0;
            if(is_all_slaver_node_finish(sp.GetDTS(),tabid,datapartid,stsessionid,
                                         launchcnt,finishcnt,loadrows,cluster_status)) {

                if(cluster_status == CS_ERROR) {
                    lgprintf("MASTER�ڵ�,�ȴ�slaver�ڵ���ɹ����з��ּ�Ⱥ��ǰ�Ự[%s]����,�˳�.",stsessionid);
                }

                break;
            }
            sleep(60);
            lgprintf("�ȵ�����ڵ����,����ڵ���[%d]����ɽڵ�[%d],�ȴ�60��.",launchcnt,finishcnt);

            if(cluster_status == CS_ERROR) {
                lgprintf("MASTER�ڵ�,�ȴ�slaver�ڵ���ɹ����з��ּ�Ⱥ��ǰ�Ự[%s]����,�˳�.",stsessionid);
            }
        }

        // �޸�����״̬
        AutoStmt st(sp.GetDTS());

        if(!filesmode) { // ���ݿ�ɼ�:

            if(origin_status == 0) {
                lgprintf("����tabid(%d),partid(%d)������״̬Ϊ:���µ���(0).",tabid,datapartid);

                st.DirectExecute("update dp.dp_datapart set status=0,launchmaster = 0, blevel=ifnull(blevel,0)+100 "
                                 "where tabid=%d and datapartid=%d",
                                 tabid,datapartid);
            } else if(origin_status == 73) {
                lgprintf("����tabid(%d),partid(%d)������״̬Ϊ:���ݲ���(73).",tabid,datapartid);

                st.DirectExecute("update dp.dp_datapart set status=73,launchmaster = 0, blevel=ifnull(blevel,0)+100 "
                                 "where tabid=%d and datapartid=%d",
                                 tabid,datapartid);
            } else {
                lgprintf("����tabid(%d),partid(%d)����ʼ״̬Ϊ(%d),δ���л���������.",tabid,datapartid,origin_status);
            }

        } else {
            if(origin_status == 0 || origin_status == 72) {
                lgprintf("����tabid(%d),partid(%d)������״̬Ϊ:�ļ�����(72).",tabid,datapartid);

                st.DirectExecute("update dp.dp_datapart set status=72,launchmaster = 0, blevel=ifnull(blevel,0)+100 "
                                 "where tabid=%d and datapartid=%d",
                                 tabid,datapartid);
            } else if(origin_status == 73) {
                lgprintf("����tabid(%d),partid(%d)������״̬Ϊ:���ݲ���(73).",tabid,datapartid);

                st.DirectExecute("update dp.dp_datapart set status=73,launchmaster = 0, blevel=ifnull(blevel,0)+100 "
                                 "where tabid=%d and datapartid=%d",
                                 tabid,datapartid);
            } else {
                lgprintf("����tabid(%d),partid(%d)����ʼ״̬Ϊ(%d),δ���л���������.",tabid,datapartid,origin_status);
            }

        }

        // 3>. ֪ͨ����Ⱥ��ֹ��������
        if(p_stDMIBSortLoader!=NULL ) {

            if(e_extract_run_status == ENUM_RUN_CUR_SESSION) {
                lgprintf("EmergencyStop��tabid(%d),partid(%d)��Ӧ����Ķ̻Ự(%s)����������.",tabid,datapartid,stsessionid);
                DMIBSortLoad_NS::EmergencyStop(p_stDMIBSortLoader->GetJobID());
            }

            // ���ü�ȺΪ������״̬
            set_cluster_status(sp.GetDTS(),tabid,datapartid,stsessionid,CS_ERROR);
        }
        if(p_ltDMIBSortLoader!=NULL) {
            // ���Ự�ϵ����ݻع�����ʵ��,�ο�:DMA-1321
            lgprintf("EmergencyStop��tabid(%d),partid(%d)��Ӧ����ĳ��Ự(%s)����������.",tabid,datapartid,ltsessionid);
            DMIBSortLoad_NS::EmergencyStop(p_ltDMIBSortLoader->GetJobID());

            // ���ü�ȺΪ������״̬
            set_cluster_status(sp.GetDTS(),tabid,datapartid,ltsessionid,CS_ERROR);
        }

    }
    return 0;
}

// ����һ���µĶ̻Ựid,master�ڵ����
int DataDump::update_new_short_session(SysAdmin &sp,int tabid,int datapartid,int clusterstatus)
{
    // ��¼��ǰ����ĻỰID
    AutoStmt st(sp.GetDTS());
    st.DirectExecute("update dp.dp_datapart set stsessionid='%s' where tabid=%d and datapartid = %d",
                     stsessionid,tabid,datapartid);

    // ��Ӷ̻Ựid
    st.Prepare("insert into dp.dp_datapartext(datapartid,tabid,sessionid,sessiontype,launchslavercnt,"
               " finishslavercnt,commitstatus,sortbegintime,clusterstatus) "
               " values(%d,%d,'%s',%d,0,0,%d,now(),%d) ",
               datapartid,tabid,stsessionid,SHORT_SESSION_TYPE,PL_LOAD_DEFAULT,clusterstatus);
    st.Execute(1);
    st.Wait();

    st_parallel_load_rows = 0;


    lgprintf("master�ڵ㴴�������̻ỰID,����master�ڵ��ڴ��ж̻ỰID[%s]�����ݿ���,׼����slaver�ڵ���.",stsessionid);

    return 0;
}


// ����һ���µĶ̻Ựid,master�ڵ����
int DataDump::create_new_short_session(SysAdmin &sp,int tabid,int datapartid)
{
    // ��ȡ��ǰ�ỰID
    std::string session_id = generate_session_id(sp,tabid,datapartid);

    sessionid_lock.Lock();
    lgprintf("master�ڵ㴴�������̻ỰID,����master�ڵ��ڴ��ж̻ỰID[%s-->%s].",stsessionid,session_id.c_str());
    strcpy(stsessionid,session_id.c_str());
    sessionid_lock.Unlock();

    return 0;
}

// �������̻Ự�������
int DataDump::create_DMIBSortLoader(SysAdmin &sp,int tabid,int datapartid,bool create_lt_session)
{
    mytimer  tm1;
    tm1.Clear();
    tm1.Start();

    if(p_stDMIBSortLoader != NULL) {
        delete p_stDMIBSortLoader;
        p_stDMIBSortLoader = NULL;
    }

    // ֻ��master�ڵ�,����������Ч
    if(create_lt_session && b_master_node) {
        if(p_ltDMIBSortLoader != NULL) {
            delete p_ltDMIBSortLoader;
            p_ltDMIBSortLoader = NULL;
        }
    }

    // �̻ỰID
    std::string st_job_id(stsessionid);


    // ��ȡ��һ�������õĻỰid
    std::string last_sort_session_id;
    if(strlen(sortsampleid)>0) {
        last_sort_session_id = std::string(sortsampleid);
    } else {
        last_sort_session_id = std::string("");
    }

    if(last_sort_session_id.size()>0) {
        lgprintf("�������IDΪ:%s",last_sort_session_id.c_str());
    } else {
        lgprintf("�������IDΪ:NULL");
    }

    lgprintf("׼�������̻Ự[%s],���Ự[%s]�ļ�Ⱥ�������.",stsessionid,st_session_interval > 0 ? ltsessionid : "NULL");

    // ��һ�����ļ�¼��
    std::string st_sort_param;
    std::string lt_sort_param;
    char *env_str_p = NULL;

    st_sort_param = CR_Class_NS::str_setparam_bool(st_sort_param, EXTERNALSORT_ARGNAME_ENABLECOMPRESS, true);
    env_str_p = getenv("CFM_LIBSORT_PARTFILESIZE");
    if (env_str_p) {
        st_sort_param = CR_Class_NS::str_setparam_u64(st_sort_param,
                        EXTERNALSORT_ARGNAME_ROUGHFILESIZE, atoll(env_str_p));
    }
    env_str_p = getenv("CFM_LIBSORT_SLICESIZE");
    if (env_str_p) {
        st_sort_param = CR_Class_NS::str_setparam_u64(st_sort_param,
                        EXTERNALSORT_ARGNAME_ROUGHSLICESIZE, atoll(env_str_p));
    }
    env_str_p = getenv("CFM_LIBSORT_MAXSLICESIZE");
    if (env_str_p) {
        st_sort_param = CR_Class_NS::str_setparam_u64(st_sort_param,
                        EXTERNALSORT_ARGNAME_MAXSLICESIZE, atoll(env_str_p));
    }
    env_str_p = getenv("CFM_LIBSORT_SORTTHREADCOUNT");
    if (env_str_p) {
        st_sort_param = CR_Class_NS::str_setparam_u64(st_sort_param,
                        EXTERNALSORT_ARGNAME_MAXSORTTHREADS, atoll(env_str_p));
    }
    env_str_p = getenv("CFM_LIBSORT_DISKTHREADCOUNT");
    if (env_str_p) {
        st_sort_param = CR_Class_NS::str_setparam_u64(st_sort_param,
                        EXTERNALSORT_ARGNAME_MAXDISKTHREADS, atoll(env_str_p));
    }
    env_str_p = getenv("CFM_LIBSORT_ENABLEPRELOAD");
    if (env_str_p) {
        st_sort_param = CR_Class_NS::str_setparam_bool(st_sort_param,
                        EXTERNALSORT_ARGNAME_ENABLEPRELOAD, atoi(env_str_p));
    }
    env_str_p = getenv("CFM_LIBSORT_PRELOADCACHESIZE");
    if (env_str_p) {
        st_sort_param = CR_Class_NS::str_setparam_u64(st_sort_param,
                        EXTERNALSORT_ARGNAME_PRELOADCACHESIZE, atoll(env_str_p));
    }

    // ��������ģʽ��,TIMEOUT�����㹻��,�Ա���Լ�Ⱥ��
    env_str_p = getenv("DP_DEBUG_MODE");
    if (env_str_p) {
        st_sort_param = CR_Class_NS::str_setparam_u64(st_sort_param,
                        DMIBSORTLOAD_PN_TIMEOUT_COMM, 86400);
    }

    if (!create_lt_session) {
        env_str_p = getenv("CFM_LIBSORT_MAXMERGETHREADS");
        if (env_str_p) {
            st_sort_param = CR_Class_NS::str_setparam_u64(st_sort_param,
                            EXTERNALSORT_ARGNAME_MAXMERGETHREADS, atoll(env_str_p));
        }
        env_str_p = getenv("CFM_LIBSORT_MERGECOUNT");
        if (env_str_p) {
            st_sort_param = CR_Class_NS::str_setparam_u64(st_sort_param,
                            EXTERNALSORT_ARGNAME_MERGECOUNTMIN, atoll(env_str_p));
            st_sort_param = CR_Class_NS::str_setparam_u64(st_sort_param,
                            EXTERNALSORT_ARGNAME_MERGECOUNTMAX, atoll(env_str_p));
        }
    }

    // �ַ����ͣ�ָ�� riak �����ַ����������ݶ����� RiakCluster::Rebuild �� conn_str ����һ��
    char* q=getenv(RIAK_HOSTNAME_ENV_NAME_CLI);
    if(NULL == q || strlen(q) <=1) {
        lgprintf("��������:RIAK_HOSTNAMEδ����,�������Ϊ�ֲ�ʽ����AP�汾.");
    } else {
        lgprintf("��������:RIAK_HOSTNAME=[%s],�������Ϊ�ֲ�ʽ����Cloud�汾.",q);

        std::string _connect_riak_str = std::string(q);
        st_sort_param = CR_Class_NS::str_setparam(st_sort_param,DMIBSORTLOAD_PN_RIAK_CONNECT_STR,_connect_riak_str);
        // lt_sort_param = CR_Class_NS::str_setparam(lt_sort_param,LIBIBLOAD_PN_RIAK_CONNECT_STR,_connect_riak_str);

        // LIBIBLOAD_PN_RIAK_BUCKET �ַ����ͣ�ָ�����ڱ������ݵ� riak bucket ���ƣ�Ŀǰ�汾 CDM ָ��Ϊ "IB_DATAPACKS"
        _connect_riak_str = std::string("IB_DATAPACKS");
        st_sort_param = CR_Class_NS::str_setparam(st_sort_param,DMIBSORTLOAD_PN_RIAK_BUCKET,_connect_riak_str);
        //lt_sort_param = CR_Class_NS::str_setparam(lt_sort_param,LIBIBLOAD_PN_RIAK_BUCKET,_connect_riak_str);
    }

    st_sort_param = CR_Class_NS::str_setparam(st_sort_param,DMIBSORTLOAD_PN_LAST_JOB_ID,last_sort_session_id);

    // ���Ự�Ͷ̻Ự�������ǲ�Ӧ����ȫ��ͬ��
    lt_sort_param = st_sort_param;

    // ����󱣴���ʱ�ļ�������ɾ������,ֱ�����ڳ��Ự����
    if(st_session_interval >0) {
        st_sort_param = CR_Class_NS::str_setparam_bool(st_sort_param, DMIBSORTLOAD_PN_KEEP_SORT_TMP,true);
    } else {
        // ֻ�ж̻Ự��ʱ��,����finish��ɺ�,�ͻὫ�̻Ự��ʱ�������
        st_sort_param = CR_Class_NS::str_setparam_bool(st_sort_param, DMIBSORTLOAD_PN_KEEP_SORT_TMP,false);
    }

    try {
        // �̻Ự�������
        p_stDMIBSortLoader = new DMIBSortLoad(st_job_id,b_master_node,&st_sort_param);

        // ֻ��master�ڵ�,����������Ч
        if(st_session_interval >0 && create_lt_session && p_ltDMIBSortLoader == NULL && b_master_node) { // ���Ự�������
            std::string lt_job_id(ltsessionid);

            // ÿ�ζ���master�ڵ�����
            p_ltDMIBSortLoader = new DMIBSortLoad(lt_job_id,b_master_node,&lt_sort_param);

            // ������ļ����������̽���ģ���Ҫ��ԭ���Ķ̻Ự������ӽ���
            if(add_stsessions_to_ltsession) {

                AutoMt mdf(sp.GetDTS(),10000);
                mdf.FetchFirst("select sessionid,rownums from dp.dp_datapartext where sessiontype=%d "
                               " and datapartid = %d and tabid = %d and commitstatus in(0,1,2) order by sessionid desc",
                               SHORT_SESSION_TYPE,datapartid,tabid);

                int ret = mdf.Wait();
                lt_parallel_load_rows =0;

                for(int i=0; i<ret; i++) {
                    int64 _rownums= 0;
                    _rownums = mdf.GetLong("rownums",i);
                    lt_parallel_load_rows += _rownums;
                    if(_rownums >0) {
                        CR_FixedLinearStorage tmp_fls(65536);
                        tmp_fls.SetTag(CR_Class_NS::str_setparam("",DMIBSORTLOAD_PN_QUERY_CMD, LIBSORT_PV_APPEND_PART_FILES));
                        tmp_fls.PushBack(_rownums,std::string(mdf.PtrStr("sessionid",i)),NULL);
                        lgprintf("�򳤻Ự[%s]������Ѿ��洢�Ķ̻Ự[%s]��[%ld]������......",ltsessionid,mdf.PtrStr("sessionid",i),_rownums);

                        int _r = p_ltDMIBSortLoader->PushLines(tmp_fls);
                        if(_r != 0) {
                            ThrowWith("�򳤻Ự[%s]������Ѿ��洢�Ķ̻Ự[%s]��[%ld]������ʧ��.",ltsessionid,mdf.PtrStr("sessionid",i),_rownums);
                        }
                    } else {
                        lgprintf("�����򳤻Ự[%s]������Ѿ��洢�Ķ̻Ự[%s]��[%ld]������......",ltsessionid,mdf.PtrStr("sessionid",i),_rownums);
                    }
                }

                add_stsessions_to_ltsession = false;
            }

        }

        st_parallel_load_rows = 0;

        tm1.Stop();
        double tm_end = tm1.GetTime();
        tm1.Clear();
        lgprintf("DataDump::create_DMIBSortLoader��������ʹ��ʱ��(%0.4f)��(��ȷ����,����sleep�Ⱥ�ʱ).",tm_end);

    } catch(...) {
        // try to fix dma-1826
        lgprintf("DataDump::create_DMIBSortLoader��������ʧ��.");
        return -1;
    }
    return 0;
}

// ��ʼmaster�ڵ�����ݵ���
int DataDump::start_master_dump_data(SysAdmin &sp,int tabid,int datapartid,const bool keepfiles)
{
    // ��¼��ǰ����ĻỰID
    AutoStmt st(sp.GetDTS());
    st.DirectExecute("update dp.dp_datapart set launchmaster=1, stsessionid='%s' ,ltsessionid='%s' where tabid=%d and datapartid = %d",
                     stsessionid,ltsessionid,tabid,datapartid);

    // ��ӳ��ỰID
    if(st_session_interval>0) {
        st.Prepare("insert into dp.dp_datapartext(datapartid,tabid,sessionid,sessiontype,ltsessionpack,ltsessionobj,"
                   " launchslavercnt,finishslavercnt,commitstatus,sortbegintime) "
                   " values(%d,%d,'%s',%d,0,0,0,0,%d,now()) ",
                   datapartid,tabid,ltsessionid,LONG_SESSION_TYPE,PL_LOAD_DEFAULT);
        st.Execute(1);
        st.Wait();

        set_cluster_status(sp.GetDTS(),tabid,datapartid,ltsessionid,CS_MASTER_INIT);
    }
    if(keepfiles) {
        add_stsessions_to_ltsession = true;
    }

    // ��Ӷ̻Ựid
    st.Prepare("insert into dp.dp_datapartext(datapartid,tabid,sessionid,sessiontype,launchslavercnt,"
               " finishslavercnt,commitstatus,sortbegintime) "
               " values(%d,%d,'%s',%d,0,0,%d,now()) ",
               datapartid,tabid,stsessionid,SHORT_SESSION_TYPE,PL_LOAD_DEFAULT);
    st.Execute(1);
    st.Wait();

    // ���ü�Ⱥ״̬
    set_cluster_status(sp.GetDTS(),tabid,datapartid,stsessionid,CS_MASTER_INIT);

    lgprintf("�ֲ�ʽ����master�ڵ��������óɹ�.");
}

// ��ʼslaver�ڵ�����ݵ���
int DataDump::start_slaver_dump_data(SysAdmin &sp,int tabid,int datapartid)
{
    AutoMt mt_slaver(sp.GetDTS(),100);
    mt_slaver.FetchFirst("select tabid,datapartid,ltsessionid,stsessionid from dp.dp_datapart where tabid = %d and datapartid =%d",
                         tabid,datapartid);
    int ret = mt_slaver.Wait();

    //>> begin: try to fix dma-2007:slaver_index Ψһֵ�Ļ�ȡ
    lgprintf("�ֲ�ʽ����slaver�ڵ�slaver_index׼����ȡ......");
    bool ec=wociIsEcho();
    wociSetEcho(FALSE);
    {
        AutoStmt _st(sp.GetDTS());
        _st.DirectExecute("lock tables dp.dp_datapartext write");
    }
    //>> end: try to fix dma-2007:slaver_index Ψһֵ�Ļ�ȡ

    assert(ret == 1);
    {
        AutoLock lk(&sessionid_lock);

        if(st_session_interval >0) {
            strcpy(ltsessionid,mt_slaver.PtrStr("ltsessionid",0));
            increase_slaver_lanuch_count(sp,tabid,datapartid,true);
        }
        strcpy(stsessionid,mt_slaver.PtrStr("stsessionid",0));
        increase_slaver_lanuch_count(sp,tabid,datapartid,false);

        e_extract_run_status = ENUM_SLAVER_WAIT_RUN_SESSION;

    }
    // �����dp.dp_datapartext��,DestLoader::Load��ʱ����ʹ���ڲ���ֵ


    // slaver�ڵ������ֵ
    {
        // ��ȡdp.dp_datapartext���Ӧslaver_index��ֵ
        mt_slaver.FetchFirst("select launchslavercnt from dp.dp_datapartext where tabid = %d and datapartid =%d and sessionid='%s' ",
                             tabid,datapartid,stsessionid);
        ret = mt_slaver.Wait();
        slaver_index = mt_slaver.GetInt("launchslavercnt",0);

        //>> begin: try to fix dma-2007:slaver_index Ψһֵ�Ļ�ȡ
        {
            AutoStmt _st(sp.GetDTS());
            _st.DirectExecute("unlock tables");
        }
        wociSetEcho(ec);
        //>> end: try to fix dma-2007:slaver_index Ψһֵ�Ļ�ȡ
    }


    lgprintf("�ֲ�ʽ����slaver�ڵ�[%d]�������óɹ�.",slaver_index);
}



// ����slaver�ڵ����������
int DataDump::increase_slaver_lanuch_count(SysAdmin &sp,int tabid,int datapartid,bool b_lt_session)
{
    int ret = 0;
    AutoStmt datapartext_mt(dtdbc);
    if(b_lt_session) {
        datapartext_mt.Prepare("update dp.dp_datapartext set launchslavercnt=launchslavercnt+1 "
                               " where tabid =%d and datapartid =%d and sessionid='%s' ",tabid,datapartid,ltsessionid);
    } else {
        datapartext_mt.Prepare("update dp.dp_datapartext set launchslavercnt=launchslavercnt+1 "
                               " where tabid =%d and datapartid =%d and sessionid='%s' ",tabid,datapartid,stsessionid);

    }
    datapartext_mt.Execute(1);
    ret = datapartext_mt.Wait();

    if(b_lt_session) {
        lgprintf("���Ự[%s]��Ӧ��slaver�ڵ�������������1[%s].",ltsessionid,ret==1?"�ɹ�":"ʧ��");
    } else {
        lgprintf("�̻Ự[%s]��Ӧ��slaver�ڵ�������������1[%s].",stsessionid,ret==1?"�ɹ�":"ʧ��");
    }

    return ret;
}


// ����slaver�ڵ����������
int DataDump::decrease_slaver_lanuch_count(SysAdmin & sp, int tabid, int datapartid, bool b_lt_session)
{
    int ret = 0;
    AutoStmt datapartext_mt(dtdbc);
    if(b_lt_session) {
        datapartext_mt.Prepare("update dp.dp_datapartext set launchslavercnt=launchslavercnt-1 "
                               " where tabid =%d and datapartid =%d and sessionid='%s' ",tabid,datapartid,ltsessionid);
    } else {
        datapartext_mt.Prepare("update dp.dp_datapartext set launchslavercnt=launchslavercnt-1 "
                               " where tabid =%d and datapartid =%d and sessionid='%s' ",tabid,datapartid,stsessionid);

    }
    datapartext_mt.Execute(1);
    ret = datapartext_mt.Wait();

    if(b_lt_session) {
        lgprintf("���Ự[%s]��Ӧ��slaver�ڵ��������Լ���1[%s].",ltsessionid,ret==1?"�ɹ�":"ʧ��");
    } else {
        lgprintf("�̻Ự[%s]��Ӧ��slaver�ڵ��������Լ���1[%s].",stsessionid,ret==1?"�ɹ�":"ʧ��");
    }

    return ret;
}



// ��ȡ��ʼ����������ʼλ��
bool ParallelLoad::get_commit_sort_info(SysAdmin &sp,
                                        int tabid,
                                        int datapartid,
                                        int64& startloadrow,
                                        int& startpackno,
                                        int& startpackobj)
{

    // ��ȡ�÷���װ�����ݵ����һ�����ź͸ð���Ҫ�ļ�¼��
    char basepth[300];
    strcpy(basepth,sp.GetMySQLPathName(0,"msys")); // ��ȡ��Ŀ¼

    // 1. ��dp.dp_datapart����,��ȡ����״̬Ϊ3��������Ϣ(�ỰID��Ϊnull)
    AutoMt mdf(sp.GetDTS(),MAX_DST_DATAFILENUM);
    char partfullname[100];
    mdf.FetchFirst("select * from dp.dp_datapart where tabid=%d and datapartid = %d",tabid,datapartid);
    int ret = mdf.Wait();
    assert(ret >0);
    strcpy(partfullname,mdf.PtrStr("partfullname",0));

    int part_last_pack_no = 0;
    int part_last_pack_obj = 0;
    int tb_pack_no=0;
    int64 tb_obj_no=0;
    int append_part = 0;
    {
        mdf.FetchFirst("select databasename,tabname from dp.dp_table where tabid=%d",tabid);
        ret = mdf.Wait();
        assert(ret == 1);
        char tabpath[300],dbname[200],tabname[200];
        strcpy(dbname,mdf.PtrStr("databasename",0));
        strcpy(tabname,mdf.PtrStr("tabname",0));
#if 0
        /*
        <<��ȡ����װ�����ݵ�һ������λ��>>
        ���[dpadmin -f dump] ��mysqld����ͬһ���������ʱ��
        ��Ҫ��$DATAMERGER_HOME/varĿ¼ͨ��nfs���ص����ش���,ͨ�����ļ��ķ�ʽ,��ȡ����װ���һ������λ��
        ���[dpadmin -f dump]�����ڶ�̨�����������,�����������ʵʩ�ĸ��Ӷ�
        */
        sprintf(tabpath,"%s/%s/%s.bht",basepth,dbname,tabname);

        // ��ȡ��İ��źͼ�¼��
        get_table_pack_obj_info(basepth,dbname,tabname,tb_pack_no,tb_obj_no);

        // ��ȡ�����Ķ�Ӧ�����һ�����źͼ�¼��
        append_part = get_part_pack_info(basepth,dbname,tabname,partfullname,part_last_pack_no,part_last_pack_obj);
#else
        /*
        <<��ȡ����װ�����ݵ�һ������λ��>>
        ��DMA-1815�����н��и������:ʹ�����������в�ѯ
        [show table status from  dma1978 where Name='xxx' and engine='brighthouse' and Create_time<now()]
        �Ӳ�ѯ��comment�л�ȡ���

        example:

        DataMerger> show table status from  dma1978 where Name='xxx' and engine='brighthouse' and Create_time<now();
        +------+-------------+---------+------------+------+----------------+-------------+-----------------+--------------+-----------+----------------+---------------------+---------------------+------------+----------------+----------+----------------+----------------------------------------------------------------------------------+
        | Name | Engine      | Version | Row_format | Rows | Avg_row_length | Data_length | Max_data_length | Index_length | Data_free | Auto_increment | Create_time         | Update_time         | Check_time | Collation      | Checksum | Create_options | Comment                                                                          |
        +------+-------------+---------+------------+------+----------------+-------------+-----------------+--------------+-----------+----------------+---------------------+---------------------+------------+----------------+----------+----------------+----------------------------------------------------------------------------------+
        | xxx  | BRIGHTHOUSE |      10 | Compressed |  109 |             23 |        2600 |               0 |            0 |         0 |           NULL | 2016-05-12 11:38:52 | 2016-05-12 11:38:52 | NULL       | gbk_chinese_ci |     NULL |                | ;tab_pack_no=xx;tab_obj_no=xx;last_part_fullname='xx';last_part_lastpackno=xx;last_part_lastpackobj=xx; |
        +------+-------------+---------+------------+------+----------------+-------------+-----------------+--------------+-----------+----------------+---------------------+---------------------+------------+----------------+----------+----------------+----------------------------------------------------------------------------------+
        1 row in set (0.00 sec)

        */
        /*
            comment ���ظ�ʽ����:
            0x0871#tab_pack_no#tab_obj_no#'last_part_fullname'#last_part_lastpackno#last_part_lastpackobj
            ����:
            0x0871#123400000#10000000000231323#'p10000000000000000000000000'#123214000000#34342
        */
        mdf.FetchFirst("show table status from %s where Name='%s' and engine='brighthouse' and Create_time<now()",dbname,tabname);
        ret = mdf.Wait();
        if(ret <=0) {
            ThrowWith("���ݿ��:%s.%s������,�޷���ȡ����װ��λ��,׼���˳�.",dbname,tabname);
        }

        std::string str_load_info = std::string(mdf.PtrStr("Comment",0));
        size_t found = str_load_info.find("0x0871#");
        if(found == std::string::npos) {
            ThrowWith("���ݿ��:%s.%s������,�޷���ȡ����װ��λ��<0x0871>,׼���˳�.",dbname,tabname);
        }

        std::string str_found = str_load_info.substr(found);   // 0x0871#...#...#'...'#...#
        lgprintf("��ȡ������װ��λ����Ϣ:%s",str_found.c_str());

        std::string str_found_numbers = "";     // 0x0871#...#...#...#
        std::string lst_load_part_name="";
        {
            const char enclose_char='\'';
            size_t _s_pos = str_found.find_first_of(enclose_char);
            size_t _e_pos = str_found.find_last_of(enclose_char);
            lst_load_part_name = str_found.substr(_s_pos+1,(_e_pos-_s_pos-1));

            // ȥ��name���ֵĽ�ȡ
            str_found_numbers = str_found.substr(0,_s_pos-1); // skip last #
            str_found_numbers += str_found.substr(_e_pos+1);  // skip last '
        }

        sscanf(str_found_numbers.c_str(),"0x0871#%d#%lld#%d#%d",&tb_pack_no,&tb_obj_no,&part_last_pack_no,&part_last_pack_obj);

        if(strcasecmp(lst_load_part_name.c_str(),partfullname) == 0) {
            append_part = 1;
        }
#endif

        if(append_part == 1) { // �ҵ�ԭ������
            if(part_last_pack_no==tb_pack_no-1) { // �÷������Ǳ�����һ����װ�������
                if(part_last_pack_obj != 65536) {
                    startloadrow = part_last_pack_obj;             // ��һ������Ҫ���ݵ�ƫ����
                } else {
                    startloadrow = 0;                              // ��һ������Ҫ���ݵ�ƫ����
                }
            } else {  // �÷����������һ������
                part_last_pack_no = tb_pack_no+1;
                part_last_pack_obj = 65536;
                startloadrow = 0;
            }
        } else { // û���ҵ�����
            if(tb_obj_no !=0) {
                part_last_pack_no = tb_pack_no+1;
            } else {
                part_last_pack_no = 0;
            }
            part_last_pack_obj = 0;
            startloadrow = 0;
        }
    }

    startpackno= part_last_pack_no;
    startpackobj = part_last_pack_obj;
    return true;
}

// �ж�Ҫ�����������Ƿ�Ӧ����master�ڵ�
bool DataDump::is_master_node(int dbc,int tabid,int datapartid)
{
    // �ж��Ƿ���master�ڵ�,����ĵ�һ���ڵ�(�����ύ�Ľڵ�)
    AutoMt mt_is_master(dbc,1000);
    mt_is_master.FetchFirst("select launchmaster from dp.dp_datapart "
                            " where tabid = %d and datapartid =%d limit 2",tabid,datapartid);

    // dp.dp_datapart.launchmaster : �Ƿ��Ѿ�����master�ڵ�,1:�Ѿ�����,0:û������'
    mt_is_master.Wait();
    return  mt_is_master.GetInt(0,0) == 0;
}

// >> begin: fix dma-1871 , add by liujs 20160104
// ���dpadmin -f dumpÿ�ν���ֻ����һ���ɼ����������[����slaver�����ظ���ִ��,��������ִ�в���]
typedef struct stru_taskinfo {
    int tabid;
    int datapartid;
    stru_taskinfo():tabid(0),datapartid(0) {};
    virtual ~stru_taskinfo()
    {
        tabid = -1;
        datapartid = -1;
    }
} stru_taskinfo,*stru_taskinfo_ptr;
typedef std::vector<stru_taskinfo> stru_taskinfo_vec;
typedef std::vector<stru_taskinfo>::iterator stru_taskinfo_vec_iter;
bool exist_taskinfo(const stru_taskinfo_vec stv,const int tabid,const int partid)
{
    for(int i=0; i<stv.size(); i++) {
        if(stv[i].tabid==tabid && stv[i].datapartid==partid) {
            return true;
        }
    }
    return false;
}
// << end:fix dma-1871 , add by liujs 20160104

int DataDump::DoDump(SysAdmin &sp,SysAdmin &plugin_sp,const char *systype)
{
    st_session_interval = 0;
    int _ret = 0;
    AutoMt mdf(sp.GetDTS(),MAX_DST_DATAFILENUM);

    // ִ�е�slaver�������
    stru_taskinfo_vec  run_slaver_taskinfo_vec;
    bool run_slaver_taskinfo_finish = false;
    long run_slaver_taskinfo_deal_rows = 0;   // slaver�����������������

lable_dump_slaver_begin: // ��ʼ����ɼ����̵ĵ�slaver

    // ����ǲ��вɼ�����ģʽ,���ȴ������ڵ���������
    bool deal_slaver_node = false;
    if(dp_parallel_load) { // ֻ�д��ļ��ɼ����ݵ�ʱ��,���ܹ������Ĳɼ�����,���ݿ�ɼ����ݲ��ܲ���ִ��

        // һ����������������������ݳ��Ự����ִ�е�ʱ����ô�ñ�ĸ÷����Ͳ��������������ִ��
        mdf.FetchAll("select a.*,(select count(1) from dp.dp_datapart b where a.tabid=b.tabid and b.status=1) busyct "
                     " from dp.dp_datapart a ,dp.dp_datapartext c where (status=1 and lower(extsql) like 'load %%') "
                     " and a.tabid = c.tabid AND a.datapartid = c.datapartid AND a.stsessionid = c.sessionid "
                     " AND c.sessiontype = '%d' and c.commitstatus='%d' AND c.launchslavercnt < a.slavercnt AND  begintime<now() %s "
                     /*>> ADD BEGIN: FIX DMA-1349: ��������������ͬʱ�������ݲɼ� */
                     " and not exists (select 1 from dp.dp_datapart x where x.tabid=a.tabid and a.datapartid != x.datapartid and x.status = 1 "
                     /* ���Ự�ڴ����ʱ��,�̻ỰҪ����Բ������� DMA-1621 */
                     " and not exists (select 1 from dp.dp_datapartext y where y.tabid = a.tabid and y.datapartid = a.datapartid and y.sessiontype='%d' )"
                     " )"
                     /*<< ADD END */
                     "  order by blevel,busyct,touchtime,tabid,datapartid limit 2 ",
                     SHORT_SESSION_TYPE,PL_LOAD_DEFAULT,sp.GetNormalTaskDesc(),LONG_SESSION_TYPE);

        _ret = mdf.Wait();
        if(_ret >=1) deal_slaver_node = true;

        paraller_sorttm.Restart();
    }

lable_dump_master_begin: // ��ʼ����ɼ����̵ĵ�master
    if(!deal_slaver_node) { //������ͨ������
        if(systype==NULL) {
            if(!dp_parallel_load) {
                // ԭ����
                mdf.FetchAll("select a.*,(select count(1) from dp.dp_datapart b where a.tabid=b.tabid and b.status=1) busyct "
                             "from dp.dp_datapart a where (status=0 or (status in(72,73) and lower(extsql) like 'load %%')) and begintime<now() %s "
                             "order by blevel,busyct,touchtime,tabid,datapartid limit 2",sp.GetNormalTaskDesc());

            } else {
                // ͬһ�����ܽ��ж��������
                // ���һ��������������̻Ự���������(���Ự������������ʱ��),�������µķ�������
                mdf.FetchAll("select a.*,(select count(1) from dp.dp_datapart b where a.tabid=b.tabid and b.status=1) busyct "
                             "from dp.dp_datapart a where (status=0 or (status in(72,73) and lower(extsql) like 'load %%')) and begintime<now() %s "

                             /*>> ADD BEGIN: FIX DMA-1372: ��һ������������,������Ϊmaster�ڵ�����*/
                             " and a.launchmaster = 0 "
                             /*<< ADD END*/

                             /*>> ADD BEGIN: FIX DMA-1349: ��������������ͬʱ�������ݲɼ�
                             " and not exists (select 1 from dp.dp_datapart x where x.tabid=a.tabid and a.datapartid != x.datapartid and x.status = 1) "
                             << ADD END */

                             /* ͬһ����,���Ự�ڴ����ʱ��,���������̻Ự���� */
                             " and not exists (select 1 from dp.dp_datapart x ,dp.dp_datapartext y where x.tabid=a.tabid and a.datapartid != x.datapartid and x.status = 1 "
                             " and y.tabid = x.tabid and y.datapartid = x.datapartid and y.sessiontype = 1 and y.commitstatus in(0,1,-1)) "

                             "order by blevel,busyct,touchtime,tabid,datapartid limit 2",sp.GetNormalTaskDesc());

            }
        } else {
            printf("�������ͣ�%s\n",systype);
            if(!dp_parallel_load) {
                // ԭ����
                mdf.FetchAll("select * from dp.dp_datapart where (status=0 or (status in(72,73) and lower(extsql) like 'load %%')) "
                             " and exists(select 1 from dp.dp_datasrc a where a.sysid=dp_datapart.srcsysid and a.systype in ( %s) ) and begintime<now() %s "
                             "order by blevel,touchtime,tabid,datapartid limit 2",systype,sp.GetNormalTaskDesc ());
            } else {
                // ͬһ�����ܽ��ж��������
                mdf.FetchAll("select a.* from dp.dp_datapart a where (a.status=0 or (a.status in(72,73) and lower(a.extsql) like 'load %%')) "
                             " and exists(select 1 from dp.dp_datasrc b where b.sysid=a.srcsysid and b.systype in ( %s) ) and a.begintime<now() %s "

                             /*>> ADD BEGIN: FIX DMA-1372: ��һ������������,������Ϊmaster�ڵ�����*/
                             " and a.launchmaster = 0 "
                             /*<< ADD END*/

                             /*>> ADD BEGIN: FIX DMA-1349: ��������������ͬʱ�������ݲɼ� */
                             " and not exists (select 1 from dp.dp_datapart x where x.tabid=a.tabid and a.datapartid != x.datapartid and x.status = 1) "
                             /*<< ADD END */

                             "order by blevel,touchtime,tabid,datapartid limit 2",systype,sp.GetNormalTaskDesc ());
            }
        }

        _ret = mdf.Wait();
        if(_ret<1) {
            if(dp_parallel_load) {
                lgprintf("û�л�ȡ������ִ�е�master������Ϣ,�����˳�.");
            } else {
                lgprintf("û�л�ȡ������ִ�еĲɼ�������Ϣ,�����˳�.");
            }
            return 0;
        }
    }

    //sp.GetFirstTaskID(NEWTASK,tabid,datapartid);
    sp.Reload();

    bool addfiles=0;
    bool keepfiles=0;
    int  origion_status = 0;
    int _slavercnt = 0;
    int tabid=0;

    if(_ret >0) {
        if(!deal_slaver_node) {
            run_slaver_taskinfo_vec.clear(); // master ���������,��Ҫ���
        }

        int i = 0;
        for(i=0; i<_ret; i++) {

            addfiles = mdf.GetInt("status",i)==73;
            keepfiles = mdf.GetInt("status",i)==72;
            origion_status=mdf.GetInt("status",i);
            tabid=mdf.GetInt("tabid",i);
            datapartid=mdf.GetInt("datapartid",i);
            _slavercnt = mdf.GetInt("slavercnt",i);

            if(tabid <=0) continue;

            if(!deal_slaver_node) { // master �������һ������
                break;
            } else { // slaver ��������

                run_slaver_taskinfo_deal_rows = 0;

                if(!exist_taskinfo(run_slaver_taskinfo_vec,tabid,datapartid)) { // �ҵ����Դ����slaver����
                    stru_taskinfo st;
                    st.tabid = tabid;
                    st.datapartid = datapartid;
                    run_slaver_taskinfo_vec.push_back(st);
                    break;
                } else {
                    continue;  // ��һ���Ѿ��������slaver��������,����ֱ�Ӻ���
                }
            }
        }

        if( i== _ret && deal_slaver_node ) { // ���е�slaver���������,�ͼ��������һ��
            addfiles = mdf.GetInt("status",0)==73;
            keepfiles = mdf.GetInt("status",0)==72;
            origion_status=mdf.GetInt("status",0);
            tabid=mdf.GetInt("tabid",0);
            datapartid=mdf.GetInt("datapartid",0);
            _slavercnt = mdf.GetInt("slavercnt",0);
            run_slaver_taskinfo_finish = true;
            run_slaver_taskinfo_deal_rows = 0;
        }
    } else {
        lgprintf("û�л�ȡ������ִ�е�slaver������Ϣ,�����˳�.");
        return 0;
    }

    if(tabid<1) {
        lgprintf("û�л�ȡ������ִ�е�������Ϣ,�����˳�.");
        return 0;
    }

    mdf.FetchFirst("select stsessiontime,sortsampleid from dp.dp_table where tabid='%d'",tabid);
    if(mdf.Wait()<1) return 0;
    st_session_interval =  mdf.GetInt("stsessiontime",0)*60;
    strcpy(sortsampleid,mdf.PtrStr("sortsampleid",0));

    sp.SetTrace("dump",tabid);

    if(dp_parallel_load) {

        char _logfn[1024];
        int  _logfnlen = 1024;
        lgfilename(_logfn,_logfnlen);

        if(_logfnlen > 0) {
            // ����������־�ӿ�
            CR_Class_NS::dprintfx_filename = std::string(_logfn);
            lgprintf("������ĻỰ��־������ļ�[%s]��.",_logfn);
        }
    }

    sorttm.Restart();
    fiotm.Clear();
    adjtm.Clear();
    sp.GetSoledIndexParam(datapartid,&dp,tabid);
    sp.OutTaskDesc("ִ�����ݵ�������: ",tabid,datapartid);
    if(xmkdir(dp.tmppath[0])) {
        sp.log(tabid,datapartid,DUMP_CREATE_PATH_ERROR,"��ʱ��·���޷�����,��:%d,������:%d,·��:%s.",tabid,datapartid,dp.tmppath[0]);
        ThrowWith("��ʱ��·���޷�����,��:%d,������:%d,·��:%s.",tabid,datapartid,dp.tmppath[0]);
    }
    AutoHandle srcdbc;
    AutoHandle fmtdbc;
    // datapartid��Ӧ��Դϵͳ����һ���Ǹ�ʽ���Ӧ��Դϵͳ
    // Jira:DM-48
    try {
        srcdbc.SetHandle(sp.BuildSrcDBC(tabid,datapartid));
        fmtdbc.SetHandle(sp.BuildSrcDBC(tabid,-1));
    } catch(...) {
        sp.log(tabid,datapartid,DUMP_CREATE_DBC_ERROR,"Դ(Ŀ��)����Դ����ʧ��,��:%d,������:%d",tabid,datapartid);
        AutoStmt st(dtdbc);
        st.DirectExecute("update dp.dp_datapart set blevel=ifnull(blevel,0)+100 where tabid=%d and datapartid=%d",
                         tabid,datapartid);
        sp.logext(tabid,datapartid,EXT_STATUS_ERROR,"");
        throw;
    }

    AutoMt srctstmt(0,10);
    int partoff=0;
    try {
        //�����ʽ����Ŀ���Ľṹ��һ��
        sp.BuildMtFromSrcTable(fmtdbc,tabid,&srctstmt);
        srctstmt.AddrFresh();
        int srl=sp.GetMySQLLen(srctstmt);//wociGetRowLen(srctstmt);
        char tabname[150];
        sp.GetTableName(tabid,-1,tabname,NULL,TBNAME_DEST);
        AutoMt dsttstmt(dtdbc,10);
        dsttstmt.FetchFirst("select * from dp.dp_datapart where tabid=%d and status=5 ",tabid);
        int ndmprn=dsttstmt.Wait();
        //����ƶ����ֵ����⣺�����������е������ڶ��������ڵ�һ����ʼ���������������dp_table.lstfid����λ������������ļ���Ŵ���
        // ���������5�д���
        if(ndmprn==0) {
            dsttstmt.FetchFirst("select * from dp.dp_middledatafile where tabid=%d and procstate>1 limit 10",tabid);
            ndmprn=dsttstmt.Wait();
            dsttstmt.FetchFirst("select * from dp.dp_datafilemap where tabid=%d limit 10",tabid);
            ndmprn+=dsttstmt.Wait();
        }
        int tstrn=0;
        try {
            //jira DMA-392: SKIP dest query since sys_lock not resolved.
            // block by dma-384
            dsttstmt.Clear();
            sp.BuildMtFromSrcTable(fmtdbc,tabid,&dsttstmt);
            dsttstmt.AddrFresh();
            tstrn=10;
            dsttstmt.FetchFirst("select * from %s limit 10",tabname);
            tstrn=dsttstmt.Wait();
        } catch(...) {
            sp.log(tabid,datapartid,DUMP_DST_TABLE_ERROR,"Ŀ���%d�����ڻ�ṹ����,��Ҫ���¹���,�Ѿ��������ع����.",tabid);
            AutoStmt st(dtdbc);
            st.DirectExecute("update dp.dp_table set cdfileid=0 where tabid=%d",tabid);
            lgprintf("Ŀ���%d�����ڻ�ṹ����,��Ҫ���¹���,�Ѿ��������ع����.",tabid);
            sp.logext(tabid,datapartid,EXT_STATUS_ERROR,"");

            throw;
        }
        if(srctstmt.CompareMt(dsttstmt)!=0 ) {
            if(tstrn>0 && ndmprn>0) {
                sp.log(tabid,datapartid,DUMP_DST_TABLE_FORMAT_MODIFIED_ERROR,
                       "��%s���Ѿ������ݣ���Դ��(��ʽ��)��ʽ���޸ģ����ܵ������ݣ��뵼���µ�(�յ�)Ŀ����С�",tabname);
                ThrowWith("��%s���Ѿ������ݣ���Դ��(��ʽ��)��ʽ���޸ģ����ܵ������ݣ��뵼���µ�(�յ�)Ŀ����С�",tabname);
            }
            lgprintf("Դ�����ݸ�ʽ�����仯�����½���Դ��... ");
            if(tstrn==0) {
                sp.CreateDT(tabid);
                sp.Reload();
                sp.GetSoledIndexParam(datapartid,&dp,tabid);
            } else {
                //ȫ�����ݷ������µ�������,��������ṹ��ʱ��һ��
                //����Ŀ��������ݣ���ʱ���޸�dt_table.recordlen
                dp.rowlen=srl;
            }
        } else if(srl!=dp.rowlen) {
            lgprintf("Ŀ����еļ�¼���ȴ���%d�޸�Ϊ%d",dp.rowlen,srl);
            sp.log(tabid,datapartid,DUMP_DST_TABLE_RECORD_LEN_MODIFY_NOTIFY,"Ŀ����еļ�¼���ȴ���%d�޸�Ϊ%d",dp.rowlen,srl);
            wociClear(dsttstmt);
            AutoStmt &st=dsttstmt.GetStmt();
            st.Prepare("update dp.dp_table set recordlen=%d where tabid=%d",srl,tabid);
            st.Execute(1);
            st.Wait();
            dp.rowlen=srl;
        }
        if(ndmprn==0 && tstrn==0) {
            //��λ�ļ����,���Ŀ���ǿ�,���ļ���Ų���λ
            wociClear(dsttstmt);
            AutoStmt &st=dsttstmt.GetStmt();
            st.Prepare("update dp.dp_table set lstfid=0 where tabid=%d",srl,tabid);
            st.Execute(1);
            st.Wait();
        }
    } catch(...) {
        AutoStmt st(dtdbc);
        st.DirectExecute("update dp.dp_datapart set blevel=ifnull(blevel,0)+100 where tabid=%d and datapartid=%d",
                         tabid,datapartid);
        sp.logext(tabid,datapartid,EXT_STATUS_ERROR,"");
        throw;
    }

    long realrn=memlimit/dp.rowlen;
    //>> Begin:DMA-458,�ڴ��¼�������ֵ����,20130129
    if(realrn > MAX_ROWS_LIMIT-8) {
        realrn = MAX_ROWS_LIMIT - 8;
        lgprintf("������¼�����Ѿ�����2G�����ֽ����%d���޸�Ϊ%d��,�������̼���ִ��",memlimit/dp.rowlen,realrn);
    }
    //<< End:
    lgprintf("��ʼ��������,���ݳ�ȡ�ڴ�%ld�ֽ�(�ۺϼ�¼��:%d)",realrn*dp.rowlen,realrn);
    sp.log(tabid,datapartid,DUMP_BEGIN_DUMPING_NOTIFY,"��ʼ���ݵ���:���ݿ�%d�ֽ�(��¼��%d),��־�ļ� '%s'.",realrn*dp.rowlen,realrn,wociGetLogFile());
    sp.log(tabid,datapartid,DUMP_BEGIN_DUMPING_NOTIFY,"��ʼ������%d,����%d����",tabid,datapartid);
    //if(realrn>dp.maxrecnum) realrn=dp.maxrecnm;
    //CMNET: ����ʱ��ɾ������
    //2011/7/1 ����������ʱ�޸ģ�������ϴ�����
    //if(false)
    //JIRA DMA-35
    // 2012/02/10: �ļ���ʽ����ʱ���в��е���Ͳ����������ֻ�ڳ�ʼ����ʱ��������������
    AutoMt clsmt(dtdbc,1000);

    add_stsessions_to_ltsession =false;

    if(!deal_slaver_node) {// master�ڵ�,ֱ�ӽ���������Ϊ���ڵ��¼���

        //>> begin: try to fix dma-1978:����̻Ự����������
        if(dp_parallel_load) // fix sasu-372
        {
            AutoMt _tmt(dtdbc,1000);
            _tmt.FetchAll("select * from  ( select tabid,datapartid,status,(select count(1) from dp.dp_datapartext b where a.tabid=b.tabid and a.datapartid=b.datapartid)  scnt "
                          " from dp.dp_datapart a where  tabid =  %d  and status=1 group by a.tabid ,a.datapartid ) tmp where  scnt=0",tabid);
            int _rt=_tmt.Wait();
            if(_rt>0) {
                lgprintf("��%d,����%d ��ʼ�������ָñ����������Ѿ��ڵ���,���ܽ��в���������ʼ,�����˳�.",tabid,datapartid);
                return 0;
            }
        }
        //<< end: try to fix dma-1978

        try {
            sp.UpdateTaskStatus(DUMPING,tabid,datapartid);
        } catch(char *str) {
            lgprintf(str);
            sp.log(tabid,datapartid,DUMP_UPDATE_TASK_STATUS_ERROR,str);
            return 0;
        }
    }

    try {
        if(!keepfiles && !addfiles && !deal_slaver_node) { //master �ڵ����: slaver �ڵ�������ʱ��,����ɾ��dp.dp_filelog������

            lgprintf("�����[���µ���<0>]����,����ϴε���������...");

            bool delete_partition = false;
            int clsrn = 0;
            if(!dp_parallel_load) {
                clsmt.FetchAll("select tabid from dp.dp_datafilemap where datapartid=%d and tabid=%d and procstatus in(2,3) limit 100",datapartid,tabid);
                clsrn=clsmt.Wait();
                if(clsrn >0) {
                    AutoStmt st(dtdbc);
                    st.DirectExecute("update dp.dp_datapart set blevel=blevel+100 where datapartid=%d and tabid=%d",datapartid,tabid);
                    lgprintf("��%d,����%d,�Ѿ�װ�������,ɾ������������������µ���!",tabid,datapartid);
                }

            } else {

                clsmt.FetchAll("select tabid from dp.dp_datapartext where datapartid=%d and tabid=%d and commitstatus in(2,3) limit 100",datapartid,tabid);
                clsrn=clsmt.Wait();
                if(clsrn >0) {
                    AutoStmt st(dtdbc);
                    st.DirectExecute("update dp.dp_datapart set blevel=blevel+100 where datapartid=%d and tabid=%d",datapartid,tabid);
                    lgprintf("��%d,����%d,�Ѿ�װ�������,ɾ������������������µ���!",tabid,datapartid);
                }

                {
                    // ɾ���ϲ���ɺ������
                    DestLoader rm_part(&sp);
                    // ��ɾ���ϲ����װ������
                    rm_part.clear_has_merged_data(tabid,datapartid);

                    // ���û����ɵĻỰid
                    clsmt.FetchAll("select sessionid from dp.dp_datapartext where tabid =%d and datapartid = %d and commitstatus in(-1,0) ",tabid,datapartid);
                    int _rt = clsmt.Wait();
                    char cancle_session_cmd[256];
                    for(int i=0; i<_rt; i++) {
                        sprintf(cancle_session_cmd,"cfm-toolbox slcancel %s",clsmt.PtrStr("sessionid",i));
                        lgprintf("Զ����ֹ�Ự,��������:%s ",cancle_session_cmd);
                        int __r = CR_Class_NS::system(cancle_session_cmd);
                        lgprintf("Զ����ֹ�Ự,��������:%s ,return %d",cancle_session_cmd,__r);
                    }

                    // ���dp.dp_datapartext �еļ�¼
                    AutoStmt st(dtdbc);
                    st.DirectExecute("delete from dp.dp_datapartext where tabid=%d and datapartid=%d",tabid,datapartid);
                }
            }

            // ɾ����������
            if(clsrn >0) {
                delete_partition = true;
            }

            AutoStmt st(dtdbc);
            clsrn=0;
            do {
                int i;
                // Ϊʲôÿ��ɾ��100�м�¼,FetchAll ���ܻᳬ��100��
                clsmt.FetchAll("select * from dp.dp_middledatafile where datapartid=%d and tabid=%d %s limit 100",
                               datapartid,tabid,keepfiles?" and procstate>1 ":"");
                clsrn=clsmt.Wait();
                for(i=0; i<clsrn; i++) {
                    unlink(clsmt.PtrStr("datafilename",i));
                    unlink(clsmt.PtrStr("indexfilename",i));
                }
                st.Prepare("delete from dp.dp_middledatafile where datapartid=%d and tabid=%d %s limit 100",
                           datapartid,tabid,keepfiles?" and procstate>1 ":"");
                st.Execute(1);
                st.Wait();
            } while(clsrn>0);


            // ɾ���ɼ����ļ���¼
            st.Prepare("delete from dp.dp_filelog where tabid=%d and datapartid=%d %s",tabid,datapartid,
                       keepfiles? " and status<>2":"");
            st.Execute(1);
            st.Wait();

            // ɾ������õ�(�ȴ�װ����ļ�)
            clsrn = 0;
            do {
                clsmt.FetchAll("select * from dp.dp_datafilemap where datapartid=%d and tabid=%d limit 100",datapartid,tabid);
                clsrn=clsmt.Wait();
                for(int i=0; i<clsrn; i++) {
                    unlink(clsmt.PtrStr("filename",i));
                    unlink(clsmt.PtrStr("idxfname",i));
                }
                st.Prepare("delete from dp.dp_datafilemap where datapartid=%d and tabid=%d limit 100",datapartid,tabid);
                st.Execute(1);
                st.Wait();
            } while(clsrn>0);

            if(delete_partition) {

                DestLoader rm_part(&sp);

                // ɾ���Ѿ�װ��ı�Ķ�Ӧ����
                // 1:������ṹɾ����
                // 2:������ṹɾ������
                // 11:��������ṹɾ����
                // 12:��������ṹɾ������
                int ret = rm_part.RemoveTable(tabid,datapartid,true);
                if(ret == 1) { // ������ɾ����,��Ҫ�ؽ���ṹ,�´ν�����ִ��
                    lgprintf("tabid(%d),datapartid(%d)ɾ����������(%d),�ָ�����״̬Ϊ:status(%d)",tabid,datapartid,ret,origion_status);
                    sp.UpdateTaskStatus((TASKSTATUS)origion_status,tabid,datapartid);
                    return 0;
                }
            }

        } else if(keepfiles && !dp_parallel_load) { //�ļ������������һ����(bhloader���)
            // ��ȡdp.dp_middledatafile���Ѿ��ύ���ļ���¼��
            clsmt.FetchAll("select sum(recordnum) srn from dp.dp_middledatafile where tabid=%d and datapartid=%d and procstate<>0",
                           tabid,datapartid);
            clsmt.Wait();
            if(wociIsNull(clsmt,0,0)) {
                ThrowWith("��(%d),����(%d),����ʱ�ļ���dp.dp_middledatafile�в����ڼ�¼,���ܽ����ļ���������.\n",tabid,datapartid);
            }
            double srn=clsmt.GetDouble("srn",0);

            // ��ȡdp.dp_filelog���Ѿ��ύ���ļ���¼��
            clsmt.FetchAll("select sum(recordnum) srn from dp.dp_filelog where tabid=%d and datapartid=%d and status=2",
                           tabid,datapartid);
            clsmt.Wait();

            if(wociIsNull(clsmt,0,0)) {
                ThrowWith("��(%d),����(%d),����ʱ�ļ���dp.dp_filelog�в����ڼ�¼,���ܽ����ļ���������.\n",tabid,datapartid);
            }

            if(srn!=clsmt.GetDouble("srn",0)) {
                sp.log(tabid,datapartid,DUMP_FILE_LINES_ERROR,"�м�����%.0f��,�����ļ�%.0f�У���һ�£����ܼ�����һ�����ݳ�ȡ���������µ��롣",srn,clsmt.GetDouble("srn",0));
                ThrowWith("�м�����%.0f��,�����ļ�%.0f�У���һ�£����ܼ�����һ�����ݳ�ȡ���������µ��롣",srn,clsmt.GetDouble("srn",0));
            }

            //ɾ����һ��δ��������������dp.dp_middledatafile���ļ�
            {
                AutoStmt st(dtdbc);
                if(st.DirectExecute("delete from dp.dp_filelog where  tabid=%d and datapartid=%d and status<>2",tabid,datapartid)>0) {
                    lgprintf("�в����ļ��ϴ��쳣�˳�ʱδ�ύ����ʱ�ļ�����Ҫ�ӱ���Ŀ¼������ȷ�����ݲɼ��򿪡�");
                }

                // ɾ����һ��δ�ύ���ļ�dp.dp_middledatafile
                {
                    lgprintf("ɾ����%d,����%d��һ��δ�ύ�������ļ���Ϣ(dp.dp_middledatafile).",tabid,datapartid);
                    clsmt.FetchAll("select datafilename,indexfilename from dp.dp_middledatafile where  tabid=%d and datapartid=%d and procstate=0",tabid,datapartid);
                    int clsrn=clsmt.Wait();
                    for(int i=0; i<clsrn; i++) {
                        unlink(clsmt.PtrStr("datafilename",i));
                        unlink(clsmt.PtrStr("indexfilename",i));
                    }
                    st.DirectExecute("delete from dp.dp_middledatafile where  tabid=%d and datapartid=%d and procstate=0",tabid,datapartid);
                }
            }
        } else if(addfiles && !dp_parallel_load) { // �����ļ�������(bhloader���)
            //ɾ����һ��δ��������������dp.dp_middledatafile���ļ�
            AutoStmt st(dtdbc);
            if(st.DirectExecute("delete from dp.dp_filelog where  tabid=%d and datapartid=%d and status<>2",tabid,datapartid)>0) {
                lgprintf("�в����ļ��ϴ��쳣�˳�ʱδ�ύ����ʱ�ļ�����Ҫ�ӱ���Ŀ¼������ȷ�����ݲɼ��򿪡�");
            }

            // ɾ����һ��δ�ύ���ļ�dp.dp_middledatafile
            {
                lgprintf("ɾ����%d,����%d��һ��δ�ύ�������ļ���Ϣ(dp.dp_middledatafile).",tabid,datapartid);
                clsmt.FetchAll("select datafilename,indexfilename from dp.dp_middledatafile where  tabid=%d and datapartid=%d and procstate=0",tabid,datapartid);
                int clsrn=clsmt.Wait();
                for(int i=0; i<clsrn; i++) {
                    unlink(clsmt.PtrStr("datafilename",i));
                    unlink(clsmt.PtrStr("indexfilename",i));
                }
                st.DirectExecute("delete from dp.dp_middledatafile where  tabid=%d and datapartid=%d and procstate=0",tabid,datapartid);
            }
        } else if(keepfiles && dp_parallel_load) { //�ļ������������һ����(cluster_fm����Ⱥ�ϲ����)

            lgprintf("�����[�ļ�����<72>]����,����ϴε���������...");

            AutoStmt st(dtdbc);
            lgprintf("���δ�ύ����ʱ�ļ�(dp.dp_filelog).");
            if(st.DirectExecute("delete from dp.dp_filelog where  tabid=%d and datapartid=%d and status<>2",tabid,datapartid)>0) {
                lgprintf("�в����ļ��ϴ��쳣�˳�ʱδ�ύ����ʱ�ļ�����Ҫ�ӱ���Ŀ¼������ȷ�����ݲɼ��򿪡�");
            }

            lgprintf("���δ�ύ���Ự����(dp.dp_datapartext).");
            clsmt.FetchAll("select sessionid from dp.dp_datapartext where tabid =%d and datapartid = %d and commitstatus = -1 and sessiontype =1  ",tabid,datapartid);
            int _rt = clsmt.Wait();
            char cancle_session_cmd[256];
            for(int i=0; i<_rt; i++) {
                sprintf(cancle_session_cmd,"cfm-toolbox slcancel %s",clsmt.PtrStr("sessionid",i));
                lgprintf("Զ����ֹ�Ự,��������:%s",cancle_session_cmd);
                int __r = CR_Class_NS::system(cancle_session_cmd);
                lgprintf("Զ����ֹ�Ự,��������:%s ,return %d",cancle_session_cmd,__r);

            }

            add_stsessions_to_ltsession = true;
            st.DirectExecute("delete from dp.dp_datapartext where tabid=%d and datapartid=%d and commitstatus = -1 and sessiontype =1 ",tabid,datapartid);

        } else if(addfiles && dp_parallel_load) { // �����ļ�������(cluster_fm����Ⱥ�ϲ����)

            lgprintf("�����[���ļ�<73>]����,����ϴε���������...");

            AutoStmt st(dtdbc);
            lgprintf("���δ�ύ����ʱ�ļ�(dp.dp_filelog).");
            if(st.DirectExecute("delete from dp.dp_filelog where  tabid=%d and datapartid=%d and status<>2",tabid,datapartid)>0) {
                lgprintf("�в����ļ��ϴ��쳣�˳�ʱδ�ύ����ʱ�ļ�����Ҫ�ӱ���Ŀ¼������ȷ�����ݲɼ��򿪡�");
            }

            lgprintf("���δ�ύ���Ự����(dp.dp_datapartext).");

            clsmt.FetchAll("select sessionid from dp.dp_datapartext where tabid =%d and datapartid = %d and commitstatus = -1 and sessiontype =1  ",tabid,datapartid);
            int _rt = clsmt.Wait();
            char cancle_session_cmd[256];
            for(int i=0; i<_rt; i++) {
                sprintf(cancle_session_cmd,"cfm-toolbox slcancel %s",clsmt.PtrStr("sessionid",i));
                lgprintf("Զ����ֹ�Ự,��������:%s",cancle_session_cmd);
                int __r = CR_Class_NS::system(cancle_session_cmd);
                lgprintf("Զ����ֹ�Ự,��������:%s ,return %d",cancle_session_cmd,__r);
            }

            st.DirectExecute("delete from dp.dp_datapartext where tabid=%d and datapartid=%d and commitstatus = -1",tabid,datapartid);

            st_session_interval = 0;  // ���ļ�������,����ֻ�ó��Ự,����ֻ����һ���̻Ự,���Ự���滻����

            // �������񲢷���Ϊ0,���벹�ļ����̵Ĳɼ���������������񲢷�
            if(_slavercnt >0 ) {
                int ret = st.DirectExecute("update dp.dp_datapart set slavercnt=0,oldstatus=%d where tabid=%d and datapartid=%d",origion_status,tabid,datapartid);

                // �ٴ��ж�*.loading�ļ��Ƿ����,���ھ��˳�
                if(ret == 1) {
                    lgprintf("���ݲ��ļ�����,tabid(%d),datapartid(%d)�ɼ�����Ĳ���ִ����(%d--->0)ִ�гɹ�.",tabid,datapartid,_slavercnt);
                } else {
                    lgprintf("���ݲ��ļ�����,tabid(%d),datapartid(%d)�ɼ�����Ĳ���ִ����(%d--->0)ִ��ʧ��,�����н����ͻ�����˳�.",tabid,datapartid,_slavercnt);
                    lgprintf("tabid(%d),datapartid(%d)�ָ�����״̬Ϊ:status(%d)",tabid,datapartid,origion_status);
                    //sp.UpdateTaskStatus(origion_status,tabid,datapartid);
                    return 0;
                }
            }
        }
    } catch(...) {
        lgprintf("tabid(%d),datapartid(%d)�ָ�����״̬Ϊ:status(%d)",tabid,datapartid,origion_status);
        sp.UpdateTaskStatus((TASKSTATUS)origion_status,tabid,datapartid);
        return 0;
    }

    //indexparam *ip=&dp.idxp[dp.psoledindex];
    maxblockrn=blocksize/dp.rowlen;
    if(maxblockrn<MIN_BLOCKRN) {
        blocksize=MIN_BLOCKRN*dp.rowlen;
        maxblockrn=blocksize/dp.rowlen;
        if(blocksize<0 || blocksize>1024*1024*1024) {
            sp.log(tabid,datapartid,DUMP_RECORD_NUM_ERROR,"��%d,����%d ��¼����̫��,�޷�Ǩ��(��¼����%d)��",tabid,datapartid,dp.rowlen);
            return 0;
        }
        lgprintf("���ڼ�¼����̫��,���С����Ϊ%d,Ŀ���Ļ�����ƽ�ʧЧ��(��¼��:%d,��¼����%d)",blocksize,maxblockrn,dp.rowlen);
    } else if(maxblockrn>MAX_BLOCKRN) {
        blocksize=(MAX_BLOCKRN-1)*dp.rowlen;
        maxblockrn=blocksize/dp.rowlen;
        lgprintf("���ڼ�¼����̫С�����С����Ϊ%d�� (��¼��:%d,��¼����%d)",blocksize,maxblockrn,dp.rowlen);
    }

    //CMNET:�������봦��
    if(!keepfiles && !addfiles && !deal_slaver_node) {
        //�������ݵ���ʱ���ÿ��¼��,�Ժ�Ĵ���Ͳ�ѯ�Դ�Ϊ����
        //�ֶ�maxrecinblock��ʹ�÷������Ϊ:������ݺ�̨���õĲ����Զ�����,�������ֻ̨��
        lgprintf("����Ŀ�����ݿ�%d�ֽ�(��¼��:%d)",maxblockrn*dp.rowlen,maxblockrn);
        AutoStmt st(dtdbc);
        st.Prepare("update dp.dp_table set maxrecinblock=%d where tabid=%d",maxblockrn,dp.tabid);
        st.Execute(1);
        st.Wait();
    }
    sp.Reload();
    maxblockrn=sp.GetMaxBlockRn(tabid);
    AutoMt blockmt(0,maxblockrn);
    fnmt.Reset();
    //int partid=0;
    fnorder=0;
    bool dumpcomplete=false;
    try {

        if(dp_parallel_load) { // �ֲ�ʽ����װ��
            paraller_sorttm.Restart();
            b_master_node = false;
            b_master_node = is_master_node(dtdbc,tabid,datapartid);

            // �¹����̻Ựid
            if(b_master_node) { // ֻ��master�ڵ���Դ����ỰID,slaver�ڵ㲻�ܴ����ỰID
                // ��ȡ��ǰ�ỰID
                std::string session_id = generate_session_id(sp,tabid,datapartid);

                {
                    AutoLock lk(&sessionid_lock);
                    strcpy(stsessionid,session_id.c_str());
                    lt_parallel_load_rows = 0;

                    if(st_session_interval>0) { // ���Ự��Ч
                        if(keepfiles) {

                            add_stsessions_to_ltsession = true;

                            // ����Ⱥ�������������,��Ҫ���¹���һ���µĻỰID
                            AutoMt lt_mt(sp.GetDTS(),10);
                            lt_mt.FetchFirst("select sessionid,rownums from dp.dp_datapartext where tabid = %d and datapartid = %d and commitstatus = -1 and sessiontype= 2 limit 5",
                                             tabid,datapartid);

                            if(lt_mt.Wait()>0) {
                                lt_parallel_load_rows = lt_mt.GetLong("rownums",0);
                                char cancle_session_cmd[256];
                                sprintf(cancle_session_cmd,"cfm-toolbox slcancel %s",lt_mt.PtrStr("sessionid",0));
                                lgprintf("Զ����ֹ�Ự,��������:%s",cancle_session_cmd);
                                int __r = CR_Class_NS::system(cancle_session_cmd);
                                lgprintf("Զ����ֹ�Ự,��������:%s ,return %d",cancle_session_cmd,__r);


                                // �����µĻỰID
                                session_id = generate_session_id(sp,tabid,datapartid);
                                strcpy(ltsessionid,session_id.c_str());

                            } else {
                                lgprintf("����:�ļ�����ʱ��,�޷���ȡ��һ�γ��ỰID,���»�ȡ�̻ỰID��Ӧ�����ݼ�¼ʹ��");

                                // ��ȡ�̻ỰID�ļ�¼����
                                lt_mt.FetchFirst("select sum(rownums) cnt from dp.dp_datapartext where tabid = %d and datapartid = %d and commitstatus != -1 and sessiontype= 1 limit 5",
                                                 tabid,datapartid);
                                if(lt_mt.Wait()>0) {
                                    if(wociIsNull(lt_mt,0,0)) { // û�м�¼����:NULL
                                        lt_parallel_load_rows = 0;
                                    } else {
                                        lt_parallel_load_rows = lt_mt.GetLong("cnt",0);
                                        if(lt_parallel_load_rows<0) {
                                            lt_parallel_load_rows = 0;
                                        }
                                    }
                                    lgprintf("���¼���:tabid(%d),datapartid(%d)�ĳ��ػ�ID��¼��:%ld.",tabid,datapartid,lt_parallel_load_rows);
                                } else {
                                    lt_parallel_load_rows = 0;
                                    lgprintf("��������:tabid(%d),datapartid(%d)�ĳ��ػ�ID��¼��:%ld.",tabid,datapartid,lt_parallel_load_rows);
                                }
                            }

                            // ɾ�����Ự
                            AutoStmt st(sp.GetDTS());
                            st.DirectExecute("delete from dp.dp_datapartext where tabid=%d and datapartid=%d and commitstatus = -1 and sessiontype =2 ",tabid,datapartid);

                        } else {
                            session_id = generate_session_id(sp,tabid,datapartid);
                            strcpy(ltsessionid,session_id.c_str());
                        }
                    } else {
                        strcpy(ltsessionid,"NULL");
                    }

                }
            } else {
                // slaver�ڵ���Ҫ�ȴ�master�ڵ����ûỰid������
                paraller_sorttm.Restart();
                int _slaver_start_ret = 0;
                while(1) {
                    _slaver_start_ret = update_new_short_session(sp,tabid,datapartid,false);
                    if( -3 == _slaver_start_ret ) { // �÷���û�жϻỰֱ���˳�,�����еȴ�����
                        lgprintf("slaver�ڵ�[%d]û�з����п��õĶ̻ỰID,�����˳�����.....",slaver_index);
                        return 0;
                    } else if(0 != _slaver_start_ret ) {
                        lgprintf("tabid(%d),datapartid(%d)�ȴ�master�ڵ㴴���̻Ựid...",tabid,datapartid);
                        sleep(5);
                        if(paraller_sorttm.GetTime() > st_session_interval) { // fix dma-15444
                            lgprintf("tabid(%d),datapartid(%d)�ȴ�master�ڵ㴴���̻Ựid��ʱ(%d)��,slaver�ڵ��˳�.",tabid,datapartid,3*st_session_interval);
                            return 0;
                        }
                    } else {
                        break;
                    }
                };
                paraller_sorttm.Restart();
            }

            if(b_master_node) { // master �����ڵ�
                start_master_dump_data(sp,tabid,datapartid,keepfiles);
                slaver_index = 0;
            } else { // slaver �ڵ�,��������
                start_slaver_dump_data(sp,tabid,datapartid);
            }
            last_stsessionid[0]=0;
            clear_ssort_key_column(); // ��ʼ�������key
            st_parallel_load_rows = 0;
            parallel_load_obj_id = 0;

            e_extract_run_status = ENUM_RUN_CUR_SESSION;

            sort_timer.Restart();
        }
    } catch(char *str) {
        lgprintf(str);
        sp.log(tabid,datapartid,DUMP_UPDATE_TASK_STATUS_ERROR,str);
        return 0;
    }

    // >> begin: try to fix : dma-1835
    if(dp_parallel_load && !b_master_node) { // slaver �ڵ��ٴ��ж�master�Ƿ���finish״̬,������˳�!

        AutoMt mt_slaver(sp.GetDTS(),100);
        mt_slaver.FetchFirst("select clusterstatus from dp.dp_datapartext where tabid = %d and datapartid =%d and sessionid='%s' ",
                             tabid,datapartid,stsessionid);
        int _ret = mt_slaver.Wait();
        assert(_ret == 1);
        int _clusterstatus = mt_slaver.GetInt("clusterstatus",0);
        if(_clusterstatus == CS_MASTER_BUSY) {
            lgprintf("slaver�ڵ�[%d]��⵽�̻Ự[%s]�������ύ״̬,׼���˳�.",slaver_index,stsessionid);
            if(st_session_interval>0) {
                _ret = decrease_slaver_lanuch_count(sp,tabid,datapartid,true);
                lgprintf("���Ự[%s]��Ӧ��slaver�ڵ��������Լ���1[%s].",ltsessionid,_ret==1?"�ɹ�":"ʧ��");
            }
            _ret = decrease_slaver_lanuch_count(sp,tabid,datapartid,false);
            lgprintf("�̻Ự[%s]��Ӧ��slaver�ڵ��������Լ���1[%s].",stsessionid,_ret==1?"�ɹ�":"ʧ��");
            return 0;
        }
    }
    // << end : try to fix : dma-1835

    sp.logext(tabid,datapartid,EXT_STATUS_DUMPING,"");
    bool filesmode=false;
    LONG64 srn=0;
    char reg_backfile[300];
    try {
        bool ec=wociIsEcho();
        wociSetEcho(TRUE);

        if(sp.GetDumpSQL(tabid,datapartid,dumpsql)!=-1) {
            //>> Begin:fix jira dma-470,dma-471,dma-472,20130121
            RemoveContinueSpace(dumpsql);
            //<< End

            //idxdt.Reset();
            sp.log(tabid,datapartid,99,"���ݳ�ȡsql:%s.",dumpsql);
            TradeOffMt dtmt(0,realrn);
            blockmt.Clear();
            sp.BuildMtFromSrcTable(fmtdbc,tabid,&blockmt);
            //blockmt.Build(stmt);
            blockmt.AddrFresh();
            // CMNET :�ļ�ģʽ�жϣ�����dumpsql������
            // Load data from files using cmnet.ctl [exclude backup]
            // ��̬���ļ����в��ܰ����ո�
            char libname[300];
            bool withbackup=true;//default to true
            if(strcasestr(dumpsql,"load data from files")!=NULL) {
                filesmode=true;
                char *plib=strcasestr(dumpsql,"using ");
                if(!plib) {
                    sp.log(tabid,datapartid,DUMP_FILE_ERROR,"��%d,����%d,�ļ���������ȱ��using�ؼ���.",tabid,datapartid);
                    ThrowWith("�ļ���������ȱ��using�ؼ���.");
                }
                plib+=strlen("using ");
                strcpy(libname,plib);
                plib=libname;
                //end by blank or null
                while(*plib) {
                    if(*plib==' ') {
                        *plib=0;
                        break;
                    } else plib++;
                }
                if(strcasestr(dumpsql,"exclude backup")!=NULL) {
                    withbackup=false;
                }
            } else if(strcasestr(dumpsql,"load data")!=NULL) {
                sp.log(tabid,datapartid,DUMP_FILE_ERROR,"��%d,����%d,��ʽ����'%s',��ʹ��'load data from files using xxx.\n",tabid,datapartid,dumpsql);
                ThrowWith("��ʽ����'%s',��ʹ��'load data from files using xxx.\n",dumpsql);
            }

            sp.BuildMtFromSrcTable(fmtdbc,tabid,dtmt.Cur());
            //if(filesmode) dtmt.SetPesuado(true);
            //else
            sp.BuildMtFromSrcTable(fmtdbc,tabid,dtmt.Next());
            AutoStmt stmt(srcdbc);
            //CMNET :�ļ�ģʽ
            if(!filesmode) {
                stmt.Prepare(dumpsql);

                //>> Begin: fix dm-230 , ���¹���Դ��mt
                sp.IgnoreBigSizeColumn(srcdbc,dumpsql);
                //<< End: fix dm-230

                //>> Begin:fix DM-451
                sp.AdjustCryptoColumn(stmt,tabid);
                //<< End: fix DM-451

                AutoMt tstmt(0,1);
                tstmt.Build(stmt);

                // try to fix dma-1593:skip check table format
                char *pcheck_format = NULL;
                pcheck_format = getenv("DP_CHECKFORMAT");
                int _check_format = 1;
                if(pcheck_format != NULL) {
                    if(strlen(pcheck_format) == 1 && atoi(pcheck_format) == 0) {
                        lgprintf("�Ѿ����û�������:DP_CHECKFORMAT=0������ʽ����ṹУ��.");
                        _check_format = 0; // ����ҪУ���ʽ����ṹ
                    }
                }
                if(_check_format != 0) {
                    if(blockmt.CompatibleMt(tstmt)!=0 ) {
                        sp.log(tabid,datapartid,DUMP_SQL_ERROR,"��%d,����%d,���ݳ�ȡ��� %s �õ��ĸ�ʽ��Դ����ĸ�ʽ��һ��.\n",tabid,datapartid,dumpsql);
                        ThrowWith("�������ݳ�ȡ��� %s �õ��ĸ�ʽ��Դ����ĸ�ʽ��һ��.\n",dumpsql);
                    }
                }

                wociReplaceStmt(*dtmt.Cur(),stmt);
                wociReplaceStmt(*dtmt.Next(),stmt);
            }
            dtmt.Cur()->AddrFresh();
            dtmt.Next()->AddrFresh();


            //>> Begin: fix dm-230
            char cfilename[256];
            strcpy(cfilename,getenv("DATAMERGER_HOME"));
            strcat(cfilename,"/ctl/");
            strcat(cfilename,MYSQL_KEYWORDS_REPLACE_LIST_FILE);
            sp.ChangeMtSqlKeyWord(*dtmt.Cur(),cfilename);
            sp.ChangeMtSqlKeyWord(*dtmt.Next(),cfilename);
            //<< End: fix dm-230

            //dtmt.Cur()->Build(stmt);
            //dtmt.Next()->Build(stmt);
            //׼����������������������
            //CMNET :�ļ�ģʽ
            int rn;
            bool filecp=false;
            bool needcp=false;
            int uncheckfct=0,uncheckbct=0;//�������ļ��������ݿ���

            //>> Begin: DM-201 , modify by liujs
            IDumpFileWrapper *dfw;
            IFileParser *fparser;
            //<< End: modify by liujs

            bool filehold=false;//true: �ļ������У��ڴ����)
            bool extractend_thread = false; // true: �ļ����ݳ�ȡ����
            bool extractend_main = false; // true: �����ļ����ݳ�ȡ����
            int  continue_extractend_times = 0; // master�ڵ�����3�ζ�û�еȵ������ݾͽ���
            bool single_file_finish = false;    // �ж��ļ��Ƿ��������
            bool slaver_exit = false;           // slaver�Ƿ��˳�
            ThreadList pickThread;

            if(filesmode) {
                dfw=new DumpFileWrapper(libname);
                fparser=dfw->getParser();
                try {
                    fparser->PluginPrintVersion();
                } catch(...) {
                    lgprintf("ʹ�õ�plugin���ϣ�ȱ���汾��Ӧ��Ϣ,PluginPrintVersion error !");
                    return -1;
                }
                fparser->SetTable(plugin_sp.GetDTS(),tabid,datapartid,dfw);

                //Get file and parser
                //����ϴε��ļ�δ������(�ڴ����),���������ļ��������ֽ����
                while(true) {
                    if(dp_parallel_load) {
                        AutoLock ak(&sessionid_lock);
                        fparser->SetSession(slaver_index);
                    }
                    if(!filehold) {
                        int gfstate=fparser->GetFile(withbackup,plugin_sp,tabid,datapartid);
                        if(gfstate==0) {
                            //û��������Ҫ����
                            if(fparser->ExtractEnd(plugin_sp.GetDTS())) {
                                extractend_main = true;
                                break;
                            } else {
                                // ��ȡ�����ļ���,��ô��ʱ�Ͳ��õ���
                                if(dp_parallel_load && slaver_index>0) { // slaver �ڵ㲻�õ�
                                    if(st_session_interval>0 && paraller_sorttm.GetTime() > st_session_interval) {
                                        extractend_main = true;
                                        lgprintf("��(%d),����(%d),SlaverID(%d)��ȡ�����ļ�,�̻Ựʱ�䵽�뿪��һ���ɼ�",tabid,datapartid,slaver_index);
                                        break;
                                    }
                                }

                                if(dp_parallel_load && slaver_index == 0) { // master �ڵ�
                                    if(fparser->GetForceEndDump() == 1) { // ֻ�вɼ�һ�����ݵ�ʱ��,���˳�
                                        if(st_session_interval>0 && paraller_sorttm.GetTime() > 1*st_session_interval) {
                                            extractend_main = true; // master �ڵ�
                                            lgprintf("��(%d),����(%d),SlaverID(%d)��ȡ�����ļ�,�̻Ựʱ�䵽�뿪��һ���ɼ�A",tabid,datapartid,slaver_index);
                                            break;
                                        }
                                    } else {
                                        if(st_session_interval>0 && paraller_sorttm.GetTime() > 2*st_session_interval) {
                                            extractend_main = false; // master �ڵ�Ҫ�ȴ�
                                            lgprintf("��(%d),����(%d),SlaverID(%d)��ȡ�����ļ�,�̻Ựʱ�䵽�뿪��һ���ɼ�B",tabid,datapartid,slaver_index);
                                            break;
                                        }
                                    }
                                }
                            }

                            extractend_main = false;
                            //if(dtmt.Cur()->GetRows()>0) break;
                            mySleep(fparser->GetIdleSeconds());

                            //>> begin: fix dma-1326: �����ļ��������,�ж��Ƿ��ύʱ��,������ύʱ���˾��ύ�ļ�
                            if(dp_parallel_load ) {

                                if(paraller_sorttm.GetTime() > st_session_interval &&
                                   dtmt.Cur()->GetRows() > 0) {
                                    break;
                                }
                            }
                            //<< end: fix dma-1326

                            continue;
                        } else if(gfstate==2) //�ļ����󣬵�Ӧ�ú���
                            continue;
                        uncheckfct++;
                    }
                    int frt=fparser->DoParse(*dtmt.Cur(),plugin_sp,tabid,datapartid);
                    filehold=false;
                    if(frt==-1) {
                        sp.log(tabid,datapartid,DUMP_FILE_ERROR,"��%d,����%d,�ļ��������.",tabid,datapartid);
                        ThrowWith("�ļ��������.");
                        break;
                    } else if(frt==0) {
                        //memory table is full,and file is not eof
                        filehold=true;
                        break;
                    } else if(frt==2) {
                        //memory table is full and file is eof
                        break;
                    }
                    //>> begin: fix dma-1326: �����ļ��������,�ж��Ƿ��ύʱ��,������ύʱ���˾��ύ�ļ�
                    if(dp_parallel_load && frt == 1 ) {

                        if(paraller_sorttm.GetTime() > st_session_interval&&
                           dtmt.Cur()->GetRows() > 0) {
                            break;
                        }
                    }
                    //<< end: fix dma-1326
                }

                single_file_finish = !filehold;

                rn=dtmt.Cur()->GetRows();
            } else {
                dtmt.FetchFirst();
                rn=dtmt.Wait();
            }

            srn=rn;
            //�ļ������Ƿ��ѵ���
            double fillrate=0;
            int forcedatatrim=0;
            char *pfdt=getenv("DP_FORCEDATATRIM");
            if(pfdt!=NULL) forcedatatrim=atoi(pfdt);
            if(forcedatatrim==1) lgprintf("..FDT");
            bool commitCheck=false;

            if(rn == 0 && b_master_node) { // ��һ�ξ�û�вɼ�������,û�е���:commit_sort_data_process
                // slaver �ڵ�ɼ��������ݾ��˳�
                extractend_main = false;
            }

            // �жϲ����������,�ļ����ݲɼ�,master�ڵ��Ƿ����ڲɼ�����
            bool master_file_extract_running = false;
            if(dp_parallel_load && b_master_node) {
                if(filesmode) {
                    if(continue_extractend_times <CONTINUE_EXTRACT_END_MAX_TIMES) {
                        master_file_extract_running = true;
                    }
                }
            }
            while(rn >0 || master_file_extract_running ) {
                //>> Begin: fix DMA-451
                sp.CryptoMtColumn(dtmt.Cur(),tabid);
                //<< End: fix DMA-451

                if(!filesmode) {
                    dtmt.FetchNext();
                } else {
                    {
                        //lgprintf("�ύ���㣺���ݿ�%d,�ļ���%d,�����:%.1f%%.",uncheckfct,uncheckbct,fillrate*100);
                        //�Ե�������������ļ�����Ϊ10
                        AutoStmt st(dtdbc);
                        wociSetEcho(false);
                        if(!dp_parallel_load) {
                            st.DirectExecute("update dp.dp_filelog set status=10 where tabid=%d and datapartid=%d and status=1"
                                             ,tabid,datapartid);
                        } else {
                            st.DirectExecute("update dp.dp_filelog set status=10 where tabid=%d and datapartid=%d and slaveridx = %d and status=1"
                                             ,tabid,datapartid,slaver_index);
                        }
                        wociSetEcho(true);
                    }

                    // pickup file and parse in seprate thread.
                    threadmtrows = 0;
                    void *params[20];
                    params[0]=&plugin_sp;
                    params[1]=fparser;
                    params[2]=(void*)&filehold;
                    params[3]=&datapartid;
                    params[4]=&dtmt;
                    params[5]=(void*)withbackup;
                    params[6]=&tabid;
                    params[7]=&uncheckbct;
                    params[8]=&uncheckfct;
                    params[9]=(void*)&dp_parallel_load;
                    if(dp_parallel_load) {
                        params[10]=(void*)stsessionid;
                        params[11]=(void*)ltsessionid;
                        params[12]=&slaver_index;
                        params[13]=&sessionid_lock;
                        params[14]=&paraller_sorttm;
                        params[15]=&st_session_interval;
                        params[16]=(void*)&extractend_thread;
                        params[17]=&threadmtrows;
                    }
                    params[18]=NULL;
                    pickThread.SetReturnValue(0);
                    pickThread.Start(params,20,PickThread);
                }
                lgprintf("��ʼ���ݴ���");
                int retryct=0;
                while(true) {
                    try {
                        freeinfo1("before call prcblk");
                        for(int i=0; i<dp.soledindexnum; i++) {
                            int mt_rows = dtmt.Cur()->GetRows();

                            ProcBlock(sp,tabid,datapartid,dtmt.Cur(),i/*dp.psoledindex*/,blockmt,sp.NextTmpFileID(),filesmode);

                            if(dp_parallel_load) { // �̻Ựid�Ĵ���

                                // assert(dp.soledindexnum == 1);

                                if(st_session_interval > 0 ) {

                                    // �̻Ự�ύʱ������ʱ����
                                    // ���ļ��ɼ���ʱ��,ֻ�е����ļ�������ɲ����л��Ự
                                    if((paraller_sorttm.GetTime() > st_session_interval ) && ( filesmode && single_file_finish )) { // �ύ�̻Ựid

                                        // �ύ�̻ỰID
                                        // �ļ��ɼ����ļ�״̬����,��commit_sort_data_process�н��д���
                                        commit_sort_data_process(sp,tabid,datapartid,true,false,false,filesmode);

                                        // �ȴ��µĻỰID
                                        e_extract_run_status = ENUM_WAIT_NEW_SESSION;

                                        if(extractend_main) { // ���̳߳�ȡ���ݽ���

                                            if(!b_master_node ) {
                                            label_slaver_exit:
                                                // slaver�ڵ��ļ���ȡ����,���贴���µĻỰ��
                                                lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)�ļ����ݳ�ȡ����,����ȴ�master�ڴ����Ựid.",tabid,datapartid,slaver_index);
                                                break;

                                            } else {
                                                // master�ڵ�Ҫ���Ự���Ƿ�������slaver�ڵ���������
                                                // ����в����˳�,��Ҫ�����ȴ�
                                                int _launch_slv_cnt = 0;
                                                int _finish_slv_cnt = 0;
                                                int64 _loadrows = 0;
                                                int _clusterstatus = 0;
                                                is_all_slaver_node_finish(sp.GetDTS(),tabid,datapartid,stsessionid,
                                                                          _launch_slv_cnt,_finish_slv_cnt,_loadrows,_clusterstatus);

                                                if(_loadrows > 0) {
                                                    lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)�ļ����ݳ�ȡ����,�ȴ�����slaver�ڵ������ݲɼ�����,��Ҫ�����Ự,�ȴ�����(%d).",tabid,datapartid,slaver_index,continue_extractend_times);
                                                    continue_extractend_times = 0;
                                                } else {
                                                    if(_launch_slv_cnt >0) { // exist slaver node
                                                        if(continue_extractend_times < CONTINUE_EXTRACT_END_MAX_TIMES) {
                                                            lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)�ļ����ݳ�ȡ����,�ȴ�����slaver�ڵ������ݲɼ�����,��Ҫ�����Ự,�ȴ�����(%d).",tabid,datapartid,slaver_index,continue_extractend_times);
                                                        } else { // slaver �ڵ�û����������
                                                            lgprintf("tabid(%d),datapartid(%d)�ļ����ݳ�ȡ����,����slaver�ڵ�����������Ҫ�ɼ�����.",tabid,datapartid);
                                                            continue_extractend_times++;
                                                            break;
                                                        }
                                                    } else {
                                                        lgprintf("tabid(%d),datapartid(%d)�ļ����ݳ�ȡ����,ֻ��masterһ���ڵ㴦������,continue_extractend_times(%d).",tabid,datapartid,continue_extractend_times);

                                                        if(continue_extractend_times > CONTINUE_EXTRACT_END_MAX_TIMES) {
                                                            lgprintf("tabid(%d),datapartid(%d)�ļ����ݳ�ȡ����,continue_extractend_times(%d),�������ݲɼ�����.",tabid,datapartid,continue_extractend_times);
                                                            continue_extractend_times=4;
                                                            break;
                                                        }
                                                    }
                                                }
                                                continue_extractend_times++;
                                                lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)�ļ����ݳ�ȡ����,����slaver�ڵ�����������Ҫ�ɼ�����,��Ҫ�����Ự.�ȴ�����(%d),�ȴ�ʱ��30��.",tabid,datapartid,slaver_index,continue_extractend_times);
                                                sleep(30);
                                            }
                                        } else {
                                            lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)�ļ����ݳ�ȡδ����",tabid,datapartid,slaver_index);
                                            continue_extractend_times = 0;
                                        }

                                        paraller_sorttm.Restart();

                                        // �¹����̻Ựid
                                        if(b_master_node) { // ֻ��master�ڵ���Դ����ỰID,slaver�ڵ㲻�ܴ����ỰID
                                            create_new_short_session(sp,tabid,datapartid);
                                        } else {
                                            // slaver�ڵ���Ҫ�ȴ�master�ڵ����ûỰid������
                                            slaver_exit = false;

                                            //  �������6��,�ñ��ж�û�����ڴ���Ķ̻Ựid,˵���̻Ự�Ѿ��������,��ʱ�ڵȴ����Ự������,ֱ���˳�����
                                            int retry_times = 0;
                                            int _slaver_start_ret2 = 0;
                                            while(1) {

                                                _slaver_start_ret2 = update_new_short_session(sp,tabid,datapartid,true);

                                                // �жϸñ����Ƿ����û�д���Ķ̻ỰID,������ڼ����ȴ�,
                                                if(-3 == _slaver_start_ret2) {
                                                    // û�ж̻Ự����,ֱ�ӷ���
                                                    long _tmp_row = 0;
                                                    {
                                                        AutoLock ak(&sessionid_lock);
                                                        _tmp_row = threadmtrows;
                                                    }
                                                    lgprintf("tabid(%d),datapartid(%d)�ȴ�master�ڵ㴴���̻Ựid,retry_times(%d),threadmtrows(%ld).",tabid,datapartid,retry_times,_tmp_row);
                                                    if(retry_times++>60 && _tmp_row == 0) { // û�������˲��˳�
                                                        goto label_slaver_exit;
                                                    }
                                                    sleep(10);

                                                } else if(0 != _slaver_start_ret2) {

                                                    lgprintf("tabid(%d),datapartid(%d)�ȴ�master�ڵ㴴���̻Ựid,�ȴ�10��.",tabid,datapartid);
                                                    sleep(10);
                                                    retry_times = 0;

                                                    if( -5 == _slaver_start_ret2) {
                                                        // master�ڵ�����init�̻Ự������,�����ȴ�

                                                        if(paraller_sorttm.GetTime() > 5 * st_session_interval) {
                                                            slaver_exit = true; // slaver ����Ҫ�˳���
                                                            lgprintf("tabid(%d),datapartid(%d)�ȴ�master�ڵ㴴���̻Ựid,����:%d.",tabid,datapartid,_slaver_start_ret2);
                                                            goto label_slaver_exit;
                                                        }
                                                    } else if (-4 == _slaver_start_ret2) {
                                                        // master�ڵ�����finish���ݹ�����,�����ȴ�

                                                        int __wait_time = 0;
                                                        // ��5����Ϊ��λ���еȴ�
                                                        __wait_time = (st_session_interval<(5*60)) ? (5*60): st_session_interval;

                                                        if(paraller_sorttm.GetTime() > 10 * __wait_time ) {
                                                            slaver_exit = true; // slaver ����Ҫ�˳���
                                                            lgprintf("tabid(%d),datapartid(%d)�ȴ�master�ڵ㴴���̻Ựid,����:%d.",tabid,datapartid,_slaver_start_ret2);
                                                            goto label_slaver_exit;
                                                        }
                                                    } else if (-2 == _slaver_start_ret2 ) {
                                                        // master�ڵ�����push������,�����ȴ�
                                                        if(paraller_sorttm.GetTime() > 10 * st_session_interval) {
                                                            slaver_exit = true; // slaver ����Ҫ�˳���
                                                            lgprintf("tabid(%d),datapartid(%d)�ȴ�master�ڵ㴴���̻Ựid,����:%d.",tabid,datapartid,_slaver_start_ret2);
                                                            goto label_slaver_exit;
                                                        }
                                                    }

                                                } else {
                                                    // �ҵ����������Ķ̻Ựid
                                                    break;
                                                }

                                                int cluster_status = 0;
                                                cluster_status = get_cluster_status(sp.GetDTS(),tabid,datapartid,stsessionid);
                                                if(cluster_status == CS_ERROR) {
                                                    ThrowWith("SLAVER�ڵ�,�ȴ�MASTER�ڵ���ɹ����з��ּ�Ⱥ��ǰ�Ự[%s]����,�˳�.",stsessionid);
                                                }

                                            };

                                            // ������������ʱ���slaver�ڵ���Ŀ
                                            increase_slaver_lanuch_count(sp,tabid,datapartid,false);

                                            e_extract_run_status = ENUM_SLAVER_WAIT_RUN_SESSION;

                                            sleep(10); // slaver ��ȡ��master�����Ķ̻Ựid��,�ȴ�10��,�Ա�����masterǰ����

                                        }
                                        extractend_main = false;

                                        // �¹����̻Ựid��Ӧ��sort����
                                        if(b_master_node) {
                                            lgprintf("MASTER�ڵ�,�̻ỰID(%s)׼�������������,���ü�Ⱥ״̬Ϊ:CS_MASTER_INIT",stsessionid);
                                            set_cluster_status(sp.GetDTS(),tabid,datapartid,stsessionid,CS_MASTER_INIT);
                                        }
                                        int ret = create_DMIBSortLoader(sp,tabid,datapartid,false);

                                        if(b_master_node) { // master �ڵ����ûỰ��Ϣ
                                            if(-1 == ret) {
                                                // ���ü�ȺΪ������״̬
                                                // �˴���������,��Ⱥ�������û�д�������
                                                update_new_short_session(sp,tabid,datapartid,CS_ERROR);
                                                ThrowWith("master�ڵ�DataDump::create_DMIBSortLoader��������ʧ��,����tabid(%d),datapartid(%d),�Ự(%s)��Ⱥ״̬Ϊ����(CS_ERROR),׼���˳�!",
                                                          tabid,datapartid,stsessionid);

                                            } else { // ��Ⱥ������󴴽��ɹ�
                                                update_new_short_session(sp,tabid,datapartid,CS_OK);
                                                lgprintf("master�ڵ�ֲ�ʽ���򴴽��̻ỰID�ɹ�.");
                                            }

                                        } else { // slaver�ڵ��˳�����
                                            if(-1 == ret) {

                                                lgprintf("Slaver�ڵ㴴��p_stDMIBSortLoader ����,����ʧ��.");

                                                // �̻Ự�Լ���
                                                decrease_slaver_lanuch_count(sp,tabid,datapartid,false);
                                                if(st_session_interval>0) {
                                                    // ���Ự�Լ���
                                                    decrease_slaver_lanuch_count(sp,tabid,datapartid,true);
                                                }

                                                ThrowWith("Slaver�ڵ�DataDump::create_DMIBSortLoader��������ʧ��,tabid(%d),datapartid(%d),�Ự(%s)׼���˳�!",
                                                          tabid,datapartid,stsessionid);
                                            } else {
                                                lgprintf("Slaver�ڵ�DataDump::create_DMIBSortLoader��������ɹ�,tabid(%d),datapartid(%d),�Ự(%s).",
                                                         tabid,datapartid,stsessionid);
                                            }
                                        }

                                        paraller_sorttm.Restart();

                                        // �ỰID���³ɹ�,���Թ���
                                        e_extract_run_status = ENUM_RUN_CUR_SESSION;

                                    } else if( mt_rows == 0) {
                                        int sleep_num = st_session_interval / 3;
                                        lgprintf("û�вɼ�������,����ʱ��%d��.",sleep_num);
                                        sleep(sleep_num);
                                    }
                                } else {
                                    if(mt_rows == 0) {
                                        break;
                                    }
                                }
                            }
                        }


                        lgprintf("���ݴ������");

                        if(fnmt.GetRows()>0 && !dp_parallel_load) {
                            wociSetEcho(false);
                            wociAppendToDbTable(fnmt,"dp.dp_middledatafile",dtdbc,true);
                            wociReset(fnmt);
                            wociSetEcho(true);
                        }

                        if(filesmode) {
                            wociReset(*dtmt.Cur());
                            //������ύ����
                            //������һ���ļ���������������������ļ��Ĵ����м�����ύ
                            //�ﵽ5�����ݿ���߷��������Ѿ�������������ύ
                            //���ߴﵽ�򳬹�2�����ݿ�δ�ύ�������һ�����ݿ��Ѿ�����80%--���߳���10���ļ�δ�ύ
                            //������ļ������ֻ���Ҫ��һ�µĵ���
                            if(commitCheck) {
                                lgprintf("�ύ���㣺���ݿ�%d,�ļ���%d,�����:%.1f%%.",uncheckbct,uncheckfct,fillrate*100);

                                AutoStmt st(dtdbc);
                                wociSetEcho(false);
                                if(!dp_parallel_load) {
                                    st.DirectExecute("update dp.dp_filelog set status=2 where tabid=%d and datapartid=%d and status=10"
                                                     ,tabid,datapartid);

                                    st.DirectExecute("update dp.dp_middledatafile set procstate=1 where tabid=%d and datapartid=%d and procstate=0",tabid,datapartid);
                                }
                                wociSetEcho(true);
                            }
                        }
                        break;
                    } catch(...) {
                        if(!dp_parallel_load) {
                            if(retryct++>20) {
                                sp.log(tabid,datapartid,DUMP_WRITE_FILE_ERROR,"��%d,����%d,д����ʧ��.",tabid,datapartid);
                                throw;
                            }
                            lgprintf("д����ʧ�ܣ�����...");
                        }
                        throw;
                    }
                }
                freeinfo1("after call prcblk");

                if(filesmode) {
                    // �ȴ��̴߳������
                    pickThread.Wait();
                    rn=pickThread.GetReturnValue();
                    single_file_finish = !filehold; // �����ļ��Ƿ��������
                    extractend_main = extractend_thread; // �ļ���ȡ������
                    dtmt.flip();
                    fillrate=(double)dtmt.Cur()->GetRows()/dtmt.Cur()->GetMaxRows();
                    uncheckbct++;
                    if(forcedatatrim==1 ||(!filehold && ( uncheckbct>8 || (uncheckbct>5 && fillrate>0.9) || fparser->ExtractEnd(sp.GetDTS())))) {
                        commitCheck=true;
                        uncheckbct=uncheckfct=0;
                    } else {
                        commitCheck=false;
                    }

                    // >>begin: fix dma-933
                    if(rn == 0 && commitCheck) {
                        lgprintf("�ύ���㣺���ݿ�%d,�ļ���%d,�����:%.1f%%.",uncheckbct,uncheckfct,fillrate*100);
                        AutoStmt st(dtdbc);
                        wociSetEcho(false);
                        if(!dp_parallel_load) {
                            st.DirectExecute("update dp.dp_filelog set status=2 where tabid=%d and datapartid=%d and status=10"
                                             ,tabid,datapartid);

                            st.DirectExecute("update dp.dp_middledatafile set procstate=1 where tabid=%d and datapartid=%d and procstate=0"
                                             ,tabid,datapartid);
                        }

                        wociSetEcho(true);

                    }
                    //<<end: fix dma-933
                } else {
                    rn=dtmt.Wait();
                }

                srn+=rn;

                if(b_master_node) {
                    if(rn == 0) {
                        if(filesmode) {
                            if(fparser->GetForceEndDump() == 1) { // master�ڵ�û�вɼ�������
                                lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)��ȡ����¼��Ϊ0,[files::forceenddump==1].",tabid,datapartid,slaver_index);
                                extractend_main = true;
                            } else {
                                lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)��ȡ����¼��Ϊ0.",tabid,datapartid,slaver_index);
                            }
                        } else {
                            lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)��ȡ����¼��Ϊ0,�����ݿ��ȡ����.",tabid,datapartid,slaver_index);
                            extractend_main = true;
                        }
                    }
                } else {

                    run_slaver_taskinfo_deal_rows += rn;

                    if(rn == 0) { // ���һ��û�вɼ�������,try to fix dma-1460
                        lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)��ȡ����¼��Ϊ0.",tabid,datapartid,slaver_index);
                        extractend_main = true;
                    }
                }

                if(!b_master_node && slaver_exit) {

                    lgprintf("tabid(%d),datapartid(%d),slaver_index(%d) û�л�ȡ��master�ڵ㴴���ĻỰID�˳���",tabid,datapartid,slaver_index);

                    // fix dma-2017: slaver�����˳���ʱ��,���ܽ�dp.dp_filelog�еļ�¼ɾ��,�����������,���´��������е�ʱ��,��ɾ������
                    AutoStmt st1(dtdbc);

                    // fix dma-2018: ֻɾ��endproctime is null��
                    st1.DirectExecute("delete from  dp.dp_filelog where tabid=%d and datapartid=%d and slaveridx = %d and status in (0) and endproctime is null",tabid,datapartid,slaver_index);

                    break;
                }

                master_file_extract_running = false;
                if(dp_parallel_load && b_master_node && st_session_interval > 0 ) {
                    if(filesmode) {
                        if(continue_extractend_times <=CONTINUE_EXTRACT_END_MAX_TIMES) {
                            master_file_extract_running = true;
                        } else {
                            master_file_extract_running = false;
                        }
                    }
                }

            } // while(rn>0)

            lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)���ݳ�ȡ���.",tabid,datapartid,slaver_index);

            wociSetEcho(ec);
            if(fnmt.GetRows()>0 && !dp_parallel_load) {
                wociAppendToDbTable(fnmt,"dp.dp_middledatafile",dtdbc,true);
                wociReset(fnmt);
            }
            if(!dp_parallel_load) {
                if(!filesmode || (filesmode && fparser->ExtractEnd(sp.GetDTS()))) {
                    dumpcomplete=true;
                    sp.UpdateTaskStatus(DUMPED,tabid,datapartid);
                }
            } else { // �ֲ�ʽ����,�ύ��������������
                int ret = 0;

                if(p_stDMIBSortLoader == NULL) { // �ɼ�����û�вɼ�������
                    e_extract_run_status = ENUM_WAIT_NEW_SESSION;

                    if(b_master_node) {
                        lgprintf("MASTER�ڵ�,�̻ỰID(%s),���ü�Ⱥ״̬Ϊ:CS_MASTER_INIT",stsessionid);
                        set_cluster_status(sp.GetDTS(),tabid, datapartid,stsessionid,CS_MASTER_INIT);

                        if(st_session_interval>0) {
                            lgprintf("MASTER�ڵ�,���ỰID(%s),���ü�Ⱥ״̬Ϊ:CS_MASTER_INIT",ltsessionid);
                            set_cluster_status(sp.GetDTS(),tabid, datapartid,ltsessionid,CS_MASTER_INIT);
                        }
                    }
                    ret = CreateEmptyDMIBSortLoad(sp,tabid,datapartid,dtmt.Cur());

                    if(ret == 0) {
                        if(b_master_node) {
                            lgprintf("MASTER�ڵ�,�̻ỰID(%s)��������������,���ü�Ⱥ״̬Ϊ:CS_OK",stsessionid);
                            set_cluster_status(sp.GetDTS(),  tabid, datapartid,stsessionid,CS_OK);

                            if(st_session_interval>0) {
                                lgprintf("MASTER�ڵ�,���ỰID(%s)��������������,���ü�Ⱥ״̬Ϊ:CS_OK",ltsessionid);
                                set_cluster_status(sp.GetDTS(),  tabid, datapartid,ltsessionid,CS_OK);
                            }
                        }
                    }
                }

                // �ύ����������,master�ڵ����ȵ����е�slaver�ڵ��ύ��ɺ�Ž��������ύ��
                if(!filesmode) {
                    if(ret == 0 && p_stDMIBSortLoader != NULL) {
                        e_extract_run_status = ENUM_RUN_CUR_SESSION;
                        ret = commit_sort_data_process(sp,tabid,datapartid,true,true,true,filesmode);
                    } else {
                        lgprintf("skip commit_sort_data_process ,tabid(%d),datapartid(%d),slaver_index(%d),stsessionid(%s),ltsessionid(%s).",tabid,datapartid,slaver_index,stsessionid,ltsessionid);
                        lgprintf("p_stDMIBSortLoader ����Ϊnull ���ܽ���DMIBSortLoad::Finish����.");
                    }
                } else {
                    // ���һ���̻Ự����Ҫ�ύ,����ж̻Ự���ύ�ᵼ�º�̨��̻Ự��Ӧ��cluster_fmһֱ�ȴ�finish���̣�����
                    if(ret == 0 && p_stDMIBSortLoader != NULL) {
                        ret = commit_sort_data_process(sp,tabid,datapartid,true,true,true,filesmode);
                    } else {
                        lgprintf("skip commit_sort_data_process ,tabid(%d),datapartid(%d),slaver_index(%d),stsessionid(%s),ltsessionid(%s).",tabid,datapartid,slaver_index,stsessionid,ltsessionid);
                        if(!b_master_node) {
                            lgprintf("p_stDMIBSortLoader ����Ϊnull ���ܽ���DMIBSortLoad::Finish����.");

                            // �̻Ự�Լ���
                            decrease_slaver_lanuch_count(sp,tabid,datapartid,false);
                            if(st_session_interval>0) {
                                // ���Ự�Լ���
                                decrease_slaver_lanuch_count(sp,tabid,datapartid,true);
                            }
                        }
                    }
                }

                e_extract_run_status = ENUM_WAIT_NEW_SESSION;

                if(ret == 0) {
                    dumpcomplete=true;
                } else {
                    dumpcomplete=false;
                }
            }
            if(filesmode) {
                delete dfw;
                dfw = NULL;
            }
        }
    } catch(...) {
        int frn=wociGetMemtableRows(fnmt);
        sp.logext(tabid,datapartid,EXT_STATUS_ERROR,"");
        errprintf("���ݵ����쳣��ֹ����%d(%d),�м��ļ���:%d.",tabid,datapartid,frn);
        AutoStmt st(dtdbc);
        st.DirectExecute("unlock tables");
        sp.log(tabid,datapartid,DUMP_EXCEPTION_ERROR,"��%d,����%d ���ݵ����쳣��ֹ���м��ļ���:%d.",tabid,datapartid,frn);
        bool restored=false;

        if(dumpcomplete && !dp_parallel_load ) {
            //��ǰ����ĵ�������ɣ����޸�DP����ʧ��.����10��,�����Ȼʧ��,�����.
            int retrytimes=0;
            while(retrytimes<10 &&!restored) {
                restored=true;
                try {
                    wociAppendToDbTable(fnmt,"dp.dp_middledatafile",dtdbc,true);
                    sp.UpdateTaskStatus(DUMPED,tabid,datapartid);
                } catch(...) {
                    sp.log(tabid,datapartid,DUMP_FINISHED_NOTIFY,"��%d(%d)���������,��д��dp������(dp_middledatafile)ʧ��,һ���Ӻ�����(%d)...",tabid,datapartid,++retrytimes);
                    lgprintf("��%d(%d)���������,��д��dp������(dp_middledatafile)ʧ��,һ���Ӻ�����(%d)...",tabid,datapartid,++retrytimes);
                    restored=false;
                    mSleep(60000);
                }
            }
        }

        if(!restored) {
            int i;
            wdbi_kill_in_progress=false;
            wociMTPrint(fnmt,0,NULL);

            //�����ָ�����״̬�Ĳ���,��Ϊ����״̬���˹�������Ϊ����.������ݿ�����һֱû�лָ�,
            //������״̬�ָ��������쳣,������ɾ���ļ��ͼ�¼�Ĳ������ᱻִ��,�������˹���ȷ���Ƿ�ɻָ�,��λָ�
            errprintf("׼���ָ�����״̬.");
            sp.log(tabid,datapartid,DUMP_RECOVER_TAST_STATUS_NOTIFY,"׼���ָ�����״̬.");


            if(!dp_parallel_load) { // �Ƕ�ڵ������������(bhloader�������)

                st.DirectExecute("update dp.dp_datapart set status=%d,blevel=ifnull(blevel,0)+100 "
                                 "where tabid=%d and datapartid=%d",
                                 filesmode?(addfiles?73:72):0,tabid,datapartid);

                if(filesmode) {
                    if(fnmt.GetRows()>0 ) {
                        wociAppendToDbTable(fnmt,"dp.dp_middledatafile",dtdbc,true);
                        wociReset(fnmt);
                    }
                    throw;
                }
                errprintf("ɾ���м��ļ�...");
                for(i=0; i<frn; i++) {
                    errprintf("\t %s \t %s",fnmt.PtrStr("datafilename",i),
                              fnmt.PtrStr("indexfilename",i));
                }
                for(i=0; i<frn; i++) {
                    unlink(fnmt.PtrStr("datafilename",i));
                    unlink(fnmt.PtrStr("indexfilename",i));
                }
                errprintf("ɾ���м��ļ���¼...");
                st.Prepare("delete from dp.dp_middledatafile where tabid=%d and datapartid=%d",tabid,datapartid);
                st.Execute(1);
                st.Wait();
                st.Prepare("delete from dp.dp_filelog where tabid=%d and datapartid=%d",tabid,datapartid);
                st.Execute(1);
                st.Wait();

                /* on a busy statement,wociSetTerminate will failed and throw a exception,so the last chance to
                restore enviriement is lost. so move to here from begining or this catch block.hope this can process more stable.
                LOGS:
                [2007:11:02 10:40:31] ��ʼ���ݴ���
                [2007:11:02 10:40:42] Write file failed! filename:/dbsturbo/dttemp/cas/mddt_340652.dat,blocklen:218816,offset:3371426
                [2007:11:02 10:40:42]  ErrorCode: -9.  Exception : Execute(Query) or Delete on a busy statement.
                */
                wociSetTerminate(dtdbc,false);
                wociSetTerminate(sp.GetDTS(),false);

            } else {
                // 1. �����ݿ�ɼ����ݣ������˳�ǰ��ֱ�ӽ�����״̬�޸ĳ����µ�������ִ��drop���ݿ����֪ͨ����Ⱥ��ֹ���е�����Ự(���̻Ự����)��
                // 2. ���ı��ļ��ɼ����ݣ������˳�ǰ: ֱ�ӽ�����״̬�޸ĳ����µ�������֪ͨ����Ⱥ��ֹ��ǰ�Ķ̻Ự�������ݣ��������ǰ�̻Ự�����Ӧ���ļ�(dp.dp_filelog�������)��
                rollback_sort_session(sp,tabid,datapartid,origion_status,filesmode);
            }
            // fix dma-2804
            // throw;
        }
    }
    if(dumpcomplete && !dp_parallel_load ) {
        lgprintf("���ݳ�ȡ����,����״̬1-->2,tabid %d(%d)",tabid,datapartid);
        lgprintf("sort time :%11.6f file io time :%11.6f adjust data time:%11.6f",
                 sorttm.GetTime(),fiotm.GetTime(),adjtm.GetTime());
        lgprintf("����");
        sp.log(tabid,datapartid,DUMP_FINISHED_NOTIFY,"��%d,����%d,���ݳ�ȡ���� ,��¼��%lld.",tabid,datapartid,srn);
    }

    release_sorter_obj();

    if(deal_slaver_node) { // slaver�ڵ�,���û�вɼ�������,�ͼ���������һ�����Բ���ִ�е�����
        if(!b_master_node && run_slaver_taskinfo_deal_rows == 0 && !run_slaver_taskinfo_finish) {
            lgprintf("slaver��������,û�вɼ�������,����������һ��slaver�ɼ�����,�Ѵ���slaver���������:[%d]",run_slaver_taskinfo_vec.size());
            goto lable_dump_slaver_begin;
        }

        if(run_slaver_taskinfo_finish) {
            lgprintf("slaver��������,û�вɼ�������,���������,����������һ��master�ɼ�����,�Ѵ���slaver���������:[%d]",run_slaver_taskinfo_vec.size());
            deal_slaver_node = false;
            goto lable_dump_master_begin;
        }
    }

    return 1;
}

MiddleDataLoader::MiddleDataLoader(SysAdmin *_sp):
    indexmt(0,0),mdindexmt(0,0),blockmt(0,0),mdf(_sp->GetDTS(),MAX_MIDDLE_FILE_NUM)
{
    sp=_sp;
    tmpfilenum=0;
    pdtf=NULL;
    pdtfid=NULL;
    //dtfidlen=0;
}

void StrToLower(char *str)
{
    while(*str!=0) {
        *str=tolower(*str);
        str++;
    }
}

//>> Begin:dm-228
// �ж�ָ�����Ƿ����������ļ��б�IndexListFile�У����ָ����columnName���򷵻أ�true,
// ���򷵻أ�false
bool GetColumnIndex(const char* IndexListFile,const char* columnName)
{
    FILE *fp = NULL;
    fp = fopen(IndexListFile,"rt");
    if(fp==NULL) {
        ThrowWith("�������б��ļ�ʧ��.");
    }
    char lines[300];
    while(fgets(lines,300,fp)!=NULL) {
        int sl = strlen(lines);
        if(lines[sl-1]=='\n') lines[sl-1]=0;
        if(strcasecmp(lines,columnName) == 0) {
            fclose(fp);
            fp = NULL;
            return true;
        }
    }
    fclose(fp);
    fp=NULL;
    return false;
}

// �������������ͨ��Դ�������������
int MiddleDataLoader::CreateSolidIndexTable(const char* orasvcname,const char * orausrname,const char* orapswd,
        const char* srcdbname,const char* srctabname,const char* dstdbname,const char* dsttabname,
        const char* indexlist,const char* tmppath,const char* backpath,const char *taskdate,
        const char* solid_index_list_file,char* ext_sql)
{
    int ret = 0;

    // 1-- �ж�Ŀ����Ƿ��Ѿ�����
    AutoMt dst_table_mt(sp->GetDTS(),10);
    dst_table_mt.FetchAll("select * from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",dstdbname,dsttabname);
    if(dst_table_mt.Wait()>0)
        ThrowWith("�� %s.%s �Ѿ����ڡ�",dstdbname,dsttabname);

    // 2-- ����Դ�����ݿ�,�ж�ORACLEԴ���Ƿ����
    AutoHandle ora_dts;
    ora_dts.SetHandle(wociCreateSession(orausrname,orapswd,orasvcname,DTDBTYPE_ORACLE));
    try {
        AutoMt ora_src_test_mt(ora_dts,10);
        char sql[1000];
        sprintf(sql,"select count(1) from %s.%s where rownum < 2",srcdbname,srctabname);
        ora_src_test_mt.FetchFirst(sql);
        ora_src_test_mt.Wait();

        lgprintf("begin to IgnoreBigSizeColumn info ...");
        sprintf(sql,"select * from %s.%s where rownum < 2",srcdbname,srctabname);

        //>> begin: fix dm-252
        sp->IgnoreBigSizeColumn(ora_dts,sql);
        //>> end: fix dm-252
    } catch(...) {
        ThrowWith("Դ��:%s.%s �������޷�������:%s.%s��",srcdbname,srctabname,dstdbname,dsttabname);
    }

    // 3-- �ж�dp�����е�����Դ�Ƿ����,����ȡ������Դ���������������Դ
    AutoMt data_src_mt(sp->GetDTS(),10);
    int  data_src_id = 0;
#define ORACLE_TYPE 1
    data_src_mt.FetchFirst("select sysid from dp.dp_datasrc where systype = %d and jdbcdbn = '%s' and username = '%s' and pswd = '%s'",
                           ORACLE_TYPE,orasvcname,orausrname,orapswd);
    int trn=data_src_mt.Wait();
    if(trn<1) {
        // dp���ݿ��в���������Դ����Ҫ���
        AutoStmt tmp_data_src_mt(sp->GetDTS());
        ret = tmp_data_src_mt.DirectExecute("INSERT INTO dp.dp_datasrc(sysid,sysname,svcname,username,pswd,systype,jdbcdbn) "
                                            " select max(sysid)+1,'%s','CreateSolidIndexTable add','%s','%s',%d,'%s' from dp.dp_datasrc",
                                            orasvcname,orausrname,orapswd,ORACLE_TYPE,orasvcname);

        if(ret != 1) {
            ThrowWith("dp ���ݿ����Oracle����Դ[%s],����ʧ��!",orasvcname);
        }

        // ��ȡ�²����sysid
        data_src_mt.FetchFirst("select max(sysid) as sysid from dp.dp_datasrc");
        trn = data_src_mt.Wait();
        data_src_id = data_src_mt.GetInt("sysid",0);
    } else {
        data_src_id = data_src_mt.GetInt("sysid",0);
    }

    // 4-- �ж�dp.dp_path��·��ID�Ƿ����
    // 4.1--��ѯ��ʱ·��
    AutoMt path_mt(sp->GetDTS(),10);
    int  data_tmp_pathid = 0,data_backup_pathid=0;
    path_mt.FetchFirst("SELECT pathid FROM dp.dp_path where pathval = '%s' and pathtype = 'temp'",tmppath);
    if(1 > path_mt.Wait()) {
        AutoStmt tmp_path_mt(sp->GetDTS());
        ret = tmp_path_mt.DirectExecute("INSERT INTO dp.dp_path(pathid,pathtype,pathdesc,pathval) values(%d,'temp','CreateSolidIndexTable add','%s')",
                                        sp->NextTableID(),tmppath);
        if(-1 == ret) ThrowWith("dp���ݿ������ʱ·��[%s]ʧ��!",tmppath);
        path_mt.FetchFirst("select pathid from dp.dp_path where pathval = '%s' and pathtype= 'temp'", tmppath);
        path_mt.Wait();
        data_tmp_pathid = path_mt.GetInt("pathid",0);
    } else {
        data_tmp_pathid =  path_mt.GetInt("pathid",0);
    }

    // 4.2--��ѯ����·��
    path_mt.FetchFirst("SELECT pathid FROM dp.dp_path where pathval = '%s' and pathtype = 'data'",backpath);
    if(1 > path_mt.Wait()) {
        AutoStmt tmp_path_mt(sp->GetDTS());
        ret = tmp_path_mt.DirectExecute("INSERT INTO dp.dp_path(pathid,pathtype,pathdesc,pathval) values(%d,'data','CreateSolidIndexTable add','%s')",
                                        sp->NextTableID(),backpath);
        if(-1 == ret) ThrowWith("dp���ݿ���뱸��·��[%s]ʧ��!",backpath);
        path_mt.FetchFirst("select pathid from dp.dp_path where pathval = '%s' and pathtype= 'data'", backpath);
        path_mt.Wait();
        data_backup_pathid = path_mt.GetInt("pathid",0);
    } else {
        data_backup_pathid =  path_mt.GetInt("pathid",0);
    }

    // 5-- �ж�ʱ���ʽ�Ƿ���ȷ
    AutoMt mt(sp->GetDTS(),100);
    char tdt[30];
    if(strcmp(taskdate,"now()")==0) {
        wociGetCurDateTime(tdt);
    } else {
        mt.FetchAll("select adddate(cast('%s' AS DATETIME),interval 0 day) as tskdate",taskdate);
        if(mt.Wait()!=1) {
            ThrowWith("���ڸ�ʽ����:'%s'",taskdate);
        }
        memcpy(tdt,mt.PtrDate("tskdate",0),7);
        if(wociGetYear(tdt)==0) {
            ThrowWith("���ڸ�ʽ����:'%s'",taskdate);
        }
    }

    // 6-- ����dp_table����������Ϣ
    int table_id = 0;
    AutoMt table_mt(sp->GetDTS(),100);
    table_mt.FetchFirst("select * from dp.dp_table limit 1");
    if(table_mt.Wait() != 1) {
        ThrowWith("���ݿ��dp_table��û�м�¼������ͨ��web����ƽ̨����һ��������ٽ��иò���.");
    }

//  strcpy(table_mt.PtrStr("databasename",0),dstdbname);
    table_mt.SetStr("databasename",0,(char *)dstdbname);
//  strcpy(table_mt.PtrStr("tabdesc",0),dsttabname);
    table_mt.SetStr("tabdesc",0,(char *)dsttabname);
//  strcpy(table_mt.PtrStr("tabname",0),dsttabname);
    table_mt.SetStr("tabname",0,(char *)dsttabname);
//  strcpy(table_mt.PtrStr("srcowner",0),srcdbname);
    table_mt.SetStr("srcowner",0,(char *)srcdbname);
//  strcpy(table_mt.PtrStr("srctabname",0),srctabname);
    table_mt.SetStr("srctabname",0,(char *)srctabname);
    *table_mt.PtrInt("sysid",0)=data_src_id;
    *table_mt.PtrInt("dstpathid",0)=data_backup_pathid;

    *table_mt.PtrInt("cdfileid",0)=0;
    *table_mt.PtrDouble("recordnum",0)=0;
    *table_mt.PtrInt("firstdatafileid",0)=0;
    *table_mt.PtrInt("datafilenum",0)=0;
    *table_mt.PtrInt("lstfid",0)=0;
    *table_mt.PtrDouble("totalbytes",0)=0;
    *table_mt.PtrInt("recordlen",0)=0;
    *table_mt.PtrInt("maxrecinblock",0)=0;


    // ��ȡtabid
    table_id = sp->NextTableID();
    *table_mt.PtrInt("tabid",0)=table_id;

    // 7-- ����id�Ƿ���ڼ�¼
    AutoMt check_mt(sp->GetDTS(),10);
    // ��Ӧ��Դ���Ժ��޸�
    check_mt.FetchAll("select * from dp.dp_table where tabid=%d",table_id);
    if(check_mt.Wait()>0)
        ThrowWith("����ظ�: ���%d��Ŀ���'%s.%s'�Ѿ�����!",check_mt.GetInt("tabid",0),check_mt.PtrStr("databasename",0),
                  check_mt.PtrStr("tabname",0));
    check_mt.FetchAll("select * from dp.dp_index where tabid=%d",table_id);
    if(check_mt.Wait()>0)
        ThrowWith("���ֲ���ȷ����������(��%d)��",check_mt.GetInt("tabid",0));
    check_mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2",table_id);
    if(check_mt.Wait()>0)
        ThrowWith("���ֲ���ȷ�������ļ���¼(��%d)!",check_mt.GetInt("tabid",0));
    check_mt.FetchAll("select * from dp.dp_datapart where tabid=%d",table_id);
    if(check_mt.Wait()>0)
        ThrowWith("���ֲ���ȷ�����ݷ������(��%d)!",check_mt.GetInt("tabid",0));


    // 8-- ����dp_index����������Ϣ

    // ��������
    char idx_col_name[256];
    idx_col_name[sizeof(idx_col_name) - 1] = 0;
    try {
        // 8.1 -- ��ѯoracleԴ���ݿ����ȡ����Ϣ,��ȡsql���
        sp->IgnoreBigSizeColumn(ora_dts,srcdbname,srctabname,ext_sql);

        // 8.2 -- ��ȡԴ��ṹ
        AutoMt  src_table_mt(ora_dts,10);
        src_table_mt.FetchFirst(ext_sql);
        src_table_mt.Wait();

        // 8.3 -- ���Դ��ṹ�д���mysql�ؼ��֣������滻��
        char cfilename[256];
        strcpy(cfilename,getenv("DATAMERGER_HOME"));
        strcat(cfilename,"/ctl/");
        strcat(cfilename,MYSQL_KEYWORDS_REPLACE_LIST_FILE);
        sp->ChangeMtSqlKeyWord(src_table_mt,cfilename);

        // 8.4 -- ����������ж�
        bool get_column_index_flag = false;
        bool index_column_name_error = false;
#define INDEX_SEPERATOR ','
        if(strlen(indexlist) > 0) {
            // �ж������Ƿ�Ϸ�
            char _indexlist[256];
            strcpy(_indexlist,indexlist);
            Trim(_indexlist,INDEX_SEPERATOR);
            std::string _index_string(_indexlist);
            std::string _index_col_item;
            std::size_t _pos = _index_string.find(INDEX_SEPERATOR);
            char _cn[128];
            bool _find_col_item = false;
            while(_pos != std::string::npos && !index_column_name_error) {
                _index_col_item = _index_string.substr(0,_pos);
                _find_col_item = false;
                for(int i=0; i<wociGetColumnNumber(src_table_mt); i++) {
                    wociGetColumnName(src_table_mt,i,_cn);
                    if(strcasecmp(_cn,_index_col_item.c_str()) == 0) {
                        _find_col_item = true; // �ҵ�����
                        break;
                    }
                }

                if(!_find_col_item) {
                    index_column_name_error = true;  // �����ƴ�����
                    break;
                }

                _index_string = _index_string.substr(_pos+1,_index_string.size() - _pos);
                _pos = _index_string.find(INDEX_SEPERATOR);
            }

            if(!index_column_name_error) { // ��û�д��������£��������һ��
                _index_col_item = _index_string;
                _find_col_item = false;
                for(int i=0; i<wociGetColumnNumber(src_table_mt); i++) {
                    wociGetColumnName(src_table_mt,i,_cn);
                    if(strcasecmp(_cn,_index_col_item.c_str()) == 0) {
                        _find_col_item = true;
                        break;
                    }
                }

                if(!_find_col_item) {
                    index_column_name_error = true;  // �����ƴ�����
                }
            }

            if(!index_column_name_error) { // ������У����ɣ�û�з��ִ���
                strcpy(idx_col_name,_indexlist);
                get_column_index_flag = true;
            }
        }

        if(!get_column_index_flag) { // û��ָ��������ʹ��Ĭ�ϵ�����
            for(int i=0; i<wociGetColumnNumber(src_table_mt); i++) {
                wociGetColumnName(src_table_mt,i,idx_col_name);
                if(GetColumnIndex(solid_index_list_file,idx_col_name)) {
                    get_column_index_flag = true;
                    break;
                }
            }
            if(!get_column_index_flag) { // �б��ļ���û���ҵ����������õ�һ������Ϊ����
                wociGetColumnName(src_table_mt,0,idx_col_name);
            }
        }
    } catch(...) {
        ThrowWith("Դ��:%s.%s �������޷�������:%s.%s��",srcdbname,srctabname,dstdbname,dsttabname);
    }


    // 8.3 -- ����dp_index���м�¼
    AutoMt index_mt(sp->GetDTS(),10);
    index_mt.FetchFirst("select * from dp.dp_index limit 1");
    if(index_mt.Wait() != 1) {
        ThrowWith("���ݿ��dp.dp_index��û�м�¼������ͨ��web����ƽ̨����һ��������ٽ��иò���.");
    }
    *index_mt.PtrInt("indexgid",0)=1;
    *index_mt.PtrInt("tabid",0)=table_id;
    //strcpy(index_mt.PtrStr("indextabname",0),"");//dsttabname);
    index_mt.SetStr("indextabname",0,(char*)"");
    *index_mt.PtrInt("seqindattab",0)=1;
    *index_mt.PtrInt("seqinidxtab",0)=1;
    *index_mt.PtrInt("issoledindex",0)=1;
    //strcpy(index_mt.PtrStr("columnsname",0),idx_col_name);
    index_mt.SetStr("columnsname",0,(char*)idx_col_name);
    *index_mt.PtrInt("idxfieldnum",0)=1;

    // 9-- ����dp_datapart����������Ϣ
    AutoMt datapart_mt(sp->GetDTS(),10);
    datapart_mt.FetchFirst("select * from dp.dp_datapart limit 1");
    if(datapart_mt.Wait() != 1) {
        ThrowWith("���ݿ��dp.dp_datapart��û�м�¼������ͨ��web����ƽ̨����һ��������ٽ��иò���.");
    }

    // 9.1 -- �ж�Դ�����Ƿ���ڴ��ֶΣ�������ڴ��ֶξͽ�����˵�
    sp->IgnoreBigSizeColumn(ora_dts,srcdbname,srctabname,ext_sql);

    // 9.2 -- ����������Ϣ��dp_datapart����
    *datapart_mt.PtrInt("datapartid",0)=1;
    memcpy(datapart_mt.PtrDate("begintime",0),tdt,7);
    *datapart_mt.PtrInt("istimelimit",0)=0;
    *datapart_mt.PtrInt("status",0)=0;
    char szPartDesc[512];
    memset(szPartDesc,0,512);
    sprintf(szPartDesc,"%s:%s.%s->%s.%s",orasvcname,srcdbname,srctabname,dstdbname,dsttabname);
    datapart_mt.SetStr("partdesc",0,szPartDesc);
//  sprintf(datapart_mt.PtrStr("partdesc",0),"%s:%s.%s->%s.%s",orasvcname,srcdbname,srctabname,dstdbname,dsttabname);

    *datapart_mt.PtrInt("tabid",0)=table_id;
    *datapart_mt.PtrInt("compflag",0)=1;
    *datapart_mt.PtrInt("oldstatus",0)=0;
    *datapart_mt.PtrInt("srcsysid",0)=data_src_id;
//  strcpy(datapart_mt.PtrStr("extsql",0),ext_sql);
    datapart_mt.SetStr("extsql",0,(char *)ext_sql);
    *datapart_mt.PtrInt("tmppathid",0)=data_tmp_pathid;
    *datapart_mt.PtrInt("blevel",0)=0;

    try {
        wociAppendToDbTable(table_mt,"dp.dp_table",sp->GetDTS(),true);
        wociAppendToDbTable(index_mt,"dp.dp_index",sp->GetDTS(),true);
        wociAppendToDbTable(datapart_mt,"dp.dp_datapart",sp->GetDTS(),true);
    } catch(...) {
        //�ָ����ݣ��������в���
        AutoStmt st(sp->GetDTS());
        st.DirectExecute("delete from dp.dp_table where tabid=%d",table_id);
        st.DirectExecute("delete from dp.dp_index where tabid=%d",table_id);
        st.DirectExecute("delete from dp.dp_datapart where tabid=%d",table_id);
        errprintf("�����ƴ���ʱ�����ύʧ�ܣ���ɾ�����ݡ�");
        throw;
    }
    {
        char dtstr[100];
        wociDateTimeToStr(tdt,dtstr);
        lgprintf("�����ɹ�,Ŀ���'%s.%s:%d',��ʼʱ��'%s'.",dstdbname,dsttabname,table_id,dtstr);
    }
    sp->Reload();
    return 0;
}


int MiddleDataLoader::CreateLike(const char *dbn,const char *tbn,const char *nsrcowner,const char *nsrctbn,const char * ndstdbn,const char *ndsttbn,const char *taskdate,bool presv_fmt)
{
    int tabid=0,srctabid=0;
    AutoMt mt(sp->GetDTS(),100);
    char tdt[30];
    char ndbn[300];
    strcpy(ndbn,ndstdbn);
    StrToLower(ndbn);
    if(strcmp(taskdate,"now")==0)
        wociGetCurDateTime(tdt);
    else {
        mt.FetchAll("select adddate(cast('%s' AS DATETIME),interval 0 day) as tskdate",taskdate);
        if(mt.Wait()!=1)
            ThrowWith("���ڸ�ʽ����:'%s'",taskdate);
        memcpy(tdt,mt.PtrDate("tskdate",0),7);
        if(wociGetYear(tdt)==0)
            ThrowWith("���ڸ�ʽ����:'%s'",taskdate);
    }
    mt.FetchAll("select * from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",ndbn,ndsttbn);
    if(mt.Wait()>0)
        ThrowWith("�� %s.%s �Ѿ����ڡ�",dbn,ndsttbn);

    //>> 1. ��������Ϣ(dp.dp_table)
    AutoMt tabmt(sp->GetDTS(),100);
    tabmt.FetchAll("select * from dp.dp_table where databasename=lower('%s') and lower(tabname)='%s'",dbn,tbn);
    if(tabmt.Wait()!=1)
        ThrowWith("�ο��� %s.%s �����ڡ�",dbn,tbn);
    int reftabid=tabmt.GetInt("tabid",0);
    //���Ŀ�����Ϣ
    tabmt.SetStr("databasename",0,(char *)ndbn,1);
    tabmt.SetStr("tabdesc",0,(char *)ndsttbn,1);
    tabmt.SetStr("tabname",0,(char *)ndsttbn,1);
    *tabmt.PtrInt("cdfileid",0)=0;
    *tabmt.PtrDouble("recordnum",0)=0;
    *tabmt.PtrInt("firstdatafileid",0)=0;
    *tabmt.PtrInt("datafilenum",0)=0;
    *tabmt.PtrInt("lstfid",0)=0;
    *tabmt.PtrDouble("totalbytes",0)=0;
    char prefsrctbn[256];
    strcpy(prefsrctbn,tabmt.PtrStr("srctabname",0));
    StrToLower(prefsrctbn);
    char prefsrcowner[256];
    strcpy(prefsrcowner,tabmt.PtrStr("srcowner",0));
    StrToLower(prefsrcowner);
    //�ο�Դ����滹Ҫ����,��ʱ���滻
    tabid=sp->NextTableID();
    *tabmt.PtrInt("tabid",0)=tabid;
    // ��Ӧ��Դ���Ժ��޸�
    mt.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
    if(mt.Wait()>0)
        ThrowWith("����ظ�: ���%d��Ŀ���'%s.%s'�Ѿ�����!",mt.GetInt("tabid",0),mt.PtrStr("databasename",0),mt.PtrStr("tabname",0));
    mt.FetchAll("select * from dp.dp_index where tabid=%d",tabid);
    if(mt.Wait()>0)
        ThrowWith("���ֲ���ȷ����������(��%d)��",mt.GetInt("tabid",0));
    mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2",tabid);
    if(mt.Wait()>0)
        ThrowWith("���ֲ���ȷ�������ļ���¼(��%d)!",mt.GetInt("tabid",0));
    mt.FetchAll("select * from dp.dp_datapart where tabid=%d",tabid);
    if(mt.Wait()>0)
        ThrowWith("���ֲ���ȷ�����ݷ������(��%d)!",mt.GetInt("tabid",0));

    //>> 2. ������������(dp.dp_index)
    AutoMt indexmt(sp->GetDTS(),200);
    indexmt.FetchAll("select * from dp.dp_index where tabid=%d",reftabid);
    int irn=indexmt.Wait();
    if(irn<1)
        ThrowWith("�ο��� %s.%s û�н���������",dbn,tbn);
    int soledct=1;
    //���������Ϣ���ؽ����ù�ϵ
    int nSrLen =indexmt.GetColLen((char*)"indextabname");
    char *pStrVal = new char[irn*nSrLen];
    memset(pStrVal,0,irn*nSrLen);
    for(int ip=0; ip<irn; ip++) {
        *indexmt.PtrInt("tabid",ip)=tabid;
    }
    indexmt.SetStr("indextabname",0,pStrVal,irn);
    delete[] pStrVal;

    //>> 3. ������չ�ֶ���Ϣ(dp_column_info)
    int externed_field_num = 0; // �Ƿ������չ�ֶ�
    AutoMt externedMt(sp->GetDTS(),200) ;
    externedMt.FetchAll("select * from dp.dp_column_info where table_id=%d",reftabid);
    externed_field_num=externedMt.Wait();
    for(int ip=0; ip<externed_field_num; ip++) {
        *externedMt.PtrInt("table_id",ip)=tabid;
    }

    //>>  4. ����������Ϣ(dp.dp_datapart)
    AutoMt taskmt(sp->GetDTS(),500);
    taskmt.FetchAll("select * from dp.dp_datapart where tabid=%d",reftabid);
    int trn=taskmt.Wait();
    if(trn<1)
        ThrowWith("�ο��� %s.%s û�����ݷ�����Ϣ��",dbn,tbn);

    //�����ݳ�ȡ�������Сд���е�Դ�������滻,��������������:
    // 1. ����ֶ�������������Դ��������ͬ,������滻ʧ��
    // 2. ���Դ�����ƴ�Сд��һ��,������滻ʧ��
    char tsrcowner[300],tsrctbn[300],tsrcfull[300],treffull[300];
    strcpy(tsrcowner,nsrcowner);
    strcpy(tsrctbn,nsrctbn);
    StrToLower(tsrcowner);
    StrToLower(tsrctbn);
    sprintf(tsrcfull,"%s.%s",tsrcowner,tsrctbn);
    sprintf(treffull,"%s.%s",prefsrcowner,prefsrctbn);

    // fix:dma-755 add by liujs
    char *psql=new char[MAX_STMT_LEN];
    for(int tp=0; tp<trn; tp++) {
        char sqlbk[MAX_STMT_LEN];
        memset(psql,0,MAX_STMT_LEN);
        const char *conpsql=taskmt.PtrStr("extsql",tp);
        strcpy(psql,conpsql);
        strcpy(sqlbk,psql);
        char tmp[MAX_STMT_LEN];
        strcpy(tmp,psql);
        StrToLower(tmp);
        if(strcmp(prefsrctbn,tsrctbn)!=0 || strcmp(prefsrcowner,tsrcowner)!=0 || presv_fmt) {
            char extsql[MAX_STMT_LEN];
            char *sch_load=strstr(tmp,"load data "); // load data from files
            char *sch=strstr(tmp," from "); // select ... from xxx
            char *schx=strstr(tmp," where "); // select ...  from xxx where ...
            if(presv_fmt) {
                if(sch_load) { // ԭ������extsql���
                    taskmt.SetStr("extsql",tp,tmp);
                } else if(sch) {
                    sch+=6;
                    strncpy(extsql,psql,sch-tmp);
                    extsql[sch-tmp]=0;
                    strcat(extsql,tsrcfull);
                    if(schx)
                        strcat(extsql,psql+(schx-tmp));
                    strcpy(psql,extsql);
                    taskmt.SetStr("extsql",tp,psql,1);
                }
            } // end if(presv_fmt)
            else {
                if(sch_load) { // ԭ������extsql���
                    taskmt.SetStr("extsql",tp,tmp);
                } else {
                    if(sch) {
                        if(schx) *schx=0;
                        sch+=6;
                        //���� from ֮ǰ�����(�� from );
                        strncpy(extsql,psql,sch-tmp);
                        extsql[sch-tmp]=0;
                        bool fullsrc=true;
                        int tablen=strlen(treffull);
                        char *sch2=strstr(sch,treffull);
                        if(sch2==NULL) {
                            //�ο����sql����в�����ʽ���������ʽ
                            //����Ƿ񺬲��֣�ֻ�б�����û�п���)
                            sch2=strstr(sch,prefsrctbn);
                            fullsrc=false;
                            tablen=strlen(prefsrctbn);
                        }
                        //����Ҫ�������򲿷���ʽ�ĸ�ʽ�����滻
                        if(sch2) {
                            //ֻ��from ... where ֮������滻�����Ĳ����滻�������ı���
                            // any chars between 'from' and tabname ?
                            strncpy(extsql+strlen(extsql),psql+(sch-tmp),sch2-sch);
                            // replace new tabname
                            strcat(extsql,tsrcfull);
                            //padding last part
                            strcat(extsql,psql+(sch2-tmp)+tablen);
                            strcpy(psql,extsql);
                            taskmt.SetStr("extsql",tp,psql,1);
                        }
                    }// end if(sch){
                }// end else{
            }// end else {
        }

        if(strcmp(sqlbk,psql)==0)
            lgprintf("���ݷ���%d�����ݳ�ȡ���δ���޸ģ��������Ҫ�ֹ�����.",taskmt.GetInt("datapartid",tp));
        else
            lgprintf("���ݷ���%d�����ݳ�ȡ����Ѿ��޸ģ�\n%s\n--->\n%s\n�������Ҫ�ֹ�����.",taskmt.GetInt("datapartid",tp),sqlbk,psql);

        *taskmt.PtrInt("tabid",tp)=tabid;

        // fix dma-755 add by liujs
        char partition_name[300];
        sprintf(partition_name,"partition_%d",*taskmt.PtrInt("datapartid",tp));
        taskmt.SetStr("partfullname",tp,partition_name);

        *taskmt.PtrInt("blevel",tp)=0;
        memcpy(taskmt.PtrDate("begintime",tp),tdt,7);
        *taskmt.PtrInt("status",tp)=0;
    }
    delete[] psql;
    if(presv_fmt) {
        tabmt.SetStr("srctabname",0,prefsrctbn);
        tabmt.SetStr("srcowner",0,prefsrcowner);
    } else {
        tabmt.SetStr("srctabname",0,tsrctbn);
        tabmt.SetStr("srcowner",0,tsrcowner);
    }
    StrToLower(ndbn);

    //check database on mysql,if not exists,create it
    sp->TouchDatabase(ndbn,true);
    try {
        wociAppendToDbTable(tabmt,"dp.dp_table",sp->GetDTS(),true);
        wociAppendToDbTable(indexmt,"dp.dp_index",sp->GetDTS(),true);
        wociAppendToDbTable(taskmt,"dp.dp_datapart",sp->GetDTS(),true);
        if(externed_field_num>0) {
            wociAppendToDbTable(externedMt,"dp.dp_column_info",sp->GetDTS(),true);
        }
    } catch(...) {
        //�ָ����ݣ��������в���
        AutoStmt st(sp->GetDTS());
        st.DirectExecute("delete from dp.dp_table where tabid=%d",tabid);
        st.DirectExecute("delete from dp.dp_index where tabid=%d",tabid);
        st.DirectExecute("delete from dp.dp_datapart where tabid=%d",tabid);
        if(externed_field_num>0) {
            st.DirectExecute("delete from dp.dp_column_info where table_id=%d",tabid);
        }
        errprintf("�����ƴ���ʱ�����ύʧ�ܣ���ɾ�����ݡ�");
        throw;
    }
    {
        char dtstr[100];
        wociDateTimeToStr(tdt,dtstr);
        lgprintf("�����ɹ�,Դ��'%s',Ŀ���'%s',��ʼʱ��'%s'.",nsrctbn,ndsttbn,dtstr);
    }
    return tabid;
}

//DT data&index File Check
int MiddleDataLoader::dtfile_chk(const char *dbname,const char *tname)
{
    //Check deserved temporary(middle) fileset
    //���״̬Ϊ1������
    mdf.FetchAll("select * from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",dbname,tname);
    int rn=mdf.Wait();
    if(rn<1) ThrowWith("DP�ļ����:��dp_table��'%s.%s'Ŀ����޼�¼��",dbname,tname);
    int firstfid=mdf.GetInt("firstdatafileid",0);
    int tabid=*mdf.PtrInt("tabid",0);
    int blockmaxrn=*mdf.PtrInt("maxrecinblock",0);
    double totrc=mdf.GetDouble("recordnum",0);
    sp->OutTaskDesc("Ŀ����� :",0,tabid);
    char *srcbf=new char[SRCBUFLEN];//ÿһ�δ����������ݿ飨��ѹ���󣩡�
    int errct=0;
    mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2 order by indexgid,fileid",tabid);
    int irn=mdf.Wait();
    if(irn<1) {
        ThrowWith("DP�ļ����:��dp_datafilemap��%dĿ����������ļ��ļ�¼��",tabid);
    }

    {
        AutoMt datmt(sp->GetDTS(),100);
        datmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileid=%d",tabid,firstfid);
        if(datmt.Wait()!=1)
            ThrowWith("��ʼ�����ļ�(���%d)��ϵͳ��û�м�¼.",firstfid);
        char linkfn[300];
        strcpy(linkfn,datmt.PtrStr("filename",0));
        datmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2 and isfirstindex=1 order by datapartid,fileid",tabid);
        int rn1=datmt.Wait();
        if(firstfid!=datmt.GetInt("fileid",0))
            ThrowWith("��ʼ�����ļ�(���%d)�����ô���Ӧ����%d..",firstfid,datmt.GetInt("fileid",0));
        int lfn=0;
        while(true) {
            file_mt dtf;
            lfn++;
            dtf.Open(linkfn,0);
            const char *fn=dtf.GetNextFileName();
            if(fn==NULL) {
                printf("%s==>����.\n",linkfn);
                break;
            }
            printf("%s==>%s\n",linkfn,fn);
            strcpy(linkfn,fn);
        }
        if(lfn!=rn1)
            ThrowWith("�ļ����Ӵ���ȱʧ%d���ļ�.",rn1-lfn);
    }

    mytimer chktm;
    chktm.Start();
    try {
        int oldidxid=-1;
        for(int iid=0; iid<irn; iid++) {
            //ȡ��������
            int indexid=mdf.GetInt("indexgid",iid);

            AutoMt idxsubmt(sp->GetDTS(),100);
            idxsubmt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d ",tabid,indexid);
            rn=idxsubmt.Wait();
            if(rn<1) {
                ThrowWith("DP�ļ����:��dp.dp_index��%dĿ�����%d�����ļ�¼��",tabid,indexid);
            }

            printf("����ļ�%s--\n",mdf.PtrStr("filename",iid));
            fflush(stdout);

            //����ȫ����������
            char dtfn[300];
            dt_file idxf;
            idxf.Open(mdf.PtrStr("idxfname",iid),0);
            printf("�����ļ���%s.\n",mdf.PtrStr("idxfname",iid));
            AutoMt idxmt(0);
            idxmt.SetHandle(idxf.CreateMt(1));
            idxmt.SetHandle(idxf.CreateMt(FIX_MAXINDEXRN/wociGetRowLen(idxmt)));
            idxf.Open(mdf.PtrStr("idxfname",iid),0);
            int brn=0;//idxf.ReadMt(-1,0,idxmt,false);
            int sbrn=0;
            while( (sbrn=idxf.ReadMt(-1,0,idxmt,true,1))>0) brn+=sbrn;
            printf("����%d�С�\n",brn);
            int thiserrct=errct;
            //�����������ļ��ĺ�׺��idx�滻Ϊdat���������ļ�.
            AutoMt destmt(0);
            strcpy(dtfn,mdf.PtrStr("idxfname",iid));
            strcpy(dtfn+strlen(dtfn)-3,"dat");
            //�������ļ�ȡ�ֶνṹ���ڴ���СΪĿ����ÿ���ݿ����������
            //destmt.SetHandle(dtf.CreateMt(blockmaxrn));
            int myrowlen=0;
            {
                dt_file datf;
                datf.Open(dtfn,0);
                myrowlen=datf.GetMySQLRowLen();
            }
            FILE *fp=fopen(dtfn,"rb");
            printf("��������ļ���%s.\n",dtfn);
            if(fp==NULL)
                ThrowWith("DP�ļ����:�ļ�'%s'����.",dtfn);
            fseek(fp,0,SEEK_END);
            unsigned int flen=ftell(fp);
            fseek(fp,0,SEEK_SET);
            file_hdr fhdr;
            fread(&fhdr,sizeof(file_hdr),1,fp);
            fhdr.ReverseByteOrder();
            fseek(fp,0,SEEK_SET);
            printf("file flag:%x, rowlen:%x myrowlen:%d.\n",fhdr.fileflag,fhdr.rowlen,myrowlen);
            block_hdr *pbhdr=(block_hdr *)srcbf;

            int oldblockstart=-1;
            int dspct=0;
            int totct=0;
            int blockstart,blocksize,blockrn;
            int rownum;
            int bkf=0;
            sbrn=idxf.ReadMt(0,0,idxmt,true,1);
            int bcn=wociGetColumnPosByName(idxmt,"dtfid");
            int dtfid=*idxmt.PtrInt(bcn,0);
            int ist=0;
            for(int i=0; i<brn; i++) {
                //ֱ��ʹ���ֶ����ƻ����idx_rownum�ֶε����Ʋ�ƥ�䣬���ڵ�idx�����ļ��е��ֶ���Ϊrownum.
                //dtfid=*idxmt.PtrInt(bcn,i);
                if(i>=sbrn) {
                    ist=sbrn;
                    sbrn+=idxf.ReadMt(-1,0,idxmt,true,1);
                }
                blockstart=*idxmt.PtrInt(bcn+1,i-ist);
                blocksize=*idxmt.PtrInt(bcn+2,i-ist);
                blockrn=*idxmt.PtrInt(bcn+3,i);
                //startrow=*idxmt.PtrInt(bcn+4,i);
                rownum=*idxmt.PtrInt(bcn+5,i-ist);
                if(oldblockstart!=blockstart) {
                    try {
                        //dtf.ReadMt(blockstart,blocksize,mdf,1,1,srcbf);
                        //if(blockstart<65109161) continue;
                        fseek(fp,blockstart,SEEK_SET);
                        if(fread(srcbf,1,blocksize,fp)!=blocksize) {//+sizeof(block_hdr)
                            errprintf("�����ݴ���λ��:%d,����:%d,�������:%d.\n",blockstart,blocksize,i);
                            throw -1;
                        }
                        pbhdr->ReverseByteOrder();
                        if(!dt_file::CheckBlockFlag(pbhdr->blockflag)) {
                            errprintf("����Ŀ��ʶ��λ��:%d,����:%d,�������:%d.\n",blockstart,blocksize,i);
                            throw -1;
                        }
                        if(pbhdr->blockflag!=bkf) {
                            bkf=pbhdr->blockflag;
                            //if(bkf==BLOCKFLAG) printf("���ݿ�����:WOCI.\n");
                            //else if(bkf==MYSQLBLOCKFLAG)
                            //  printf("���ݿ�����:MYISAM.\n");
                            printf("���ݿ�����: %s .\n",dt_file::GetBlockTypeName(pbhdr->blockflag));
                            printf("ѹ������:%d.\n",pbhdr->compressflag);
                        }
                        if(pbhdr->storelen!=blocksize-sizeof(block_hdr)) {
                            errprintf("����Ŀ鳤�ȣ�λ��:%d,����:%d,�������:%d.\n",blockstart,blocksize,i);
                            throw -1;
                        }
                        //JIRA DM-13 . add block uncompress test
                        int dml=(blockrn+7)/8;
                        int rcl=pbhdr->storelen-dml-sizeof(delmask_hdr);
                        char *pblock=srcbf
                                     +sizeof(block_hdr)
                                     +dml
                                     +sizeof(delmask_hdr);
                        //bzlib2
                        char destbuf[1024*800];//800KB as buffer
                        uLong dstlen=1024*800;
                        int rt=0;
                        if(pbhdr->compressflag==10) {
                            unsigned int dstlen2=dstlen;
                            rt=BZ2_bzBuffToBuffDecompress(destbuf,&dstlen2,pblock,rcl,0,0);
                            dstlen=dstlen2;
                        }
                        /***********UCL decompress ***********/
                        else if(pbhdr->compressflag==8) {
                            unsigned int dstlen2=dstlen;
#ifdef USE_ASM_8
                            rt = ucl_nrv2d_decompress_asm_fast_8((Bytef *)pblock,rcl,(Bytef *)destbuf,(unsigned int *)&dstlen2,NULL);
#else
                            rt = ucl_nrv2d_decompress_8((Bytef *)pblock,rcl,(Bytef *)destbuf,(unsigned int *)&dstlen2,NULL);
#endif
                            dstlen=dstlen2;
                        }
                        /******* lzo compress ****/
                        else if(pbhdr->compressflag==5) {
                            lzo_uint dstlen2=dstlen;
#ifdef USE_ASM_5
                            rt=lzo1x_decompress_asm_fast((unsigned char*)pblock,rcl,(unsigned char *)destbuf,&dstlen2,NULL);
#else
                            rt=lzo1x_decompress((unsigned char*)pblock,rcl,(unsigned char *)destbuf,&dstlen2,NULL);
#endif
                            dstlen=dstlen2;
                        }
                        /*** zlib compress ***/
                        else if(pbhdr->compressflag==1) {
                            rt=uncompress((Bytef *)destbuf,&dstlen,(Bytef *)pblock,rcl);
                        } else
                            ThrowWith("Invalid uncompress flag %d",pbhdr->compressflag);
                        if(rt!=Z_OK) {
                            printf("blockrn%d, buffer head len:%d.pblock-srcbf:%d\n",blockrn,sizeof(block_hdr)
                                   +dml
                                   +sizeof(delmask_hdr),pblock-srcbf);
                            for (int trc=0; trc<256; trc++) {
                                printf("%02x ",(unsigned char)srcbf[trc]);
                                if((trc+1)%16==0) printf("\n");
                            }
                            ThrowWith("Decompress failed,fileid:%d,off:%d,blocksize:%d,datalen:%d,compress flag%d,errcode:%d",
                                      dtfid,blockstart,blocksize,pbhdr->storelen,pbhdr->compressflag,rt);
                        } else if(dstlen!=pbhdr->origlen) {
                            ThrowWith("Decompress failed,datalen %d should be %d.fileid:%d,off:%d,datalen:%d,compress flag%d,errcode:%d",
                                      dstlen,pbhdr->origlen,bcn,blockstart,pbhdr->storelen,pbhdr->compressflag,rt);
                        }
                        //for test only
                        //printf("%d:%d:%d:%d.\n",blockstart,pbhdr->origlen,dstlen,rcl);
                        //if(i>10) ThrowWith("debug stop");
                    } catch (...) {
                        if(errct++>20) {
                            errprintf("̫��Ĵ����ѷ�����飡");
                            throw;
                        }
                    }
                    //int mt=dtf.ReadBlock(blockstart,0,1,true);
                    //destmt.SetHandle(mt,true);
                    oldblockstart=blockstart;
                }
                totct+=rownum;
                if(totct-dspct>200000) {
                    printf("%d/%d    --- %d%%\r",i,brn,i*100/brn);
                    fflush(stdout);
                    dspct=totct;
                }
            } // end of for(...)
            if(ftell(fp)!=flen) {
                errprintf("�ļ����Ȳ�ƥ�䣬�����ļ�����:%d,�����ļ�ָʾ�Ľ���λ��:%d\n",flen,ftell(fp));
                errct++;
            }
            printf("�ļ������ϣ������� ��%d.    \n",errct-thiserrct);
            fclose(fp);
        }// end of for
    } // end of try
    catch (...) {
        errprintf("DP�ļ��������쳣��tabid:%d.",tabid);
    }
    delete []srcbf;
    if(errct>0)
        errprintf("DP�ļ������ִ���������װ��������³�ȡ����.");
    printf("\n");
    chktm.Stop();
    lgprintf("DP�ļ�������,������%d���ļ�,����%d������(%.2fs).",irn,errct,chktm.GetTime());
    return 1;
}

//����״̬���ѵ�������û����Ҫ������ļ�����ȡ�ձ�������״ֱ̬���޸�Ϊ������
void MiddleDataLoader::CheckEmptyDump()
{
    //���������޸� 2011 07 01
    mdf.FetchAll("select * from dp.dp_datapart where status in(1,2) %s limit 100",sp->GetNormalTaskDesc());
    int rn=mdf.Wait();
    if(rn>0) {
        while(--rn>=0) {
            //���������޸� 2011 07 01
            if(mdf.GetInt("status",rn)==1) continue;
            AutoMt tmt(sp->GetDTS(),10);
            tmt.FetchAll("select * from dp.dp_middledatafile where tabid=%d and datapartid=%d and procstate!=3 limit 10",
                         mdf.GetInt("tabid",rn),mdf.GetInt("datapartid",rn));
            if(tmt.Wait()<1) {
                AutoStmt st(sp->GetDTS());
                st.Prepare(" update dp.dp_datapart set status=3 where tabid=%d and datapartid=%d",
                           mdf.GetInt("tabid",rn),mdf.GetInt("datapartid",rn));
                st.Execute(1);
                st.Wait();
                lgprintf("��%d(%d)������Ϊ�գ��޸�Ϊ������",mdf.GetInt("tabid",rn),mdf.GetInt("datapartid",rn));
            }
        }
    }
}
// MAXINDEXRN ȱʡֵ500MB,LOADIDXSIZEȱʡֵ1600MB
// MAXINDEXRN Ϊ���������ļ�,��¼���ȶ�Ӧ������.
// LOADIDXSIZE �洢��ʱ����,��¼���ȴ����ݵ���ʱ�������ļ����ڴ�����.

//2005/08/24�޸ģ� MAXINDEXRN����ʹ��
//2005/11/01�޸ģ� װ��״̬��Ϊ1(��װ�룩��2����װ��),3����װ��);ȡ��10״̬(ԭ�������ֵ�һ�κ͵ڶ���װ��)
//2005/11/01�޸ģ� ��������װ���ڴ�ʱ�Ĳ��Ը�Ϊ�������ֶΡ�
//2006/02/15�޸ģ� һ��װ�������������ݣ���Ϊ�ֶ�װ��
//          һ��ֻװ��ÿ���ļ��е�һ���ڴ��(��ѹ����1M,������¼��50�ֽڳ�,����Լ2������¼,1G�ڴ��������1000/(1*1.2)=800�������ļ�)
//procstate״̬˵����
//  >1 :���ڵ�������
//  <=1:��������ݵ���
// 2011-06-23������InfoBright��ֱ�ӵ��빦��
//      1. �������ӣ����÷��Զ��ύ����鲢����Ŀ��� Ŀ�����ݿ�
//      2. �򿪹ܵ�
//      3. ��̨��ʽ��������
//      4. ��ʱ2���ʼ�ӹܵ�����Ŀ������
//      5. ���ִ�����˵��������
//      6. ִ����ɺ��ύ
// Ŀ���������Ҫ�˹�����
// ��ʱ����DM�а��д洢��ʽΪIB����ı�־��
typedef struct StruDataTrimItem {
    int tabid;
    int datapartid;
    int indexgid;
    StruDataTrimItem():tabid(0),datapartid(0),indexgid(0)
    {
    }
} StruDataTrimItem,*StruDataTrimItemPtr;
#define DATATRIM_NUM 100

int MiddleDataLoader::Load(int MAXINDEXRN,long LOADTIDXSIZE,bool useOldBlock)
{
    const char* pRiakHost = getenv(RIAK_HOSTNAME_ENV_NAME_CLI);
    if(NULL != pRiakHost && pRiakHost[0] != '\0') {
        ThrowWith("DMC�ֲ�ʽ����汾(export RIAK_HOSTNAME=%s):��֧�������������,������������ɷֲ�ʽ����Ⱥ���.",pRiakHost);
    }

    //Check deserved temporary(middle) fileset
    //���״̬Ϊ1������1Ϊ��ȡ�����ȴ�װ��.
    CheckEmptyDump();
    //2010-12-01 ���ӷ�������״̬������2��ѡ��
    //2011-07-02 ���������޸ģ����������м�������������
    mdf.FetchAll("select dp.status,mf.blevel,mf.tabid,mf.datapartid,mf.indexgid,sum(filesize) fsz from dp.dp_middledatafile mf,dp.dp_datapart dp "
                 " where mf.procstate=1 and mf.tabid=dp.tabid and mf.datapartid=dp.datapartid  "
                 " and ifnull(dp.blevel,0)%s and dp.status in(1,2) "// �������������� DMA-35 and mf.indexgid=1" //���������޸� 20110701
                 " group by mf.blevel,mf.tabid,mf.datapartid,mf.indexgid "
                 " order by dp.status desc,mf.blevel,mf.tabid,fsz desc,mf.datapartid,mf.indexgid limit %d",
                 sp->GetNormalTask()?"<100":">=100",DATATRIM_NUM);
    int rn=mdf.Wait();
    //�����������LOADIDXSIZE,��Ҫ��������,��ֻ�ڴ����һ������ʱ���DT_DATAFILEMAP/DT_INDEXFILEMAP��
    //���ֵ�һ�κ͵ڶ���װ������壺���һ�������Ӽ���ָ�����ݼ�-������(datapartid)->����)���������ڿ�ʼװ����ǰ��Ҫ
    //  ��ɾ���ϴ�����װ������ݣ�ע�⣺�����ߵ����ݲ�������ɾ��������װ�������ɾ��).
    bool firstbatch=true;
    if(rn<1) return 0;

    //>> begin: fix DMA-909 ,������ڵ����������������������в������ݵ�����
    int _datapart_index = 0;
    int _datapart_num = rn;
    StruDataTrimItem data_trim_arry[DATATRIM_NUM+1];
    for(int i=0; i<_datapart_num; i++) {
        data_trim_arry[i].tabid = mdf.GetInt("tabid",i);
        data_trim_arry[i].indexgid= mdf.GetInt("indexgid",i);
        data_trim_arry[i].datapartid = mdf.GetInt("datapartid",i);
    }

start_datatrim:
    //<< end: fix DMA-909

    //���������Ӽ��Ƿ��һ��װ��
    int tabid=data_trim_arry[_datapart_index].tabid;
    int indexid=data_trim_arry[_datapart_index].indexgid;
    int datapartid=data_trim_arry[_datapart_index].datapartid;

    mdf.FetchAll("select procstate from dp.dp_middledatafile "
                 " where tabid=%d and datapartid=%d and indexgid=%d and procstate>1 limit 10",
                 tabid,datapartid,indexid);
    firstbatch=mdf.Wait()<1;//�����Ӽ�û���������������ļ�¼��

    //ȡ���м��ļ���¼
    mdf.FetchAll("select * from dp.dp_middledatafile "
                 " where  tabid=%d and datapartid=%d and indexgid=%d and procstate=1 order by mdfid limit %d",
                 tabid,datapartid,indexid,MAX_MIDDLE_FILE_NUM);
    rn=mdf.Wait();
    if(rn<1) {
        sp->log(tabid,datapartid,MLOAD_CAN_NOT_FIND_MIDDLEDATA_ERROR,"��%d,����%d,ȷ�������Ӽ����Ҳ����м����ݼ�¼(δ����)��",tabid,datapartid);
        ThrowWith("MiddleDataLoader::Load : ȷ�������Ӽ����Ҳ����м����ݼ�¼(δ����)��");
    }
    long idxtlimit=0,idxdlimit=0;//��ʱ��(�����ȡ�����ļ���Ӧ)��Ŀ����(����Ŀ�������ļ���Ӧ)����������¼��.
    tabid=mdf.GetInt("tabid",0);
    sp->SetTrace("datatrim",tabid);
    indexid=mdf.GetInt("indexgid",0);
    datapartid=mdf.GetInt("datapartid",0);
    int compflag=5;
    //���������޸� 20110701
    int curdatapartstatus=1; //
    char db[300],table[300];
    bool col_object=false; // useOldBlock=trueʱ������col_object
    //ȡѹ�����ͺ����ݿ�����
    {
        AutoMt tmt(sp->GetDTS(),10);
        //���������޸� 20110701
        tmt.FetchAll("select compflag,status from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
        if(tmt.Wait()>0) {
            compflag=tmt.GetInt("compflag",0);
            //���������޸� 20110701
            curdatapartstatus=tmt.GetInt("status",0);
        }
        tmt.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
        if(tmt.Wait()>0) {
            if(TestColumn(tmt,"blocktype"))
                col_object=tmt.GetInt("blocktype",0)&1; // bit 1 means column object block type.
            strcpy(db,tmt.PtrStr("databasename",0));
            strcpy(table,tmt.PtrStr("tabname",0));
        }
    }
    //��dt_datafilemap(��blockmt�ļ���)��dt_indexfilemap(��indexmt�ļ���)
    //�����ڴ��ṹ
    char fn[300];
    AutoMt fnmt(sp->GetDTS(),MAX_DST_DATAFILENUM);

    fnmt.FetchAll("select * from dp.dp_datafilemap limit 2");
    fnmt.Wait();
    wociReset(fnmt);
    LONG64 dispct=0,lstdispct=0,llt=0;
    sp->OutTaskDesc("�������� :",tabid,datapartid,indexid);
    sp->log(tabid,datapartid,MLOAD_DATA_RECOMBINATION_NOTIFY,"��%d,����%d,��������,������%d,��־�ļ� '%s' .",tabid,datapartid,indexid,wociGetLogFile());
    int start_mdfid=0,end_mdfid=0;
    char sqlbf[MAX_STMT_LEN];
    LONG64 extrn=0,lmtextrn=-1;
    LONG64 adjrn=0;
    //IB����
    //�����ļ������try����
    FILE *ibfile=NULL;
    bool ib_engine=false;
    char ibfilename[300];
    try {
        tmpfilenum=rn;
        //���������ļ��������ۼ�����������
        LONG64 idxrn=0;
        long i;
        long mdrowlen=0;
        //ȡ������¼�ĳ���(��ʱ�������ݼ�¼)
        {
            dt_file df;
            df.Open(mdf.PtrStr("indexfilename",0),0);
            mdrowlen=df.GetRowLen();
        }

        lgprintf("��ʱ�������ݼ�¼����:%d",mdrowlen);
        long lmtrn=-1,lmtfn=-1;
        //�����ʱ�������ڴ�����,�жϵ�ǰ�Ĳ��������Ƿ����һ��װ��ȫ��������¼
        for( i=0; i<rn; i++) {
            dt_file df;
            df.Open(mdf.PtrStr("indexfilename",i),0);
            if(mdrowlen==0)
                mdrowlen=df.GetRowLen();
            extrn+=mdf.GetInt("recordnum",i);
            idxrn+=df.GetRowNum();
            llt=idxrn;
            llt*=mdrowlen;
            llt/=(1024*1024); //-->(MB)
            if(llt>LOADTIDXSIZE && lmtrn==-1) {
                //ʹ�õ���ʱ���������ڴ�������������ޣ���Ҫ���
                if(i==0) {
                    sp->log(tabid,datapartid,MLOAD_DP_LOADTIDXSIZE_TOO_LOW_ERROR,"�ڴ����DP_LOADTIDXSIZE����̫��:%dMB��������װ������һ����ʱ��ȡ��:%dMB��\n",LOADTIDXSIZE,(int)llt);
                    ThrowWith("MLoader:�ڴ����DP_LOADTIDXSIZE����̫��:%dMB��\n"
                              "������װ������һ����ʱ��ȡ��:%dMB��\n",LOADTIDXSIZE,(int)llt);
                }

                lmtrn=idxrn-df.GetRowNum();
                lmtfn=i;
                lmtextrn=extrn-mdf.GetInt("recordnum",i);
            }
            if(idxrn > (MAX_ROWS_LIMIT-8) && lmtrn==-1) {
                // ��¼��������2G������
                lmtrn=idxrn-df.GetRowNum();
                lmtfn=i;
                lmtextrn=extrn-mdf.GetInt("recordnum",i);
            }
            if((wociGetMaxMemTable()==(i+1)) && (-1==lmtrn)) {
                // ����ܹ��򿪵��ļ����ڴ���¼
                lmtrn=idxrn-df.GetRowNum();
                lmtfn=i;
                lmtextrn=extrn-mdf.GetInt("recordnum",i);
            }
        }
        if(lmtrn!=-1) { //ʹ�õ���ʱ���������ڴ�������������ޣ���Ҫ���
            lgprintf("MLoader:�����������ڴ�����%dMB,��Ҫ�����ļ���%d,������%d,��ʼ��:%d,������:%d,�ļ���:%d,��¼��:%d.",LOADTIDXSIZE,rn,datapartid,mdf.GetInt("mdfid",0),mdf.GetInt("mdfid",lmtfn-1),lmtfn,lmtrn);
            lgprintf("������Ҫ�ڴ�%dM ",idxrn*mdrowlen/(1024*1024));
            sp->log(tabid,datapartid,MLOAD_DATA_NOTIFY,
                    "MLoader:�����������ڴ�����%dMB,��Ҫ�����ļ���%d,��ʼ��:%d,������:%d,�ļ���:%d ,��¼��:%d,��Ҫ�ڴ�%dMB.",LOADTIDXSIZE,rn,mdf.GetInt("mdfid",0),mdf.GetInt("mdfid",lmtfn-1),lmtfn,lmtrn,idxrn*mdrowlen/(1024*1024));
            idxrn=lmtrn;
            //fix a bug
            rn=lmtfn;
        } else if(curdatapartstatus==1) {
            lgprintf("����������ʾ����Ҫ��������ݻ�������������ڴ���(�ļ���%d����%dM��¼%d�ڴ���%dM)���ȴ�...",rn,idxrn*mdrowlen/(1024*1024),extrn,LOADTIDXSIZE);

            //>>Begin:fix  DMA-909
            if(_datapart_index<_datapart_num-1) { // �÷������������ݴ���������������һ������
                _datapart_index++;
                goto start_datatrim;
            }
            //<<End:fix DMA-909

            return 0;
        }
        if(lmtextrn==-1) lmtextrn=extrn;
        lgprintf("��������ʵ��ʹ���ڴ�%dM,�����п�%d.",idxrn*mdrowlen/(1024*1024),mdrowlen);
        start_mdfid=mdf.GetInt("mdfid",0);
        end_mdfid=mdf.GetInt("mdfid",rn-1);
        lgprintf("������¼��:%d",idxrn);
        //Ϊ��ֹ��������,�м��ļ�״̬�޸�.
        lgprintf("�޸��м��ļ��Ĵ���״̬(tabid:%d,datapartid:%d,indexid:%d,%d���ļ�)��1-->2",tabid,datapartid,indexid,rn);
        //��ȡ���̻��2�Ļ�1������ȸ�Ϊ20��
        //�ͻ��˲��ܴ���20���Ļ�2
        sprintf(sqlbf,"update dp.dp_middledatafile set procstate=2 where tabid=%d  and datapartid=%d and indexgid=%d and procstate=1 and mdfid>=%d and mdfid<=%d ",
                tabid,datapartid,indexid,start_mdfid,end_mdfid);
        int ern=sp->DoQuery(sqlbf);
        if(ern!=rn) {
            if(ern>0) {  //�����UPdate����޸���һ���ּ�¼״̬,�Ҳ��ɻָ�,��Ҫ�������Ӽ�������װ��.
                sp->log(tabid,datapartid,MLOAD_UPDATE_MIDDLE_FILE_STATUS_ERROR,"��%d,����%d,�޸��м��ļ��Ĵ���״̬�쳣�����������������̳�ͻ.�����ļ��Ĵ���״̬��һ�£�������ֹͣ���е��������������������������������",
                        tabid,datapartid);

                ThrowWith("MLoader�޸��м��ļ��Ĵ���״̬�쳣�����������������̳�ͻ��\n"
                          " �����ļ��Ĵ���״̬��һ�£�������ֹͣ���е��������������������������������\n"
                          "  tabid:%d,datapartid:%d,indexid:%d.\n",
                          tabid,datapartid,indexid);
            } else { //�����update���δ����ʵ���޸Ĳ���,�������̿��Լ�������.
                //��Ҫ��ThrowWith,���������catch�лָ�dp_middledatafile��.
                errprintf("MLoader�޸��м��ļ��Ĵ���״̬�쳣�����������������̳�ͻ��\n"
                          "  tabid:%d,datapartid:%d,indexid:%d.\n",
                          tabid,datapartid,indexid);
                return 0;
            }
        }

        //ThrowWith("������ֹ---%d������.",dispct);
        if(firstbatch) {
            lgprintf("ɾ�����ݷ���%d,����%d �����ݺ�������¼(��:%d)...",datapartid,indexid,tabid);
            sp->log(tabid,datapartid,MLOAD_DELETE_DATA_NOTIFY,"��%d,����%d,ɾ��������%d��ȥ���ɵ����ݺ�������¼...",tabid,datapartid,indexid);
            AutoMt dfnmt(sp->GetDTS(),MAX_DST_DATAFILENUM);
            dfnmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and datapartid=%d and indexgid=%d and fileflag=1",tabid,datapartid,indexid);
            int dfn=dfnmt.Wait();
            if(dfn>0) {
                AutoStmt st(sp->GetDTS());
                for(int di=0; di<dfn; di++) {
                    lgprintf("ɾ��'%s'/'%s'�͸��ӵ�depcp,dep5�ļ�",dfnmt.PtrStr("filename",di),dfnmt.PtrStr("idxfname",di));
                    unlink(dfnmt.PtrStr("filename",di));
                    unlink(dfnmt.PtrStr("idxfname",di));
                    char tmp[300];
                    sprintf(tmp,"%s.depcp",dfnmt.PtrStr("filename",di));
                    unlink(tmp);
                    sprintf(tmp,"%s.dep5",dfnmt.PtrStr("filename",di));
                    unlink(tmp);
                    sprintf(tmp,"%s.depcp",dfnmt.PtrStr("idxfname",di));
                    unlink(tmp);
                    sprintf(tmp,"%s.dep5",dfnmt.PtrStr("idxfname",di));
                    unlink(tmp);
                }
                st.Prepare(" delete from dp.dp_datafilemap where tabid=%d and datapartid=%d and indexgid=%d and fileflag=1",tabid,datapartid,indexid);
                st.Execute(1);
                st.Wait();
            }
        }

        //�����м�����(�м��ļ����ݿ�����)�ڴ��mdindexmt��Ŀ�����ݿ��ڴ��blockmt
        int maxblockrn=sp->GetMaxBlockRn(tabid);
        {
            dt_file idf;
            idf.Open(mdf.PtrStr("indexfilename",0),0);
            mdindexmt.SetHandle(idf.CreateMt(idxrn));
            //wociAddColumn(idxmt,"dtfileid",NULL,COLUMN_TYPE_INT,4,0);
            //idxmt.SetMaxRows(idxrn);
            //mdindexmt.Build();
            idf.Open(mdf.PtrStr("datafilename",0),0);
            blockmt.SetHandle(idf.CreateMt(maxblockrn));
            //mdblockmt.SetHandle(idf.CreateMt(maxblockrn));
        }
        //IB������ð��и�ʽ
        ib_engine=col_object;
        if(!ib_engine) {
            sp->log(tabid,datapartid,MLOAD_STORAGE_FORMAT_ERROR,"DM�汾��֧�ֱ�%d�Ĵ洢��ʽ",tabid);
            ThrowWith("DM�汾��֧�ֱ�%d�Ĵ洢��ʽ",tabid);
        }
        MySQLConn ibe;
        IBDataFile ib_datafile;


        LONG64 crn=0;
        //  wociGetIntAddrByName(idxmt,"dtfileid",0,&pdtfid);
        // pdtfidΪһ���ַ����飬ƫ��Ϊx��ֵ��ʾ�м������ڴ���x�е��ļ����(Base0);
        if(pdtfid)
            delete [] pdtfid;
        pdtfid=new unsigned short [idxrn];
        //dtfidlen=idxrn;
        //pdtfΪfile_mt��������顣��������ļ�����
        if(pdtf) delete [] pdtf;
        pdtf=new file_mt[rn];
        //mdindexmt.SetMaxRows(idxrn);
        //����ȫ���������ݵ�mdindexmt(�м������ڴ��),����ȫ�������ļ�
        //pdtfidָ���Ӧ���ļ���š�
        lgprintf("����������...");
        for(i=0; i<rn; i++) {
            dt_file df;
            df.Open(mdf.PtrStr("indexfilename",i),0);
            int brn=0;
            int sbrn=0;
            while( (sbrn=df.ReadMt(-1,0,mdindexmt,false))>0) brn+=sbrn;

            for(int j=crn; j<crn+brn; j++)
                pdtfid[j]=(unsigned short )i;
            crn+=brn;

            //pdtf[i].SetParalMode(true);
            pdtf[i].Open(mdf.PtrStr("datafilename",i),0);
            //      if(crn>10000000) break; ///DEBUG
        }
        lgprintf("��������:%d.",crn);
        if(crn!=idxrn) {
            sp->log(tabid,0,MLOAD_INDEX_DATA_FILE_RECORD_NUM_ERROR,"��%d,���������ļ����ܼ�¼��%lld,��ָʾ��Ϣ��һ��:%lld",tabid,crn,idxrn);
            ThrowWith("���������ļ����ܼ�¼��%lld,��ָʾ��Ϣ��һ��:%lld",crn,idxrn);
        }
        //��mdindexmt(�м������ڴ��)������
        //���������漰�ڴ����������У������½���¼˳�����ˣ�
        // pdtfid��Ϊ�ڴ����ĵ�Ч�ڴ���ֶΣ�����������
        lgprintf("����('%s')...",mdf.PtrStr("soledindexcols",0));
        {
            char sort[300];
            sprintf(sort,"%s,idx_fid,idx_blockoffset",mdf.PtrStr("soledindexcols",0));
            wociSetSortColumn(mdindexmt,sort);
            wociSortHeap(mdindexmt);
        }
        lgprintf("�������.");
        //ȡ��ȫ�����������ṹ
        sp->GetSoledIndexParam(datapartid,&dp,tabid);
        //�����Ҫ������м������Ƿ�ʹ������������������ǣ�isfirstidx=1.
        int isfirstidx=0;
        indexparam *ip;
        {
            int idxp=dp.GetOffset(indexid);
            ip=&dp.idxp[idxp];
            if(idxp==dp.psoledindex) isfirstidx=1;
        }
        //�ӽṹ�����ļ�����indexmt,indexmt��Ŀ�������ڴ���ǽ���Ŀ�������������Դ��
        //indexmt.SetHandle(CreateMtFromFile(MAXINDEXRN,ip->cdsfilename));
        int pblockc[20];
        char colsname[500];
        void *indexptr[40];
        indexmt.SetMaxRows(1);
        int stcn=sp->CreateIndexMT(indexmt,blockmt,tabid,indexid,pblockc,colsname,true),bcn=stcn;
        llt=FIX_MAXINDEXRN;
        llt/=wociGetRowLen(indexmt); //==> to rownum;
        idxdlimit=(int)llt;
        indexmt.SetMaxRows(idxdlimit);
        sp->CreateIndexMT(indexmt,blockmt,tabid,indexid,pblockc,colsname,true);
        bool pkmode=false;
        //ȡ����������mdindexmt(�м��������)�ṹ�е�λ�á�
        //���ö�indexmt�����¼��Ҫ�Ľṹ�ͱ�����
        int pidxc1[20];
        int cn1=wociConvertColStrToInt(mdindexmt,ip->idxcolsname,pidxc1);
        int dtfid,blocksize,blockrn=0,startrow,rownum;
        int blockstart=0;
        indexptr[stcn++]=&dtfid;
        indexptr[stcn++]=&blockstart;
        indexptr[stcn++]=&blocksize;
        indexptr[stcn++]=&blockrn;
        indexptr[stcn++]=&startrow;
        indexptr[stcn++]=&rownum;
        indexptr[stcn]=NULL;
        //indexmt�е�blocksize,blockstart?,blockrownum��Ҫ�ͺ�д�룬
        //�����Ҫȡ����Щ�ֶε��׵�ַ��
        int *pblocksize;
        int *pblockstart;
        int *pblockrn;
        wociGetIntAddrByName(indexmt,"blocksize",0,&pblocksize);
        wociGetIntAddrByName(indexmt,"blockstart",0,&pblockstart);
        wociGetIntAddrByName(indexmt,"blockrownum",0,&pblockrn);
        //mdindexmt�������ֶ��Ƕ��м������ļ��Ĺؼ��
        LONG64 *poffset;
        int *pstartrow,*prownum;
        wociGetLongAddrByName(mdindexmt,"idx_blockoffset",0,&poffset);
        wociGetIntAddrByName(mdindexmt,"idx_startrow",0,&pstartrow);
        wociGetIntAddrByName(mdindexmt,"idx_rownum",0,&prownum);

        //indexmt ��¼����������λ
        int indexmtrn=0;

        //����Ŀ�������ļ���Ŀ�������ļ�����(dt_file).
        // Ŀ�������ļ���Ŀ�������ļ�һһ��Ӧ��Ŀ�������ļ��а��ӿ鷽ʽ�洢�ڴ��
        //  Ŀ�������ļ���Ϊһ����һ���ڴ���ļ�ͷ�ṹ��д���ڴ��ʱ����
        dtfid=sp->NextDstFileID(tabid);
        char tbname[150];
        sp->GetTableName(tabid,-1,tbname,NULL,TBNAME_DEST);
        dp.usingpathid=0;
        sprintf(fn,"%s%s_%d_%d_%d.dat",dp.dstpath[0],tbname,datapartid,indexid,dtfid);
        {
            FILE *fp;
            fp=fopen(fn,"rb");
            if(fp!=NULL) {
                fclose(fp);
                fp = NULL;
                sp->log(tabid,datapartid,MLOAD_CAN_NOT_MLOAD_DATA_ERROR,"��%d,����%d,�ļ�'%s'�Ѿ����ڣ����ܼ����������ݡ�",tabid,datapartid,fn);
                ThrowWith("�ļ�'%s'�Ѿ����ڣ����ܼ����������ݡ�",fn);
            }
        }
        dt_file dstfile;
        bool trimtofile=false;
        // pipe file init moved

        if(!ib_engine) {
            dstfile.Open(fn,1);
            blockstart=dstfile.WriteHeader(blockmt,0,dtfid);
        } else {
            ib_datafile.SetFileName(fn);
            ib_datafile.CreateInit(1024*1024);
            blockstart=0;
        }
        char idxfn[300];
        sprintf(idxfn,"%s%s_%d_%d_%d.idx",dp.dstpath[0],tbname,datapartid,indexid,dtfid);
        dt_file idxf;
        // IB does not use index file
        if(!ib_engine) {
            idxf.Open(idxfn,1);
            idxf.WriteHeader(indexmt,0,dtfid);
        }
        startrow=0;
        rownum=0;
        blockrn=0;
        int subtotrn=0;
        int idxtotrn=0;
        lgprintf("��ʼ���ݴ���(MiddleDataLoading)....");

        /*******����Sort˳�����mdindexmt(�м������ڴ��)***********/
        lgprintf("�����ļ�,���:%d...",dtfid);
        int firstrn=wociGetRawrnBySort(mdindexmt,0);
        mytimer arrtm;
        arrtm.Start();
        for(i=0; i<idxrn; i++) {
            int thisrn=wociGetRawrnBySort(mdindexmt,i);
            int rfid=pdtfid[thisrn];
            int sbrn=prownum[thisrn];
            int sbstart=pstartrow[thisrn];
            int sameval=mdindexmt.CompareRows(firstrn,thisrn,pidxc1,cn1);
            if(sameval!=0) {
                //Ҫ��������ݿ���ϴε����ݿ鲻��һ���ؼ��֣�
                // ���ԣ��ȱ����ϴεĹؼ����������ؼ��ֵ�ֵ��blockmt����ȡ��
                // startrow����ʼ�ձ����һ��δ�洢�ؼ������������ݿ鿪ʼ�кš�
                int c;
                for(c=0; c<bcn; c++) {
                    if(blockmt.isVarStrCol(pblockc[c])) {
                        int nllen = 0;
                        indexptr[c]=(void*)blockmt.GetVarLenStrAddr(pblockc[c],startrow,&nllen);
                    } else {
                        indexptr[c]=blockmt.PtrVoidNoVar(pblockc[c],startrow);
                    }
                }
                if(rownum>0) {
                    wociInsertRows(indexmt,indexptr,NULL,1);
                    idxtotrn++;
                }
                firstrn=thisrn;
                //�������startrow��ʱָ����Ч�е����(����δ���).
                startrow=blockrn;
                rownum=0;
            }
            //�������ļ��ж������ݿ�
            int mt=pdtf[rfid].ReadBlock(poffset[thisrn],0,true);
            //2005/08/24 �޸ģ� ���������ļ�Ϊ�������˳��洢

            //���ڽ�����ǰ�ļ��п��ܻ�Ҫ����һ������,����ж��ڴ����������Ϊ(idxdlimit-1)
            int irn=wociGetMemtableRows(indexmt);
            if(irn>=(idxdlimit-2)) {
                int pos=irn-1;
                //���blocksize��blockrn���ֶλ�δ���ã������
                while(pos>=0 && pblockstart[pos]==blockstart) pos--;
                if(pos>0) {
                    //�����Ѿ�������������������,false������ʾ����Ҫɾ��λͼ��.
                    //IB���� ������������д��
                    if(!ib_engine)
                        idxf.WriteMt(indexmt,COMPRESSLEVEL,pos+1,false);
                    if(pos+1<irn)
                        wociCopyRowsTo(indexmt,indexmt,0,pos+1,irn-pos-1);
                    else wociReset(indexmt);
                } else {
                    sp->log(tabid,datapartid,MLOAD_INDEX_NUM_OVER_ERROR,"Ŀ���%d,������%d,װ��ʱ�����������һ�������¼��%d",tabid,indexid,idxdlimit);
                    ThrowWith("Ŀ���%d,������%d,װ��ʱ�����������һ�������¼��%d",tabid,indexid,idxdlimit);
                }
            }
            //���ݿ���
            //������ݿ��Ƿ���Ҫ���
            // ����һ��ѭ�������ڴ���mt�е�sbrn�������maxblockrn���쳣�����
            //  �����쳣ԭ������ʱ����������������ݿ飬�������ۺ�����ʱ������ֲ���һ������������ʱ��¼
            while(true) {
                if(blockrn+sbrn>maxblockrn ) {
                    //ÿ�����ݿ�������Ҫ�ﵽ���ֵ��80%��
                    if(blockrn<maxblockrn*.8 ) {
                        //�������80%���ѵ�ǰ��������ݿ���
                        int rmrn=maxblockrn-blockrn;
                        wociCopyRowsTo(mt,blockmt,-1,sbstart,rmrn);
                        rownum+=rmrn;
                        sbrn-=rmrn;
                        sbstart+=rmrn;
                        blockrn+=rmrn;
                    }

                    //���������
                    //IB���� д������
                    if(ib_engine) {
                        //replace with a new black(compressed) writor
                        //blocksize=1,wociCopyToIBBinary(blockmt,0,0,ibfile);
                        blocksize=ib_datafile.Write(blockmt);
                    } else if(useOldBlock)
                        blocksize=dstfile.WriteMt(blockmt,compflag,0,false)-blockstart;
                    else if(col_object)
                        blocksize=dstfile.WriteMySQLMt(blockmt,compflag)-blockstart;
                    else
                        blocksize=dstfile.WriteMySQLMt(blockmt,compflag,false)-blockstart;
                    adjrn+=wociGetMemtableRows(blockmt);
                    //�����ӿ�����
                    if(startrow<blockrn) {
                        int c;
                        //  for(c=0;c<bcn;c++) {
                        //  indexptr[c]=blockmt.PtrVoid(pblockc[c],startrow);
                        //}
                        for(c=0; c<bcn; c++) {
                            if(blockmt.isVarStrCol(pblockc[c])) {
                                int nllen = 0;
                                indexptr[c]=(void*)blockmt.GetVarLenStrAddr(pblockc[c],startrow,&nllen);
                            } else {
                                indexptr[c]=blockmt.PtrVoidNoVar(pblockc[c],startrow);
                            }
                        }
                        if(rownum>0) {
                            wociInsertRows(indexmt,indexptr,NULL,1);
                            dispct++;
                            idxtotrn++;
                        }
                    }
                    int irn=wociGetMemtableRows(indexmt);
                    int irn1=irn;
                    while(--irn>=0) {
                        if(pblockstart[irn]==blockstart) {
                            pblocksize[irn]=blocksize;
                            pblockrn[irn]=blockrn;
                        } else break;
                    }

                    blockstart+=blocksize;
                    subtotrn+=blockrn;
                    //�����ļ����ȳ���2Gʱ���
                    // IB may be need spilt more small to provide more parallel performance
                    // TODO : JIRA DMA-36
                    // ��Ϊ1G
                    if(blockstart>1*1024*1024*1024 ) {
                        //�����ļ����ձ��¼(dt_datafilemap)
                        {
                            void *fnmtptr[20];
                            int idxfsize=0;
                            //IB���� д������
                            if(!ib_engine)  idxfsize=idxf.WriteMt(indexmt,COMPRESSLEVEL,0,false);
                            fnmtptr[0]=&dtfid;
                            fnmtptr[1]=fn;
                            fnmtptr[2]=&datapartid;
                            fnmtptr[3]=&dp.dstpathid[dp.usingpathid];
                            fnmtptr[4]=&dp.tabid;
                            fnmtptr[5]=&ip->idxid;
                            fnmtptr[6]=&isfirstidx;
                            fnmtptr[7]=&blockstart;
                            fnmtptr[8]=&subtotrn;
                            int procstatus=0;/*ib_engine?10:0;*/
                            fnmtptr[9]=&procstatus;
                            //int compflag=COMPRESSLEVEL;
                            fnmtptr[10]=&compflag;
                            int fileflag=1;
                            fnmtptr[11]=&fileflag;
                            fnmtptr[12]=idxfn;
                            fnmtptr[13]=&idxfsize;
                            fnmtptr[14]=&idxtotrn;
                            fnmtptr[15]=NULL;
                            wociInsertRows(fnmt,fnmtptr,NULL,1);
                        }
                        //
                        dtfid=sp->NextDstFileID(tabid);
                        sprintf(fn,"%s%s_%d_%d_%d.dat",dp.dstpath[0],tbname,datapartid,indexid,dtfid);
                        {
                            FILE *fp;
                            fp=fopen(fn,"rb");
                            if(fp!=NULL) {
                                fclose(fp);
                                sp->log(tabid,datapartid,MLOAD_FILE_EXISTS_ERROR,"��%d,����%d,�ļ�'%s'�Ѿ����ڣ����ܼ����������ݡ�",tabid,datapartid,fn);
                                ThrowWith("�ļ�'%s'�Ѿ����ڣ����ܼ����������ݡ�",fn);
                            }
                        }
                        if(!ib_engine)  {
                            dstfile.SetFileHeader(subtotrn,fn);
                            dstfile.Open(fn,1);
                            blockstart=dstfile.WriteHeader(blockmt,0,dtfid);
                        } else {
                            ib_datafile.CloseWriter();
                            ib_datafile.SetFileName(fn);
                            ib_datafile.CreateInit(1024*1024);
                            blockstart=0;
                        }
                        printf("\r                                                                            \r");
                        lgprintf("�����ļ�,���:%d...",dtfid);
                        sprintf(idxfn,"%s%s_%d_%d_%d.idx",dp.dstpath[0],tbname,datapartid,indexid,dtfid);

                        // create another file
                        if(!ib_engine)  {
                            idxf.SetFileHeader(idxtotrn,idxfn);
                            idxf.Open(idxfn,1);
                            idxf.WriteHeader(indexmt,0,dtfid);
                        }

                        indexmt.Reset();
                        subtotrn=0;
                        blockrn=0;
                        idxtotrn=0;
                        //lgprintf("�����ļ�,���:%d...",dtfid);

                    } // end of IF blockstart>2000000000)
                    blockmt.Reset();
                    blockrn=0;
                    firstrn=thisrn;
                    startrow=blockrn;
                    rownum=0;
                    dispct++;
                    if(wdbi_kill_in_progress) {
                        wdbi_kill_in_progress=false;
                        ThrowWith("�û�ȡ������!");
                    }
                    if(dispct-lstdispct>=200) {
                        lstdispct=dispct;
                        arrtm.Stop();
                        double tm1=arrtm.GetTime();
                        arrtm.Start();
                        printf("  ������%lld���ݿ�(%ld),��ʱ%.0f��,Ԥ�ƻ���Ҫ%.0f��          .\r",dispct,i,tm1,(tm1/(double)i*(idxrn-i)));
                        fflush(stdout);
                    }
                } //end of blockrn+sbrn>maxblockrn
                if(blockrn+sbrn>maxblockrn) {
                    int rmrn=maxblockrn-blockrn;
                    wociCopyRowsTo(mt,blockmt,-1,sbstart,rmrn);
                    rownum+=rmrn;
                    sbrn-=rmrn;
                    sbstart+=rmrn;
                    blockrn+=rmrn;
                } else {
                    wociCopyRowsTo(mt,blockmt,-1,sbstart,sbrn);
                    rownum+=sbrn;
                    blockrn+=sbrn;
                    break;
                }
            } // end of while(true)
        } // end of for(...)
        if(blockrn>0) {
            //�����ӿ�����
            int c;
            for(c=0; c<bcn; c++) {
                if(blockmt.isVarStrCol(pblockc[c])) {
                    int nllen = 0;
                    indexptr[c]=(void*)blockmt.GetVarLenStrAddr(pblockc[c],startrow,&nllen);
                } else {
                    indexptr[c]=blockmt.PtrVoidNoVar(pblockc[c],startrow);
                }
            }
            //for(c=0;c<bcn2;c++) {
            //  indexptr[bcn1+c]=blockmt.PtrVoid(pblockc2[c],startrow);
            //}
            if(rownum>0) {
                wociInsertRows(indexmt,indexptr,NULL,1);
                dispct++;
            }
            //���������
            //���������
            //IB���� д������
            if(ib_engine) {
                //replace with a new black(compressed) writor
                //blocksize=1,wociCopyToIBBinary(blockmt,0,0,ibfile);
                blocksize=ib_datafile.Write(blockmt);
            } else if(useOldBlock)
                blocksize=dstfile.WriteMt(blockmt,compflag,0,false)-blockstart;
            else if(col_object)
                blocksize=dstfile.WriteMySQLMt(blockmt,compflag)-blockstart;
            else
                blocksize=dstfile.WriteMySQLMt(blockmt,compflag,false)-blockstart;
            int irn=wociGetMemtableRows(indexmt);
            adjrn+=wociGetMemtableRows(blockmt);
            while(--irn>=0) {
                if(pblockstart[irn]==blockstart) {
                    pblocksize[irn]=blocksize;
                    pblockrn[irn]=blockrn;
                } else break;
            }
            blockstart+=blocksize;
            subtotrn+=blockrn;
            //�����ļ����ձ��¼(dt_datafilemap)
            {
                void *fnmtptr[20];
                //IB���� д������
                int idxfsize=0;
                if(!ib_engine)
                    idxfsize=idxf.WriteMt(indexmt,COMPRESSLEVEL,0,false);
                fnmtptr[0]=&dtfid;
                fnmtptr[1]=fn;
                fnmtptr[2]=&datapartid;
                fnmtptr[3]=&dp.dstpathid[dp.usingpathid];
                fnmtptr[4]=&dp.tabid;
                fnmtptr[5]=&ip->idxid;
                fnmtptr[6]=&isfirstidx;
                fnmtptr[7]=&blockstart;
                fnmtptr[8]=&subtotrn;
                int procstatus=0;/*ib_engine?10:0;*/
                fnmtptr[9]=&procstatus;
                //int compflag=COMPRESSLEVEL;
                fnmtptr[10]=&compflag;
                int fileflag=1;
                fnmtptr[11]=&fileflag;
                fnmtptr[12]=idxfn;
                fnmtptr[13]=&idxfsize;
                fnmtptr[14]=&idxtotrn;
                fnmtptr[15]=NULL;
                wociInsertRows(fnmt,fnmtptr,NULL,1);
            }

            if(!ib_engine)  {
                dstfile.SetFileHeader(subtotrn,NULL);
            } else {
                ib_datafile.CloseWriter();
            }
            if(!ib_engine)  idxf.SetFileHeader(idxtotrn,NULL);
            indexmt.Reset();
            blockmt.Reset();
            //IB ���� �ر��ļ�
            //IB���� д������
            //if(ib_engine) {
            //fclose(ibfile);
            //if(!trimtofile) unlink(ibfilename);
            //}
            blockrn=0;
            startrow=blockrn;
            rownum=0;
        }

        //��¼��У�顣�����У����Ǳ����������飬�п�����һ���������ĳ���������һ���֡������У������һ�������飬У������������
        if(adjrn!=lmtextrn) {
            sp->log(tabid,datapartid,MLOAD_CHECK_RESULT_ERROR,"��%d,����%d,��������Ҫ����������%lld�У���ʵ������%lld��! ������%d.",tabid,datapartid,lmtextrn,adjrn,indexid);
            ThrowWith("��������Ҫ����������%lld�У���ʵ������%lld��! ��%d(%d),������%d.",lmtextrn,adjrn,tabid,datapartid,indexid);
        }
        wociAppendToDbTable(fnmt,"dp.dp_datafilemap",sp->GetDTS(),true);
        sp->log(tabid,datapartid,MLOAD_DATA_NOTIFY,"�޸��м��ļ��Ĵ���״̬(��%d,����%d,������:%d,%d���ļ�)��2-->3",tabid,indexid,datapartid,rn);
        lgprintf("�޸��м��ļ��Ĵ���״̬(��%d,����%d,������:%d,%d���ļ�)��2-->3",tabid,indexid,datapartid,rn);
        //��ȡ���̻��2�Ļ�1������ȸ�Ϊ20
        //JIRA DMA-25
        sprintf(sqlbf,"update dp.dp_middledatafile set procstate=3 where tabid=%d and datapartid=%d and indexgid=%d and procstate=2 and mdfid>=%d and mdfid<=%d ",
                tabid,datapartid,indexid,start_mdfid,end_mdfid);
        sp->DoQuery(sqlbf);
        sp->log(tabid,datapartid,MLOAD_DATA_NOTIFY,"��%d,����%d,��������,������:%d,��¼��%lld.",tabid,datapartid,indexid,lmtextrn);
    } catch (...) {
        //IB ���� �ر��ļ�
        //TODO��ȱ��ʧ�ܻ��˻���
        //IB���� д������
        int frn=wociGetMemtableRows(fnmt);
        errprintf("������������쳣����:%d,������:%d.",tabid,datapartid);
        sp->log(tabid,datapartid,MLOAD_EXCEPTION_ERROR,"��%d,����%d,������������쳣",tabid,datapartid);
        errprintf("�ָ��м��ļ��Ĵ���״̬(������:%d,%d���ļ�)��2-->1",datapartid,rn);
        sprintf(sqlbf,"update dp.dp_middledatafile set procstate=1,blevel=ifnull(blevel,0)+1 where tabid=%d and datapartid=%d and indexgid=%d and mdfid>=%d and mdfid<=%d",tabid,datapartid,indexid,start_mdfid,end_mdfid);
        sp->DoQuery(sqlbf);
        sprintf(sqlbf,"update dp.dp_datapart set blevel=ifnull(blevel,0)+100 where tabid=%d and datapartid=%d ",tabid,datapartid);
        sp->DoQuery(sqlbf);
        errprintf("ɾ������������ݺ������ļ�.");
        errprintf("ɾ�������ļ�...");
        int i;
        for(i=0; i<frn; i++) {
            errprintf("\t %s ",fnmt.PtrStr("filename",i));
            errprintf("\t %s ",fnmt.PtrStr("idxfname",i));
        }
        for(i=0; i<frn; i++) {
            unlink(fnmt.PtrStr("filename",i));
            unlink(fnmt.PtrStr("idxfname",i));
        }
        errprintf("ɾ���Ѵ��������ļ��������ļ���¼...");
        AutoStmt st(sp->GetDTS());
        for(i=0; i<frn; i++) {
            st.Prepare("delete from dp.dp_datafilemap where tabid=%d and datapartid=%d and indexgid=%d and fileflag=1 and fileid=%d",tabid,datapartid,indexid,fnmt.PtrInt("fileid",i));
            st.Execute(1);
            st.Wait();
        }
        wociCommit(sp->GetDTS());
        sp->logext(tabid,datapartid,EXT_STATUS_ERROR,"");
        sp->log(tabid,datapartid,MLOAD_DATA_NOTIFY,"��%d,����%d,��������д���״̬�ѻָ�.",tabid,datapartid);
        throw ;
    }

    lgprintf("���ݴ���(MiddleDataLoading)����,���������ݰ�%lld��.",dispct);
    lgprintf("����%d�������ļ�,�Ѳ���dp.dp_datafilemap��.",wociGetMemtableRows(fnmt));
    sp->log(tabid,datapartid,MLOAD_DATA_NOTIFY,"��%d,����%d,�����������,���������ݰ�%lld��,�����ļ�%d��.",tabid,datapartid,dispct,wociGetMemtableRows(fnmt));
    //wociMTPrint(fnmt,0,NULL);
    //����Ƿ�����ݷ�������һ������
    // ��������һ�����ݣ���˵����
    //  1. ��ִ�������һ�����������Ѵ����ꡣ
    //  2. ���и÷����Ӧ��һ������������������������ɡ�
    //���������²�ɾ����������ʱ���ݡ�
    try {
        // TODO : JIRA DMA-37 ���������װ�벻�ָܻ�����
        //�����������������ͬһ���������������ε����ݣ����������Ӽ����Ƿ�������ϡ�
        mdf.FetchAll("select * from dp.dp_middledatafile where procstate!=3 and tabid=%d and datapartid=%d limit 10",
                     tabid,datapartid);
        int rn=mdf.Wait();
        //JIRA DMA-148 skip temporary file clean
        if(rn==0) {
            mdf.FetchAll("select compflag,status from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
            if( mdf.Wait()>0 && mdf.GetInt("status",0)>=2) {// && curdatapartstatus==2) {
                mdf.FetchAll("select sum(recordnum) rn from dp.dp_middledatafile where tabid=%d and datapartid=%d and indexgid=%d",
                             tabid,datapartid,indexid);
                mdf.Wait();
                LONG64 trn=mdf.GetLong("rn",0);
                //Jira DMA-145 ,�������ڴ治����һ�����ȫ���������ݵ�����ʱ,��һ�������ݿ����Ѿ���װ��Ĺ��̰�״̬�޸�Ϊ11/12,�������У����Ҫ����11/12
                mdf.FetchAll("select sum(recordnum) rn from dp.dp_datafilemap where tabid=%d and datapartid=%d and indexgid=%d and fileflag=1 and procstatus in(0,1,2)",
                             tabid,datapartid,indexid);
                mdf.Wait();
                //����У��
                if(trn!=mdf.GetLong("rn",0)) {
                    //δ֪ԭ������Ĵ��󣬲���ֱ�ӻָ�״̬. ��ͣ�����ִ��
                    sp->log(tabid,datapartid,MLOAD_CHECK_RESULT_ERROR,"��%d,����%d,����У�����,����%lld�У���������%lld��(��֤������%d),����Ǩ�ƹ����ѱ���ͣ��",tabid,datapartid,trn,mdf.GetLong("rn",0),indexid);
                    sprintf(sqlbf,"update dp.dp_datapart set oldstatus=status,status=70,blevel=ifnull(blevel,0)+100 where tabid=%d and datapartid=%d",tabid,datapartid);
                    sp->DoQuery(sqlbf);
                    ThrowWith("����У�����,��%d(%d),����%lld�У���������%lld��(��֤������%d),����Ǩ�ƹ����ѱ���ͣ",tabid,datapartid,trn,mdf.GetLong("rn",0),indexid);
                }
                if(mdf.GetLong("rn",0))
                    lgprintf("���һ�������Ѵ�����,����״̬2-->%d,��%d(%d)",ib_engine?3:3,tabid,datapartid);
                //����ǵ������������񣬱�����������ͬ���ݼ�������״̬Ϊ3������������һ���Ĳ���������װ�룩��
                //IB ���죬�����������񣬲�����Ҫװ�����
                //�޸� DMA �� �����װ��ֿ� ����ִ��
                sprintf(sqlbf,"update dp.dp_datapart set status=%d where tabid=%d and datapartid=%d",
                        /*ib_engine?5:*/3,
                        tabid,datapartid);
                sp->DoQuery(sqlbf);
                //���´�����ṹ��
                //sp->CreateDT(tabid);
                //}
                //2011/07/03 ������ʱ�޸� �ݲ�ɾ����ʱ�ļ�

                lgprintf("ɾ���м���ʱ�ļ�...");
                mdf.FetchAll("select * from dp.dp_middledatafile where tabid=%d and datapartid=%d and procstate=3",tabid,datapartid);
                int dfn=mdf.Wait();
                {
                    for(int di=0; di<dfn; di++) {
                        lgprintf("ɾ���ļ�'%s'",mdf.PtrStr("datafilename",di));
                        unlink(mdf.PtrStr("datafilename",di));
                        lgprintf("ɾ���ļ�'%s'",mdf.PtrStr("indexfilename",di));
                        unlink(mdf.PtrStr("indexfilename",di));
                    }
                    lgprintf("ɾ����¼...");
                    AutoStmt st(sp->GetDTS());
                    st.Prepare("delete from dp.dp_middledatafile where tabid=%d and datapartid=%d and procstate=3",tabid,datapartid);
                    st.Execute(1);
                    st.Wait();
                }
            }
        }
    } catch(...) {
        sp->logext(tabid,datapartid,EXT_STATUS_ERROR,"");
        errprintf("����������������ɣ�������״̬��������ʱ�м��ļ�ɾ��ʱ���ִ�����Ҫ�˹�������\n��%d(%d)��",tabid,datapartid);
        sp->log(tabid,datapartid,MLOAD_EXCEPTION_ERROR,"����������������ɣ�������״̬��������ʱ�м��ļ�ɾ��ʱ���ִ�����Ҫ�˹�������\n��%d(%d)��",tabid,datapartid);
        throw;
    }
    return 1;
    //Load index data into memory table (indexmt)
}


//>> begin: fix dma-739
const int conFileNameLen=256;
typedef struct StruLoadFileItem {
    int     fileid;    // װ����ļ�id
    char    filename[conFileNameLen];   // װ����ļ�����
    long    recordnum;  // ��¼����
    StruLoadFileItem():fileid(0),recordnum(0)
    {
        filename[0] = 0;
    }
} StruLoadFileItem,*StruLoadFileItemPtr;
const int conMaxLoadFileNum = 50;
typedef struct StruLoadFileArray {
    int     tabid;
    int     datapartid;
    int     indexid;
    int     filenum;    // С��conMaxLoadFileNum
    StruLoadFileItem  stLoadFileLst[conMaxLoadFileNum];
    StruLoadFileArray():tabid(-1),indexid(0),datapartid(-1),filenum(0)
    {
    }
} StruLoadFileArray,*StruLoadFileArrayPtr;
//<< end: fix dma-739

void ServerLoadData(ThreadList *tl)
{
    int dbs=(long)tl->GetParams()[0];
    const char *loadsql=(const char *)tl->GetParams()[1];
    AutoStmt st(dbs);

    /* exception catched in caller
    try {
    */
    st.ExecuteNC("set @bh_dataformat='binary'");
    st.ExecuteNC("set autocommit=0");
    // auto commit in this procedure/ no effect!!
    st.ExecuteNC(loadsql);
    lgprintf("�����ύ...");
    // must commit!!
    st.ExecuteNC("commit");
    lgprintf("�ύ�ɹ�.");
    tl->SetReturnValue(TRET_OK);
    /*
    }
    catch(WDBIError &e) {
        char *msg=NULL;
        int errcode=0;
        e.GetLastError(errcode,&msg);
        tl->SetError(errcode,msg);
    }
    catch(...) {
        tl->SetError(-1,"Unknow error in loading data thread(ServerLoadData).");
    }
    */
}
// return piped row number to param[4]
void PipeData(ThreadList *tl)
{
    IBDataFile *pibdatafile=(IBDataFile *) tl->GetParams()[0];      // �����ļ�ת�������
    const char *pipefilename=(const char*)tl->GetParams()[1];       // �ܵ��ļ����
    // open with 'wb' options!!
    FILE *ibfile=fopen(pipefilename,"wb");                          // �򿪹ܵ��ļ�
    if(ibfile==NULL)
        ThrowWith("Open pipe file '%' failed.",pipefilename);

    FILE *keep_file=(FILE *)tl->GetParams()[2];
    StruLoadFileArrayPtr pLoadDataAry = (StruLoadFileArrayPtr)tl->GetParams()[3];       // �ļ��б�����

    /* exception catched in caller*/
    try {
        bool    has_error = false;
        LONG64 deal_rows_number = 0;                            // ������ļ�¼����
        for(int i=0; i<pLoadDataAry->filenum; i++) {
            pibdatafile->SetFileName(pLoadDataAry->stLoadFileLst[i].filename);
            LONG64 recimp=pibdatafile->Pipe(ibfile,keep_file);
            if(recimp != -1l) {
                deal_rows_number += recimp;
            } else {
                has_error = true;
                break; // -1 : error
            }
        }
        tl->GetParams()[4]=(void *)deal_rows_number;        // �Ѿ�������ļ���¼����
        if(has_error) {
            tl->SetReturnValue(-1);
        } else {
            tl->SetReturnValue(TRET_OK);
        }
    } catch(...) {
        fclose(ibfile);
        throw;
    }
    fclose(ibfile);
    lgprintf("���ݴ������.");
}

//>> begin: ��ȡ��ռ��С,add by liujs
LONG64 GetSizeFromFile(const char* file)
{
    LONG64 tosize = 0;
    FILE *pf = fopen(file,"rt");
    if(pf==NULL) ThrowWith("�ļ����ļ� %s ʧ��.",file);
    char lines[300]= {0};
    while(fgets(lines,300,pf)!=NULL) {
        //du lineorder.bht -sb
        //106083154       lineorder.bht
        char *p = lines;
        while(*p !=' ' && *p !='\t' && *p!='\n') {
            p++;
        }
        *p = 0; // �ո񲿷ֽ׶�

        tosize += atoll(lines);
    }
    fclose(pf);
    pf=NULL;
    return tosize;
}
// ��ȡ$DATAMERGER_HOME/var/database/table.bht/Table.ctb �е����4�ֽ�
int GetTableId(const char* pbasepth,const char *dbn,const char *tabname)
{
    char strTablectl[500];
    sprintf(strTablectl,"%s/%s/%s.bht/Table.ctb",pbasepth,dbn,tabname);
    struct stat stat_info;
    if(0 != stat(strTablectl, &stat_info)) {
        ThrowWith("���ݿ��%s.%s ������!",dbn,tabname);
    }

    // �򿪱�id�ļ�
    FILE  *pFile  = fopen(strTablectl,"rb");
    if(!pFile) {
        ThrowWith("�ļ�%s��ʧ��!",strTablectl);
    }
    fseek(pFile,-4,SEEK_END);
    int _tabid = 0;
    fread(&_tabid,4,1,pFile);
    fclose(pFile);

    return _tabid;
}

LONG64 DestLoader::GetTableSize(const char* dbn,const char* tbn,const bool echo)
{
    const char* pRiakHost = getenv(RIAK_HOSTNAME_ENV_NAME);
    if(NULL != pRiakHost && pRiakHost[0] != '\0') {
           lgprintf("������װ����̽�������ʱ�������������ת�����ͳ��װ���ռ��С!");
          return 1;        
    }
    LONG64 totalbytes = 0 ;

    char basepth[300];
    strcpy(basepth,psa->GetMySQLPathName(0,"msys"));
    char size_file[300];
    sprintf(size_file,"%s/dpadmin_%s.%s_sz_%d.lst",basepth,dbn,tbn,getpid());
    unlink(size_file);

    // �ж��ļ����Ƿ����
    char table_name[256];
    sprintf(table_name,"%s/%s/%s.frm",basepth,dbn,tbn);
    struct stat stat_info;
    if(0 != stat(table_name, &stat_info)) {
        lgprintf("���ݿ��%s.%s������,�޷���ȡ��С\n",dbn,tbn);
        return 0;
    }

    try {
        char cmd[500];

        // 1>  ��ȡ��$DATAMERGER_HOME/var/db/tb.bht ��С, du $DATAMERGER_HOME/var/db/tb.bht -sb
        sprintf(cmd,"du %s/%s/%s.bht -sb >> %s",basepth,dbn,tbn,size_file);
        system(cmd);
        if(echo) {
            lgprintf("cmd : %s \n",cmd);
        }
        // 2>   ��ȡ��$DATAMERGER_HOME/var/db/tb.frm ��С, du $DATAMERGER_HOME/var/db/tb.frm -sb
        sprintf(cmd,"du %s/%s/%s.frm -sb >> %s",basepth,dbn,tbn,size_file);
        system(cmd);
        if(echo) {
            lgprintf("cmd : %s \n",cmd);
        }

        //  3>     ��ȡ$DATAMERGER_HOME/var/BH_RSI_Repository/????.tabid.*.rsi ��С, du $DATAMERGER_HOME/var/BH_RSI_Repository/????.tabid.*.rsi -sb

        // ���д������ݲ�ͳ��rsi��Ϣ
        AutoMt mdf(psa->GetDTS(),10);
        mdf.FetchFirst("select count(1) cnt from %s.%s",dbn,tbn);
        long cnt = 0;
        mdf.Wait();
        cnt = mdf.GetDouble("cnt",0);
        if(cnt > 0) { // ���д�������
            int tabid = GetTableId(basepth,dbn,tbn);
            sprintf(cmd,"du %s/BH_RSI_Repository/????.%d.*.rsi -sb >> %s",basepth,tabid,size_file);
            system(cmd);
            if(echo) {
                lgprintf("cmd : %s \n",cmd);
            }

            totalbytes += GetSizeFromFile(size_file);
            unlink(size_file);

            //>> begin: fix sasu-105
            //  4>   ����Ƿֲ�ʽ�汾����ȡ�洢�ڷֲ�ʽ�汾�е����ݰ���С
            //  export LOAD_SORTED_DATA_DIR='riak://IB_DATAPACKS@cloud01:8087,cloud02:8087,cloud02:8087'
            const char* pRiakHost = getenv(RIAK_HOSTNAME_ENV_NAME);
            if(NULL != pRiakHost && strlen(pRiakHost) > 0 ) {
                char spfn[256];
                sprintf(spfn,"%s/%s/%s.bht/PackSize_%d.ctl",basepth,dbn,tbn,tabid);

                if(echo) {
                    lgprintf("cmd : get pack size from file %s .\n",spfn);
                }

                // �ж��ļ��Ƿ����
                struct stat stat_info;
                if(0 != stat(spfn, &stat_info)) {
                    lgprintf("file:%s do not exist.��ȷ���Ƿ�û����ȷ���û�������:RIAK_HOSTNAME\n",spfn);
                    return 0;
                }

                int filesize = stat_info.st_size;
                // �ļ����ڣ���ȡ�ļ������ݰ��Ĵ�С
                unsigned long spsize_total = 0;
                FILE* pFile = NULL;
                pFile = fopen(spfn,"rb");
                assert(pFile);
                bool read_error = false;

                // read
                if(filesize == 8) { // ֮ǰװ������ݣ�ֻ��8�ֽڼ�¼��С
                    if(fread(&spsize_total,sizeof(spsize_total),1,pFile) != 1) {
                        lgprintf("read file : %s return error. \n",spfn);
                        read_error = true;
                    } else {
                        totalbytes += spsize_total;
                    }
                } else if(filesize>8) {
                    short part_num = 0;
                    if(fread(&part_num,sizeof(part_num),1,pFile) != 1) {
                        read_error = true;
                    }

                    int _part_index=0;
                    while(_part_index<part_num && read_error == false) {
                        // read partname len
                        short part_name_len = 0;
                        if(fread(&part_name_len,sizeof(part_name_len),1,pFile) != 1) {
                            read_error = true;
                        }

                        // read part_name
                        char part_name[300];
                        if(fread(part_name,part_name_len,1,pFile) != 1) {
                            read_error = true;
                        }

                        // read pack size
                        unsigned long packsize = 0;
                        if(fread(&packsize,8,1,pFile) != 1) {
                            read_error = true;
                        }
                        spsize_total+= packsize;

                        _part_index++;
                    }

                    totalbytes += spsize_total;
                } else {
                    assert(0);
                }
                fclose(pFile);
                pFile = NULL;
            }
        }
        //<< end: fix sasu-105
        unlink(size_file);

        return totalbytes;
    } catch(...) {
        lgprintf("Get table[%s.%s] size error,try again...\n");
        unlink(size_file);
        return -1;
    }

    return 0;
}

//>> end: ��ȡ��ռ��С,add by liujs

#ifdef WORDS_BIGENDIAN
#define revlint(v) v
#else
#define revlint(V)   { char def_temp[8];\
    ((mbyte*) &def_temp)[0]=((mbyte*)(V))[7];\
    ((mbyte*) &def_temp)[1]=((mbyte*)(V))[6];\
    ((mbyte*) &def_temp)[2]=((mbyte*)(V))[5];\
    ((mbyte*) &def_temp)[3]=((mbyte*)(V))[4];\
    ((mbyte*) &def_temp)[4]=((mbyte*)(V))[3];\
    ((mbyte*) &def_temp)[5]=((mbyte*)(V))[2];\
    ((mbyte*) &def_temp)[6]=((mbyte*)(V))[1];\
    ((mbyte*) &def_temp)[7]=((mbyte*)(V))[0];\
    memcpy(V,def_temp,sizeof(LONG64)); }
#endif


// �ڱ�������ߺ󣬽���dp.dp_filelog���еļ�¼����move�����ݱ�dp.dp_filelogbk�У�����Դ����ɾ��;
int DestLoader::MoveFilelogInfo(const int tabid,const int datapartid)
{
    // ����Ҫ���ļ������̲��ܽ��н�dp.dp_filelog--->dp.dp_filelogbk����Ǩ��
    char * p_need_not_add_file = getenv("DP_NEED_NOT_ADD_FILE");
    if(p_need_not_add_file == NULL) {
        lgprintf("DP_NEED_NOT_ADD_FILE ��������δ����,��Ҫ���ļ������̲��ܽ��н�dp.dp_filelog--->dp.dp_filelogbk����Ǩ��");
        return 0;
    } else if(atoi(p_need_not_add_file) != 1) {
        lgprintf("DP_NEED_NOT_ADD_FILE ��������δ����,��Ҫ���ļ������̲��ܽ��н�dp.dp_filelog--->dp.dp_filelogbk����Ǩ��");
        return 0;
    }

    try {
        // 1. �����Ѿ���ɵķ����ļ�¼
        {
            AutoStmt st(psa->GetDTS());
            st.Prepare(" insert into dp.dp_filelogbk select * from dp.dp_filelog where tabid = %d and datapartid = %d and tabid in (select tabid from dp.dp_datapart where tabid=%d and datapartid = %d and status = 5)",tabid,datapartid,tabid,datapartid);
            st.Execute(1);
            st.Wait();
            lgprintf("��(%d),����(%d)������ļ��Ѿ���dp.dp_filelog --> dp.dp_filelogbk ��: insert �ɹ�.",tabid,datapartid);
        }
        // 2. ɾ���Ѿ�insert�ļ�¼
        {
            AutoStmt st(psa->GetDTS());
            st.Prepare(" delete from dp.dp_filelog where tabid = %d and datapartid = %d and tabid in (select tabid from dp.dp_datapart where tabid=%d and datapartid = %d and status = 5)",tabid,datapartid,tabid,datapartid);
            st.Execute(1);
            st.Wait();
            lgprintf("��(%d),����(%d)������ļ��Ѿ���dp.dp_filelog --> dp.dp_filelogbk ��: delete �ɹ�.",tabid,datapartid);
        }
    } catch(...) {
        lgprintf("��(%d),����(%d)������ļ���dp.dp_filelog --> dp.dp_filelogbk ��ʧ��.",tabid,datapartid);
        return -1;
    }
    return 0;
}


// װ��һ���ļ��󣬽���¼����ռ�ÿռ��С��Ϣ�����µ�dp.dp_table����
int DestLoader::UpdateTableSizeRecordnum(int tabid)
{
    try {
        AutoMt mdf(psa->GetDTS(),50);
        mdf.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
        mdf.Wait();

        char tabname[300];
        char dbname[300];
        strcpy(tabname,mdf.PtrStr("tabname",0));
        strcpy(dbname,mdf.PtrStr("databasename",0));

        lgprintf("���±�[%s.%s,tabid=%d]��¼����ռ�ÿռ��С��dp.dp_table��.",dbname,tabname,tabid);

        mdf.FetchFirst("select count(1) srn from %s.%s",dbname,tabname); // ��ѯ���ݱ��еļ�¼
        mdf.Wait();
        double trn=mdf.GetDouble("srn",0);
        LONG64 totsize=0;

        totsize = GetTableSize(dbname,tabname,false);
        if(totsize == -1) {
            totsize = GetTableSize(dbname,tabname,false);
        }

        AutoStmt st(psa->GetDTS());
        st.DirectExecute(" update dp.dp_table set totalbytes=%ld,recordnum=%.0f where tabid=%d",totsize,trn,tabid);
    } catch(...) {
        lgprintf("���±�[tabid=%d]��¼����ռ�ÿռ��С��dp.dp_table��.",tabid);
        return -1;
    }
    return 1;
}

// �ύ��/�̻Ựid�������������
int DestLoader::commit_sorted_data(const int sessiontype)
{
    if(!dp_parallel_load) return 0;

    // �ύ�Ự����������,������������:
    // 1. ��dp.dp_datapartext����,��ȡ���������ĳ��Ự����/�̻Ự������Ϣ
    // 2. ��ȡ��ȡ�ỰID,��������,�ϲ�����Ŀ¼����Ϣ,д��loadinfo.ctl�ļ���
    // 3. ��ʼ׼�����ݺϲ�����(��¼���±�dp.dp_datapartext�е�commitstatus=1����Ϣ
    // 4.    �̻Ự ��mysqld�ύ: load data infile 'st_mergetable' into table db.tb ��
    //->4.  ���Ự ��mysqld�ύ: load data infile 'lt_mergetable' into table db.tb ��
    //5. �ύ��ɺ�,�����޸�����״̬(dp.dp_datapart.status)�ͻỰ����״̬(dp.dp_datapartext.commitstatus)
    //6. ���ºϲ���ı�Ŀռ��С

    int ret =0;
    AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
    char quesql[4096];
    if(sessiontype == SHORT_SESSION_TYPE) {
        // ��dp.dp_datapartext����,��ȡ����״̬Ϊcommitstatus=0 && sessiontype=1 ��������Ϣ
        sprintf(quesql,"select a.tabid as tabid ,a.datapartid as datapartid,rownums,sessionid,commitstatus,partfullname from "
                " dp.dp_datapartext a,dp.dp_datapart b where a.tabid = b.tabid and a.datapartid = b.datapartid and "
                " not exists (select 1 from dp.dp_datapartext c where a.tabid=c.tabid "
                " and c.commitstatus = '%d' ) "
                " and b.status in(1,3,72) "
                " and sessiontype=%d and commitstatus='%d' and rownums>=0 %s order by a.sessionid limit 2 ",
                PL_LOADING,
                sessiontype,
                PL_LOAD_WAIT,
                psa->GetNormalTaskDesc());

    } else if(sessiontype == LONG_SESSION_TYPE) {
        // ��dp.dp_datapartext����,��ȡ����״̬Ϊ(commitstatus=0 && sessiontype=2)
        // ��(sessiontype=1 && commitstatus not in(-1,0,1)) ��������Ϣ
        /*
        select a.tabid as tabid ,a.datapartid as datapartid,rownums,sessionid,commitstatus,partfullname
        from  dp.dp_datapartext a, dp.dp_datapart b
        where a.tabid = b.tabid and a.datapartid = b.datapartid
         and a.sessiontype=2 and a.commitstatus=0 and rownums>0
         and
         not exists (select 1 from dp.dp_datapartext c where a.tabid = c.tabid and a.datapartid = c.datapartid and
                     ((c.commitstatus in(-1,0,1) and c.sessiontype =1) or (c.commitstatus in(-1,1) and c.sessiontype =2)))
        */
        sprintf(quesql,"select a.tabid as tabid ,a.datapartid as datapartid,rownums,sessionid,commitstatus,partfullname "
                " from dp.dp_datapartext a,dp.dp_datapart b  "
                " where a.tabid = b.tabid and a.datapartid = b.datapartid "
                " and  not exists (select 1 from dp.dp_datapartext c where  "
                " a.tabid=c.tabid and c.commitstatus = '%d' )"
                " and b.status in(1,3,72) "
                " and sessiontype=%d and commitstatus='%d' and rownums>=0 %s order by a.sessionid limit 2 ",
                PL_LOADING,sessiontype,
                PL_LOAD_WAIT,
                psa->GetNormalTaskDesc());
    } else {
        lgprintf("DestLoader::commit_sorted_data �ύ�Ự���ʹ���,�ύ�Ự����[%d].",sessiontype);
        assert(0);
        return 0;
    }

    mdf.FetchAll(quesql);

    int rn=mdf.Wait();

    if(rn>0) {
        // var Ŀ¼��ȡ
        char basepth[300];
        strcpy(basepth,psa->GetMySQLPathName(0,"msys"));

        // 1. ��dp.dp_datapart����,��ȡ����״̬Ϊ3��������Ϣ(�ỰID��Ϊnull)
        int tabid,datapartid;
        char sessionid[32],partfullname[100];
        tabid = mdf.GetInt("tabid",0);
        datapartid = mdf.GetInt("datapartid",0);
        strcpy(sessionid,mdf.PtrStr("sessionid",0));
        strcpy(partfullname,mdf.PtrStr("partfullname",0));
        long rownums = mdf.GetLong("rownums",0);

        // fix dma-1781:װ����־Ŀ¼���
        psa->SetTrace("dataload",tabid);

        // 2. ��ȡ��ȡ�ỰID,��������,�ϲ�����Ŀ¼����Ϣ,д��loadinfo.ctl�ļ���
        mdf.FetchAll("select databasename,tabname from dp.dp_table where tabid=%d",tabid);
        mdf.Wait();
        char tabpath[300],dbname[200],tabname[200];
        strcpy(dbname,mdf.PtrStr("databasename",0));
        strcpy(tabname,mdf.PtrStr("tabname",0));
        sprintf(tabpath,"%s/%s.bht",dbname,tabname);

        char *pload_data_dir = getenv("DMA_LOAD_SORTED_DATA_DIR");
        if(pload_data_dir == NULL) {
            ThrowWith("��������DMA_LOAD_SORTED_DATA_DIRû������,�޷��������ݺϲ�.");
        }

        // �Ựid��ȡ
        mdf.FetchFirst("select connection_id() cid");
        mdf.Wait();
        int connectid=(int)mdf.GetLong("cid",0);

        // �����ļ�����
        lgprintf("��ʼ�ϲ�װ������,tabid(%d),datapartid(%d),����(%s),�Ự(S%s),���Ӻ�:%d,��¼��:%ld.",
                 tabid,datapartid,partfullname,sessionid,connectid,rownums);

        // ��ʼ׼�����ݺϲ�����(��¼���±�dp.dp_datapartext�е�commitstatus����Ϣ,���������ס
        AutoStmt st(psa->GetDTS());
        ret = st.DirectExecute("update dp.dp_datapartext set commitstatus=%d where tabid=%d and datapartid=%d and  sessionid = '%s'",
                               PL_LOADING,tabid,datapartid,sessionid);

        // �ٴ��ж�*.loading�ļ��Ƿ����,���ھ��˳�
        if(ret == 1) {
            char loadingfn[256];
            sprintf(loadingfn,"%s/%s/%s.loading",basepth,dbname,tabname);
            struct stat stat_info;
            if(0 == stat(loadingfn, &stat_info)) { // exist

                //st.DirectExecute("update dp.dp_datapartext set commitstatus=%d where tabid=%d and datapartid=%d and  sessionid = '%s'",PL_LOAD_WAIT,tabid,datapartid,sessionid);
                lgprintf("���ݺϲ�����,tabid(%d),datapartid(%d),����(%s),�Ự(S%s) ����[%s]�ļ�,�˳����ݺϲ�����.",
                         tabid,datapartid,partfullname,sessionid,loadingfn);

                return 0;
            }

            // ����ʱ��
            st.DirectExecute("update dp.dp_datapartext set commitbegintime=now() where tabid=%d and datapartid=%d and  sessionid = '%s'",
                             tabid,datapartid,sessionid);


        } else {
            lgprintf("���ݺϲ�����,tabid(%d),datapartid(%d),����(%s),�Ự(S%s) �ϲ����ݸ��������ͻ,�˳�.",
                     tabid,datapartid,partfullname,sessionid);
            return 0;
        }


        if(rownums > 0 ) {

            // ÿһ���ļ�����Ϊ���һ���ļ�װ�����ݣ�Ĭ��ֵΪ1��0:A+B=A����
            bool islastload = false;
            char *ploadaslastfile = getenv("DP_LOAD_AS_LAST_FILE");
            if(ploadaslastfile == NULL || 1 == atoi(ploadaslastfile)) {
                islastload = true;
            }
            lgprintf("����װ��ϲ��ļ�:%s/%s/loadinfo.ctl",basepth,tabpath);
            psa->UpdateLoadPartition(connectid,tabpath,islastload,partfullname,sessionid,pload_data_dir,rownums);

            // 4. ��mysqld�ύ: load data infile 'mergetable' into table db.tb ��
            try {
                char loadsql[1024];
                if(sessiontype == SHORT_SESSION_TYPE) {
                    sprintf(loadsql,"load data infile 'st_mergetable' into table %s.%s",dbname,tabname);
                } else {
                    sprintf(loadsql,"load data infile 'lt_mergetable' into table %s.%s",dbname,tabname);
                }
                st.ExecuteNC("set @bh_dataformat='binary'");
                st.ExecuteNC("set autocommit=0");
                lgprintf("���ݺϲ�����[%s]...",loadsql);
                st.ExecuteNC(loadsql);
                lgprintf("���ݺϲ��ύ[commit --> %s]...",loadsql);
                st.ExecuteNC("commit");
                lgprintf("�����ύ�ɹ�.");
            } catch(...) {
                st.DirectExecute("update dp.dp_datapartext set commitstatus=%d "
                                 " where tabid=%d and datapartid=%d and commitstatus=%d and sessionid = '%s'",
                                 PL_LOAD_WAIT,tabid,datapartid,PL_LOADING,sessionid);
                lgprintf("���ݺϲ��ύʧ��,�ѻָ��ع������ύ�ϲ�����״̬.");
                return 0;
            }

            //5. �ύ��ɺ�,�����޸�����״̬(dp.dp_datapart.status)�ͻỰ����״̬(dp.dp_datapartext.commitstatus)
            st.DirectExecute("update dp.dp_datapartext set commitstatus=%d,commitendtime=now()"
                             " where tabid=%d and datapartid=%d and commitstatus=%d and sessionid = '%s'",
                             PL_LOADED,tabid,datapartid,PL_LOADING,sessionid);

            //6. ���±��С
            UpdateTableSizeRecordnum(tabid);

            //7. ɾ���Ѿ��ϲ���ɵ���ʱ�ļ�,��ʱע��
            const char* p_DP_PARALLEL_KEEP_LOAD_FILE = getenv("DP_PARALLEL_KEEP_LOAD_FILE");
            if(p_DP_PARALLEL_KEEP_LOAD_FILE == NULL || atoi(p_DP_PARALLEL_KEEP_LOAD_FILE) == 0) {
                clear_has_merged_data(sessionid,pload_data_dir);
            }

            lgprintf("���ݺϲ�װ��tabid(%d),datapartid(%d),����(%s),�Ự(S%s),(%ld)������.",
                     tabid,datapartid,partfullname,sessionid,rownums);

        } else {

            // 3. ��ʼ׼�����ݺϲ�����(��¼���±�dp.dp_datapartext�е�commitstatus����Ϣ,���������ס
            AutoStmt st(psa->GetDTS());
            st.DirectExecute("update dp.dp_datapartext set commitstatus=%d,commitbegintime=now(),commitendtime=now() where tabid=%d and datapartid=%d and  sessionid = '%s'",
                             PL_LOADED,tabid,datapartid,sessionid);

            lgprintf("���ݺϲ�װ��tabid(%d),datapartid(%d),����(%s),�Ự(S%s),(%ld)������.",
                     tabid,datapartid,partfullname,sessionid,rownums);

            //7. ɾ���Ѿ��ϲ���ɵ���ʱ�ļ�,��ʱע��
            const char* p_DP_PARALLEL_KEEP_LOAD_FILE = getenv("DP_PARALLEL_KEEP_LOAD_FILE");
            if(p_DP_PARALLEL_KEEP_LOAD_FILE == NULL || atoi(p_DP_PARALLEL_KEEP_LOAD_FILE) == 0) {
                clear_has_merged_data(sessionid,pload_data_dir);
            }

        }

        // ��������״̬
        update_datapart_finish(tabid,datapartid);

        return 1;
    } else {
        if(sessiontype == SHORT_SESSION_TYPE) {
            lgprintf("û���ҵ���Ҫ�ύ�ϲ��Ķ̻Ự���ݵļ�¼.");
        } else if(sessiontype == LONG_SESSION_TYPE) {
            lgprintf("û���ҵ���Ҫ�ύ�ϲ��ĳ��Ự���ݵļ�¼.");
        }
        return 0;
    }
}

// ���ݺϲ���ɺ�,��������״̬Ϊ�Ѿ����
int DestLoader::update_datapart_finish(const int tabid,const int datapartid)
{
    int ret = 0;
    AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
    mdf.FetchFirst("select tabid,datapartid,sessionid from dp.dp_datapartext where tabid=%d and datapartid = %d "
                   " and commitstatus in(%d,%d,%d) ",
                   tabid,datapartid,PL_LOAD_DEFAULT,PL_LOAD_WAIT,PL_LOADING);
    ret = mdf.Wait();
    if(ret > 0) {
        lgprintf("tabid(%d),datapartid(%d) ����(%d)���Ựû�кϲ��������,�����ȴ��ϲ�����.",tabid,datapartid,ret);
        return ret;
    }

    AutoStmt st(psa->GetDTS());
    lgprintf("����tabid(%d),datapartid(%d)����Ϊ�����(status = 5).",datapartid,datapartid);
    st.DirectExecute("update dp.dp_datapart set status=5 "
                     " where tabid=%d and datapartid=%d and status = %d",
                     tabid,datapartid,LOADING);
    return 0;
}


int DestLoader::commit_load_data()
{
    int ret = 0;
    if(!dp_parallel_load) return ret;

    // �ύ�̻Ự����
    do {
        ret = commit_sorted_data(SHORT_SESSION_TYPE);
    } while(ret >0);

    // �ύ���Ự����
    do {
        ret = commit_sorted_data(LONG_SESSION_TYPE);
    } while(ret > 0 );

    return 0;
}

// ����Ѿ��ϲ�������ļ�
int DestLoader::clear_has_merged_data(const char* sessionid,const char* mergepath)
{
    try {
        int ret = 0;
        char cmd[1024];

        // ɾ��dpn
        sprintf(cmd,"rm %s/S%s-*.dpn -rf",mergepath,sessionid);
        lgprintf("ɾ�����ݹ���:%s\n",cmd);
        ret = system(cmd);
        if(ret == -1) {
            ThrowWith("ɾ�����ݹ���:[%s]����.\n",cmd);
        }
        sleep(1);

        // ɾ��rsi
        sprintf(cmd,"rm %s/S%s-*.rsi -rf",mergepath,sessionid);
        lgprintf("ɾ�����ݹ���:%s\n",cmd);
        ret = system(cmd);
        if(ret == -1) {
            ThrowWith("ɾ�����ݹ���:[%s]����.\n",cmd);
        }
        sleep(1);
        // ɾ��pack
        sprintf(cmd,"rm %s/S%s-*.pck -rf",mergepath,sessionid);
        lgprintf("ɾ�����ݹ���:%s\n",cmd);
        ret = system(cmd);
        if(ret == -1) {
            ThrowWith("ɾ�����ݹ���:[%s]����.\n",cmd);
        }
        sleep(1);
        // ɾ��leveldb�ļ�,���������ж��е�
        sprintf(cmd,"rm %s/S%s-*.pki -rf",mergepath,sessionid);
        lgprintf("ɾ�����ݹ���:%s\n",cmd);
        ret = system(cmd);
        if(ret == -1) {
            ThrowWith("ɾ���ϲ�������ݹ���:[%s]����.\n",cmd);
        }
        sleep(1);

        // ɾ��leveldb�ļ�,���������ж��е�
        sprintf(cmd,"rm %s/S%s-*.pkiv -rf",mergepath,sessionid);
        lgprintf("ɾ�����ݹ���:%s\n",cmd);
        ret = system(cmd);
        if(ret == -1) {
            ThrowWith("ɾ���ϲ�������ݹ���:[%s]����.\n",cmd);
        }
        sleep(1);

        // riak �汾ɾ��keyspace
        bool b_riak_version = CheckRiakVersion();
        if(b_riak_version) {
            sprintf(cmd,"rm %s/S%s-*.ksi -rf",mergepath,sessionid);
            lgprintf("ɾ�����ݹ���:%s\n",cmd);
            ret = system(cmd);
            if(ret == -1) {
                ThrowWith("ɾ���ϲ�������ݹ���:[%s]����.\n",cmd);
            }
            sleep(1);
        }

        lgprintf("ɾ���������.\n");

        return ret;
    } catch(...) {
        return 1;
    }
}

// ������������ʱ����
int DestLoader::clear_table_tmp_data(const int tabid,const int datapartid)
{
    AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);

    int retrows = 0;
    char sessionid[256];
    int session_nums = 0;

    char *pload_data_dir = getenv("DMA_LOAD_SORTED_DATA_DIR");
    if(pload_data_dir == NULL) {
        ThrowWith("��������DMA_LOAD_SORTED_DATA_DIRû������,�޷����������������.");
    }

    AutoStmt st(psa->GetDTS());
    do {
        if(datapartid>0) {
            mdf.FetchFirst("select sessionid from dp.dp_datapartext where tabid = %d and datapartid = %d limit 100",
                           tabid,datapartid);
        } else { // ����ɾ��
            mdf.FetchFirst("select sessionid from dp.dp_datapartext where tabid = %d limit 100",
                           tabid);
        }

        retrows = mdf.Wait();
        for(int i=0; i<retrows; i++) {
            strcpy(sessionid,mdf.PtrStr("sessionid",i));
            lgprintf("׼�����:tabid(%d),datapartid(%d),sessionid('%s')������.",tabid,datapartid,sessionid);
            clear_has_merged_data(sessionid,pload_data_dir);
            lgprintf("������:tabid(%d),datapartid(%d),sessionid('%s')������.",tabid,datapartid,sessionid);
        }
        session_nums += retrows;

        // �����¼
        if(datapartid>0) {
            st.Prepare("delete from dp.dp_datapartext where tabid=%d and datapartid=%d  limit 100",
                       tabid,datapartid);
        } else {
            st.Prepare("delete from dp.dp_datapartext where tabid=%d limit 100",
                       tabid);
        }
        st.Execute(1);
        st.Wait();

    } while(retrows>0);

    lgprintf("�Ѿ�������:tabid(%d),datapartid(%d)�Ự��(%d)������.",tabid,datapartid,session_nums);

    return session_nums;
}



// ����÷����ϵ��Ѿ�����ĻỰ����
int DestLoader::clear_has_merged_data(const int tabid,const int datapartid)
{
    AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);

    int retrows = 0;
    char sessionid[256];
    int session_nums = 0;

    char *pload_data_dir = getenv("DMA_LOAD_SORTED_DATA_DIR");
    if(pload_data_dir == NULL) {
        ThrowWith("��������DMA_LOAD_SORTED_DATA_DIRû������,�޷����������������.");
    }

    AutoStmt st(psa->GetDTS());
    do {
        if(datapartid>0) {
            mdf.FetchFirst("select sessionid from dp.dp_datapartext where tabid = %d and datapartid = %d and commitstatus = 2 limit 100",
                           tabid,datapartid);
        } else { // ����ɾ��
            mdf.FetchFirst("select sessionid from dp.dp_datapartext where tabid = %d and commitstatus = 2 limit 100",
                           tabid);
        }

        retrows = mdf.Wait();
        for(int i=0; i<retrows; i++) {
            strcpy(sessionid,mdf.PtrStr("sessionid",i));
            lgprintf("׼�����:tabid(%d),datapartid(%d),sessionid('%s')�ĺϲ��������.",tabid,datapartid,sessionid);
            clear_has_merged_data(sessionid,pload_data_dir);
            lgprintf("������:tabid(%d),datapartid(%d),sessionid('%s')�ĺϲ��������.",tabid,datapartid,sessionid);
        }
        session_nums += retrows;

        // �����¼
        if(datapartid>0) {
            st.Prepare("delete from dp.dp_datapartext where tabid=%d and datapartid=%d and commitstatus = 2 limit 100",
                       tabid,datapartid);
        } else {
            st.Prepare("delete from dp.dp_datapartext where tabid=%d and commitstatus = 2 limit 100",
                       tabid);
        }
        st.Execute(1);
        st.Wait();

    } while(retrows>0);

    lgprintf("�Ѿ�������:tabid(%d),datapartid(%d)�Ự��(%d)������.",tabid,datapartid,session_nums);

    return session_nums;
}


int DestLoader::Load(bool directIOSkip)
{
    if(dp_parallel_load) { //��������װ��
        return commit_load_data();
    } else {
        const char* pRiakHost = getenv(RIAK_HOSTNAME_ENV_NAME_CLI);
        if(NULL != pRiakHost && pRiakHost[0] != '\0') {
            ThrowWith("DMC�ֲ�ʽ����汾(export RIAK_HOSTNAME=%s):��֧������bhloaderװ�����,����װ������ɷֲ�ʽ����Ⱥ���.",pRiakHost);
        }
    }

    // ������bhloader����װ������
    {
        AutoStmt st(psa->GetDTS());
        st.DirectExecute(" update dp.dp_datafilemap set procstatus=0 where procstatus=10");
    }
    // fix error : set reload status on web admin tool cause delete file and force set status=5
    AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
    //Check deserved temporary(middle) fileset
    //���ȴ����Ѿ�װ����ɣ��������������
    //mdf.FetchAll("select * from dp.dp_datapart where status=3 and begintime<now() %s order by blevel,tabid,datapartid limit 2",psa->GetNormalTaskDesc());
    mdf.FetchAll("select * from dp_datapart a where status=3 and not exists (select 1 from dp_datafilemap b where "
                 "a.tabid=b.tabid and a.datapartid=b.datapartid and b.procstatus<2) and begintime<now() %s order by blevel,tabid,datapartid limit 2",
                 psa->GetNormalTaskDesc());
    int rn=mdf.Wait();
    if(rn>0) {
        //printf("û�з��ִ�����ɵȴ�װ�������(����״̬=3).\n");
        //return 0;

        char sqlbf[MAX_STMT_LEN];
        tabid=mdf.GetInt("tabid",0);
        psa->SetTrace("dataload",tabid);
        datapartid=mdf.GetInt("datapartid",0);
        bool preponl=mdf.GetInt("status",0)==3;
        //�÷������еĶ�����������ȫ��װ�����?
        mdf.FetchAll("select * from dp.dp_datafilemap where procstatus in(0,1) and tabid=%d and datapartid=%d limit 2",tabid,datapartid);
        rn=mdf.Wait();
        if(rn<1) {
            if(preponl) {
                lgprintf("��(%d)����(%d)������װ����ɡ�",tabid,datapartid);

                // ����dp.dp_table��Ŀռ��С�ͼ�¼��
                UpdateTableSizeRecordnum(tabid);

                // TODO : store raw data size and compress ratio to dp_table.
                AutoStmt st(psa->GetDTS());
                st.DirectExecute(" update dp.dp_datapart set status=5 where tabid=%d and datapartid=%d",tabid,datapartid);
                mdf.FetchAll("select filename from dp.dp_datafilemap where procstatus =2 and tabid=%d and datapartid=%d ",tabid,datapartid);
                rn=mdf.Wait();
#ifndef KEEP_LOAD_FILE
                for(int i=0; i<rn; i++) {
                    lgprintf("ɾ����װ����ļ�'%s'.",mdf.PtrStr("filename",i));
                    unlink(mdf.PtrStr("filename",i));
                }
#endif
                st.DirectExecute(" update dp.dp_datafilemap set procstatus=3 where tabid=%d and datapartid=%d",tabid,datapartid);
                lgprintf("��(%d)����(%d)������״̬3-5",tabid,datapartid);
                psa->log(tabid,datapartid,DLOAD_UPDATE_TASK_STATUS_NOTIFY,"��(%d)����(%d)������״̬3-5",tabid,datapartid);
                psa->logext(tabid,datapartid,EXT_STATUS_END,"");

                // ������ļ��ɼ�,������ɺ�dp.dp_filelog--> dp.dp_filelogbk��
                // ����Ҫ���ļ������̲��ܽ��н�dp.dp_filelog--->dp.dp_filelogbk����Ǩ��
                char * p_need_not_add_file = getenv("DP_NEED_NOT_ADD_FILE");
                if(p_need_not_add_file && atoi(p_need_not_add_file) == 1) {
                    mdf.FetchAll("select tabid from dp.dp_datapart where tabid = %d and datapartid = %d and status = 5 and extsql like '%%'",tabid,datapartid);
                    int _rn = mdf.Wait();
                    if(_rn >0 ) {
                        MoveFilelogInfo(tabid,datapartid);
                    }
                } else {
                    lgprintf("DP_NEED_NOT_ADD_FILE ��������δ����,��Ҫ���ļ������̲��ܽ��н�dp.dp_filelog--->dp.dp_filelogbk����Ǩ��");
                }
            }
            return 1;
        }
    }
    //���ҵȴ�������ļ�
    // ������
    //  1. ����״̬������������ߵȴ�����
    //  2. �ļ�״̬�ǵȴ�����
    //  3. һ������ͬһ����������û�����������ڵ���
    //2013/4/19 ����:
    //  4. ����״̬ʱ���ڵ���
    mdf.FetchAll("select a.*,b.partfullname from dp.dp_datafilemap a,dp.dp_datapart b where b.status in(1,2,3) and b.begintime<now() %s and a.procstatus=0 and a.tabid = b.tabid"
                 " and a.datapartid=b.datapartid and not exists (select 1 from dp.dp_datafilemap c where c.tabid=a.tabid and c.indexgid=a.indexgid and c.procstatus=1)"
                 " order by a.tabid, a.datapartid,a.fileid,a.indexgid limit %d",
                 psa->GetNormalTask()?" and ifnull(b.blevel,0)<100 ":" and ifnull(b.blevel,0)>=100 ",conMaxLoadFileNum);
    rn=mdf.Wait();
    if(rn<1) return 0;

    //>>begin fix : dma-739
    StruLoadFileArray loadfilearray;
    int loadfsize = 0;
    char *ploadfsize = getenv("DP_LOAD_DATA_FILE_SIZE");
    loadfsize = (NULL != ploadfsize) ? atoi(ploadfsize) : 2048;

    tabid=mdf.GetInt("tabid",0);
    datapartid=mdf.GetInt("datapartid",0);
    indexid=mdf.GetInt("indexgid",0);
    char partname[300];
    strcpy(partname,mdf.PtrStr("partfullname",0));

    loadfilearray.tabid = tabid;
    loadfilearray.datapartid = datapartid;
    loadfilearray.indexid = indexid;
    loadfilearray.filenum =0;
    int new_indexid = 0;
    unsigned long filesizesum = 0;
    for(int i=0; i<min(rn,(conMaxLoadFileNum-1)); i++) {
        if(mdf.GetInt("tabid",i) != tabid) {
            break;
        }
        if(mdf.GetInt("datapartid",i) != datapartid) {
            break;
        }
        if(mdf.GetInt("indexgid",i) != indexid) {
            break; // ��ͬ������������װ��
        }

        // ��������ļ�id������
        loadfilearray.stLoadFileLst[loadfilearray.filenum].fileid = mdf.GetInt("fileid",i);
        strcpy(loadfilearray.stLoadFileLst[loadfilearray.filenum].filename,mdf.PtrStr("filename",i));
        loadfilearray.stLoadFileLst[loadfilearray.filenum].recordnum = mdf.GetInt("recordnum",i);
        loadfilearray.filenum ++ ;

        filesizesum += mdf.GetInt("filesize",i);
        if((filesizesum/1024/1024) > loadfsize) {
            break; // �ļ���С���������˳�
        }
    }
    //<<end fix : dma-739

    //Jira DMA-145: ������̵�tabid�������select��䱻�����޸�,��Ҫ�ٴε�����־���ļ�����:
    psa->SetTrace("dataload",tabid);

    int pid = getpid(); // pid

    char fileidlist[1000];
    fileidlist[0] = 0;
    /* try to lock file */
    {
        AutoStmt st(psa->GetDTS());

        // in(?,?,?,)
        for(int i=0; i<loadfilearray.filenum; i++) {
            sprintf(fileidlist+strlen(fileidlist),"%d",loadfilearray.stLoadFileLst[i].fileid);
            if(i != loadfilearray.filenum -1) {
                strcat(fileidlist,",");
            }
        }

        if(st.DirectExecute("update dp.dp_datafilemap set procstatus=1,pid=%d,begintime=now() where procstatus=0 and tabid=%d and datapartid=%d and indexgid=%d and fileid in(%s)",
                            pid,tabid,datapartid,indexid,fileidlist)!=loadfilearray.filenum) {
            psa->log(tabid,datapartid,DLOAD_DATA_NOTIFY,"�ļ�id'%s'װ��ʱ��������״̬����������������װ�롣���û����������װ�룬��ָ��ļ�״̬Ϊ0.",fileidlist);
            lgprintf("�ļ�'%s'װ��ʱ��������״̬����������������װ�롣���û����������װ�룬��ָ��ļ�״̬Ϊ0.",fileidlist);
            return 0;
        }
    }

    //��ʼװ�����
    mdf.FetchAll("select indexgid ct from dp.dp_index where tabid=%d and issoledindex>0",tabid);
    bool singleidx=mdf.Wait()==1;
    mdf.FetchAll("select databasename,tabname from dp.dp_table where tabid=%d",tabid);
    mdf.Wait();
    char tabpath[300],dbname[200],tabname[200];
    strcpy(dbname,mdf.PtrStr("databasename",0));
    strcpy(tabname,mdf.PtrStr("tabname",0));

    if(singleidx) {
        sprintf(tabpath,"%s/%s.bht",dbname,tabname);
    } else {
        sprintf(tabpath,"%s/%s_dma%d.bht",dbname,tabname,indexid);
    }

    // fix dma-739, the following do not used
    mdf.FetchAll("select 1 from dp.dp_datapart where tabid=%d and datapartid=%d and status=3 and "
                 "not exists (select 1 from dp.dp_datafilemap where tabid=%d and datapartid=%d and procstatus=0)",tabid,datapartid,tabid,datapartid);
    bool islastload=mdf.Wait()>0;

    // ÿһ���ļ�����Ϊ���һ���ļ�װ�����ݣ�Ĭ��ֵΪ1��0:A+B=A����
    char *ploadaslastfile = getenv("DP_LOAD_AS_LAST_FILE");
    if(ploadaslastfile == NULL || 1 == atoi(ploadaslastfile)) {
        islastload = true;
    }

    mdf.FetchAll("select connection_id() cid");
    rn=mdf.Wait();
    int connectid=(int)mdf.GetLong("cid",0);
    lgprintf("װ������,����:%s,���Ӻ�:%d.",partname,connectid);

    psa->UpdateLoadPartition(connectid,tabpath,islastload,partname);

    IBDataFile ibdatafile;

    char cmd[2000];
    char ibfilename[300];

    // װ��ܵ��ļ�·��
    // �������õ��ļ���nfs���ص�����£���Ҫ���ñ���Ŀ¼��Ϊ�ܵ��ļ���װ���
    const char* dpload_dir = getenv("DP_LOAD_DIR");
    if(dpload_dir != NULL && strlen(dpload_dir) >0) { // ָ����·������װ������
        sprintf(ibfilename,"%s/%s.%s.%s.%d_pipe.bin",dpload_dir,dbname,tabname,partname,loadfilearray.stLoadFileLst[0].fileid);
    } else { // ʹ��Ĭ�ϵ�·����������װ��
        sprintf(ibfilename,"%s_pipe.bin",loadfilearray.stLoadFileLst[0].filename); // ��һ���ļ�����ʹ��
    }

    sprintf(cmd,"rm -f %s",ibfilename);
    lgprintf("��ʼ���ݵ���,��%d/����%d/����%d,�ļ��б�{%s} ��",tabid,datapartid,indexid,fileidlist);
    try {
        //��������������������_<indexid>,ֻ��һ���������������������
        mdf.FetchAll("select indexgid ct from dp.dp_index where tabid=%d and issoledindex>0",tabid);
        int simpname=mdf.Wait()==1;
        mdf.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
        rn=mdf.Wait();
        char tbname1[300];
        if(simpname)
            strcpy(tbname1,mdf.PtrStr("tabname",0));
        else {
            sprintf(tbname1,"%s_dma%d",mdf.PtrStr("tabname",0),indexid);
        }
        system(cmd);
        sprintf(cmd,"mkfifo %s",ibfilename);
        FILE *keep_file=NULL;
#ifdef KEEP_LOAD_FILE
        //char bkfilename[300];
        //sprintf(bkfilename,"/tmp/bhloader_restore_%d.dat",getpid());
        //keep_file=fopen(bkfilename,"w+b");
#endif
        if(system(cmd)==-1) {
            psa->log(tabid,datapartid,DLOAD_CREATE_PIPE_FILE_ERROR,"��%d,����%d,�����ܵ��ļ�%sʧ��.",tabid,datapartid,ibfilename);
            ThrowWith("�ļ�����ʧ�ܣ�%s",ibfilename);
        }

        /*
        sprintf(cmd,
        "mysql -S /tmp/mysql-dma.sock -u%s -p%s <<EOF &\n"
        "set @bh_dataformat='binary';\n"
        "set autocommit=0;\n"
        "load data infile '%s' into table %s.%s;\n"
        "commit ;\n"
        "EOF",getenv("DP_CUSERNAME"),getenv("DP_CPASSWORD"),ibfilename,mdf.PtrStr("databasename",0),tbname1);
        int cmdrt=0;
        cmdrt=system(cmd);
        if(cmdrt==-1) ThrowWith���ʧ�ܡ?);
        lgprintf("�ȴ����̾���");
        sleep(3);
        */
        for(int i=0; i<loadfilearray.filenum; i++) {
            FILE *fptest=fopen(loadfilearray.stLoadFileLst[i].filename,"rb");
            if(fptest==NULL) {
                psa->log(tabid,datapartid,DLOAD_OPEN_DATA_SOURCE_FILE_ERROR,"��%d,����%d,�������ļ�%sʧ��.",
                         tabid,datapartid,loadfilearray.stLoadFileLst[i].filename);

                if(i>0) { // ���´����ļ��ģ��ļ�״̬
                    AutoStmt st(psa->GetDTS());

                    char _fileidlist[300];
                    // in(?,?,?)
                    for(int j=0; j<i; j++) {
                        sprintf(_fileidlist+strlen(_fileidlist),"%d",loadfilearray.stLoadFileLst[j].fileid);
                        if(i != loadfilearray.filenum -1) {
                            strcat(_fileidlist,",");
                        }
                    }
                    st.DirectExecute("update dp.dp_datafilemap set procstatus=0,pid=0,begintime=now() where procstatus=1 and tabid=%d and datapartid=%d and indexgid=%d and fileid in(%s)",
                                     pid,tabid,datapartid,indexid,_fileidlist);
                }

                ThrowWith("�����ļ����ܴ�:'%s'",loadfilearray.stLoadFileLst[i].filename);
            }
            fclose(fptest);
        }

        //����������߳�
        ThreadList tload;
        //  TODO: ȱ��ʧ�ܻ��˻���,����쳣ʱ�Ƿ�����ѵ��������
        void *params[2];
        // connect handle
        params[0]=( void *)psa->GetDTS();
        char loadsql[3000];
        sprintf(loadsql,"load data infile '%s' into table %s.%s",ibfilename,mdf.PtrStr("databasename",0),tbname1);
        params[1]=( void *) loadsql;
        lgprintf("��ʼת��tabid=%d,datapartid=%d,�ļ��б�{%s}->%s",tabid,datapartid,fileidlist,ibfilename);

        void *pipeparams[5];
        pipeparams[0]=(void *)&ibdatafile;  // datafile
        pipeparams[1]=(void *)ibfilename;   // ibfile;
        pipeparams[2]=(void *)keep_file;
        pipeparams[3]=(void *)&loadfilearray;
        pipeparams[4]=(void *)NULL;

        //�����ܵ�����д���߳�
        ThreadList *tpipe=tload.AddThread(new ThreadList());
        tload.SetWaitOne();
        tload.Start(params,2,ServerLoadData);
        tpipe->Start(pipeparams,5,PipeData);
        try {
            while(ThreadList *pthread=tload.WaitOne()) {
                if(pthread==tpipe) {
                    LONG64 recimp=(long )tpipe->GetParams()[4];
                    int retFlag = (int ) tpipe->GetReturnValue();
                    if(recimp!=-1l && retFlag != -1) {
                        for(int i=0; i<loadfilearray.filenum; i++) {
                            lgprintf("�ѵ����ļ�%s,��¼��%ld ��.",loadfilearray.stLoadFileLst[i].filename,
                                     loadfilearray.stLoadFileLst[i].recordnum);
                        }
                        if(loadfilearray.filenum>1) {
                            lgprintf("����%d���ļ�������%ld �м�¼.",loadfilearray.filenum,recimp);
                        }
                    }
                } else {
                    lgprintf("���ݿ⵼�����.");
                }
            }
        } catch(...) {
            if(tload.GetExceptedThread()==tpipe) {
                lgprintf("�ļ�д���쳣,��ֹ���ݿ⵼��...");
                tload.Terminate();
            } else {
                lgprintf("���ݿ⵼���쳣,��ֹ�ļ�д...");
                tpipe->Terminate();
            }
#ifdef KEEP_LOAD_FILE
            //fclose(keep_file);
#endif
            throw;
        }

#ifndef KEEP_LOAD_FILE
        unlink(ibfilename);
#endif

        // ����dp.dp_table��Ŀռ��С�ͼ�¼��
        UpdateTableSizeRecordnum(tabid);

        AutoStmt st(psa->GetDTS());
        if(st.DirectExecute(" update dp.dp_datafilemap set procstatus=2,endtime=now() where procstatus=1 and tabid=%d and datapartid=%d and indexgid=%d and fileid in(%s)",
                            tabid,datapartid,indexid,fileidlist)!=loadfilearray.filenum) {
            psa->log(tabid,datapartid,DLOAD_UPDATE_FILE_STATUS_ERROR,"��%d,����%d,�ļ��б�{%s}״̬���´���,��Ҫ���µ���÷���!",tabid,datapartid,fileidlist);
            ThrowWith("��%d,����%d,�ļ��б�{%s}״̬���´���",tabid,datapartid,fileidlist);
        }
    } catch(...) {
        lgprintf("�����%d,����%d,�ļ��б�{%s}�쳣��",tabid,datapartid,fileidlist);;
        psa->log(tabid,datapartid,DLOAD_UPDATE_FILE_STATUS_NOTIFY,"��%d,����%d,�����ļ��б�{%s}�쳣���ָ�����״̬.",tabid,datapartid,fileidlist);
        ibdatafile.CloseReader();
        psa->logext(tabid,datapartid,EXT_STATUS_ERROR,"");

        AutoStmt st(psa->GetDTS());
        if(st.DirectExecute(" update dp.dp_datafilemap set procstatus=0,pid = 0 where procstatus=1 and tabid=%d and datapartid=%d and indexgid=%d and fileid in(%s)",
                            tabid,datapartid,indexid,fileidlist)!=loadfilearray.filenum) {
            psa->log(tabid,datapartid,DLOAD_UPDATE_FILE_STATUS_ERROR,"��%d,����%d,�����ļ��б�{%s}��״̬�ָ�����.",tabid,datapartid,fileidlist);
            ThrowWith("��%d,����%d,�����ļ��б�{%s}��״̬�ָ�����",tabid,datapartid,fileidlist);
        }
        lgprintf("��%d,����%d,�����ļ��б�{%s}�쳣���ѻָ�����״̬.",tabid,datapartid,fileidlist);

        // �������ȼ�����
        if(1 != st.DirectExecute(" update dp.dp_datapart set blevel=blevel+100 where tabid = %d and datapartid =%d",tabid,datapartid)) {
            psa->log(tabid,datapartid,DLOAD_UPDATE_TASK_STATUS_ERROR,"��%d,����%d,�޸��������ȼ�����.",tabid,datapartid);
            ThrowWith("��%d,����%d,�޸��������ȼ�����.��",tabid,datapartid);
        }
        lgprintf("��%d,����%d,�����������ȼ��ɹ�.",tabid,datapartid);

#ifndef KEEP_LOAD_FILE
        unlink(ibfilename);
#endif
        throw;
    }
    return 1;

}

// Դ��ΪDBPLUS�����Ŀ����Ҽ�¼���ǿա�
int DestLoader::MoveTable(const char *srcdbn,const char *srctabname,const char * dstdbn,const char *dsttabname)
{
    char dtpath[300];
    lgprintf("Ŀ������(ת��) '%s.%s -> '%s.%s'.",srcdbn,srctabname,dstdbn,dsttabname);
    sprintf(dtpath,"%s.%s",srcdbn,srctabname);
    if(!psa->TouchTable(dtpath))
        ThrowWith("Դ��û�ҵ�");
    sprintf(dtpath,"%s.%s",dstdbn,dsttabname);
    if(psa->TouchTable(dtpath)) {
        lgprintf(dtpath,"��%s.%s�Ѵ���,����ִ�и�������!",dstdbn,dsttabname);
        //      if(!GetYesNo(dtpath,false)) {
        lgprintf("ȡ�������� ");
        return 0;
        //      }
    }
    AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);
    int rn;
    //mt.FetchAll("select pathval from dp.dp_path where pathtype='msys'");
    //int rn=mt.Wait();
    //i/f(rn<1)
    //  ThrowWith("�Ҳ���MySQL����Ŀ¼(dt_path.pathtype='msys'),����ת���쳣��ֹ.");
    strcpy(dtpath,psa->GetMySQLPathName(0,"msys"));
    if(STRICMP(srcdbn,dstdbn)==0 && STRICMP(srctabname,dsttabname)==0)
        ThrowWith("Դ���Ŀ������Ʋ�����ͬ.");
    mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",dsttabname,dstdbn);
    rn=mt.Wait();
    if(rn>0) {
        ThrowWith("��'%s.%s'�Ѵ���(��¼��:%d)������ʧ��!",dstdbn,dsttabname,mt.GetInt("recordnum",0));
    }
    mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",srctabname,srcdbn);
    rn=mt.Wait();
    if(rn<1) {
        ThrowWith("Դ��'%s.%s'������.",srcdbn,srctabname);
    }
    mt.FetchAll("select * from dp.dp_datapart where tabid=%d and status not in(0,5)",mt.GetInt("tabid",0));
    rn=mt.Wait();
    if(rn>0) {
        ThrowWith("Դ��'%s.%s'Ǩ�ƹ���δ��ɣ����ܸ���.",srcdbn,srctabname);
    }
    mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",srctabname,srcdbn);
    rn=mt.Wait();
    lgprintf("���ò���ת��.");
    int dsttabid=psa->NextTableID();
    tabid=mt.GetInt("tabid",0);
    int recordnum=mt.GetInt("recordnum",0);
    int firstdatafileid=mt.GetInt("firstdatafileid",0);
    double totalbytes=mt.GetDouble("totalbytes",0);
    int datafilenum=mt.GetInt("datafilenum",0);
    if(recordnum<1) {
        lgprintf("Դ��'%s.%s'����Ϊ�գ������ʧ�ܡ�",srcdbn,srctabname);
        return 0;
    }
    lgprintf("Դ��'%s.%s' id:%d,��¼��:%d,��ʼ�����ļ��� :%d",
             srcdbn,srctabname,tabid,recordnum,firstdatafileid);

    //�±����ڣ���dt_table���½�һ����¼
    *mt.PtrInt("tabid",0)=dsttabid;
    char *pStrVal = new char[250];
    memset(pStrVal,0,250);
    strcpy(pStrVal,mt.PtrStr("tabdesc",0));
    strcat(pStrVal,"_r");
    mt.SetStr("tabdesc",0,pStrVal,1);
    delete[] pStrVal;
    //strcat(mt.PtrStr("tabdesc",0),"_r");
    //strcpy(mt.PtrStr("tabname",0),dsttabname);
    //strcpy(mt.PtrStr("databasename",0),dstdbn);
    mt.SetStr("tabname",0,(char *)dsttabname);
    mt.SetStr("databasename",0,(char *)dstdbn);
    wociAppendToDbTable(mt,"dp.dp_table",psa->GetDTS(),true);
    psa->CloseTable(tabid,NULL,false);
    CopyMySQLTable(dtpath,srcdbn,srctabname,dstdbn,dsttabname);
    //��ʱ�ر�Դ������ݷ��ʣ���¼���Ѵ��ڱ��ر���recordnum��
    //Ŀ����.DTP�ļ��Ѿ�����,��ʱ���η���
    psa->CloseTable(dsttabid,NULL,false);
    char sqlbuf[MAX_STMT_LEN];
    mt.FetchAll("select * from dp.dp_datapart where tabid=%d order by datapartid ",tabid);
    rn=mt.Wait();
    //����������¼���������޸������ļ��������ļ���tabid ָ��
    int i=0;
    for(i=0; i<rn; i++)
        *mt.PtrInt("tabid",i)=dsttabid;
    wociAppendToDbTable(mt,"dp.dp_datapart",psa->GetDTS(),true);

    mt.FetchAll("select * from dp.dp_index where tabid=%d order by seqindattab ",tabid);
    rn=mt.Wait();
    //����������¼���������޸������ļ��������ļ���tabid ָ��

    int nStrValLen = mt.GetColLen((char*)"indextabname");
    pStrVal = new char[rn * nStrValLen];
    memset(pStrVal,0,rn*nStrValLen);
    for(i=0; i<rn; i++) {
        *mt.PtrInt("tabid",i)=dsttabid;
        //strcpy(mt.PtrStr("indextabname",i),"");
    }
    mt.SetStr("indextabname",0,pStrVal,rn);
    delete[] pStrVal;

    wociAppendToDbTable(mt,"dp.dp_index",psa->GetDTS(),true);

    lgprintf("����ת��.");
    for(i=0; i<rn; i++) {
        if(mt.GetInt("issoledindex",i)>0) {
            char tbn1[300],tbn2[300];
            //������ϵ�����������,���ٴ������������.
            psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tbn1,TBNAME_DEST);
            if(psa->TouchTable(tbn1)) {
                psa->GetTableName(dsttabid,mt.GetInt("indexgid",i),NULL,tbn2,TBNAME_DEST);
                lgprintf("������ '%s'-->'%s...'",tbn1,tbn2);
                psa->FlushTables(tbn1);
                MoveMySQLTable(dtpath,srcdbn,strstr(tbn1,".")+1,dstdbn,strstr(tbn2,".")+1);
            } else
                ThrowWith("�Ҳ���������'%s',�����쳣��ֹ,��Ҫ�ֹ���鲢�޸�����!",tbn1);
            int ilp=0;
            psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tbn1,TBNAME_DEST,ilp);
            if(psa->TouchTable(tbn1)) {
                //3.10���¸�ʽ����.
                char srcf[300];
                //����MERGE�ļ�
                //
                sprintf(srcf,"%s%s/%s.MRG",dtpath,dstdbn,strstr(tbn2,".")+1);
                FILE *fp=fopen(srcf,"w+t");
                while(psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tbn1,TBNAME_DEST,ilp)) {
                    psa->GetTableName(dsttabid,mt.GetInt("indexgid",i),NULL,tbn2,TBNAME_DEST,ilp++);
                    lgprintf("������ '%s'-->'%s...'",tbn1,tbn2);
                    psa->FlushTables(tbn1);
                    MoveMySQLTable(dtpath,srcdbn,strstr(tbn1,".")+1,dstdbn,strstr(tbn2,".")+1);
                    fprintf(fp,"%s\n",strstr(tbn2,".")+1);
                }
                fprintf(fp,"#INSERT_METHOD=LAST\n");
                fclose(fp);
            }
        }
    }
    mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d order by fileid",tabid);
    rn=mt.Wait();
    AutoMt idxmt(psa->GetDTS(),MAX_DST_DATAFILENUM);
    lgprintf("�����ļ�ת��.");

    for(i=0; i<rn; i++) {
        char fn[300];
        psa->GetMySQLPathName(mt.GetInt("pathid",i));
        sprintf(fn,"%s%s.%s_%d_%d_%d.dat",psa->GetMySQLPathName(mt.GetInt("pathid",i)),
                dstdbn,dsttabname,mt.GetInt("datapartid",i),mt.GetInt("indexgid",i),mt.GetInt("fileid",i));

        FILE *fp;
        fp=fopen(mt.PtrStr("filename",i),"rb");
        if(fp==NULL) ThrowWith("�Ҳ����ļ�'%s'.",mt.PtrStr("filename",i));
        fclose(fp);
        fp=fopen(fn,"rb");
        if(fp!=NULL) ThrowWith("�ļ�'%s'�Ѿ�����.",fn);
        rename(mt.PtrStr("filename",i),fn);
        //strcpy(mt.PtrStr("filename",i),fn);
        mt.SetStr("filename",i,fn);
        *mt.PtrInt("tabid",i)=dsttabid;

        //psa->GetMySQLPathName(idxmt.GetInt("pathid",i));
        sprintf(fn,"%s%s.%s_%d_%d_%d.idx",psa->GetMySQLPathName(mt.GetInt("pathid",i)),
                dstdbn,dsttabname,mt.GetInt("datapartid",i),mt.GetInt("indexgid",i),mt.GetInt("fileid",i));
        rename(mt.PtrStr("idxfname",i),fn);
        //  strcpy(mt.PtrStr("idxfname",i),fn);
        mt.SetStr("idxfname",i,fn);
    }
    wociAppendToDbTable(mt,"dp.dp_datafilemap",psa->GetDTS(),true);

    sprintf(sqlbuf,"delete from dp.dp_datafilemap where tabid=%d",tabid);
    psa->DoQuery(sqlbuf);
    sprintf(sqlbuf,"update dp.dp_log set tabid=%d where tabid=%d",dsttabid,tabid);
    psa->DoQuery(sqlbuf);
    sprintf(sqlbuf,"update dp.dp_table set recordnum=0,cdfileid=0  where tabid=%d ",tabid);
    psa->DoQuery(sqlbuf);
    psa->log(dsttabid,0,118,"���ݴ�%s.%s��ת�ƶ���.",srcdbn,srctabname);
    //�����ļ�����
    mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) and isfirstindex=1 order by datapartid ,fileid",dsttabid);
    rn=mt.Wait();
    int k;
    for(k=0; k<rn; k++) {
        //Build table data file link information.
        if(k+1==rn) {
            dt_file df;
            df.Open(mt.PtrStr("filename",k),2,mt.GetInt("fileid",k));
            df.SetFileHeader(0,NULL);
            df.Open(mt.PtrStr("idxfname",k),2,mt.GetInt("fileid",k));
            df.SetFileHeader(0,NULL);
        } else {
            dt_file df;
            df.Open(mt.PtrStr("filename",k),2,mt.GetInt("fileid",k));
            df.SetFileHeader(0,mt.PtrStr("filename",k+1));
            df.Open(mt.PtrStr("idxfname",k),2,mt.GetInt("fileid",k));
            df.SetFileHeader(0,mt.PtrStr("idxfname",k+1));
        }
    }

    //Move��������,��Ŀ���
    lgprintf("MySQLˢ��...");
    char tbn[300];
    sprintf(tbn,"%s.%s",dstdbn,dsttabname);
    psa->BuildDTP(tbn);
    psa->FlushTables(tbn);
    lgprintf("ɾ��Դ��..");
    RemoveTable(srcdbn,srctabname,false);
    lgprintf("�����Ѵӱ�'%s'ת�Ƶ�'%s'��",srctabname,dsttabname);
    return 1;
}


// 7,10������״̬����:
//   1. ���ļ�ϵͳ��ȡ����ѹ������ļ���С.
//   2. ����״̬�޸�Ϊ(8,11) (?? ����ʡ��)
//   3. �ر�Ŀ���(unlink DTP file,flush table).
//   4. �޸�����/����ӳ����е��ļ���С��ѹ������.
//   5. ����/���� �ļ��滻.
//   6. ����״̬�޸�Ϊ30(�ȴ�����װ��).
int DestLoader::ReLoad()
{

    AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
    mdf.FetchAll("select * from dp.dp_datapart where status in (7,10) and begintime<now() %s order by blevel,tabid,datapartid limit 2",psa->GetNormalTaskDesc());
    int rn=mdf.Wait();
    if(rn<1) {
        printf("û�з�������ѹ����ɵȴ�װ�������.\n");
        return 0;
    }
    bool dpcp=mdf.GetInt("status",0)==7;
    int compflag=mdf.GetInt("compflag",0);
    tabid=mdf.GetInt("tabid",0);
    psa->SetTrace("reload",tabid);
    mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) order by indexgid",tabid);
    rn=mdf.Wait();
    if(rn<1) {
        lgprintf("װ�����ѹ������ʱ�Ҳ��������ļ���¼,���ձ���");
        char sqlbf[MAX_STMT_LEN];
        sprintf(sqlbf,"update dp.dp_datapart set status=30 where tabid=%d", tabid);
        if(psa->DoQuery(sqlbf)<1) {
            psa->log(tabid,0,DLOAD_UPDATE_TASK_STATUS_ERROR,"����ѹ����������װ������޸�����״̬�쳣�����������������̳�ͻ(tabid:%d)��\n",tabid);
            ThrowWith("����ѹ����������װ������޸�����״̬�쳣�����������������̳�ͻ(tabid:%d)��\n",tabid);
        }
        return 0;
    }
    mdf.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
    rn=mdf.Wait();
    if(rn<1) {
        lgprintf("װ�����ѹ������ʱ�Ҳ���dp.dp_table��¼(tabid:%d).",tabid);
        return 0;
    }
    char dbname[100],tbname[100];
    strcpy(dbname,mdf.PtrStr("databasename",0));
    strcpy(tbname,mdf.PtrStr("tabname",0));
    AutoMt datmt(psa->GetDTS(),MAX_DST_DATAFILENUM);
    datmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) order by datapartid,indexgid,fileid",
                   tabid);
    int datrn=datmt.Wait();
    rn=datrn;
    AutoStmt updst(psa->GetDTS());
    char tmpfn[300];
    int k;
    unsigned long dtflen[MAX_DST_DATAFILENUM];
    unsigned long idxflen[MAX_DST_DATAFILENUM];
    //�ȼ��
    for(k=0; k<datrn; k++) {
        sprintf(tmpfn,"%s.%s",datmt.PtrStr("filename",k),dpcp?"depcp":"dep5");
        dt_file df;
        df.Open(tmpfn,0);
        dtflen[k]=df.GetFileSize();
        if(dtflen[k]<1) {
            psa->log(tabid,0,DLOAD_CHECK_FIEL_ERROR,"��%d,�����ļ�%sΪ��.",tabid,tmpfn);
            ThrowWith("file '%s' is empty!",tmpfn);
        }
    }
    for(k=0; k<rn; k++) {
        sprintf(tmpfn,"%s.%s",datmt.PtrStr("idxfname",k),dpcp?"depcp":"dep5");
        dt_file df;
        df.Open(tmpfn,0);
        idxflen[k]=df.GetFileSize();
        if(idxflen[k]<1) {
            psa->log(tabid,0,DLOAD_CHECK_FIEL_ERROR,"��%d,�����ļ�%sΪ��.",tabid,tmpfn);
            ThrowWith("file '%s' is empty!",tmpfn);
        }
    }
    char sqlbf[MAX_STMT_LEN];
    sprintf(sqlbf,"update dp.dp_datapart set status=%d where tabid=%d", dpcp?8:11,tabid);
    if(psa->DoQuery(sqlbf)<1) {
        psa->log(tabid,0,DLOAD_UPDATE_TASK_STATUS_ERROR,"����ѹ����������װ������޸�����״̬�쳣�����������������̳�ͻ(tabid:%d)��\n",tabid);
        ThrowWith("����ѹ����������װ������޸�����״̬�쳣�����������������̳�ͻ(tabid:%d)��\n",tabid);
    }
    //��ֹ�������룬�޸�����״̬
    //�����޸Ľ��漰�����ļ����滻,��������ǰ�ȹرձ�
    // ����رղ�������ס��ķ��ʣ�ֱ�����߳ɹ���
    //  TODO  �ļ�����������ʱ����Ϊ����
    psa->CloseTable(tabid,NULL,false,true);
    lgprintf("�����ѹر�.");
    //���µ������ļ��滻ԭ�����ļ�����ɾ��ԭ�ļ������ļ����Ƹ���Ϊԭ�ļ����޸��ļ���¼�е��ļ���С�ֶΡ�
    lgprintf("��ʼ���ݺ������ļ��滻...");
    for(k=0; k<datrn; k++) {
        updst.Prepare("update dp.dp_datafilemap set filesize=%d,compflag=%d,idxfsize=%d where tabid=%d and fileid=%d and fileflag=0",
                      dtflen[k],compflag,idxflen[k],tabid,datmt.GetInt("fileid",k));
        updst.Execute(1);
        updst.Wait();
        const char *filename=datmt.PtrStr("filename",k);
        unlink(filename);
        sprintf(tmpfn,"%s.%s",filename,dpcp?"depcp":"dep5");
        rename(tmpfn,filename);
        lgprintf("rename file '%s' as '%s'",tmpfn,filename);
        filename=datmt.PtrStr("idxfname",k);
        unlink(filename);
        sprintf(tmpfn,"%s.%s",filename,dpcp?"depcp":"dep5");
        rename(tmpfn,filename);
        lgprintf("rename file '%s' as '%s'",tmpfn,filename);
    }
    lgprintf("���ݺ������ļ��ѳɹ��滻...");
    sprintf(sqlbf,"update dp.dp_datapart set status=30,istimelimit=0 where tabid=%d", tabid);
    psa->DoQuery(sqlbf);
    sprintf(sqlbf,"update dp.dp_datafilemap set procstatus=0 where tabid=%d and fileflag=0",tabid);
    psa->DoQuery(sqlbf);
    lgprintf("����״̬�޸�Ϊ�����������(3),�����ļ�����״̬��Ϊδ����(10).");
    Load();
    return 1;
}


int DestLoader::RecreateIndex(SysAdmin *_Psa)
{
    AutoMt mdf(psa->GetDTS(),MAX_MIDDLE_FILE_NUM);
    AutoMt mdf1(psa->GetDTS(),MAX_MIDDLE_FILE_NUM);
    char sqlbf[MAX_STMT_LEN];
    //�����Ҫ���ߵ�����
    // ʹ���Ӳ�ѯ������ʹ�߼�����
    //   select * from dp.dp_datapart a where status=21 and istimelimit!=22 and begintime<now() %s and tabid not in (
    //select tabid from dp_datapart b where b.tabid=a.tabid and b.status!=21 and b.status!=5 and b.begintime<now())
    //order by blevel,tabid,datapartid

    mdf1.FetchAll("select * from dp.dp_datapart where status=21 and begintime<now() %s order by blevel,tabid,datapartid",psa->GetNormalTaskDesc());
    if(mdf1.Wait()>0) {
        int mrn=mdf1.GetRows();
        for(int i=0; i<mrn; i++) {
            //�����ߴ���
            tabid=mdf1.GetInt("tabid",i);
            psa->SetTrace("dataload",tabid);
            datapartid=mdf1.GetInt("datapartid",i);
            int newld=mdf1.GetInt("oldstatus",i)==4?1:0;
            int oldstatus=mdf1.GetInt("oldstatus",i);
            if(mdf1.GetInt("istimelimit",i)==22) { //���������ڴ�������
                mdf.FetchAll("select * from dp.dp_table where tabid=%d ",
                             tabid);
                if(mdf.Wait()>0)
                    lgprintf("�� %s.%s ���߹������������̴���������������쳣�˳���������װ��",
                             mdf.PtrStr("databasename",0),mdf.PtrStr("tabname",0));
                else
                    lgprintf("tabidΪ%d�ı����߹������������̴���������������쳣�˳���������װ��",tabid);
                continue;
            }
            mdf.FetchAll("select * from dp.dp_datapart where tabid=%d and status!=21 and status!=5 and begintime<now()",tabid);
            if(mdf.Wait()<1) {
                //��Դ��(��ʽ��)����Ŀ���ṹ,����������ݳ�ȡΪ�յ����
                char tbname[150],idxname[150];
                psa->GetTableName(tabid,-1,tbname,idxname,TBNAME_PREPONL);
                sprintf(sqlbf,"update dp.dp_datapart set istimelimit=22 where tabid=%d and status =21 and begintime<now()",tabid);
                if(psa->DoQuery(sqlbf)<1)  {
                    lgprintf("��%d����ʱ�޸�����״̬�쳣�����������������̳�ͻ��\n",tabid);
                    continue;
                }
                try {
                    AutoMt destmt(0,10);
                    //����Ҳ��������ļ�,���Դ������ṹ
                    if(psa->CreateDataMtFromFile(destmt,0,tabid,newld)==0) {
                        AutoHandle srcdbc;
                        srcdbc.SetHandle(psa->BuildSrcDBC(tabid,-1));
                        psa->BuildMtFromSrcTable(srcdbc,tabid,&destmt);
                    }
                    psa->CreateTableOnMysql(destmt,tbname,true);

                    psa->CreateAllIndex(tabid,TBNAME_PREPONL,true,CI_DAT_ONLY,-1);
                    psa->DataOnLine(tabid);
                    AutoStmt st(psa->GetDTS());
                    st.DirectExecute("update dp.dp_datapart set istimelimit=0,blevel=mod(blevel,100) where tabid=%d  and blevel>=100 and begintime<now()",tabid);
                    st.DirectExecute("update dp.dp_datafilemap set blevel=0 where tabid=%d ",tabid);

                    lgprintf("ɾ���м���ʱ�ļ�...");
                    mdf.FetchAll("select * from dp.dp_middledatafile where tabid=%d",tabid);

                    int dfn=mdf.Wait();
                    for(int di=0; di<dfn; di++) {
                        lgprintf("ɾ���ļ�'%s'",mdf.PtrStr("datafilename",di));
                        unlink(mdf.PtrStr("datafilename",di));
                        lgprintf("ɾ���ļ�'%s'",mdf.PtrStr("indexfilename",di));
                        unlink(mdf.PtrStr("indexfilename",di));
                    }
                    lgprintf("ɾ����¼...");
                    st.Prepare("delete from dp.dp_middledatafile where tabid=%d",tabid);
                    st.Execute(1);
                    st.Wait();
                    psa->CleanData(false,tabid);
                    psa->logext(tabid,datapartid,EXT_STATUS_END,"");
                    return 1;
                } catch(...) {
                    errprintf("��%d ����ʱ�����쳣����,�ָ�����״̬...",tabid);
                    psa->log(tabid,0,DLOAD_ONLINE_EXCEPTION_ERROR,"��%d,����ʱ�����쳣����.",tabid);
                    //
                    //sprintf(sqlbf,"update dp.dp_datapart set status=21,istimelimit=0,blevel=if(ifnull(blevel,0)>=100,blevel,ifnull(blevel,0)+100),oldstatus=%d where tabid=%d and istimelimit=22", oldstatus,tabid);
                    sprintf(sqlbf,"update dp.dp_datapart set istimelimit=0,blevel=if(ifnull(blevel,0)>=100,blevel,ifnull(blevel,0)+100),oldstatus=%d where tabid=%d and istimelimit=22", oldstatus,tabid);
                    //sprintf(sqlbf,"update dp.dp_datapart set status=21 where tabid=%d and istimelimit=22", oldstatus,tabid);
                    psa->DoQuery(sqlbf);
                    psa->log(tabid,0,DLOAD_UPDATE_TASK_STATUS_NOTIFY,"��%d,����ʱ�����쳣����,�ѻָ�����״̬.",tabid);
                    throw;
                }
            }
            //else {
            //  AutoStmt st(psa->GetDTS());
            //  st.DirectExecute("update dp.dp_datapart set blevel=if(ifnull(blevel,0)>=100,blevel,ifnull(blevel,0)+100) where tabid=%d and datapartid=%d and ifnull(blevel,0)<100",tabid,datapartid);
            //}
        }
    }

    // ��װ�������ݵ��������ϣ��޸����������޸�����״̬Ϊ21
    mdf.FetchAll("select * from dp.dp_datapart where (status =4 or status=40 ) %s order by blevel,tabid,datapartid limit 2",psa->GetNormalTaskDesc());
    int rn=mdf.Wait();
    if(rn<1) {
        return 0;
    }
    datapartid=mdf.GetInt("datapartid",0);
    tabid=mdf.GetInt("tabid",0);
    psa->SetTrace("dataload",tabid);
    bool preponl=mdf.GetInt("status",0)==4;
    //if(tabid<1) ThrowWith("�Ҳ��������:%d��Tabid",taskid);

    //check intergrity.
    mdf.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
    rn=mdf.Wait();
    if(rn<1) {
        psa->log(tabid,0,DLOAD_DST_TABLE_MISS_INDEX,"Ŀ���%dȱ����������¼.",tabid);
        ThrowWith("Ŀ���%dȱ����������¼.",tabid);
    }

    mdf.FetchAll("select distinct indexgid from dp.dp_datafilemap where tabid=%d and fileflag=%d",tabid,preponl?1:0);
    rn=mdf.Wait();
    mdf.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
    int rni=mdf.Wait();
    if(rni!=rn) {
        lgprintf("���ִ���: �ؽ�����ʱ�������ļ��еĶ���������(%d)�������������е�ֵ(%d)����,tabid:%d,datapartid:%d.",rn,rni,tabid,datapartid);
        return 0; //dump && destload(create temporary index table) have not complete.
    }
    try {
        sprintf(sqlbf,"update dp.dp_datapart set oldstatus=status,status=%d where tabid=%d and datapartid=%d and (status =4 or status=40 )",20,tabid,datapartid);
        if(psa->DoQuery(sqlbf)<1) {
            psa->log(tabid,0,DLOAD_DST_TABLE_CREATE_INDEX_ERROR,"��%d,����װ���ؽ����������޸�����״̬�쳣�����������������̳�ͻ��",tabid);
            ThrowWith("����װ���ؽ����������޸�����״̬�쳣�����������������̳�ͻ��\n"
                      "  tabid:%d.\n",
                      tabid);
        }
        lgprintf("��ʼ�����ؽ�,tabid:%d,�������� :%d",tabid,rn);
        psa->log(tabid,0,DLOAD_CREATE_INDEX_NOTIFY,"��ʼ��������.");
        //2005/12/01 ������Ϊ�����������ؽ�(�޸�)��
        lgprintf("���������Ĺ��̿�����Ҫ�ϳ���ʱ�䣬�����ĵȴ�...");
        psa->RepairAllIndex(tabid,TBNAME_PREPONL,datapartid);
        lgprintf("�����������.");
        psa->log(tabid,0,DLOAD_CREATE_INDEX_NOTIFY,"�����������.");
        AutoStmt st(psa->GetDTS());
        st.DirectExecute("update dp.dp_datapart set status=21 where tabid=%d and datapartid=%d",
                         tabid,datapartid);
        mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and isfirstindex=1 and fileflag!=2 order by datapartid,indexgid,fileid",
                     tabid);
        rn=mdf.Wait();
    } catch (...) {
        errprintf("���������ṹʱ�����쳣����,�ָ�����״̬...");
        sprintf(sqlbf,"update dp.dp_datapart set status=%d,blevel=if(ifnull(blevel,0)>=100,blevel,ifnull(blevel,0)+100) where tabid=%d and datapartid=%d", preponl?3:30,tabid,datapartid);
        psa->DoQuery(sqlbf);
        sprintf(sqlbf,"update dp.dp_datafilemap set procstatus=0 where tabid=%d and "
                "procstatus=1 and fileflag=%d and datapartid=%d",tabid,preponl?1:0,datapartid);
        psa->DoQuery(sqlbf);
        psa->log(tabid,0,DLOAD_CREATE_INDEX_NOTIFY,"���������ṹʱ�����쳣����,�ѻָ�����״̬.");
        throw;
    }
    return 1;
}

thread_rt LaunchWork(void *ptr)
{
    ((worker *) ptr)->work();
    thread_end;
}

//���´����д���
//up to 2005/04/13, the bugs of this routine continuous produce error occursionnaly .
//   ReCompress sometimes give up last block of data file,but remain original index record in idx file.
int DestLoader::ReCompress(int threadnum)
{
    AutoMt mdt(psa->GetDTS(),MAX_DST_DATAFILENUM);
    AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
    mdt.FetchAll("select distinct tabid,datapartid,status,compflag from dp.dp_datapart where (status=6 or status=9) and begintime<now() %s order by blevel,tabid,datapartid",psa->GetNormalTaskDesc());
    int rn1=mdt.Wait();
    int rn;
    int i=0;
    bool deepcmp;
    if(rn1<1) {
        return 0;
    }
    for(i=0; i<rn1; i++) {
        tabid=mdt.GetInt("tabid",i);
        psa->SetTrace("recompress",tabid);
        datapartid=mdt.GetInt("datapartid",i);
        deepcmp=mdt.GetInt("status",i)==6;
        mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus =0 and (fileflag=0 or fileflag is null)  order by blevel,datapartid,indexgid,fileid",tabid);
        rn=mdf.Wait();
        if(rn<1) {
            mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus <>2 and (fileflag=0 or fileflag is null)  order by datapartid,indexgid,fileid",tabid);
            rn=mdf.Wait();
            if(rn<1) {
                AutoStmt st1(psa->GetDTS());
                st1.Prepare("update dp.dp_datapart set status=%d where tabid=%d",
                            deepcmp?7:10,tabid);
                st1.Execute(1);
                st1.Wait();
                st1.Prepare("update dp.dp_datafilemap set procstatus =0 where tabid=%d and fileflag=0",tabid);
                st1.Execute(1);
                st1.Wait();
                lgprintf("��%d--����ѹ����������ɣ�����״̬���޸�Ϊ%d,�����ļ�����״̬�޸�Ϊ����(0)",tabid,deepcmp?7:10);
                return 1;
            } else lgprintf("��%d(%d)---����ѹ������δ���,����û�еȴ�ѹ��������",tabid,datapartid);
        } else break;
    }
    if(i==rn1) return 0;

    //��ֹ��һ��mdt�е����ݱ���������������Ĵ���Ĺ�dp_datapart status
    mdt.FetchAll("select tabid,datapartid,status,compflag from dp.dp_datapart where (status=6 or status=9) and begintime<now() and tabid=%d and datapartid=%d",
                 mdf.GetInt("tabid",0),mdf.GetInt("datapartid",0));
    if(mdt.Wait()<1) {
        lgprintf("Ҫ����ѹ������������ļ�,��Ӧ����״̬�Ѹı�,ȡ������.\n"
                 " tabid:%d,datapartid:%d,fileid:%d.",mdf.GetInt("tabid",0),mdf.GetInt("datapartid",0),mdf.GetInt("fileid",0));
        return 0;
    }
    psa->OutTaskDesc("��������ѹ������(tabid %d datapartid %d)",mdf.GetInt("tabid",0),mdf.GetInt("datapartid",0));
    int compflag=mdt.GetInt("compflag",0);
    lgprintf("ԭѹ������:%d, �µ�ѹ������:%d .",mdf.GetInt("compflag",0),compflag);
    int fid=mdf.GetInt("fileid",0);
    psa->log(tabid,0,121,"����ѹ�������ͣ� %d-->%d ,�ļ���%d,��־�ļ� '%s' .",mdf.GetInt("compflag",0),compflag,fid,wociGetLogFile());

    char srcfn[300];
    strcpy(srcfn,mdf.PtrStr("filename",0));
    int origsize=mdf.GetInt("filesize",0);
    char dstfn[300];
    sprintf(dstfn,"%s.%s",srcfn,deepcmp?"depcp":"dep5");
    tabid=mdf.GetInt("tabid",0);
    mdf.FetchAll("select filename,idxfname from dp.dp_datafilemap where tabid=%d and fileid=%d and fileflag!=2",
                 tabid,fid);
    if(mdf.Wait()<1)
        ThrowWith(" �Ҳ��������ļ���¼,dp_datafilemap�еļ�¼����,����.\n"
                  " ��Ӧ�������ļ�Ϊ:'%s',�ļ����: '%d'",srcfn,fid);
    char idxdstfn[300];
    sprintf(idxdstfn,"%s.%s",mdf.PtrStr("idxfname",0),deepcmp?"depcp":"dep5");
    double dstfilelen=0;
    try {
        //��ֹ���룬�޸������ļ�״̬��
        AutoStmt st(psa->GetDTS());
        st.Prepare("update dp.dp_datafilemap set procstatus=1 where tabid=%d and fileid=%d and procstatus=10 and fileflag!=2",
                   tabid,fid);
        st.Execute(1);
        st.Wait();
        if(wociGetFetchedRows(st)!=1) {
            lgprintf("�����ļ�ѹ��ʱ״̬�쳣,tabid:%d,fid:%d,�������������̳�ͻ��"
                     ,tabid,fid);
            return 0;
        }
        file_mt idxf;
        lgprintf("���ݴ��������ļ�:'%s',�ֽ���:%d,�����ļ�:'%s'.",srcfn,origsize,mdf.PtrStr("idxfname",0));
        idxf.Open(mdf.PtrStr("idxfname",0),0);

        dt_file srcf;
        srcf.Open(srcfn,0,fid);
        dt_file dstf;
        dstf.Open(dstfn,1,fid);
        mdf.SetHandle(srcf.CreateMt());
        long lastoffset=dstf.WriteHeader(mdf,0,fid,srcf.GetNextFileName());

        dt_file idxdstf;
        idxdstf.Open(idxdstfn,1,fid);
        mdf.SetHandle(idxf.CreateMt());
        int idxrn=idxf.GetRowNum();
        idxdstf.WriteHeader(mdf,idxf.GetRowNum(),fid,idxf.GetNextFileName());
        if(idxf.ReadBlock(-1,0)<0)
            ThrowWith("�����ļ���ȡ����: '%s'",mdf.PtrStr("filename",0));
        AutoMt *pidxmt=(AutoMt *)idxf;
        //lgprintf("�������ļ�����%d����¼.",wociGetMemtableRows(*pidxmt));
        int *pblockstart=pidxmt->PtrInt("blockstart",0);
        int *pblocksize=pidxmt->PtrInt("blocksize",0);
        blockcompress bc(compflag);
        for(i=1; i<threadnum; i++) {
            bc.AddWorker(new blockcompress(compflag));
        }
        lgprintf("�����߳���:%d.",threadnum);
#define BFNUM 32
        char *srcbf=new char[SRCBUFLEN];//?�һ�δ����������ݿ飨��ѹ���󣩡
        char *dstbf=new char[DSTBUFLEN*BFNUM];//���ۻ����������(ѹ����).
        int dstseplen=DSTBUFLEN;
        bool isfilled[BFNUM];
        int filledlen[BFNUM];
        int filledworkid[BFNUM];
        char *outcache[BFNUM];
        for(i=0; i<BFNUM; i++) {
            isfilled[i]=false;
            filledworkid[i]=0;
            outcache[i]=dstbf+i*DSTBUFLEN;
            filledlen[i]=0;
        }
        int workid=0;
        int nextid=0;
        int oldblockstart=pblockstart[0];
        int lastrow=0;
        int slastrow=0;
        bool iseof=false;
        bool isalldone=false;
        int lastdsp=0;
        mytimer tmr;
        tmr.Start();
        while(!isalldone) {//�ļ��������˳�
            if(srcf.ReadMt(-1,0,mdf,1,1,srcbf,false,true)<0) {
                iseof=true;
            }
            if(wdbi_kill_in_progress) {
                wdbi_kill_in_progress=false;
                ThrowWith("�û�ȡ������!");
            }
            block_hdr *pbh=(block_hdr *)srcbf;
            int doff=srcf.GetDataOffset(pbh);
            if(pbh->origlen+doff>SRCBUFLEN)
                ThrowWith("Decompress data exceed buffer length. dec:%d,bufl:%d",
                          pbh->origlen+sizeof(block_hdr),SRCBUFLEN);
            bool deliverd=false;
            while(!deliverd) { //���񽻸����˳�
                worker *pbc=NULL;
                if(!iseof) {
                    pbc=bc.GetIdleWorker();
                    if(pbc) {

                        //pbc->Do(workid++,srcbf,pbh->origlen+sizeof(block_hdr),
                        //  pbh->origlen/2); //Unlock internal
                        pbc->Do(workid++,srcbf,pbh->origlen+doff,doff,
                                pbh->origlen<1024?1024:pbh->origlen); //Unlock internal
                        deliverd=true;
                    }
                }
                pbc=bc.GetDoneWorker();
                while(pbc) {
                    char *pout;
                    int dstlen=pbc->GetOutput(&pout);//Unlock internal;
                    int doneid=pbc->GetWorkID();
                    if(dstlen>dstseplen)
                        ThrowWith("Ҫѹ��������:%d,������������:%d.",dstlen,dstseplen);
                    //get empty buf:
                    for(i=0; i<BFNUM; i++) if(!isfilled[i]) break;
                    if(i==BFNUM) ThrowWith("���ش���ѹ���������������޷�����!.");
                    memcpy(outcache[i],pout,dstlen);
                    filledworkid[i]=doneid;
                    filledlen[i]=dstlen;
                    isfilled[i]=true;
                    pbc=bc.GetDoneWorker();
                    //lgprintf("Fill to cache %d,doneid:%d,len:%d",i,doneid,dstlen);
                }
                bool idleall=bc.isidleall();
                for(i=0; i<BFNUM; i++) {
                    if(isfilled[i] && filledworkid[i]==nextid) {
                        int idxrn1=wociGetMemtableRows(*pidxmt);
                        for(; pblockstart[lastrow]==oldblockstart;) {
                            pblockstart[lastrow]=(int)lastoffset;
                            pblocksize[lastrow++]=filledlen[i];
                            slastrow++;
                            if(lastrow==idxrn1) {
                                idxdstf.WriteMt(*pidxmt,compflag,0,false);
                                pidxmt->Reset();
                                lastrow=0;
                                if(idxf.ReadBlock(-1,0)>0) {
                                    //ThrowWith("�����ļ���ȡ����: '%s'",mdf.PtrStr("filename",0));
                                    pidxmt=(AutoMt *)idxf;
                                    //lgprintf("�������ļ�����%d����¼.",wociGetMemtableRows(*pidxmt));
                                    pblockstart=pidxmt->PtrInt("blockstart",0);
                                    pblocksize=pidxmt->PtrInt("blocksize",0);
                                } else break;
                            } else if(lastrow>idxrn1)
                                ThrowWith("�����ļ���ȡ����: '%s'",mdf.PtrStr("filename",0));

                        }
                        lastoffset=dstf.WriteBlock(outcache[i],filledlen[i],0,true);
                        oldblockstart=pblockstart[lastrow];
                        dstfilelen+=filledlen[i];
                        filledworkid[i]=0;
                        filledlen[i]=0;
                        isfilled[i]=false;
                        nextid++;
                        tmr.Stop();
                        double tm1=tmr.GetTime();
                        if(nextid-lastdsp>=50) {
                            printf("�Ѵ���%d�����ݿ�(%d%%),%.2f(MB/s) ��ʱ%.0f��--Ԥ�ƻ���Ҫ%.0f��.\r",nextid,slastrow*100/idxrn,lastoffset/tm1/1024/1024,tm1,tm1/slastrow*(idxrn-slastrow));
                            fflush(stdout);
                            lastdsp=nextid;
                        }
                        i=-1; //Loop from begining.
                    }
                }
                if(idleall && iseof) {
                    //                  if(bc.isidleall()) {
                    isalldone=true;
                    break;
                    //                  }
                }
                if(!pbc)
                    mSleep(10);
            }
        }
        if(lastrow!=wociGetMemtableRows(*pidxmt))
            ThrowWith("�쳣���󣺲����������ݶ��������Ѵ���%d,Ӧ����%d.",lastrow,wociGetMemtableRows(*pidxmt));
        if(wociGetMemtableRows(*pidxmt)>0)
            idxdstf.WriteMt(*pidxmt,compflag,0,false);
        dstf.Close();
        idxdstf.Close();
        st.Prepare("update dp.dp_datafilemap set procstatus=2 where tabid=%d and fileid=%d and procstatus=1 and fileflag=0",
                   tabid,fid);
        st.Execute(1);
        st.Wait();
        delete []srcbf;
        delete []dstbf;
    } catch(...) {
        errprintf("���ݶ���ѹ�������쳣���ļ�����״̬�ָ�...");
        AutoStmt st(psa->GetDTS());
        st.DirectExecute("update dp.dp_datafilemap set procstatus=0, blevel=ifnull(blevel,0)+1 where tabid=%d and fileid=%d and procstatus=1 and fileflag=0",
                         tabid,fid);
        st.DirectExecute("update dp.dp_datapart set blevel=ifnull(blevel,0)+100 where tabid=%d and datapartid=%d",
                         tabid,datapartid);
        errprintf("ɾ�������ļ��������ļ�");
        unlink(dstfn);
        unlink(idxdstfn);
        throw;
    }

    psa->log(tabid,0,122,"����ѹ������,�ļ�%d����С%d->%.0f",fid,origsize,dstfilelen);
    lgprintf("�ļ�ת������,Ŀ���ļ�:'%s',�ļ�����(�ֽ�):%.0f.",dstfn,dstfilelen);
    return 1;
}


int DestLoader::ToMySQLBlock(const char *dbn, const char *tabname)
{
    lgprintf("��ʽת�� '%s.%s' ...",dbn,tabname);
    AutoMt mt(psa->GetDTS(),100);
    mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",tabname,dbn);
    int rn=mt.Wait();
    if(rn<1) {
        printf("��'%s'������!",tabname);
        return 0;
    }
    tabid=mt.GetInt("tabid",0);
    int recordnum=mt.GetInt("recordnum",0);
    int firstdatafileid=mt.GetInt("firstdatafileid",0);
    if(recordnum<1) {
        lgprintf("Դ��'%s'����Ϊ��.",tabname);
        return 0;
    }
    AutoMt mdt(psa->GetDTS(),MAX_DST_DATAFILENUM);
    AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
    psa->SetTrace("transblock",tabid);
    mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus =0 and (fileflag=0 or fileflag is null) order by datapartid,indexgid,fileid",tabid);
    rn=mdf.Wait();
    //��ֹ���룬�޸������ļ�״̬��
    int fid=mdf.GetInt("fileid",0);
    char srcfn[300];
    strcpy(srcfn,mdf.PtrStr("filename",0));
    int origsize=mdf.GetInt("filesize",0);
    char dstfn[300];
    sprintf(dstfn,"%s.%s",srcfn,"dep5");
    tabid=mdf.GetInt("tabid",0);

    mdf.FetchAll("select idxfname as filename from dp.dp_datafilemap where tabid=%d and fileid=%d and fileflag!=2",
                 tabid,fid);
    rn=mdf.Wait();
    char idxdstfn[300];
    sprintf(idxdstfn,"%s.%s",mdf.PtrStr("filename",0),"dep5");
    double dstfilelen=0;
    try {
        AutoStmt st(psa->GetDTS());
        st.Prepare("update dp.dp_datafilemap set procstatus=1 where tabid=%d and fileid=%d and procstatus=0 and fileflag!=2",
                   tabid,fid);
        st.Execute(1);
        st.Wait();
        if(wociGetFetchedRows(st)!=1) {
            lgprintf("�����ļ�ת��ʱ״̬�쳣,tabid:%d,fid:%d,�������������̳�ͻ��"
                     ,tabid,fid);
            return 1;
        }
        file_mt idxf;
        lgprintf("���ݴ��������ļ�:'%s',�ֽ���:%d,�����ļ�:'%s'.",srcfn,origsize,mdf.PtrStr("filename",0));
        idxf.Open(mdf.PtrStr("filename",0),0);
        if(idxf.ReadBlock(-1,0)<0)
            ThrowWith("�����ļ���ȡ����: '%s'",mdf.PtrStr("filename",0));

        file_mt srcf;
        srcf.Open(srcfn,0,fid);
        dt_file dstf;
        dstf.Open(dstfn,1,fid);
        mdf.SetHandle(srcf.CreateMt());
        int lastoffset=dstf.WriteHeader(mdf,0,fid,srcf.GetNextFileName());

        AutoMt *pidxmt=(AutoMt *)idxf;
        int idxrn=wociGetMemtableRows(*pidxmt);
        lgprintf("�������ļ�����%d����¼.",idxrn);
        int *pblockstart=pidxmt->PtrInt("blockstart",0);
        int *pblocksize=pidxmt->PtrInt("blocksize",0);
        int lastrow=0;
        int oldblockstart=pblockstart[0];
        int dspct=0;
        while(true) {//�ļ��������˳�
            if(wdbi_kill_in_progress) {
                wdbi_kill_in_progress=false;
                ThrowWith("�û�ȡ������!");
            }
            int srcmt=srcf.ReadBlock(-1,0,1);
            if(srcmt==0) break;
            int tmpoffset=dstf.WriteMySQLMt(srcmt,COMPRESSLEVEL);
            int storesize=tmpoffset-lastoffset;
            for(; pblockstart[lastrow]==oldblockstart;) {
                pblockstart[lastrow]=lastoffset;
                pblocksize[lastrow++]=storesize;
            }
            if(++dspct>1000) {
                dspct=0;
                printf("\r...%d%% ",lastrow*100/idxrn);
                fflush(stdout);
                //          break;
            }
            lastoffset=tmpoffset;
            oldblockstart=pblockstart[lastrow];
        }
        dt_file idxdstf;
        idxdstf.Open(idxdstfn,1,fid);
        //mdf.SetHandle(idxf.CreateMt());
        idxdstf.WriteHeader(*pidxmt,idxrn,fid,idxf.GetNextFileName());
        dstfilelen=lastoffset;
        idxdstf.WriteMt(*pidxmt,COMPRESSLEVEL,0,false);
        dstf.Close();
        idxdstf.Close();
        st.Prepare("update dp.dp_datafilemap set procstatus=2 where tabid=%d and fileid=%d and procstatus=1 and fileflag=0",
                   tabid,fid);
        st.Execute(1);
        st.Wait();
    } catch(...) {
        errprintf("����ת�������쳣���ļ�����״̬�ָ�...");
        AutoStmt st(psa->GetDTS());
        st.Prepare("update dp.dp_datafilemap set procstatus=0 where tabid=%d and fileid=%d and procstatus=1 and fileflag=0",
                   tabid,fid);
        st.Execute(1);
        st.Wait();
        errprintf("ɾ�������ļ��������ļ�");
        unlink(dstfn);
        unlink(idxdstfn);
        throw;
    }

    lgprintf("�ļ�ת������,Ŀ���ļ�:'%s',�ļ�����(�ֽ�):%f.",dstfn,dstfilelen);
    return 1;
}



// ɾ������add by liujs
int DestLoader::RemoveTable(const char *dbn,const char *tabname,bool prompt)
{
    char sqlbuf[MAX_STMT_LEN];
    char choose[200];
    wociSetEcho(FALSE);

    sprintf(choose,"��ȷ���ñ�'%s.%s'��ǰδ��������Ǩ�Ʋ���? (Y/N)",dbn,tabname);
    bool forcedel = false;
    if(prompt) {
        forcedel = GetYesNo(choose,false);
        if(!forcedel) {
            return 0;
        }
    } else {
        forcedel = true;
    }
    lgprintf("remove table '%s.%s ' ...",dbn,tabname);
    AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);

    mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",tabname,dbn);
    int rn=mt.Wait();
    if(rn<1) ThrowWith("��%s.%s��dp_table���Ҳ���!",dbn,tabname);

    int tabid=mt.GetInt("tabid",0);

    mt.FetchFirst("select datapartid from dp.dp_datapart where tabid = %d",tabid);
    rn = mt.Wait();
    if(rn <1)ThrowWith("��%s.%s��dp_datapart���Ҳ���!",dbn,tabname);

    mt.FetchAll("select * from dp.dp_index where issoledindex>0 and tabid=%d",tabid);
    int indexrn=mt.Wait();
    if(indexrn<1) ThrowWith("��%s.%s��dp_index���Ҳ���!",dbn,tabname);

    // ɾ�������
    AutoStmt st(psa->GetDTS());

    sprintf(choose,"��ȷ���Ƿ�ɾ��%s.%s����������?(Y/N)",dbn,tabname);
    if(prompt) {
        forcedel = GetYesNo(choose,false);
    } else {
        forcedel = true;
    }
    if(forcedel) {
        try {
            sprintf(sqlbuf,"drop table %s.%s",dbn,tabname);
            psa->DoQuery(sqlbuf);
        } catch(...) {
            // drop tableʧ�ܣ�˵�������ڣ�������û��ִ�й�
            lgprintf("���ݿ��%s.%s������,�޷�ɾ�����ݱ��е�����!",dbn,tabname);
            sprintf(choose,"��ȷ���Ƿ�ɾ��%s.%s����������ù���������Ϣ?(Y/N)",dbn,tabname);
            forcedel = GetYesNo(choose,false);
            if(forcedel) {
                goto DeleteConfigInfo;
            }
        }
        st.Prepare(" update dp.dp_table set recordnum=0 where tabid=%d",tabid);
        st.Execute(1);
        st.Wait();
        lgprintf("��'%s.%s' ���ݿ������ѱ�ɾ��.",dbn,tabname);
    } else {
        lgprintf("��'%s.%s' ���ݿ�����δ��ɾ��.",dbn,tabname);
        return 0;
    }

DeleteConfigInfo:
    //----------------------------------------------------------------
    // ɾ���м���ʱ�ļ���Ϣ

    bool parallel_load = CheckParallelLoad();  // �ֲ�ʽ�ϲ����ݰ汾

    sprintf(choose,"�Ƿ�ɾ����'%s.%s'�Ĵ�װ���ļ�(dp.dp_datafilemap)? (Y/N)",dbn,tabname);
    if(prompt) {
        forcedel = GetYesNo(choose,false);
    } else {
        forcedel = true;
    }
    if(forcedel) {
        //----------------------------------------------------------------
        // ɾ���������ʱ�ṹ�ļ�
        lgprintf("ɾ��װ������������ļ�.");

        if(!parallel_load) {
            mt.FetchFirst("select * from dp.dp_datafilemap where tabid=%d",tabid);
            rn=mt.Wait();
            int i=0;
            for(i=0; i<rn; i++) {
                char tmp[300];
                lgprintf("ɾ��'%s'�͸��ӵ�depcp,dep5�ļ�",mt.PtrStr("filename",i));
                unlink(mt.PtrStr("filename",i));
                sprintf(tmp,"%s.depcp",mt.PtrStr("filename",i));
                unlink(tmp);
                sprintf(tmp,"%s.dep5",mt.PtrStr("filename",i));
                unlink(tmp);
                lgprintf("ɾ��'%s'�͸��ӵ�depcp,dep5�ļ�",mt.PtrStr("idxfname",i));
                unlink(mt.PtrStr("idxfname",i));
                sprintf(tmp,"%s.depcp",mt.PtrStr("idxfname",i));
                unlink(tmp);
                sprintf(tmp,"%s.dep5",mt.PtrStr("idxfname",i));
                unlink(tmp);
            }
            st.Prepare(" delete from dp.dp_datafilemap where tabid=%d ",tabid);
            st.Execute(1);
            st.Wait();

            // ɾ��������м������������
            lgprintf("ɾ���ɼ���������������ļ�.");
            mt.FetchFirst("select tabid,datafilename,indexfilename from dp.dp_middledatafile where tabid =%d",
                          tabid);
            rn=mt.Wait();
            for(i=0; i<rn; i++) {
                lgprintf("ɾ���ɼ���ɺ�������ļ�'%s'",mt.PtrStr("datafilename",i));
                unlink(mt.PtrStr("datafilename",i));

                lgprintf("ɾ���ɼ���ɺ�������ļ�'%s'",mt.PtrStr("indexfilename",i));
                unlink(mt.PtrStr("indexfilename",i));
            }
            st.Prepare(" delete from dp.dp_middledatafile where tabid=%d",tabid);
            st.Execute(1);
            st.Wait();

        } else {
            clear_table_tmp_data(tabid);

            st.Prepare(" delete from dp.dp_datapartext where tabid=%d",tabid);
            st.Execute(1);
            st.Wait();
        }
    }

    sprintf(choose,"��'%s.%s'�����ò���(������Ϣ<dp.dp_datapart>,������Ϣ<dp.dp_index>,��չ�ֶ���Ϣ<dp.dp_column_info>,��ṹ��Ϣ<dp.dp_table>)�Ƿ�ɾ��?(Y/N)",dbn,tabname);
    if(prompt) {
        forcedel = GetYesNo(choose,false);
    } else {
        forcedel = true;
    }
    if(forcedel) {
        st.Prepare(" delete from dp.dp_datapart where tabid=%d",tabid);
        st.Execute(1);
        st.Wait();

        st.Prepare(" delete from dp.dp_index where tabid=%d",tabid);
        st.Execute(1);
        st.Wait();

        st.Prepare(" delete from dp.dp_column_info where table_id=%d",tabid);
        st.Execute(1);
        st.Wait();

        st.Prepare(" delete from dp.dp_table where tabid=%d",tabid);
        st.Execute(1);
        st.Wait();
    }

    lgprintf("��'%s.%s' ��ɾ�����.",dbn,tabname);
    return 1;
}

// ɾ�����һ����������1,ɾ���������һ����������2
int DestLoader::RemoveTable_exclude_struct(const char *dbn,const char *tabname,const char *partname)
{
    lgprintf("remove table '%s.%s ' partition '%s' ...",dbn,tabname,partname);
    AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);

    mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",tabname,dbn);
    int rn=mt.Wait();
    if(rn<1) ThrowWith("��%s.%s��dp_table���Ҳ���!",dbn,tabname);

    int tabid=mt.GetInt("tabid",0);

    mt.FetchFirst("select datapartid from dp.dp_datapart where partfullname = lower('%s') and tabid = %d",partname,tabid);
    rn = mt.Wait();
    if(rn <1)ThrowWith("��%s.%s��dp_datapart���Ҳ���!",dbn,tabname);

    int datapartid = mt.GetInt("datapartid",0);

    mt.FetchAll("select count(1) ct from %s.%s",dbn,tabname);
    mt.Wait();
    long origrecs=mt.GetLong("ct", 0);

    mt.FetchAll("select connection_id() cid");
    mt.Wait();
    int connectid=(int)mt.GetLong("cid",0);

    mt.FetchAll("select * from dp.dp_index where issoledindex>0 and tabid=%d",tabid);
    int indexrn=mt.Wait();
    if(indexrn<1) ThrowWith("��%s.%s��dp_index���Ҳ���!",dbn,tabname);
    char targetname[300];
    if(indexrn>1)
        sprintf(targetname,"%s_dma1",tabname);
    else
        strcpy(targetname,tabname);

    AutoStmt st(psa->GetDTS());
    //-----------------------------------------------------------
    // ɾ��������Ϣ
    char truncinfo[300];
    sprintf(truncinfo,"%s%s/%s.bht/truncinfo.ctl",psa->GetMySQLPathName(0,"msys"),dbn,targetname);
    FILE *ftruncinfo=fopen(truncinfo,"rb");
    if(ftruncinfo!=NULL) {
        fclose(ftruncinfo);
        lgprintf("�ϴ�ɾ��δ���(%s.%s)!",dbn,tabname);
        return 0;
    }
    char partinfo[300];
    sprintf(partinfo,"%s%s/%s.bht/PART_00000.ctl",psa->GetMySQLPathName(0,"msys"),dbn,targetname);
    FILE *fpartinfo=fopen(partinfo,"rb");
    if(fpartinfo==NULL) {
        lgprintf("��δ����,�޷�ɾ��(%s.%s)!",dbn,tabname);
        return 0;
    }

    // parse partition info.
    char buf[4000];
    bool lastPartition = false;


    // RIAK �� AP�汾��PART_XXXXX.ctl�ļ���ʽ��һ��
    // fileid ���Ȳ�һ��
    // RIAK:8�ֽ�
    // AP:4�ֽ�
    bool b_riak_version = CheckRiakVersion();

    fread(buf,1,8,fpartinfo);
    bool oldversion=false;
    if(memcmp(buf,"PARTIF15",8)!=0) {
        if(memcmp(buf,"PARTINFO",8)!=0) {
            lgprintf("�����ļ���(%s.%s)!",dbn,tabname);
            return 0;
        }
        oldversion=true;
    }
    int vh=0;
    fread(&vh,sizeof(int),1,fpartinfo);//_attrnum
    short partnum=0;
    fread(&partnum,sizeof(short),1,fpartinfo);
    bool validname=false;
    int parnum=0;
    bool has_find_MAINPART= false;
    char *pMAINPART = (char*)"MAINPART";
    for(int i=0; i<partnum; i++) {
        memset(buf,0,8);
        fread(buf,1,8,fpartinfo);
        if(memcmp(buf,"PARTPARA",8)!=0) {
            lgprintf("�����ļ���(%s.%s),����(%d)!",dbn,tabname,i+1);
            return 0;
        }
        short parts=0;
        short nl=0;//name len
        fread(&nl,sizeof(short),1,fpartinfo);//save part name len
        fread(buf,1,nl,fpartinfo);// save partname


        if(strncmp(buf,pMAINPART,strlen(pMAINPART)) == 0) { // fix dma-1296
            has_find_MAINPART = true;
        }
        buf[nl]=0;
        if(strcmp(buf,partname)==0) validname=true;
        fread(buf,sizeof(int),7,fpartinfo);//skip
        if(!oldversion) {

            if(b_riak_version) {
                fread(buf,sizeof(long),1,fpartinfo);//skip lastfileid
            } else {
                fread(buf,sizeof(uint),1,fpartinfo);//skip lastfileid
            }

            fread(buf,sizeof(int),1,fpartinfo);//skip savepos
            fread(buf,sizeof(int),1,fpartinfo);//skip lastsavepos
        }

        fread(&vh,sizeof(int),1,fpartinfo);//// files vector number
        if(b_riak_version) {
            fread(buf,sizeof(long),vh,fpartinfo); // skip file list
        } else {
            fread(buf,sizeof(int),vh,fpartinfo); // skip file list
        }

        fread(&vh,sizeof(int),1,fpartinfo);//// part list number

        // skip partlist
        // 3 --->{
        // pack_start
        // pack_end
        // pack_obj
        // }
        fread(buf,sizeof(int),3*vh,fpartinfo);

    }
    fclose(fpartinfo);
    if(!validname) {
        lgprintf("�Ҳ���Ҫɾ���ķ���(%s)!",partname);
        return 0;
    }
    if(has_find_MAINPART&& partnum > 0) { // fix dma-1296
        partnum --;
    }
    if(partnum<2) {
        //lgprintf("��(%s.%s)�������һ������,ʹ�ñ�ɾ��(drop table)!",dbn,tabname);
        //return 0;
        lastPartition = true;
    }

    // ���ֵ���/��� ɾ��
    if(!lastPartition) { // �������һ������(���ڶ�����������)
        for(int idx=0; idx<indexrn; idx++) {
            if(indexrn>1)
                sprintf(targetname,"%s_dma%d",tabname,idx);
            else
                strcpy(targetname,tabname);
            // fill partition parameter control file truncinfo.ctl
            // reading at: attr_partitions::GetTruncatePartInfo(Server)
            sprintf(truncinfo,"%s%s/%s.bht/truncinfo.ctl",psa->GetMySQLPathName(0,"msys"),dbn,targetname);
            FILE *ftruncinfo=fopen(truncinfo,"wb");
            if(ftruncinfo==NULL) {
                lgprintf("�����ļ�'%s'ʧ��!",truncinfo);
                return -1;
            }
            fwrite("TRUNCTRL",1,8,ftruncinfo);
            fwrite(&connectid,sizeof(int),1,ftruncinfo);
            short plen=(short)strlen(partname);
            fwrite(&plen,sizeof(short),1,ftruncinfo);
            fwrite(partname,plen,1,ftruncinfo);
            fclose(ftruncinfo);
            lgprintf("������������:%d ...",idx+1);
            try {
                AutoStmt st(psa->GetDTS());
                // commit internal
                st.ExecuteNC("truncate table %s.%s",dbn,targetname);
            } catch(...) {
                lgprintf("������������:%d ʧ��,�����������־!",idx+1);
                return -1;
            }
        }
        lgprintf("��'%s.%s' ���� '%s' ���ݿ������ѱ�ɾ��.",dbn,tabname,partname);

        //----------------------------------------------------------------
        // ���¼�¼����
        mt.FetchAll("select count(1) ct from %s.%s",dbn,tabname);
        mt.Wait();
        long currentRn=mt.GetLong("ct", 0);

        AutoStmt st(psa->GetDTS());
        st.Prepare(" update dp.dp_table set recordnum=%d where tabid=%d",currentRn,tabid);
        st.Execute(1);
        st.Wait();
    } else { // ���һ������
        char sqlbuf[300];
        sprintf(sqlbuf,"drop table %s.%s",dbn,tabname);
        psa->DoQuery(sqlbuf);
        st.Prepare(" update dp.dp_table set recordnum=0 where tabid=%d",tabid);
        st.Execute(1);
        st.Wait();

        lgprintf("��'%s.%s' ���� '%s' ���ݿ������ѱ�ɾ��.",dbn,tabname,partname);
    }

    //----------------------------------------------------------------
    // ɾ���м���ʱ�ļ���Ϣ
    bool forcedel = true;
    bool parallel_load = CheckParallelLoad();  // �ֲ�ʽ�ϲ����ݰ汾
    if(forcedel) {
        //----------------------------------------------------------------
        // ɾ���������ʱ�ṹ�ļ�
        lgprintf("ɾ��װ������������ļ�.");

        if(!parallel_load) { // ԭ��AP�汾
            mt.FetchFirst("select * from dp.dp_datafilemap where tabid=%d and datapartid=%d",
                          tabid,datapartid);
            rn=mt.Wait();
            int i=0;
            for(i=0; i<rn; i++) {
                char tmp[300];
                lgprintf("ɾ��'%s'�͸��ӵ�depcp,dep5�ļ�",mt.PtrStr("filename",i));
                unlink(mt.PtrStr("filename",i));
                sprintf(tmp,"%s.depcp",mt.PtrStr("filename",i));
                unlink(tmp);
                sprintf(tmp,"%s.dep5",mt.PtrStr("filename",i));
                unlink(tmp);
                lgprintf("ɾ��'%s'�͸��ӵ�depcp,dep5�ļ�",mt.PtrStr("idxfname",i));
                unlink(mt.PtrStr("idxfname",i));
                sprintf(tmp,"%s.depcp",mt.PtrStr("idxfname",i));
                unlink(tmp);
                sprintf(tmp,"%s.dep5",mt.PtrStr("idxfname",i));
                unlink(tmp);
            }
            st.Prepare(" delete from dp.dp_datafilemap where tabid=%d and datapartid=%d",tabid,datapartid);
            st.Execute(1);
            st.Wait();

            // ɾ��������м������������
            lgprintf("ɾ���ɼ���������������ļ�.");
            mt.FetchFirst("select tabid,datafilename,indexfilename from dp.dp_middledatafile where tabid = %d and datapartid=%d",
                          tabid,datapartid);
            rn=mt.Wait();
            for(i=0; i<rn; i++) {
                lgprintf("ɾ���ɼ���ɺ�������ļ�'%s'",mt.PtrStr("datafilename",i));
                unlink(mt.PtrStr("datafilename",i));

                lgprintf("ɾ���ɼ���ɺ�������ļ�'%s'",mt.PtrStr("indexfilename",i));
                unlink(mt.PtrStr("indexfilename",i));
            }
            st.Prepare(" delete from dp.dp_middledatafile where tabid=%d and datapartid=%d",tabid,datapartid);
            st.Execute(1);
            st.Wait();

        } else { // �ֲ�ʽ�ϲ����ݰ汾,ɾ���ϲ���ɺ������
            clear_table_tmp_data(tabid,datapartid);

            st.Prepare(" delete from dp.dp_datapartext where tabid=%d and datapartid=%d",tabid,datapartid);
            st.Execute(1);
            st.Wait();
        }

    }

    lgprintf("��'%s.%s' ���� '%s' ��ɾ�����.",dbn,tabname,partname);

    return lastPartition ? 1:2;

}

// ����ֵ:
// 1:������ṹɾ����
// 2:������ṹɾ������
// 11:��������ṹɾ����
// 12:��������ṹɾ������
int DestLoader::RemoveTable(const int tabid,const int partid,bool exclude_struct)
{

    char dbn[128];
    char tabname[256];

    AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);

    mt.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
    int rn=mt.Wait();
    if(rn<1) ThrowWith("��(%d)��dp_table���Ҳ���!",tabid);

    strcpy(dbn,mt.PtrStr("databasename",0));
    strcpy(tabname,mt.PtrStr("tabname",0));

    mt.FetchFirst("select partfullname from dp.dp_datapart where datapartid = %d and tabid = %d",partid,tabid);
    rn = mt.Wait();
    if(rn <1)ThrowWith("��%s.%s��dp_datapart���Ҳ���!",dbn,tabname);

    // ɾ������
    char  partname[300];
    strcpy(partname,mt.PtrStr("partfullname",0));

    int ret = 0;
    if(!exclude_struct) {
        ret = RemoveTable(dbn,tabname,partname,true,false);
        ret +=10;
    } else {
        ret = RemoveTable_exclude_struct(dbn,tabname,partname);
    }

    return ret;
}


// ����idɾ������
int DestLoader::RemoveTable(const char *dbn,const char *tabname,const int partid,bool prompt)
{
    lgprintf("remove table '%s.%s ' partition id '%d' ...",dbn,tabname,partid);
    AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);

    mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",tabname,dbn);
    int rn=mt.Wait();
    if(rn<1) ThrowWith("��%s.%s��dp_table���Ҳ���!",dbn,tabname);

    int tabid=mt.GetInt("tabid",0);

    mt.FetchFirst("select partfullname from dp.dp_datapart where datapartid = %d and tabid = %d",partid,tabid);
    rn = mt.Wait();
    if(rn <1)ThrowWith("��%s.%s��dp_datapart���Ҳ���!",dbn,tabname);

    // ɾ������
    char  partname[300];
    strcpy(partname,mt.PtrStr("partfullname",0));

    return RemoveTable(dbn,tabname,partname,true,false);

}


//return :
//   0: abort
int DestLoader::RemoveTable(const char *dbn, const char *tabname,const char *partname,bool prompt,bool checkdel)
{
    char sqlbuf[MAX_STMT_LEN];
    char choose[200];
    wociSetEcho(FALSE);
    bool forcedel = false;
    if(checkdel) {
        sprintf(choose,"��ȷ���ñ�'%s.%s'��ǰδ��������Ǩ�Ʋ���? (Y/N)",dbn,tabname);
        forcedel = GetYesNo(choose,false);
        if(!forcedel) {
            return 0;
        }
    }

    lgprintf("remove table '%s.%s ' partition '%s' ...",dbn,tabname,partname);
    AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);

    mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",tabname,dbn);
    int rn=mt.Wait();
    if(rn<1) ThrowWith("��%s.%s��dp_table���Ҳ���!",dbn,tabname);

    int tabid=mt.GetInt("tabid",0);

    mt.FetchFirst("select datapartid from dp.dp_datapart where partfullname = lower('%s') and tabid = %d",partname,tabid);
    rn = mt.Wait();
    if(rn <1)ThrowWith("��%s.%s��dp_datapart���Ҳ���!",dbn,tabname);

    int datapartid = mt.GetInt("datapartid",0);

    mt.FetchAll("select count(1) ct from %s.%s",dbn,tabname);
    mt.Wait();
    long origrecs=mt.GetLong("ct", 0);

    mt.FetchAll("select connection_id() cid");
    mt.Wait();
    int connectid=(int)mt.GetLong("cid",0);

    mt.FetchAll("select * from dp.dp_index where issoledindex>0 and tabid=%d",tabid);
    int indexrn=mt.Wait();
    if(indexrn<1) ThrowWith("��%s.%s��dp_index���Ҳ���!",dbn,tabname);
    char targetname[300];
    if(indexrn>1)
        sprintf(targetname,"%s_dma1",tabname);
    else
        strcpy(targetname,tabname);


    // RIAK �� AP�汾��PART_XXXXX.ctl�ļ���ʽ��һ��
    // fileid ���Ȳ�һ��
    // RIAK:8�ֽ�
    // AP:4�ֽ�
    bool b_riak_version = CheckRiakVersion();


    AutoStmt st(psa->GetDTS());
    //-----------------------------------------------------------
    // ɾ��������Ϣ
    char truncinfo[300];
    sprintf(truncinfo,"%s%s/%s.bht/truncinfo.ctl",psa->GetMySQLPathName(0,"msys"),dbn,targetname);
    FILE *ftruncinfo=fopen(truncinfo,"rb");
    if(ftruncinfo!=NULL) {
        fclose(ftruncinfo);
        lgprintf("�ϴ�ɾ��δ���(%s.%s)!",dbn,tabname);
        return 0;
    }
    char partinfo[300];
    sprintf(partinfo,"%s%s/%s.bht/PART_00000.ctl",psa->GetMySQLPathName(0,"msys"),dbn,targetname);
    FILE *fpartinfo=fopen(partinfo,"rb");
    if(fpartinfo==NULL) {
        lgprintf("��δ����,�޷�ɾ��(%s.%s)!",dbn,tabname);
        return 0;
    }

    // parse partition info.
    char buf[1000];
    bool lastPartition = false;

    fread(buf,1,8,fpartinfo);
    bool oldversion=false;
    if(memcmp(buf,"PARTIF15",8)!=0) {
        if(memcmp(buf,"PARTINFO",8)!=0) {
            lgprintf("�����ļ���(%s.%s)!",dbn,tabname);
            return 0;
        }
        oldversion=true;
    }
    int vh=0;
    fread(&vh,sizeof(int),1,fpartinfo);//_attrnum
    short partnum=0;
    fread(&partnum,sizeof(short),1,fpartinfo);
    bool validname=false;
    int parnum=0;
    bool has_find_MAINPART= false;
    char *pMAINPART = (char*)"MAINPART";
    for(int i=0; i<partnum; i++) {
        memset(buf,0,8);
        fread(buf,1,8,fpartinfo);
        if(memcmp(buf,"PARTPARA",8)!=0) {
            lgprintf("�����ļ���(%s.%s),����(%d)!",dbn,tabname,i+1);
            return 0;
        }
        short parts=0;
        short nl=0;//name len
        fread(&nl,sizeof(short),1,fpartinfo);//save part name len
        fread(buf,1,nl,fpartinfo);// save partname


        if(strncmp(buf,pMAINPART,strlen(pMAINPART)) == 0) { // fix dma-1296
            has_find_MAINPART = true;
        }
        buf[nl]=0;
        if(strcmp(buf,partname)==0) validname=true;
        fread(buf,sizeof(int),7,fpartinfo);//skip
        if(!oldversion) {
            if(b_riak_version) {
                fread(buf,sizeof(long),1,fpartinfo);//skip lastfileid
            } else {
                fread(buf,sizeof(uint),1,fpartinfo);//skip lastfileid
            }
            fread(buf,sizeof(int),1,fpartinfo);//skip savepos
            fread(buf,sizeof(int),1,fpartinfo);//skip lastsavepos
        }
        fread(&vh,sizeof(int),1,fpartinfo);//// files vector number
        if(b_riak_version) {
            fread(buf,sizeof(long),vh,fpartinfo); // skip file list
        } else {
            fread(buf,sizeof(int),vh,fpartinfo); // skip file list
        }
        fread(&vh,sizeof(int),1,fpartinfo);//// part list number
        fread(buf,sizeof(int),3*vh,fpartinfo);//// skip partlist

    }
    fclose(fpartinfo);
    if(!validname) {
        lgprintf("�Ҳ���Ҫɾ���ķ���(%s)!",partname);
        return 0;
    }
    if(has_find_MAINPART&& partnum > 0) { // fix dma-1296
        partnum --;
    }
    if(partnum<2) {
        //lgprintf("��(%s.%s)�������һ������,ʹ�ñ�ɾ��(drop table)!",dbn,tabname);
        //return 0;
        lastPartition = true;
    }
    if(prompt) {
        sprintf(choose,"��'%s.%s'�ķ���'%s'����ɾ����ɾ��ǰ���еļ�¼����:%lld. ȷ��ɾ�� ?(Y/N)",dbn,tabname,partname,origrecs);
        if(checkdel) {
            if(!GetYesNo(choose,false)) {
                lgprintf("ȡ��ɾ���� ");
                return 0;
            }
        }
    }
    // ���ֵ���/��� ɾ��
    if(!lastPartition) { // �������һ������(���ڶ�����������)
        for(int idx=0; idx<indexrn; idx++) {
            if(indexrn>1)
                sprintf(targetname,"%s_dma%d",tabname,idx);
            else
                strcpy(targetname,tabname);
            // fill partition parameter control file truncinfo.ctl
            // reading at: attr_partitions::GetTruncatePartInfo(Server)
            sprintf(truncinfo,"%s%s/%s.bht/truncinfo.ctl",psa->GetMySQLPathName(0,"msys"),dbn,targetname);
            FILE *ftruncinfo=fopen(truncinfo,"wb");
            if(ftruncinfo==NULL) {
                lgprintf("�����ļ�'%s'ʧ��!",truncinfo);
                return -1;
            }
            fwrite("TRUNCTRL",1,8,ftruncinfo);
            fwrite(&connectid,sizeof(int),1,ftruncinfo);
            short plen=(short)strlen(partname);
            fwrite(&plen,sizeof(short),1,ftruncinfo);
            fwrite(partname,plen,1,ftruncinfo);
            fclose(ftruncinfo);
            lgprintf("������������:%d ...",idx+1);
            try {
                AutoStmt st(psa->GetDTS());
                // commit internal
                st.ExecuteNC("truncate table %s.%s",dbn,targetname);
            } catch(...) {
                lgprintf("������������:%d ʧ��,�����������־!",idx+1);
                return -1;
            }
        }
        lgprintf("��'%s.%s' ���� '%s' ���ݿ������ѱ�ɾ��.",dbn,tabname,partname);

        //----------------------------------------------------------------
        // ���¼�¼����
        mt.FetchAll("select count(1) ct from %s.%s",dbn,tabname);
        mt.Wait();
        long currentRn=mt.GetLong("ct", 0);

        AutoStmt st(psa->GetDTS());
        st.Prepare(" update dp.dp_table set recordnum=%d where tabid=%d",currentRn,tabid);
        st.Execute(1);
        st.Wait();
    } else { // ���һ������
        char sqlbuf[300];
        sprintf(sqlbuf,"drop table %s.%s",dbn,tabname);
        psa->DoQuery(sqlbuf);
        st.Prepare(" update dp.dp_table set recordnum=0 where tabid=%d",tabid);
        st.Execute(1);
        st.Wait();

        lgprintf("��'%s.%s' ���� '%s' ���ݿ������ѱ�ɾ��.",dbn,tabname,partname);
    }

    //----------------------------------------------------------------
    // ɾ���м���ʱ�ļ���Ϣ
    sprintf(choose,"�Ƿ�ɾ����'%s.%s'�Ĵ�װ���ļ�(dp.dp_datafilemap/dp.dp_middledatafile)? (Y/N)",dbn,tabname);

    bool parallel_load = CheckParallelLoad();  // �ֲ�ʽ�ϲ����ݰ汾

    forcedel = true;
    if(checkdel) {
        forcedel = GetYesNo(choose,false);
    }
    if(forcedel) {
        //----------------------------------------------------------------
        // ɾ���������ʱ�ṹ�ļ�
        lgprintf("ɾ��װ������������ļ�.");

        if(!parallel_load) {

            mt.FetchFirst("select * from dp.dp_datafilemap where tabid=%d and datapartid=%d",
                          tabid,datapartid);
            rn=mt.Wait();
            int i=0;
            for(i=0; i<rn; i++) {
                char tmp[300];
                lgprintf("ɾ��'%s'�͸��ӵ�depcp,dep5�ļ�",mt.PtrStr("filename",i));
                unlink(mt.PtrStr("filename",i));
                sprintf(tmp,"%s.depcp",mt.PtrStr("filename",i));
                unlink(tmp);
                sprintf(tmp,"%s.dep5",mt.PtrStr("filename",i));
                unlink(tmp);
                lgprintf("ɾ��'%s'�͸��ӵ�depcp,dep5�ļ�",mt.PtrStr("idxfname",i));
                unlink(mt.PtrStr("idxfname",i));
                sprintf(tmp,"%s.depcp",mt.PtrStr("idxfname",i));
                unlink(tmp);
                sprintf(tmp,"%s.dep5",mt.PtrStr("idxfname",i));
                unlink(tmp);
            }
            st.Prepare(" delete from dp.dp_datafilemap where tabid=%d and datapartid=%d",tabid,datapartid);
            st.Execute(1);
            st.Wait();

            // ɾ��������м������������
            lgprintf("ɾ���ɼ���������������ļ�.");
            mt.FetchFirst("select tabid,datafilename,indexfilename from dp.dp_middledatafile where tabid = %d and datapartid=%d",
                          tabid,datapartid);
            rn=mt.Wait();
            for(i=0; i<rn; i++) {
                lgprintf("ɾ���ɼ���ɺ�������ļ�'%s'",mt.PtrStr("datafilename",i));
                unlink(mt.PtrStr("datafilename",i));

                lgprintf("ɾ���ɼ���ɺ�������ļ�'%s'",mt.PtrStr("indexfilename",i));
                unlink(mt.PtrStr("indexfilename",i));
            }
            st.Prepare(" delete from dp.dp_middledatafile where tabid=%d and datapartid=%d",tabid,datapartid);
            st.Execute(1);
            st.Wait();

        } else { // �ֲ�ʽ�汾,ɾ���ϲ����ݼ�¼
            clear_has_merged_data(tabid,datapartid);

            st.Prepare(" delete from dp.dp_datapartext where tabid=%d and datapartid=%d",tabid,datapartid);
            st.Execute(1);
            st.Wait();
        }

    }

    sprintf(choose,"��'%s.%s'�����ò���(������Ϣ<dp.dp_datapart>,������Ϣ<dp.dp_index>,��չ�ֶ���Ϣ<dp.dp_column_info>,��ṹ��Ϣ<dp.dp_table>)�Ƿ�ɾ��?(Y/N)",dbn,tabname);
    forcedel = true;
    if(checkdel) {
        forcedel=GetYesNo(choose,false);
    }
    if(forcedel) {
        st.Prepare(" delete from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
        st.Execute(1);
        st.Wait();

        // ���û������������ɾ���ṹ��Ϣ
        if(lastPartition) { // û����������������ɾ����ṹ
            st.Prepare(" delete from dp.dp_index where tabid=%d",tabid);
            st.Execute(1);
            st.Wait();

            st.Prepare(" delete from dp.dp_column_info where table_id=%d",tabid);
            st.Execute(1);
            st.Wait();

            st.Prepare(" delete from dp.dp_table where tabid=%d",tabid);
            st.Execute(1);
            st.Wait();
        }

        // dp_log
        st.Prepare(" delete from dp.dp_log where tabid=%d and datapartid=%d",tabid,datapartid);
        st.Execute(1);
        st.Wait();

        // dp_filelog
        st.Prepare("delete from dp.dp_filelog where tabid=%d and datapartid=%d",tabid,datapartid);
        st.Execute(1);
        st.Wait();
    }

    lgprintf("��'%s.%s' ���� '%s' ��ɾ�����.",dbn,tabname,partname);
    if(lastPartition) {
        lgprintf("��'%s.%s' ��ɾ�����.",dbn,tabname);
    }
    return lastPartition?1:2;
}


//-------------------------------------------------------------------------------
// �ж��ļ��Ƿ����
bool BackupTables::DoesFileExist(const char* pfile)
{
    struct stat stat_info;
    return (0 == stat(pfile, &stat_info));
}

// tar�ļ��еĴ���б��ļ�����:  dbname_tablename.lst
void BackupTables::GetTarfileLst(const char* dbn,const char* tabname,char* tarfilelst)
{
    sprintf(tarfilelst,"%s.%s.lst",dbn,tabname);
}

bool BackupTables::DoesTableExist(const char* dbn,const char* tabname)
{
    const char* pbasepth = psa->GetMySQLPathName(0,"msys");
    char strTablectl[500];
    sprintf(strTablectl,"%s/%s/%s.bht/Table.ctb",pbasepth,dbn,tabname);
    return DoesFileExist(strTablectl);
}

// �ж��Ƿ����rsi��Ϣ
bool BackupTables::DoesRsiExist(const char* filepattern)
{
    const char* parttern= filepattern;
    glob_t globbuf;
    memset(&globbuf,0,sizeof(globbuf));
    globbuf.gl_offs = 0;
    //GLOB_NOSORT  Don��t sort the returned pathnames.  The only reason to do this is to save processing time.  By default, the returned pathnames are sorted.
    if(!glob(parttern, GLOB_DOOFFS, NULL, &globbuf)) {
        if(globbuf.gl_pathc>0) {
            globfree(&globbuf);
            return true;
        }
    }
    globfree(&globbuf);
    return false;
}

int  BackupTables::GetSeqId(const char* pfile,int & tabid)
{
    // ���ļ��б����ļ��б��Ƿ��tar���е�һ��
    FILE* pFile = fopen(pfile,"rt");
    if(NULL == pFile) {
        ThrowWith("Open file %s error \n",pfile);
        return -1;
    }
    char lines[300];
    fgets(lines,300,pFile);
    tabid = atoi(lines);
    fclose(pFile);
}
int BackupTables::UpdateSeqId(const char* pfile,const int tabid)
{
    FILE* pFile = fopen(pfile,"wt");
    if(NULL == pFile) {
        ThrowWith("Open file %s error \n",pfile);
        return -1;
    }
    fprintf(pFile,"%d",tabid);
    fclose(pFile);
}

int BackupTables::getTableId(const char* pth,int & tabid)
{
    FILE  *pFile  = fopen(pth,"rb");
    if(!pFile) {
        ThrowWith("�ļ�%s��ʧ��!",pth);
    }
    fseek(pFile,-4,SEEK_END);
    int _tabid = 0;
    fread(&_tabid,4,1,pFile);
    fclose(pFile);

    tabid = _tabid;
    return tabid;
}

// ��ȡ$DATAMERGER_HOME/var/database/table.bht/Table.ctb �е����4�ֽ�
int BackupTables::GetTableId(const char* pbasepth,const char *dbn,const char *tabname,int & tabid)
{
    char strTablectl[500];
    sprintf(strTablectl,"%s/%s/%s.bht/Table.ctb",pbasepth,dbn,tabname);
    if(!DoesFileExist(strTablectl)) {
        ThrowWith("���ݿ��%s.%s ������!",dbn,tabname);
    }
    return  getTableId(strTablectl,tabid);
}

int BackupTables::updateTableId(const char* pth,const int tabid)
{
    FILE  *pFile  = fopen(pth,"r+b");
    if(!pFile) {
        ThrowWith("�ļ�%s��ʧ��!",pth);
    }
    fseek(pFile,4,SEEK_END);
    fwrite(&tabid,4,1,pFile);;
    fclose(pFile);
    pFile = NULL;
    return 0;
}

// ��*.bht/table.ctb�ļ�������д
// 1> *.bht/table.ctb ----> *.bht/old_table.ctb
// 2> *.bht/old_table.ctb + tabid + tabname ------> *.bht/table.ctb
bool BackupTables::GenerateNew_table_tcb(const char* pth,const char* pnewtabname,const int tabid)
{
    char cmd[300];

    char old_name[300];
    sprintf(old_name,"%s/old_Table.ctb",pth);
    sprintf(cmd,"mv %s/Table.ctb %s",pth,old_name);
    int ret = system(cmd);
    assert(-1 != ret);

    char new_name[300];
    sprintf(new_name,"%s/Table.ctb",pth);

    FILE* pFile;
    pFile = fopen(old_name,"rb");
    assert(pFile != NULL);

    char buf[8192] = {0};
    char *b = (char*)buf;
    ret = fread(buf,1,8192,pFile);

    assert(ret > 0);
    fclose(pFile);

    char *bend = b + ret; // end buf
    int no_attr =   *(int*)(b+25);

    //------------------------------------------
    char new_buf[8192] = {0};
    char *nb = (char*)new_buf;
    int nb_len = 0;

    memcpy(nb,b,33);
    b += 33;
    nb_len += 33;
    nb += 33;

    // copy column
    memcpy(nb,b,no_attr);
    b += no_attr;
    nb_len += no_attr;
    nb += no_attr;

    // new table name
    strcpy(nb,pnewtabname);

    nb_len += strlen(pnewtabname);
    nb += strlen(pnewtabname);

    *nb = 0;
    nb_len += 1;
    nb += 1;

    // table desc
    char *p_old_table_desc = b+strlen(b);
    strcpy(nb,p_old_table_desc);

    nb_len += strlen(p_old_table_desc);
    nb += strlen(p_old_table_desc);

    *nb = 0;
    nb_len += 1;
    nb += 1;

    // copy  special blocks : 6 bytes
    memcpy(nb,bend-10,6);
    nb_len += 6;
    nb+=6;

    // copy tabld id
    memcpy(nb,&tabid,4);
    nb_len += 4;

    // set block_offset
    *(int*)(new_buf+29) = nb_len-10;

    //--------------------------------------------------------
    // write new table.tcb
    pFile = fopen(new_name,"wb");
    assert(pFile != NULL);
    ret = fwrite(new_buf,1,nb_len,pFile);
    assert(ret == nb_len);
    fclose(pFile);

    sprintf(cmd,"rm %s -rf",old_name);
    system(cmd);

    return true;
}

// ��ȡ$DATAMERGER_HOME/var/database/table.bht/Table.ctb �е����4�ֽ�
int BackupTables::UpdateTableId(const char* pbasepth,const char *dbn,const char *tabname,const int tableid)
{
    char strTablectl[500];
    sprintf(strTablectl,"%s/%s/%s.bht/Table.ctb",pbasepth,dbn,tabname);
    if(!DoesFileExist(strTablectl)) {
        ThrowWith("���ݿ��%s.%s ������!",dbn,tabname);
    }

    updateTableId(strTablectl,tableid);

    return 0;
}

int BackupTables::RenameTable(const char *src_dbn,const char* src_tbn,const char* dst_dbn,const char* dst_tbn)
{
    const char* pbasepth = psa->GetMySQLPathName(0,"msys");

    //0. �ж����ݿ���Ƿ����
    lgprintf("rename table '%s.%s ' to  '%s.%s' ...",src_dbn,src_tbn,dst_dbn,dst_tbn);
    AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);

    mt.FetchAll("select tabid from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",src_tbn,src_dbn);
    int rn=mt.Wait();
    if(rn<1) ThrowWith("��%s.%s��dp_table���Ҳ���!",src_dbn,src_tbn);

    char choose[300];
    sprintf(choose,"��ȷ���ñ�'%s.%s'��ǰδ��������Ǩ�Ʋ����Ͳ�ѯ����? (Y/N)",src_dbn,src_tbn);
    bool forcedel = false;
    forcedel = GetYesNo(choose,false);
    if(!forcedel) {
        return 0;
    }

    // ���ı�����
    int tabid = mt.GetInt(0,0);
    AutoStmt st(psa->GetDTS());
    st.DirectExecute("update dp.dp_table set tabdesc='%s' ,tabname='%s',databasename='%s' where tabid = %d ",dst_tbn,dst_tbn,dst_dbn,tabid);

    // 1. ��ȡԴ��id
    GetTableId(pbasepth,src_dbn,src_tbn,tabid);

    // 2. �ж�RSI��Ϣ�ļ��Ƿ����
    char rsi_parttern[300] = {0};
    bool exist_rsi = true;
    sprintf(rsi_parttern,"%s/BH_RSI_Repository/????.%d.*.rsi",pbasepth,tabid);
    if(!DoesRsiExist(rsi_parttern)) {
        // �ձ�����RSI�ļ�����
        lgprintf("[����]��%s.%s��RSI�ļ�������!",src_dbn,src_tbn);
        exist_rsi = false;
    }

    // 3. �±�id��ȡ
    int seqid = 0;
    int dst_tabid = 0;
    char seq_file[300];
    sprintf(seq_file,"%s/brighthouse.seq",pbasepth);
    GetSeqId(seq_file,seqid);
    seqid = seqid +1;
    dst_tabid= seqid;
    seqid +=1;
    UpdateSeqId(seq_file,seqid);

    // 4. �ƶ��ļ�Ŀ¼
    char cmd[1000];
    int ret = 0;

    // 4.1 mv $DATAMERGER_HOME/var/src_dbn/src_tbn.frm  $DATAMERGER_HOME/var/dst_dbn/dst_tbn.frm
    sprintf(cmd,"mv %s/%s/%s.frm  %s/%s/%s.frm",pbasepth,src_dbn,src_tbn,pbasepth,dst_dbn,dst_tbn);
    lgprintf("rename����:%s\n",cmd);
    ret = system(cmd);
    if(ret == -1) {
        ThrowWith("rename ����: mv %s/%s/%s.frm  %s/%s/%s.frm ʧ��!",pbasepth,src_dbn,src_tbn,pbasepth,dst_dbn,dst_tbn);
    }

    // 4.2 mv $DATAMERGER_HOME/var/src_dbn/src_tbn.bht  $DATAMERGER_HOME/var/dst_dbn/dst_tbn.bht
    sprintf(cmd,"mv %s/%s/%s.bht  %s/%s/%s.bht",pbasepth,src_dbn,src_tbn,pbasepth,dst_dbn,dst_tbn);
    lgprintf("rename����:%s\n",cmd);
    ret = system(cmd);
    if(ret == -1) {
        ThrowWith("rename ����: mv %s/%s/%s.bht  %s/%s/%s.bht ʧ��!",pbasepth,src_dbn,src_tbn,pbasepth,dst_dbn,dst_tbn);
    }

    // 4.3 ���±����ƺ�tabid�� $DATAMERGER_HOME/var/dst_dbn.bht/Table.ctb
    char new_path[300];
    sprintf(new_path,"%s/%s/%s.bht",pbasepth,dst_dbn,dst_tbn);
    lgprintf("rename����:����%sĿ¼�еı�����id.\n",new_path);
    GenerateNew_table_tcb(new_path,dst_tbn,dst_tabid);

    // 4.4 ����rsi�е�tabid��$DATAMERGER_HOME/var/BH_RSI_Repository/????.tabid.%d*.rsi �滻
    if(exist_rsi) {
        char tmp_rsi_path[300];
        sprintf(tmp_rsi_path,"%s/BH_RSI_Repository",pbasepth);
        lgprintf("rename����:����%sĿ¼�е�ris�ļ��ı�id.\n",tmp_rsi_path);
        char tmp_rsi_pattern[300];
        sprintf(tmp_rsi_pattern,"????.%d.*.rsi",tabid);
        UpdateRsiID(tmp_rsi_path,dst_tabid,tmp_rsi_pattern);
    }

    lgprintf("rename����:rename table [%s.%s] -->[%s.%s] �ɹ�.\n",src_dbn,src_tbn,dst_dbn,dst_tbn);

    return 0;
}


// ���ݱ�,��dbn���ݿ�ı�tabname�����б��ݣ����ݺ���ļ�: bkpath/dbn_tabname.pid.tar
//  tar -C /app/dma/var/zkb/ �ı�Ŀ¼
int BackupTables::BackupTable(const char *dbn,const char *tabname,const char *bkpath)
{
    const char* pbasepth = psa->GetMySQLPathName(0,"msys");

    // 1. ��ȡ��Ӧ��ID
    int tabid = 0;
    GetTableId(pbasepth,dbn,tabname,tabid);

    // 2. �ж�RSI��Ϣ�ļ��Ƿ����
    char rsi_parttern[300] = {0};
    sprintf(rsi_parttern,"%s/BH_RSI_Repository/????.%d.*.rsi",pbasepth,tabid);
    if(!DoesRsiExist(rsi_parttern)) {
        ThrowWith("��%s.%s��RSI�ļ�������!",dbn,tabname);
    }

    // 3. ��������·����tar������
    char tarfile[500];
    sprintf(tarfile,"%s.%s.%d.tar",dbn,tabname,getpid());

    // 4. ��tar���м����ļ�
    char cmd[1000];
    int ret = 0;

    // 4.1 tar����׷���ļ� $DATAMERGER_HOME/var/database/table.frm
    sprintf(cmd,"tar -C %s/%s -c -f %s %s.frm",pbasepth,dbn,tarfile,tabname);
    lgprintf("���ݹ���:%s\n",cmd);
    ret = system(cmd);
    assert(ret != -1);

    // 4.2 tar����׷���ļ� $DATAMERGER_HOME/var/database/table.bht
    char _pattern[30000];
    sprintf(_pattern,"tar -C %s -r -f %s %s/%s.bht",pbasepth,tarfile,dbn,tabname);

    std::string tmp;
    sprintf(cmd,"%s/BH_RSI_Repository/????.%d.*.rsi",pbasepth,tabid);
    string_vector rsi_file_vector;
    GetPatternFile(cmd,rsi_file_vector,tmp);
    for(int i=0; i<rsi_file_vector.size(); i++) {
        sprintf(_pattern+strlen(_pattern)," BH_RSI_Repository/%s",rsi_file_vector[i].c_str());
    }
    lgprintf("���ݹ���:%s\n",_pattern);
    ret = system(_pattern);
    assert(ret != -1);

    // 5. ���ɴ���б��ļ� tar tf xxxx >> database_table.lst
    char tarfile_lst[256];
    GetTarfileLst(dbn,tabname,tarfile_lst);
    sprintf(cmd,"tar -t -f %s > %s",tarfile,tarfile_lst);
    lgprintf("���ݹ���:%s\n",cmd);
    ret = system(cmd);
    assert(ret != -1);

    // 5.1 ��database_table.lst�ϲ���tar����ȥ
    sprintf(cmd,"tar -r -f %s %s",tarfile,tarfile_lst);
    lgprintf("���ݹ���:%s\n",cmd);
    ret = system(cmd);
    assert(ret != -1);

    // 5.2 ɾ��������database_table.lst�ļ�
    sprintf(cmd,"rm %s -rf",tarfile_lst);
    lgprintf("���ݹ���:%s\n",cmd);
    ret = system(cmd);
    assert(ret != -1);

    // 5.3 �ƶ�tar�ļ���ָ��Ŀ¼
    sprintf(cmd,"mv %s %s",tarfile,bkpath);
    lgprintf("���ݹ���:%s\n",cmd);
    ret = system(cmd);
    assert(ret != -1);

    lgprintf("���ݿ�:%s ��:%s ���ݳɹ������ݵ�·��:%s/%s \n",dbn,tabname,bkpath,tarfile);
    return 0;
}

// ��ȡ����ƥ���������ļ����ƴ��ڶ���
int BackupTables::GetPatternFile(const char* filepattern,string_vector& rsi_file_vec,std::string &old_dbname)
{
    rsi_file_vec.clear();
    glob_t globbuf;
    memset(&globbuf,0,sizeof(globbuf));
    globbuf.gl_offs = 0;
    //GLOB_NOSORT  Don��t sort the returned pathnames.  The only reason to do this is to save processing time.  By default, the returned pathnames are sorted.
    if(!glob(filepattern, GLOB_DOOFFS, NULL, &globbuf)) {
        for(int i=0; i<globbuf.gl_pathc; i++) {
            const char* pstart = globbuf.gl_pathv[i];
            const char* pend = pstart + strlen(pstart) -1;
            while(pend>pstart) {
                if(*pend == '/') {
                    break;
                }
                pend--;
            }
            pend++;
            rsi_file_vec.push_back(std::string(pend));

            //  // �����ƻ�ȡ
            if(i==0) {
                int _pos = 0;
                char _nm[100];
                pend--; // ȥ��/
                pend--;
                while(pend>pstart) {
                    if(*pend == '/') {
                        break;
                    }
                    pend--;
                    _pos ++;
                }
                pend++;
                strncpy(_nm,pend,_pos);
                _nm[_pos] = 0;
                old_dbname = std::string(_nm);
            }
        }
    }
    globfree(&globbuf);
    return rsi_file_vec.size();
}


// ��黹ԭ���Ƿ����
// tmppth:$DATAMERGER_HOME/dbn_tabname_pid_tmp
int  BackupTables::CheckRestorePackageOK(const char* tmppth,char* tarfilelst)
{
    // list file vector
    string_vector tar_file_list;

    int tmppth_len = strlen(tmppth);
    char cmd[300];
    // find /tmp/ljs_test > file.pid.lst
    char tmp_file_lst[100];
    sprintf(tmp_file_lst,"tmp_file_%d.lst",getpid());

    sprintf(cmd,"find %s > %s",tmppth,tmp_file_lst);
    int ret = 0;
    ret = system(cmd);
    assert(-1 != ret);

    // ���ļ�tmp_fle_lst�����ڲ���¼�����б���
    {
        FILE* pFile = fopen(tmp_file_lst,"rt");
        if(NULL == pFile) {
            return -1;
        }
        char lines[300];
        while(fgets(lines,300,pFile)!=NULL) {
            int sl=strlen(lines);
            if(lines[sl-1]=='\n') lines[sl-1]=0;
            char *pfn=lines;
            if(strlen(lines) <= tmppth_len) {
                continue; // ����Ŀ¼��
            }

            // ����tmppth·������ ��ͷ�� '/'
            pfn += tmppth_len+1;

            // �������
            tar_file_list.push_back(std::string(pfn));
        }

        fclose(pFile);
    }
    sprintf(cmd,"rm %s -rf",tmp_file_lst);
    ret = system(cmd);
    assert(-1 != ret);

    // ���ļ��б����ļ��б��Ƿ��tar���е�һ��
    sprintf(cmd,"%s/%s",tmppth,tarfilelst);
    FILE* pFile = fopen(cmd,"rt");
    if(NULL == pFile) {
        return -1;
    }
    char lines[300];
    while(fgets(lines,300,pFile)!=NULL) {
        int sl=strlen(lines);
        if(lines[sl-1]=='\n') lines[sl-1]=0;
        sl = strlen(lines);
        if(lines[sl-1]=='/') lines[sl-1]=0;
        char *pfn=lines;

        // ��tar_file_list �н��в�֤
        bool  find_flag = false;
        for(int i=0; i<tar_file_list.size(); i++) {
            if(strcmp(tar_file_list[i].c_str(),pfn) == 0) {
                find_flag = true;
                break;
            }
        }
        if(!find_flag) {
            fclose(pFile);
            return -2;
        }
    }

    fclose(pFile);

    return 0;
}

// ����ָ��Ŀ¼�µ�*.rsi�ļ��ı�id
// ????.%d.*.rsi �ļ��滻��ͨ�� %d �ı�id
#define RSI_FILE_HEAD_LEN  4   // "CMAP." or  "HIST."
#define RSI_POS         '.'
bool BackupTables::UpdateRsiID(const char* pth,const int tabid,const char* pattern)
{
    char rsi_parttern[300] = {0};
    // tabid�Ѿ����ˣ���������µ�id
    //sprintf(rsi_parttern,"%s/????.%d.*.rsi",pth,tabid);
    if(pattern == NULL) {
        sprintf(rsi_parttern,"%s/*",pth);
    } else {
        sprintf(rsi_parttern,"%s/%s",pth,pattern);
    }

    char path_head[300];
    sprintf(path_head,"%s",pth);
    int path_head_len = strlen(path_head)+1;  // ������/���ĳ���

    glob_t globbuf;
    memset(&globbuf,0,sizeof(globbuf));
    globbuf.gl_offs = 0;
    //GLOB_NOSORT  Don��t sort the returned pathnames.  The only reason to do this is to save processing time.  By default, the returned pathnames are sorted.
    if(!glob(rsi_parttern, GLOB_DOOFFS, NULL, &globbuf)) {
        for(int i=0; i<globbuf.gl_pathc; i++) {
            char *poldname = globbuf.gl_pathv[i];

            std::string strname(poldname);
            int str_len = strname.size();
            // ��ȡCMAP ����HIST
            std::string strhead = strname.substr(path_head_len,RSI_FILE_HEAD_LEN);

            // %d.*.rsi �Ľ�ȡ
            int  _tmp1 = path_head_len+RSI_FILE_HEAD_LEN + 1;  // '.' һ���ַ�
            int  _tmp2 = str_len-_tmp1; //  %d.*.rsi �ĳ���

            std::string strtmp = strname.substr(_tmp1,_tmp2);

            // *.rsi�Ľ�ȡ
            int pos = strtmp.find(RSI_POS);
            str_len = strtmp.size();
            strtmp = strtmp.substr(pos+1);  // get 'colid.rsi'

            // get new filename by new tabid
            char pnewname[300];
            // %s/BH_RSI_Repository/????.%d.*.rsi
            sprintf(pnewname,"%s/%s.%d.%s",pth,strhead.c_str(),tabid,strtmp.c_str());

            // �ļ�����������  poldname ---> pnewname
            char cmd[300];
            sprintf(cmd,"mv %s %s",poldname,pnewname);
            int ret = system(cmd);
            assert(ret != -1);
        }
    }
    globfree(&globbuf);
    return true;
}

bool BackupTables::ClearRestorePath(const char* pth,bool success_flag)
{
    char cmd[300];
    sprintf(cmd,"rm %s -rf",pth);
    if(!success_flag) {
        lgprintf("��ԭ����:��ԭʧ�ܣ����Ŀ¼%s����.",pth);
    } else {
        lgprintf("��ԭ����:��ԭ�ɹ������Ŀ¼%s����.",pth);
    }
    system(cmd);
    return true;
}

// ����ع�Ŀ¼
// ���ݿ�����Ŀ¼�ļ������tabid != 0 ���RSI�ļ�
bool BackupTables::RollbackRestore(const char* pth,const char* dbn,const char* tabname,const int tabid)
{
    char tmp[300];
    char cmd[300];

    if(tabname != NULL) {
        sprintf(tmp,"%s/%s/%s",pth,dbn,tabname);
        lgprintf("��ԭ����:�ع����������Ŀ¼%s����.",tmp);
        sprintf(cmd,"rm %s -rf",tmp);
        system(cmd);
    }

    if(tabid > 0) {
        sprintf(tmp,"%s/BH_RSI_Repository/????.%d.*.rsi",pth,tabid);
        lgprintf("��ԭ����:�ع����������%s����.",tmp);
        sprintf(cmd,"rm %s -rf",tmp);
        system(cmd);
    }

    return true;
}


// ��ԭ�����,��bkfile��Ӧ���ļ�����ԭ��dbn���ݿ��tabname����ȥ
//  tar -C /app/dma/var/zkb/ �ı�Ŀ¼
int  BackupTables::RestoreTable(const char* dbn,const char* tabname,const char* bkfile)
{
    // 1. �ж�Ҫ��ԭ�ı��Ƿ���ڣ�����Ѿ��������ܱ���ԭ
    if(!DoesFileExist(bkfile)) {
        ThrowWith("�ļ�:%s�����ڣ��޷���ɻ�ԭ����!\n",bkfile);
    }
    if(DoesTableExist(dbn,tabname)) {
        lgprintf("���ݿ�:%s,��:%s�Ѿ����ڣ�����ɾ������ڻ�ԭ��!\n",dbn,tabname);
        return 0;
    }

    const char* pbasepth = psa->GetMySQLPathName(0,"msys");
    int ret  = 0;
    char cmd[300];
    char tmp[300];

    // 2. ������ԭ����ʱĿ¼$DATAMERGER_HOME/dbn.tabname_pid_tmp
    char restore_tmp_pth[300];
    sprintf(restore_tmp_pth,"%s/%s.%s_%d_tmp",pbasepth,dbn,tabname,getpid());
    sprintf(cmd,"mkdir %s",restore_tmp_pth);
    lgprintf("��ԭ����:%s\n",cmd);
    ret = system(cmd);
    assert(ret != -1);

    // 3. ��ȡbkfile ���������ļ�����ʱĿ¼$DATAMERGER_HOME/dbn_tabname_pid_tmp ��
    sprintf(cmd,"cd %s",restore_tmp_pth);
    ret = system(cmd);
    assert(ret != -1);

    lgprintf("��ԭ����:%s\n",cmd);

    sprintf(cmd,"tar -C %s -x -f %s",restore_tmp_pth,bkfile);
    lgprintf("��ԭ����:%s\n",cmd);
    if(-1== system(cmd)) {
        lgprintf("��ԭ����ʧ��!\n");
        ClearRestorePath(restore_tmp_pth);
        return -1;
    }

    // 4. У�����ݰ��Ƿ�������ȷ��ͨ��database_table.lst ����ȡ
    char database_table_lst[300];
    char old_tabname[300];
    std::string old_dbname="";
    sprintf(tmp,"%s/*.lst",restore_tmp_pth);
    string_vector lst_file_vec;
    GetPatternFile(tmp,lst_file_vec,old_dbname);
    if(lst_file_vec.size()>0) {
        strcpy(database_table_lst,lst_file_vec[0].c_str());
    } else {
        lgprintf("��ԭ����:��ȡ�ļ��б�[%s]ʧ��!\n",tmp);
        ClearRestorePath(restore_tmp_pth);
        return -1;
    }

    lgprintf("��ԭ����:������ݰ�%s �Ƿ���ȷ...\n",bkfile);
    ret = CheckRestorePackageOK(restore_tmp_pth,database_table_lst);
    if(-1 == ret) {
        lgprintf("��ԭ����:���б��ļ�:%s  ʧ��!\n",database_table_lst);
        ClearRestorePath(restore_tmp_pth);
        return -1;
    } else if( -2 == ret) {
        lgprintf("��ԭ����:���ݰ�:%s  ���ʧ��!\n",bkfile);
        ClearRestorePath(restore_tmp_pth);
        return -1;
    }
    lgprintf("��ԭ����:������ݰ�%s ���.\n",bkfile);


    // 5. �����Ҫ����������޸ı�����
    sprintf(tmp,"%s/*/*.bht",restore_tmp_pth);
    lst_file_vec.clear();
    GetPatternFile(tmp,lst_file_vec,old_dbname);
    if(lst_file_vec.size()>0) {
        strcpy(old_tabname,lst_file_vec[0].c_str());
        char *p = old_tabname;
        while(*p) { // ȥ��.bht
            if(*p == '.') {
                *p = 0;
                break;
            }
            p++;
        }
    } else {
        lgprintf("��ԭ����:��ȡԭ��Ŀ¼[%s]ʧ��!\n",tmp);
        ClearRestorePath(restore_tmp_pth);
        return -1;
    }

    // 6. ���ļ��л�ȡ��id������brighthouse.seq�л�ȡid��ʹ��brighthouse.seq�е�id��Ϊ���µ�id
    int tabid=0;

    int seqid = 0;
    char seq_file[300];
    sprintf(seq_file,"%s/brighthouse.seq",pbasepth);
    GetSeqId(seq_file,seqid);

    //6.1 ֱ��ʹ��һ���µ�id��seqid + 1
    seqid = seqid +1;
    tabid= seqid;
    lgprintf("��ԭ��id:%d,���ݿ�������id:%d\n",tabid,seqid);
    seqid +=1;
    lgprintf("��ԭ����:����%s���%d\n",seq_file,seqid);
    UpdateSeqId(seq_file,seqid);

    //6.2 ����tabid
    char _table_tcb_pth[300];
    sprintf(_table_tcb_pth,"%s/%s/%s.bht/Table.tcb",restore_tmp_pth,old_dbname.c_str(),old_tabname);
    lgprintf("��ԭ����:����%s��id %d\n",_table_tcb_pth,tabid);

    //getTableId(_table_tcb_pth,tabid);
    sprintf(tmp,"%s/%s/%s.bht",restore_tmp_pth,old_dbname.c_str(),old_tabname);
    // */table.bht
    GenerateNew_table_tcb(tmp,tabname,tabid);

    // 7. ����rsi�е�id��Ϣ
    char tmp_rsi_path[300];
    sprintf(tmp_rsi_path,"%s/BH_RSI_Repository",restore_tmp_pth);
    lgprintf("��ԭ����:����%sĿ¼�е�ris�ļ��ı�id.\n",tmp_rsi_path);
    UpdateRsiID(tmp_rsi_path,tabid);

    // 8. �ƶ�$DATAMERGER_HOME/dbn_tabname_pid_tmp�е������ļ���$DATAMERGER_HOME��

    // �������ݿ�Ŀ¼
    char dbpth[300];
    sprintf(dbpth,"%s/%s",pbasepth,dbn);
    if(!DoesFileExist(dbpth)) {
        sprintf(cmd,"mkdir %s",dbpth);
        system(cmd);
    }

    // 8.1 �ƶ�rsi�ļ���$DATAMERGER_HOME/BH_RSI_Repository��
    char rsi_path[300];
    sprintf(rsi_path,"%s/BH_RSI_Repository",pbasepth);
    strcat(tmp_rsi_path,"/*.rsi");

    lgprintf("��ԭ����:�ƶ�%sĿ¼rsi�ļ���%sĿ¼��.\n",tmp_rsi_path,rsi_path);
    sprintf(cmd,"mv %s %s",tmp_rsi_path,rsi_path);
    ret = system(cmd);
    if(ret == -1) {
        lgprintf("��ԭ����:run cmd :' %s 'ʧ��",cmd);
        RollbackRestore(pbasepth,dbn,tabname,tabid);
        ClearRestorePath(restore_tmp_pth);
        return -1;
    }
    // 8.2 �ƶ�table.frm ��$DATAMERGER_HOME/dbn����
    sprintf(tmp,"%s/%s.frm",restore_tmp_pth,old_tabname);

    char new_name[300];
    sprintf(new_name,"%s/%s.frm",dbpth,tabname);

    lgprintf("��ԭ����:�ƶ�%s�ļ���%s.\n",tmp,new_name);

    sprintf(cmd,"mv %s %s",tmp,new_name);
    ret = system(cmd);
    if(ret == -1) {
        lgprintf("��ԭ����:run cmd :' %s 'ʧ��",cmd);
        RollbackRestore(pbasepth,dbn,tabname,tabid);
        ClearRestorePath(restore_tmp_pth);
        return -1;
    }

    // 8.3 �ƶ�table.bht��$DATAMERGER_HOME/dbn����
    // ���ı�����:hexdump -C Table.ctb ��
    sprintf(new_name,"%s/%s.bht",dbpth,tabname);
    sprintf(cmd,"mkdir %s",new_name);
    system(cmd);

    sprintf(tmp,"%s/%s/%s.bht/*",restore_tmp_pth,old_dbname.c_str(),old_tabname);
    lgprintf("��ԭ����:�ƶ�%s�ļ���%sĿ¼��.\n",tmp,new_name);

    sprintf(cmd,"mv %s %s",tmp,new_name);
    ret = system(cmd);
    if(ret == -1) {
        lgprintf("��ԭ����:run cmd :' %s 'ʧ��",cmd);
        RollbackRestore(pbasepth,dbn,tabname,tabid);
        ClearRestorePath(restore_tmp_pth);
        return -1;
    }

    // 9 ��ԭ��ɣ������ԭ·��
    ClearRestorePath(restore_tmp_pth,true);
    return 0;
}


