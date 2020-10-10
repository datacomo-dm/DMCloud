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
// 2005/08/27修改，partid等效于sub
// 为保持wdbi库的连续运行(dtadmin单独升级),TestColumn在此实现。理想位置是到wdbi库实现
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

// 判断软件是否是RIAK版本
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

// 判断软件是否支持并行入库过程
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
Description : 删除字符串中连续重复的空格，create by liujs
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

    //----- 分布式排序添加部分
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
    LOAD_SORTED_DATA_DIR: 排序数据存储路径

    非mysqld节点运行dpadmin的时候：
    export LOAD_SORTED_DATA_DIR=nfs://192.168.1.1:/app/dma/var/preparedir

    或者(mysqld 服务器运行dpadmin的时候):
    export LOAD_SORTED_DATA_DIR=/app/dma/var/preparedir
    */
    std::string hostname;
    char* p=getenv("LOAD_SORTED_DATA_DIR"); // fix DMA-1456, by hwei
    if(p==NULL) {
        ThrowWith("dpadmin分布式排序启动时候,获取不到环境变量[LOAD_SORTED_DATA_DIR]的设置内容,无法向集群提交排序后生成数据的路径,异常退出.");
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



// 获取表id和列数目
int ParallelLoad::get_table_info(const char* pbasepth,const char* pdbname,const char* ptbname,int& tabid,int& colnum)
{
    char pth[300];
    // ---> /data/dt2/app/dma/var/zkb/test_continu_dump.bht/Table.ctb
    sprintf(pth,"%s/%s/%s.bht/Table.ctb",pbasepth,pdbname,ptbname);

    FILE  *pFile  = fopen(pth,"rb");
    if(!pFile) {
        ThrowWith("文件%s打开失败!\n",pth);
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

// 每一个分区存储的包的分段信息
typedef struct stru_part_section {
    int packstart;
    int packend;
    int lastobjs;
    stru_part_section()
    {
        packstart = packend = lastobjs=0;
    }
} stru_part_section,*stru_part_section_ptr;
// 分区结构存储信息
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

    // 分区内部的部分包的位置
    std::vector<stru_part_section>    part_section_list;

    // ap版本的分区的文件列表
    std::vector<long> part_file_list;

    // 存入riak中的pnode列表
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
            ThrowWith("文件格式错误,'%s'.",filename);
        }
        version15=false;
    }

    fread(&attrid,sizeof(int),1,fp);
    fread(&parts,sizeof(short),1,fp);
    long total_objs=0;

    // RIAK 和 AP版本的PART_XXXXX.ctl文件格式不一样
    // fileid 长度不一致
    // RIAK:8字节
    // AP:4字节
    bool b_riak_version = CheckRiakVersion();

    for(int i=0; i<parts; i++) { // 逐个分区读取到st_part_list
        stru_part _st_part;
        fread(buf,1,8,fp);
        if(memcmp(buf,"PARTPARA",8)!=0) {
            lgprintf("%d分区格式错误'%s'.",i+1,filename);
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

        // ap版本的分区的文件列表
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

        // part section 信息读取到内存
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

    // 查找对应分区的最后一个packno
    for( int i=0; i<st_part_list.size(); i++) {
        if(strcasecmp(st_part_list[i].partname,partname) == 0) { // 找到对应分区
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

// 获取包数目和列类型
int ParallelLoad::get_table_pack_obj_info(const char* pbasepth,const char* pdbname,const char* ptbname,
        int& pack_no,int64& no_obj)
{

    char path[256];
    sprintf(path,"%s/%s/%s.bht",pbasepth,pdbname,ptbname);
    std::string attr_file = get_attr_file_name(path,0);

    FILE  *pFile  = fopen(attr_file.c_str(),"rb");
    if(!pFile) {
        lgprintf("文件%s打开失败!\n",attr_file.c_str());
        return -__LINE__;
    }

    const int CLMD_HEADER_LEN = 46;
    char read_buf[CLMD_HEADER_LEN+1];
    int ret = fread(read_buf,CLMD_HEADER_LEN,1,pFile);
    if(ret != 1) {
        lgprintf("文件%s读取失败!\n",attr_file.c_str());
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
    if(stat(name.c_str(),&st)) { // 成功返回0
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
        ThrowWith("MASTER节点,向排序集群中提交长会话排序失败,错误信息[%s],错误代码[%s].",errmsg.c_str(),CR_Class_NS::strerrno(errno));
        tl->SetReturnValue(-1);
    }
    lgprintf("MASTER节点,向分布式排序集群提交[%ld]行的长会话数据成功,Finish = %d.",_loadrows,ret);
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
            // fix dma-1402: slaver节点不能更改dp.dp_datapartext.commitstatus状态
            // 长会话上slaver节点不用更新记录数,master统一更新
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
            // fix dma-1402: slaver节点不能更改dp.dp_datapartext.commitstatus状态
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


// 更新launchmaster新
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


// 分布式排序完成后提交排过程
int DataDump::commit_sort_data_process(SysAdmin &sp,int tabid,int datapartid,bool cmt_st_session,
                                       bool cmt_lt_session,bool part_is_finish,bool filesmode)
{
    if(!dp_parallel_load) return 0;

    // ======= 提交排序过程流程 ======== SASU-318
    // 1. 判断是否为master节点
    // 2.  slaver 节点:
    // 2.1 调用排序完成接口
    // 2.2 更新: dp.dp_datapartext.finishslavercnt,rownums
    // ------------------------------------------------------
    // 2. master 节点:
    // 2.1 等待所有slaver节点排序完成
    // 2.2 获取数据库,表名,分区名称,该分区的最后一个包的记录数等信息
    // 2.3 构造排序提交所需的参数
    // 2.4 调用排序完成接口
    // 2.5 更新: dp.dp_datapartext.launchstarttime,dp.dp_datapartext.launchendtime,rownumber
    // 2.6 更新任务状态为3
    // ---------------------------------
    lgprintf("enter commit_sort_data_process ,tabid(%d),datapartid(%d),slaver_index(%d),stsessionid(%s),ltsessionid(%s).",tabid,datapartid,slaver_index,stsessionid,ltsessionid);

    std::string errmsg;
    int ret = 0;
    int launchcnt = 0;
    int finishcnt = 0;
    if(b_master_node) { // master节点

        // 2.1 判断是否所有的slaver节点都已经结束了,否则等待
        int64 loadrows = 0;
        int cluster_status = 0;
        while(1) {
            if(is_all_slaver_node_finish(sp.GetDTS(),tabid,datapartid,stsessionid,
                                         launchcnt,finishcnt,loadrows,cluster_status)) {

                if(cluster_status == CS_ERROR) {
                    ThrowWith("MASTER节点,等待slaver节点完成过程中发现集群当前会话[%s]故障,退出.",stsessionid);
                }

                break;
            }
            lgprintf("Master节点,等待排序提交节点完成,排序节点数[%d]已完成节点[%d],等待30秒.",launchcnt,finishcnt);
            sleep(30);

            if(cluster_status == CS_ERROR) {
                ThrowWith("MASTER节点,等待slaver节点完成过程中发现集群当前会话[%s]故障,退出.",stsessionid);
            }
        }

        loadrows += st_parallel_load_rows; // master节点是数据还没有更新进入dp.dp_datapartext表中

        // master 节点设置集群状态为锁住,此时避免slaver节点进入
        set_cluster_status(sp.GetDTS(),tabid,datapartid,stsessionid,CS_MASTER_BUSY);

        // 2.1x 等待该表的其他分区已经完成排序过程提交了,但是还没有装入的数据进行装入
        //     dp.dp_datapartext.commitstatus in(0,1) and tabid = %d
        //     fix DMA-1347
        while(1) {
            int need_load_sessions = 0;
            int64 need_load_rows = 0;
            if(is_all_sorted_data_has_loaded(sp.GetDTS(),tabid,need_load_sessions,need_load_rows)) {
                break;
            }
            lgprintf("Master节点,等待表(%d)合并装入数据会话数(%d)记录数(%ld),等待30秒.",tabid,need_load_sessions,need_load_rows);
            sleep(30);
        }

        // 2.2 获取数据库,表名,分区名称,该分区的最后一个包的记录数等信息
        int64 startloadrow=0;
        int startpackno=0;
        int startpackobj = 0;
        get_commit_sort_info(sp,tabid,datapartid,startloadrow,
                             startpackno,startpackobj);

        lgprintf("表(%d),分区(%d)准备提交数据,起始包号(%d),起始包记录数(%d),排序合并第一个包相对位置(%d).",
                 tabid,datapartid,startpackno,startpackobj,startloadrow);

        // 2.3 构造排序提交所需的参数
        std::string st_load_param;
        std::string lt_load_param;
        {
            st_load_param = CR_Class_NS::str_setparam(st_load_param,
                            LIBIBLOAD_PN_TABLE_INFO,tbl_info_t_str);
            lt_load_param = CR_Class_NS::str_setparam(lt_load_param,
                            LIBIBLOAD_PN_TABLE_INFO,tbl_info_t_str);

            // 每一个包的记录数
            unsigned int block_lines = 65536;
            st_load_param = CR_Class_NS::str_setparam_u64(st_load_param,
                            DMIBSORTLOAD_PN_BLOCK_LINES,block_lines);
            lt_load_param = CR_Class_NS::str_setparam_u64(lt_load_param,
                            DMIBSORTLOAD_PN_BLOCK_LINES,block_lines);

            // 第一行的行号(新的分区或者不连续的分区需要进行对齐考虑)
            // 第一个包需要的行数起始相对位置(包内的相对位置)
            st_load_param = CR_Class_NS::str_setparam_i64(st_load_param,
                            DMIBSORTLOAD_PN_LINEID_BEGIN,startloadrow);
            lt_load_param = CR_Class_NS::str_setparam_i64(lt_load_param,
                            DMIBSORTLOAD_PN_LINEID_BEGIN,0);


            // 包索引的编号
            st_load_param = CR_Class_NS::str_setparam_u64arr(st_load_param,
                            LIBIBLOAD_PN_PACKINDEX_INFO,tb_packindex_arr);
            lt_load_param = CR_Class_NS::str_setparam_u64arr(lt_load_param,
                            LIBIBLOAD_PN_PACKINDEX_INFO,tb_packindex_arr);


            st_load_param = CR_Class_NS::str_setparam_bool(st_load_param,
                            LIBIBLOAD_PN_PACKINDEX_MULTIMODE,true);
            lt_load_param = CR_Class_NS::str_setparam_bool(lt_load_param,
                            LIBIBLOAD_PN_PACKINDEX_MULTIMODE,true);

            /*
            LOAD_SORTED_DATA_DIR: 排序数据存储路径

            非mysqld节点运行dpadmin的时候：
            export LOAD_SORTED_DATA_DIR=nfs://192.168.1.1:/app/dma/var/preparedir

            或者(mysqld 服务器运行dpadmin的时候):
            export LOAD_SORTED_DATA_DIR=/app/dma/var/preparedir
            */
            std::string hostname;
            char* p=getenv("LOAD_SORTED_DATA_DIR"); // fix DMA-1456, by hwei
            if(p==NULL) {
                ThrowWith("MASTER节点,获取不到环境变量[LOAD_SORTED_DATA_DIR]的设置内容,无法向集群提交排序后生成数据的路径,异常退出.");
            } else {
                hostname=std::string(p);
            }

            lgprintf("排序节点数据排序,转换后的提交数据路径为:%s. ",hostname.c_str());

            st_load_param = CR_Class_NS::str_setparam(st_load_param,LIBIBLOAD_PN_SAVE_PATHNAME,hostname);

            lt_load_param = CR_Class_NS::str_setparam(lt_load_param,LIBIBLOAD_PN_SAVE_PATHNAME,hostname);


            // 字符串型，指定 riak 连接字符串，其内容定义与 RiakCluster::Rebuild 的 conn_str 参数一致
            /*
            // 该部分在创建排序对象的时候进行设置
            char* q=getenv(RIAK_HOSTNAME_ENV_NAME);
            if(NULL == q || strlen(q) <=1){
                lgprintf("环境变量:RIAK_HOSTNAME未设置,数据入库为分布式排序AP版本.");
            }else{
                lgprintf("环境变量:RIAK_HOSTNAME=[%s],数据入库为分布式排序Cloud版本.",q);

                std::string _connect_riak_str = std::string(q);
                st_load_param = CR_Class_NS::str_setparam(st_load_param,LIBIBLOAD_PN_RIAK_CONNECT_STR,_connect_riak_str);
                lt_load_param = CR_Class_NS::str_setparam(lt_load_param,LIBIBLOAD_PN_RIAK_CONNECT_STR,_connect_riak_str);

                // LIBIBLOAD_PN_RIAK_BUCKET 字符串型，指定用于保存数据的 riak bucket 名称，目前版本 CDM 指定为 "IB_DATAPACKS"
                _connect_riak_str = std::string("IB_DATAPACKS");
                st_load_param = CR_Class_NS::str_setparam(st_load_param,LIBIBLOAD_PN_RIAK_BUCKET,_connect_riak_str);
                lt_load_param = CR_Class_NS::str_setparam(lt_load_param,LIBIBLOAD_PN_RIAK_BUCKET,_connect_riak_str);
            }
            */

            // 2.4 调用排序完成接口

            // 2.4.1 短会话数据提交
            if(cmt_st_session) {
                if(strcmp(last_stsessionid,stsessionid) != 0) {
                    ret = p_stDMIBSortLoader->Finish(st_load_param,&errmsg);
                    if(ret < 0) {
                        ThrowWith("MASTER节点,向排序集群中提交短会话排序失败,错误信息[%s],错误代码[%s].",errmsg.c_str(),CR_Class_NS::strerrno(errno));
                    }
                    lgprintf("MASTER节点,向分布式排序集群提交[%ld]行的短会话[%s]数据成功,Finish = %d.",loadrows,stsessionid,ret);
                } else {
                    lgprintf("MASTER节点,向分布式排序集群提交会话ID[%s]相同.不进行finish动作.",stsessionid);
                }
            } else {
                lgprintf("MASTER节点,无需向分布式排序集群提交[%ld]行的短会话数据.",loadrows);
            }
            // 2.4.2 长会话数据提交,短会话数据结果
            if(p_ltDMIBSortLoader != NULL && st_session_interval>0) {

                lgprintf("MASTER节点,准备向分布式排序集群提交短会话[%s]的[%ld]行长会话数据成功.",stsessionid,loadrows);
                CR_FixedLinearStorage tmp_fls(65536);
                tmp_fls.SetTag(CR_Class_NS::str_setparam("",DMIBSORTLOAD_PN_QUERY_CMD, LIBSORT_PV_APPEND_PART_FILES));
                tmp_fls.PushBack(loadrows,std::string(stsessionid),NULL);
                ret = p_ltDMIBSortLoader->PushLines(tmp_fls);

                if(ret != 0) {
                    ThrowWith("MASTER节点,向排序集群中提长会话排序失败,错误信息[%s],错误代码[%s].",errmsg.c_str(),CR_Class_NS::strerrno(errno));
                }
                lgprintf("MASTER节点,向分布式排序集群提交[%ld]行的长会话[%s]数据成功.",loadrows,stsessionid);

                lt_parallel_load_rows+=loadrows;

                // 更新长会话记录数,fix dma-1479
                {
                    AutoStmt st(sp.GetDTS());
                    lgprintf("设置表(%d),分区(%d),长会话('%s')的记录数为(%ld).",tabid,datapartid,ltsessionid,lt_parallel_load_rows);
                    st.DirectExecute("update dp.dp_datapartext set rownums = %ld  where tabid = %d and datapartid=%d and sessionid='%s'",
                                     lt_parallel_load_rows,tabid,datapartid,ltsessionid);
                }
            }


            // 2.5 更新: dp.dp_datapartext表的:
            // sortendtime,rownums,commitstatus

            // 2.5.1 短会话上更新
            if(strcmp(last_stsessionid,stsessionid) != 0) {
                update_commit_info_to_db(sp,tabid,datapartid,st_parallel_load_rows,false);
            }

        }

        // 2.5.2 长会话上更新

        // 等待长会话上数据提交完成
        if(cmt_lt_session && p_ltDMIBSortLoader != NULL && st_session_interval>0) {// 只有长会话对象存在的时候才进行提交

            ret = p_ltDMIBSortLoader->Finish(lt_load_param,&errmsg);
            if(ret < 0) {
                ThrowWith("MASTER节点,向排序集群中提交长会话排序失败,错误信息[%s],错误代码[%s].",errmsg.c_str(),CR_Class_NS::strerrno(errno));
            }
            lgprintf("MASTER节点,向分布式排序集群提交[%ld]行的长会话[%s]数据成功,Finish=%d.",lt_parallel_load_rows,ltsessionid,ret);

            update_commit_info_to_db(sp,tabid,datapartid,lt_parallel_load_rows,true);
        }


        // 2.6 提交文件状态
        if(filesmode) {
            // 更新文件状态,为已经提交完成状态
            // 文件状态的提交只能有master节点来完成
            AutoStmt st(dtdbc);
            char slaver_index_arry[128]; // 合并出slaver_index,例如:'0','1','2'...
            slaver_index_arry[0] = 0;
            for(int i=0; i<=launchcnt; i++) {
                sprintf(slaver_index_arry+strlen(slaver_index_arry),"'%d'",i);
                if(i != launchcnt) {
                    strcat(slaver_index_arry,",");
                }
            }
            lgprintf("提交表(%d),分区(%d),节点(%s)的文件出状态[10--->2].",tabid,datapartid,slaver_index_arry);
            if(strlen(slaver_index_arry)>0) {
                st.DirectExecute("update dp.dp_filelog set status=2 where tabid=%d and datapartid=%d and slaveridx in (%s) and status=10"
                                 ,tabid,datapartid,slaver_index_arry);
            }
        }

        // 2.7 排序过程完成后，直接将任务状态修改成3(排序完成)即可
        if(part_is_finish) {
            sp.UpdateTaskStatus(LOADING,tabid,datapartid);

            reset_part_launch_master_info(sp,tabid,datapartid);

        }

        st_parallel_load_rows = 0;

        // 恢复设置集群状态
        set_cluster_status(sp.GetDTS(),tabid,datapartid,stsessionid,CS_OK);

    } else { // slaver 节点

        // 2 调用排序完成接口
        // 2.1 短会话数据提交
        std::string load_param="";
        if(strcmp(last_stsessionid,stsessionid) != 0) {
            ret = p_stDMIBSortLoader->Finish(load_param,&errmsg);
            if(ret <0) { // fix dma-2094
                ThrowWith("SLAVER节点[%d],向排序集群中提交短会话排序失败,错误信息[%s],错误代码[%s].",slaver_index,errmsg.c_str(),CR_Class_NS::strerrno(errno));
            }
            lgprintf("SLAVER节点[%d],向分布式排序集群提交[%ld]行的短会话[%s]数据成功,Finish=%d.",slaver_index,st_parallel_load_rows,stsessionid,ret);

        } else {
            lgprintf("SLAVER节点[%d],向分布式排序集群提交会话ID[%s]相同.不进行finish动作.",slaver_index,stsessionid);
        }

        // 2.2 全局会话数据提交
        if(cmt_lt_session && p_ltDMIBSortLoader != NULL && st_session_interval>0) {// 只有长会话对象存在的时候才进行提交
            ret = p_ltDMIBSortLoader->Finish(load_param,&errmsg);
            if(ret < 0) {
                ThrowWith("SLAVER节点,向排序集群中提交长会话排序失败,错误信息[%s],错误代码[%s].",errmsg.c_str(),CR_Class_NS::strerrno(errno));
            }
            lgprintf("SLAVER节点,向分布式排序集群提交[%ld]行的长会话[%s]数据成功,Finish=%d.",lt_parallel_load_rows,ltsessionid,ret);
        }

        if(strcmp(last_stsessionid,stsessionid) != 0) {
            // 短会话上的更新
            update_commit_info_to_db(sp,tabid,datapartid,st_parallel_load_rows,false);
        }
        if(cmt_lt_session) { // 长会话上更新记录数澹(0)和finish数
            update_commit_info_to_db(sp,tabid,datapartid,0,cmt_lt_session);
        }
        st_parallel_load_rows= 0;

    }

    strcpy(last_stsessionid,stsessionid);

    if(part_is_finish) {
        lgprintf("分区任务tabid(%d),datapartid(%d)数据排序完成,等待装入数据.",tabid,datapartid);
    }

    lgprintf("leave commit_sort_data_process ,tabid(%d),datapartid(%d),slaver_index(%d).",tabid,datapartid,slaver_index);


    return 0;
}


// 没有采集到数据的时候,构建一个空的DMIBSortLoad对象,否则提交的时候会失败
int DataDump::CreateEmptyDMIBSortLoad(SysAdmin &sp,int tabid,int partid,AutoMt *pCur)
{
    if(dp_parallel_load) {

        int row_str_len = wociGetRowLen(*pCur);
        dp_parallel_load_columns = pCur->GetColumnNum();
        row_str_len += dp_parallel_load_columns*256;

        // 数值类型的临时变量
        uint64_t numeric_val_tmp=0;
        uint32_t uint32_val_tmp=0;
        uint16_t uint16_val_tmp= 0;

        // ======= 并行排序装入流程========= SASU-318
        // 1. 连接到排序集群
        // 2. 通过已经连接的排序集群,创建排序句柄对象
        // 3. 在创建好的handle对象句柄上,逐行的封装好数据，并将封装好的数据通过PushRow发往排序集群
        // 4. 整个分区的需要排序的数据都发送完成后,等待排序完成,调用函数 DataDump::commit_sort_data
        // ---------------------------------

        // 1. 连接到排序集群
        // 2. 通过已经连接的排序集群,创建排序句柄对象
        if(p_stDMIBSortLoader== NULL) { // 该分区第一次排序的时候,才会进入执行

            int ret = 0;

            // 获取包索引列名称
            std::vector<std::string> packidx_column_list;
            get_packindex_column_list(sp,tabid,packidx_column_list);
            tb_packindex_arr.clear();

            // 获取zip压缩列
            std::vector<std::string> zipcompress_column_list;
            get_zipcompress_column_list(sp,tabid,zipcompress_column_list);

            std::vector<std::string> snappycompress_column_list;
            get_snappycompress_column_list(sp,tabid,snappycompress_column_list);

            std::vector<std::string> lz4compress_column_list;
            get_lz4compress_column_list(sp,tabid,lz4compress_column_list);



            // 1. 连接到排序集群,已经在构造函数中调用
            // init_riak_cluster_for_sort()实现

            // 2. 通过已经连接的排序集群,创建排序句柄对象
            table_info.clear();

            // 设置列数目
            uint16_t col_numbers = dp_parallel_load_columns;

            std::string _col_name="";
            int _tmp_col_len = 0;
            for(int i = 0; i<col_numbers ; i++) {

                char _tmp_col_name[256];
                wociGetColumnName(*pCur,i,_tmp_col_name);
                _col_name = std::string(_tmp_col_name);

                // 判断该列是否是包索引
                if(column_is_packindex(packidx_column_list,_col_name)) {
                    tb_packindex_arr.push_back(i);
                }

                // 判断该列是否是zip压缩
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
                        _tmp_col_len = pCur->getColLen(i) -1; // fix DMA-1383:mysqld 端的长度是不带0结尾的,需要减1
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
            row_data.BindTableInfo(table_info); //行对象绑定到 table_info 对象

            // 构建长短会话对象
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
                    lgprintf("PushThreadA向排序集群[%s]中添加数据失败,错误信息[%s],错误代码[%d].",_stsessionid,errmsg.c_str(),ret);
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
                lgprintf("PushThreadB向排序集群[%s]中添加数据失败,错误信息[%s],错误代码[%d].",_stsessionid,errmsg.c_str(),ret);
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
    if(dp_parallel_load) { // 并行排序装入数据

        mytimer  ProcBlockAll;
        ProcBlockAll.Clear();
        ProcBlockAll.Start();

        mytimer  ProcBlockPush;
        ProcBlockPush.Clear();

        int row_str_len = wociGetRowLen(*pCur);
        dp_parallel_load_columns = pCur->GetColumnNum();
        row_str_len += dp_parallel_load_columns*256;

        // 数值类型的临时变量
        uint64_t numeric_val_tmp=0;
        uint32_t uint32_val_tmp=0;
        uint16_t uint16_val_tmp= 0;

        // ======= 并行排序装入流程========= SASU-318
        // 1. 连接到排序集群
        // 2. 通过已经连接的排序集群,创建排序句柄对象
        // 3. 在创建好的handle对象句柄上,逐行的封装好数据，并将封装好的数据通过PushRow发往排序集群
        // 4. 整个分区的需要排序的数据都发送完成后,等待排序完成,调用函数 DataDump::commit_sort_data
        // ---------------------------------

        // 1. 连接到排序集群
        // 2. 通过已经连接的排序集群,创建排序句柄对象
        if(p_stDMIBSortLoader== NULL) { // 该分区第一次排序的时候,才会进入执行
            int ret = 0;

            // 获取包索引列名称
            std::vector<std::string> packidx_column_list;
            get_packindex_column_list(sp,tabid,packidx_column_list);
            tb_packindex_arr.clear();

            // 获取zip压缩列
            std::vector<std::string> zipcompress_column_list;
            get_zipcompress_column_list(sp,tabid,zipcompress_column_list);

            std::vector<std::string> snappycompress_column_list;
            get_snappycompress_column_list(sp,tabid,snappycompress_column_list);

            std::vector<std::string> lz4compress_column_list;
            get_lz4compress_column_list(sp,tabid,lz4compress_column_list);


            // 1. 连接到排序集群,已经在构造函数中调用
            // init_riak_cluster_for_sort()实现

            // 2. 通过已经连接的排序集群,创建排序句柄对象
            table_info.clear();
            // 设置列数目
            uint16_t col_numbers = dp_parallel_load_columns;

            std::string _col_name="";
            int _tmp_col_len = 0;
            for(int i = 0; i<col_numbers ; i++) {

                char _tmp_col_name[256];
                wociGetColumnName(*pCur,i,_tmp_col_name);
                _col_name = std::string(_tmp_col_name);

                // 判断该列是否是包索引
                if(column_is_packindex(packidx_column_list,_col_name)) {
                    tb_packindex_arr.push_back(i);
                }

                // 判断该列是否是zip压缩
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
                        _tmp_col_len = pCur->getColLen(i) -1; // fix DMA-1383:mysqld 端的长度是不带0结尾的,需要减1
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

            row_data.BindTableInfo(table_info); //行对象绑定到 table_info 对象

            // 构建长短会话对象
            if(b_master_node) {
                lgprintf("MASTER节点,刚启动短会话ID(%s),设置集群状态为:CS_MASTER_INIT",stsessionid);
                set_cluster_status(sp.GetDTS(),tabid, datapartid,stsessionid,CS_MASTER_INIT);

                if(st_session_interval>0) {
                    lgprintf("MASTER节点,刚启动长会话ID(%s),设置集群状态为:CS_MASTER_INIT",ltsessionid);
                    set_cluster_status(sp.GetDTS(),tabid, datapartid,ltsessionid,CS_MASTER_INIT);
                }
            }
            int _ret = create_DMIBSortLoader(sp,tabid,partid,true);

            if(_ret == -1) {

                if(!b_master_node) {
                    // 短会话自减少
                    decrease_slaver_lanuch_count(sp,tabid,datapartid,false);
                    if(st_session_interval>0) {
                        // 长会话自减少
                        decrease_slaver_lanuch_count(sp,tabid,datapartid,true);
                    }

                    ThrowWith("Slaver节点DataDump::create_DMIBSortLoader创建对象失败,tabid(%d),datapartid(%d),会话(%s)准备退出!",
                              tabid,datapartid,stsessionid);
                } else {

                    ThrowWith("Master节点DataDump::create_DMIBSortLoader创建对象失败,tabid(%d),datapartid(%d),会话(%s)准备退出!",
                              tabid,datapartid,stsessionid);

                }
            } else if (_ret == 0) {
                if(b_master_node) {
                    lgprintf("MASTER节点,短会话ID(%s)创建排序对象完成,设置集群状态为:CS_OK",stsessionid);
                    set_cluster_status(sp.GetDTS(),  tabid, datapartid,stsessionid,CS_OK);

                    if(st_session_interval>0) {
                        lgprintf("MASTER节点,长会话ID(%s)创建排序对象完成,设置集群状态为:CS_OK",ltsessionid);
                        set_cluster_status(sp.GetDTS(),  tabid, datapartid,ltsessionid,CS_OK);
                    }
                }
            }

            // 构建排序列的对象
            int cur=*pCur;
            char *idxcolsname=dp.idxp[idx].idxcolsname;
            lgprintf("排序字段列[%s].",idxcolsname);
            wociConvertColStrToInt2(cur,idxcolsname,sort_key_col_info.idxcol,sort_key_col_info.partlen);
        }

        // 3. 在创建好的handle对象句柄上,逐行的封装好数据，并将封装好的数据通过PushRow发往排序集群
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
                lgprintf("准备向分布式排序集群[%s]推送[%d]行[%d]列的数据...",stsessionid,pCur->GetRows(),pCur->GetColumnNum());

                // 排序的key值
                char *p_mem_sort_key = new char[row_str_len];
                int p_mem_sort_key_pos = 0;

                // null 值的key
                std::string null_key = "";
                null_key.assign(1,(char)0);

                // 非null的值的key,字符串的flag
                std::string non_null_key = "";
                non_null_key.assign(1,(char)1);
                char _tmp[2000];
                _tmp[sizeof(_tmp)-1] = 0;
                char sz_date[200];
                sz_date[sizeof(sz_date)-1] = 0;

                for(int idx_row=0; idx_row < pCur->GetRows(); idx_row++) {
                    p_mem_sort_key_pos =0;

                    for(int _i = 0; _i<(sizeof(sort_key_col_info.idxcol)/sizeof(int)); _i++) {

                        if(sort_key_col_info.idxcol[_i] == -1) { // 已经结束
                            break;
                        }
                        // 3.1> 生成排序用的key,多列之间直接字符串连接
                        // a> 多列的排序的KEY使用字符串连接直接合并构成
                        // b> 如果是NULL的列,采用字符null_key[0]构成
                        // c> 如果不是NULL采用non_null_key[1]构成

                        int idx_col = sort_key_col_info.idxcol[_i];

                        // 设置排序列的NULL值: null flag[0],non-null[1]
                        if(wociIsNull(*pCur, idx_col, idx_row)) {
                            memcpy(p_mem_sort_key+p_mem_sort_key_pos,null_key.data(),null_key.length());
                            p_mem_sort_key_pos+=null_key.length();
                            break;  // 当前列处理完成
                        } else {
                            memcpy(p_mem_sort_key+p_mem_sort_key_pos,non_null_key.data(),non_null_key.length());
                            p_mem_sort_key_pos += non_null_key.length();
                        }

                        // 非NULL值的列,设置对应的值,生成对应的排序KEY
                        char *_p;
                        switch(pCur->getColType(idx_col)) {

                            case COLUMN_TYPE_CHAR:

                                _p = (char*)pCur->PtrStr(idx_col,idx_row);
                                if(sort_key_col_info.partlen[_i] == 0) { // 无需截取
                                    memcpy(p_mem_sort_key+p_mem_sort_key_pos,_p,strlen(_p));
                                    p_mem_sort_key_pos += strlen(_p);
                                } else if(sort_key_col_info.partlen[_i] < 0) { // 从尾部截取
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
                                } else { // 从头截取
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
                                if(sort_key_col_info.partlen[_i] <= 0) { // 无需截取
                                    sprintf(_tmp,"%04d%02d%02d%02d%02d%02d",
                                            wociGetYear(sz_date),
                                            wociGetMonth(sz_date),
                                            wociGetDay(sz_date),
                                            wociGetHour(sz_date),
                                            wociGetMin(sz_date),
                                            wociGetSec(sz_date));
                                } else { // 从头部截取
                                    // yyyy-mm-dd hh:mi:ss 支持截取到: 年,月,日,时,分
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

                    // 3.2> 生成往下传输的行数据信息
                    row_data.Clear();
                    int fret = 0;
                    for(int idx_col=0; idx_col < dp_parallel_load_columns; idx_col++) {

                        // 行数据由NULL位图部分和数据部分构成
                        int _len = 0;
                        if(wociIsNull(*pCur, idx_col, idx_row)) { // 设置NULL bit
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
                            lgprintf("执行返回错误[%s].",CR_Class_NS::strerrno(fret));
                            ThrowWith("DM_IB_RowData::setdata row[%d] col[%d] return %d \n",idx_row,idx_col,fret);
                        }
                    }

                    // 获取数据和长度
                    const void *row_data_data;
                    size_t row_data_size;
                    fret = row_data.Data(row_data_data, row_data_size);
                    if (fret) {
                        lgprintf("执行返回错误[%s].",CR_Class_NS::strerrno(fret));
                        ThrowWith("DM_IB_RowData::Data row[%d] return %d \n",idx_row,fret);
                    }

                    st_parallel_load_rows++;
                    parallel_load_obj_id++;

                    // 3.3> 向分布式排序集群发送函数数据
                    if(1) {
                        std::string errmsg="";

                        // 通过节点ID(高8bit)+记录数(低56bit)构成
                        unsigned long _push_obj_id = generate_obj_id();

                        ProcBlockPush.Start();
#ifndef THREAD_PUSH
                        ret = lines.PushBack(_push_obj_id, p_mem_sort_key,p_mem_sort_key_pos,row_data_data,row_data_size);
#else
                        ret = plines->PushBack(_push_obj_id,p_mem_sort_key,p_mem_sort_key_pos,row_data_data,row_data_size);
#endif
                        if (ret == ENOBUFS) {

#ifndef THREAD_PUSH
                            // 短会话id上提交数据
                            //lgprintf("tabid(%d),datapartid(%d),slaver_index(%d),ProcBlock DMIBSortLoader->PushLines start.",tabid,datapartid,slaver_index);
                            ret = p_stDMIBSortLoader->PushLines(lines, &errmsg);
                            //lgprintf("tabid(%d),datapartid(%d),slaver_index(%d),ProcBlock DMIBSortLoader->PushLines end.",tabid,datapartid,slaver_index);
                            if(ret) {
                                lgprintf("执行返回错误[%s].",CR_Class_NS::strerrno(ret));
                                ThrowWith("向排序集群[%s]中添加数据失败,错误信息[%s],错误代码[%d].",stsessionid,errmsg.c_str(),ret);
                            }
                            lines.Clear();
#else
                            fixed_linear_storage_queue_mutex.Lock();
                            fixed_linear_storage_queue.push(plines);
                            fixed_linear_storage_queue_mutex.Unlock();
#endif
                            //lgprintf("tabid(%d),datapartid(%d),slaver_index(%d),CR_FixedLinearStorage::PushBack(%ld) 记录行数(%d) return ENOBUFS,切换下一片CR_FixedLinearStorage.",tabid,datapartid,slaver_index,CR_FixedLinearStorage_size,push_line_nums);
                            push_line_nums=0;
                            // 满了一片之后,需要分配一个新的ID
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
                                lgprintf("执行返回错误[%s].",CR_Class_NS::strerrno(ret));
                                ThrowWith("向排序集群[%s]中添加数据失败,push_obj_id[%ld].",stsessionid,_push_obj_id);
                            }

                            continue;
                        } else if(ret) {
                            lgprintf("执行返回错误[%s].",CR_Class_NS::strerrno(ret));
                            ThrowWith("向排序集群[%s]中添加数据失败,错误信息[%s],错误代码[%d].",stsessionid,errmsg.c_str(),ret);
                        }

                        push_line_nums++;

                        ProcBlockPush.Stop();
                    }
                }

                delete [] p_mem_sort_key;
                p_mem_sort_key = NULL;

                lgprintf("tabid(%d),datapartid(%d),slaver_index(%d),完成向分布式排序集群[%s]推送[%d]行[%d]列的数据.",tabid,datapartid,slaver_index,stsessionid,pCur->GetRows(),pCur->GetColumnNum());
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
            ThrowWith("tabid(%d),datapartid(%d),slaver_index(%d),完成向分布式排序集群[%s]推送数据失败,抛出异常.",tabid,datapartid,slaver_index,stsessionid);
        }

        // 4. 整个分区的需要排序的数据都发送完成后,等待排序完成,调用函数 DataDump::commit_sort_data
        if(pCur->GetRows()>0) {
            std::string errmsg="";
            // 短会话数据提交

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
                // 设置集群状态未故障

#ifdef THREAD_PUSH
                // 清理内存
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
                ThrowWith("向排序集群[%s]中添加数据失败,错误信息[%s],错误代码[%d].",stsessionid,errmsg.c_str(),ret);
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

                // 清理内存
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

                // 设置集群状态未故障
                set_cluster_status(sp.GetDTS(),tabid,datapartid,stsessionid,CS_ERROR);
                ThrowWith("向排序集群[%s]中添加数据失败,错误代码[%d].",stsessionid,ret);
            }
#endif
        }


        ProcBlockAll.Stop();
        double tm_end = ProcBlockAll.GetTime();
        ProcBlockAll.Clear();
        lgprintf("tabid(%d),datapartid(%d),slaver_index(%d),stsessionid(%s),DataDump::ProcBlock 处理%d行数据,累计使用时间(%0.4f)秒(精确度秒,不含sleep等耗时).",tabid,datapartid,slaver_index,stsessionid,pCur->GetRows(),tm_end);

        tm_end = ProcBlockPush.GetTime();
        lgprintf("tabid(%d),datapartid(%d),slaver_index(%d),stsessionid(%s),DataDump::ProcBlock 向集群推送%d行数据,累计使用时间(%0.4f)秒(精确度秒,不含sleep等耗时).",tabid,datapartid,slaver_index,stsessionid,pCur->GetRows(),tm_end);
        ProcBlockPush.Clear();

    } else { // 走原流程,不进行分布式排序
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
            sp.log(dp.tabid,partid,DUMP_DST_TABLE_DATA_BLOCK_SIZE_ERROR,"表%d的目标表的数据块大小(%d)不合适，至少为%d.",dp.tabid,maxblockrn,MIN_BLOCKRN);
            ThrowWith("目标表的数据块大小(%d)不合适，至少为%d.",maxblockrn,MIN_BLOCKRN);
        }
        if(maxblockrn>MAX_BLOCKRN) {
            sp.log(dp.tabid,partid,DUMP_DST_TABLE_DATA_BLOCK_SIZE_ERROR,"表%d的目标表的数据块大小(%d)不合适，不能超过%d.",dp.tabid,maxblockrn,MAX_BLOCKRN);
            ThrowWith("目标表的数据块大小(%d)不合适，不能超过%d.",maxblockrn,MAX_BLOCKRN);
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
        //屏蔽PK模式，全部按普通模式处理
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
                //子块分割
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
                            sp.log(dp.tabid,partid,DUMP_INDEX_BLOCK_SIZE_ERROR,"表%d,数据预处理错误,分区号%d,索引字段'%s',超过允许索引块长度%d.",dp.tabid,partid,idxcolsname,MAX_BLOCKRN);
                            ThrowWith("数据预处理错误,分区号%d,索引字段'%s',超过允许索引块长度%d.",partid,idxcolsname,MAX_BLOCKRN);
                        }
                    }
                    strow=thisrow;
                    idx_startrow=blockrn;
                }
                //blockmt.QuickCopyFrom(pcur,blockrn,thisrow);
                wociCopyRowsTo(cur,blockmt,-1,thisrow,1);
                blockrn++;//=wociGetMemtableRows(blockmt);
                //块和子块的分割
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
                            sp.log(dp.tabid,partid,DUMP_INDEX_BLOCK_SIZE_ERROR,"表%d,数据预处理错误,分区号%d,索引字段'%s',超过允许索引块长度%d.",dp.tabid,partid,idxcolsname,MAX_BLOCKRN);
                            ThrowWith("数据预处理错误,分区号%d,索引字段'%s',超过允许索引块长度%d.",partid,idxcolsname,MAX_BLOCKRN);
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
            //保存最后的块数据
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

            //保存索引数据
            {
                di.WriteMt(idxdt,COMPRESSLEVEL,0,false);
                di.SetFileHeader(totidxrn,NULL);
            }
            //保存文件索引
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

/* 增量加分区的功能在这里实现 */
// Jira DMA102
//返回值
// 0: 没有新增
// 1:正常新增
// 2: 新增异常
// Jira 124: 自动创建不存在的表
int MiddleDataLoader::AutoAddPartition(SysAdmin &sp)
{
    /* 检查需要自动增加的分区 */
    printf("检查自动增加分区...\n");
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
                lgprintf("新建分区NEW_PARTFULLNAME字段不能为空！");
                ret=2;
                continue;
            }

            AutoMt tabinfo(sp.GetDTS(),10);
            tabinfo.FetchAll("select * from dp.dp_table where lower(tabname)=lower('%s') and lower(databasename)=lower('%s')"
                             ,newpart.PtrStr("NEW_TABNAME_INDM",0),newpart.PtrStr("NEW_DBNAME_INDM",0));
            if(tabinfo.Wait()<1) {
                lgprintf("增加分区的表没有定义 %s.%s ,开始创建 ... ",newpart.PtrStr("NEW_DBNAME_INDM",0),newpart.PtrStr("NEW_TABNAME_INDM",0));
                if(st.DirectExecute("update %s set imp_status=4 where new_tabname_indm='%s' and new_dbname_indm='%s' and NEW_PARTFULLNAME='%s'"
                                    ,mt.PtrStr("tabname",ern),newpart.PtrStr("NEW_TABNAME_INDM",0),newpart.PtrStr("NEW_DBNAME_INDM",0),newpart.PtrStr("NEW_PARTFULLNAME",0))!=1)  {
                    lgprintf("新建分区异常,可能是其它进程在处理");
                    ret=2;
                    continue;
                }
                int tabid=CreateLike(
                              (const char *)newpart.PtrStr("BASE_DMDBNAME",0),(const char *)newpart.PtrStr("BASE_DMTABNAME",0),
                              (const char *)newpart.PtrStr("NEW_DBNAME",0),(const char *)newpart.PtrStr("NEW_TABNAME",0),
                              (const char *)newpart.PtrStr("NEW_DBNAME_INDM",0),(const char *)newpart.PtrStr("NEW_TABNAME_INDM",0),
                              "now",true);
                AutoStmt stdm(sp.GetDTS());
                //更新分区信息
                if(stdm.DirectExecute("update dp.dp_datapart set set partfullname='%s', extsql='select * from %s.%s %s' where tabid=%d and datapartid=1",
                                      newpart.PtrStr("NEW_PARTFULLNAME",0),
                                      newpart.PtrStr("NEW_DBNAME",0),newpart.PtrStr("NEW_TABNAME",0),newpart.PtrStr("NEW_PARTFULLNAME",0),tabid)!=1) {
                    if(st.DirectExecute("update %s set imp_status=11 where new_tabname_indm='%s' and new_dbname_indm='%s' and NEW_PARTFULLNAME='%s'"
                                        ,mt.PtrStr("tabname",ern),newpart.PtrStr("NEW_TABNAME_INDM",0),newpart.PtrStr("NEW_DBNAME_INDM",0),newpart.PtrStr("NEW_PARTFULLNAME",0))!=1)
                        ern++;
                    lgprintf("新建分区异常,更新分区信息(dp.dp_datapart.partfullname)失败,表%d",tabid);
                    ret=2;
                    continue;
                }
                //lgprintf("增加分区的表没有定义 %s.%s ",newpart.PtrStr("NEW_DBNAME_INDM",0),newpart.PtrStr("NEW_TABNAME_INDM",0));
                continue;
            }
            int tabid=tabinfo.GetInt("tabid",0);
            tabinfo.FetchFirst("select * from dp.dp_datapart where tabid=%d and lower(extsql) like lower(' %%%s%% ')",
                               tabid,newpart.PtrStr("NEW_PARTFULLNAME",0));
            if(tabinfo.Wait()>0) {
                if(st.DirectExecute("update %s set imp_status=11 where new_tabname_indm='%s' and new_dbname_indm='%s'"
                                    ,mt.PtrStr("tabname",ern),newpart.PtrStr("NEW_TABNAME_INDM",0),newpart.PtrStr("NEW_DBNAME_INDM",0))!=1)
                    ern++;
                lgprintf("增加的分区已经存在 %s.%s (%d)",newpart.PtrStr("NEW_DBNAME_INDM",0),newpart.PtrStr("NEW_TABNAME_INDM",0),tabid);
                ret=2;
                continue;
            }
            tabinfo.FetchFirst("select max(datapartid) mdp,max(tmppathid) tmppathid from dp.dp_datapart where tabid=%d ",tabid);
            if(tabinfo.Wait()<1) {
                if(st.DirectExecute("update %s set imp_status=11 where new_tabname_indm='%s' and new_dbname_indm='%s'"
                                    ,mt.PtrStr("tabname",ern),newpart.PtrStr("NEW_TABNAME_INDM",0),newpart.PtrStr("NEW_DBNAME_INDM",0))!=1)
                    ern++;
                lgprintf("增加的分区没有预定义分区 %s.%s (%d)",newpart.PtrStr("NEW_DBNAME_INDM",0),newpart.PtrStr("NEW_TABNAME_INDM",0),tabid);
                ret=2;
                continue;
            }
            int datapartid=tabinfo.GetInt("mdp",0)+1;
            int tmppathid=tabinfo.GetInt("tmppathid",0);
            if(st.DirectExecute("update %s set imp_status=4 where new_tabname_indm='%s' and new_dbname_indm='%s' and NEW_PARTFULLNAME='%s'"
                                ,mt.PtrStr("tabname",ern),newpart.PtrStr("NEW_TABNAME_INDM",0),newpart.PtrStr("NEW_DBNAME_INDM",0),newpart.PtrStr("NEW_PARTFULLNAME",0))!=1)  {
                lgprintf("新建分区异常,可能是其它进程在处理");
                ret=2;
                continue;
            }
            AutoStmt stdm(sp.GetDTS());
            stdm.DirectExecute("insert into dp.dp_datapart(datapartid,begintime,status,partfullname,partdesc,tabid,tmppathid,compflag,srcsysid,extsql) "
                               " values (%d,now(),0,'%s','%s',%d,%d,1,%d,'select * from %s.%s %s')",
                               datapartid,newpart.PtrStr("NEW_PARTFULLNAME",0),newpart.PtrStr("NEW_PARTFULLNAME",0),tabid,tmppathid,newpart.GetInt("DBSID_INDM",0),
                               newpart.PtrStr("NEW_DBNAME",0),newpart.PtrStr("NEW_TABNAME",0),newpart.PtrStr("NEW_PARTFULLNAME",0));
            lgprintf("已新建分区：表%d ",tabid);
            ret=1;
        } // end for
    } // end try
    catch(...) {
        lgprintf("新建分区异常！");
        return 2;
    }
    return ret;
}

//返回值
//0 :没有需要处理的表
//1 :检测到需要追加数据的表
//2 :没有检测到需要追加数据的表
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
        printf("有%d个表需要检查数据文件补录.\n",ct);
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
                sp.log(tabid,datapartid,DUMP_FILE_ERROR,"表%d,分区%d,文件导入数据缺少using关键字.",tabid,datapartid);
                ThrowWith("文件导入数据缺少using关键字.");
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
                lgprintf("表%d需要追加数据。",tabid);
                sp.log(tabid,datapartid,DUMP_BEGIN_DUMPING_NOTIFY,"表%d,分区%d,开始追加数据",tabid,datapartid);
                return 1;
            }
            //下次查找其它表，不在同一个表上循环
            if(ct>1)
                st.DirectExecute("update dp.dp_datapart set blevel=blevel+1 where tabid=%d and datapartid=%d",
                                 tabid,datapartid);
            return 2;
        } else if(strcasestr(dumpsql,"load data")!=NULL) {
            sp.log(tabid,datapartid,DUMP_FILE_ERROR,"表%d,分区%d,格式错误'%s',请使用'load data from files using xxx\n%s.",tabid,datapartid,dumpsql);
            ThrowWith("格式错误'%s',请使用'load data from files using xxx\n%s.",dumpsql);
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
        // 外面可能会进行会话更新
        AutoLock ak(_session_lock);
        fparser->SetSession(_slaver_index);
    }

    //Get file and parser
    //如果上次的文件未处理完(内存表满),或者有新文件，则进入分解过程
    while(true) {
        if(!filehold) {
            int gfstate=0;

            // 获取文件
            gfstate = fparser->GetFile(withbackup,plugin_sp,tabid,datapartid);

            if(gfstate==0) {

                //没有数据需要处理
                if(fparser->ExtractEnd(plugin_sp.GetDTS())) {
                    if(_dp_parallel_load) {
                        _extractend = true;
                    }

                    if(_dp_parallel_load) {
                        lgprintf("表(%d),分区(%d),slavercnt(%d) 抽取数据结束,退出PickThread.",tabid,datapartid,_slaver_index);
                    } else {
                        lgprintf("表(%d),分区(%d,slavercnt(%d) 抽取数据结束,退出PickThread.",tabid,datapartid);
                    }
                    break;
                } else {
                    // 获取不到文件了,那么超时就不用等了
                    if(_dp_parallel_load && _slaver_index>0) { // slaver 节点不用等
                        if(_st_session_interval>0 && _sorttm->GetTime() > _st_session_interval) {
                            _extractend = true;
                            lgprintf("表(%d),分区(%d)获取不到文件,短会话时间到离开PickThread.",tabid,datapartid);
                            break;
                        }
                    }

                    if( _dp_parallel_load && _slaver_index == 0) { // master 节点
                        if(fparser->GetForceEndDump() == 1) { // 只有采集一次数据的时候,才退出
                            if(_st_session_interval>0 && _sorttm->GetTime() > 1*_st_session_interval) {
                                _extractend = true;  // master 节点不要等待
                                lgprintf("表(%d),分区(%d)获取不到文件,短会话时间到离开PickThread A.",tabid,datapartid);
                                break;
                            }
                        } else {
                            if(_st_session_interval>0 && _sorttm->GetTime() > 1*_st_session_interval) {
                                _extractend = false; // master 节点要等待
                                lgprintf("表(%d),分区(%d)获取不到文件,短会话时间到离开PickThread B.",tabid,datapartid);
                                break;
                            }
                        }
                    }
                }

                if(_dp_parallel_load) {
                    _extractend = false;
                }
                mySleep(fparser->GetIdleSeconds());

                //>> begin: fix dma-1326: 单个文件处理完成,判断是否到提交时间,如果到提交时间了就提交文件
                if(_dp_parallel_load ) {
                    if(_sorttm->GetTime() > _st_session_interval &&
                       dtmt.Next()->GetRows() > 0) {
                        lgprintf("表(%d),分区(%d)获取不到文件,短会话时间到离开PickThread.",tabid,datapartid);
                        break;
                    }
                }
                //<< end: fix dma-1326

                continue;
            } else if(gfstate==2) //文件错误，但应该忽略
                continue;

            uncheckfct++;
        }
        int frt=fparser->DoParse(*dtmt.Next(),plugin_sp,tabid,datapartid);
        filehold=false;
        if(frt==-1) {
            plugin_sp.log(tabid,datapartid,DUMP_FILE_ERROR,"表%d,分区%d,文件处理错误.",tabid,datapartid);
            ThrowWith("文件处理错误.");
            break;
        } else if(frt==0) {
            //memory table is full,and file is not eof
            lgprintf("表(%d),分区(%d)内存表满单个文件处理中,短会话时间到离开PickThread.",tabid,datapartid);
            filehold=true;
            break;
        } else if(frt==2) {
            //memory table is full and file is eof
            lgprintf("表(%d),分区(%d)内存表满单个文件处理完成,短会话时间到离开PickThread.",tabid,datapartid);
            break;
        }

        if(_dp_parallel_load) {
            AutoLock ak(_session_lock);
            _threadmtrows = dtmt.Next()->GetRows();
        }

        //>> begin: fix dma-1326: 单个文件处理完成,判断是否到提交时间,如果到提交时间了就提交文件
        if(_dp_parallel_load && frt== 1) {
            if(_st_session_interval>0 && _sorttm->GetTime() > _st_session_interval &&
               dtmt.Next()->GetRows() > 0) {
                lgprintf("表(%d),分区(%d)内存表未满单个文件处理完成,短会话时间到离开PickThread.",tabid,datapartid);
                break;
            }
        }
        //<< end: fix dma-1326

        double fillrate=(double)dtmt.Next()->GetRows()/dtmt.Next()->GetMaxRows();
        // 6个MT未提交且内存表已填入超过90%, 或者9个MT未提交
        if(!filehold && (uncheckbct>8 || (uncheckbct>5 && fillrate>0.9) )) {
            lgprintf("表(%d),分区(%d)内存表未满单个文件处理完成,短会话时间到离开PickThread.",tabid,datapartid);
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
    // 判断是否所有的slaver节点都已经排序完成
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

        lgprintf("恢复任务tabid(%d),datapartid(%d)状态为重新导出(0).",tabid,datapartid);
        AutoStmt st(dbc);
        st.DirectExecute("update dp.dp_datapart set status=0,launchmaster=0 where tabid=%d and datapartid=%d and status=1 and launchmaster=1",tabid,datapartid);

        ThrowWith("发现任务启动过程可能是与其它进程冲突B,表%d(%d).",tabid,datapartid);
    }
    return false;
}


// 设置集群状态
void DataDump::set_cluster_status(int dbc,int tabid,int datapartid,const char* session_id,const int cluster_status)
{
    AutoStmt st(dbc);
    lgprintf("设置表(%d),分区(%d),会话(%s)的集群状态为(%d).",tabid,datapartid,session_id,cluster_status);
    st.DirectExecute("update dp.dp_datapartext set clusterstatus = %d where tabid = %d and datapartid=%d and sessionid='%s'",
                     cluster_status,tabid,datapartid,session_id);
}

// 获取集群状态
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
    // 判断该表是否所有排序完成的数据都已经提交合并装入
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

// 获取表的包索引字段名称
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


// 获取表的包索引字段名称
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

// 更新短会话id,slaver节点调用
int DataDump::update_new_short_session(SysAdmin &sp,int tabid,int datapartid,bool switch_stsession)
{

    // 获取当前会话短会话ID
    /*
    select stsessionid from dp.dp_datapart a,dp.dp_datapartext b where a.stsessionid=b.sessionid and b.sessiontype=1 and b.commitstatus=-1;
    */
    AutoMt mdf(sp.GetDTS());
    if(switch_stsession) {

        mdf.FetchFirst("select stsessionid,ltsessionid,clusterstatus,launchslavercnt,finishslavercnt from dp.dp_datapart a,dp.dp_datapartext b where a.stsessionid=b.sessionid and  "
                       " a.tabid = b.tabid and a.datapartid = b.datapartid and "
                       /* 必须要等待所有的slaver节点处完成后才能统一更新短会话id */
                       " ((b.launchslavercnt = 0 and b.finishslavercnt = 0) "
                       /* 启动的slaver数小于a.slavercnt 的情况*/
                       /* TODO: 如果slaver没有数据采集,就会导致刚启动就结束了????*/
                       /** 下面进行会话id重复的判断,如果已经重复了，就继续等待 **/
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
        // 判断集群状态
        int cluster_status = mdf.GetInt("clusterstatus",0);
        if(cluster_status == CS_ERROR) {
            ThrowWith("Slaver节点,等待获取短会话ID过程中发现集群当前会话[%s]故障,退出.",mdf.PtrStr("stsessionid",0));
        } else if(cluster_status == CS_MASTER_BUSY) {
            lgprintf("slaver节点[%d]发现master正处于提交数据过程中,继续等待.",slaver_index);
            return -4;
        } else if(cluster_status == CS_MASTER_INIT) {
            lgprintf("slaver节点[%d]发现master正处于集群创建对象过程中,继续等待.",slaver_index);
            return -5;
        }

        // 如果其中一个slaver自身文件已经采集完成,但是其他节点(master,slaver)还在处理数据中
        // 此时不能直接获取会话ID使用
        if(strcmp(stsessionid,mdf.PtrStr("stsessionid",0)) == 0) {
            lgprintf("slaver节点[%d]会话id[%s]，已经使用过不能重用,继续等待.",slaver_index,stsessionid);
            return -2;
        }

        AutoLock lk(&sessionid_lock);
        strcpy(stsessionid,mdf.PtrStr("stsessionid",0));
        if(st_session_interval>0) {
            strcpy(ltsessionid,mdf.PtrStr("ltsessionid",0));
        }
        lgprintf("分布式排序slaver节点[%d]短会话ID[%s]更新成功.",slaver_index,stsessionid);
        lgprintf("分布式排序slaver节点[%d]长会话ID[%s]更新成功.",slaver_index,ltsessionid);
        return 0;
    } else {

        lgprintf("slaver节点[%d]没有发现有可用的短会话ID,程序返回.",slaver_index);

        // 在判断短会话是否在提交状态,如果是继续等待
        mdf.FetchFirst("select clusterstatus,launchslavercnt,finishslavercnt from dp.dp_datapartext  "
                       " where tabid='%d' and datapartid='%d' and sessiontype='%d' and commitstatus = '%d' ",
                       tabid,datapartid,SHORT_SESSION_TYPE,PL_LOAD_DEFAULT);
        ret = mdf.Wait();
        if(ret >0) {
            int cluster_status = mdf.GetInt("clusterstatus",0);
            if(cluster_status == CS_ERROR) {
                ThrowWith("Slaver节点,等待获取短会话ID过程中发现集群当前会话[%s]故障,退出.",mdf.PtrStr("stsessionid",0));
            } else if(cluster_status == CS_MASTER_BUSY) {
                lgprintf("slaver节点[%d]发现master正处于提交数据过程中,继续等待.",slaver_index);
                return -4;
            }
        }

        return -3;
    }
}


// 从数据库采集数据,遇到错误进行排序回滚过程
int DataDump::rollback_sort_session(SysAdmin &sp,const int tabid,const int datapartid,const int origin_status,const bool filesmode)
{

    // 从数据库采集数据，程序退出前：直接将任务状态修改成重新导出，
    // 并执行drop数据库表，并通知排序集群终止所有的排序会话(长短会话数据)。
    AutoStmt st(sp.GetDTS());

    // 1>. 修改任务状态
    if(!b_master_node) { // slaver任务处理结束

        if(p_stDMIBSortLoader!=NULL ) {

            if(e_extract_run_status == ENUM_SLAVER_WAIT_RUN_SESSION || e_extract_run_status == ENUM_RUN_CUR_SESSION) { // fix dma-1998 : slaver 才需要处理的

                // 更新短会话节点数据
                lgprintf("Slaver节点:设置短会话[%s]的finishslavercnt=finishslavercnt+1.",stsessionid);

                st.DirectExecute("update dp.dp_datapartext set finishslavercnt=finishslavercnt+1 "
                                 " where tabid =%d and datapartid =%d and sessionid='%s' and sessiontype=%d ",
                                 tabid,datapartid,stsessionid,SHORT_SESSION_TYPE);
            }


            if(e_extract_run_status == ENUM_RUN_CUR_SESSION) {
                lgprintf("Slaver节点:跳过EmergencyStop表tabid(%d),partid(%d)对应任务的短会话(%s)的排序数据.",tabid,datapartid,stsessionid);
                // DMIBSortLoad_NS::EmergencyStop(p_stDMIBSortLoader->GetJobID());
            }

        } else {
            // 创建集群对象失败,launchslavercnt已经自减少了,无需进行在采集更新短会话节点数据

            lgprintf("Slaver节点:检测到p_stDMIBSortLoader为NULL,无需EmergencyStop表tabid(%d),partid(%d)对应任务的短会话(%s)的排序数据.",tabid,datapartid,stsessionid);
        }

        // 设置集群为不可用状态
        if(e_extract_run_status == ENUM_RUN_CUR_SESSION ) {
            set_cluster_status(sp.GetDTS(),tabid,datapartid,stsessionid,CS_ERROR);
        } else {
            lgprintf("数据抽取运行状态为:ENUM_WAIT_NEW_SESSION,无需设置集群状态为故障.");
        }
    }

    // 2>. 清理表
    if(b_master_node) {

        // 等所有的slaver节点终止后,处理
        while(1) {
            int launchcnt = 0;
            int finishcnt = 0;
            int64 loadrows = 0;
            int cluster_status = 0;
            if(is_all_slaver_node_finish(sp.GetDTS(),tabid,datapartid,stsessionid,
                                         launchcnt,finishcnt,loadrows,cluster_status)) {

                if(cluster_status == CS_ERROR) {
                    lgprintf("MASTER节点,等待slaver节点完成过程中发现集群当前会话[%s]故障,退出.",stsessionid);
                }

                break;
            }
            sleep(60);
            lgprintf("等等排序节点完成,排序节点数[%d]已完成节点[%d],等待60秒.",launchcnt,finishcnt);

            if(cluster_status == CS_ERROR) {
                lgprintf("MASTER节点,等待slaver节点完成过程中发现集群当前会话[%s]故障,退出.",stsessionid);
            }
        }

        // 修改任务状态
        AutoStmt st(sp.GetDTS());

        if(!filesmode) { // 数据库采集:

            if(origin_status == 0) {
                lgprintf("回退tabid(%d),partid(%d)的任务状态为:重新导出(0).",tabid,datapartid);

                st.DirectExecute("update dp.dp_datapart set status=0,launchmaster = 0, blevel=ifnull(blevel,0)+100 "
                                 "where tabid=%d and datapartid=%d",
                                 tabid,datapartid);
            } else if(origin_status == 73) {
                lgprintf("回退tabid(%d),partid(%d)的任务状态为:数据补入(73).",tabid,datapartid);

                st.DirectExecute("update dp.dp_datapart set status=73,launchmaster = 0, blevel=ifnull(blevel,0)+100 "
                                 "where tabid=%d and datapartid=%d",
                                 tabid,datapartid);
            } else {
                lgprintf("任务tabid(%d),partid(%d)的起始状态为(%d),未进行回退任务处理.",tabid,datapartid,origin_status);
            }

        } else {
            if(origin_status == 0 || origin_status == 72) {
                lgprintf("回退tabid(%d),partid(%d)的任务状态为:文件续传(72).",tabid,datapartid);

                st.DirectExecute("update dp.dp_datapart set status=72,launchmaster = 0, blevel=ifnull(blevel,0)+100 "
                                 "where tabid=%d and datapartid=%d",
                                 tabid,datapartid);
            } else if(origin_status == 73) {
                lgprintf("回退tabid(%d),partid(%d)的任务状态为:数据补入(73).",tabid,datapartid);

                st.DirectExecute("update dp.dp_datapart set status=73,launchmaster = 0, blevel=ifnull(blevel,0)+100 "
                                 "where tabid=%d and datapartid=%d",
                                 tabid,datapartid);
            } else {
                lgprintf("任务tabid(%d),partid(%d)的起始状态为(%d),未进行回退任务处理.",tabid,datapartid,origin_status);
            }

        }

        // 3>. 通知排序集群终止单次排序
        if(p_stDMIBSortLoader!=NULL ) {

            if(e_extract_run_status == ENUM_RUN_CUR_SESSION) {
                lgprintf("EmergencyStop表tabid(%d),partid(%d)对应任务的短会话(%s)的排序数据.",tabid,datapartid,stsessionid);
                DMIBSortLoad_NS::EmergencyStop(p_stDMIBSortLoader->GetJobID());
            }

            // 设置集群为不可用状态
            set_cluster_status(sp.GetDTS(),tabid,datapartid,stsessionid,CS_ERROR);
        }
        if(p_ltDMIBSortLoader!=NULL) {
            // 长会话上的数据回滚过程实现,参考:DMA-1321
            lgprintf("EmergencyStop表tabid(%d),partid(%d)对应任务的长会话(%s)的排序数据.",tabid,datapartid,ltsessionid);
            DMIBSortLoad_NS::EmergencyStop(p_ltDMIBSortLoader->GetJobID());

            // 设置集群为不可用状态
            set_cluster_status(sp.GetDTS(),tabid,datapartid,ltsessionid,CS_ERROR);
        }

    }
    return 0;
}

// 生成一个新的短会话id,master节点调用
int DataDump::update_new_short_session(SysAdmin &sp,int tabid,int datapartid,int clusterstatus)
{
    // 记录当前任务的会话ID
    AutoStmt st(sp.GetDTS());
    st.DirectExecute("update dp.dp_datapart set stsessionid='%s' where tabid=%d and datapartid = %d",
                     stsessionid,tabid,datapartid);

    // 添加短会话id
    st.Prepare("insert into dp.dp_datapartext(datapartid,tabid,sessionid,sessiontype,launchslavercnt,"
               " finishslavercnt,commitstatus,sortbegintime,clusterstatus) "
               " values(%d,%d,'%s',%d,0,0,%d,now(),%d) ",
               datapartid,tabid,stsessionid,SHORT_SESSION_TYPE,PL_LOAD_DEFAULT,clusterstatus);
    st.Execute(1);
    st.Wait();

    st_parallel_load_rows = 0;


    lgprintf("master节点创建创建短会话ID,更新master节点内存中短会话ID[%s]到数据库中,准备给slaver节点用.",stsessionid);

    return 0;
}


// 生成一个新的短会话id,master节点调用
int DataDump::create_new_short_session(SysAdmin &sp,int tabid,int datapartid)
{
    // 获取当前会话ID
    std::string session_id = generate_session_id(sp,tabid,datapartid);

    sessionid_lock.Lock();
    lgprintf("master节点创建创建短会话ID,更新master节点内存中短会话ID[%s-->%s].",stsessionid,session_id.c_str());
    strcpy(stsessionid,session_id.c_str());
    sessionid_lock.Unlock();

    return 0;
}

// 创建长短会话排序对象
int DataDump::create_DMIBSortLoader(SysAdmin &sp,int tabid,int datapartid,bool create_lt_session)
{
    mytimer  tm1;
    tm1.Clear();
    tm1.Start();

    if(p_stDMIBSortLoader != NULL) {
        delete p_stDMIBSortLoader;
        p_stDMIBSortLoader = NULL;
    }

    // 只有master节点,排序对象才有效
    if(create_lt_session && b_master_node) {
        if(p_ltDMIBSortLoader != NULL) {
            delete p_ltDMIBSortLoader;
            p_ltDMIBSortLoader = NULL;
        }
    }

    // 短会话ID
    std::string st_job_id(stsessionid);


    // 获取上一次排序用的会话id
    std::string last_sort_session_id;
    if(strlen(sortsampleid)>0) {
        last_sort_session_id = std::string(sortsampleid);
    } else {
        last_sort_session_id = std::string("");
    }

    if(last_sort_session_id.size()>0) {
        lgprintf("排序采样ID为:%s",last_sort_session_id.c_str());
    } else {
        lgprintf("排序采样ID为:NULL");
    }

    lgprintf("准备创建短会话[%s],长会话[%s]的集群排序对象.",stsessionid,st_session_interval > 0 ? ltsessionid : "NULL");

    // 第一个包的记录数
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

    // 开启调试模式后,TIMEOUT设置足够大,以便调试集群用
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

    // 字符串型，指定 riak 连接字符串，其内容定义与 RiakCluster::Rebuild 的 conn_str 参数一致
    char* q=getenv(RIAK_HOSTNAME_ENV_NAME_CLI);
    if(NULL == q || strlen(q) <=1) {
        lgprintf("环境变量:RIAK_HOSTNAME未设置,数据入库为分布式排序AP版本.");
    } else {
        lgprintf("环境变量:RIAK_HOSTNAME=[%s],数据入库为分布式排序Cloud版本.",q);

        std::string _connect_riak_str = std::string(q);
        st_sort_param = CR_Class_NS::str_setparam(st_sort_param,DMIBSORTLOAD_PN_RIAK_CONNECT_STR,_connect_riak_str);
        // lt_sort_param = CR_Class_NS::str_setparam(lt_sort_param,LIBIBLOAD_PN_RIAK_CONNECT_STR,_connect_riak_str);

        // LIBIBLOAD_PN_RIAK_BUCKET 字符串型，指定用于保存数据的 riak bucket 名称，目前版本 CDM 指定为 "IB_DATAPACKS"
        _connect_riak_str = std::string("IB_DATAPACKS");
        st_sort_param = CR_Class_NS::str_setparam(st_sort_param,DMIBSORTLOAD_PN_RIAK_BUCKET,_connect_riak_str);
        //lt_sort_param = CR_Class_NS::str_setparam(lt_sort_param,LIBIBLOAD_PN_RIAK_BUCKET,_connect_riak_str);
    }

    st_sort_param = CR_Class_NS::str_setparam(st_sort_param,DMIBSORTLOAD_PN_LAST_JOB_ID,last_sort_session_id);

    // 长会话和短会话的设置是不应该完全相同的
    lt_sort_param = st_sort_param;

    // 排序后保存临时文件不进行删除操作,直接用于长会话排序
    if(st_session_interval >0) {
        st_sort_param = CR_Class_NS::str_setparam_bool(st_sort_param, DMIBSORTLOAD_PN_KEEP_SORT_TMP,true);
    } else {
        // 只有短会话的时候,调用finish完成后,就会将短会话临时数据清除
        st_sort_param = CR_Class_NS::str_setparam_bool(st_sort_param, DMIBSORTLOAD_PN_KEEP_SORT_TMP,false);
    }

    try {
        // 短会话排序对象
        p_stDMIBSortLoader = new DMIBSortLoad(st_job_id,b_master_node,&st_sort_param);

        // 只有master节点,排序对象才有效
        if(st_session_interval >0 && create_lt_session && p_ltDMIBSortLoader == NULL && b_master_node) { // 长会话排序对象
            std::string lt_job_id(ltsessionid);

            // 每次都以master节点启动
            p_ltDMIBSortLoader = new DMIBSortLoad(lt_job_id,b_master_node,&lt_sort_param);

            // 如果是文件续传的流程进入的，需要将原来的短会话数据添加进入
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
                        lgprintf("向长会话[%s]中添加已经存储的短会话[%s]的[%ld]行数据......",ltsessionid,mdf.PtrStr("sessionid",i),_rownums);

                        int _r = p_ltDMIBSortLoader->PushLines(tmp_fls);
                        if(_r != 0) {
                            ThrowWith("向长会话[%s]中添加已经存储的短会话[%s]的[%ld]行数据失败.",ltsessionid,mdf.PtrStr("sessionid",i),_rownums);
                        }
                    } else {
                        lgprintf("无需向长会话[%s]中添加已经存储的短会话[%s]的[%ld]行数据......",ltsessionid,mdf.PtrStr("sessionid",i),_rownums);
                    }
                }

                add_stsessions_to_ltsession = false;
            }

        }

        st_parallel_load_rows = 0;

        tm1.Stop();
        double tm_end = tm1.GetTime();
        tm1.Clear();
        lgprintf("DataDump::create_DMIBSortLoader创建对象使用时间(%0.4f)秒(精确度秒,不含sleep等耗时).",tm_end);

    } catch(...) {
        // try to fix dma-1826
        lgprintf("DataDump::create_DMIBSortLoader创建对象失败.");
        return -1;
    }
    return 0;
}

// 开始master节点的数据导出
int DataDump::start_master_dump_data(SysAdmin &sp,int tabid,int datapartid,const bool keepfiles)
{
    // 记录当前任务的会话ID
    AutoStmt st(sp.GetDTS());
    st.DirectExecute("update dp.dp_datapart set launchmaster=1, stsessionid='%s' ,ltsessionid='%s' where tabid=%d and datapartid = %d",
                     stsessionid,ltsessionid,tabid,datapartid);

    // 添加长会话ID
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

    // 添加短会话id
    st.Prepare("insert into dp.dp_datapartext(datapartid,tabid,sessionid,sessiontype,launchslavercnt,"
               " finishslavercnt,commitstatus,sortbegintime) "
               " values(%d,%d,'%s',%d,0,0,%d,now()) ",
               datapartid,tabid,stsessionid,SHORT_SESSION_TYPE,PL_LOAD_DEFAULT);
    st.Execute(1);
    st.Wait();

    // 设置集群状态
    set_cluster_status(sp.GetDTS(),tabid,datapartid,stsessionid,CS_MASTER_INIT);

    lgprintf("分布式排序master节点启动设置成功.");
}

// 开始slaver节点的数据导出
int DataDump::start_slaver_dump_data(SysAdmin &sp,int tabid,int datapartid)
{
    AutoMt mt_slaver(sp.GetDTS(),100);
    mt_slaver.FetchFirst("select tabid,datapartid,ltsessionid,stsessionid from dp.dp_datapart where tabid = %d and datapartid =%d",
                         tabid,datapartid);
    int ret = mt_slaver.Wait();

    //>> begin: try to fix dma-2007:slaver_index 唯一值的获取
    lgprintf("分布式排序slaver节点slaver_index准备获取......");
    bool ec=wociIsEcho();
    wociSetEcho(FALSE);
    {
        AutoStmt _st(sp.GetDTS());
        _st.DirectExecute("lock tables dp.dp_datapartext write");
    }
    //>> end: try to fix dma-2007:slaver_index 唯一值的获取

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
    // 插入表dp.dp_datapartext中,DestLoader::Load的时候在使用内部的值


    // slaver节点的索引值
    {
        // 获取dp.dp_datapartext表对应slaver_index的值
        mt_slaver.FetchFirst("select launchslavercnt from dp.dp_datapartext where tabid = %d and datapartid =%d and sessionid='%s' ",
                             tabid,datapartid,stsessionid);
        ret = mt_slaver.Wait();
        slaver_index = mt_slaver.GetInt("launchslavercnt",0);

        //>> begin: try to fix dma-2007:slaver_index 唯一值的获取
        {
            AutoStmt _st(sp.GetDTS());
            _st.DirectExecute("unlock tables");
        }
        wociSetEcho(ec);
        //>> end: try to fix dma-2007:slaver_index 唯一值的获取
    }


    lgprintf("分布式排序slaver节点[%d]启动设置成功.",slaver_index);
}



// 增加slaver节点的启动个数
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
        lgprintf("长会话[%s]对应的slaver节点启动数自增加1[%s].",ltsessionid,ret==1?"成功":"失败");
    } else {
        lgprintf("短会话[%s]对应的slaver节点启动数自增加1[%s].",stsessionid,ret==1?"成功":"失败");
    }

    return ret;
}


// 减少slaver节点的启动个数
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
        lgprintf("长会话[%s]对应的slaver节点启动数自减少1[%s].",ltsessionid,ret==1?"成功":"失败");
    } else {
        lgprintf("短会话[%s]对应的slaver节点启动数自减少1[%s].",stsessionid,ret==1?"成功":"失败");
    }

    return ret;
}



// 获取开始生成数据起始位置
bool ParallelLoad::get_commit_sort_info(SysAdmin &sp,
                                        int tabid,
                                        int datapartid,
                                        int64& startloadrow,
                                        int& startpackno,
                                        int& startpackobj)
{

    // 获取该分区装入数据的最后一个包号和该包需要的记录数
    char basepth[300];
    strcpy(basepth,sp.GetMySQLPathName(0,"msys")); // 获取根目录

    // 1. 从dp.dp_datapart表中,获取任务状态为3的任务信息(会话ID不为null)
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
        <<获取本次装入数据第一个包的位置>>
        如果[dpadmin -f dump] 与mysqld不在同一主机部署的时候
        需要将$DATAMERGER_HOME/var目录通过nfs挂载到本地磁盘,通过打开文件的方式,获取本次装入第一个包的位置
        如果[dpadmin -f dump]部署在多台主机的情况下,增加软件部署实施的复杂度
        */
        sprintf(tabpath,"%s/%s/%s.bht",basepth,dbname,tabname);

        // 获取表的包号和记录数
        get_table_pack_obj_info(basepth,dbname,tabname,tb_pack_no,tb_obj_no);

        // 获取分区的对应的最后一个包号和记录数
        append_part = get_part_pack_info(basepth,dbname,tabname,partfullname,part_last_pack_no,part_last_pack_obj);
#else
        /*
        <<获取本次装入数据第一个包的位置>>
        在DMA-1815问题中进行跟进解决:使用如下语句进行查询
        [show table status from  dma1978 where Name='xxx' and engine='brighthouse' and Create_time<now()]
        从查询的comment中获取结果

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
            comment 返回格式内容:
            0x0871#tab_pack_no#tab_obj_no#'last_part_fullname'#last_part_lastpackno#last_part_lastpackobj
            例如:
            0x0871#123400000#10000000000231323#'p10000000000000000000000000'#123214000000#34342
        */
        mdf.FetchFirst("show table status from %s where Name='%s' and engine='brighthouse' and Create_time<now()",dbname,tabname);
        ret = mdf.Wait();
        if(ret <=0) {
            ThrowWith("数据库表:%s.%s不存在,无法获取数据装入位置,准备退出.",dbname,tabname);
        }

        std::string str_load_info = std::string(mdf.PtrStr("Comment",0));
        size_t found = str_load_info.find("0x0871#");
        if(found == std::string::npos) {
            ThrowWith("数据库表:%s.%s不存在,无法获取数据装入位置<0x0871>,准备退出.",dbname,tabname);
        }

        std::string str_found = str_load_info.substr(found);   // 0x0871#...#...#'...'#...#
        lgprintf("获取到数据装入位置信息:%s",str_found.c_str());

        std::string str_found_numbers = "";     // 0x0871#...#...#...#
        std::string lst_load_part_name="";
        {
            const char enclose_char='\'';
            size_t _s_pos = str_found.find_first_of(enclose_char);
            size_t _e_pos = str_found.find_last_of(enclose_char);
            lst_load_part_name = str_found.substr(_s_pos+1,(_e_pos-_s_pos-1));

            // 去除name部分的截取
            str_found_numbers = str_found.substr(0,_s_pos-1); // skip last #
            str_found_numbers += str_found.substr(_e_pos+1);  // skip last '
        }

        sscanf(str_found_numbers.c_str(),"0x0871#%d#%lld#%d#%d",&tb_pack_no,&tb_obj_no,&part_last_pack_no,&part_last_pack_obj);

        if(strcasecmp(lst_load_part_name.c_str(),partfullname) == 0) {
            append_part = 1;
        }
#endif

        if(append_part == 1) { // 找到原来分区
            if(part_last_pack_no==tb_pack_no-1) { // 该分区就是表的最后一部分装入的数据
                if(part_last_pack_obj != 65536) {
                    startloadrow = part_last_pack_obj;             // 第一个包所要数据的偏移量
                } else {
                    startloadrow = 0;                              // 第一个包所要数据的偏移量
                }
            } else {  // 该分区不是最后一个分区
                part_last_pack_no = tb_pack_no+1;
                part_last_pack_obj = 65536;
                startloadrow = 0;
            }
        } else { // 没有找到分区
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

// 判断要启动的任务是否应该是master节点
bool DataDump::is_master_node(int dbc,int tabid,int datapartid)
{
    // 判断是否是master节点,排序的第一个节点(数据提交的节点)
    AutoMt mt_is_master(dbc,1000);
    mt_is_master.FetchFirst("select launchmaster from dp.dp_datapart "
                            " where tabid = %d and datapartid =%d limit 2",tabid,datapartid);

    // dp.dp_datapart.launchmaster : 是否已经启动master节点,1:已经启动,0:没有启动'
    mt_is_master.Wait();
    return  mt_is_master.GetInt(0,0) == 0;
}

// >> begin: fix dma-1871 , add by liujs 20160104
// 解决dpadmin -f dump每次进入只处理一个采集任务的问题[单个slaver任务重复的执行,其他任务执行不到]
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

    // 执行的slaver任务队列
    stru_taskinfo_vec  run_slaver_taskinfo_vec;
    bool run_slaver_taskinfo_finish = false;
    long run_slaver_taskinfo_deal_rows = 0;   // slaver任务处理过的数据行数

lable_dump_slaver_begin: // 开始进入采集过程的点slaver

    // 如果是并行采集数据模式,优先处理正在导出的数据
    bool deal_slaver_node = false;
    if(dp_parallel_load) { // 只有从文件采集数据的时候,才能够并发的采集数据,数据库采集数据不能并发执行

        // 一个表如果有其他分区的数据长会话还在执行的时候，那么该表的该分区就不允许并发任务进来执行
        mdf.FetchAll("select a.*,(select count(1) from dp.dp_datapart b where a.tabid=b.tabid and b.status=1) busyct "
                     " from dp.dp_datapart a ,dp.dp_datapartext c where (status=1 and lower(extsql) like 'load %%') "
                     " and a.tabid = c.tabid AND a.datapartid = c.datapartid AND a.stsessionid = c.sessionid "
                     " AND c.sessiontype = '%d' and c.commitstatus='%d' AND c.launchslavercnt < a.slavercnt AND  begintime<now() %s "
                     /*>> ADD BEGIN: FIX DMA-1349: 单个表不允许多分区同时进行数据采集 */
                     " and not exists (select 1 from dp.dp_datapart x where x.tabid=a.tabid and a.datapartid != x.datapartid and x.status = 1 "
                     /* 长会话在处理的时候,短会话要求可以并发进入 DMA-1621 */
                     " and not exists (select 1 from dp.dp_datapartext y where y.tabid = a.tabid and y.datapartid = a.datapartid and y.sessiontype='%d' )"
                     " )"
                     /*<< ADD END */
                     "  order by blevel,busyct,touchtime,tabid,datapartid limit 2 ",
                     SHORT_SESSION_TYPE,PL_LOAD_DEFAULT,sp.GetNormalTaskDesc(),LONG_SESSION_TYPE);

        _ret = mdf.Wait();
        if(_ret >=1) deal_slaver_node = true;

        paraller_sorttm.Restart();
    }

lable_dump_master_begin: // 开始进入采集过程的点master
    if(!deal_slaver_node) { //处理普通的任务
        if(systype==NULL) {
            if(!dp_parallel_load) {
                // 原流程
                mdf.FetchAll("select a.*,(select count(1) from dp.dp_datapart b where a.tabid=b.tabid and b.status=1) busyct "
                             "from dp.dp_datapart a where (status=0 or (status in(72,73) and lower(extsql) like 'load %%')) and begintime<now() %s "
                             "order by blevel,busyct,touchtime,tabid,datapartid limit 2",sp.GetNormalTaskDesc());

            } else {
                // 同一个表不能进行多分区并发
                // 如果一个表的其他分区短会话都处理结束(长会话还在生成数据时候),就允许新的分区进入
                mdf.FetchAll("select a.*,(select count(1) from dp.dp_datapart b where a.tabid=b.tabid and b.status=1) busyct "
                             "from dp.dp_datapart a where (status=0 or (status in(72,73) and lower(extsql) like 'load %%')) and begintime<now() %s "

                             /*>> ADD BEGIN: FIX DMA-1372: 第一个启动的任务,必须作为master节点启动*/
                             " and a.launchmaster = 0 "
                             /*<< ADD END*/

                             /*>> ADD BEGIN: FIX DMA-1349: 单个表不允许多分区同时进行数据采集
                             " and not exists (select 1 from dp.dp_datapart x where x.tabid=a.tabid and a.datapartid != x.datapartid and x.status = 1) "
                             << ADD END */

                             /* 同一个表,长会话在处理的时候,允许其他短会话进入 */
                             " and not exists (select 1 from dp.dp_datapart x ,dp.dp_datapartext y where x.tabid=a.tabid and a.datapartid != x.datapartid and x.status = 1 "
                             " and y.tabid = x.tabid and y.datapartid = x.datapartid and y.sessiontype = 1 and y.commitstatus in(0,1,-1)) "

                             "order by blevel,busyct,touchtime,tabid,datapartid limit 2",sp.GetNormalTaskDesc());

            }
        } else {
            printf("限制类型：%s\n",systype);
            if(!dp_parallel_load) {
                // 原流程
                mdf.FetchAll("select * from dp.dp_datapart where (status=0 or (status in(72,73) and lower(extsql) like 'load %%')) "
                             " and exists(select 1 from dp.dp_datasrc a where a.sysid=dp_datapart.srcsysid and a.systype in ( %s) ) and begintime<now() %s "
                             "order by blevel,touchtime,tabid,datapartid limit 2",systype,sp.GetNormalTaskDesc ());
            } else {
                // 同一个表不能进行多分区并发
                mdf.FetchAll("select a.* from dp.dp_datapart a where (a.status=0 or (a.status in(72,73) and lower(a.extsql) like 'load %%')) "
                             " and exists(select 1 from dp.dp_datasrc b where b.sysid=a.srcsysid and b.systype in ( %s) ) and a.begintime<now() %s "

                             /*>> ADD BEGIN: FIX DMA-1372: 第一个启动的任务,必须作为master节点启动*/
                             " and a.launchmaster = 0 "
                             /*<< ADD END*/

                             /*>> ADD BEGIN: FIX DMA-1349: 单个表不允许多分区同时进行数据采集 */
                             " and not exists (select 1 from dp.dp_datapart x where x.tabid=a.tabid and a.datapartid != x.datapartid and x.status = 1) "
                             /*<< ADD END */

                             "order by blevel,touchtime,tabid,datapartid limit 2",systype,sp.GetNormalTaskDesc ());
            }
        }

        _ret = mdf.Wait();
        if(_ret<1) {
            if(dp_parallel_load) {
                lgprintf("没有获取到可以执行的master任务信息,进程退出.");
            } else {
                lgprintf("没有获取到可以执行的采集任务信息,进程退出.");
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
            run_slaver_taskinfo_vec.clear(); // master 任务进入了,需要清空
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

            if(!deal_slaver_node) { // master 任务处理第一个即可
                break;
            } else { // slaver 任务数据

                run_slaver_taskinfo_deal_rows = 0;

                if(!exist_taskinfo(run_slaver_taskinfo_vec,tabid,datapartid)) { // 找到可以处理的slaver任务
                    stru_taskinfo st;
                    st.tabid = tabid;
                    st.datapartid = datapartid;
                    run_slaver_taskinfo_vec.push_back(st);
                    break;
                } else {
                    continue;  // 上一次已经处理过的slaver任务数据,本次直接忽略
                }
            }
        }

        if( i== _ret && deal_slaver_node ) { // 所有的slaver都处理过了,就继续处理第一个
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
        lgprintf("没有获取到可以执行的slaver任务信息,进程退出.");
        return 0;
    }

    if(tabid<1) {
        lgprintf("没有获取到可以执行的任务信息,进程退出.");
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
            // 设置驱动日志接口
            CR_Class_NS::dprintfx_filename = std::string(_logfn);
            lgprintf("驱动层的会话日志输出到文件[%s]中.",_logfn);
        }
    }

    sorttm.Restart();
    fiotm.Clear();
    adjtm.Clear();
    sp.GetSoledIndexParam(datapartid,&dp,tabid);
    sp.OutTaskDesc("执行数据导出任务: ",tabid,datapartid);
    if(xmkdir(dp.tmppath[0])) {
        sp.log(tabid,datapartid,DUMP_CREATE_PATH_ERROR,"临时主路径无法建立,表:%d,数据组:%d,路径:%s.",tabid,datapartid,dp.tmppath[0]);
        ThrowWith("临时主路径无法建立,表:%d,数据组:%d,路径:%s.",tabid,datapartid,dp.tmppath[0]);
    }
    AutoHandle srcdbc;
    AutoHandle fmtdbc;
    // datapartid对应的源系统，不一定是格式表对应的源系统
    // Jira:DM-48
    try {
        srcdbc.SetHandle(sp.BuildSrcDBC(tabid,datapartid));
        fmtdbc.SetHandle(sp.BuildSrcDBC(tabid,-1));
    } catch(...) {
        sp.log(tabid,datapartid,DUMP_CREATE_DBC_ERROR,"源(目标)数据源连接失败,表:%d,数据组:%d",tabid,datapartid);
        AutoStmt st(dtdbc);
        st.DirectExecute("update dp.dp_datapart set blevel=ifnull(blevel,0)+100 where tabid=%d and datapartid=%d",
                         tabid,datapartid);
        sp.logext(tabid,datapartid,EXT_STATUS_ERROR,"");
        throw;
    }

    AutoMt srctstmt(0,10);
    int partoff=0;
    try {
        //如果格式表与目标表的结构不一致
        sp.BuildMtFromSrcTable(fmtdbc,tabid,&srctstmt);
        srctstmt.AddrFresh();
        int srl=sp.GetMySQLLen(srctstmt);//wociGetRowLen(srctstmt);
        char tabname[150];
        sp.GetTableName(tabid,-1,tabname,NULL,TBNAME_DEST);
        AutoMt dsttstmt(dtdbc,10);
        dsttstmt.FetchFirst("select * from dp.dp_datapart where tabid=%d and status=5 ",tabid);
        int ndmprn=dsttstmt.Wait();
        //天津移动发现的问题：两个分区并行导出，第二个分区在第一个开始整理后启动导出，dp_table.lstfid被复位，整理过程中文件序号错乱
        // 增加下面的5行代码
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
            sp.log(tabid,datapartid,DUMP_DST_TABLE_ERROR,"目标表%d不存在或结构错误,需要重新构造,已经设置了重构标记.",tabid);
            AutoStmt st(dtdbc);
            st.DirectExecute("update dp.dp_table set cdfileid=0 where tabid=%d",tabid);
            lgprintf("目标表%d不存在或结构错误,需要重新构造,已经设置了重构标记.",tabid);
            sp.logext(tabid,datapartid,EXT_STATUS_ERROR,"");

            throw;
        }
        if(srctstmt.CompareMt(dsttstmt)!=0 ) {
            if(tstrn>0 && ndmprn>0) {
                sp.log(tabid,datapartid,DUMP_DST_TABLE_FORMAT_MODIFIED_ERROR,
                       "表%s中已经有数据，但源表(格式表)格式被修改，不能导入数据，请导入新的(空的)目标表中。",tabname);
                ThrowWith("表%s中已经有数据，但源表(格式表)格式被修改，不能导入数据，请导入新的(空的)目标表中。",tabname);
            }
            lgprintf("源表数据格式发生变化，重新解析源表... ");
            if(tstrn==0) {
                sp.CreateDT(tabid);
                sp.Reload();
                sp.GetSoledIndexParam(datapartid,&dp,tabid);
            } else {
                //全部数据分组重新导入数据,可以允许结构暂时不一致
                //由于目标表有数据，暂时不修改dt_table.recordlen
                dp.rowlen=srl;
            }
        } else if(srl!=dp.rowlen) {
            lgprintf("目标表中的记录长度错误，%d修改为%d",dp.rowlen,srl);
            sp.log(tabid,datapartid,DUMP_DST_TABLE_RECORD_LEN_MODIFY_NOTIFY,"目标表中的记录长度错误，%d修改为%d",dp.rowlen,srl);
            wociClear(dsttstmt);
            AutoStmt &st=dsttstmt.GetStmt();
            st.Prepare("update dp.dp_table set recordlen=%d where tabid=%d",srl,tabid);
            st.Execute(1);
            st.Wait();
            dp.rowlen=srl;
        }
        if(ndmprn==0 && tstrn==0) {
            //复位文件编号,如果目标表非空,则文件编号不置位
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
    //>> Begin:DMA-458,内存记录条数最大值限制,20130129
    if(realrn > MAX_ROWS_LIMIT-8) {
        realrn = MAX_ROWS_LIMIT - 8;
        lgprintf("导出记录条数已经超过2G条，现将其从%d条修改为%d条,导出过程继续执行",memlimit/dp.rowlen,realrn);
    }
    //<< End:
    lgprintf("开始导出数据,数据抽取内存%ld字节(折合记录数:%d)",realrn*dp.rowlen,realrn);
    sp.log(tabid,datapartid,DUMP_BEGIN_DUMPING_NOTIFY,"开始数据导出:数据块%d字节(记录数%d),日志文件 '%s'.",realrn*dp.rowlen,realrn,wociGetLogFile());
    sp.log(tabid,datapartid,DUMP_BEGIN_DUMPING_NOTIFY,"开始导出表%d,分区%d数据",tabid,datapartid);
    //if(realrn>dp.maxrecnum) realrn=dp.maxrecnm;
    //CMNET: 重入时不删除数据
    //2011/7/1 西安测试临时修改：不清除上次数据
    //if(false)
    //JIRA DMA-35
    // 2012/02/10: 文件方式导入时运行并行导入和并行整理，因此只在初始导入时运行做数据清理
    AutoMt clsmt(dtdbc,1000);

    add_stsessions_to_ltsession =false;

    if(!deal_slaver_node) {// master节点,直接将任务设置为正在导致即可

        //>> begin: try to fix dma-1978:解决短会话的重入问题
        if(dp_parallel_load) // fix sasu-372
        {
            AutoMt _tmt(dtdbc,1000);
            _tmt.FetchAll("select * from  ( select tabid,datapartid,status,(select count(1) from dp.dp_datapartext b where a.tabid=b.tabid and a.datapartid=b.datapartid)  scnt "
                          " from dp.dp_datapart a where  tabid =  %d  and status=1 group by a.tabid ,a.datapartid ) tmp where  scnt=0",tabid);
            int _rt=_tmt.Wait();
            if(_rt>0) {
                lgprintf("表%d,分区%d 开始导出发现该表其他分区已经在导出,不能进行并发导出开始,程序退出.",tabid,datapartid);
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
        if(!keepfiles && !addfiles && !deal_slaver_node) { //master 节点进入: slaver 节点起来的时候,不能删除dp.dp_filelog的数据

            lgprintf("任务从[重新导出<0>]进入,清除上次导出的数据...");

            bool delete_partition = false;
            int clsrn = 0;
            if(!dp_parallel_load) {
                clsmt.FetchAll("select tabid from dp.dp_datafilemap where datapartid=%d and tabid=%d and procstatus in(2,3) limit 100",datapartid,tabid);
                clsrn=clsmt.Wait();
                if(clsrn >0) {
                    AutoStmt st(dtdbc);
                    st.DirectExecute("update dp.dp_datapart set blevel=blevel+100 where datapartid=%d and tabid=%d",datapartid,tabid);
                    lgprintf("表%d,分区%d,已经装入过数据,删除分区后继续进行重新导出!",tabid,datapartid);
                }

            } else {

                clsmt.FetchAll("select tabid from dp.dp_datapartext where datapartid=%d and tabid=%d and commitstatus in(2,3) limit 100",datapartid,tabid);
                clsrn=clsmt.Wait();
                if(clsrn >0) {
                    AutoStmt st(dtdbc);
                    st.DirectExecute("update dp.dp_datapart set blevel=blevel+100 where datapartid=%d and tabid=%d",datapartid,tabid);
                    lgprintf("表%d,分区%d,已经装入过数据,删除分区后继续进行重新导出!",tabid,datapartid);
                }

                {
                    // 删除合并完成后的数据
                    DestLoader rm_part(&sp);
                    // 先删除合并后的装入数据
                    rm_part.clear_has_merged_data(tabid,datapartid);

                    // 清除没有完成的会话id
                    clsmt.FetchAll("select sessionid from dp.dp_datapartext where tabid =%d and datapartid = %d and commitstatus in(-1,0) ",tabid,datapartid);
                    int _rt = clsmt.Wait();
                    char cancle_session_cmd[256];
                    for(int i=0; i<_rt; i++) {
                        sprintf(cancle_session_cmd,"cfm-toolbox slcancel %s",clsmt.PtrStr("sessionid",i));
                        lgprintf("远程终止会话,运行命令:%s ",cancle_session_cmd);
                        int __r = CR_Class_NS::system(cancle_session_cmd);
                        lgprintf("远程终止会话,运行命令:%s ,return %d",cancle_session_cmd,__r);
                    }

                    // 清除dp.dp_datapartext 中的记录
                    AutoStmt st(dtdbc);
                    st.DirectExecute("delete from dp.dp_datapartext where tabid=%d and datapartid=%d",tabid,datapartid);
                }
            }

            // 删除分区动作
            if(clsrn >0) {
                delete_partition = true;
            }

            AutoStmt st(dtdbc);
            clsrn=0;
            do {
                int i;
                // 为什么每次删除100行记录,FetchAll 可能会超过100行
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


            // 删除采集的文件记录
            st.Prepare("delete from dp.dp_filelog where tabid=%d and datapartid=%d %s",tabid,datapartid,
                       keepfiles? " and status<>2":"");
            st.Execute(1);
            st.Wait();

            // 删除整理好的(等待装入的文件)
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

                // 删除已经装入的表的对应分区
                // 1:保留表结构删除表
                // 2:保留表结构删除分区
                // 11:不保留表结构删除表
                // 12:不保留表结构删除分区
                int ret = rm_part.RemoveTable(tabid,datapartid,true);
                if(ret == 1) { // 整个表删除了,需要重建表结构,下次进入在执行
                    lgprintf("tabid(%d),datapartid(%d)删除分区返回(%d),恢复任务状态为:status(%d)",tabid,datapartid,ret,origion_status);
                    sp.UpdateTaskStatus((TASKSTATUS)origion_status,tabid,datapartid);
                    return 0;
                }
            }

        } else if(keepfiles && !dp_parallel_load) { //文件续传检查数据一致性(bhloader入库)
            // 获取dp.dp_middledatafile中已经提交的文件记录数
            clsmt.FetchAll("select sum(recordnum) srn from dp.dp_middledatafile where tabid=%d and datapartid=%d and procstate<>0",
                           tabid,datapartid);
            clsmt.Wait();
            if(wociIsNull(clsmt,0,0)) {
                ThrowWith("表(%d),分区(%d),在临时文件表dp.dp_middledatafile中不存在记录,不能进行文件续传操作.\n",tabid,datapartid);
            }
            double srn=clsmt.GetDouble("srn",0);

            // 获取dp.dp_filelog中已经提交的文件记录数
            clsmt.FetchAll("select sum(recordnum) srn from dp.dp_filelog where tabid=%d and datapartid=%d and status=2",
                           tabid,datapartid);
            clsmt.Wait();

            if(wociIsNull(clsmt,0,0)) {
                ThrowWith("表(%d),分区(%d),在临时文件表dp.dp_filelog中不存在记录,不能进行文件续传操作.\n",tabid,datapartid);
            }

            if(srn!=clsmt.GetDouble("srn",0)) {
                sp.log(tabid,datapartid,DUMP_FILE_LINES_ERROR,"中间数据%.0f行,数据文件%.0f行，不一致，不能继续上一次数据抽取，建议重新导入。",srn,clsmt.GetDouble("srn",0));
                ThrowWith("中间数据%.0f行,数据文件%.0f行，不一致，不能继续上一次数据抽取，建议重新导入。",srn,clsmt.GetDouble("srn",0));
            }

            //删除上一次未正常解析并存入dp.dp_middledatafile的文件
            {
                AutoStmt st(dtdbc);
                if(st.DirectExecute("delete from dp.dp_filelog where  tabid=%d and datapartid=%d and status<>2",tabid,datapartid)>0) {
                    lgprintf("有部分文件上次异常退出时未提交到临时文件，需要从备份目录补，请确保备份采集打开。");
                }

                // 删除上一次未提交的文件dp.dp_middledatafile
                {
                    lgprintf("删除表%d,分区%d上一次未提交的数据文件信息(dp.dp_middledatafile).",tabid,datapartid);
                    clsmt.FetchAll("select datafilename,indexfilename from dp.dp_middledatafile where  tabid=%d and datapartid=%d and procstate=0",tabid,datapartid);
                    int clsrn=clsmt.Wait();
                    for(int i=0; i<clsrn; i++) {
                        unlink(clsmt.PtrStr("datafilename",i));
                        unlink(clsmt.PtrStr("indexfilename",i));
                    }
                    st.DirectExecute("delete from dp.dp_middledatafile where  tabid=%d and datapartid=%d and procstate=0",tabid,datapartid);
                }
            }
        } else if(addfiles && !dp_parallel_load) { // 补加文件的流程(bhloader入库)
            //删除上一次未正常解析并存入dp.dp_middledatafile的文件
            AutoStmt st(dtdbc);
            if(st.DirectExecute("delete from dp.dp_filelog where  tabid=%d and datapartid=%d and status<>2",tabid,datapartid)>0) {
                lgprintf("有部分文件上次异常退出时未提交到临时文件，需要从备份目录补，请确保备份采集打开。");
            }

            // 删除上一次未提交的文件dp.dp_middledatafile
            {
                lgprintf("删除表%d,分区%d上一次未提交的数据文件信息(dp.dp_middledatafile).",tabid,datapartid);
                clsmt.FetchAll("select datafilename,indexfilename from dp.dp_middledatafile where  tabid=%d and datapartid=%d and procstate=0",tabid,datapartid);
                int clsrn=clsmt.Wait();
                for(int i=0; i<clsrn; i++) {
                    unlink(clsmt.PtrStr("datafilename",i));
                    unlink(clsmt.PtrStr("indexfilename",i));
                }
                st.DirectExecute("delete from dp.dp_middledatafile where  tabid=%d and datapartid=%d and procstate=0",tabid,datapartid);
            }
        } else if(keepfiles && dp_parallel_load) { //文件续传检查数据一致性(cluster_fm排序集群合并入库)

            lgprintf("任务从[文件续传<72>]进入,清除上次导出的数据...");

            AutoStmt st(dtdbc);
            lgprintf("清除未提交到临时文件(dp.dp_filelog).");
            if(st.DirectExecute("delete from dp.dp_filelog where  tabid=%d and datapartid=%d and status<>2",tabid,datapartid)>0) {
                lgprintf("有部分文件上次异常退出时未提交到临时文件，需要从备份目录补，请确保备份采集打开。");
            }

            lgprintf("清除未提交到会话数据(dp.dp_datapartext).");
            clsmt.FetchAll("select sessionid from dp.dp_datapartext where tabid =%d and datapartid = %d and commitstatus = -1 and sessiontype =1  ",tabid,datapartid);
            int _rt = clsmt.Wait();
            char cancle_session_cmd[256];
            for(int i=0; i<_rt; i++) {
                sprintf(cancle_session_cmd,"cfm-toolbox slcancel %s",clsmt.PtrStr("sessionid",i));
                lgprintf("远程终止会话,运行命令:%s",cancle_session_cmd);
                int __r = CR_Class_NS::system(cancle_session_cmd);
                lgprintf("远程终止会话,运行命令:%s ,return %d",cancle_session_cmd,__r);

            }

            add_stsessions_to_ltsession = true;
            st.DirectExecute("delete from dp.dp_datapartext where tabid=%d and datapartid=%d and commitstatus = -1 and sessiontype =1 ",tabid,datapartid);

        } else if(addfiles && dp_parallel_load) { // 补加文件的流程(cluster_fm排序集群合并入库)

            lgprintf("任务从[补文件<73>]进入,清除上次导出的数据...");

            AutoStmt st(dtdbc);
            lgprintf("清除未提交到临时文件(dp.dp_filelog).");
            if(st.DirectExecute("delete from dp.dp_filelog where  tabid=%d and datapartid=%d and status<>2",tabid,datapartid)>0) {
                lgprintf("有部分文件上次异常退出时未提交到临时文件，需要从备份目录补，请确保备份采集打开。");
            }

            lgprintf("清除未提交到会话数据(dp.dp_datapartext).");

            clsmt.FetchAll("select sessionid from dp.dp_datapartext where tabid =%d and datapartid = %d and commitstatus = -1 and sessiontype =1  ",tabid,datapartid);
            int _rt = clsmt.Wait();
            char cancle_session_cmd[256];
            for(int i=0; i<_rt; i++) {
                sprintf(cancle_session_cmd,"cfm-toolbox slcancel %s",clsmt.PtrStr("sessionid",i));
                lgprintf("远程终止会话,运行命令:%s",cancle_session_cmd);
                int __r = CR_Class_NS::system(cancle_session_cmd);
                lgprintf("远程终止会话,运行命令:%s ,return %d",cancle_session_cmd,__r);
            }

            st.DirectExecute("delete from dp.dp_datapartext where tabid=%d and datapartid=%d and commitstatus = -1",tabid,datapartid);

            st_session_interval = 0;  // 补文件的流程,不能只用长会话,必须只能有一个短会话,长会话会替换程序

            // 设置任务并发数为0,进入补文件流程的采集任务不允许进行任务并发
            if(_slavercnt >0 ) {
                int ret = st.DirectExecute("update dp.dp_datapart set slavercnt=0,oldstatus=%d where tabid=%d and datapartid=%d",origion_status,tabid,datapartid);

                // 再次判断*.loading文件是否存在,存在就退出
                if(ret == 1) {
                    lgprintf("数据补文件流程,tabid(%d),datapartid(%d)采集任务的并发执行数(%d--->0)执行成功.",tabid,datapartid,_slavercnt);
                } else {
                    lgprintf("数据补文件流程,tabid(%d),datapartid(%d)采集任务的并发执行数(%d--->0)执行失败,任务并行进入冲突程序退出.",tabid,datapartid,_slavercnt);
                    lgprintf("tabid(%d),datapartid(%d)恢复任务状态为:status(%d)",tabid,datapartid,origion_status);
                    //sp.UpdateTaskStatus(origion_status,tabid,datapartid);
                    return 0;
                }
            }
        }
    } catch(...) {
        lgprintf("tabid(%d),datapartid(%d)恢复任务状态为:status(%d)",tabid,datapartid,origion_status);
        sp.UpdateTaskStatus((TASKSTATUS)origion_status,tabid,datapartid);
        return 0;
    }

    //indexparam *ip=&dp.idxp[dp.psoledindex];
    maxblockrn=blocksize/dp.rowlen;
    if(maxblockrn<MIN_BLOCKRN) {
        blocksize=MIN_BLOCKRN*dp.rowlen;
        maxblockrn=blocksize/dp.rowlen;
        if(blocksize<0 || blocksize>1024*1024*1024) {
            sp.log(tabid,datapartid,DUMP_RECORD_NUM_ERROR,"表%d,分区%d 记录长度太大,无法迁移(记录长度%d)。",tabid,datapartid,dp.rowlen);
            return 0;
        }
        lgprintf("由于记录长度太大,块大小调整为%d,目标表的缓存机制将失效。(记录数:%d,记录长度%d)",blocksize,maxblockrn,dp.rowlen);
    } else if(maxblockrn>MAX_BLOCKRN) {
        blocksize=(MAX_BLOCKRN-1)*dp.rowlen;
        maxblockrn=blocksize/dp.rowlen;
        lgprintf("由于记录长度太小，块大小调整为%d。 (记录数:%d,记录长度%d)",blocksize,maxblockrn,dp.rowlen);
    }

    //CMNET:增加重入处理
    if(!keepfiles && !addfiles && !deal_slaver_node) {
        //在作数据导出时设置块记录数,以后的处理和查询以此为依据
        //字段maxrecinblock的使用方法变更为:程序根据后台设置的参数自动计算,管理控制台只读
        lgprintf("设置目标数据块%d字节(记录数:%d)",maxblockrn*dp.rowlen,maxblockrn);
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

        if(dp_parallel_load) { // 分布式排序装入
            paraller_sorttm.Restart();
            b_master_node = false;
            b_master_node = is_master_node(dtdbc,tabid,datapartid);

            // 新构建短会话id
            if(b_master_node) { // 只有master节点可以创建会话ID,slaver节点不能创建会话ID
                // 获取当前会话ID
                std::string session_id = generate_session_id(sp,tabid,datapartid);

                {
                    AutoLock lk(&sessionid_lock);
                    strcpy(stsessionid,session_id.c_str());
                    lt_parallel_load_rows = 0;

                    if(st_session_interval>0) { // 长会话有效
                        if(keepfiles) {

                            add_stsessions_to_ltsession = true;

                            // 排序集群重启过的情况下,就要重新构建一个新的会话ID
                            AutoMt lt_mt(sp.GetDTS(),10);
                            lt_mt.FetchFirst("select sessionid,rownums from dp.dp_datapartext where tabid = %d and datapartid = %d and commitstatus = -1 and sessiontype= 2 limit 5",
                                             tabid,datapartid);

                            if(lt_mt.Wait()>0) {
                                lt_parallel_load_rows = lt_mt.GetLong("rownums",0);
                                char cancle_session_cmd[256];
                                sprintf(cancle_session_cmd,"cfm-toolbox slcancel %s",lt_mt.PtrStr("sessionid",0));
                                lgprintf("远程终止会话,运行命令:%s",cancle_session_cmd);
                                int __r = CR_Class_NS::system(cancle_session_cmd);
                                lgprintf("远程终止会话,运行命令:%s ,return %d",cancle_session_cmd,__r);


                                // 生成新的会话ID
                                session_id = generate_session_id(sp,tabid,datapartid);
                                strcpy(ltsessionid,session_id.c_str());

                            } else {
                                lgprintf("警告:文件续传时候,无法获取上一次长会话ID,重新获取短会话ID对应的数据记录使用");

                                // 获取短会话ID的记录总数
                                lt_mt.FetchFirst("select sum(rownums) cnt from dp.dp_datapartext where tabid = %d and datapartid = %d and commitstatus != -1 and sessiontype= 1 limit 5",
                                                 tabid,datapartid);
                                if(lt_mt.Wait()>0) {
                                    if(wociIsNull(lt_mt,0,0)) { // 没有记录返回:NULL
                                        lt_parallel_load_rows = 0;
                                    } else {
                                        lt_parallel_load_rows = lt_mt.GetLong("cnt",0);
                                        if(lt_parallel_load_rows<0) {
                                            lt_parallel_load_rows = 0;
                                        }
                                    }
                                    lgprintf("重新计算:tabid(%d),datapartid(%d)的长回话ID记录数:%ld.",tabid,datapartid,lt_parallel_load_rows);
                                } else {
                                    lt_parallel_load_rows = 0;
                                    lgprintf("重新设置:tabid(%d),datapartid(%d)的长回话ID记录数:%ld.",tabid,datapartid,lt_parallel_load_rows);
                                }
                            }

                            // 删除长会话
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
                // slaver节点需要等待master节点分配好会话id才能用
                paraller_sorttm.Restart();
                int _slaver_start_ret = 0;
                while(1) {
                    _slaver_start_ret = update_new_short_session(sp,tabid,datapartid,false);
                    if( -3 == _slaver_start_ret ) { // 该分区没有断会话直接退出,不进行等待操作
                        lgprintf("slaver节点[%d]没有发现有可用的短会话ID,程序退出返回.....",slaver_index);
                        return 0;
                    } else if(0 != _slaver_start_ret ) {
                        lgprintf("tabid(%d),datapartid(%d)等待master节点创建短会话id...",tabid,datapartid);
                        sleep(5);
                        if(paraller_sorttm.GetTime() > st_session_interval) { // fix dma-15444
                            lgprintf("tabid(%d),datapartid(%d)等待master节点创建短会话id超时(%d)秒,slaver节点退出.",tabid,datapartid,3*st_session_interval);
                            return 0;
                        }
                    } else {
                        break;
                    }
                };
                paraller_sorttm.Restart();
            }

            if(b_master_node) { // master 启动节点
                start_master_dump_data(sp,tabid,datapartid,keepfiles);
                slaver_index = 0;
            } else { // slaver 节点,启动流程
                start_slaver_dump_data(sp,tabid,datapartid);
            }
            last_stsessionid[0]=0;
            clear_ssort_key_column(); // 初始化排序的key
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
    if(dp_parallel_load && !b_master_node) { // slaver 节点再次判断master是否处于finish状态,如果是退出!

        AutoMt mt_slaver(sp.GetDTS(),100);
        mt_slaver.FetchFirst("select clusterstatus from dp.dp_datapartext where tabid = %d and datapartid =%d and sessionid='%s' ",
                             tabid,datapartid,stsessionid);
        int _ret = mt_slaver.Wait();
        assert(_ret == 1);
        int _clusterstatus = mt_slaver.GetInt("clusterstatus",0);
        if(_clusterstatus == CS_MASTER_BUSY) {
            lgprintf("slaver节点[%d]检测到短会话[%s]正处于提交状态,准备退出.",slaver_index,stsessionid);
            if(st_session_interval>0) {
                _ret = decrease_slaver_lanuch_count(sp,tabid,datapartid,true);
                lgprintf("长会话[%s]对应的slaver节点启动数自减少1[%s].",ltsessionid,_ret==1?"成功":"失败");
            }
            _ret = decrease_slaver_lanuch_count(sp,tabid,datapartid,false);
            lgprintf("短会话[%s]对应的slaver节点启动数自减少1[%s].",stsessionid,_ret==1?"成功":"失败");
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
            sp.log(tabid,datapartid,99,"数据抽取sql:%s.",dumpsql);
            TradeOffMt dtmt(0,realrn);
            blockmt.Clear();
            sp.BuildMtFromSrcTable(fmtdbc,tabid,&blockmt);
            //blockmt.Build(stmt);
            blockmt.AddrFresh();
            // CMNET :文件模式判断，依据dumpsql的内容
            // Load data from files using cmnet.ctl [exclude backup]
            // 动态库文件名中不能包含空格
            char libname[300];
            bool withbackup=true;//default to true
            if(strcasestr(dumpsql,"load data from files")!=NULL) {
                filesmode=true;
                char *plib=strcasestr(dumpsql,"using ");
                if(!plib) {
                    sp.log(tabid,datapartid,DUMP_FILE_ERROR,"表%d,分区%d,文件导入数据缺少using关键字.",tabid,datapartid);
                    ThrowWith("文件导入数据缺少using关键字.");
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
                sp.log(tabid,datapartid,DUMP_FILE_ERROR,"表%d,分区%d,格式错误'%s',请使用'load data from files using xxx.\n",tabid,datapartid,dumpsql);
                ThrowWith("格式错误'%s',请使用'load data from files using xxx.\n",dumpsql);
            }

            sp.BuildMtFromSrcTable(fmtdbc,tabid,dtmt.Cur());
            //if(filesmode) dtmt.SetPesuado(true);
            //else
            sp.BuildMtFromSrcTable(fmtdbc,tabid,dtmt.Next());
            AutoStmt stmt(srcdbc);
            //CMNET :文件模式
            if(!filesmode) {
                stmt.Prepare(dumpsql);

                //>> Begin: fix dm-230 , 重新构造源表mt
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
                        lgprintf("已经设置环境变量:DP_CHECKFORMAT=0跳过格式化表结构校验.");
                        _check_format = 0; // 不需要校验格式化表结构
                    }
                }
                if(_check_format != 0) {
                    if(blockmt.CompatibleMt(tstmt)!=0 ) {
                        sp.log(tabid,datapartid,DUMP_SQL_ERROR,"表%d,分区%d,数据抽取语句 %s 得到的格式与源表定义的格式不一致.\n",tabid,datapartid,dumpsql);
                        ThrowWith("以下数据抽取语句 %s 得到的格式与源表定义的格式不一致.\n",dumpsql);
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
            //准备数据索引表插入变量数组
            //CMNET :文件模式
            int rn;
            bool filecp=false;
            bool needcp=false;
            int uncheckfct=0,uncheckbct=0;//检查点后的文件数和数据库数

            //>> Begin: DM-201 , modify by liujs
            IDumpFileWrapper *dfw;
            IFileParser *fparser;
            //<< End: modify by liujs

            bool filehold=false;//true: 文件处理中（内存表满)
            bool extractend_thread = false; // true: 文件数据抽取结束
            bool extractend_main = false; // true: 所有文件数据抽取结束
            int  continue_extractend_times = 0; // master节点连续3次都没有等到有数据就结束
            bool single_file_finish = false;    // 判断文件是否解析结束
            bool slaver_exit = false;           // slaver是否退出
            ThreadList pickThread;

            if(filesmode) {
                dfw=new DumpFileWrapper(libname);
                fparser=dfw->getParser();
                try {
                    fparser->PluginPrintVersion();
                } catch(...) {
                    lgprintf("使用的plugin过老，缺乏版本对应信息,PluginPrintVersion error !");
                    return -1;
                }
                fparser->SetTable(plugin_sp.GetDTS(),tabid,datapartid,dfw);

                //Get file and parser
                //如果上次的文件未处理完(内存表满),或者有新文件，则进入分解过程
                while(true) {
                    if(dp_parallel_load) {
                        AutoLock ak(&sessionid_lock);
                        fparser->SetSession(slaver_index);
                    }
                    if(!filehold) {
                        int gfstate=fparser->GetFile(withbackup,plugin_sp,tabid,datapartid);
                        if(gfstate==0) {
                            //没有数据需要处理
                            if(fparser->ExtractEnd(plugin_sp.GetDTS())) {
                                extractend_main = true;
                                break;
                            } else {
                                // 获取不到文件了,那么超时就不用等了
                                if(dp_parallel_load && slaver_index>0) { // slaver 节点不用等
                                    if(st_session_interval>0 && paraller_sorttm.GetTime() > st_session_interval) {
                                        extractend_main = true;
                                        lgprintf("表(%d),分区(%d),SlaverID(%d)获取不到文件,短会话时间到离开第一个采集",tabid,datapartid,slaver_index);
                                        break;
                                    }
                                }

                                if(dp_parallel_load && slaver_index == 0) { // master 节点
                                    if(fparser->GetForceEndDump() == 1) { // 只有采集一次数据的时候,才退出
                                        if(st_session_interval>0 && paraller_sorttm.GetTime() > 1*st_session_interval) {
                                            extractend_main = true; // master 节点
                                            lgprintf("表(%d),分区(%d),SlaverID(%d)获取不到文件,短会话时间到离开第一个采集A",tabid,datapartid,slaver_index);
                                            break;
                                        }
                                    } else {
                                        if(st_session_interval>0 && paraller_sorttm.GetTime() > 2*st_session_interval) {
                                            extractend_main = false; // master 节点要等待
                                            lgprintf("表(%d),分区(%d),SlaverID(%d)获取不到文件,短会话时间到离开第一个采集B",tabid,datapartid,slaver_index);
                                            break;
                                        }
                                    }
                                }
                            }

                            extractend_main = false;
                            //if(dtmt.Cur()->GetRows()>0) break;
                            mySleep(fparser->GetIdleSeconds());

                            //>> begin: fix dma-1326: 单个文件处理完成,判断是否到提交时间,如果到提交时间了就提交文件
                            if(dp_parallel_load ) {

                                if(paraller_sorttm.GetTime() > st_session_interval &&
                                   dtmt.Cur()->GetRows() > 0) {
                                    break;
                                }
                            }
                            //<< end: fix dma-1326

                            continue;
                        } else if(gfstate==2) //文件错误，但应该忽略
                            continue;
                        uncheckfct++;
                    }
                    int frt=fparser->DoParse(*dtmt.Cur(),plugin_sp,tabid,datapartid);
                    filehold=false;
                    if(frt==-1) {
                        sp.log(tabid,datapartid,DUMP_FILE_ERROR,"表%d,分区%d,文件处理错误.",tabid,datapartid);
                        ThrowWith("文件处理错误.");
                        break;
                    } else if(frt==0) {
                        //memory table is full,and file is not eof
                        filehold=true;
                        break;
                    } else if(frt==2) {
                        //memory table is full and file is eof
                        break;
                    }
                    //>> begin: fix dma-1326: 单个文件处理完成,判断是否到提交时间,如果到提交时间了就提交文件
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
            //文件检查点是否已到达
            double fillrate=0;
            int forcedatatrim=0;
            char *pfdt=getenv("DP_FORCEDATATRIM");
            if(pfdt!=NULL) forcedatatrim=atoi(pfdt);
            if(forcedatatrim==1) lgprintf("..FDT");
            bool commitCheck=false;

            if(rn == 0 && b_master_node) { // 第一次就没有采集到数据,没有调用:commit_sort_data_process
                // slaver 节点采集不到数据就退出
                extractend_main = false;
            }

            // 判断并行数据入库,文件数据采集,master节点是否正在采集数据
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
                        //lgprintf("提交检查点：数据块%d,文件数%d,填充率:%.1f%%.",uncheckfct,uncheckbct,fillrate*100);
                        //以导出，待处理的文件设置为10
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
                lgprintf("开始数据处理");
                int retryct=0;
                while(true) {
                    try {
                        freeinfo1("before call prcblk");
                        for(int i=0; i<dp.soledindexnum; i++) {
                            int mt_rows = dtmt.Cur()->GetRows();

                            ProcBlock(sp,tabid,datapartid,dtmt.Cur(),i/*dp.psoledindex*/,blockmt,sp.NextTmpFileID(),filesmode);

                            if(dp_parallel_load) { // 短会话id的处理

                                // assert(dp.soledindexnum == 1);

                                if(st_session_interval > 0 ) {

                                    // 短会话提交时间满足时间间隔
                                    // 从文件采集的时候,只有单个文件处理完成才能切换会话
                                    if((paraller_sorttm.GetTime() > st_session_interval ) && ( filesmode && single_file_finish )) { // 提交短会话id

                                        // 提交短会话ID
                                        // 文件采集的文件状态更新,在commit_sort_data_process中进行处理
                                        commit_sort_data_process(sp,tabid,datapartid,true,false,false,filesmode);

                                        // 等待新的会话ID
                                        e_extract_run_status = ENUM_WAIT_NEW_SESSION;

                                        if(extractend_main) { // 主线程抽取数据结束

                                            if(!b_master_node ) {
                                            label_slaver_exit:
                                                // slaver节点文件抽取结束,无需创建新的会话了
                                                lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)文件数据抽取结束,无需等待master在创建会话id.",tabid,datapartid,slaver_index);
                                                break;

                                            } else {
                                                // master节点要看会话上是否有其他slaver节点生成数据
                                                // 如果有不能退出,需要继续等待
                                                int _launch_slv_cnt = 0;
                                                int _finish_slv_cnt = 0;
                                                int64 _loadrows = 0;
                                                int _clusterstatus = 0;
                                                is_all_slaver_node_finish(sp.GetDTS(),tabid,datapartid,stsessionid,
                                                                          _launch_slv_cnt,_finish_slv_cnt,_loadrows,_clusterstatus);

                                                if(_loadrows > 0) {
                                                    lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)文件数据抽取结束,等待其他slaver节点上数据采集处理,需要创建会话,等待次数(%d).",tabid,datapartid,slaver_index,continue_extractend_times);
                                                    continue_extractend_times = 0;
                                                } else {
                                                    if(_launch_slv_cnt >0) { // exist slaver node
                                                        if(continue_extractend_times < CONTINUE_EXTRACT_END_MAX_TIMES) {
                                                            lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)文件数据抽取结束,等待其他slaver节点上数据采集处理,需要创建会话,等待次数(%d).",tabid,datapartid,slaver_index,continue_extractend_times);
                                                        } else { // slaver 节点没有生成数据
                                                            lgprintf("tabid(%d),datapartid(%d)文件数据抽取结束,且无slaver节点上有数据需要采集处理.",tabid,datapartid);
                                                            continue_extractend_times++;
                                                            break;
                                                        }
                                                    } else {
                                                        lgprintf("tabid(%d),datapartid(%d)文件数据抽取结束,只有master一个节点处理数据,continue_extractend_times(%d).",tabid,datapartid,continue_extractend_times);

                                                        if(continue_extractend_times > CONTINUE_EXTRACT_END_MAX_TIMES) {
                                                            lgprintf("tabid(%d),datapartid(%d)文件数据抽取结束,continue_extractend_times(%d),结束数据采集过程.",tabid,datapartid,continue_extractend_times);
                                                            continue_extractend_times=4;
                                                            break;
                                                        }
                                                    }
                                                }
                                                continue_extractend_times++;
                                                lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)文件数据抽取结束,其他slaver节点上有数据需要采集处理,需要创建会话.等待次数(%d),等待时间30秒.",tabid,datapartid,slaver_index,continue_extractend_times);
                                                sleep(30);
                                            }
                                        } else {
                                            lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)文件数据抽取未结束",tabid,datapartid,slaver_index);
                                            continue_extractend_times = 0;
                                        }

                                        paraller_sorttm.Restart();

                                        // 新构建短会话id
                                        if(b_master_node) { // 只有master节点可以创建会话ID,slaver节点不能创建会话ID
                                            create_new_short_session(sp,tabid,datapartid);
                                        } else {
                                            // slaver节点需要等待master节点分配好会话id才能用
                                            slaver_exit = false;

                                            //  如果连续6次,该表中都没有正在处理的短会话id,说明短会话已经处理完成,此时在等待长会话过程中,直接退出即可
                                            int retry_times = 0;
                                            int _slaver_start_ret2 = 0;
                                            while(1) {

                                                _slaver_start_ret2 = update_new_short_session(sp,tabid,datapartid,true);

                                                // 判断该表中是否存在没有处理的短会话ID,如果存在继续等待,
                                                if(-3 == _slaver_start_ret2) {
                                                    // 没有短会话存在,直接返回
                                                    long _tmp_row = 0;
                                                    {
                                                        AutoLock ak(&sessionid_lock);
                                                        _tmp_row = threadmtrows;
                                                    }
                                                    lgprintf("tabid(%d),datapartid(%d)等待master节点创建短会话id,retry_times(%d),threadmtrows(%ld).",tabid,datapartid,retry_times,_tmp_row);
                                                    if(retry_times++>60 && _tmp_row == 0) { // 没有数据了才退出
                                                        goto label_slaver_exit;
                                                    }
                                                    sleep(10);

                                                } else if(0 != _slaver_start_ret2) {

                                                    lgprintf("tabid(%d),datapartid(%d)等待master节点创建短会话id,等待10秒.",tabid,datapartid);
                                                    sleep(10);
                                                    retry_times = 0;

                                                    if( -5 == _slaver_start_ret2) {
                                                        // master节点正在init短会话过程中,继续等待

                                                        if(paraller_sorttm.GetTime() > 5 * st_session_interval) {
                                                            slaver_exit = true; // slaver 必须要退出了
                                                            lgprintf("tabid(%d),datapartid(%d)等待master节点创建短会话id,返回:%d.",tabid,datapartid,_slaver_start_ret2);
                                                            goto label_slaver_exit;
                                                        }
                                                    } else if (-4 == _slaver_start_ret2) {
                                                        // master节点正在finish数据过程中,继续等待

                                                        int __wait_time = 0;
                                                        // 以5分钟为单位进行等待
                                                        __wait_time = (st_session_interval<(5*60)) ? (5*60): st_session_interval;

                                                        if(paraller_sorttm.GetTime() > 10 * __wait_time ) {
                                                            slaver_exit = true; // slaver 必须要退出了
                                                            lgprintf("tabid(%d),datapartid(%d)等待master节点创建短会话id,返回:%d.",tabid,datapartid,_slaver_start_ret2);
                                                            goto label_slaver_exit;
                                                        }
                                                    } else if (-2 == _slaver_start_ret2 ) {
                                                        // master节点正在push数据中,继续等待
                                                        if(paraller_sorttm.GetTime() > 10 * st_session_interval) {
                                                            slaver_exit = true; // slaver 必须要退出了
                                                            lgprintf("tabid(%d),datapartid(%d)等待master节点创建短会话id,返回:%d.",tabid,datapartid,_slaver_start_ret2);
                                                            goto label_slaver_exit;
                                                        }
                                                    }

                                                } else {
                                                    // 找到满足条件的短会话id
                                                    break;
                                                }

                                                int cluster_status = 0;
                                                cluster_status = get_cluster_status(sp.GetDTS(),tabid,datapartid,stsessionid);
                                                if(cluster_status == CS_ERROR) {
                                                    ThrowWith("SLAVER节点,等待MASTER节点完成过程中发现集群当前会话[%s]故障,退出.",stsessionid);
                                                }

                                            };

                                            // 更新任务启动时候的slaver节点数目
                                            increase_slaver_lanuch_count(sp,tabid,datapartid,false);

                                            e_extract_run_status = ENUM_SLAVER_WAIT_RUN_SESSION;

                                            sleep(10); // slaver 获取到master创建的短会话id后,等待10秒,以避免在master前启动

                                        }
                                        extractend_main = false;

                                        // 新构建短会话id对应的sort对象
                                        if(b_master_node) {
                                            lgprintf("MASTER节点,短会话ID(%s)准备创建排序对象,设置集群状态为:CS_MASTER_INIT",stsessionid);
                                            set_cluster_status(sp.GetDTS(),tabid,datapartid,stsessionid,CS_MASTER_INIT);
                                        }
                                        int ret = create_DMIBSortLoader(sp,tabid,datapartid,false);

                                        if(b_master_node) { // master 节点设置会话信息
                                            if(-1 == ret) {
                                                // 设置集群为不可用状态
                                                // 此处不用设置,集群排序对象还没有创建出来
                                                update_new_short_session(sp,tabid,datapartid,CS_ERROR);
                                                ThrowWith("master节点DataDump::create_DMIBSortLoader创建对象失败,设置tabid(%d),datapartid(%d),会话(%s)集群状态为故障(CS_ERROR),准备退出!",
                                                          tabid,datapartid,stsessionid);

                                            } else { // 集群排序对象创建成功
                                                update_new_short_session(sp,tabid,datapartid,CS_OK);
                                                lgprintf("master节点分布式排序创建短会话ID成功.");
                                            }

                                        } else { // slaver节点退出即可
                                            if(-1 == ret) {

                                                lgprintf("Slaver节点创建p_stDMIBSortLoader 对象,创建失败.");

                                                // 短会话自减少
                                                decrease_slaver_lanuch_count(sp,tabid,datapartid,false);
                                                if(st_session_interval>0) {
                                                    // 长会话自减少
                                                    decrease_slaver_lanuch_count(sp,tabid,datapartid,true);
                                                }

                                                ThrowWith("Slaver节点DataDump::create_DMIBSortLoader创建对象失败,tabid(%d),datapartid(%d),会话(%s)准备退出!",
                                                          tabid,datapartid,stsessionid);
                                            } else {
                                                lgprintf("Slaver节点DataDump::create_DMIBSortLoader创建对象成功,tabid(%d),datapartid(%d),会话(%s).",
                                                         tabid,datapartid,stsessionid);
                                            }
                                        }

                                        paraller_sorttm.Restart();

                                        // 会话ID更新成功,可以工作
                                        e_extract_run_status = ENUM_RUN_CUR_SESSION;

                                    } else if( mt_rows == 0) {
                                        int sleep_num = st_session_interval / 3;
                                        lgprintf("没有采集到数据,休眠时间%d秒.",sleep_num);
                                        sleep(sleep_num);
                                    }
                                } else {
                                    if(mt_rows == 0) {
                                        break;
                                    }
                                }
                            }
                        }


                        lgprintf("数据处理结束");

                        if(fnmt.GetRows()>0 && !dp_parallel_load) {
                            wociSetEcho(false);
                            wociAppendToDbTable(fnmt,"dp.dp_middledatafile",dtdbc,true);
                            wociReset(fnmt);
                            wociSetEcho(true);
                        }

                        if(filesmode) {
                            wociReset(*dtmt.Cur());
                            //检查点的提交条件
                            //必须是一个文件处理结束，不允许数据文件的处理中间过程提交
                            //达到5个数据块或者分区数据已经处理结束，则提交
                            //或者达到或超过2个数据库未提交，但最后一个数据块已经超过80%--或者超过10个文件未提交
                            //后面的文件处理部分还需要做一致的调整
                            if(commitCheck) {
                                lgprintf("提交检查点：数据块%d,文件数%d,填充率:%.1f%%.",uncheckbct,uncheckfct,fillrate*100);

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
                                sp.log(tabid,datapartid,DUMP_WRITE_FILE_ERROR,"表%d,分区%d,写数据失败.",tabid,datapartid);
                                throw;
                            }
                            lgprintf("写数据失败，重试...");
                        }
                        throw;
                    }
                }
                freeinfo1("after call prcblk");

                if(filesmode) {
                    // 等待线程处理结束
                    pickThread.Wait();
                    rn=pickThread.GetReturnValue();
                    single_file_finish = !filehold; // 单个文件是否解析结束
                    extractend_main = extractend_thread; // 文件抽取结束了
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
                        lgprintf("提交检查点：数据块%d,文件数%d,填充率:%.1f%%.",uncheckbct,uncheckfct,fillrate*100);
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
                            if(fparser->GetForceEndDump() == 1) { // master节点没有采集到数据
                                lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)抽取到记录数为0,[files::forceenddump==1].",tabid,datapartid,slaver_index);
                                extractend_main = true;
                            } else {
                                lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)抽取到记录数为0.",tabid,datapartid,slaver_index);
                            }
                        } else {
                            lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)抽取到记录数为0,从数据库抽取数据.",tabid,datapartid,slaver_index);
                            extractend_main = true;
                        }
                    }
                } else {

                    run_slaver_taskinfo_deal_rows += rn;

                    if(rn == 0) { // 最后一次没有采集到数据,try to fix dma-1460
                        lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)抽取到记录数为0.",tabid,datapartid,slaver_index);
                        extractend_main = true;
                    }
                }

                if(!b_master_node && slaver_exit) {

                    lgprintf("tabid(%d),datapartid(%d),slaver_index(%d) 没有获取到master节点创建的会话ID退出。",tabid,datapartid,slaver_index);

                    // fix dma-2017: slaver进程退出的时候,不能将dp.dp_filelog中的记录删除,如果遇到错误,在下次启动运行的时候,再删除数据
                    AutoStmt st1(dtdbc);

                    // fix dma-2018: 只删除endproctime is null的
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

            lgprintf("tabid(%d),datapartid(%d),slaver_index(%d)数据抽取完成.",tabid,datapartid,slaver_index);

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
            } else { // 分布式排序,提交完成数据排序过程
                int ret = 0;

                if(p_stDMIBSortLoader == NULL) { // 采集过程没有采集到数据
                    e_extract_run_status = ENUM_WAIT_NEW_SESSION;

                    if(b_master_node) {
                        lgprintf("MASTER节点,短会话ID(%s),设置集群状态为:CS_MASTER_INIT",stsessionid);
                        set_cluster_status(sp.GetDTS(),tabid, datapartid,stsessionid,CS_MASTER_INIT);

                        if(st_session_interval>0) {
                            lgprintf("MASTER节点,长会话ID(%s),设置集群状态为:CS_MASTER_INIT",ltsessionid);
                            set_cluster_status(sp.GetDTS(),tabid, datapartid,ltsessionid,CS_MASTER_INIT);
                        }
                    }
                    ret = CreateEmptyDMIBSortLoad(sp,tabid,datapartid,dtmt.Cur());

                    if(ret == 0) {
                        if(b_master_node) {
                            lgprintf("MASTER节点,短会话ID(%s)创建排序对象完成,设置集群状态为:CS_OK",stsessionid);
                            set_cluster_status(sp.GetDTS(),  tabid, datapartid,stsessionid,CS_OK);

                            if(st_session_interval>0) {
                                lgprintf("MASTER节点,长会话ID(%s)创建排序对象完成,设置集群状态为:CS_OK",ltsessionid);
                                set_cluster_status(sp.GetDTS(),  tabid, datapartid,ltsessionid,CS_OK);
                            }
                        }
                    }
                }

                // 提交排序后的数据,master节点必须等到所有的slaver节点提交完成后才进行数据提交。
                if(!filesmode) {
                    if(ret == 0 && p_stDMIBSortLoader != NULL) {
                        e_extract_run_status = ENUM_RUN_CUR_SESSION;
                        ret = commit_sort_data_process(sp,tabid,datapartid,true,true,true,filesmode);
                    } else {
                        lgprintf("skip commit_sort_data_process ,tabid(%d),datapartid(%d),slaver_index(%d),stsessionid(%s),ltsessionid(%s).",tabid,datapartid,slaver_index,stsessionid,ltsessionid);
                        lgprintf("p_stDMIBSortLoader 对象为null 不能进行DMIBSortLoad::Finish动作.");
                    }
                } else {
                    // 最后一个短会话必须要提交,如果有短会话不提交会导致后台与短会话对应的cluster_fm一直等待finish过程，挂起
                    if(ret == 0 && p_stDMIBSortLoader != NULL) {
                        ret = commit_sort_data_process(sp,tabid,datapartid,true,true,true,filesmode);
                    } else {
                        lgprintf("skip commit_sort_data_process ,tabid(%d),datapartid(%d),slaver_index(%d),stsessionid(%s),ltsessionid(%s).",tabid,datapartid,slaver_index,stsessionid,ltsessionid);
                        if(!b_master_node) {
                            lgprintf("p_stDMIBSortLoader 对象为null 不能进行DMIBSortLoad::Finish动作.");

                            // 短会话自减少
                            decrease_slaver_lanuch_count(sp,tabid,datapartid,false);
                            if(st_session_interval>0) {
                                // 长会话自减少
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
        errprintf("数据导出异常终止，表%d(%d),中间文件数:%d.",tabid,datapartid,frn);
        AutoStmt st(dtdbc);
        st.DirectExecute("unlock tables");
        sp.log(tabid,datapartid,DUMP_EXCEPTION_ERROR,"表%d,分区%d 数据导出异常终止，中间文件数:%d.",tabid,datapartid,frn);
        bool restored=false;

        if(dumpcomplete && !dp_parallel_load ) {
            //当前任务的导出已完成，但修改DP参数失败.重试10次,如果仍然失败,则放弃.
            int retrytimes=0;
            while(retrytimes<10 &&!restored) {
                restored=true;
                try {
                    wociAppendToDbTable(fnmt,"dp.dp_middledatafile",dtdbc,true);
                    sp.UpdateTaskStatus(DUMPED,tabid,datapartid);
                } catch(...) {
                    sp.log(tabid,datapartid,DUMP_FINISHED_NOTIFY,"表%d(%d)导出已完成,但写入dp参数表(dp_middledatafile)失败,一分钟后重试(%d)...",tabid,datapartid,++retrytimes);
                    lgprintf("表%d(%d)导出已完成,但写入dp参数表(dp_middledatafile)失败,一分钟后重试(%d)...",tabid,datapartid,++retrytimes);
                    restored=false;
                    mSleep(60000);
                }
            }
        }

        if(!restored) {
            int i;
            wdbi_kill_in_progress=false;
            wociMTPrint(fnmt,0,NULL);

            //先做恢复任务状态的操作,因为任务状态的人工调整最为容易.如果数据库连接一直没有恢复,
            //则任务状态恢复会引起异常,后续的删除文件和记录的操作不会被执行,可以由人工来确定是否可恢复,如何恢复
            errprintf("准备恢复任务状态.");
            sp.log(tabid,datapartid,DUMP_RECOVER_TAST_STATUS_NOTIFY,"准备恢复任务状态.");


            if(!dp_parallel_load) { // 非多节点排序入库流程(bhloader入库流程)

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
                errprintf("删除中间文件...");
                for(i=0; i<frn; i++) {
                    errprintf("\t %s \t %s",fnmt.PtrStr("datafilename",i),
                              fnmt.PtrStr("indexfilename",i));
                }
                for(i=0; i<frn; i++) {
                    unlink(fnmt.PtrStr("datafilename",i));
                    unlink(fnmt.PtrStr("indexfilename",i));
                }
                errprintf("删除中间文件记录...");
                st.Prepare("delete from dp.dp_middledatafile where tabid=%d and datapartid=%d",tabid,datapartid);
                st.Execute(1);
                st.Wait();
                st.Prepare("delete from dp.dp_filelog where tabid=%d and datapartid=%d",tabid,datapartid);
                st.Execute(1);
                st.Wait();

                /* on a busy statement,wociSetTerminate will failed and throw a exception,so the last chance to
                restore enviriement is lost. so move to here from begining or this catch block.hope this can process more stable.
                LOGS:
                [2007:11:02 10:40:31] 开始数据处理
                [2007:11:02 10:40:42] Write file failed! filename:/dbsturbo/dttemp/cas/mddt_340652.dat,blocklen:218816,offset:3371426
                [2007:11:02 10:40:42]  ErrorCode: -9.  Exception : Execute(Query) or Delete on a busy statement.
                */
                wociSetTerminate(dtdbc,false);
                wociSetTerminate(sp.GetDTS(),false);

            } else {
                // 1. 从数据库采集数据，程序退出前：直接将任务状态修改成重新导出，并执行drop数据库表，并通知排序集群终止所有的排序会话(长短会话数据)。
                // 2. 从文本文件采集数据，程序退出前: 直接将任务状态修改成重新导出，并通知排序集群终止当前的短会话排序数据，并清除当前短会话排序对应的文件(dp.dp_filelog表中清除)。
                rollback_sort_session(sp,tabid,datapartid,origion_status,filesmode);
            }
            // fix dma-2804
            // throw;
        }
    }
    if(dumpcomplete && !dp_parallel_load ) {
        lgprintf("数据抽取结束,任务状态1-->2,tabid %d(%d)",tabid,datapartid);
        lgprintf("sort time :%11.6f file io time :%11.6f adjust data time:%11.6f",
                 sorttm.GetTime(),fiotm.GetTime(),adjtm.GetTime());
        lgprintf("结束");
        sp.log(tabid,datapartid,DUMP_FINISHED_NOTIFY,"表%d,分区%d,数据抽取结束 ,记录数%lld.",tabid,datapartid,srn);
    }

    release_sorter_obj();

    if(deal_slaver_node) { // slaver节点,如果没有采集到数据,就继续处理下一个可以并发执行的任务
        if(!b_master_node && run_slaver_taskinfo_deal_rows == 0 && !run_slaver_taskinfo_finish) {
            lgprintf("slaver启动任务,没有采集到数据,继续处理下一个slaver采集任务,已处理slaver任务队列数:[%d]",run_slaver_taskinfo_vec.size());
            goto lable_dump_slaver_begin;
        }

        if(run_slaver_taskinfo_finish) {
            lgprintf("slaver启动任务,没有采集到数据,任务处理结束,继续处理下一个master采集任务,已处理slaver任务队列数:[%d]",run_slaver_taskinfo_vec.size());
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
// 判断指定列是否在索引列文件列表IndexListFile中，如果指定列columnName在则返回：true,
// 否则返回：false
bool GetColumnIndex(const char* IndexListFile,const char* columnName)
{
    FILE *fp = NULL;
    fp = fopen(IndexListFile,"rt");
    if(fp==NULL) {
        ThrowWith("打开索引列表文件失败.");
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

// 创建数据任务表，通过源表命令参数设置
int MiddleDataLoader::CreateSolidIndexTable(const char* orasvcname,const char * orausrname,const char* orapswd,
        const char* srcdbname,const char* srctabname,const char* dstdbname,const char* dsttabname,
        const char* indexlist,const char* tmppath,const char* backpath,const char *taskdate,
        const char* solid_index_list_file,char* ext_sql)
{
    int ret = 0;

    // 1-- 判断目标表是否已经存在
    AutoMt dst_table_mt(sp->GetDTS(),10);
    dst_table_mt.FetchAll("select * from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",dstdbname,dsttabname);
    if(dst_table_mt.Wait()>0)
        ThrowWith("表 %s.%s 已经存在。",dstdbname,dsttabname);

    // 2-- 连接源表数据库,判断ORACLE源表是否存在
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
        ThrowWith("源表:%s.%s 不存在无法创建表:%s.%s。",srcdbname,srctabname,dstdbname,dsttabname);
    }

    // 3-- 判断dp数据中的数据源是否存在,存在取出数据源，不存在添加数据源
    AutoMt data_src_mt(sp->GetDTS(),10);
    int  data_src_id = 0;
#define ORACLE_TYPE 1
    data_src_mt.FetchFirst("select sysid from dp.dp_datasrc where systype = %d and jdbcdbn = '%s' and username = '%s' and pswd = '%s'",
                           ORACLE_TYPE,orasvcname,orausrname,orapswd);
    int trn=data_src_mt.Wait();
    if(trn<1) {
        // dp数据库中不存在数据源，需要添加
        AutoStmt tmp_data_src_mt(sp->GetDTS());
        ret = tmp_data_src_mt.DirectExecute("INSERT INTO dp.dp_datasrc(sysid,sysname,svcname,username,pswd,systype,jdbcdbn) "
                                            " select max(sysid)+1,'%s','CreateSolidIndexTable add','%s','%s',%d,'%s' from dp.dp_datasrc",
                                            orasvcname,orausrname,orapswd,ORACLE_TYPE,orasvcname);

        if(ret != 1) {
            ThrowWith("dp 数据库插入Oracle数据源[%s],插入失败!",orasvcname);
        }

        // 获取新插入的sysid
        data_src_mt.FetchFirst("select max(sysid) as sysid from dp.dp_datasrc");
        trn = data_src_mt.Wait();
        data_src_id = data_src_mt.GetInt("sysid",0);
    } else {
        data_src_id = data_src_mt.GetInt("sysid",0);
    }

    // 4-- 判断dp.dp_path中路径ID是否存在
    // 4.1--查询临时路径
    AutoMt path_mt(sp->GetDTS(),10);
    int  data_tmp_pathid = 0,data_backup_pathid=0;
    path_mt.FetchFirst("SELECT pathid FROM dp.dp_path where pathval = '%s' and pathtype = 'temp'",tmppath);
    if(1 > path_mt.Wait()) {
        AutoStmt tmp_path_mt(sp->GetDTS());
        ret = tmp_path_mt.DirectExecute("INSERT INTO dp.dp_path(pathid,pathtype,pathdesc,pathval) values(%d,'temp','CreateSolidIndexTable add','%s')",
                                        sp->NextTableID(),tmppath);
        if(-1 == ret) ThrowWith("dp数据库插入临时路径[%s]失败!",tmppath);
        path_mt.FetchFirst("select pathid from dp.dp_path where pathval = '%s' and pathtype= 'temp'", tmppath);
        path_mt.Wait();
        data_tmp_pathid = path_mt.GetInt("pathid",0);
    } else {
        data_tmp_pathid =  path_mt.GetInt("pathid",0);
    }

    // 4.2--查询数据路径
    path_mt.FetchFirst("SELECT pathid FROM dp.dp_path where pathval = '%s' and pathtype = 'data'",backpath);
    if(1 > path_mt.Wait()) {
        AutoStmt tmp_path_mt(sp->GetDTS());
        ret = tmp_path_mt.DirectExecute("INSERT INTO dp.dp_path(pathid,pathtype,pathdesc,pathval) values(%d,'data','CreateSolidIndexTable add','%s')",
                                        sp->NextTableID(),backpath);
        if(-1 == ret) ThrowWith("dp数据库插入备份路径[%s]失败!",backpath);
        path_mt.FetchFirst("select pathid from dp.dp_path where pathval = '%s' and pathtype= 'data'", backpath);
        path_mt.Wait();
        data_backup_pathid = path_mt.GetInt("pathid",0);
    } else {
        data_backup_pathid =  path_mt.GetInt("pathid",0);
    }

    // 5-- 判断时间格式是否正确
    AutoMt mt(sp->GetDTS(),100);
    char tdt[30];
    if(strcmp(taskdate,"now()")==0) {
        wociGetCurDateTime(tdt);
    } else {
        mt.FetchAll("select adddate(cast('%s' AS DATETIME),interval 0 day) as tskdate",taskdate);
        if(mt.Wait()!=1) {
            ThrowWith("日期格式错误:'%s'",taskdate);
        }
        memcpy(tdt,mt.PtrDate("tskdate",0),7);
        if(wociGetYear(tdt)==0) {
            ThrowWith("日期格式错误:'%s'",taskdate);
        }
    }

    // 6-- 插入dp_table表中数据信息
    int table_id = 0;
    AutoMt table_mt(sp->GetDTS(),100);
    table_mt.FetchFirst("select * from dp.dp_table limit 1");
    if(table_mt.Wait() != 1) {
        ThrowWith("数据库表dp_table中没有记录，请先通过web管理平台创建一个任务后再进行该操作.");
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


    // 获取tabid
    table_id = sp->NextTableID();
    *table_mt.PtrInt("tabid",0)=table_id;

    // 7-- 检查表id是否存在记录
    AutoMt check_mt(sp->GetDTS(),10);
    // 对应的源表稍后修改
    check_mt.FetchAll("select * from dp.dp_table where tabid=%d",table_id);
    if(check_mt.Wait()>0)
        ThrowWith("编号重复: 编号%d的目标表'%s.%s'已经存在!",check_mt.GetInt("tabid",0),check_mt.PtrStr("databasename",0),
                  check_mt.PtrStr("tabname",0));
    check_mt.FetchAll("select * from dp.dp_index where tabid=%d",table_id);
    if(check_mt.Wait()>0)
        ThrowWith("发现不正确的索引参数(表%d)！",check_mt.GetInt("tabid",0));
    check_mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2",table_id);
    if(check_mt.Wait()>0)
        ThrowWith("发现不正确的数据文件记录(表%d)!",check_mt.GetInt("tabid",0));
    check_mt.FetchAll("select * from dp.dp_datapart where tabid=%d",table_id);
    if(check_mt.Wait()>0)
        ThrowWith("发现不正确的数据分组参数(表%d)!",check_mt.GetInt("tabid",0));


    // 8-- 插入dp_index表中数据信息

    // 索引列名
    char idx_col_name[256];
    idx_col_name[sizeof(idx_col_name) - 1] = 0;
    try {
        // 8.1 -- 查询oracle源数据库表，获取列信息,获取sql语句
        sp->IgnoreBigSizeColumn(ora_dts,srcdbname,srctabname,ext_sql);

        // 8.2 -- 获取源表结构
        AutoMt  src_table_mt(ora_dts,10);
        src_table_mt.FetchFirst(ext_sql);
        src_table_mt.Wait();

        // 8.3 -- 如果源表结构中存在mysql关键字，则将其替换掉
        char cfilename[256];
        strcpy(cfilename,getenv("DATAMERGER_HOME"));
        strcat(cfilename,"/ctl/");
        strcat(cfilename,MYSQL_KEYWORDS_REPLACE_LIST_FILE);
        sp->ChangeMtSqlKeyWord(src_table_mt,cfilename);

        // 8.4 -- 逐个索引列判断
        bool get_column_index_flag = false;
        bool index_column_name_error = false;
#define INDEX_SEPERATOR ','
        if(strlen(indexlist) > 0) {
            // 判断索引是否合法
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
                        _find_col_item = true; // 找到数据
                        break;
                    }
                }

                if(!_find_col_item) {
                    index_column_name_error = true;  // 列名称错误了
                    break;
                }

                _index_string = _index_string.substr(_pos+1,_index_string.size() - _pos);
                _pos = _index_string.find(INDEX_SEPERATOR);
            }

            if(!index_column_name_error) { // 列没有错误的情况下，处理最后一列
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
                    index_column_name_error = true;  // 列名称错误了
                }
            }

            if(!index_column_name_error) { // 所有列校验完成，没有发现错误
                strcpy(idx_col_name,_indexlist);
                get_column_index_flag = true;
            }
        }

        if(!get_column_index_flag) { // 没有指定索引，使用默认的索引
            for(int i=0; i<wociGetColumnNumber(src_table_mt); i++) {
                wociGetColumnName(src_table_mt,i,idx_col_name);
                if(GetColumnIndex(solid_index_list_file,idx_col_name)) {
                    get_column_index_flag = true;
                    break;
                }
            }
            if(!get_column_index_flag) { // 列表文件中没有找到索引，就用第一个列作为索引
                wociGetColumnName(src_table_mt,0,idx_col_name);
            }
        }
    } catch(...) {
        ThrowWith("源表:%s.%s 不存在无法创建表:%s.%s。",srcdbname,srctabname,dstdbname,dsttabname);
    }


    // 8.3 -- 插入dp_index表中记录
    AutoMt index_mt(sp->GetDTS(),10);
    index_mt.FetchFirst("select * from dp.dp_index limit 1");
    if(index_mt.Wait() != 1) {
        ThrowWith("数据库表dp.dp_index中没有记录，请先通过web管理平台创建一个任务后再进行该操作.");
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

    // 9-- 插入dp_datapart表中数据信息
    AutoMt datapart_mt(sp->GetDTS(),10);
    datapart_mt.FetchFirst("select * from dp.dp_datapart limit 1");
    if(datapart_mt.Wait() != 1) {
        ThrowWith("数据库表dp.dp_datapart中没有记录，请先通过web管理平台创建一个任务后再进行该操作.");
    }

    // 9.1 -- 判断源表中是否存在大字段，如果存在大字段就将其过滤掉
    sp->IgnoreBigSizeColumn(ora_dts,srcdbname,srctabname,ext_sql);

    // 9.2 -- 保存任务信息到dp_datapart表中
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
        //恢复数据，回退所有操作
        AutoStmt st(sp->GetDTS());
        st.DirectExecute("delete from dp.dp_table where tabid=%d",table_id);
        st.DirectExecute("delete from dp.dp_index where tabid=%d",table_id);
        st.DirectExecute("delete from dp.dp_datapart where tabid=%d",table_id);
        errprintf("作类似创建时数据提交失败，已删除数据。");
        throw;
    }
    {
        char dtstr[100];
        wociDateTimeToStr(tdt,dtstr);
        lgprintf("创建成功,目标表'%s.%s:%d',开始时间'%s'.",dstdbname,dsttabname,table_id,dtstr);
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
            ThrowWith("日期格式错误:'%s'",taskdate);
        memcpy(tdt,mt.PtrDate("tskdate",0),7);
        if(wociGetYear(tdt)==0)
            ThrowWith("日期格式错误:'%s'",taskdate);
    }
    mt.FetchAll("select * from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",ndbn,ndsttbn);
    if(mt.Wait()>0)
        ThrowWith("表 %s.%s 已经存在。",dbn,ndsttbn);

    //>> 1. 创建表信息(dp.dp_table)
    AutoMt tabmt(sp->GetDTS(),100);
    tabmt.FetchAll("select * from dp.dp_table where databasename=lower('%s') and lower(tabname)='%s'",dbn,tbn);
    if(tabmt.Wait()!=1)
        ThrowWith("参考表 %s.%s 不存在。",dbn,tbn);
    int reftabid=tabmt.GetInt("tabid",0);
    //填充目标表信息
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
    //参考源表后面还要引用,暂时不替换
    tabid=sp->NextTableID();
    *tabmt.PtrInt("tabid",0)=tabid;
    // 对应的源表稍后修改
    mt.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
    if(mt.Wait()>0)
        ThrowWith("编号重复: 编号%d的目标表'%s.%s'已经存在!",mt.GetInt("tabid",0),mt.PtrStr("databasename",0),mt.PtrStr("tabname",0));
    mt.FetchAll("select * from dp.dp_index where tabid=%d",tabid);
    if(mt.Wait()>0)
        ThrowWith("发现不正确的索引参数(表%d)！",mt.GetInt("tabid",0));
    mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2",tabid);
    if(mt.Wait()>0)
        ThrowWith("发现不正确的数据文件记录(表%d)!",mt.GetInt("tabid",0));
    mt.FetchAll("select * from dp.dp_datapart where tabid=%d",tabid);
    if(mt.Wait()>0)
        ThrowWith("发现不正确的数据分组参数(表%d)!",mt.GetInt("tabid",0));

    //>> 2. 创建独立索引(dp.dp_index)
    AutoMt indexmt(sp->GetDTS(),200);
    indexmt.FetchAll("select * from dp.dp_index where tabid=%d",reftabid);
    int irn=indexmt.Wait();
    if(irn<1)
        ThrowWith("参考表 %s.%s 没有建立索引。",dbn,tbn);
    int soledct=1;
    //填充索引信息，重建复用关系
    int nSrLen =indexmt.GetColLen((char*)"indextabname");
    char *pStrVal = new char[irn*nSrLen];
    memset(pStrVal,0,irn*nSrLen);
    for(int ip=0; ip<irn; ip++) {
        *indexmt.PtrInt("tabid",ip)=tabid;
    }
    indexmt.SetStr("indextabname",0,pStrVal,irn);
    delete[] pStrVal;

    //>> 3. 创建扩展字段信息(dp_column_info)
    int externed_field_num = 0; // 是否存在扩展字段
    AutoMt externedMt(sp->GetDTS(),200) ;
    externedMt.FetchAll("select * from dp.dp_column_info where table_id=%d",reftabid);
    externed_field_num=externedMt.Wait();
    for(int ip=0; ip<externed_field_num; ip++) {
        *externedMt.PtrInt("table_id",ip)=tabid;
    }

    //>>  4. 创建任务信息(dp.dp_datapart)
    AutoMt taskmt(sp->GetDTS(),500);
    taskmt.FetchAll("select * from dp.dp_datapart where tabid=%d",reftabid);
    int trn=taskmt.Wait();
    if(trn<1)
        ThrowWith("参考表 %s.%s 没有数据分组信息。",dbn,tbn);

    //对数据抽取语句作大小写敏感的源表名称替换,可能有以下问题:
    // 1. 如果字段名或常量名中与源表名称相同,会造成替换失败
    // 2. 如果源表名称大小写不一致,会造成替换失败
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
                if(sch_load) { // 原样拷贝extsql语句
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
                if(sch_load) { // 原样拷贝extsql语句
                    taskmt.SetStr("extsql",tp,tmp);
                } else {
                    if(sch) {
                        if(schx) *schx=0;
                        sch+=6;
                        //复制 from 之前的语句(含 from );
                        strncpy(extsql,psql,sch-tmp);
                        extsql[sch-tmp]=0;
                        bool fullsrc=true;
                        int tablen=strlen(treffull);
                        char *sch2=strstr(sch,treffull);
                        if(sch2==NULL) {
                            //参考表的sql语句中不含格式表的完整形式
                            //检查是否含部分（只有表名，没有库名)
                            sch2=strstr(sch,prefsrctbn);
                            fullsrc=false;
                            tablen=strlen(prefsrctbn);
                        }
                        //至少要含完整或部分形式的格式表，才替换
                        if(sch2) {
                            //只把from ... where 之间符合替换条件的部分替换，其它的保留
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
            lgprintf("数据分组%d的数据抽取语句未作修改，请根据需要手工调整.",taskmt.GetInt("datapartid",tp));
        else
            lgprintf("数据分组%d的数据抽取语句已经修改：\n%s\n--->\n%s\n请根据需要手工调整.",taskmt.GetInt("datapartid",tp),sqlbk,psql);

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
        //恢复数据，回退所有操作
        AutoStmt st(sp->GetDTS());
        st.DirectExecute("delete from dp.dp_table where tabid=%d",tabid);
        st.DirectExecute("delete from dp.dp_index where tabid=%d",tabid);
        st.DirectExecute("delete from dp.dp_datapart where tabid=%d",tabid);
        if(externed_field_num>0) {
            st.DirectExecute("delete from dp.dp_column_info where table_id=%d",tabid);
        }
        errprintf("作类似创建时数据提交失败，已删除数据。");
        throw;
    }
    {
        char dtstr[100];
        wociDateTimeToStr(tdt,dtstr);
        lgprintf("创建成功,源表'%s',目标表'%s',开始时间'%s'.",nsrctbn,ndsttbn,dtstr);
    }
    return tabid;
}

//DT data&index File Check
int MiddleDataLoader::dtfile_chk(const char *dbname,const char *tname)
{
    //Check deserved temporary(middle) fileset
    //检查状态为1的任务
    mdf.FetchAll("select * from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",dbname,tname);
    int rn=mdf.Wait();
    if(rn<1) ThrowWith("DP文件检查:在dp_table中'%s.%s'目标表无记录。",dbname,tname);
    int firstfid=mdf.GetInt("firstdatafileid",0);
    int tabid=*mdf.PtrInt("tabid",0);
    int blockmaxrn=*mdf.PtrInt("maxrecinblock",0);
    double totrc=mdf.GetDouble("recordnum",0);
    sp->OutTaskDesc("目标表检查 :",0,tabid);
    char *srcbf=new char[SRCBUFLEN];//每一次处理的最大数据块（解压缩后）。
    int errct=0;
    mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2 order by indexgid,fileid",tabid);
    int irn=mdf.Wait();
    if(irn<1) {
        ThrowWith("DP文件检查:在dp_datafilemap中%d目标表无数据文件的记录。",tabid);
    }

    {
        AutoMt datmt(sp->GetDTS(),100);
        datmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileid=%d",tabid,firstfid);
        if(datmt.Wait()!=1)
            ThrowWith("开始数据文件(编号%d)在系统中没有记录.",firstfid);
        char linkfn[300];
        strcpy(linkfn,datmt.PtrStr("filename",0));
        datmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2 and isfirstindex=1 order by datapartid,fileid",tabid);
        int rn1=datmt.Wait();
        if(firstfid!=datmt.GetInt("fileid",0))
            ThrowWith("开始数据文件(编号%d)在设置错误，应该是%d..",firstfid,datmt.GetInt("fileid",0));
        int lfn=0;
        while(true) {
            file_mt dtf;
            lfn++;
            dtf.Open(linkfn,0);
            const char *fn=dtf.GetNextFileName();
            if(fn==NULL) {
                printf("%s==>结束.\n",linkfn);
                break;
            }
            printf("%s==>%s\n",linkfn,fn);
            strcpy(linkfn,fn);
        }
        if(lfn!=rn1)
            ThrowWith("文件链接错误，缺失%d个文件.",rn1-lfn);
    }

    mytimer chktm;
    chktm.Start();
    try {
        int oldidxid=-1;
        for(int iid=0; iid<irn; iid++) {
            //取基本参数
            int indexid=mdf.GetInt("indexgid",iid);

            AutoMt idxsubmt(sp->GetDTS(),100);
            idxsubmt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d ",tabid,indexid);
            rn=idxsubmt.Wait();
            if(rn<1) {
                ThrowWith("DP文件检查:在dp.dp_index中%d目标表无%d索引的记录。",tabid,indexid);
            }

            printf("检查文件%s--\n",mdf.PtrStr("filename",iid));
            fflush(stdout);

            //遍历全部索引数据
            char dtfn[300];
            dt_file idxf;
            idxf.Open(mdf.PtrStr("idxfname",iid),0);
            printf("索引文件：%s.\n",mdf.PtrStr("idxfname",iid));
            AutoMt idxmt(0);
            idxmt.SetHandle(idxf.CreateMt(1));
            idxmt.SetHandle(idxf.CreateMt(FIX_MAXINDEXRN/wociGetRowLen(idxmt)));
            idxf.Open(mdf.PtrStr("idxfname",iid),0);
            int brn=0;//idxf.ReadMt(-1,0,idxmt,false);
            int sbrn=0;
            while( (sbrn=idxf.ReadMt(-1,0,idxmt,true,1))>0) brn+=sbrn;
            printf("索引%d行。\n",brn);
            int thiserrct=errct;
            //把索引数据文件的后缀由idx替换为dat就是数据文件.
            AutoMt destmt(0);
            strcpy(dtfn,mdf.PtrStr("idxfname",iid));
            strcpy(dtfn+strlen(dtfn)-3,"dat");
            //从数据文件取字段结构，内存表大小为目标表的每数据块最大行数。
            //destmt.SetHandle(dtf.CreateMt(blockmaxrn));
            int myrowlen=0;
            {
                dt_file datf;
                datf.Open(dtfn,0);
                myrowlen=datf.GetMySQLRowLen();
            }
            FILE *fp=fopen(dtfn,"rb");
            printf("检查数据文件：%s.\n",dtfn);
            if(fp==NULL)
                ThrowWith("DP文件检查:文件'%s'错误.",dtfn);
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
                //直接使用字段名称会造成idx_rownum字段的名称不匹配，早期的idx数据文件中的字段名为rownum.
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
                            errprintf("读数据错误，位置:%d,长度:%d,索引序号:%d.\n",blockstart,blocksize,i);
                            throw -1;
                        }
                        pbhdr->ReverseByteOrder();
                        if(!dt_file::CheckBlockFlag(pbhdr->blockflag)) {
                            errprintf("错误的块标识，位置:%d,长度:%d,索引序号:%d.\n",blockstart,blocksize,i);
                            throw -1;
                        }
                        if(pbhdr->blockflag!=bkf) {
                            bkf=pbhdr->blockflag;
                            //if(bkf==BLOCKFLAG) printf("数据块类型:WOCI.\n");
                            //else if(bkf==MYSQLBLOCKFLAG)
                            //  printf("数据块类型:MYISAM.\n");
                            printf("数据块类型: %s .\n",dt_file::GetBlockTypeName(pbhdr->blockflag));
                            printf("压缩类型:%d.\n",pbhdr->compressflag);
                        }
                        if(pbhdr->storelen!=blocksize-sizeof(block_hdr)) {
                            errprintf("错误的块长度，位置:%d,长度:%d,索引序号:%d.\n",blockstart,blocksize,i);
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
                            errprintf("太多的错误，已放弃检查！");
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
                errprintf("文件长度不匹配，数据文件长度:%d,索引文件指示的结束位置:%d\n",flen,ftell(fp));
                errct++;
            }
            printf("文件检查完毕，错误数 ：%d.    \n",errct-thiserrct);
            fclose(fp);
        }// end of for
    } // end of try
    catch (...) {
        errprintf("DP文件检查出现异常，tabid:%d.",tabid);
    }
    delete []srcbf;
    if(errct>0)
        errprintf("DP文件检查出现错误，请重新装入或者重新抽取数据.");
    printf("\n");
    chktm.Stop();
    lgprintf("DP文件检查结束,共处理%d个文件,发现%d个错误(%.2fs).",irn,errct,chktm.GetTime());
    return 1;
}

//任务状态是已导出，但没有需要整理的文件（抽取空表），任务状态直接修改为已整理
void MiddleDataLoader::CheckEmptyDump()
{
    //并行整理修改 2011 07 01
    mdf.FetchAll("select * from dp.dp_datapart where status in(1,2) %s limit 100",sp->GetNormalTaskDesc());
    int rn=mdf.Wait();
    if(rn>0) {
        while(--rn>=0) {
            //并行整理修改 2011 07 01
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
                lgprintf("表%d(%d)的数据为空，修改为已整理。",mdf.GetInt("tabid",rn),mdf.GetInt("datapartid",rn));
            }
        }
    }
}
// MAXINDEXRN 缺省值500MB,LOADIDXSIZE缺省值1600MB
// MAXINDEXRN 为最终索引文件,记录长度对应索引表.
// LOADIDXSIZE 存储临时索引,记录长度从数据导出时的索引文件中内存表计算.

//2005/08/24修改： MAXINDEXRN不再使用
//2005/11/01修改： 装入状态分为1(待装入），2（正装入),3（已装入);取消10状态(原用于区分第一次和第二次装入)
//2005/11/01修改： 索引数据装入内存时的策略改为按索引分段。
//2006/02/15修改： 一次装入所有索引数据，改为分段装入
//          一次只装入每个文件中的一个内存表(解压缩后1M,索引记录设50字节长,则有约2万条记录,1G内存可以容纳1000/(1*1.2)=800个数据文件)
//procstate状态说明：
//  >1 :正在导出数据
//  <=1:已完成数据导出
// 2011-06-23：增加InfoBright的直接导入功能
//      1. 建立连接，设置非自动提交，检查并建立目标表 目标数据库
//      2. 打开管道
//      3. 后台方式启动导入
//      4. 延时2秒后开始从管道导入目标数据
//      5. 出现错误回退导入的数据
//      6. 执行完成后提交
// 目标表的清空需要人工操作
// 暂时借用DM中按列存储方式为IB引擎的标志。
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
        ThrowWith("DMC分布式排序版本(export RIAK_HOSTNAME=%s):不支持数据整理过程,数据整理过程由分布式排序集群完成.",pRiakHost);
    }

    //Check deserved temporary(middle) fileset
    //检查状态为1的任务，1为抽取结束等待装入.
    CheckEmptyDump();
    //2010-12-01 增加分区任务状态必须是2的选项
    //2011-07-02 并行整理修改，按待整理中间数据数量排序
    mdf.FetchAll("select dp.status,mf.blevel,mf.tabid,mf.datapartid,mf.indexgid,sum(filesize) fsz from dp.dp_middledatafile mf,dp.dp_datapart dp "
                 " where mf.procstate=1 and mf.tabid=dp.tabid and mf.datapartid=dp.datapartid  "
                 " and ifnull(dp.blevel,0)%s and dp.status in(1,2) "// 分区允许并行整理 DMA-35 and mf.indexgid=1" //并行整理修改 20110701
                 " group by mf.blevel,mf.tabid,mf.datapartid,mf.indexgid "
                 " order by dp.status desc,mf.blevel,mf.tabid,fsz desc,mf.datapartid,mf.indexgid limit %d",
                 sp->GetNormalTask()?"<100":">=100",DATATRIM_NUM);
    int rn=mdf.Wait();
    //如果索引超过LOADIDXSIZE,需要分批处理,则只在处理第一批数据时清空DT_DATAFILEMAP/DT_INDEXFILEMAP表
    //区分第一次和第二次装入的意义：如果一份数据子集（指定数据集-〉分区(datapartid)->索引)被导出后，在开始装入以前，要
    //  先删除上次正在装入的数据（注意：已上线的数据不在这里删除，而在装入结束后删除).
    bool firstbatch=true;
    if(rn<1) return 0;

    //>> begin: fix DMA-909 ,多个正在导出的任务可以让其进行期中部分数据的整理
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

    //检查该数据子集是否第一次装入
    int tabid=data_trim_arry[_datapart_index].tabid;
    int indexid=data_trim_arry[_datapart_index].indexgid;
    int datapartid=data_trim_arry[_datapart_index].datapartid;

    mdf.FetchAll("select procstate from dp.dp_middledatafile "
                 " where tabid=%d and datapartid=%d and indexgid=%d and procstate>1 limit 10",
                 tabid,datapartid,indexid);
    firstbatch=mdf.Wait()<1;//数据子集没有正整理或已整理的记录。

    //取出中间文件记录
    mdf.FetchAll("select * from dp.dp_middledatafile "
                 " where  tabid=%d and datapartid=%d and indexgid=%d and procstate=1 order by mdfid limit %d",
                 tabid,datapartid,indexid,MAX_MIDDLE_FILE_NUM);
    rn=mdf.Wait();
    if(rn<1) {
        sp->log(tabid,datapartid,MLOAD_CAN_NOT_FIND_MIDDLEDATA_ERROR,"表%d,分区%d,确定数据子集后找不到中间数据记录(未处理)。",tabid,datapartid);
        ThrowWith("MiddleDataLoader::Load : 确定数据子集后找不到中间数据记录(未处理)。");
    }
    long idxtlimit=0,idxdlimit=0;//临时区(多个抽取数据文件对应)和目标区(单个目标数据文件对应)的索引最大记录数.
    tabid=mdf.GetInt("tabid",0);
    sp->SetTrace("datatrim",tabid);
    indexid=mdf.GetInt("indexgid",0);
    datapartid=mdf.GetInt("datapartid",0);
    int compflag=5;
    //并行整理修改 20110701
    int curdatapartstatus=1; //
    char db[300],table[300];
    bool col_object=false; // useOldBlock=true时，忽略col_object
    //取压缩类型和数据块类型
    {
        AutoMt tmt(sp->GetDTS(),10);
        //并行整理修改 20110701
        tmt.FetchAll("select compflag,status from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
        if(tmt.Wait()>0) {
            compflag=tmt.GetInt("compflag",0);
            //并行整理修改 20110701
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
    //从dt_datafilemap(存blockmt文件表)和dt_indexfilemap(存indexmt文件表)
    //建立内存表结构
    char fn[300];
    AutoMt fnmt(sp->GetDTS(),MAX_DST_DATAFILENUM);

    fnmt.FetchAll("select * from dp.dp_datafilemap limit 2");
    fnmt.Wait();
    wociReset(fnmt);
    LONG64 dispct=0,lstdispct=0,llt=0;
    sp->OutTaskDesc("数据重组 :",tabid,datapartid,indexid);
    sp->log(tabid,datapartid,MLOAD_DATA_RECOMBINATION_NOTIFY,"表%d,分区%d,数据重组,索引组%d,日志文件 '%s' .",tabid,datapartid,indexid,wociGetLogFile());
    int start_mdfid=0,end_mdfid=0;
    char sqlbf[MAX_STMT_LEN];
    LONG64 extrn=0,lmtextrn=-1;
    LONG64 adjrn=0;
    //IB改造
    //增加文件句柄在try以外
    FILE *ibfile=NULL;
    bool ib_engine=false;
    char ibfilename[300];
    try {
        tmpfilenum=rn;
        //索引数据文件遍历，累加索引总行数
        LONG64 idxrn=0;
        long i;
        long mdrowlen=0;
        //取索引记录的长度(临时索引数据记录)
        {
            dt_file df;
            df.Open(mdf.PtrStr("indexfilename",0),0);
            mdrowlen=df.GetRowLen();
        }

        lgprintf("临时索引数据记录长度:%d",mdrowlen);
        long lmtrn=-1,lmtfn=-1;
        //检查临时索引的内存用量,判断当前的参数设置是否可以一次装入全部索引记录
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
                //使用的临时索引超过内存允许参数的上限，需要拆分
                if(i==0) {
                    sp->log(tabid,datapartid,MLOAD_DP_LOADTIDXSIZE_TOO_LOW_ERROR,"内存参数DP_LOADTIDXSIZE设置太低:%dMB，不足以装载至少一个临时抽取块:%dMB。\n",LOADTIDXSIZE,(int)llt);
                    ThrowWith("MLoader:内存参数DP_LOADTIDXSIZE设置太低:%dMB，\n"
                              "不足以装载至少一个临时抽取块:%dMB。\n",LOADTIDXSIZE,(int)llt);
                }

                lmtrn=idxrn-df.GetRowNum();
                lmtfn=i;
                lmtextrn=extrn-mdf.GetInt("recordnum",i);
            }
            if(idxrn > (MAX_ROWS_LIMIT-8) && lmtrn==-1) {
                // 记录行数超过2G条控制
                lmtrn=idxrn-df.GetRowNum();
                lmtfn=i;
                lmtextrn=extrn-mdf.GetInt("recordnum",i);
            }
            if((wociGetMaxMemTable()==(i+1)) && (-1==lmtrn)) {
                // 最大能够打开的文件的内存表记录
                lmtrn=idxrn-df.GetRowNum();
                lmtfn=i;
                lmtextrn=extrn-mdf.GetInt("recordnum",i);
            }
        }
        if(lmtrn!=-1) { //使用的临时索引超过内存允许参数的上限，需要拆分
            lgprintf("MLoader:数据整理超过内存限制%dMB,需要处理文件数%d,数据组%d,起始点:%d,截至点:%d,文件数:%d,记录数:%d.",LOADTIDXSIZE,rn,datapartid,mdf.GetInt("mdfid",0),mdf.GetInt("mdfid",lmtfn-1),lmtfn,lmtrn);
            lgprintf("索引需要内存%dM ",idxrn*mdrowlen/(1024*1024));
            sp->log(tabid,datapartid,MLOAD_DATA_NOTIFY,
                    "MLoader:数据整理超过内存限制%dMB,需要处理文件数%d,起始点:%d,截至点:%d,文件数:%d ,记录数:%d,需要内存%dMB.",LOADTIDXSIZE,rn,mdf.GetInt("mdfid",0),mdf.GetInt("mdfid",lmtfn-1),lmtfn,lmtrn,idxrn*mdrowlen/(1024*1024));
            idxrn=lmtrn;
            //fix a bug
            rn=lmtfn;
        } else if(curdatapartstatus==1) {
            lgprintf("并行整理提示：需要整理的数据还不足最大索引内存量(文件数%d索引%dM记录%d内存量%dM)，等待...",rn,idxrn*mdrowlen/(1024*1024),extrn,LOADTIDXSIZE);

            //>>Begin:fix  DMA-909
            if(_datapart_index<_datapart_num-1) { // 该分区不满足数据处理条件，处理下一个分区
                _datapart_index++;
                goto start_datatrim;
            }
            //<<End:fix DMA-909

            return 0;
        }
        if(lmtextrn==-1) lmtextrn=extrn;
        lgprintf("数据整理实际使用内存%dM,索引列宽%d.",idxrn*mdrowlen/(1024*1024),mdrowlen);
        start_mdfid=mdf.GetInt("mdfid",0);
        end_mdfid=mdf.GetInt("mdfid",rn-1);
        lgprintf("索引记录数:%d",idxrn);
        //为防止功能重入,中间文件状态修改.
        lgprintf("修改中间文件的处理状态(tabid:%d,datapartid:%d,indexid:%d,%d个文件)：1-->2",tabid,datapartid,indexid,rn);
        //抽取过程会把2改回1，因此先改为20？
        //客户端不能处理20，改回2
        sprintf(sqlbf,"update dp.dp_middledatafile set procstate=2 where tabid=%d  and datapartid=%d and indexgid=%d and procstate=1 and mdfid>=%d and mdfid<=%d ",
                tabid,datapartid,indexid,start_mdfid,end_mdfid);
        int ern=sp->DoQuery(sqlbf);
        if(ern!=rn) {
            if(ern>0) {  //上面的UPdate语句修改了一部分记录状态,且不可恢复,需要对数据子集作重新装入.
                sp->log(tabid,datapartid,MLOAD_UPDATE_MIDDLE_FILE_STATUS_ERROR,"表%d,分区%d,修改中间文件的处理状态异常，可能是与其它进程冲突.部分文件的处理状态不一致，请立即停止所有的数据整理任务，重新作数据整理操作。",
                        tabid,datapartid);

                ThrowWith("MLoader修改中间文件的处理状态异常，可能是与其它进程冲突。\n"
                          " 部分文件的处理状态不一致，请立即停止所有的数据整理任务，重新作数据整理操作。\n"
                          "  tabid:%d,datapartid:%d,indexid:%d.\n",
                          tabid,datapartid,indexid);
            } else { //上面的update语句未产生实际修改操作,其它进程可以继续处理.
                //不要用ThrowWith,否则会引起catch中恢复dp_middledatafile表.
                errprintf("MLoader修改中间文件的处理状态异常，可能是与其它进程冲突。\n"
                          "  tabid:%d,datapartid:%d,indexid:%d.\n",
                          tabid,datapartid,indexid);
                return 0;
            }
        }

        //ThrowWith("调试终止---%d组数据.",dispct);
        if(firstbatch) {
            lgprintf("删除数据分组%d,索引%d 的数据和索引记录(表:%d)...",datapartid,indexid,tabid);
            sp->log(tabid,datapartid,MLOAD_DELETE_DATA_NOTIFY,"表%d,分区%d,删除索引组%d过去生成的数据和索引记录...",tabid,datapartid,indexid);
            AutoMt dfnmt(sp->GetDTS(),MAX_DST_DATAFILENUM);
            dfnmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and datapartid=%d and indexgid=%d and fileflag=1",tabid,datapartid,indexid);
            int dfn=dfnmt.Wait();
            if(dfn>0) {
                AutoStmt st(sp->GetDTS());
                for(int di=0; di<dfn; di++) {
                    lgprintf("删除'%s'/'%s'和附加的depcp,dep5文件",dfnmt.PtrStr("filename",di),dfnmt.PtrStr("idxfname",di));
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

        //建立中间索引(中间文件数据块索引)内存表mdindexmt和目标数据块内存表blockmt
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
        //IB引擎借用按列格式
        ib_engine=col_object;
        if(!ib_engine) {
            sp->log(tabid,datapartid,MLOAD_STORAGE_FORMAT_ERROR,"DM版本不支持表%d的存储格式",tabid);
            ThrowWith("DM版本不支持表%d的存储格式",tabid);
        }
        MySQLConn ibe;
        IBDataFile ib_datafile;


        LONG64 crn=0;
        //  wociGetIntAddrByName(idxmt,"dtfileid",0,&pdtfid);
        // pdtfid为一个字符数组，偏移为x的值表示中间索引内存表第x行的文件序号(Base0);
        if(pdtfid)
            delete [] pdtfid;
        pdtfid=new unsigned short [idxrn];
        //dtfidlen=idxrn;
        //pdtf为file_mt对象的数组。存放数据文件对象。
        if(pdtf) delete [] pdtf;
        pdtf=new file_mt[rn];
        //mdindexmt.SetMaxRows(idxrn);
        //读入全部索引数据到mdindexmt(中间索引内存表),并打开全部数据文件
        //pdtfid指向对应的文件序号。
        lgprintf("读索引数据...");
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
        lgprintf("索引数据:%d.",crn);
        if(crn!=idxrn) {
            sp->log(tabid,0,MLOAD_INDEX_DATA_FILE_RECORD_NUM_ERROR,"表%d,索引数据文件的总记录数%lld,与指示信息不一致:%lld",tabid,crn,idxrn);
            ThrowWith("索引数据文件的总记录数%lld,与指示信息不一致:%lld",crn,idxrn);
        }
        //对mdindexmt(中间索引内存表)做排序。
        //由于排序不涉及内存表的数据排列，而是新建记录顺序表，因此，
        // pdtfid作为内存表外的等效内存表字段，不需做处理。
        lgprintf("排序('%s')...",mdf.PtrStr("soledindexcols",0));
        {
            char sort[300];
            sprintf(sort,"%s,idx_fid,idx_blockoffset",mdf.PtrStr("soledindexcols",0));
            wociSetSortColumn(mdindexmt,sort);
            wociSortHeap(mdindexmt);
        }
        lgprintf("排序完成.");
        //取得全部独立索引结构
        sp->GetSoledIndexParam(datapartid,&dp,tabid);
        //检查需要处理的中间数据是否使用主独立索引，如果是，isfirstidx=1.
        int isfirstidx=0;
        indexparam *ip;
        {
            int idxp=dp.GetOffset(indexid);
            ip=&dp.idxp[idxp];
            if(idxp==dp.psoledindex) isfirstidx=1;
        }
        //从结构描述文件建立indexmt,indexmt是目标索引内存表。是建立目标索引表的数据源。
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
        //取独立索引在mdindexmt(中间索引存表)结构中的位置。
        //设置对indexmt插入记录需要的结构和变量。
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
        //indexmt中的blocksize,blockstart?,blockrownum需要滞后写入，
        //因而需要取出这些字段的首地址。
        int *pblocksize;
        int *pblockstart;
        int *pblockrn;
        wociGetIntAddrByName(indexmt,"blocksize",0,&pblocksize);
        wociGetIntAddrByName(indexmt,"blockstart",0,&pblockstart);
        wociGetIntAddrByName(indexmt,"blockrownum",0,&pblockrn);
        //mdindexmt中下列字段是读中间数据文件的关键项。
        LONG64 *poffset;
        int *pstartrow,*prownum;
        wociGetLongAddrByName(mdindexmt,"idx_blockoffset",0,&poffset);
        wociGetIntAddrByName(mdindexmt,"idx_startrow",0,&pstartrow);
        wociGetIntAddrByName(mdindexmt,"idx_rownum",0,&prownum);

        //indexmt 记录行数计数复位
        int indexmtrn=0;

        //建立目标数据文件和目标索引文件对象(dt_file).
        // 目标数据文件和目标索引文件一一对应，目标数据文件中按子块方式存储内存表
        //  目标索引文件中为一个单一的内存表，文件头结构在写入内存表时建立
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
                sp->log(tabid,datapartid,MLOAD_CAN_NOT_MLOAD_DATA_ERROR,"表%d,分区%d,文件'%s'已经存在，不能继续整理数据。",tabid,datapartid,fn);
                ThrowWith("文件'%s'已经存在，不能继续整理数据。",fn);
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
        lgprintf("开始数据处理(MiddleDataLoading)....");

        /*******按照Sort顺序遍历mdindexmt(中间索引内存表)***********/
        lgprintf("创建文件,编号:%d...",dtfid);
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
                //要处理的数据块和上次的数据块不是一个关键字，
                // 所以，先保存上次的关键字索引，关键字的值从blockmt中提取。
                // startrow变量始终保存第一个未存储关键字索引的数据块开始行号。
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
                //这里，出现startrow暂时指向无效行的情况(数据未填充).
                startrow=blockrn;
                rownum=0;
            }
            //从数据文件中读入数据块
            int mt=pdtf[rfid].ReadBlock(poffset[thisrn],0,true);
            //2005/08/24 修改： 索引数据文件为多块数据顺序存储

            //由于结束当前文件有可能还要增加一条索引,因此判断内存表满的条件为(idxdlimit-1)
            int irn=wociGetMemtableRows(indexmt);
            if(irn>=(idxdlimit-2)) {
                int pos=irn-1;
                //如果blocksize，blockrn等字段还未设置，则不清除
                while(pos>=0 && pblockstart[pos]==blockstart) pos--;
                if(pos>0) {
                    //保存已经设置完整的索引数据,false参数表示不需要删除位图区.
                    //IB改造 忽略索引数据写入
                    if(!ib_engine)
                        idxf.WriteMt(indexmt,COMPRESSLEVEL,pos+1,false);
                    if(pos+1<irn)
                        wociCopyRowsTo(indexmt,indexmt,0,pos+1,irn-pos-1);
                    else wociReset(indexmt);
                } else {
                    sp->log(tabid,datapartid,MLOAD_INDEX_NUM_OVER_ERROR,"目标表%d,索引号%d,装入时索引超过最大单一块允许记录数%d",tabid,indexid,idxdlimit);
                    ThrowWith("目标表%d,索引号%d,装入时索引超过最大单一块允许记录数%d",tabid,indexid,idxdlimit);
                }
            }
            //数据块拆分
            //检查数据块是否需要拆分
            // 增加一个循环，用于处理mt中的sbrn本身大于maxblockrn的异常情况：
            //  由于异常原因导致临时导出块大于最终数据块，在索引聚合良好时，会出现不能一次容纳所有临时记录
            while(true) {
                if(blockrn+sbrn>maxblockrn ) {
                    //每个数据块至少需要达到最大值的80%。
                    if(blockrn<maxblockrn*.8 ) {
                        //如果不足80%，把当前处理的数据块拆分
                        int rmrn=maxblockrn-blockrn;
                        wociCopyRowsTo(mt,blockmt,-1,sbstart,rmrn);
                        rownum+=rmrn;
                        sbrn-=rmrn;
                        sbstart+=rmrn;
                        blockrn+=rmrn;
                    }

                    //保存块数据
                    //IB改造 写入数据
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
                    //保存子快索引
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
                    //数据文件长度超过2G时拆分
                    // IB may be need spilt more small to provide more parallel performance
                    // TODO : JIRA DMA-36
                    // 改为1G
                    if(blockstart>1*1024*1024*1024 ) {
                        //增加文件对照表记录(dt_datafilemap)
                        {
                            void *fnmtptr[20];
                            int idxfsize=0;
                            //IB改造 写入数据
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
                                sp->log(tabid,datapartid,MLOAD_FILE_EXISTS_ERROR,"表%d,分区%d,文件'%s'已经存在，不能继续整理数据。",tabid,datapartid,fn);
                                ThrowWith("文件'%s'已经存在，不能继续整理数据。",fn);
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
                        lgprintf("创建文件,编号:%d...",dtfid);
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
                        //lgprintf("创建文件,编号:%d...",dtfid);

                    } // end of IF blockstart>2000000000)
                    blockmt.Reset();
                    blockrn=0;
                    firstrn=thisrn;
                    startrow=blockrn;
                    rownum=0;
                    dispct++;
                    if(wdbi_kill_in_progress) {
                        wdbi_kill_in_progress=false;
                        ThrowWith("用户取消操作!");
                    }
                    if(dispct-lstdispct>=200) {
                        lstdispct=dispct;
                        arrtm.Stop();
                        double tm1=arrtm.GetTime();
                        arrtm.Start();
                        printf("  已生成%lld数据块(%ld),用时%.0f秒,预计还需要%.0f秒          .\r",dispct,i,tm1,(tm1/(double)i*(idxrn-i)));
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
            //保存子快索引
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
            //保存块数据
            //保存块数据
            //IB改造 写入数据
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
            //增加文件对照表记录(dt_datafilemap)
            {
                void *fnmtptr[20];
                //IB改造 写入数据
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
            //IB 改造 关闭文件
            //IB改造 写入数据
            //if(ib_engine) {
            //fclose(ibfile);
            //if(!trimtofile) unlink(ibfilename);
            //}
            blockrn=0;
            startrow=blockrn;
            rownum=0;
        }

        //记录数校验。这里的校验仅是本次数据重组，有可能是一个数据组的某个索引组的一部分。后面的校验是挑一个索引组，校验整个数据组
        if(adjrn!=lmtextrn) {
            sp->log(tabid,datapartid,MLOAD_CHECK_RESULT_ERROR,"表%d,分区%d,数据重组要求处理导出数据%lld行，但实际生成%lld行! 索引组%d.",tabid,datapartid,lmtextrn,adjrn,indexid);
            ThrowWith("数据重组要求处理导出数据%lld行，但实际生成%lld行! 表%d(%d),索引组%d.",lmtextrn,adjrn,tabid,datapartid,indexid);
        }
        wociAppendToDbTable(fnmt,"dp.dp_datafilemap",sp->GetDTS(),true);
        sp->log(tabid,datapartid,MLOAD_DATA_NOTIFY,"修改中间文件的处理状态(表%d,索引%d,数据组:%d,%d个文件)：2-->3",tabid,indexid,datapartid,rn);
        lgprintf("修改中间文件的处理状态(表%d,索引%d,数据组:%d,%d个文件)：2-->3",tabid,indexid,datapartid,rn);
        //抽取过程会把2改回1，因此先改为20
        //JIRA DMA-25
        sprintf(sqlbf,"update dp.dp_middledatafile set procstate=3 where tabid=%d and datapartid=%d and indexgid=%d and procstate=2 and mdfid>=%d and mdfid<=%d ",
                tabid,datapartid,indexid,start_mdfid,end_mdfid);
        sp->DoQuery(sqlbf);
        sp->log(tabid,datapartid,MLOAD_DATA_NOTIFY,"表%d,分区%d,数据重组,索引组:%d,记录数%lld.",tabid,datapartid,indexid,lmtextrn);
    } catch (...) {
        //IB 改造 关闭文件
        //TODO：缺少失败回退机制
        //IB改造 写入数据
        int frn=wociGetMemtableRows(fnmt);
        errprintf("数据整理出现异常，表:%d,数据组:%d.",tabid,datapartid);
        sp->log(tabid,datapartid,MLOAD_EXCEPTION_ERROR,"表%d,分区%d,数据整理出现异常",tabid,datapartid);
        errprintf("恢复中间文件的处理状态(数据组:%d,%d个文件)：2-->1",datapartid,rn);
        sprintf(sqlbf,"update dp.dp_middledatafile set procstate=1,blevel=ifnull(blevel,0)+1 where tabid=%d and datapartid=%d and indexgid=%d and mdfid>=%d and mdfid<=%d",tabid,datapartid,indexid,start_mdfid,end_mdfid);
        sp->DoQuery(sqlbf);
        sprintf(sqlbf,"update dp.dp_datapart set blevel=ifnull(blevel,0)+100 where tabid=%d and datapartid=%d ",tabid,datapartid);
        sp->DoQuery(sqlbf);
        errprintf("删除已整理的数据和索引文件.");
        errprintf("删除数据文件...");
        int i;
        for(i=0; i<frn; i++) {
            errprintf("\t %s ",fnmt.PtrStr("filename",i));
            errprintf("\t %s ",fnmt.PtrStr("idxfname",i));
        }
        for(i=0; i<frn; i++) {
            unlink(fnmt.PtrStr("filename",i));
            unlink(fnmt.PtrStr("idxfname",i));
        }
        errprintf("删除已处理数据文件和索引文件记录...");
        AutoStmt st(sp->GetDTS());
        for(i=0; i<frn; i++) {
            st.Prepare("delete from dp.dp_datafilemap where tabid=%d and datapartid=%d and indexgid=%d and fileflag=1 and fileid=%d",tabid,datapartid,indexid,fnmt.PtrInt("fileid",i));
            st.Execute(1);
            st.Wait();
        }
        wociCommit(sp->GetDTS());
        sp->logext(tabid,datapartid,EXT_STATUS_ERROR,"");
        sp->log(tabid,datapartid,MLOAD_DATA_NOTIFY,"表%d,分区%d,整理过程中错误，状态已恢复.",tabid,datapartid);
        throw ;
    }

    lgprintf("数据处理(MiddleDataLoading)结束,共处理数据包%lld个.",dispct);
    lgprintf("生成%d个数据文件,已插入dp.dp_datafilemap表.",wociGetMemtableRows(fnmt));
    sp->log(tabid,datapartid,MLOAD_DATA_NOTIFY,"表%d,分区%d,数据重组结束,共处理数据包%lld个,生成文件%d个.",tabid,datapartid,dispct,wociGetMemtableRows(fnmt));
    //wociMTPrint(fnmt,0,NULL);
    //检查是否该数据分组的最后一批数据
    // 如果是最后一批数据，则说明：
    //  1. 拆分处理的最后一个批次数据已处理完。
    //  2. 所有该分组对应的一个或多个独立索引都已整理完成。
    //在这个情况下才删除导出的临时数据。
    try {
        // TODO : JIRA DMA-37 并行整理和装入不能恢复错误
        //检查其他独立索引或同一个分组中其它批次的数据（其他数据子集）是否整理完毕。
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
                //Jira DMA-145 ,在整理内存不足以一次完成全部导出数据的整理时,有一部分数据可能已经被装入的过程把状态修改为11/12,因此数据校验需要增加11/12
                mdf.FetchAll("select sum(recordnum) rn from dp.dp_datafilemap where tabid=%d and datapartid=%d and indexgid=%d and fileflag=1 and procstatus in(0,1,2)",
                             tabid,datapartid,indexid);
                mdf.Wait();
                //数据校验
                if(trn!=mdf.GetLong("rn",0)) {
                    //未知原因引起的错误，不能直接恢复状态. 暂停任务的执行
                    sp->log(tabid,datapartid,MLOAD_CHECK_RESULT_ERROR,"表%d,分区%d,数据校验错误,导出%lld行，整理生成%lld行(验证索引组%d),数据迁移过程已被暂停。",tabid,datapartid,trn,mdf.GetLong("rn",0),indexid);
                    sprintf(sqlbf,"update dp.dp_datapart set oldstatus=status,status=70,blevel=ifnull(blevel,0)+100 where tabid=%d and datapartid=%d",tabid,datapartid);
                    sp->DoQuery(sqlbf);
                    ThrowWith("数据校验错误,表%d(%d),导出%lld行，整理生成%lld行(验证索引组%d),数据迁移过程已被暂停",tabid,datapartid,trn,mdf.GetLong("rn",0),indexid);
                }
                if(mdf.GetLong("rn",0))
                    lgprintf("最后一批数据已处理完,任务状态2-->%d,表%d(%d)",ib_engine?3:3,tabid,datapartid);
                //如果是单分区处理任务，必须是所有相同数据集的任务状态为3，才能启动下一步的操作（数据装入）。
                //IB 改造，整理后结束任务，不再需要装入过程
                //修改 DMA 的 整理和装入分开 并行执行
                sprintf(sqlbf,"update dp.dp_datapart set status=%d where tabid=%d and datapartid=%d",
                        /*ib_engine?5:*/3,
                        tabid,datapartid);
                sp->DoQuery(sqlbf);
                //重新创建表结构。
                //sp->CreateDT(tabid);
                //}
                //2011/07/03 西安临时修改 暂不删除临时文件

                lgprintf("删除中间临时文件...");
                mdf.FetchAll("select * from dp.dp_middledatafile where tabid=%d and datapartid=%d and procstate=3",tabid,datapartid);
                int dfn=mdf.Wait();
                {
                    for(int di=0; di<dfn; di++) {
                        lgprintf("删除文件'%s'",mdf.PtrStr("datafilename",di));
                        unlink(mdf.PtrStr("datafilename",di));
                        lgprintf("删除文件'%s'",mdf.PtrStr("indexfilename",di));
                        unlink(mdf.PtrStr("indexfilename",di));
                    }
                    lgprintf("删除记录...");
                    AutoStmt st(sp->GetDTS());
                    st.Prepare("delete from dp.dp_middledatafile where tabid=%d and datapartid=%d and procstate=3",tabid,datapartid);
                    st.Execute(1);
                    st.Wait();
                }
            }
        }
    } catch(...) {
        sp->logext(tabid,datapartid,EXT_STATUS_ERROR,"");
        errprintf("数据整理任务已完成，但任务状态调整或临时中间文件删除时出现错误，需要人工调整。\n表%d(%d)。",tabid,datapartid);
        sp->log(tabid,datapartid,MLOAD_EXCEPTION_ERROR,"数据整理任务已完成，但任务状态调整或临时中间文件删除时出现错误，需要人工调整。\n表%d(%d)。",tabid,datapartid);
        throw;
    }
    return 1;
    //Load index data into memory table (indexmt)
}


//>> begin: fix dma-739
const int conFileNameLen=256;
typedef struct StruLoadFileItem {
    int     fileid;    // 装入的文件id
    char    filename[conFileNameLen];   // 装入的文件名称
    long    recordnum;  // 记录条数
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
    int     filenum;    // 小于conMaxLoadFileNum
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
    lgprintf("数据提交...");
    // must commit!!
    st.ExecuteNC("commit");
    lgprintf("提交成功.");
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
    IBDataFile *pibdatafile=(IBDataFile *) tl->GetParams()[0];      // 数据文件转换类对象
    const char *pipefilename=(const char*)tl->GetParams()[1];       // 管道文件句柄
    // open with 'wb' options!!
    FILE *ibfile=fopen(pipefilename,"wb");                          // 打开管道文件
    if(ibfile==NULL)
        ThrowWith("Open pipe file '%' failed.",pipefilename);

    FILE *keep_file=(FILE *)tl->GetParams()[2];
    StruLoadFileArrayPtr pLoadDataAry = (StruLoadFileArrayPtr)tl->GetParams()[3];       // 文件列表名称

    /* exception catched in caller*/
    try {
        bool    has_error = false;
        LONG64 deal_rows_number = 0;                            // 共处理的记录行数
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
        tl->GetParams()[4]=(void *)deal_rows_number;        // 已经处理的文件记录行数
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
    lgprintf("数据传入结束.");
}

//>> begin: 获取表空间大小,add by liujs
LONG64 GetSizeFromFile(const char* file)
{
    LONG64 tosize = 0;
    FILE *pf = fopen(file,"rt");
    if(pf==NULL) ThrowWith("文件打开文件 %s 失败.",file);
    char lines[300]= {0};
    while(fgets(lines,300,pf)!=NULL) {
        //du lineorder.bht -sb
        //106083154       lineorder.bht
        char *p = lines;
        while(*p !=' ' && *p !='\t' && *p!='\n') {
            p++;
        }
        *p = 0; // 空格部分阶段

        tosize += atoll(lines);
    }
    fclose(pf);
    pf=NULL;
    return tosize;
}
// 获取$DATAMERGER_HOME/var/database/table.bht/Table.ctb 中的最后4字节
int GetTableId(const char* pbasepth,const char *dbn,const char *tabname)
{
    char strTablectl[500];
    sprintf(strTablectl,"%s/%s/%s.bht/Table.ctb",pbasepth,dbn,tabname);
    struct stat stat_info;
    if(0 != stat(strTablectl, &stat_info)) {
        ThrowWith("数据库表%s.%s 不存在!",dbn,tabname);
    }

    // 打开表id文件
    FILE  *pFile  = fopen(strTablectl,"rb");
    if(!pFile) {
        ThrowWith("文件%s打开失败!",strTablectl);
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
           lgprintf("因数据装入过程较慢，临时解决方法，屏蔽转入过程统计装入后空间大小!");
          return 1;        
    }
    LONG64 totalbytes = 0 ;

    char basepth[300];
    strcpy(basepth,psa->GetMySQLPathName(0,"msys"));
    char size_file[300];
    sprintf(size_file,"%s/dpadmin_%s.%s_sz_%d.lst",basepth,dbn,tbn,getpid());
    unlink(size_file);

    // 判断文件表是否存在
    char table_name[256];
    sprintf(table_name,"%s/%s/%s.frm",basepth,dbn,tbn);
    struct stat stat_info;
    if(0 != stat(table_name, &stat_info)) {
        lgprintf("数据库表%s.%s不存在,无法获取大小\n",dbn,tbn);
        return 0;
    }

    try {
        char cmd[500];

        // 1>  获取表$DATAMERGER_HOME/var/db/tb.bht 大小, du $DATAMERGER_HOME/var/db/tb.bht -sb
        sprintf(cmd,"du %s/%s/%s.bht -sb >> %s",basepth,dbn,tbn,size_file);
        system(cmd);
        if(echo) {
            lgprintf("cmd : %s \n",cmd);
        }
        // 2>   获取表$DATAMERGER_HOME/var/db/tb.frm 大小, du $DATAMERGER_HOME/var/db/tb.frm -sb
        sprintf(cmd,"du %s/%s/%s.frm -sb >> %s",basepth,dbn,tbn,size_file);
        system(cmd);
        if(echo) {
            lgprintf("cmd : %s \n",cmd);
        }

        //  3>     获取$DATAMERGER_HOME/var/BH_RSI_Repository/????.tabid.*.rsi 大小, du $DATAMERGER_HOME/var/BH_RSI_Repository/????.tabid.*.rsi -sb

        // 表中存在数据才统计rsi信息
        AutoMt mdf(psa->GetDTS(),10);
        mdf.FetchFirst("select count(1) cnt from %s.%s",dbn,tbn);
        long cnt = 0;
        mdf.Wait();
        cnt = mdf.GetDouble("cnt",0);
        if(cnt > 0) { // 表中存在数据
            int tabid = GetTableId(basepth,dbn,tbn);
            sprintf(cmd,"du %s/BH_RSI_Repository/????.%d.*.rsi -sb >> %s",basepth,tabid,size_file);
            system(cmd);
            if(echo) {
                lgprintf("cmd : %s \n",cmd);
            }

            totalbytes += GetSizeFromFile(size_file);
            unlink(size_file);

            //>> begin: fix sasu-105
            //  4>   如果是分布式版本，获取存储在分布式版本中的数据包大小
            //  export LOAD_SORTED_DATA_DIR='riak://IB_DATAPACKS@cloud01:8087,cloud02:8087,cloud02:8087'
            const char* pRiakHost = getenv(RIAK_HOSTNAME_ENV_NAME);
            if(NULL != pRiakHost && strlen(pRiakHost) > 0 ) {
                char spfn[256];
                sprintf(spfn,"%s/%s/%s.bht/PackSize_%d.ctl",basepth,dbn,tbn,tabid);

                if(echo) {
                    lgprintf("cmd : get pack size from file %s .\n",spfn);
                }

                // 判断文件是否存在
                struct stat stat_info;
                if(0 != stat(spfn, &stat_info)) {
                    lgprintf("file:%s do not exist.请确认是否没有正确设置环境变量:RIAK_HOSTNAME\n",spfn);
                    return 0;
                }

                int filesize = stat_info.st_size;
                // 文件存在，获取文件中数据包的大小
                unsigned long spsize_total = 0;
                FILE* pFile = NULL;
                pFile = fopen(spfn,"rb");
                assert(pFile);
                bool read_error = false;

                // read
                if(filesize == 8) { // 之前装入的数据，只有8字节记录大小
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

//>> end: 获取表空间大小,add by liujs

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


// 在表分区上线后，将其dp.dp_filelog表中的记录进行move到备份表dp.dp_filelogbk中，并将源表中删除;
int DestLoader::MoveFilelogInfo(const int tabid,const int datapartid)
{
    // 不需要补文件的流程才能进行将dp.dp_filelog--->dp.dp_filelogbk进行迁移
    char * p_need_not_add_file = getenv("DP_NEED_NOT_ADD_FILE");
    if(p_need_not_add_file == NULL) {
        lgprintf("DP_NEED_NOT_ADD_FILE 环境变量未设置,需要补文件的流程不能进行将dp.dp_filelog--->dp.dp_filelogbk进行迁移");
        return 0;
    } else if(atoi(p_need_not_add_file) != 1) {
        lgprintf("DP_NEED_NOT_ADD_FILE 环境变量未设置,需要补文件的流程不能进行将dp.dp_filelog--->dp.dp_filelogbk进行迁移");
        return 0;
    }

    try {
        // 1. 插入已经完成的分区的记录
        {
            AutoStmt st(psa->GetDTS());
            st.Prepare(" insert into dp.dp_filelogbk select * from dp.dp_filelog where tabid = %d and datapartid = %d and tabid in (select tabid from dp.dp_datapart where tabid=%d and datapartid = %d and status = 5)",tabid,datapartid,tabid,datapartid);
            st.Execute(1);
            st.Wait();
            lgprintf("表(%d),分区(%d)的入库文件已经从dp.dp_filelog --> dp.dp_filelogbk 中: insert 成功.",tabid,datapartid);
        }
        // 2. 删除已经insert的记录
        {
            AutoStmt st(psa->GetDTS());
            st.Prepare(" delete from dp.dp_filelog where tabid = %d and datapartid = %d and tabid in (select tabid from dp.dp_datapart where tabid=%d and datapartid = %d and status = 5)",tabid,datapartid,tabid,datapartid);
            st.Execute(1);
            st.Wait();
            lgprintf("表(%d),分区(%d)的入库文件已经从dp.dp_filelog --> dp.dp_filelogbk 中: delete 成功.",tabid,datapartid);
        }
    } catch(...) {
        lgprintf("表(%d),分区(%d)的入库文件从dp.dp_filelog --> dp.dp_filelogbk 中失败.",tabid,datapartid);
        return -1;
    }
    return 0;
}


// 装入一组文件后，将记录数和占用空间大小信息，更新到dp.dp_table表中
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

        lgprintf("更新表[%s.%s,tabid=%d]记录数和占用空间大小到dp.dp_table中.",dbname,tabname,tabid);

        mdf.FetchFirst("select count(1) srn from %s.%s",dbname,tabname); // 查询数据表中的记录
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
        lgprintf("更新表[tabid=%d]记录数和占用空间大小到dp.dp_table中.",tabid);
        return -1;
    }
    return 1;
}

// 提交长/短会话id的排序完成数据
int DestLoader::commit_sorted_data(const int sessiontype)
{
    if(!dp_parallel_load) return 0;

    // 提交会话排序处理数据,处理流程如下:
    // 1. 从dp.dp_datapartext表中,获取满足条件的长会话任务/短会话任务信息
    // 2. 获取获取会话ID,分区名称,合并数据目录等信息,写入loadinfo.ctl文件中
    // 3. 开始准备数据合并过程(记录更新表dp.dp_datapartext中的commitstatus=1等信息
    // 4.    短会话 向mysqld提交: load data infile 'st_mergetable' into table db.tb 中
    //->4.  长会话 向mysqld提交: load data infile 'lt_mergetable' into table db.tb 中
    //5. 提交完成后,返回修改任务状态(dp.dp_datapart.status)和会话处理状态(dp.dp_datapartext.commitstatus)
    //6. 更新合并后的表的空间大小

    int ret =0;
    AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
    char quesql[4096];
    if(sessiontype == SHORT_SESSION_TYPE) {
        // 从dp.dp_datapartext表中,获取任务状态为commitstatus=0 && sessiontype=1 的任务信息
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
        // 从dp.dp_datapartext表中,获取任务状态为(commitstatus=0 && sessiontype=2)
        // 且(sessiontype=1 && commitstatus not in(-1,0,1)) 的任务信息
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
        lgprintf("DestLoader::commit_sorted_data 提交会话类型错误,提交会话类型[%d].",sessiontype);
        assert(0);
        return 0;
    }

    mdf.FetchAll(quesql);

    int rn=mdf.Wait();

    if(rn>0) {
        // var 目录获取
        char basepth[300];
        strcpy(basepth,psa->GetMySQLPathName(0,"msys"));

        // 1. 从dp.dp_datapart表中,获取任务状态为3的任务信息(会话ID不为null)
        int tabid,datapartid;
        char sessionid[32],partfullname[100];
        tabid = mdf.GetInt("tabid",0);
        datapartid = mdf.GetInt("datapartid",0);
        strcpy(sessionid,mdf.PtrStr("sessionid",0));
        strcpy(partfullname,mdf.PtrStr("partfullname",0));
        long rownums = mdf.GetLong("rownums",0);

        // fix dma-1781:装入日志目录输出
        psa->SetTrace("dataload",tabid);

        // 2. 获取获取会话ID,分区名称,合并数据目录等信息,写入loadinfo.ctl文件中
        mdf.FetchAll("select databasename,tabname from dp.dp_table where tabid=%d",tabid);
        mdf.Wait();
        char tabpath[300],dbname[200],tabname[200];
        strcpy(dbname,mdf.PtrStr("databasename",0));
        strcpy(tabname,mdf.PtrStr("tabname",0));
        sprintf(tabpath,"%s/%s.bht",dbname,tabname);

        char *pload_data_dir = getenv("DMA_LOAD_SORTED_DATA_DIR");
        if(pload_data_dir == NULL) {
            ThrowWith("环境变量DMA_LOAD_SORTED_DATA_DIR没有设置,无法进行数据合并.");
        }

        // 会话id获取
        mdf.FetchFirst("select connection_id() cid");
        mdf.Wait();
        int connectid=(int)mdf.GetLong("cid",0);

        // 分区文件名称
        lgprintf("开始合并装入数据,tabid(%d),datapartid(%d),分区(%s),会话(S%s),连接号:%d,记录数:%ld.",
                 tabid,datapartid,partfullname,sessionid,connectid,rownums);

        // 开始准备数据合并过程(记录更新表dp.dp_datapartext中的commitstatus等信息,将表进行锁住
        AutoStmt st(psa->GetDTS());
        ret = st.DirectExecute("update dp.dp_datapartext set commitstatus=%d where tabid=%d and datapartid=%d and  sessionid = '%s'",
                               PL_LOADING,tabid,datapartid,sessionid);

        // 再次判断*.loading文件是否存在,存在就退出
        if(ret == 1) {
            char loadingfn[256];
            sprintf(loadingfn,"%s/%s/%s.loading",basepth,dbname,tabname);
            struct stat stat_info;
            if(0 == stat(loadingfn, &stat_info)) { // exist

                //st.DirectExecute("update dp.dp_datapartext set commitstatus=%d where tabid=%d and datapartid=%d and  sessionid = '%s'",PL_LOAD_WAIT,tabid,datapartid,sessionid);
                lgprintf("数据合并过程,tabid(%d),datapartid(%d),分区(%s),会话(S%s) 发现[%s]文件,退出数据合并过程.",
                         tabid,datapartid,partfullname,sessionid,loadingfn);

                return 0;
            }

            // 更新时间
            st.DirectExecute("update dp.dp_datapartext set commitbegintime=now() where tabid=%d and datapartid=%d and  sessionid = '%s'",
                             tabid,datapartid,sessionid);


        } else {
            lgprintf("数据合并过程,tabid(%d),datapartid(%d),分区(%s),会话(S%s) 合并数据更新任务冲突,退出.",
                     tabid,datapartid,partfullname,sessionid);
            return 0;
        }


        if(rownums > 0 ) {

            // 每一个文件都作为最后一个文件装入数据，默认值为1，0:A+B=A流程
            bool islastload = false;
            char *ploadaslastfile = getenv("DP_LOAD_AS_LAST_FILE");
            if(ploadaslastfile == NULL || 1 == atoi(ploadaslastfile)) {
                islastload = true;
            }
            lgprintf("生成装入合并文件:%s/%s/loadinfo.ctl",basepth,tabpath);
            psa->UpdateLoadPartition(connectid,tabpath,islastload,partfullname,sessionid,pload_data_dir,rownums);

            // 4. 向mysqld提交: load data infile 'mergetable' into table db.tb 中
            try {
                char loadsql[1024];
                if(sessiontype == SHORT_SESSION_TYPE) {
                    sprintf(loadsql,"load data infile 'st_mergetable' into table %s.%s",dbname,tabname);
                } else {
                    sprintf(loadsql,"load data infile 'lt_mergetable' into table %s.%s",dbname,tabname);
                }
                st.ExecuteNC("set @bh_dataformat='binary'");
                st.ExecuteNC("set autocommit=0");
                lgprintf("数据合并过程[%s]...",loadsql);
                st.ExecuteNC(loadsql);
                lgprintf("数据合并提交[commit --> %s]...",loadsql);
                st.ExecuteNC("commit");
                lgprintf("数据提交成功.");
            } catch(...) {
                st.DirectExecute("update dp.dp_datapartext set commitstatus=%d "
                                 " where tabid=%d and datapartid=%d and commitstatus=%d and sessionid = '%s'",
                                 PL_LOAD_WAIT,tabid,datapartid,PL_LOADING,sessionid);
                lgprintf("数据合并提交失败,已恢复回滚数据提交合并任务状态.");
                return 0;
            }

            //5. 提交完成后,返回修改任务状态(dp.dp_datapart.status)和会话处理状态(dp.dp_datapartext.commitstatus)
            st.DirectExecute("update dp.dp_datapartext set commitstatus=%d,commitendtime=now()"
                             " where tabid=%d and datapartid=%d and commitstatus=%d and sessionid = '%s'",
                             PL_LOADED,tabid,datapartid,PL_LOADING,sessionid);

            //6. 更新表大小
            UpdateTableSizeRecordnum(tabid);

            //7. 删除已经合并完成的临时文件,暂时注释
            const char* p_DP_PARALLEL_KEEP_LOAD_FILE = getenv("DP_PARALLEL_KEEP_LOAD_FILE");
            if(p_DP_PARALLEL_KEEP_LOAD_FILE == NULL || atoi(p_DP_PARALLEL_KEEP_LOAD_FILE) == 0) {
                clear_has_merged_data(sessionid,pload_data_dir);
            }

            lgprintf("数据合并装入tabid(%d),datapartid(%d),分区(%s),会话(S%s),(%ld)行数据.",
                     tabid,datapartid,partfullname,sessionid,rownums);

        } else {

            // 3. 开始准备数据合并过程(记录更新表dp.dp_datapartext中的commitstatus等信息,将表进行锁住
            AutoStmt st(psa->GetDTS());
            st.DirectExecute("update dp.dp_datapartext set commitstatus=%d,commitbegintime=now(),commitendtime=now() where tabid=%d and datapartid=%d and  sessionid = '%s'",
                             PL_LOADED,tabid,datapartid,sessionid);

            lgprintf("数据合并装入tabid(%d),datapartid(%d),分区(%s),会话(S%s),(%ld)行数据.",
                     tabid,datapartid,partfullname,sessionid,rownums);

            //7. 删除已经合并完成的临时文件,暂时注释
            const char* p_DP_PARALLEL_KEEP_LOAD_FILE = getenv("DP_PARALLEL_KEEP_LOAD_FILE");
            if(p_DP_PARALLEL_KEEP_LOAD_FILE == NULL || atoi(p_DP_PARALLEL_KEEP_LOAD_FILE) == 0) {
                clear_has_merged_data(sessionid,pload_data_dir);
            }

        }

        // 更新任务状态
        update_datapart_finish(tabid,datapartid);

        return 1;
    } else {
        if(sessiontype == SHORT_SESSION_TYPE) {
            lgprintf("没有找到需要提交合并的短会话数据的记录.");
        } else if(sessiontype == LONG_SESSION_TYPE) {
            lgprintf("没有找到需要提交合并的长会话数据的记录.");
        }
        return 0;
    }
}

// 数据合并完成后,更新任务状态为已经完成
int DestLoader::update_datapart_finish(const int tabid,const int datapartid)
{
    int ret = 0;
    AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
    mdf.FetchFirst("select tabid,datapartid,sessionid from dp.dp_datapartext where tabid=%d and datapartid = %d "
                   " and commitstatus in(%d,%d,%d) ",
                   tabid,datapartid,PL_LOAD_DEFAULT,PL_LOAD_WAIT,PL_LOADING);
    ret = mdf.Wait();
    if(ret > 0) {
        lgprintf("tabid(%d),datapartid(%d) 还有(%d)个会话没有合并数据完成,继续等待合并数据.",tabid,datapartid,ret);
        return ret;
    }

    AutoStmt st(psa->GetDTS());
    lgprintf("更新tabid(%d),datapartid(%d)任务为已完成(status = 5).",datapartid,datapartid);
    st.DirectExecute("update dp.dp_datapart set status=5 "
                     " where tabid=%d and datapartid=%d and status = %d",
                     tabid,datapartid,LOADING);
    return 0;
}


int DestLoader::commit_load_data()
{
    int ret = 0;
    if(!dp_parallel_load) return ret;

    // 提交短会话数据
    do {
        ret = commit_sorted_data(SHORT_SESSION_TYPE);
    } while(ret >0);

    // 提交长会话数据
    do {
        ret = commit_sorted_data(LONG_SESSION_TYPE);
    } while(ret > 0 );

    return 0;
}

// 清除已经合并过后的文件
int DestLoader::clear_has_merged_data(const char* sessionid,const char* mergepath)
{
    try {
        int ret = 0;
        char cmd[1024];

        // 删除dpn
        sprintf(cmd,"rm %s/S%s-*.dpn -rf",mergepath,sessionid);
        lgprintf("删除数据过程:%s\n",cmd);
        ret = system(cmd);
        if(ret == -1) {
            ThrowWith("删除数据过程:[%s]错误.\n",cmd);
        }
        sleep(1);

        // 删除rsi
        sprintf(cmd,"rm %s/S%s-*.rsi -rf",mergepath,sessionid);
        lgprintf("删除数据过程:%s\n",cmd);
        ret = system(cmd);
        if(ret == -1) {
            ThrowWith("删除数据过程:[%s]错误.\n",cmd);
        }
        sleep(1);
        // 删除pack
        sprintf(cmd,"rm %s/S%s-*.pck -rf",mergepath,sessionid);
        lgprintf("删除数据过程:%s\n",cmd);
        ret = system(cmd);
        if(ret == -1) {
            ThrowWith("删除数据过程:[%s]错误.\n",cmd);
        }
        sleep(1);
        // 删除leveldb文件,不是所有列都有的
        sprintf(cmd,"rm %s/S%s-*.pki -rf",mergepath,sessionid);
        lgprintf("删除数据过程:%s\n",cmd);
        ret = system(cmd);
        if(ret == -1) {
            ThrowWith("删除合并完成数据过程:[%s]错误.\n",cmd);
        }
        sleep(1);

        // 删除leveldb文件,不是所有列都有的
        sprintf(cmd,"rm %s/S%s-*.pkiv -rf",mergepath,sessionid);
        lgprintf("删除数据过程:%s\n",cmd);
        ret = system(cmd);
        if(ret == -1) {
            ThrowWith("删除合并完成数据过程:[%s]错误.\n",cmd);
        }
        sleep(1);

        // riak 版本删除keyspace
        bool b_riak_version = CheckRiakVersion();
        if(b_riak_version) {
            sprintf(cmd,"rm %s/S%s-*.ksi -rf",mergepath,sessionid);
            lgprintf("删除数据过程:%s\n",cmd);
            ret = system(cmd);
            if(ret == -1) {
                ThrowWith("删除合并完成数据过程:[%s]错误.\n",cmd);
            }
            sleep(1);
        }

        lgprintf("删除数据完成.\n");

        return ret;
    } catch(...) {
        return 1;
    }
}

// 清除表分区的临时数据
int DestLoader::clear_table_tmp_data(const int tabid,const int datapartid)
{
    AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);

    int retrows = 0;
    char sessionid[256];
    int session_nums = 0;

    char *pload_data_dir = getenv("DMA_LOAD_SORTED_DATA_DIR");
    if(pload_data_dir == NULL) {
        ThrowWith("环境变量DMA_LOAD_SORTED_DATA_DIR没有设置,无法进行数据清除过程.");
    }

    AutoStmt st(psa->GetDTS());
    do {
        if(datapartid>0) {
            mdf.FetchFirst("select sessionid from dp.dp_datapartext where tabid = %d and datapartid = %d limit 100",
                           tabid,datapartid);
        } else { // 整表删除
            mdf.FetchFirst("select sessionid from dp.dp_datapartext where tabid = %d limit 100",
                           tabid);
        }

        retrows = mdf.Wait();
        for(int i=0; i<retrows; i++) {
            strcpy(sessionid,mdf.PtrStr("sessionid",i));
            lgprintf("准备清除:tabid(%d),datapartid(%d),sessionid('%s')的数据.",tabid,datapartid,sessionid);
            clear_has_merged_data(sessionid,pload_data_dir);
            lgprintf("清除完成:tabid(%d),datapartid(%d),sessionid('%s')的数据.",tabid,datapartid,sessionid);
        }
        session_nums += retrows;

        // 清除记录
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

    lgprintf("已经清除完成:tabid(%d),datapartid(%d)会话数(%d)的数据.",tabid,datapartid,session_nums);

    return session_nums;
}



// 清除该分区上的已经排序的会话数据
int DestLoader::clear_has_merged_data(const int tabid,const int datapartid)
{
    AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);

    int retrows = 0;
    char sessionid[256];
    int session_nums = 0;

    char *pload_data_dir = getenv("DMA_LOAD_SORTED_DATA_DIR");
    if(pload_data_dir == NULL) {
        ThrowWith("环境变量DMA_LOAD_SORTED_DATA_DIR没有设置,无法进行数据清除过程.");
    }

    AutoStmt st(psa->GetDTS());
    do {
        if(datapartid>0) {
            mdf.FetchFirst("select sessionid from dp.dp_datapartext where tabid = %d and datapartid = %d and commitstatus = 2 limit 100",
                           tabid,datapartid);
        } else { // 整表删除
            mdf.FetchFirst("select sessionid from dp.dp_datapartext where tabid = %d and commitstatus = 2 limit 100",
                           tabid);
        }

        retrows = mdf.Wait();
        for(int i=0; i<retrows; i++) {
            strcpy(sessionid,mdf.PtrStr("sessionid",i));
            lgprintf("准备清除:tabid(%d),datapartid(%d),sessionid('%s')的合并完成数据.",tabid,datapartid,sessionid);
            clear_has_merged_data(sessionid,pload_data_dir);
            lgprintf("清除完成:tabid(%d),datapartid(%d),sessionid('%s')的合并完成数据.",tabid,datapartid,sessionid);
        }
        session_nums += retrows;

        // 清除记录
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

    lgprintf("已经清除完成:tabid(%d),datapartid(%d)会话数(%d)的数据.",tabid,datapartid,session_nums);

    return session_nums;
}


int DestLoader::Load(bool directIOSkip)
{
    if(dp_parallel_load) { //并行数据装入
        return commit_load_data();
    } else {
        const char* pRiakHost = getenv(RIAK_HOSTNAME_ENV_NAME_CLI);
        if(NULL != pRiakHost && pRiakHost[0] != '\0') {
            ThrowWith("DMC分布式排序版本(export RIAK_HOSTNAME=%s):不支持数据bhloader装入过程,数据装入过程由分布式排序集群完成.",pRiakHost);
        }
    }

    // 正常的bhloader数据装入流程
    {
        AutoStmt st(psa->GetDTS());
        st.DirectExecute(" update dp.dp_datafilemap set procstatus=0 where procstatus=10");
    }
    // fix error : set reload status on web admin tool cause delete file and force set status=5
    AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
    //Check deserved temporary(middle) fileset
    //优先处理已经装入完成，可以清理的任务！
    //mdf.FetchAll("select * from dp.dp_datapart where status=3 and begintime<now() %s order by blevel,tabid,datapartid limit 2",psa->GetNormalTaskDesc());
    mdf.FetchAll("select * from dp_datapart a where status=3 and not exists (select 1 from dp_datafilemap b where "
                 "a.tabid=b.tabid and a.datapartid=b.datapartid and b.procstatus<2) and begintime<now() %s order by blevel,tabid,datapartid limit 2",
                 psa->GetNormalTaskDesc());
    int rn=mdf.Wait();
    if(rn>0) {
        //printf("没有发现处理完成等待装入的数据(任务状态=3).\n");
        //return 0;

        char sqlbf[MAX_STMT_LEN];
        tabid=mdf.GetInt("tabid",0);
        psa->SetTrace("dataload",tabid);
        datapartid=mdf.GetInt("datapartid",0);
        bool preponl=mdf.GetInt("status",0)==3;
        //该分区所有的独立索引数据全部装入完成?
        mdf.FetchAll("select * from dp.dp_datafilemap where procstatus in(0,1) and tabid=%d and datapartid=%d limit 2",tabid,datapartid);
        rn=mdf.Wait();
        if(rn<1) {
            if(preponl) {
                lgprintf("表(%d)分区(%d)的数据装入完成。",tabid,datapartid);

                // 更新dp.dp_table表的空间大小和记录数
                UpdateTableSizeRecordnum(tabid);

                // TODO : store raw data size and compress ratio to dp_table.
                AutoStmt st(psa->GetDTS());
                st.DirectExecute(" update dp.dp_datapart set status=5 where tabid=%d and datapartid=%d",tabid,datapartid);
                mdf.FetchAll("select filename from dp.dp_datafilemap where procstatus =2 and tabid=%d and datapartid=%d ",tabid,datapartid);
                rn=mdf.Wait();
#ifndef KEEP_LOAD_FILE
                for(int i=0; i<rn; i++) {
                    lgprintf("删除已装入的文件'%s'.",mdf.PtrStr("filename",i));
                    unlink(mdf.PtrStr("filename",i));
                }
#endif
                st.DirectExecute(" update dp.dp_datafilemap set procstatus=3 where tabid=%d and datapartid=%d",tabid,datapartid);
                lgprintf("表(%d)分区(%d)的任务状态3-5",tabid,datapartid);
                psa->log(tabid,datapartid,DLOAD_UPDATE_TASK_STATUS_NOTIFY,"表(%d)分区(%d)的任务状态3-5",tabid,datapartid);
                psa->logext(tabid,datapartid,EXT_STATUS_END,"");

                // 如果是文件采集,上线完成后将dp.dp_filelog--> dp.dp_filelogbk中
                // 不需要补文件的流程才能进行将dp.dp_filelog--->dp.dp_filelogbk进行迁移
                char * p_need_not_add_file = getenv("DP_NEED_NOT_ADD_FILE");
                if(p_need_not_add_file && atoi(p_need_not_add_file) == 1) {
                    mdf.FetchAll("select tabid from dp.dp_datapart where tabid = %d and datapartid = %d and status = 5 and extsql like '%%'",tabid,datapartid);
                    int _rn = mdf.Wait();
                    if(_rn >0 ) {
                        MoveFilelogInfo(tabid,datapartid);
                    }
                } else {
                    lgprintf("DP_NEED_NOT_ADD_FILE 环境变量未设置,需要补文件的流程不能进行将dp.dp_filelog--->dp.dp_filelogbk进行迁移");
                }
            }
            return 1;
        }
    }
    //查找等待导入的文件
    // 条件：
    //  1. 任务状态是数据整理或者等待导入
    //  2. 文件状态是等待导入
    //  3. 一个表中同一个独立索引没有其它数据在导入
    //2013/4/19 增加:
    //  4. 任务状态时正在导出
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
            break; // 不同的索引，不能装入
        }

        // 逐个设置文件id和名称
        loadfilearray.stLoadFileLst[loadfilearray.filenum].fileid = mdf.GetInt("fileid",i);
        strcpy(loadfilearray.stLoadFileLst[loadfilearray.filenum].filename,mdf.PtrStr("filename",i));
        loadfilearray.stLoadFileLst[loadfilearray.filenum].recordnum = mdf.GetInt("recordnum",i);
        loadfilearray.filenum ++ ;

        filesizesum += mdf.GetInt("filesize",i);
        if((filesizesum/1024/1024) > loadfsize) {
            break; // 文件大小满足条件退出
        }
    }
    //<<end fix : dma-739

    //Jira DMA-145: 导入进程的tabid在上面的select语句被重新修改,需要再次调整日志的文件名称:
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
            psa->log(tabid,datapartid,DLOAD_DATA_NOTIFY,"文件id'%s'装入时不能锁定状态，可能其它进程在装入。如果没有其它进程装入，请恢复文件状态为0.",fileidlist);
            lgprintf("文件'%s'装入时不能锁定状态，可能其它进程在装入。如果没有其它进程装入，请恢复文件状态为0.",fileidlist);
            return 0;
        }
    }

    //开始装入过程
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

    // 每一个文件都作为最后一个文件装入数据，默认值为1，0:A+B=A流程
    char *ploadaslastfile = getenv("DP_LOAD_AS_LAST_FILE");
    if(ploadaslastfile == NULL || 1 == atoi(ploadaslastfile)) {
        islastload = true;
    }

    mdf.FetchAll("select connection_id() cid");
    rn=mdf.Wait();
    int connectid=(int)mdf.GetLong("cid",0);
    lgprintf("装入数据,分区:%s,连接号:%d.",partname,connectid);

    psa->UpdateLoadPartition(connectid,tabpath,islastload,partname);

    IBDataFile ibdatafile;

    char cmd[2000];
    char ibfilename[300];

    // 装入管道文件路径
    // 如果整理好的文件是nfs挂载的情况下，需要设置本地目录作为管道文件的装入点
    const char* dpload_dir = getenv("DP_LOAD_DIR");
    if(dpload_dir != NULL && strlen(dpload_dir) >0) { // 指定的路径进行装入数据
        sprintf(ibfilename,"%s/%s.%s.%s.%d_pipe.bin",dpload_dir,dbname,tabname,partname,loadfilearray.stLoadFileLst[0].fileid);
    } else { // 使用默认的路径进行数据装入
        sprintf(ibfilename,"%s_pipe.bin",loadfilearray.stLoadFileLst[0].filename); // 第一个文件名称使用
    }

    sprintf(cmd,"rm -f %s",ibfilename);
    lgprintf("开始数据导入,表%d/分区%d/索引%d,文件列表{%s} 。",tabid,datapartid,indexid,fileidlist);
    try {
        //多个独立索引，表名后加_<indexid>,只有一个独立索引，则表名不变
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
            psa->log(tabid,datapartid,DLOAD_CREATE_PIPE_FILE_ERROR,"表%d,分区%d,创建管道文件%s失败.",tabid,datapartid,ibfilename);
            ThrowWith("文件建立失败：%s",ibfilename);
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
        if(cmdrt==-1) ThrowWith既胧О堋?);
        lgprintf("等待进程就绪");
        sleep(3);
        */
        for(int i=0; i<loadfilearray.filenum; i++) {
            FILE *fptest=fopen(loadfilearray.stLoadFileLst[i].filename,"rb");
            if(fptest==NULL) {
                psa->log(tabid,datapartid,DLOAD_OPEN_DATA_SOURCE_FILE_ERROR,"表%d,分区%d,打开数据文件%s失败.",
                         tabid,datapartid,loadfilearray.stLoadFileLst[i].filename);

                if(i>0) { // 更新存在文件的，文件状态
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

                ThrowWith("数据文件不能打开:'%s'",loadfilearray.stLoadFileLst[i].filename);
            }
            fclose(fptest);
        }

        //启动服务端线程
        ThreadList tload;
        //  TODO: 缺少失败回退机制,检查异常时是否回退已导入的数据
        void *params[2];
        // connect handle
        params[0]=( void *)psa->GetDTS();
        char loadsql[3000];
        sprintf(loadsql,"load data infile '%s' into table %s.%s",ibfilename,mdf.PtrStr("databasename",0),tbname1);
        params[1]=( void *) loadsql;
        lgprintf("开始转换tabid=%d,datapartid=%d,文件列表{%s}->%s",tabid,datapartid,fileidlist,ibfilename);

        void *pipeparams[5];
        pipeparams[0]=(void *)&ibdatafile;  // datafile
        pipeparams[1]=(void *)ibfilename;   // ibfile;
        pipeparams[2]=(void *)keep_file;
        pipeparams[3]=(void *)&loadfilearray;
        pipeparams[4]=(void *)NULL;

        //启动管道数据写入线程
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
                            lgprintf("已导入文件%s,记录数%ld 行.",loadfilearray.stLoadFileLst[i].filename,
                                     loadfilearray.stLoadFileLst[i].recordnum);
                        }
                        if(loadfilearray.filenum>1) {
                            lgprintf("导入%d个文件，导入%ld 行记录.",loadfilearray.filenum,recimp);
                        }
                    }
                } else {
                    lgprintf("数据库导入结束.");
                }
            }
        } catch(...) {
            if(tload.GetExceptedThread()==tpipe) {
                lgprintf("文件写入异常,终止数据库导入...");
                tload.Terminate();
            } else {
                lgprintf("数据库导入异常,终止文件写...");
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

        // 更新dp.dp_table表的空间大小和记录数
        UpdateTableSizeRecordnum(tabid);

        AutoStmt st(psa->GetDTS());
        if(st.DirectExecute(" update dp.dp_datafilemap set procstatus=2,endtime=now() where procstatus=1 and tabid=%d and datapartid=%d and indexgid=%d and fileid in(%s)",
                            tabid,datapartid,indexid,fileidlist)!=loadfilearray.filenum) {
            psa->log(tabid,datapartid,DLOAD_UPDATE_FILE_STATUS_ERROR,"表%d,分区%d,文件列表{%s}状态更新错误,需要重新导入该分区!",tabid,datapartid,fileidlist);
            ThrowWith("表%d,分区%d,文件列表{%s}状态更新错误！",tabid,datapartid,fileidlist);
        }
    } catch(...) {
        lgprintf("导入表%d,分区%d,文件列表{%s}异常！",tabid,datapartid,fileidlist);;
        psa->log(tabid,datapartid,DLOAD_UPDATE_FILE_STATUS_NOTIFY,"表%d,分区%d,导入文件列表{%s}异常，恢复处理状态.",tabid,datapartid,fileidlist);
        ibdatafile.CloseReader();
        psa->logext(tabid,datapartid,EXT_STATUS_ERROR,"");

        AutoStmt st(psa->GetDTS());
        if(st.DirectExecute(" update dp.dp_datafilemap set procstatus=0,pid = 0 where procstatus=1 and tabid=%d and datapartid=%d and indexgid=%d and fileid in(%s)",
                            tabid,datapartid,indexid,fileidlist)!=loadfilearray.filenum) {
            psa->log(tabid,datapartid,DLOAD_UPDATE_FILE_STATUS_ERROR,"表%d,分区%d,导入文件列表{%s}，状态恢复错误.",tabid,datapartid,fileidlist);
            ThrowWith("表%d,分区%d,导入文件列表{%s}，状态恢复错误！",tabid,datapartid,fileidlist);
        }
        lgprintf("表%d,分区%d,导入文件列表{%s}异常，已恢复处理状态.",tabid,datapartid,fileidlist);

        // 任务优先级调整
        if(1 != st.DirectExecute(" update dp.dp_datapart set blevel=blevel+100 where tabid = %d and datapartid =%d",tabid,datapartid)) {
            psa->log(tabid,datapartid,DLOAD_UPDATE_TASK_STATUS_ERROR,"表%d,分区%d,修改任务优先级错误.",tabid,datapartid);
            ThrowWith("表%d,分区%d,修改任务优先级错误.！",tabid,datapartid);
        }
        lgprintf("表%d,分区%d,调整任务优先级成功.",tabid,datapartid);

#ifndef KEEP_LOAD_FILE
        unlink(ibfilename);
#endif
        throw;
    }
    return 1;

}

// 源表为DBPLUS管理的目标表，且记录数非空。
int DestLoader::MoveTable(const char *srcdbn,const char *srctabname,const char * dstdbn,const char *dsttabname)
{
    char dtpath[300];
    lgprintf("目标表改名(转移) '%s.%s -> '%s.%s'.",srcdbn,srctabname,dstdbn,dsttabname);
    sprintf(dtpath,"%s.%s",srcdbn,srctabname);
    if(!psa->TouchTable(dtpath))
        ThrowWith("源表没找到");
    sprintf(dtpath,"%s.%s",dstdbn,dsttabname);
    if(psa->TouchTable(dtpath)) {
        lgprintf(dtpath,"表%s.%s已存在,不能执行更名操作!",dstdbn,dsttabname);
        //      if(!GetYesNo(dtpath,false)) {
        lgprintf("取消操作。 ");
        return 0;
        //      }
    }
    AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);
    int rn;
    //mt.FetchAll("select pathval from dp.dp_path where pathtype='msys'");
    //int rn=mt.Wait();
    //i/f(rn<1)
    //  ThrowWith("找不到MySQL数据目录(dt_path.pathtype='msys'),数据转移异常中止.");
    strcpy(dtpath,psa->GetMySQLPathName(0,"msys"));
    if(STRICMP(srcdbn,dstdbn)==0 && STRICMP(srctabname,dsttabname)==0)
        ThrowWith("源表和目标表名称不能相同.");
    mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",dsttabname,dstdbn);
    rn=mt.Wait();
    if(rn>0) {
        ThrowWith("表'%s.%s'已存在(记录数:%d)，操作失败!",dstdbn,dsttabname,mt.GetInt("recordnum",0));
    }
    mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",srctabname,srcdbn);
    rn=mt.Wait();
    if(rn<1) {
        ThrowWith("源表'%s.%s'不存在.",srcdbn,srctabname);
    }
    mt.FetchAll("select * from dp.dp_datapart where tabid=%d and status not in(0,5)",mt.GetInt("tabid",0));
    rn=mt.Wait();
    if(rn>0) {
        ThrowWith("源表'%s.%s'迁移过程未完成，不能更名.",srcdbn,srctabname);
    }
    mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",srctabname,srcdbn);
    rn=mt.Wait();
    lgprintf("配置参数转移.");
    int dsttabid=psa->NextTableID();
    tabid=mt.GetInt("tabid",0);
    int recordnum=mt.GetInt("recordnum",0);
    int firstdatafileid=mt.GetInt("firstdatafileid",0);
    double totalbytes=mt.GetDouble("totalbytes",0);
    int datafilenum=mt.GetInt("datafilenum",0);
    if(recordnum<1) {
        lgprintf("源表'%s.%s'数据为空，表更名失败。",srcdbn,srctabname);
        return 0;
    }
    lgprintf("源表'%s.%s' id:%d,记录数:%d,起始数据文件号 :%d",
             srcdbn,srctabname,tabid,recordnum,firstdatafileid);

    //新表不存在，在dt_table中新建一条记录
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
    //暂时关闭源表的数据访问，记录数已存在本地变量recordnum。
    //目标表的.DTP文件已经存在,暂时屏蔽访问
    psa->CloseTable(dsttabid,NULL,false);
    char sqlbuf[MAX_STMT_LEN];
    mt.FetchAll("select * from dp.dp_datapart where tabid=%d order by datapartid ",tabid);
    rn=mt.Wait();
    //创建索引记录和索引表，修改索引文件和数据文件的tabid 指向
    int i=0;
    for(i=0; i<rn; i++)
        *mt.PtrInt("tabid",i)=dsttabid;
    wociAppendToDbTable(mt,"dp.dp_datapart",psa->GetDTS(),true);

    mt.FetchAll("select * from dp.dp_index where tabid=%d order by seqindattab ",tabid);
    rn=mt.Wait();
    //创建索引记录和索引表，修改索引文件和数据文件的tabid 指向

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

    lgprintf("索引转移.");
    for(i=0; i<rn; i++) {
        if(mt.GetInt("issoledindex",i)>0) {
            char tbn1[300],tbn2[300];
            //如果有老的索引表表存在,则不再处理分区索引表.
            psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tbn1,TBNAME_DEST);
            if(psa->TouchTable(tbn1)) {
                psa->GetTableName(dsttabid,mt.GetInt("indexgid",i),NULL,tbn2,TBNAME_DEST);
                lgprintf("索引表 '%s'-->'%s...'",tbn1,tbn2);
                psa->FlushTables(tbn1);
                MoveMySQLTable(dtpath,srcdbn,strstr(tbn1,".")+1,dstdbn,strstr(tbn2,".")+1);
            } else
                ThrowWith("找不到索引表'%s',程序异常终止,需要手工检查并修复参数!",tbn1);
            int ilp=0;
            psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tbn1,TBNAME_DEST,ilp);
            if(psa->TouchTable(tbn1)) {
                //3.10版新格式索引.
                char srcf[300];
                //修正MERGE文件
                //
                sprintf(srcf,"%s%s/%s.MRG",dtpath,dstdbn,strstr(tbn2,".")+1);
                FILE *fp=fopen(srcf,"w+t");
                while(psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tbn1,TBNAME_DEST,ilp)) {
                    psa->GetTableName(dsttabid,mt.GetInt("indexgid",i),NULL,tbn2,TBNAME_DEST,ilp++);
                    lgprintf("索引表 '%s'-->'%s...'",tbn1,tbn2);
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
    lgprintf("数据文件转移.");

    for(i=0; i<rn; i++) {
        char fn[300];
        psa->GetMySQLPathName(mt.GetInt("pathid",i));
        sprintf(fn,"%s%s.%s_%d_%d_%d.dat",psa->GetMySQLPathName(mt.GetInt("pathid",i)),
                dstdbn,dsttabname,mt.GetInt("datapartid",i),mt.GetInt("indexgid",i),mt.GetInt("fileid",i));

        FILE *fp;
        fp=fopen(mt.PtrStr("filename",i),"rb");
        if(fp==NULL) ThrowWith("找不到文件'%s'.",mt.PtrStr("filename",i));
        fclose(fp);
        fp=fopen(fn,"rb");
        if(fp!=NULL) ThrowWith("文件'%s'已经存在.",fn);
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
    psa->log(dsttabid,0,118,"数据从%s.%s表转移而来.",srcdbn,srctabname);
    //建立文件链接
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

    //Move操作结束,打开目标表
    lgprintf("MySQL刷新...");
    char tbn[300];
    sprintf(tbn,"%s.%s",dstdbn,dsttabname);
    psa->BuildDTP(tbn);
    psa->FlushTables(tbn);
    lgprintf("删除源表..");
    RemoveTable(srcdbn,srctabname,false);
    lgprintf("数据已从表'%s'转移到'%s'。",srctabname,dsttabname);
    return 1;
}


// 7,10的任务状态处理:
//   1. 从文件系统获取二次压缩后的文件大小.
//   2. 任务状态修改为(8,11) (?? 可以省略)
//   3. 关闭目标表(unlink DTP file,flush table).
//   4. 修改数据/索引映射表中的文件大小和压缩类型.
//   5. 数据/索引 文件替换.
//   6. 任务状态修改为30(等待重新装入).
int DestLoader::ReLoad()
{

    AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
    mdf.FetchAll("select * from dp.dp_datapart where status in (7,10) and begintime<now() %s order by blevel,tabid,datapartid limit 2",psa->GetNormalTaskDesc());
    int rn=mdf.Wait();
    if(rn<1) {
        printf("没有发现重新压缩完成等待装入的数据.\n");
        return 0;
    }
    bool dpcp=mdf.GetInt("status",0)==7;
    int compflag=mdf.GetInt("compflag",0);
    tabid=mdf.GetInt("tabid",0);
    psa->SetTrace("reload",tabid);
    mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) order by indexgid",tabid);
    rn=mdf.Wait();
    if(rn<1) {
        lgprintf("装入二次压缩数据时找不到数据文件记录,按空表处理。");
        char sqlbf[MAX_STMT_LEN];
        sprintf(sqlbf,"update dp.dp_datapart set status=30 where tabid=%d", tabid);
        if(psa->DoQuery(sqlbf)<1) {
            psa->log(tabid,0,DLOAD_UPDATE_TASK_STATUS_ERROR,"二次压缩数据重新装入过程修改任务状态异常，可能是与其它进程冲突(tabid:%d)。\n",tabid);
            ThrowWith("二次压缩数据重新装入过程修改任务状态异常，可能是与其它进程冲突(tabid:%d)。\n",tabid);
        }
        return 0;
    }
    mdf.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
    rn=mdf.Wait();
    if(rn<1) {
        lgprintf("装入二次压缩数据时找不到dp.dp_table记录(tabid:%d).",tabid);
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
    //先检查
    for(k=0; k<datrn; k++) {
        sprintf(tmpfn,"%s.%s",datmt.PtrStr("filename",k),dpcp?"depcp":"dep5");
        dt_file df;
        df.Open(tmpfn,0);
        dtflen[k]=df.GetFileSize();
        if(dtflen[k]<1) {
            psa->log(tabid,0,DLOAD_CHECK_FIEL_ERROR,"表%d,数据文件%s为空.",tabid,tmpfn);
            ThrowWith("file '%s' is empty!",tmpfn);
        }
    }
    for(k=0; k<rn; k++) {
        sprintf(tmpfn,"%s.%s",datmt.PtrStr("idxfname",k),dpcp?"depcp":"dep5");
        dt_file df;
        df.Open(tmpfn,0);
        idxflen[k]=df.GetFileSize();
        if(idxflen[k]<1) {
            psa->log(tabid,0,DLOAD_CHECK_FIEL_ERROR,"表%d,索引文件%s为空.",tabid,tmpfn);
            ThrowWith("file '%s' is empty!",tmpfn);
        }
    }
    char sqlbf[MAX_STMT_LEN];
    sprintf(sqlbf,"update dp.dp_datapart set status=%d where tabid=%d", dpcp?8:11,tabid);
    if(psa->DoQuery(sqlbf)<1) {
        psa->log(tabid,0,DLOAD_UPDATE_TASK_STATUS_ERROR,"二次压缩数据重新装入过程修改任务状态异常，可能是与其它进程冲突(tabid:%d)。\n",tabid);
        ThrowWith("二次压缩数据重新装入过程修改任务状态异常，可能是与其它进程冲突(tabid:%d)。\n",tabid);
    }
    //防止功能重入，修改任务状态
    //后续修改将涉及数据文件的替换,操作数据前先关闭表
    // 这个关闭操作将锁住表的访问，直至上线成功。
    //  TODO  文件保留到上线时刻最为理想
    psa->CloseTable(tabid,NULL,false,true);
    lgprintf("数据已关闭.");
    //用新的数据文件替换原来的文件：先删除原文件，新文件名称更改为原文件并修改文件记录中的文件大小字段。
    lgprintf("开始数据和索引文件替换...");
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
    lgprintf("数据和索引文件已成功替换...");
    sprintf(sqlbf,"update dp.dp_datapart set status=30,istimelimit=0 where tabid=%d", tabid);
    psa->DoQuery(sqlbf);
    sprintf(sqlbf,"update dp.dp_datafilemap set procstatus=0 where tabid=%d and fileflag=0",tabid);
    psa->DoQuery(sqlbf);
    lgprintf("任务状态修改为数据整理结束(3),数据文件处理状态改为未处理(10).");
    Load();
    return 1;
}


int DestLoader::RecreateIndex(SysAdmin *_Psa)
{
    AutoMt mdf(psa->GetDTS(),MAX_MIDDLE_FILE_NUM);
    AutoMt mdf1(psa->GetDTS(),MAX_MIDDLE_FILE_NUM);
    char sqlbf[MAX_STMT_LEN];
    //检查需要上线的数据
    // 使用子查询，可以使逻辑更简单
    //   select * from dp.dp_datapart a where status=21 and istimelimit!=22 and begintime<now() %s and tabid not in (
    //select tabid from dp_datapart b where b.tabid=a.tabid and b.status!=21 and b.status!=5 and b.begintime<now())
    //order by blevel,tabid,datapartid

    mdf1.FetchAll("select * from dp.dp_datapart where status=21 and begintime<now() %s order by blevel,tabid,datapartid",psa->GetNormalTaskDesc());
    if(mdf1.Wait()>0) {
        int mrn=mdf1.GetRows();
        for(int i=0; i<mrn; i++) {
            //做上线处理
            tabid=mdf1.GetInt("tabid",i);
            psa->SetTrace("dataload",tabid);
            datapartid=mdf1.GetInt("datapartid",i);
            int newld=mdf1.GetInt("oldstatus",i)==4?1:0;
            int oldstatus=mdf1.GetInt("oldstatus",i);
            if(mdf1.GetInt("istimelimit",i)==22) { //其它进程在处理上线
                mdf.FetchAll("select * from dp.dp_table where tabid=%d ",
                             tabid);
                if(mdf.Wait()>0)
                    lgprintf("表 %s.%s 上线过程在其它进程处理，如果其它进程异常退出，请重新装入",
                             mdf.PtrStr("databasename",0),mdf.PtrStr("tabname",0));
                else
                    lgprintf("tabid为%d的表上线过程在其它进程处理，如果其它进程异常退出，请重新装入",tabid);
                continue;
            }
            mdf.FetchAll("select * from dp.dp_datapart where tabid=%d and status!=21 and status!=5 and begintime<now()",tabid);
            if(mdf.Wait()<1) {
                //从源表(格式表)构建目标表结构,避免分区数据抽取为空的情况
                char tbname[150],idxname[150];
                psa->GetTableName(tabid,-1,tbname,idxname,TBNAME_PREPONL);
                sprintf(sqlbf,"update dp.dp_datapart set istimelimit=22 where tabid=%d and status =21 and begintime<now()",tabid);
                if(psa->DoQuery(sqlbf)<1)  {
                    lgprintf("表%d上线时修改任务状态异常，可能是与其它进程冲突。\n",tabid);
                    continue;
                }
                try {
                    AutoMt destmt(0,10);
                    //如果找不到数据文件,则从源表创建表结构
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

                    lgprintf("删除中间临时文件...");
                    mdf.FetchAll("select * from dp.dp_middledatafile where tabid=%d",tabid);

                    int dfn=mdf.Wait();
                    for(int di=0; di<dfn; di++) {
                        lgprintf("删除文件'%s'",mdf.PtrStr("datafilename",di));
                        unlink(mdf.PtrStr("datafilename",di));
                        lgprintf("删除文件'%s'",mdf.PtrStr("indexfilename",di));
                        unlink(mdf.PtrStr("indexfilename",di));
                    }
                    lgprintf("删除记录...");
                    st.Prepare("delete from dp.dp_middledatafile where tabid=%d",tabid);
                    st.Execute(1);
                    st.Wait();
                    psa->CleanData(false,tabid);
                    psa->logext(tabid,datapartid,EXT_STATUS_END,"");
                    return 1;
                } catch(...) {
                    errprintf("表%d 上线时出现异常错误,恢复处理状态...",tabid);
                    psa->log(tabid,0,DLOAD_ONLINE_EXCEPTION_ERROR,"表%d,上线时出现异常错误.",tabid);
                    //
                    //sprintf(sqlbf,"update dp.dp_datapart set status=21,istimelimit=0,blevel=if(ifnull(blevel,0)>=100,blevel,ifnull(blevel,0)+100),oldstatus=%d where tabid=%d and istimelimit=22", oldstatus,tabid);
                    sprintf(sqlbf,"update dp.dp_datapart set istimelimit=0,blevel=if(ifnull(blevel,0)>=100,blevel,ifnull(blevel,0)+100),oldstatus=%d where tabid=%d and istimelimit=22", oldstatus,tabid);
                    //sprintf(sqlbf,"update dp.dp_datapart set status=21 where tabid=%d and istimelimit=22", oldstatus,tabid);
                    psa->DoQuery(sqlbf);
                    psa->log(tabid,0,DLOAD_UPDATE_TASK_STATUS_NOTIFY,"表%d,上线时出现异常错误,已恢复处理状态.",tabid);
                    throw;
                }
            }
            //else {
            //  AutoStmt st(psa->GetDTS());
            //  st.DirectExecute("update dp.dp_datapart set blevel=if(ifnull(blevel,0)>=100,blevel,ifnull(blevel,0)+100) where tabid=%d and datapartid=%d and ifnull(blevel,0)<100",tabid,datapartid);
            //}
        }
    }

    // 在装载完数据的索引表上，修复索引，并修改任务状态为21
    mdf.FetchAll("select * from dp.dp_datapart where (status =4 or status=40 ) %s order by blevel,tabid,datapartid limit 2",psa->GetNormalTaskDesc());
    int rn=mdf.Wait();
    if(rn<1) {
        return 0;
    }
    datapartid=mdf.GetInt("datapartid",0);
    tabid=mdf.GetInt("tabid",0);
    psa->SetTrace("dataload",tabid);
    bool preponl=mdf.GetInt("status",0)==4;
    //if(tabid<1) ThrowWith("找不到任务号:%d中Tabid",taskid);

    //check intergrity.
    mdf.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
    rn=mdf.Wait();
    if(rn<1) {
        psa->log(tabid,0,DLOAD_DST_TABLE_MISS_INDEX,"目标表%d缺少主索引记录.",tabid);
        ThrowWith("目标表%d缺少主索引记录.",tabid);
    }

    mdf.FetchAll("select distinct indexgid from dp.dp_datafilemap where tabid=%d and fileflag=%d",tabid,preponl?1:0);
    rn=mdf.Wait();
    mdf.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
    int rni=mdf.Wait();
    if(rni!=rn) {
        lgprintf("出现错误: 重建索引时，数据文件中的独立索引数(%d)和索引参数表中的值(%d)不符,tabid:%d,datapartid:%d.",rn,rni,tabid,datapartid);
        return 0; //dump && destload(create temporary index table) have not complete.
    }
    try {
        sprintf(sqlbf,"update dp.dp_datapart set oldstatus=status,status=%d where tabid=%d and datapartid=%d and (status =4 or status=40 )",20,tabid,datapartid);
        if(psa->DoQuery(sqlbf)<1) {
            psa->log(tabid,0,DLOAD_DST_TABLE_CREATE_INDEX_ERROR,"表%d,数据装入重建索引过程修改任务状态异常，可能是与其它进程冲突。",tabid);
            ThrowWith("数据装入重建索引过程修改任务状态异常，可能是与其它进程冲突。\n"
                      "  tabid:%d.\n",
                      tabid);
        }
        lgprintf("开始索引重建,tabid:%d,总索引数 :%d",tabid,rn);
        psa->log(tabid,0,DLOAD_CREATE_INDEX_NOTIFY,"开始建立索引.");
        //2005/12/01 索引改为数据新增后重建(修复)。
        lgprintf("建立索引的过程可能需要较长的时间，请耐心等待...");
        psa->RepairAllIndex(tabid,TBNAME_PREPONL,datapartid);
        lgprintf("索引建立完成.");
        psa->log(tabid,0,DLOAD_CREATE_INDEX_NOTIFY,"索引建立完成.");
        AutoStmt st(psa->GetDTS());
        st.DirectExecute("update dp.dp_datapart set status=21 where tabid=%d and datapartid=%d",
                         tabid,datapartid);
        mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and isfirstindex=1 and fileflag!=2 order by datapartid,indexgid,fileid",
                     tabid);
        rn=mdf.Wait();
    } catch (...) {
        errprintf("建立索引结构时出现异常错误,恢复处理状态...");
        sprintf(sqlbf,"update dp.dp_datapart set status=%d,blevel=if(ifnull(blevel,0)>=100,blevel,ifnull(blevel,0)+100) where tabid=%d and datapartid=%d", preponl?3:30,tabid,datapartid);
        psa->DoQuery(sqlbf);
        sprintf(sqlbf,"update dp.dp_datafilemap set procstatus=0 where tabid=%d and "
                "procstatus=1 and fileflag=%d and datapartid=%d",tabid,preponl?1:0,datapartid);
        psa->DoQuery(sqlbf);
        psa->log(tabid,0,DLOAD_CREATE_INDEX_NOTIFY,"建立索引结构时出现异常错误,已恢复处理状态.");
        throw;
    }
    return 1;
}

thread_rt LaunchWork(void *ptr)
{
    ((worker *) ptr)->work();
    thread_end;
}

//以下代码有错误
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
                lgprintf("表%d--二次压缩任务已完成，任务状态已修改为%d,数据文件处理状态修改为空闲(0)",tabid,deepcmp?7:10);
                return 1;
            } else lgprintf("表%d(%d)---二次压缩任务未完成,但已没有等待压缩的数据",tabid,datapartid);
        } else break;
    }
    if(i==rn1) return 0;

    //防止上一次mdt中的数据被其它进程中上面的代码改过dp_datapart status
    mdt.FetchAll("select tabid,datapartid,status,compflag from dp.dp_datapart where (status=6 or status=9) and begintime<now() and tabid=%d and datapartid=%d",
                 mdf.GetInt("tabid",0),mdf.GetInt("datapartid",0));
    if(mdt.Wait()<1) {
        lgprintf("要二次压缩处理的数据文件,对应任务状态已改变,取消处理.\n"
                 " tabid:%d,datapartid:%d,fileid:%d.",mdf.GetInt("tabid",0),mdf.GetInt("datapartid",0),mdf.GetInt("fileid",0));
        return 0;
    }
    psa->OutTaskDesc("数据重新压缩任务(tabid %d datapartid %d)",mdf.GetInt("tabid",0),mdf.GetInt("datapartid",0));
    int compflag=mdt.GetInt("compflag",0);
    lgprintf("原压缩类型:%d, 新的压缩类型:%d .",mdf.GetInt("compflag",0),compflag);
    int fid=mdf.GetInt("fileid",0);
    psa->log(tabid,0,121,"二次压缩，类型： %d-->%d ,文件号%d,日志文件 '%s' .",mdf.GetInt("compflag",0),compflag,fid,wociGetLogFile());

    char srcfn[300];
    strcpy(srcfn,mdf.PtrStr("filename",0));
    int origsize=mdf.GetInt("filesize",0);
    char dstfn[300];
    sprintf(dstfn,"%s.%s",srcfn,deepcmp?"depcp":"dep5");
    tabid=mdf.GetInt("tabid",0);
    mdf.FetchAll("select filename,idxfname from dp.dp_datafilemap where tabid=%d and fileid=%d and fileflag!=2",
                 tabid,fid);
    if(mdf.Wait()<1)
        ThrowWith(" 找不到数据文件记录,dp_datafilemap中的记录已损坏,请检查.\n"
                  " 对应的数据文件为:'%s',文件编号: '%d'",srcfn,fid);
    char idxdstfn[300];
    sprintf(idxdstfn,"%s.%s",mdf.PtrStr("idxfname",0),deepcmp?"depcp":"dep5");
    double dstfilelen=0;
    try {
        //防止重入，修改数据文件状态。
        AutoStmt st(psa->GetDTS());
        st.Prepare("update dp.dp_datafilemap set procstatus=1 where tabid=%d and fileid=%d and procstatus=10 and fileflag!=2",
                   tabid,fid);
        st.Execute(1);
        st.Wait();
        if(wociGetFetchedRows(st)!=1) {
            lgprintf("处理文件压缩时状态异常,tabid:%d,fid:%d,可能与其它进程冲突！"
                     ,tabid,fid);
            return 0;
        }
        file_mt idxf;
        lgprintf("数据处理，数据文件:'%s',字节数:%d,索引文件:'%s'.",srcfn,origsize,mdf.PtrStr("idxfname",0));
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
            ThrowWith("索引文件读取错误: '%s'",mdf.PtrStr("filename",0));
        AutoMt *pidxmt=(AutoMt *)idxf;
        //lgprintf("从索引文件读入%d条记录.",wociGetMemtableRows(*pidxmt));
        int *pblockstart=pidxmt->PtrInt("blockstart",0);
        int *pblocksize=pidxmt->PtrInt("blocksize",0);
        blockcompress bc(compflag);
        for(i=1; i<threadnum; i++) {
            bc.AddWorker(new blockcompress(compflag));
        }
        lgprintf("启用线程数:%d.",threadnum);
#define BFNUM 32
        char *srcbf=new char[SRCBUFLEN];//?恳淮未淼淖畲笫菘椋ń庋顾鹾螅
        char *dstbf=new char[DSTBUFLEN*BFNUM];//可累积的最多数据(压缩后).
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
        while(!isalldone) {//文件处理完退出
            if(srcf.ReadMt(-1,0,mdf,1,1,srcbf,false,true)<0) {
                iseof=true;
            }
            if(wdbi_kill_in_progress) {
                wdbi_kill_in_progress=false;
                ThrowWith("用户取消操作!");
            }
            block_hdr *pbh=(block_hdr *)srcbf;
            int doff=srcf.GetDataOffset(pbh);
            if(pbh->origlen+doff>SRCBUFLEN)
                ThrowWith("Decompress data exceed buffer length. dec:%d,bufl:%d",
                          pbh->origlen+sizeof(block_hdr),SRCBUFLEN);
            bool deliverd=false;
            while(!deliverd) { //任务交付后退出
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
                        ThrowWith("要压缩的数据:%d,超过缓存上限:%d.",dstlen,dstseplen);
                    //get empty buf:
                    for(i=0; i<BFNUM; i++) if(!isfilled[i]) break;
                    if(i==BFNUM) ThrowWith("严重错误：压缩缓冲区已满，无法继续!.");
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
                                    //ThrowWith("索引文件读取错误: '%s'",mdf.PtrStr("filename",0));
                                    pidxmt=(AutoMt *)idxf;
                                    //lgprintf("从索引文件读入%d条记录.",wociGetMemtableRows(*pidxmt));
                                    pblockstart=pidxmt->PtrInt("blockstart",0);
                                    pblocksize=pidxmt->PtrInt("blocksize",0);
                                } else break;
                            } else if(lastrow>idxrn1)
                                ThrowWith("索引文件读取错误: '%s'",mdf.PtrStr("filename",0));

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
                            printf("已处理%d个数据块(%d%%),%.2f(MB/s) 用时%.0f秒--预计还需要%.0f秒.\r",nextid,slastrow*100/idxrn,lastoffset/tm1/1024/1024,tm1,tm1/slastrow*(idxrn-slastrow));
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
            ThrowWith("异常错误：并非所有数据都被处理，已处理%d,应处理%d.",lastrow,wociGetMemtableRows(*pidxmt));
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
        errprintf("数据二次压缩出现异常，文件处理状态恢复...");
        AutoStmt st(psa->GetDTS());
        st.DirectExecute("update dp.dp_datafilemap set procstatus=0, blevel=ifnull(blevel,0)+1 where tabid=%d and fileid=%d and procstatus=1 and fileflag=0",
                         tabid,fid);
        st.DirectExecute("update dp.dp_datapart set blevel=ifnull(blevel,0)+100 where tabid=%d and datapartid=%d",
                         tabid,datapartid);
        errprintf("删除数据文件和索引文件");
        unlink(dstfn);
        unlink(idxdstfn);
        throw;
    }

    psa->log(tabid,0,122,"二次压缩结束,文件%d，大小%d->%.0f",fid,origsize,dstfilelen);
    lgprintf("文件转换结束,目标文件:'%s',文件长度(字节):%.0f.",dstfn,dstfilelen);
    return 1;
}


int DestLoader::ToMySQLBlock(const char *dbn, const char *tabname)
{
    lgprintf("格式转换 '%s.%s' ...",dbn,tabname);
    AutoMt mt(psa->GetDTS(),100);
    mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",tabname,dbn);
    int rn=mt.Wait();
    if(rn<1) {
        printf("表'%s'不存在!",tabname);
        return 0;
    }
    tabid=mt.GetInt("tabid",0);
    int recordnum=mt.GetInt("recordnum",0);
    int firstdatafileid=mt.GetInt("firstdatafileid",0);
    if(recordnum<1) {
        lgprintf("源表'%s'数据为空.",tabname);
        return 0;
    }
    AutoMt mdt(psa->GetDTS(),MAX_DST_DATAFILENUM);
    AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
    psa->SetTrace("transblock",tabid);
    mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus =0 and (fileflag=0 or fileflag is null) order by datapartid,indexgid,fileid",tabid);
    rn=mdf.Wait();
    //防止重入，修改数据文件状态。
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
            lgprintf("处理文件转换时状态异常,tabid:%d,fid:%d,可能与其它进程冲突！"
                     ,tabid,fid);
            return 1;
        }
        file_mt idxf;
        lgprintf("数据处理，数据文件:'%s',字节数:%d,索引文件:'%s'.",srcfn,origsize,mdf.PtrStr("filename",0));
        idxf.Open(mdf.PtrStr("filename",0),0);
        if(idxf.ReadBlock(-1,0)<0)
            ThrowWith("索引文件读取错误: '%s'",mdf.PtrStr("filename",0));

        file_mt srcf;
        srcf.Open(srcfn,0,fid);
        dt_file dstf;
        dstf.Open(dstfn,1,fid);
        mdf.SetHandle(srcf.CreateMt());
        int lastoffset=dstf.WriteHeader(mdf,0,fid,srcf.GetNextFileName());

        AutoMt *pidxmt=(AutoMt *)idxf;
        int idxrn=wociGetMemtableRows(*pidxmt);
        lgprintf("从索引文件读入%d条记录.",idxrn);
        int *pblockstart=pidxmt->PtrInt("blockstart",0);
        int *pblocksize=pidxmt->PtrInt("blocksize",0);
        int lastrow=0;
        int oldblockstart=pblockstart[0];
        int dspct=0;
        while(true) {//文件处理完退出
            if(wdbi_kill_in_progress) {
                wdbi_kill_in_progress=false;
                ThrowWith("用户取消操作!");
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
        errprintf("数据转换出现异常，文件处理状态恢复...");
        AutoStmt st(psa->GetDTS());
        st.Prepare("update dp.dp_datafilemap set procstatus=0 where tabid=%d and fileid=%d and procstatus=1 and fileflag=0",
                   tabid,fid);
        st.Execute(1);
        st.Wait();
        errprintf("删除数据文件和索引文件");
        unlink(dstfn);
        unlink(idxdstfn);
        throw;
    }

    lgprintf("文件转换结束,目标文件:'%s',文件长度(字节):%f.",dstfn,dstfilelen);
    return 1;
}



// 删除整表，add by liujs
int DestLoader::RemoveTable(const char *dbn,const char *tabname,bool prompt)
{
    char sqlbuf[MAX_STMT_LEN];
    char choose[200];
    wociSetEcho(FALSE);

    sprintf(choose,"请确保该表'%s.%s'当前未进行数据迁移操作? (Y/N)",dbn,tabname);
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
    if(rn<1) ThrowWith("表%s.%s在dp_table中找不到!",dbn,tabname);

    int tabid=mt.GetInt("tabid",0);

    mt.FetchFirst("select datapartid from dp.dp_datapart where tabid = %d",tabid);
    rn = mt.Wait();
    if(rn <1)ThrowWith("表%s.%s在dp_datapart中找不到!",dbn,tabname);

    mt.FetchAll("select * from dp.dp_index where issoledindex>0 and tabid=%d",tabid);
    int indexrn=mt.Wait();
    if(indexrn<1) ThrowWith("表%s.%s在dp_index中找不到!",dbn,tabname);

    // 删除表操作
    AutoStmt st(psa->GetDTS());

    sprintf(choose,"请确认是否删除%s.%s的整表数据?(Y/N)",dbn,tabname);
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
            // drop table失败，说明表不存在，及任务没有执行过
            lgprintf("数据库表%s.%s不存在,无法删除数据表中的数据!",dbn,tabname);
            sprintf(choose,"请确认是否删除%s.%s的整表的配置管理数据信息?(Y/N)",dbn,tabname);
            forcedel = GetYesNo(choose,false);
            if(forcedel) {
                goto DeleteConfigInfo;
            }
        }
        st.Prepare(" update dp.dp_table set recordnum=0 where tabid=%d",tabid);
        st.Execute(1);
        st.Wait();
        lgprintf("表'%s.%s' 数据库数据已被删除.",dbn,tabname);
    } else {
        lgprintf("表'%s.%s' 数据库数据未被删除.",dbn,tabname);
        return 0;
    }

DeleteConfigInfo:
    //----------------------------------------------------------------
    // 删除中间临时文件信息

    bool parallel_load = CheckParallelLoad();  // 分布式合并数据版本

    sprintf(choose,"是否删除表'%s.%s'的待装入文件(dp.dp_datafilemap)? (Y/N)",dbn,tabname);
    if(prompt) {
        forcedel = GetYesNo(choose,false);
    } else {
        forcedel = true;
    }
    if(forcedel) {
        //----------------------------------------------------------------
        // 删除表分区临时结构文件
        lgprintf("删除装入过程中数据文件.");

        if(!parallel_load) {
            mt.FetchFirst("select * from dp.dp_datafilemap where tabid=%d",tabid);
            rn=mt.Wait();
            int i=0;
            for(i=0; i<rn; i++) {
                char tmp[300];
                lgprintf("删除'%s'和附加的depcp,dep5文件",mt.PtrStr("filename",i));
                unlink(mt.PtrStr("filename",i));
                sprintf(tmp,"%s.depcp",mt.PtrStr("filename",i));
                unlink(tmp);
                sprintf(tmp,"%s.dep5",mt.PtrStr("filename",i));
                unlink(tmp);
                lgprintf("删除'%s'和附加的depcp,dep5文件",mt.PtrStr("idxfname",i));
                unlink(mt.PtrStr("idxfname",i));
                sprintf(tmp,"%s.depcp",mt.PtrStr("idxfname",i));
                unlink(tmp);
                sprintf(tmp,"%s.dep5",mt.PtrStr("idxfname",i));
                unlink(tmp);
            }
            st.Prepare(" delete from dp.dp_datafilemap where tabid=%d ",tabid);
            st.Execute(1);
            st.Wait();

            // 删除整理过中及待整理的问题
            lgprintf("删除采集整理过程中数据文件.");
            mt.FetchFirst("select tabid,datafilename,indexfilename from dp.dp_middledatafile where tabid =%d",
                          tabid);
            rn=mt.Wait();
            for(i=0; i<rn; i++) {
                lgprintf("删除采集完成后的数据文件'%s'",mt.PtrStr("datafilename",i));
                unlink(mt.PtrStr("datafilename",i));

                lgprintf("删除采集完成后的索引文件'%s'",mt.PtrStr("indexfilename",i));
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

    sprintf(choose,"表'%s.%s'的配置参数(任务信息<dp.dp_datapart>,索引信息<dp.dp_index>,扩展字段信息<dp.dp_column_info>,表结构信息<dp.dp_table>)是否删除?(Y/N)",dbn,tabname);
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

    lgprintf("表'%s.%s' 已删除完成.",dbn,tabname);
    return 1;
}

// 删除最后一个分区返回1,删除不是最后一个分区返回2
int DestLoader::RemoveTable_exclude_struct(const char *dbn,const char *tabname,const char *partname)
{
    lgprintf("remove table '%s.%s ' partition '%s' ...",dbn,tabname,partname);
    AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);

    mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",tabname,dbn);
    int rn=mt.Wait();
    if(rn<1) ThrowWith("表%s.%s在dp_table中找不到!",dbn,tabname);

    int tabid=mt.GetInt("tabid",0);

    mt.FetchFirst("select datapartid from dp.dp_datapart where partfullname = lower('%s') and tabid = %d",partname,tabid);
    rn = mt.Wait();
    if(rn <1)ThrowWith("表%s.%s在dp_datapart中找不到!",dbn,tabname);

    int datapartid = mt.GetInt("datapartid",0);

    mt.FetchAll("select count(1) ct from %s.%s",dbn,tabname);
    mt.Wait();
    long origrecs=mt.GetLong("ct", 0);

    mt.FetchAll("select connection_id() cid");
    mt.Wait();
    int connectid=(int)mt.GetLong("cid",0);

    mt.FetchAll("select * from dp.dp_index where issoledindex>0 and tabid=%d",tabid);
    int indexrn=mt.Wait();
    if(indexrn<1) ThrowWith("表%s.%s在dp_index中找不到!",dbn,tabname);
    char targetname[300];
    if(indexrn>1)
        sprintf(targetname,"%s_dma1",tabname);
    else
        strcpy(targetname,tabname);

    AutoStmt st(psa->GetDTS());
    //-----------------------------------------------------------
    // 删除分区信息
    char truncinfo[300];
    sprintf(truncinfo,"%s%s/%s.bht/truncinfo.ctl",psa->GetMySQLPathName(0,"msys"),dbn,targetname);
    FILE *ftruncinfo=fopen(truncinfo,"rb");
    if(ftruncinfo!=NULL) {
        fclose(ftruncinfo);
        lgprintf("上次删除未完成(%s.%s)!",dbn,tabname);
        return 0;
    }
    char partinfo[300];
    sprintf(partinfo,"%s%s/%s.bht/PART_00000.ctl",psa->GetMySQLPathName(0,"msys"),dbn,targetname);
    FILE *fpartinfo=fopen(partinfo,"rb");
    if(fpartinfo==NULL) {
        lgprintf("表未分区,无法删除(%s.%s)!",dbn,tabname);
        return 0;
    }

    // parse partition info.
    char buf[4000];
    bool lastPartition = false;


    // RIAK 和 AP版本的PART_XXXXX.ctl文件格式不一样
    // fileid 长度不一致
    // RIAK:8字节
    // AP:4字节
    bool b_riak_version = CheckRiakVersion();

    fread(buf,1,8,fpartinfo);
    bool oldversion=false;
    if(memcmp(buf,"PARTIF15",8)!=0) {
        if(memcmp(buf,"PARTINFO",8)!=0) {
            lgprintf("分区文件损坏(%s.%s)!",dbn,tabname);
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
            lgprintf("分区文件损坏(%s.%s),分区(%d)!",dbn,tabname,i+1);
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
        lgprintf("找不到要删除的分区(%s)!",partname);
        return 0;
    }
    if(has_find_MAINPART&& partnum > 0) { // fix dma-1296
        partnum --;
    }
    if(partnum<2) {
        //lgprintf("表(%s.%s)仅有最后一个分区,使用表删除(drop table)!",dbn,tabname);
        //return 0;
        lastPartition = true;
    }

    // 区分单表/多表 删除
    if(!lastPartition) { // 不是最后一个分区(存在多个分区的情况)
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
                lgprintf("建立文件'%s'失败!",truncinfo);
                return -1;
            }
            fwrite("TRUNCTRL",1,8,ftruncinfo);
            fwrite(&connectid,sizeof(int),1,ftruncinfo);
            short plen=(short)strlen(partname);
            fwrite(&plen,sizeof(short),1,ftruncinfo);
            fwrite(partname,plen,1,ftruncinfo);
            fclose(ftruncinfo);
            lgprintf("建立索引序列:%d ...",idx+1);
            try {
                AutoStmt st(psa->GetDTS());
                // commit internal
                st.ExecuteNC("truncate table %s.%s",dbn,targetname);
            } catch(...) {
                lgprintf("建立索引序列:%d 失败,请检查服务器日志!",idx+1);
                return -1;
            }
        }
        lgprintf("表'%s.%s' 分区 '%s' 数据库数据已被删除.",dbn,tabname,partname);

        //----------------------------------------------------------------
        // 更新记录条数
        mt.FetchAll("select count(1) ct from %s.%s",dbn,tabname);
        mt.Wait();
        long currentRn=mt.GetLong("ct", 0);

        AutoStmt st(psa->GetDTS());
        st.Prepare(" update dp.dp_table set recordnum=%d where tabid=%d",currentRn,tabid);
        st.Execute(1);
        st.Wait();
    } else { // 最后一个分区
        char sqlbuf[300];
        sprintf(sqlbuf,"drop table %s.%s",dbn,tabname);
        psa->DoQuery(sqlbuf);
        st.Prepare(" update dp.dp_table set recordnum=0 where tabid=%d",tabid);
        st.Execute(1);
        st.Wait();

        lgprintf("表'%s.%s' 分区 '%s' 数据库数据已被删除.",dbn,tabname,partname);
    }

    //----------------------------------------------------------------
    // 删除中间临时文件信息
    bool forcedel = true;
    bool parallel_load = CheckParallelLoad();  // 分布式合并数据版本
    if(forcedel) {
        //----------------------------------------------------------------
        // 删除表分区临时结构文件
        lgprintf("删除装入过程中数据文件.");

        if(!parallel_load) { // 原来AP版本
            mt.FetchFirst("select * from dp.dp_datafilemap where tabid=%d and datapartid=%d",
                          tabid,datapartid);
            rn=mt.Wait();
            int i=0;
            for(i=0; i<rn; i++) {
                char tmp[300];
                lgprintf("删除'%s'和附加的depcp,dep5文件",mt.PtrStr("filename",i));
                unlink(mt.PtrStr("filename",i));
                sprintf(tmp,"%s.depcp",mt.PtrStr("filename",i));
                unlink(tmp);
                sprintf(tmp,"%s.dep5",mt.PtrStr("filename",i));
                unlink(tmp);
                lgprintf("删除'%s'和附加的depcp,dep5文件",mt.PtrStr("idxfname",i));
                unlink(mt.PtrStr("idxfname",i));
                sprintf(tmp,"%s.depcp",mt.PtrStr("idxfname",i));
                unlink(tmp);
                sprintf(tmp,"%s.dep5",mt.PtrStr("idxfname",i));
                unlink(tmp);
            }
            st.Prepare(" delete from dp.dp_datafilemap where tabid=%d and datapartid=%d",tabid,datapartid);
            st.Execute(1);
            st.Wait();

            // 删除整理过中及待整理的问题
            lgprintf("删除采集整理过程中数据文件.");
            mt.FetchFirst("select tabid,datafilename,indexfilename from dp.dp_middledatafile where tabid = %d and datapartid=%d",
                          tabid,datapartid);
            rn=mt.Wait();
            for(i=0; i<rn; i++) {
                lgprintf("删除采集完成后的数据文件'%s'",mt.PtrStr("datafilename",i));
                unlink(mt.PtrStr("datafilename",i));

                lgprintf("删除采集完成后的索引文件'%s'",mt.PtrStr("indexfilename",i));
                unlink(mt.PtrStr("indexfilename",i));
            }
            st.Prepare(" delete from dp.dp_middledatafile where tabid=%d and datapartid=%d",tabid,datapartid);
            st.Execute(1);
            st.Wait();

        } else { // 分布式合并数据版本,删除合并完成后的数据
            clear_table_tmp_data(tabid,datapartid);

            st.Prepare(" delete from dp.dp_datapartext where tabid=%d and datapartid=%d",tabid,datapartid);
            st.Execute(1);
            st.Wait();
        }

    }

    lgprintf("表'%s.%s' 分区 '%s' 已删除完成.",dbn,tabname,partname);

    return lastPartition ? 1:2;

}

// 返回值:
// 1:保留表结构删除表
// 2:保留表结构删除分区
// 11:不保留表结构删除表
// 12:不保留表结构删除分区
int DestLoader::RemoveTable(const int tabid,const int partid,bool exclude_struct)
{

    char dbn[128];
    char tabname[256];

    AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);

    mt.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
    int rn=mt.Wait();
    if(rn<1) ThrowWith("表(%d)在dp_table中找不到!",tabid);

    strcpy(dbn,mt.PtrStr("databasename",0));
    strcpy(tabname,mt.PtrStr("tabname",0));

    mt.FetchFirst("select partfullname from dp.dp_datapart where datapartid = %d and tabid = %d",partid,tabid);
    rn = mt.Wait();
    if(rn <1)ThrowWith("表%s.%s在dp_datapart中找不到!",dbn,tabname);

    // 删除分区
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


// 根据id删除分区
int DestLoader::RemoveTable(const char *dbn,const char *tabname,const int partid,bool prompt)
{
    lgprintf("remove table '%s.%s ' partition id '%d' ...",dbn,tabname,partid);
    AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);

    mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",tabname,dbn);
    int rn=mt.Wait();
    if(rn<1) ThrowWith("表%s.%s在dp_table中找不到!",dbn,tabname);

    int tabid=mt.GetInt("tabid",0);

    mt.FetchFirst("select partfullname from dp.dp_datapart where datapartid = %d and tabid = %d",partid,tabid);
    rn = mt.Wait();
    if(rn <1)ThrowWith("表%s.%s在dp_datapart中找不到!",dbn,tabname);

    // 删除分区
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
        sprintf(choose,"请确保该表'%s.%s'当前未进行数据迁移操作? (Y/N)",dbn,tabname);
        forcedel = GetYesNo(choose,false);
        if(!forcedel) {
            return 0;
        }
    }

    lgprintf("remove table '%s.%s ' partition '%s' ...",dbn,tabname,partname);
    AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);

    mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",tabname,dbn);
    int rn=mt.Wait();
    if(rn<1) ThrowWith("表%s.%s在dp_table中找不到!",dbn,tabname);

    int tabid=mt.GetInt("tabid",0);

    mt.FetchFirst("select datapartid from dp.dp_datapart where partfullname = lower('%s') and tabid = %d",partname,tabid);
    rn = mt.Wait();
    if(rn <1)ThrowWith("表%s.%s在dp_datapart中找不到!",dbn,tabname);

    int datapartid = mt.GetInt("datapartid",0);

    mt.FetchAll("select count(1) ct from %s.%s",dbn,tabname);
    mt.Wait();
    long origrecs=mt.GetLong("ct", 0);

    mt.FetchAll("select connection_id() cid");
    mt.Wait();
    int connectid=(int)mt.GetLong("cid",0);

    mt.FetchAll("select * from dp.dp_index where issoledindex>0 and tabid=%d",tabid);
    int indexrn=mt.Wait();
    if(indexrn<1) ThrowWith("表%s.%s在dp_index中找不到!",dbn,tabname);
    char targetname[300];
    if(indexrn>1)
        sprintf(targetname,"%s_dma1",tabname);
    else
        strcpy(targetname,tabname);


    // RIAK 和 AP版本的PART_XXXXX.ctl文件格式不一样
    // fileid 长度不一致
    // RIAK:8字节
    // AP:4字节
    bool b_riak_version = CheckRiakVersion();


    AutoStmt st(psa->GetDTS());
    //-----------------------------------------------------------
    // 删除分区信息
    char truncinfo[300];
    sprintf(truncinfo,"%s%s/%s.bht/truncinfo.ctl",psa->GetMySQLPathName(0,"msys"),dbn,targetname);
    FILE *ftruncinfo=fopen(truncinfo,"rb");
    if(ftruncinfo!=NULL) {
        fclose(ftruncinfo);
        lgprintf("上次删除未完成(%s.%s)!",dbn,tabname);
        return 0;
    }
    char partinfo[300];
    sprintf(partinfo,"%s%s/%s.bht/PART_00000.ctl",psa->GetMySQLPathName(0,"msys"),dbn,targetname);
    FILE *fpartinfo=fopen(partinfo,"rb");
    if(fpartinfo==NULL) {
        lgprintf("表未分区,无法删除(%s.%s)!",dbn,tabname);
        return 0;
    }

    // parse partition info.
    char buf[1000];
    bool lastPartition = false;

    fread(buf,1,8,fpartinfo);
    bool oldversion=false;
    if(memcmp(buf,"PARTIF15",8)!=0) {
        if(memcmp(buf,"PARTINFO",8)!=0) {
            lgprintf("分区文件损坏(%s.%s)!",dbn,tabname);
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
            lgprintf("分区文件损坏(%s.%s),分区(%d)!",dbn,tabname,i+1);
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
        lgprintf("找不到要删除的分区(%s)!",partname);
        return 0;
    }
    if(has_find_MAINPART&& partnum > 0) { // fix dma-1296
        partnum --;
    }
    if(partnum<2) {
        //lgprintf("表(%s.%s)仅有最后一个分区,使用表删除(drop table)!",dbn,tabname);
        //return 0;
        lastPartition = true;
    }
    if(prompt) {
        sprintf(choose,"表'%s.%s'的分区'%s'将被删除，删除前表中的记录总数:%lld. 确认删除 ?(Y/N)",dbn,tabname,partname,origrecs);
        if(checkdel) {
            if(!GetYesNo(choose,false)) {
                lgprintf("取消删除。 ");
                return 0;
            }
        }
    }
    // 区分单表/多表 删除
    if(!lastPartition) { // 不是最后一个分区(存在多个分区的情况)
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
                lgprintf("建立文件'%s'失败!",truncinfo);
                return -1;
            }
            fwrite("TRUNCTRL",1,8,ftruncinfo);
            fwrite(&connectid,sizeof(int),1,ftruncinfo);
            short plen=(short)strlen(partname);
            fwrite(&plen,sizeof(short),1,ftruncinfo);
            fwrite(partname,plen,1,ftruncinfo);
            fclose(ftruncinfo);
            lgprintf("建立索引序列:%d ...",idx+1);
            try {
                AutoStmt st(psa->GetDTS());
                // commit internal
                st.ExecuteNC("truncate table %s.%s",dbn,targetname);
            } catch(...) {
                lgprintf("建立索引序列:%d 失败,请检查服务器日志!",idx+1);
                return -1;
            }
        }
        lgprintf("表'%s.%s' 分区 '%s' 数据库数据已被删除.",dbn,tabname,partname);

        //----------------------------------------------------------------
        // 更新记录条数
        mt.FetchAll("select count(1) ct from %s.%s",dbn,tabname);
        mt.Wait();
        long currentRn=mt.GetLong("ct", 0);

        AutoStmt st(psa->GetDTS());
        st.Prepare(" update dp.dp_table set recordnum=%d where tabid=%d",currentRn,tabid);
        st.Execute(1);
        st.Wait();
    } else { // 最后一个分区
        char sqlbuf[300];
        sprintf(sqlbuf,"drop table %s.%s",dbn,tabname);
        psa->DoQuery(sqlbuf);
        st.Prepare(" update dp.dp_table set recordnum=0 where tabid=%d",tabid);
        st.Execute(1);
        st.Wait();

        lgprintf("表'%s.%s' 分区 '%s' 数据库数据已被删除.",dbn,tabname,partname);
    }

    //----------------------------------------------------------------
    // 删除中间临时文件信息
    sprintf(choose,"是否删除表'%s.%s'的待装入文件(dp.dp_datafilemap/dp.dp_middledatafile)? (Y/N)",dbn,tabname);

    bool parallel_load = CheckParallelLoad();  // 分布式合并数据版本

    forcedel = true;
    if(checkdel) {
        forcedel = GetYesNo(choose,false);
    }
    if(forcedel) {
        //----------------------------------------------------------------
        // 删除表分区临时结构文件
        lgprintf("删除装入过程中数据文件.");

        if(!parallel_load) {

            mt.FetchFirst("select * from dp.dp_datafilemap where tabid=%d and datapartid=%d",
                          tabid,datapartid);
            rn=mt.Wait();
            int i=0;
            for(i=0; i<rn; i++) {
                char tmp[300];
                lgprintf("删除'%s'和附加的depcp,dep5文件",mt.PtrStr("filename",i));
                unlink(mt.PtrStr("filename",i));
                sprintf(tmp,"%s.depcp",mt.PtrStr("filename",i));
                unlink(tmp);
                sprintf(tmp,"%s.dep5",mt.PtrStr("filename",i));
                unlink(tmp);
                lgprintf("删除'%s'和附加的depcp,dep5文件",mt.PtrStr("idxfname",i));
                unlink(mt.PtrStr("idxfname",i));
                sprintf(tmp,"%s.depcp",mt.PtrStr("idxfname",i));
                unlink(tmp);
                sprintf(tmp,"%s.dep5",mt.PtrStr("idxfname",i));
                unlink(tmp);
            }
            st.Prepare(" delete from dp.dp_datafilemap where tabid=%d and datapartid=%d",tabid,datapartid);
            st.Execute(1);
            st.Wait();

            // 删除整理过中及待整理的问题
            lgprintf("删除采集整理过程中数据文件.");
            mt.FetchFirst("select tabid,datafilename,indexfilename from dp.dp_middledatafile where tabid = %d and datapartid=%d",
                          tabid,datapartid);
            rn=mt.Wait();
            for(i=0; i<rn; i++) {
                lgprintf("删除采集完成后的数据文件'%s'",mt.PtrStr("datafilename",i));
                unlink(mt.PtrStr("datafilename",i));

                lgprintf("删除采集完成后的索引文件'%s'",mt.PtrStr("indexfilename",i));
                unlink(mt.PtrStr("indexfilename",i));
            }
            st.Prepare(" delete from dp.dp_middledatafile where tabid=%d and datapartid=%d",tabid,datapartid);
            st.Execute(1);
            st.Wait();

        } else { // 分布式版本,删除合并数据记录
            clear_has_merged_data(tabid,datapartid);

            st.Prepare(" delete from dp.dp_datapartext where tabid=%d and datapartid=%d",tabid,datapartid);
            st.Execute(1);
            st.Wait();
        }

    }

    sprintf(choose,"表'%s.%s'的配置参数(任务信息<dp.dp_datapart>,索引信息<dp.dp_index>,扩展字段信息<dp.dp_column_info>,表结构信息<dp.dp_table>)是否删除?(Y/N)",dbn,tabname);
    forcedel = true;
    if(checkdel) {
        forcedel=GetYesNo(choose,false);
    }
    if(forcedel) {
        st.Prepare(" delete from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
        st.Execute(1);
        st.Wait();

        // 如果没有其他分区，删除结构信息
        if(lastPartition) { // 没有其他分区，可以删除表结构
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

    lgprintf("表'%s.%s' 分区 '%s' 已删除完成.",dbn,tabname,partname);
    if(lastPartition) {
        lgprintf("表'%s.%s' 已删除完成.",dbn,tabname);
    }
    return lastPartition?1:2;
}


//-------------------------------------------------------------------------------
// 判断文件是否存在
bool BackupTables::DoesFileExist(const char* pfile)
{
    struct stat stat_info;
    return (0 == stat(pfile, &stat_info));
}

// tar文件中的打包列表文件名称:  dbname_tablename.lst
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

// 判断是否存在rsi信息
bool BackupTables::DoesRsiExist(const char* filepattern)
{
    const char* parttern= filepattern;
    glob_t globbuf;
    memset(&globbuf,0,sizeof(globbuf));
    globbuf.gl_offs = 0;
    //GLOB_NOSORT  Don’t sort the returned pathnames.  The only reason to do this is to save processing time.  By default, the returned pathnames are sorted.
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
    // 打开文件列表，看文件列表是否和tar包中的一致
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
        ThrowWith("文件%s打开失败!",pth);
    }
    fseek(pFile,-4,SEEK_END);
    int _tabid = 0;
    fread(&_tabid,4,1,pFile);
    fclose(pFile);

    tabid = _tabid;
    return tabid;
}

// 获取$DATAMERGER_HOME/var/database/table.bht/Table.ctb 中的最后4字节
int BackupTables::GetTableId(const char* pbasepth,const char *dbn,const char *tabname,int & tabid)
{
    char strTablectl[500];
    sprintf(strTablectl,"%s/%s/%s.bht/Table.ctb",pbasepth,dbn,tabname);
    if(!DoesFileExist(strTablectl)) {
        ThrowWith("数据库表%s.%s 不存在!",dbn,tabname);
    }
    return  getTableId(strTablectl,tabid);
}

int BackupTables::updateTableId(const char* pth,const int tabid)
{
    FILE  *pFile  = fopen(pth,"r+b");
    if(!pFile) {
        ThrowWith("文件%s打开失败!",pth);
    }
    fseek(pFile,4,SEEK_END);
    fwrite(&tabid,4,1,pFile);;
    fclose(pFile);
    pFile = NULL;
    return 0;
}

// 将*.bht/table.ctb文件进行重写
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

// 获取$DATAMERGER_HOME/var/database/table.bht/Table.ctb 中的最后4字节
int BackupTables::UpdateTableId(const char* pbasepth,const char *dbn,const char *tabname,const int tableid)
{
    char strTablectl[500];
    sprintf(strTablectl,"%s/%s/%s.bht/Table.ctb",pbasepth,dbn,tabname);
    if(!DoesFileExist(strTablectl)) {
        ThrowWith("数据库表%s.%s 不存在!",dbn,tabname);
    }

    updateTableId(strTablectl,tableid);

    return 0;
}

int BackupTables::RenameTable(const char *src_dbn,const char* src_tbn,const char* dst_dbn,const char* dst_tbn)
{
    const char* pbasepth = psa->GetMySQLPathName(0,"msys");

    //0. 判断数据库表是否存在
    lgprintf("rename table '%s.%s ' to  '%s.%s' ...",src_dbn,src_tbn,dst_dbn,dst_tbn);
    AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);

    mt.FetchAll("select tabid from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",src_tbn,src_dbn);
    int rn=mt.Wait();
    if(rn<1) ThrowWith("表%s.%s在dp_table中找不到!",src_dbn,src_tbn);

    char choose[300];
    sprintf(choose,"请确保该表'%s.%s'当前未进行数据迁移操作和查询操作? (Y/N)",src_dbn,src_tbn);
    bool forcedel = false;
    forcedel = GetYesNo(choose,false);
    if(!forcedel) {
        return 0;
    }

    // 更改表名称
    int tabid = mt.GetInt(0,0);
    AutoStmt st(psa->GetDTS());
    st.DirectExecute("update dp.dp_table set tabdesc='%s' ,tabname='%s',databasename='%s' where tabid = %d ",dst_tbn,dst_tbn,dst_dbn,tabid);

    // 1. 获取源表id
    GetTableId(pbasepth,src_dbn,src_tbn,tabid);

    // 2. 判断RSI信息文件是否存在
    char rsi_parttern[300] = {0};
    bool exist_rsi = true;
    sprintf(rsi_parttern,"%s/BH_RSI_Repository/????.%d.*.rsi",pbasepth,tabid);
    if(!DoesRsiExist(rsi_parttern)) {
        // 空表不存在RSI文件内容
        lgprintf("[警告]表%s.%s的RSI文件不存在!",src_dbn,src_tbn);
        exist_rsi = false;
    }

    // 3. 新表id获取
    int seqid = 0;
    int dst_tabid = 0;
    char seq_file[300];
    sprintf(seq_file,"%s/brighthouse.seq",pbasepth);
    GetSeqId(seq_file,seqid);
    seqid = seqid +1;
    dst_tabid= seqid;
    seqid +=1;
    UpdateSeqId(seq_file,seqid);

    // 4. 移动文件目录
    char cmd[1000];
    int ret = 0;

    // 4.1 mv $DATAMERGER_HOME/var/src_dbn/src_tbn.frm  $DATAMERGER_HOME/var/dst_dbn/dst_tbn.frm
    sprintf(cmd,"mv %s/%s/%s.frm  %s/%s/%s.frm",pbasepth,src_dbn,src_tbn,pbasepth,dst_dbn,dst_tbn);
    lgprintf("rename过程:%s\n",cmd);
    ret = system(cmd);
    if(ret == -1) {
        ThrowWith("rename 过程: mv %s/%s/%s.frm  %s/%s/%s.frm 失败!",pbasepth,src_dbn,src_tbn,pbasepth,dst_dbn,dst_tbn);
    }

    // 4.2 mv $DATAMERGER_HOME/var/src_dbn/src_tbn.bht  $DATAMERGER_HOME/var/dst_dbn/dst_tbn.bht
    sprintf(cmd,"mv %s/%s/%s.bht  %s/%s/%s.bht",pbasepth,src_dbn,src_tbn,pbasepth,dst_dbn,dst_tbn);
    lgprintf("rename过程:%s\n",cmd);
    ret = system(cmd);
    if(ret == -1) {
        ThrowWith("rename 过程: mv %s/%s/%s.bht  %s/%s/%s.bht 失败!",pbasepth,src_dbn,src_tbn,pbasepth,dst_dbn,dst_tbn);
    }

    // 4.3 更新表名称和tabid， $DATAMERGER_HOME/var/dst_dbn.bht/Table.ctb
    char new_path[300];
    sprintf(new_path,"%s/%s/%s.bht",pbasepth,dst_dbn,dst_tbn);
    lgprintf("rename过程:更新%s目录中的表名和id.\n",new_path);
    GenerateNew_table_tcb(new_path,dst_tbn,dst_tabid);

    // 4.4 更新rsi中的tabid，$DATAMERGER_HOME/var/BH_RSI_Repository/????.tabid.%d*.rsi 替换
    if(exist_rsi) {
        char tmp_rsi_path[300];
        sprintf(tmp_rsi_path,"%s/BH_RSI_Repository",pbasepth);
        lgprintf("rename过程:更新%s目录中的ris文件的表id.\n",tmp_rsi_path);
        char tmp_rsi_pattern[300];
        sprintf(tmp_rsi_pattern,"????.%d.*.rsi",tabid);
        UpdateRsiID(tmp_rsi_path,dst_tabid,tmp_rsi_pattern);
    }

    lgprintf("rename过程:rename table [%s.%s] -->[%s.%s] 成功.\n",src_dbn,src_tbn,dst_dbn,dst_tbn);

    return 0;
}


// 备份表,将dbn数据库的表tabname，进行备份，备份后的文件: bkpath/dbn_tabname.pid.tar
//  tar -C /app/dma/var/zkb/ 改变目录
int BackupTables::BackupTable(const char *dbn,const char *tabname,const char *bkpath)
{
    const char* pbasepth = psa->GetMySQLPathName(0,"msys");

    // 1. 获取对应表ID
    int tabid = 0;
    GetTableId(pbasepth,dbn,tabname,tabid);

    // 2. 判断RSI信息文件是否存在
    char rsi_parttern[300] = {0};
    sprintf(rsi_parttern,"%s/BH_RSI_Repository/????.%d.*.rsi",pbasepth,tabid);
    if(!DoesRsiExist(rsi_parttern)) {
        ThrowWith("表%s.%s的RSI文件不存在!",dbn,tabname);
    }

    // 3. 生成完整路径的tar包名称
    char tarfile[500];
    sprintf(tarfile,"%s.%s.%d.tar",dbn,tabname,getpid());

    // 4. 向tar包中加入文件
    char cmd[1000];
    int ret = 0;

    // 4.1 tar包中追加文件 $DATAMERGER_HOME/var/database/table.frm
    sprintf(cmd,"tar -C %s/%s -c -f %s %s.frm",pbasepth,dbn,tarfile,tabname);
    lgprintf("备份过程:%s\n",cmd);
    ret = system(cmd);
    assert(ret != -1);

    // 4.2 tar包中追加文件 $DATAMERGER_HOME/var/database/table.bht
    char _pattern[30000];
    sprintf(_pattern,"tar -C %s -r -f %s %s/%s.bht",pbasepth,tarfile,dbn,tabname);

    std::string tmp;
    sprintf(cmd,"%s/BH_RSI_Repository/????.%d.*.rsi",pbasepth,tabid);
    string_vector rsi_file_vector;
    GetPatternFile(cmd,rsi_file_vector,tmp);
    for(int i=0; i<rsi_file_vector.size(); i++) {
        sprintf(_pattern+strlen(_pattern)," BH_RSI_Repository/%s",rsi_file_vector[i].c_str());
    }
    lgprintf("备份过程:%s\n",_pattern);
    ret = system(_pattern);
    assert(ret != -1);

    // 5. 生成打包列表文件 tar tf xxxx >> database_table.lst
    char tarfile_lst[256];
    GetTarfileLst(dbn,tabname,tarfile_lst);
    sprintf(cmd,"tar -t -f %s > %s",tarfile,tarfile_lst);
    lgprintf("备份过程:%s\n",cmd);
    ret = system(cmd);
    assert(ret != -1);

    // 5.1 将database_table.lst合并到tar包中去
    sprintf(cmd,"tar -r -f %s %s",tarfile,tarfile_lst);
    lgprintf("备份过程:%s\n",cmd);
    ret = system(cmd);
    assert(ret != -1);

    // 5.2 删除打包后的database_table.lst文件
    sprintf(cmd,"rm %s -rf",tarfile_lst);
    lgprintf("备份过程:%s\n",cmd);
    ret = system(cmd);
    assert(ret != -1);

    // 5.3 移动tar文件到指定目录
    sprintf(cmd,"mv %s %s",tarfile,bkpath);
    lgprintf("备份过程:%s\n",cmd);
    ret = system(cmd);
    assert(ret != -1);

    lgprintf("数据库:%s 表:%s 备份成功，备份到路径:%s/%s \n",dbn,tabname,bkpath,tarfile);
    return 0;
}

// 获取满足匹配条件的文件名称存在队列
int BackupTables::GetPatternFile(const char* filepattern,string_vector& rsi_file_vec,std::string &old_dbname)
{
    rsi_file_vec.clear();
    glob_t globbuf;
    memset(&globbuf,0,sizeof(globbuf));
    globbuf.gl_offs = 0;
    //GLOB_NOSORT  Don’t sort the returned pathnames.  The only reason to do this is to save processing time.  By default, the returned pathnames are sorted.
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

            //  // 表名称获取
            if(i==0) {
                int _pos = 0;
                char _nm[100];
                pend--; // 去除/
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


// 检查还原包是否合理
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

    // 打开文件tmp_fle_lst，将内部记录存入列表中
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
                continue; // 跳过目录行
            }

            // 跳过tmppth路径部分 和头部 '/'
            pfn += tmppth_len+1;

            // 加入队列
            tar_file_list.push_back(std::string(pfn));
        }

        fclose(pFile);
    }
    sprintf(cmd,"rm %s -rf",tmp_file_lst);
    ret = system(cmd);
    assert(-1 != ret);

    // 打开文件列表，看文件列表是否和tar包中的一致
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

        // 到tar_file_list 中进行查证
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

// 更新指定目录下的*.rsi文件的表id
// ????.%d.*.rsi 文件替换中通的 %d 的表id
#define RSI_FILE_HEAD_LEN  4   // "CMAP." or  "HIST."
#define RSI_POS         '.'
bool BackupTables::UpdateRsiID(const char* pth,const int tabid,const char* pattern)
{
    char rsi_parttern[300] = {0};
    // tabid已经变了，不用这个新的id
    //sprintf(rsi_parttern,"%s/????.%d.*.rsi",pth,tabid);
    if(pattern == NULL) {
        sprintf(rsi_parttern,"%s/*",pth);
    } else {
        sprintf(rsi_parttern,"%s/%s",pth,pattern);
    }

    char path_head[300];
    sprintf(path_head,"%s",pth);
    int path_head_len = strlen(path_head)+1;  // 包括‘/’的长度

    glob_t globbuf;
    memset(&globbuf,0,sizeof(globbuf));
    globbuf.gl_offs = 0;
    //GLOB_NOSORT  Don’t sort the returned pathnames.  The only reason to do this is to save processing time.  By default, the returned pathnames are sorted.
    if(!glob(rsi_parttern, GLOB_DOOFFS, NULL, &globbuf)) {
        for(int i=0; i<globbuf.gl_pathc; i++) {
            char *poldname = globbuf.gl_pathv[i];

            std::string strname(poldname);
            int str_len = strname.size();
            // 截取CMAP 或者HIST
            std::string strhead = strname.substr(path_head_len,RSI_FILE_HEAD_LEN);

            // %d.*.rsi 的截取
            int  _tmp1 = path_head_len+RSI_FILE_HEAD_LEN + 1;  // '.' 一个字符
            int  _tmp2 = str_len-_tmp1; //  %d.*.rsi 的长度

            std::string strtmp = strname.substr(_tmp1,_tmp2);

            // *.rsi的截取
            int pos = strtmp.find(RSI_POS);
            str_len = strtmp.size();
            strtmp = strtmp.substr(pos+1);  // get 'colid.rsi'

            // get new filename by new tabid
            char pnewname[300];
            // %s/BH_RSI_Repository/????.%d.*.rsi
            sprintf(pnewname,"%s/%s.%d.%s",pth,strhead.c_str(),tabid,strtmp.c_str());

            // 文件重命名操作  poldname ---> pnewname
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
        lgprintf("还原过程:还原失败，清除目录%s数据.",pth);
    } else {
        lgprintf("还原过程:还原成功，清除目录%s数据.",pth);
    }
    system(cmd);
    return true;
}

// 清除回滚目录
// 数据库整个目录文件，如果tabid != 0 清除RSI文件
bool BackupTables::RollbackRestore(const char* pth,const char* dbn,const char* tabname,const int tabid)
{
    char tmp[300];
    char cmd[300];

    if(tabname != NULL) {
        sprintf(tmp,"%s/%s/%s",pth,dbn,tabname);
        lgprintf("还原过程:回滚操作，清除目录%s数据.",tmp);
        sprintf(cmd,"rm %s -rf",tmp);
        system(cmd);
    }

    if(tabid > 0) {
        sprintf(tmp,"%s/BH_RSI_Repository/????.%d.*.rsi",pth,tabid);
        lgprintf("还原过程:回滚操作，清除%s数据.",tmp);
        sprintf(cmd,"rm %s -rf",tmp);
        system(cmd);
    }

    return true;
}


// 还原表操作,将bkfile对应的文件，还原到dbn数据库的tabname表中去
//  tar -C /app/dma/var/zkb/ 改变目录
int  BackupTables::RestoreTable(const char* dbn,const char* tabname,const char* bkfile)
{
    // 1. 判断要还原的表是否存在，如果已经存在则不能被还原
    if(!DoesFileExist(bkfile)) {
        ThrowWith("文件:%s不存在，无法完成还原操作!\n",bkfile);
    }
    if(DoesTableExist(dbn,tabname)) {
        lgprintf("数据库:%s,表:%s已经存在，请先删除表后在还原表!\n",dbn,tabname);
        return 0;
    }

    const char* pbasepth = psa->GetMySQLPathName(0,"msys");
    int ret  = 0;
    char cmd[300];
    char tmp[300];

    // 2. 建立还原表临时目录$DATAMERGER_HOME/dbn.tabname_pid_tmp
    char restore_tmp_pth[300];
    sprintf(restore_tmp_pth,"%s/%s.%s_%d_tmp",pbasepth,dbn,tabname,getpid());
    sprintf(cmd,"mkdir %s",restore_tmp_pth);
    lgprintf("还原过程:%s\n",cmd);
    ret = system(cmd);
    assert(ret != -1);

    // 3. 抽取bkfile 包中所有文件到临时目录$DATAMERGER_HOME/dbn_tabname_pid_tmp 中
    sprintf(cmd,"cd %s",restore_tmp_pth);
    ret = system(cmd);
    assert(ret != -1);

    lgprintf("还原过程:%s\n",cmd);

    sprintf(cmd,"tar -C %s -x -f %s",restore_tmp_pth,bkfile);
    lgprintf("还原过程:%s\n",cmd);
    if(-1== system(cmd)) {
        lgprintf("还原过程失败!\n");
        ClearRestorePath(restore_tmp_pth);
        return -1;
    }

    // 4. 校验数据包是否完整正确，通过database_table.lst 来获取
    char database_table_lst[300];
    char old_tabname[300];
    std::string old_dbname="";
    sprintf(tmp,"%s/*.lst",restore_tmp_pth);
    string_vector lst_file_vec;
    GetPatternFile(tmp,lst_file_vec,old_dbname);
    if(lst_file_vec.size()>0) {
        strcpy(database_table_lst,lst_file_vec[0].c_str());
    } else {
        lgprintf("还原过程:获取文件列表[%s]失败!\n",tmp);
        ClearRestorePath(restore_tmp_pth);
        return -1;
    }

    lgprintf("还原过程:检测数据包%s 是否正确...\n",bkfile);
    ret = CheckRestorePackageOK(restore_tmp_pth,database_table_lst);
    if(-1 == ret) {
        lgprintf("还原过程:打开列表文件:%s  失败!\n",database_table_lst);
        ClearRestorePath(restore_tmp_pth);
        return -1;
    } else if( -2 == ret) {
        lgprintf("还原过程:数据包:%s  检测失败!\n",bkfile);
        ClearRestorePath(restore_tmp_pth);
        return -1;
    }
    lgprintf("还原过程:检测数据包%s 完成.\n",bkfile);


    // 5. 如果需要重命名表就修改表名称
    sprintf(tmp,"%s/*/*.bht",restore_tmp_pth);
    lst_file_vec.clear();
    GetPatternFile(tmp,lst_file_vec,old_dbname);
    if(lst_file_vec.size()>0) {
        strcpy(old_tabname,lst_file_vec[0].c_str());
        char *p = old_tabname;
        while(*p) { // 去除.bht
            if(*p == '.') {
                *p = 0;
                break;
            }
            p++;
        }
    } else {
        lgprintf("还原过程:获取原表目录[%s]失败!\n",tmp);
        ClearRestorePath(restore_tmp_pth);
        return -1;
    }

    // 6. 从文件中获取表id，并从brighthouse.seq中获取id，使用brighthouse.seq中的id作为最新的id
    int tabid=0;

    int seqid = 0;
    char seq_file[300];
    sprintf(seq_file,"%s/brighthouse.seq",pbasepth);
    GetSeqId(seq_file,seqid);

    //6.1 直接使用一个新的id，seqid + 1
    seqid = seqid +1;
    tabid= seqid;
    lgprintf("还原表id:%d,数据库中最大表id:%d\n",tabid,seqid);
    seqid +=1;
    lgprintf("还原过程:更新%s序号%d\n",seq_file,seqid);
    UpdateSeqId(seq_file,seqid);

    //6.2 更新tabid
    char _table_tcb_pth[300];
    sprintf(_table_tcb_pth,"%s/%s/%s.bht/Table.tcb",restore_tmp_pth,old_dbname.c_str(),old_tabname);
    lgprintf("还原过程:更新%s表id %d\n",_table_tcb_pth,tabid);

    //getTableId(_table_tcb_pth,tabid);
    sprintf(tmp,"%s/%s/%s.bht",restore_tmp_pth,old_dbname.c_str(),old_tabname);
    // */table.bht
    GenerateNew_table_tcb(tmp,tabname,tabid);

    // 7. 更新rsi中的id信息
    char tmp_rsi_path[300];
    sprintf(tmp_rsi_path,"%s/BH_RSI_Repository",restore_tmp_pth);
    lgprintf("还原过程:更新%s目录中的ris文件的表id.\n",tmp_rsi_path);
    UpdateRsiID(tmp_rsi_path,tabid);

    // 8. 移动$DATAMERGER_HOME/dbn_tabname_pid_tmp中的数据文件到$DATAMERGER_HOME中

    // 构建数据库目录
    char dbpth[300];
    sprintf(dbpth,"%s/%s",pbasepth,dbn);
    if(!DoesFileExist(dbpth)) {
        sprintf(cmd,"mkdir %s",dbpth);
        system(cmd);
    }

    // 8.1 移动rsi文件到$DATAMERGER_HOME/BH_RSI_Repository中
    char rsi_path[300];
    sprintf(rsi_path,"%s/BH_RSI_Repository",pbasepth);
    strcat(tmp_rsi_path,"/*.rsi");

    lgprintf("还原过程:移动%s目录rsi文件到%s目录中.\n",tmp_rsi_path,rsi_path);
    sprintf(cmd,"mv %s %s",tmp_rsi_path,rsi_path);
    ret = system(cmd);
    if(ret == -1) {
        lgprintf("还原过程:run cmd :' %s '失败",cmd);
        RollbackRestore(pbasepth,dbn,tabname,tabid);
        ClearRestorePath(restore_tmp_pth);
        return -1;
    }
    // 8.2 移动table.frm 到$DATAMERGER_HOME/dbn下面
    sprintf(tmp,"%s/%s.frm",restore_tmp_pth,old_tabname);

    char new_name[300];
    sprintf(new_name,"%s/%s.frm",dbpth,tabname);

    lgprintf("还原过程:移动%s文件到%s.\n",tmp,new_name);

    sprintf(cmd,"mv %s %s",tmp,new_name);
    ret = system(cmd);
    if(ret == -1) {
        lgprintf("还原过程:run cmd :' %s '失败",cmd);
        RollbackRestore(pbasepth,dbn,tabname,tabid);
        ClearRestorePath(restore_tmp_pth);
        return -1;
    }

    // 8.3 移动table.bht到$DATAMERGER_HOME/dbn下面
    // 更改表明在:hexdump -C Table.ctb 中
    sprintf(new_name,"%s/%s.bht",dbpth,tabname);
    sprintf(cmd,"mkdir %s",new_name);
    system(cmd);

    sprintf(tmp,"%s/%s/%s.bht/*",restore_tmp_pth,old_dbname.c_str(),old_tabname);
    lgprintf("还原过程:移动%s文件到%s目录中.\n",tmp,new_name);

    sprintf(cmd,"mv %s %s",tmp,new_name);
    ret = system(cmd);
    if(ret == -1) {
        lgprintf("还原过程:run cmd :' %s '失败",cmd);
        RollbackRestore(pbasepth,dbn,tabname,tabid);
        ClearRestorePath(restore_tmp_pth);
        return -1;
    }

    // 9 还原完成，清除还原路径
    ClearRestorePath(restore_tmp_pth,true);
    return 0;
}


