#ifndef DBS_TURBO_LIB_HEADER
#define DBS_TURBO_LIB_HEADER
#ifdef __unix
#include <pthread.h>
#include <malloc.h>
#include <stdlib.h>
#ifndef BOOL
#define BOOL int
#endif
#ifndef FALSE
#define FALSE 0
#endif
#ifndef TRUE
#define TRUE 1
#endif
#else
#include <windows.h>
#include <process.h>
#include <conio.h>
#endif
#include <stdio.h>
#include <vector>
#include <string.h>

#include <cfm_drv/dm_ib_sort_load.h> // 分布式排序使用
#include <cfm_drv/dm_ib_rowdata.h>
#include "AutoHandle.h"
#include "dt_svrlib.h"
#include "zlib.h"
#include "ucl.h"
#include "mt_worker.h"
#include <lzo1x.h>
#include <bzlib.h>

#include "ThreadList.h"

#ifdef PLATFORM_64
#define MAX_LOADIDXMEM  200000
#else
#define MAX_LOADIDXMEM  200000
#endif

#define RIAK_HOSTNAME_ENV_NAME "RIAK_HOSTNAME"  // 分布式版本判断环境变量
#define RIAK_HOSTNAME_ENV_NAME_CLI "RIAK_HOSTNAME_CLI"  // 分布式版本判断环境变量
#define SORTER_HOSTNAME_ENV_NAME    "SORTER_HOSTNAME"
#define SORTER_PORT_ENV_NAME        "SORTER_PORT"
#define SHORT_SESSION_TYPE          1    // 短会话类型
#define LONG_SESSION_TYPE           2    // 长回话类型

// 0:表中等待装入数据,1:正在合并装入数据,2:已完成装入合并数据,3:清除装入合并临时数据
#define PL_LOAD_DEFAULT -1  // 默认值,排序过程正在进行中
#define PL_LOAD_WAIT    0   // 0:表中等待装入数据
#define PL_LOADING      1   // 1:正在合并装入数据
#define PL_LOADED       2   // 2:已完成装入合并数据
#define PL_LOAD_DEL     3   // 3:清除装入合并临时数据

typedef int64_t int64;

// 集群状态
#define CS_DEFAULT  -1    // 默认缺省值
#define CS_OK       0     // 集群正常工作状态(处于push过程中)
#define CS_ERROR    1     // 集群中遇到错误了
#define CS_MASTER_BUSY 2  // master节点处于忙碌状态(finish过程),新开启的slaver节点不能开始运行
#define CS_MASTER_INIT 3  // master节点处于忙碌状态(init过程),新开启的slaver节点不能开始运行

#define CONTINUE_EXTRACT_END_MAX_TIMES  2

class mytimer2  // 计时器,sleep也计算在内
{
public:
    mytimer2()
    {
        Restart();
    }
    virtual ~mytimer2()
    {
        Restart();
    }

public:
    void Restart()
    {
        tm_mutex.Lock();
        tm_start = time(NULL);
        tm_stop = time(NULL);
        tm_mutex.Unlock();
    }
    double GetTime()
    {
        tm_mutex.Lock();
        tm_stop = time(NULL);
        double diff_sec = difftime(tm_stop,tm_start);
        tm_mutex.Unlock();
        return diff_sec;
    }

private:
    Mutex  tm_mutex;
    time_t tm_start;
    time_t tm_stop;
};

class ParallelLoad  // 并行排序装入基类
{
protected:
    bool dp_parallel_load;         // DP_PARALLEL_LOAD 环境变量

    // 数据抽取运行状态
    typedef enum Enum_Run_Status{ 
        ENUM_UNINIT = 0,  // 未开始
        ENUM_WAIT_NEW_SESSION = 1,   // 等待新的会话生成
        ENUM_SLAVER_WAIT_RUN_SESSION = 2,  // 已经更新了dp.dp_datapartext的launchslavercnt ,fix dma-1998
        ENUM_RUN_CUR_SESSION =3,     // 当前会话上正在执行数据采集
    }Enum_Run_Status;

    // 数据抽取运行状态,发现会话异常的时候进行判断调用
    Enum_Run_Status e_extract_run_status;    
    
public:
    ParallelLoad()
    {
        dp_parallel_load = false;
        char *p = getenv("DP_PARALLEL_LOAD");
        if(p != NULL && strcmp(p,"1") == 0) {
            dp_parallel_load = true;
        } else {
            dp_parallel_load = false;
        }
        e_extract_run_status = ENUM_UNINIT;
    }
    virtual ~ParallelLoad()
    {
        dp_parallel_load = false;
    }

public:
    // 获取表id,列数目
    int get_table_info(const char* pbasepth,const char* pdbname,const char* ptbname,int& tabid,int& colnum);

    // 获取表的包总数,记录总数
    int get_table_pack_obj_info(const char* pbasepth,const char* pdbname,const char* ptbname,
                                int& pack_no,int64& no_obj);

    // 获取分区的最后一个包号和包内记录数
    int get_part_pack_info(const char* pbasepth,const char* pdbname,const char* ptbname,
                           const char* partname,int& pack_no,int& lstpackobj);

    // 获取dpn名称
    std::string get_dpn_file_name(const char* path,const int attr_number);

    // 获取ATTR 文件名称
    std::string get_attr_file_name(const std::string path,const int  attr_number);

    // 获取开始生成数据起始位置
    bool get_commit_sort_info(SysAdmin &sp,
                              int tabid,
                              int datapartid,
                              int64& startloadrow,
                              int& startpackno,
                              int& startpackobj);

};

class DataDump:public ParallelLoad
{
    AutoMt fnmt;
    int datapartid;
    char tmpfn[300];
    char tmpidxfn[300];
    char dumpsql[MAX_STMT_LEN];
    dumpparam dp;
    long memlimit;
    int dtdbc;
    int fnorder;
    int maxblockrn;
    int blocksize;
    mytimer fiotm,sorttm,adjtm;
    mytimer2 paraller_sorttm;   // sorttm + sleep 的时间,fix dma-1323

private:
    // -------------- 分布式排序装入-----------------------------------
    int                   dp_parallel_load_columns; // 并行装入的表的列数
    long                  st_parallel_load_rows;    // 短会话并行装入的数据行数据累计
    long                  lt_parallel_load_rows;    // 长会话并行装入的数据行数据累计

    unsigned long         parallel_load_obj_id;     // 会话ID

    Mutex                 sessionid_lock;           // 更换sessionid的锁
    long                  threadmtrows;              // 线程中获取到的记录数
    char                  stsessionid[32];          // 短会话id
    char                  last_stsessionid[32];    // 上一次短会话id
    int                   st_session_interval;      // 短会话提交时间长度
    char                  ltsessionid[32];          // 长会话id
    DMIBSortLoad*         p_stDMIBSortLoader;       // 短会话排序装入数据句柄
    int                   push_line_nums ;          // 每次push的行数
    DMIBSortLoad*         p_ltDMIBSortLoader;       // 短会话排序装入数据句柄
    bool                  b_master_node;            // master节点
    int                   slaver_index;             // slaver节点的下标
    bool                  add_stsessions_to_ltsession;   // 是否需要将短会话添加到长会话中
    std::string           tbl_info_t_str;           // 表结构对象字符串
    DM_IB_RowData         row_data;                 // 行数据        
    DM_IB_TableInfo       table_info;               // 表结构
    std::vector<uint64_t> tb_packindex_arr;         // 包索引的列数组
    mytimer               sort_timer;               // 排序用的计时器
    char                  sortsampleid[32];         // 排序采样ID

    typedef struct stru_sort_key_column {           // 排序列号及排序长度
#define max_idx_size 100
        int idxcol[max_idx_size];
        int partlen[max_idx_size];
        void clear()
        {
            for(int i=0; i<max_idx_size; i++) {
                idxcol[i]=-1;
                partlen[i]=0;
            }
        }
    } stru_sort_key_column,*stru_sort_key_column_ptr;
    stru_sort_key_column  sort_key_col_info;

private:
    // 初始化排序列信息(列编号和长度)
    void clear_ssort_key_column()
    {
        sort_key_col_info.clear();
    }

    // 初始化排序集群
    int init_sorter_cluster();

    // 提交排序完成
    int commit_sort_data_process(SysAdmin &sp,int tabid,int datapartid,bool cmt_st_session,
                                 bool cmt_lt_session,bool part_is_finish,bool filesmode);

    // 生成会话id
    std::string generate_session_id(SysAdmin &sp,int tabid,int datapartid);

    // 判断是否是master节点
    bool is_master_node(int dbc,int tabid,int datapartid);

    // 判断所有的slaver节点都已经完成
    bool is_all_slaver_node_finish(int dbc,int tabid,int datapartid,
                                   const char* st_session_id,
                                   int& launchcnt,int &finishcnt,
                                   int64& loadrows,int &clusterstatus);
    // 设置集群状态
    void set_cluster_status(int dbc,int tabid,int datapartid,const char* session_id,const int cluster_status);

    // 获取集群状态
    int  get_cluster_status(int dbc,int tabid,int datapartid,const char* session_id);

    bool is_all_sorted_data_has_loaded(int dbc,int tabid,int & need_load_session,int64& loadrows);

    // 开始master节点的数据导出
    int start_master_dump_data(SysAdmin &sp,int tabid,int datapartid,const bool keepfiles = false);

    // 开始slaver节点的数据导出
    int start_slaver_dump_data(SysAdmin &sp,int tabid,int datapartid);

    // 更新slaver节点的启动个数
    int increase_slaver_lanuch_count(SysAdmin &sp,int tabid,int datapartid,bool b_lt_session = false);

    // 更新slaver节点的启动个数
    int decrease_slaver_lanuch_count(SysAdmin &sp,int tabid,int datapartid,bool b_lt_session = false);


    // 更新提交过后的数据信息
    int update_commit_info_to_db(SysAdmin &sp,int tabid,int datapartid,unsigned long commit_rows,bool b_lt_session = false);

    // 更新launchmaster新
    int reset_part_launch_master_info(SysAdmin &sp,int tabid,int datapartid);

    // 生成一个新的短会话id,master节点调用
    int create_new_short_session(SysAdmin &sp,int tabid,int datapartid);

    // 更新短会话id,从内存更新到数据库中给slaver节点用,在master节点创建排序对象完成后调用
    int update_new_short_session(SysAdmin &sp,int tabid,int datapartid,int commitstatus);

    // 更新短会话id,slaver节点调用
    int update_new_short_session(SysAdmin &sp,int tabid,int datapartid,bool switch_stsession);

    // 获取表的包索引字段名称
    int get_packindex_column_list(SysAdmin &sp,int tabid,std::vector<std::string> & packidx_column_list);
    bool column_is_packindex(const std::vector<std::string>  packidx_column_list,const std::string column_name);

	// 获取表的zip压缩
	int get_zipcompress_column_list(SysAdmin &sp,int tabid,std::vector<std::string> & zip_column_list);
	bool column_is_zipcompress(const std::vector<std::string>  zip_column_list,const std::string column_name);

	// 获取表的snappy压缩
	int get_snappycompress_column_list(SysAdmin &sp,int tabid,std::vector<std::string> & snappy_column_list);
	bool column_is_snappycompress(const std::vector<std::string>  snappy_column_list,const std::string column_name);

	// 获取表的lz4压缩
	int get_lz4compress_column_list(SysAdmin &sp,int tabid,std::vector<std::string> & lz4_column_list);
	bool column_is_lz4compress(const std::vector<std::string>  lz4_column_list,const std::string column_name);


    // 创建长短会话排序对象
    int create_DMIBSortLoader(SysAdmin &sp,int tabid,int datapartid,bool create_lt_session = false);

    // 从数据库采集数据,遇到错误进行排序回滚过程
    int rollback_sort_session(SysAdmin &sp,const int tabid,const int datapartid,const int origin_status,const bool filesmode=true);

    // 生成排序会话ID
    inline unsigned long generate_obj_id()
    {
        // 高8位节点ID,低56位装入会话ID
        return parallel_load_obj_id|((unsigned long)(slaver_index & 0xff)<<56);
    }

    // 释放对象
    void release_sorter_obj()
    {
        if(p_stDMIBSortLoader != NULL) {
            delete p_stDMIBSortLoader;
            p_stDMIBSortLoader = NULL;
        }
        if(p_ltDMIBSortLoader != NULL) {
            delete p_ltDMIBSortLoader;
            p_ltDMIBSortLoader = NULL;
        }

        parallel_load_obj_id = 0;
    }

    void debug_time();

    // 没有采集到数据的时候,构建一个空的DMIBSortLoad对象,否则提交的时候会失败
    int CreateEmptyDMIBSortLoad(SysAdmin &sp,int tabid,int partid,AutoMt *pCur);

public:
    DataDump(int dtdbc,int maxmem,int _blocksize);
    int BuildMiddleFilePath(int _fid) ;
    int CheckAddFiles(SysAdmin &sp);
    void ProcBlock(SysAdmin &sp,int tabid,int partid,AutoMt *pCur,int idx,AutoMt &blockmt,int _fid,bool filesmode);
    int DoDump(SysAdmin &sp,SysAdmin &plugin_sp,const char *systype=NULL) ;
    int heteroRebuild(SysAdmin &sp);
    virtual ~DataDump()
    {
        release_sorter_obj();
    };
};

/*IBData file:
    目标数据文件是已经按binary格式处理好的导入二进制文件，文件长度没有做2gb限制，每次整理操作一个文件
  按最大数据库长度的限制，把数据整理生成的结果顺序写入数据块，对数据库做压缩
  在装入阶段，将数据库解压缩，按PIPE方式写入loader过程
  */
#define IBDATAFILE  0x01230123
#define IBDATAVER   0x0100


struct ib_datafile_header {
    int fileflag,maxblockbytes;
    LONG64 rownum;
    int blocknum;
    int fileversion;
    // default compress lzo
};

struct ib_block_header {
    int originlen,blocklen;
    int rownum;
};

class IBDataFile
{
    FILE *fp;
    char filename[300];
    ib_datafile_header idh;
    ib_block_header ibh;
    int buflen;
    char *blockbuf;
    char *pwrkmem;
public:
    IBDataFile()
    {
        blockbuf=NULL;
        fp=NULL;
        buflen=0;
        ResetHeader();
        pwrkmem =
            new char[LZO1X_MEM_COMPRESS+2048];
        memset(pwrkmem,0,LZO1X_MEM_COMPRESS+2048);
    }
    void ResetHeader()
    {
        memset(&idh,0,sizeof(idh));
        idh.fileflag=IBDATAFILE;
        idh.fileversion=IBDATAVER;
    }
    void SetFileName(char *_filename)
    {
        strcpy(filename,_filename);
        if(fp) fclose(fp);
        fp=NULL;
    }
    ~IBDataFile()
    {
        if(fp) fclose(fp);
        if(blockbuf) delete[]blockbuf;
        fp=NULL;
        blockbuf=NULL;
        delete []pwrkmem;
    }
    //return piped record number;
    LONG64 Pipe(FILE *fpipe,FILE *keep_file)
    {
        if(fp) fclose(fp);
        LONG64 recnum=0;
        LONG64 writed=0;
        fp=fopen(filename,"rb");
        if(!fp) ThrowWith("File '%s' open failed.",filename);
        if(fread(&idh,sizeof(idh),1,fp)!=1) ThrowWith("File '%s' read hdr failed.",filename);
        for(int i=0; i<idh.blocknum; i++) {
            fread(&ibh,sizeof(ibh),1,fp);
            if((ibh.originlen+ibh.blocklen)>buflen || !blockbuf) {
                if(blockbuf) delete [] blockbuf;
                buflen=(int)((ibh.originlen+ibh.blocklen)*1.2);
                blockbuf=new char[buflen*2];
            }
            int rt=0;
            if(fread(blockbuf,1,ibh.blocklen,fp)!=ibh.blocklen) ThrowWith("File '%s' read block %d failed.",filename,i+1);
            lzo_uint dstlen=ibh.originlen;
#ifdef USE_ASM_5
            rt=lzo1x_decompress_asm_fast((unsigned char*)blockbuf,ibh.blocklen,(unsigned char *)blockbuf+ibh.blocklen,&dstlen,NULL);
#else
            rt=lzo1x_decompress((unsigned char*)blockbuf,ibh.blocklen,(unsigned char *)blockbuf+ibh.blocklen,&dstlen,NULL);
#endif
            if(rt!=Z_OK)
                ThrowWith("decompress failed,datalen:%d,compress flag%d,errcode:%d",ibh.blocklen,5,rt);
            if(dstlen!=ibh.originlen) ThrowWith("File '%s' decompress block %d failed,len %d should be %d.",filename,i+1,dstlen,ibh.originlen);
            if(fwrite(blockbuf+ibh.blocklen,1,dstlen,fpipe)==0) {
                fclose(fp);
                return -1;
            }
            if(keep_file)
                fwrite(blockbuf+ibh.blocklen,1,dstlen,keep_file);
            recnum+=ibh.rownum;
            writed+=dstlen;
        }
        fclose(fp);
        fp=NULL;
        return recnum;
    }

    void CreateInit(int _maxblockbytes)
    {
        if(buflen<_maxblockbytes) {
            if(blockbuf) delete []blockbuf;
            buflen=_maxblockbytes;
            //prepare another buffer  for store compressed data
            blockbuf=new char[buflen*2];
        }
        if(fp) fclose(fp);
        fp=fopen(filename,"w+b");
        if(!fp) ThrowWith("File '%s' open for write failed.",filename);
        ResetHeader();
        idh.maxblockbytes=_maxblockbytes;
        fwrite(&idh,sizeof(idh),1,fp);
    }
    int Write(int mt)
    {
        if(!fp) ThrowWith("Invalid file handle for '%s'.",filename);
        int rownum=wociGetMemtableRows(mt);
        unsigned int startrow=0;
        int actual_len;
        int writesize=0;
        while(startrow<rownum) {
            int writedrows=wociCopyToIBBuffer(mt,startrow,
                                              blockbuf,buflen,&actual_len);
            lzo_uint dstlen=buflen;
            int rt=0;

            rt=lzo1x_1_compress((const unsigned char*)blockbuf,actual_len,(unsigned char *)blockbuf+buflen,&dstlen,pwrkmem);
            if(rt!=Z_OK) {
                ThrowWith("compress failed,datalen:%d,compress flag%d,errcode:%d",ibh.blocklen,5,rt);
            }

            ibh.originlen=actual_len;
            ibh.rownum=writedrows;
            ibh.blocklen=dstlen;
            rt = fwrite(&ibh,sizeof(ibh),1,fp);
            if(rt != 1) {
                ThrowWith("write ibh failed ,return %d ",rt);
            }

            rt = fwrite(blockbuf+buflen,dstlen,1,fp);
            if(rt != 1) {
                ThrowWith("write blockbuf failed ,return %d ",rt);
            }

            idh.rownum+=writedrows;
            idh.blocknum++;
            startrow+=writedrows;
            writesize=sizeof(ibh)+dstlen;
        }
        return writesize;
    }
    void CloseReader()
    {
        if(fp) fclose(fp);
        fp=NULL;
    }
    // end of file write,flush file header
    void CloseWriter()
    {
        if(!fp) ThrowWith("Invalid file handle for '%s' on close.",filename);
        fseek(fp,0,SEEK_SET);
        fwrite(&idh,sizeof(idh),1,fp);
        fclose(fp);
        fp=NULL;
    }
};

class MiddleDataLoader
{
    //int blockmaxrows;
    //int maxindexrows;
    SysAdmin *sp;
    AutoMt mdindexmt;
    AutoMt indexmt;
    AutoMt blockmt;
    ///AutoMt mdblockmt;
    AutoMt mdf;
    dumpparam dp;
    int tmpfilenum;
    file_mt *pdtf;
    unsigned short *pdtfid;
    //int dtfidlen;
public:
    void CheckEmptyDump();
    int Load(int MAXINDEXRN,long LOADTIDXSIZE,bool useOldBlock=true) ;
    int homo_reindex(const char *dbname,const char *tname);
    int dtfile_chk(const char *dbname,const char *tname) ;
    int AutoAddPartition(SysAdmin &sp);
    int CreateLike(const char *dbn,const char *tbn,const char *nsrcowner,const char *nsrctbn,const char * ndstdbn,const char *ndsttbn,const char *taskdate,bool presv_fmt);
    MiddleDataLoader(SysAdmin *_sp);

    //>> Begin:dm-228,创建数据任务表，通过源表命令参数设置
    int CreateSolidIndexTable(const char* orasvcname,const char * orausrname,const char* orapswd,
                              const char* srcdbname,const char* srctabname,const char* dstdbname,const char* dsttabname,
                              const char* indexlist,const char* tmppath,const char* backpath,const char *taskdate,
                              const char* solid_index_list_file,char* ext_sql);

    // 忽略大字段列，并更新采集数据sql语句
    int IgnoreBigSizeColumn(int dts,const char* dbname,const char* tabname,char* dp_datapart_extsql);
    //<< End:dm-228

    ~MiddleDataLoader()
    {
        if(pdtf) delete [] pdtf;
        if(pdtfid) delete [] pdtfid;
        pdtf=NULL;
        pdtfid=NULL;
        //dtfidlen=0;
    }
};

class DestLoader:public ParallelLoad
{
    int indexid;
    int datapartid;
    int tabid;
    SysAdmin *psa;
    dumpparam dp;
public :
    int ReCompress(int threadnum);
    DestLoader(SysAdmin *_psa)
    {
        psa=_psa;
    }
    int MoveTable(const char *srcdbn,const char *srctabname,const char * dstdbn,const char *dsttabname);
    int Load (bool directIO=false) ;
    LONG64 GetTableSize(const char* dbn,const char* tbn,const bool echo = true);
    int ToMySQLBlock(const char *dbn, const char *tabname);
    int ReLoad ();
    ~DestLoader() {};
    int RecreateIndex(SysAdmin *_Psa) ;
    int UpdateTableSizeRecordnum(int tabid);
    int MoveFilelogInfo(const int tabid,const int datapartid);

public:
    //------------------------分布式排序合并数据-----------------------------
    int commit_load_data();
    int commit_sorted_data(const int sessiontype = SHORT_SESSION_TYPE);
    int clear_has_merged_data(const char* sessionid,const char* mergepath);
    int update_datapart_finish(const int tabid,const int datapartid);
    int clear_has_merged_data(const int tabid,const int datapartid = 0);
    int clear_table_tmp_data(const int tabid,const int datapartid = 0);

public:
    int RemoveTable_exclude_struct(const char *dbn,const char *tabname,const char *partname);
    int RemoveTable(const char *dbn,const char *tabname,const char *partname,bool prompt=true,bool checkdel=true);
    int RemoveTable(const char *dbn,const char *tabname,const int partid,bool prompt=true);
    int RemoveTable(const char *dbn,const char *tabname,bool prompt=true);
    int RemoveTable(const int tabid,const int partid,bool exclude_struct = false);
};

//>> begin: fix dma-737,add by liujs
// 表备份还原操作
class BackupTables
{
protected:
    SysAdmin *psa;

public:
    BackupTables(SysAdmin *_psa)
    {
        psa=_psa;
    }
    ~BackupTables() {};
    int BackupTable(const char *dbn,const char *tabname,const char *bkpath);
    int RestoreTable(const char* dbn,const char* tabname,const char* bkfile);

    int RenameTable(const char *src_dbn,const char* src_tbn,const char* dst_dbn,const char* dst_tbn);
protected:
    int GetTableId(const char* pbasepth,const char *dbn,const char *tabname,int & tabid);
    int getTableId(const char* pth,int & tabid);

    int UpdateTableId(const char* pbasepth,const char *dbn,const char *tabname,const int tableid);
    int updateTableId(const char* pth,const int tabid);

    bool DoesFileExist(const char* pfile);
    bool DoesRsiExist(const char* filepattern);

    typedef std::vector<std::string>  string_vector;
    int GetPatternFile(const char* filepattern,string_vector& rsi_file_vec,std::string &old_dbname);

    bool DoesTableExist(const char* dbn,const char* tabname);

    bool ClearRestorePath(const char* pth,bool success_flag = false);
    bool RollbackRestore(const char* pth,const char* dbn,const char* tabname,const int tabid = 0);

    void GetTarfileLst(const char* dbn,const char* tabname,char* tarfilelst);
    int  CheckRestorePackageOK(const char* tmppth,char* tarfilelst);

    int  GetSeqId(const char* pfile,int & tabid);
    int  UpdateSeqId(const char* pfile,const int tabid);

    bool UpdateRsiID(const char* pth,const int tabid,const char* pattern = NULL);
    bool GenerateNew_table_tcb(const char* pth,const char* pnewtabname,const int tabid);
};
//<< end: fix dma-737,add by liujs

class blockcompress: public worker
{
    int compress;
    char *pwrkmem;
public :
    blockcompress(int cmpflag)
    {
        compress=cmpflag;
        pwrkmem=NULL;
    };
    virtual ~blockcompress()
    {
        if(pwrkmem) delete[] pwrkmem;
    };
    int work()
    {
        //memcpy(outbuf,inbuf,sizeof(block_hdr));
        //char *cmprsbf=outbuf+sizeof(block_hdr);
        //char *bf=inbuf+sizeof(block_hdr);
        memcpy(outbuf,inbuf,blockoffset);
        char *cmprsbf=outbuf+blockoffset;
        char *bf=inbuf+blockoffset;
        block_hdr * pbh=(block_hdr * )(inbuf);
        unsigned int len=pbh->origlen;

        int rt=0;
        //dstlen=outbuflen-sizeof(block_hdr);
        dstlen=outbuflen-blockoffset;
        /******bz2 compress *********/
        if(compress==10) {
            unsigned int dst2=dstlen;
            rt=BZ2_bzBuffToBuffCompress(cmprsbf,&dst2,bf,len,1,0,0);
            if(dstlen>outbuflen-blockoffset)
                ThrowWith("Compress error,inlen:%d,outlen:%d.",len,dstlen);
            dstlen=dst2;
            //  printf("com %d->%d.\n",len,dst2);
        }
        /****   UCL compress **********/
        else if(compress==8) {
            unsigned int dst2=dstlen;
            rt = ucl_nrv2d_99_compress((Bytef *)bf,len,(Bytef *)cmprsbf, &dst2,NULL,5,NULL,NULL);
            if(dstlen>outbuflen-blockoffset)
                ThrowWith("Compress error,inlen:%d,outlen:%d.",len,dstlen);
            dstlen=dst2;
        }
        /******* lzo compress ****/
        else if(compress==5) {
            if(!pwrkmem) {
                pwrkmem = //new char[LZO1X_999_MEM_COMPRESS];
                    new char[LZO1X_MEM_COMPRESS+2048];
                memset(pwrkmem,0,LZO1X_MEM_COMPRESS+2048);
            }
            lzo_uint dst2=dstlen;
            rt=lzo1x_1_compress((const unsigned char*)bf,len,(unsigned char *)cmprsbf,&dst2,pwrkmem);
            dstlen=dst2;
            if(dstlen>outbuflen-blockoffset)
                ThrowWith("Compress error,inlen:%d,outlen:%d.",len,dstlen);
        }
        /*** zlib compress ***/
        else if(compress==1) {
            rt=compress2((Bytef *)cmprsbf,&dstlen,(Bytef *)bf,len,1);
            if(dstlen>outbuflen-blockoffset)
                ThrowWith("Compress error,inlen:%d,outlen:%d.",len,dstlen);
        } else
            ThrowWith("Invalid compress flag %d",compress);
        if(rt!=Z_OK) {
            ThrowWith("Compress failed on compressworker,datalen:%d,compress flag%d,errcode:%d",
                      len,compress,rt);
        }
        //lgprintf("workid %d,data len 1:%d===> cmp as %d",workid,len,dstlen);
        pbh=(block_hdr *)outbuf;
        pbh->storelen=dstlen+blockoffset-sizeof(block_hdr);
        pbh->compressflag=compress;
        dstlen+=blockoffset;
        //printf("storelen:%d,dstlen:%d\n",pbh->storelen,dstlen);
        LockStatus();
        //isidle=true;
        isdone=true;
        Unlock();
        return 1;
    }
};

#endif
