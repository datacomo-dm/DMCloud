
/* dumpfile.h --

This file is part of the DataMerger data extrator.

Copyright (C) 2010 DataComo
All Rights Reserved.

Wang Guosheng
<wangguosheng@datacomo.com>
http://www.datacomo.com
http://mobi5.cn
*/
#ifndef DUMPFILE_H
#define DUMPFILE_H
#include "dt_common.h"
#include <stdio.h>
#include <map>
#include <string>
#include <dlfcn.h>
#include "dt_svrlib.h"
#include "IDumpfile.h"
#include "dmncftp.h"
#include "hdfs.h"
#include <vector>

#define MAX_COLUMNS 500
class file_man
{
    FILE *flist;
    bool ftpmode;
    //duplicated process file
    bool ascmode;
    //���Ա��ظ����ļ�
    bool skippickup;
    //�����ļ�����
    bool skipbackup;
    char lines[300];
    //bool firstpart;
    //char *ptr;
    char LISTFILE[300];
    char bakuperrfile[300];
    char host[100],username[100],passwd[100],path[300],localpath[300];
    char localfile[300];
    // �Ƿ�ɾ��ԭ�ļ� ,add by liujs
    enum DELETE_FILE_FLAG {
        DELETE_NO = 0, // ��ɾ���ļ�
        DELETE_YES = 1, // ɾ���ļ�
    };
    enum REPLASE_FTP_GET_PARAM { // �滻1: ��ʾ��Ҫ�滻ftpget�ļ��Ĳ���%2F
        REPLASE_NO = 0,  // ���滻
        REPLASE_YES = 1, // �滻
    };
    int delFileFlag;           // ɾ���ļ���ʶ
    //>> Begin: delete src file 20130131
    char current_file[300];    // ��ǰ���ڴ�����ļ����ƣ�ɾ��Դ�ļ�ʹ�ã���������·��
    long current_filesize;     // ��¼��ǰ�ļ���С
    bool is_backup_dir;        // �Ƿ���backupĿ¼,�����backupĿ¼����ɾ��Դ�ļ�
    //<< end:

    FTPClient ftpclient;

    // hadoop ���Ӷ���
    hdfsFS  hdfsclient;
    char hdfs_path[300];
    char backup_dir[300];
    bool hdfs_mode;
    long hdfs_getfilesize(hdfsFS hf,const char* path,const char* file);
    int hdfs_copy_file_to_local(hdfsFS hf,const char* path,const char* file,const char* localfile);
public:
    std::string _get_file_title();
    file_man();
    virtual ~file_man() ;
    void SetLocalPath(const char *p);
    void SetAscMode(bool val) ;
    void SetSkipPickup(bool val) ;
    void SetSkipBackup(bool val) ;
    void FTPDisconnect();
    void listHdfs(const char* nodename,const int port,const char* path,const char* filepatt=NULL);
    void list(const char *_host,const char *_username,const char *_passwd,const char *_path,const char *filepatt);
    bool listhasopen()
    {
        return flist!=NULL;
    }
    void clearlist()
    {
        if(flist) fclose(flist);
        flist=NULL;
        unlink(LISTFILE);
    }
    void listlocal(const char *_path,const char *filepatt);
    const char *getlocalfile(const char *fn) ;
    //��ȡ�ļ��������ݣ����backupfile==null,����backupfile[0]=0,�򲻱���
    //  ����ļ�����.gz,�򱸷�ʱ��gzipѹ��
    bool getfile(char *fn,char *backupfile,bool removeori,SysAdmin &sp,int tabid,int datapartid);
    bool getnextfile(char *fn,char *full_fn = NULL);
    // ����ɾ����־
    inline void SetDeleteFileFlag(const int delFlag)
    {
        delFileFlag = delFlag;
    };
    inline bool GetFtpMode()
    {
        return ftpmode;
    };
    inline void SetIsBackupDir(bool isBackup)
    {
        is_backup_dir = isBackup;
    };
    inline bool GetIsBackupDir()
    {
        return is_backup_dir;
    };
    int  DeleteSrcFile();
    inline long GetCurFileSize()
    {
        return current_filesize;
    };
    inline void SetCurrentFile(const char* fn)
    {
        strcpy(current_file,fn);
    }
    bool SrcFileIsZipped()
    {
        return strcmp(current_file+strlen(current_file)-3,".gz")==0;
    }
    inline char* GetPath()
    {
        return path;
    };
    inline void SetBackupDir(const char* pbackup_dir)
    {
        strcpy(backup_dir,pbackup_dir);
    }
};

//plugin must implemtation subclass of FileParser
// construction a subclass of FileParser to process a
//  specified table,once table parse end,destruction object,and next
//  table build another parser
#define MAXLINELEN 40000

#define MAX_FILE_COLUMNS  600
#define BACKUP_LOGFILE "backup.log"

#ifndef _int64_
#ifdef __GNUC__
typedef long long int _int64;
#endif
#define _int64_
#endif

#define PLUS_INF_64     _int64(0x7FFFFFFFFFFFFFFFULL)
#define MINUS_INF_64    _int64(0x8000000000000000ULL)

class FileParser : public IFileParser
{
public:
    char fix_ftp_path[300];     // ���������ftp·��,Ϊ֧��filename�е�Ŀ¼�����ƶ���path��
    char fix_localpath[300];    // �޸Ĺ����local·��,Ϊ֧��filename�е�Ŀ¼�����ƶ���path��
    char fix_cur_pach_md5[34];  // ��ǰ·����MD5ֵ,fix DMA-1315:��·�����ļ���������������
    char fix_filepatt[300];     // ���������filename
    char fix_filename[300];     // �ļ�����
    // ��·�������е�Ŀ¼���ƺ��ļ����Ʒֱ�ȡ����
    void splite_path_name(const char* path,char* dir,char* name);

public:
    std::vector<std::string> str_path_vector;    // ·�������б�,�Ӷ�·���²ɼ��ļ�
    int  current_path_index ;                    // ��ǰ·������
    void splite_path_info(const char* str_path_list,std::vector<std::string>& str_path_vector);
protected :
    int dbc;
    int filerows;
    int fileseq;
    std::string filename;
    // file must be able to hold larger than 4GB contents
    FILE *fp;
    file_man fm;
    int curoffset;//offset line
    int tabid;
    int datapartid;
    int colspos[MAX_FILE_COLUMNS];
    void *colptr[MAX_FILE_COLUMNS];
    int colct;
    char *line;

    // ���ݿ���д��ڵ���(�ַ�����)�������ļ��в�����,�����mt��ʱ����Ҫ��������Ϊnull
    int nulls_colspos[MAX_FILE_COLUMNS];
    int nulls_colct;

    //������·�����ļ���
    std::string basefilename;
    int errinfile;
    int ignore_rows;

    IDumpFileWrapper *pWrapper;
    //�����ļ���ȡ����
    //  ��  FTP SERVER/����·��/����Ŀ¼ ��ȡԴ�ļ��������ѹ���ļ�(.gz),���ѹ��
    //    ������Ŀ¼���ȡ�ļ���ʽ���⣬���ļ������ݣ����Դ�ļ�����ѹ���ģ���ѹ�������ļ�������ֱ�ӱ���
    //    backuppath: �����ļ���·��
    //    frombackup:�Ƿ�ӱ���·����ȡ�ļ�
    //    filepatt:�ļ�����ģʽ(���Ժ�ͨ���)
    //  ����ֵ��1, ��ȡ����Ч�ļ�, 0,û��ȡ���ļ�,2��ȡ�ļ����󣬵�Ӧ�ú���
    // ʹ�õ��Ĳ�����
    //     files:localpath,��ȡ�ļ�Ŀ��·��
    //     ftp:textmode/ftp:host/ftp:username/ftp:password/ftp:path, ��ȡ�ļ���ftp����
    //     local:path
    // ȡ����Ч�ļ�ʱ���ļ�����filename��Ա
    int commonGetFile(const char *backuppath,bool frombackup,const char *filepatt,SysAdmin &sp,int tabid,int datapartid,bool checkonly=false) ;


protected:
    int     slaver_index;             // slaver�ڵ���±�

public :
    FileParser() ;
    virtual ~FileParser();

    virtual int GetErrors()
    {
        return errinfile;
    }
    virtual int WriteErrorData(const char *line) = 0;

    //������������ڶ��������������Ա����֮ǰ����
    virtual void SetTable(int _dbc,int _tabid,int _datapartid,IDumpFileWrapper *pdfw) ;
    virtual int GetForceEndDump();

    // fill filename member,the wrapper check conflict concurrent process,
    // if one has privillege to process current file ,then invoke DoParse
    // ����ֵ��
    //  -1 ������������������״̬�Ѿ��������������޸�Ϊȡ��������ͣ
    //  0�������ļ�
    //  1��ȡ����Ч���ļ�
    virtual int GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly=false) = 0;
    // if end ,update task to next step(with lock)
    // critical function !
    // checked every file parser completed.
    // �����ǲ������ļ���Ҫ�����ʱ�򣬲��ܵ��������麯��
    virtual bool ExtractEnd(int _dbc) = 0;
    virtual int GetIdleSeconds() ;
    
    // return value:
    //  -1: a error occurs.�����ڴ���ֶνṹ��ƥ��
    //  0:memory table has been filled full,and need to parse again
    //  1:file parse end
    //while parser file ended,this function record file process log and
    // sequence of file in dp dictions tab.
    virtual int DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) = 0;
    virtual int GetFileSeq(const char *filename) = 0;

    // ����Ƿ�������Ϊͳһ��λ����һ��ɼ�����
    // 1> [default: false]���������ɼ�����
    // 2> [true]����ɼ�����,���DMA-1448����,ͨ��[files::dumpfileforholetable]���л�ȡ
    virtual bool GetDumpfileForHoleTable(); 

    virtual void PluginPrintVersion() ;
    virtual void SetSession(const int slaverIndex=0);
};

#endif


