
/* dumpfile.h --

This file is part of the DataMerger data extrator.

Copyright (C) 2010 DataComo
All Rights Reserved.

Wang Guosheng
<wangguosheng@datacomo.com>
http://www.datacomo.com
http://mobi5.cn
*/
#include "dumpfile.h"
#include "dmncftp.h"
#include <openssl/md5.h>   // MD5
//>> Begin: ���FTP list & get ʧ�ܺ��������5�β�����ÿ�����Խ��еȴ�2���ӣ�DM-198 , add by liujs
#define FTP_GET_LIST_RETRY_TIMES    3
#define FTP_OPER_FAIL_SLEEP_SEC     1
//<< End: add by liujs

/*
 *  ����: ��·���ж����'/'����ȡ����
 *  ����: pattern[in/out]
 *  ����: commontext_test//101206_*.gz ---> commontext_test/101206_*.gz]
 */
void convert_path_pattern(char * pattern)
{
    char tmp[500]= {0};
    char *p = pattern;
    int idx = 0;
    while (*p) {
        if( (idx>0) && (tmp[idx-1]=='/') && (tmp[idx-1] == *p) ) {
            p++;
        } else {
            tmp[idx] = *p;
            idx ++;
            p++;
        }
    }

    strcpy(pattern,tmp);
}

void file_man::FTPDisconnect()
{
    ftpclient.Close();
}
file_man::file_man()
{
    memset(localfile,0,300);
    flist=NULL;
    memset(lines,0,sizeof(lines));
    ascmode=true;
    ftpmode=false;
    //����ƶ������޸�
    skippickup=false;
    skipbackup=false;
    sprintf(LISTFILE,"%s/lst/filelist%d.txt",getenv("DATAMERGER_HOME"),getpid());
    strcpy(localpath,"./");

    bakuperrfile[0]=0;
    const char *path=NULL;
    if(path==NULL) path=getenv("WDBI_LOGPATH");
    if(path) {
        strcpy(bakuperrfile,path);

        if(path[strlen(path)-1]!=PATH_SEPCHAR) {
            strcat(bakuperrfile,PATH_SEP);
        }
    }
    strcpy(bakuperrfile,BACKUP_LOGFILE);

    delFileFlag = DELETE_YES;
    memset(current_file,0,300);
    current_filesize = 0;

    is_backup_dir = false;

    hdfs_mode = false;
    hdfsclient = NULL;

}
file_man::~file_man()
{
    if(flist) fclose(flist);
    flist=NULL;
    remove(LISTFILE);
    if(hdfsclient != NULL) {
        hdfsDisconnect(hdfsclient);
        hdfsclient = NULL;
    }

    memset(current_file,0,300);
}
void file_man::SetLocalPath(const char *p)
{
    strcpy(localpath,p);
}
void file_man::SetAscMode(bool val)
{
    ascmode=val;
}
void file_man::SetSkipPickup(bool val)
{
    skippickup=val;
}
void file_man::SetSkipBackup(bool val)
{
    skipbackup=val;
}

void file_man::list(const char *_host,const char *_username,const char *_passwd,const char *_path,const char *filepatt)
{
    char remotepath[500];
    strcpy(host,_host);
    strcpy(username,_username);
    strcpy(passwd,_passwd);

    //if(_path[0]=='/')
    //  sprintf(path,"%%2F%s",_path+1);
    //else
    strcpy(path,_path);
    if(flist) fclose(flist);
    flist=NULL;
    remove(LISTFILE);

    // remote path end with '/' cause list result no path prefix,only filename
    sprintf(remotepath,"%s/%s",     path,filepatt);

    convert_path_pattern(remotepath);

    lgprintf("begin to list ftp , pattern [%s].....",remotepath);

    ftpclient.SetContext(_host,_username,_passwd);
    ftpclient.ListFile(remotepath,LISTFILE);
    lgprintf("��ȡ�ļ��б� ...");

    flist=fopen(LISTFILE,"rt");
    if(flist==NULL) ThrowWith("�ļ��б�ʧ��.���ļ��б�%sʧ��.");
    memset(lines,0,sizeof(lines));
    fclose(flist);
    flist=NULL;
    ftpmode=true;
}


// ��ȡ�ļ���С
long file_man::hdfs_getfilesize(hdfsFS hf,const char* path,const char* file)
{
    char path_file[300];
    sprintf(path_file,"%s/%s",path,file);
    if(hdfsclient == NULL) {
        ThrowWith("hdfs_getfilesize ��ȡ�ļ�%s ��Сʧ��!",path_file);
    }

    hdfsFileInfo * phdfsFileInfo  = hdfsGetPathInfo(hdfsclient,path_file);
    if(phdfsFileInfo == NULL) {
        ThrowWith("hdfs_getfilesize ��ȡ�ļ�%s ��Сʧ��!",path_file);
    }
    hdfsFreeFileInfo(phdfsFileInfo,1);
    return phdfsFileInfo->mSize;

}

// ���������ļ���ͨ��read��write�ķ�ʽ
#define GZ_BUFLEN 1024*64
int file_man::hdfs_copy_file_to_local(hdfsFS hf,const char* path,const char* file,const char* localfile)
{
    char path_file[300];
    sprintf(path_file,"%s/%s",path,file);
    if(hdfsclient == NULL) {
        ThrowWith("hdfs_copy_file_to_local ��ȡ�ļ�%s ---> %sʧ��,hdfsclient is null!",path_file,localfile);
    }

    hdfsFile    hdfs_src_file = hdfsOpenFile(hf,path_file,O_RDONLY,0,0,0);
    if(NULL == hdfs_src_file) {
        ThrowWith("hdfs_copy_file_to_local  ���ļ�%sʧ��,hdfsclient is null!",path_file);
    }

    FILE *local_dest_fp=fopen(localfile,"wb");
    if(local_dest_fp==NULL) {
        hdfsCloseFile(hf,hdfs_src_file);
        ThrowWith("hdfs_copy_file_to_local:%s��ʧ��.",localfile);
    }
    char buf[GZ_BUFLEN];
    long wrted=0;
    int errcode=0;
    int readed=hdfsRead(hf,hdfs_src_file,buf,GZ_BUFLEN);
    while(readed>0) {
        int w=fwrite(buf,readed,1,local_dest_fp);
        if(w<1) {
            hdfsCloseFile(hf,hdfs_src_file);
            fclose(local_dest_fp);
            ThrowWith("hdfs_copy_file_to_local:%s->%sд�����ݴ���.",path_file,localfile);
        }
        wrted+=w;
        readed= readed=hdfsRead(hf,hdfs_src_file,buf,GZ_BUFLEN);
    }
    if(readed == -1) {
        hdfsCloseFile(hf,hdfs_src_file);
        fclose(local_dest_fp);
        ThrowWith("hdfs_copy_file_to_local:read %s ����.",path_file);
    }
    fclose(local_dest_fp);
    hdfsCloseFile(hf,hdfs_src_file);

    return wrted;
}

// ��hdfs hdfsListDirectory �������ļ����л�ȡ�ļ�����
// file:/tmp/pth/tmp/data_2013010206_14.txt --> /tmp/pth/tmp/data_2013010206_14.tx
std::string _get_file_name(const char* ls_hdfs_file)
{
    // printf("begin:--> %s \n",ls_hdfs_file);
    const char *p=ls_hdfs_file;
    std::string _hdfs_file(p+5);
    // printf("end:--> %s \n",_hdfs_file.c_str());
    return _hdfs_file;
}

void file_man::listHdfs(const char* nodename,const int port,const char* path,const char* filepatt)
{
    if(flist) fclose(flist);
    flist=NULL;
    remove(LISTFILE);

    if(NULL != hdfsclient) {
        hdfsDisconnect(hdfsclient);
        hdfsclient = NULL;
    }
    hdfsclient = hdfsConnect(nodename,port);
    if(hdfsclient == NULL) {
        ThrowWith("����hdfs�ļ�ϵͳ[nodename=%s,port=%s] ʧ��.\n",nodename,port);
    }

    strcpy(hdfs_path,path);  // ���»�ȡ�ļ���Сʹ��

    // api : hadoop fs -ls /tmp/*.cpp | ls  -1
    char path_pattern[300] = {0};
#if 0
    // hdfsListDirectory ����NULL
    sprintf(path_pattern,"%s/%s",path,filepatt);
#else
    sprintf(path_pattern,"%s",path);
#endif

    lgprintf("begin to list hdfs , pattern [%s].....",path_pattern);

    int hdfs_num_entries = 0;
    hdfsFileInfo * phdfsFileInfoAry = hdfsListDirectory(hdfsclient,path_pattern,&hdfs_num_entries);
    if( NULL == phdfsFileInfoAry) {
        lgprintf(" file_man::listHdfs  hdfsListDirectory(%p,%s,%p) return NULL",hdfsclient,path_pattern,&hdfs_num_entries);
        return ;
    } else {
        // ��ȡ�ļ���Ϣ����list�ļ���
        FILE *fp=fopen(LISTFILE,"wt");
        if(fp==NULL) {
            ThrowWith("����ļ�%s��ʧ��.",LISTFILE);
        }
        //GLOB_NOSORT  Don��t sort the returned pathnames.  The only reason to do this is to save processing time.  By default, the returned pathnames are sorted.
        for(int i=0; i<hdfs_num_entries; i++) {
            if(phdfsFileInfoAry[i].mKind == kObjectKindFile) {
                std::string _hdfs_file = _get_file_name(phdfsFileInfoAry[i].mName);
                fprintf(fp,"%s\n",_hdfs_file.c_str());
            }
        }
        fclose(fp);
    }

    lgprintf("��ȡ�ļ��б� ...");
    flist=fopen(LISTFILE,"rt");
    if(flist==NULL) ThrowWith("�ļ��б�ʧ��.���ļ��б�%sʧ��.");
    memset(lines,0,sizeof(lines));
    fclose(flist);
    flist=NULL;
    hdfs_mode = true;

    hdfsFreeFileInfo(phdfsFileInfoAry,hdfs_num_entries);

}

std::string file_man::_get_file_title()
{
    std::string _get_file_title;
    if(hdfs_mode)
        _get_file_title = "HDFS GET";
    else if(ftpmode)
        _get_file_title = "FTP GET";
    else
        _get_file_title = "LOCAL COPY";

    return _get_file_title;
}

void file_man::listlocal(const char *_path,const char *filepatt)
{
    char localpath[300];
    if(flist) fclose(flist);
    flist=NULL;
    strcpy(path,_path);
    remove(LISTFILE);

    sprintf(localpath,"%s/%s",_path,filepatt);
    convert_path_pattern(localpath);
    lgprintf("begin to listlocal , pattern [%s].....",localpath);
    listfile(localpath,LISTFILE);

    flist=fopen(LISTFILE,"rt");
    if(flist==NULL) ThrowWith("�ļ��б�ʧ��.");
    memset(lines,0,sizeof(lines));
    fclose(flist);
    flist=NULL;
    ftpmode=false;
}

const char *file_man::getlocalfile(const char *fn)
{
    if(fn!=NULL) {
        memset(localfile,0,300);
        sprintf(localfile,"%s/%s",skippickup?path:localpath,fn);
    }
    return localfile;
}


typedef struct backup_file_info { // add by liujs
    int  backup_type;      //��������:[1-localcopyfile][2-gzipfile]
    char src_file[256];    //Դ�ļ�
    char dst_file[256];    //�����ļ�
} backup_file_info,*backup_file_info_ptr;

void* OnBackupFileProc(void *params) // �����ļ����߳�
{
    backup_file_info_ptr _pobj = NULL;
    _pobj = (backup_file_info_ptr)params;

    backup_file_info thread_param;
    thread_param.backup_type = _pobj->backup_type;
    strcpy(thread_param.src_file,_pobj->src_file);
    strcpy(thread_param.dst_file,_pobj->dst_file);

    pthread_detach(pthread_self());

    const char* psrc_file = thread_param.src_file;
    const char* pdst_file = thread_param.dst_file;
    if(thread_param.backup_type  == 1) {
        localcopyfile(psrc_file,pdst_file);
    } else if(thread_param.backup_type  == 2) {
        gzipfile(psrc_file,pdst_file);
    } else {
        printf("OnBackupFileProc thread_param.backup_type [%d] error",thread_param.backup_type);
    }
    return NULL;
}


//��ȡ�ļ��������ݣ����backupfile==null,����backupfile[0]=0,�򲻱���
// ����ļ�����.gz,�򱸷�ʱ��gzipѹ��
// filename �ⲿʹ��:filename=fm.getlocalfile(localfile);
bool file_man::getfile(char *fn,char *backupfile,bool removeori,SysAdmin &sp,int tabid,int datapartid)
{
    lgprintf("file_man::getfile begin to getfile %s , ftpmode = %d,hdfs_mode = %d \n",fn,ftpmode?1:0,hdfs_mode?1:0);

    char ls_current_file_Info[300] = {0};
    char ls_current_file_name[300] = {0};
    long ls_current_file_size = 0;
    char tmpfile[300] = {0};
    //tmf.Start();
    char *end=fn+strlen(fn)-1;
    while(*end<' ') *end--=0;

    if(ftpmode && !is_backup_dir) { // ftp �ɼ������Ҳ���backupĿ¼��ʱ��
        // ��¼��ǰ���ļ�����(�������ļ�����)
        sprintf(current_file,"%s/%s",path,fn);
        ls_current_file_size=ftpclient.FileSize(current_file);
        sprintf(tmpfile,"%s/%s",localpath,fn);
        remove(tmpfile);

        lgprintf("FTP Get %s->%s ...",current_file,tmpfile);
        ftpclient.GetFile(current_file,tmpfile);

        if(strcmp(fn+strlen(fn)-4,".ack")==0) {
            //ɾ�����ص�ȷ���ļ�(���ļ�)
            remove(getlocalfile(fn));
            current_file[strlen(current_file)-4]=0;
            tmpfile[strlen(tmpfile)-4]=0;
            ftpclient.GetFile(current_file,tmpfile);
        }
    } else if(hdfs_mode && !is_backup_dir) { // hadoop hdfs ���Ҳ���backupĿ¼��ʱ��
        sprintf(current_file,"%s/%s",hdfs_path,fn);

        ls_current_file_size=hdfs_getfilesize(hdfsclient,hdfs_path,fn);
        sprintf(tmpfile,"%s/%s",localpath,fn);
        remove(tmpfile);

        lgprintf("hdfs Get %s->%s ...",current_file,tmpfile);
        hdfs_copy_file_to_local(hdfsclient,hdfs_path,fn,tmpfile);
    } else if(!is_backup_dir) { // �����ļ� ���Ҳ���backupĿ¼��ʱ��
        // ��¼��ǰ���ļ�����(�������ļ�����)
        memset(current_file,0,300);
        sprintf(current_file,"%s/%s",path,fn);
        sprintf(tmpfile,"%s/%s",localpath,fn);
        ls_current_file_size=localfilesize(current_file);

        // local file
        if(!skippickup) {
            localcopyfile(current_file,tmpfile);
            if(strcmp(fn+strlen(fn)-4,".ack")==0) {
                //����ļ����ڣ�ɾ�����ص�ȷ���ļ�(���ļ�)
                if( -1 == remove(getlocalfile(fn))) {
                    // lgprintf("ɾ���ļ�:%s ʧ�ܣ���ȷ���Ƿ����ɾ���ļ�Ȩ��.",getlocalfile(fn));
                }
                current_file[strlen(current_file)-4]=0;
                tmpfile[strlen(tmpfile)-4]=0;
                localcopyfile(current_file,tmpfile);
            }
        } // skippickup endif
    }


    struct stat st;
    int retry_times = 0;
    char bk_dir_file[300];
retry_begin:  // fix dma-1040
    if(!is_backup_dir) {
        if(stat(getlocalfile(fn),&st)) { // �ɹ�����0
            ThrowWith("�ļ��ɼ�ʧ��:%s.",fn);
        }
    } else {
        sprintf(bk_dir_file,"%s/%s",backup_dir,fn);
        if(stat(bk_dir_file,&st)) { // �ɹ�����0
            ThrowWith("�ӱ���Ŀ¼�ļ��ɼ�ʧ��:%s.",bk_dir_file);
        }
    }

    // �Ա��ļ���С
    //long _f_size = localfilesize(getlocalfile(fn));
    if(!is_backup_dir && st.st_size != ls_current_file_size) {
        if(retry_times < 10) {
            lgprintf("Դ�ļ���С(%ld)��ɼ������ļ���С(%ld)��һ�������Դ���(%d).",ls_current_file_size,st.st_size,retry_times);
            sleep(1);
            retry_times++;
            goto retry_begin;
        }
        ThrowWith("Դ�ļ���С(%ld)��ɼ������ļ���С(%ld)��һ���������˳�.",ls_current_file_size,st.st_size);
    }
    current_filesize = st.st_size ;  // ��¼�ļ���С������д���dp.dp_filelog���õ�

    //---------------------------------------------------------------------------------
    // 1>  ���ԷǱ���Ŀ¼���ļ�,����Ҫ���б��ݲ�����
    if( !is_backup_dir && (backupfile && backupfile[0]!=0) && !skipbackup) {
        if(strcmp(backupfile+strlen(backupfile)-4,".ack")==0)
            backupfile[strlen(backupfile)-4]=0;

        // �����ѹ���ļ���ֱ�ӽ�ѹ���ļ�����������Ŀ¼
        if(strcmp(fn+strlen(fn)-3,".gz") == 0) {
            strcat(backupfile,".gz");

#ifdef BACKUP_BLOCK
            localcopyfile(getlocalfile(fn),backupfile);
#else
            //  TODO: �᲻�����getlocalfile(fn) ��û��ѹ����ɾͱ�ɾ�������
            pthread_t backup_pthread_id ;
            backup_file_info thread_param;
            thread_param.backup_type = 1;
            strcpy(thread_param.src_file,getlocalfile(fn));
            strcpy(thread_param.dst_file,backupfile);

            if(pthread_create(&backup_pthread_id,NULL,OnBackupFileProc,(void*)&thread_param)!=0) {
                lgprintf("can't create thread to backup [%s] --->[%s].\n",getlocalfile(fn),backupfile);
                return -1;
            }
            sleep(1);
            lgprintf("create thread to backup [%s] --->[%s].\n",getlocalfile(fn),backupfile);
#endif

        } else { // �������ѹ���ļ�,�������ѹ�����뱸��Ŀ¼
            strcat(backupfile,".gz");
            remove(backupfile);

#ifdef BACKUP_BLOCK
            gzipfile(getlocalfile(fn),backupfile);
#else
            pthread_t backup_pthread_id ;
            backup_file_info thread_param;
            thread_param.backup_type = 2;
            strcpy(thread_param.src_file,getlocalfile(fn));
            strcpy(thread_param.dst_file,backupfile);

            if(pthread_create(&backup_pthread_id,NULL,OnBackupFileProc,(void*)&thread_param)!=0) {
                lgprintf("can't create thread to backup [%s] --->[%s].\n",getlocalfile(fn),backupfile);
                return -1;
            }
            sleep(1);
            lgprintf("create thread to backup [%s] --->[%s].\n",getlocalfile(fn),backupfile);
#endif


        }
    } else { // if(!is_backup_dir && skipbackup){
        // ����Ǳ���Ŀ¼�ɼ�,���Ҳ���Ҫ���ݵ�,����Ҫ�����κδ������
        // �ⲿʹ���ļ�����:filename=fm.getlocalfile(localfile);
    }

    // ѹ���ļ�
    char _file_name[300];
    char _temp1[300],_temp2[300];

    if(is_backup_dir) { // 2>  �ӱ���Ŀ¼��ѹ���ļ������н�ѹ�� ����ǰĿ¼
        int remove = 0;
        strcpy(_file_name,bk_dir_file);
        if(strcmp(_file_name+strlen(_file_name)-3,".gz")==0) { // ��ѹ�ļ�, x.gz ------> x
            _file_name[strlen(_file_name)-3] = 0;
            remove =3;
        }

        fn[strlen(fn)-remove]=0;
        strcpy(_temp1,bk_dir_file);
        strcpy(_temp2,getlocalfile(fn));
        gunzipfile(_temp1,_temp2);

        if(stat(_temp2,&st)) {
            lgprintf("�ļ���ѹ��ʧ��:%s,���Դ���...",_temp1);
            sp.log(tabid,datapartid,106,"�ļ���ѹ��ʧ��:%s,���Դ���...",_temp1);
            return false;
        }
    } else { // 3>   ���Ǳ���Ŀ¼�����ļ����Ҳ���Ҫ���ݣ���Ҫ���ݵģ���if(backupfile && backupfile[0]!=0 && !skipbackup) ���Ѿ����ݣ�
        strcpy(_file_name,fn);

        if(strcmp(fn+strlen(fn)-3,".gz")==0) {  // ��ѹ�ļ�,x.gz -----> x
            _file_name[strlen(_file_name)-3] = 0;

            strcpy(_temp2,getlocalfile(_file_name));
            remove(_temp2);

            strcpy(_temp1,getlocalfile(fn));

            // x.gz -----------> x �ļ�
            gunzipfile(_temp1,_temp2);

            fn[strlen(fn)-3]=0;
            if(stat(_temp2,&st)) {
                lgprintf("�ļ���ѹ��ʧ��:%s,���Դ���...",_temp1);
                sp.log(tabid,datapartid,106,"�ļ���ѹ��ʧ��:%s,���Դ���...",_temp1);
                return false;
            }

            // ɾ��ftp�ɼ������ص�.gzѹ���ļ�,
            // ɾ������·!skippickup����²ɼ������ص��ļ�,
            // ��Ϊ�Ѿ���ѹ����,����Ҫ���б�����
            if(ftpmode || (!ftpmode && !hdfs_mode && !skippickup )) {
                lgprintf("ɾ��ѹ���ļ�[%s]...",_temp1);
                remove(_temp1);
            }
        } else {
            // ����ѹ���ļ�������Ҫ����ѹ������������Ҫ�����������Ȳ���
            // todo nothing
            // �ⲿʹ���ļ�����:filename=fm.getlocalfile(localfile);
        }
    }

    return true;
}

bool file_man::getnextfile(char *fn,char *full_fn)
{
    struct stat stat_info;
    if(0 != stat(LISTFILE, &stat_info)) { // fix bug
        return false;
    }

    if(!flist) flist=fopen(LISTFILE,"rt");
    if(flist==NULL) ThrowWith("�ļ��б�ʧ��.");
    while(fgets(lines,300,flist)!=NULL) {
        int sl=strlen(lines);
        if(lines[sl-1]=='\n') lines[sl-1]=0;
        if(full_fn != NULL) { // �����ļ����ƵĻ�ȡ
            strcpy(full_fn,lines);
        }
        char *pfn=lines+strlen(lines)-1;
        //�ҵ��ļ����Ŀ�ʼλ�ã��ų�·������)
        while(pfn>lines) if(*pfn--=='/')  {
                pfn+=2;
                break;
            }
        strcpy(fn,pfn);
        return true;
    }
    fclose(flist);
    flist=NULL;

    //>> begin: fix DM-264
    remove(LISTFILE);
    //<< end:fix dm-264

    return false; // reach eof
}

//>> DM-216 , 20130131��ɾ��Դ�ļ�
int  file_man::DeleteSrcFile()
{
    if(strlen(current_file)< 1) {
        lgprintf("�ļ������ڣ�ɾ��Դ�ļ�ʧ��.");
        return -1;
    }

    convert_path_pattern(current_file);
    if(ftpmode) { // ftp ģʽɾ��FTP������ļ�
        if(DELETE_YES == delFileFlag) {
            try {
                lgprintf("ɾ���ļ�:%s,is_backup_dir = %s",current_file,is_backup_dir?"true":"false");
                ftpclient.Delete(current_file);
                // ɾ��ȷ��.ack�ļ�
                if(strcmp(current_file+strlen(current_file)-4,".ack")==0) {
                    current_file[strlen(current_file)-4] = 0;
                    lgprintf("ɾ���ļ�:%s,is_backup_dir = %s",current_file,is_backup_dir?"true":"false");
                    ftpclient.Delete(current_file);
                }
            } catch(...) {
                // ����ftpɾ���ļ���ʾ:"remote delete failed."����,�����µĽ����˳��ɼ�
            }
        }
    } else if(hdfs_mode) { // hdfs
        if(DELETE_YES == delFileFlag) {
            lgprintf("ɾ���ļ�:%s,is_backup_dir = %s",current_file,is_backup_dir?"true":"false");
            hdfsDelete(hdfsclient,current_file,0);
            // ɾ��ȷ���ļ�
            if(strcmp(current_file+strlen(current_file)-4,".ack")==0) {
                current_file[strlen(current_file)-4] = 0;
                hdfsDelete(hdfsclient,current_file,0);
            }
        }
    } else { // ɾ�������ļ�,backupĿ¼�Ĳ�ɾ��
        if( !is_backup_dir &&  DELETE_YES == delFileFlag) {
            lgprintf("ɾ���ļ�:%s,is_backup_dir = %s",current_file,is_backup_dir?"true":"false");
            remove(current_file);
            if(strcmp(current_file+strlen(current_file)-4,".ack")==0) {
                current_file[strlen(current_file)-4]=0;
                remove(current_file);
            }
        } // end if(DELETE_YES == delFileFlag)

    } // end else

    return 0;
}

// ��·�������еĶ��·���ֱ�ȡ����
// str_path_list[in]: ·�������б�����:"/app/dir1;/app/dir2;/app/dir3"
// str_path_vector[in/out]: Ŀ¼���ƶ���
#ifndef SEPERATE_FLAG
#define SEPERATE_FLAG   ';'
#endif
void FileParser::splite_path_info(const char* str_path_list,std::vector<std::string>& str_path_vector)
{
    int _tmp_list_len = strlen(str_path_list) + 100;
    char *_tmp_lst = new char[_tmp_list_len]; // fix dma-1288
    strcpy(_tmp_lst,str_path_list);
    const char* p = _tmp_lst;
    char *q = _tmp_lst + (strlen(_tmp_lst)-1);

    while((*p) == SEPERATE_FLAG) {
        p++;
    };
    while((*q) == SEPERATE_FLAG) {
        *q = 0;
        q--;
    };

    char _path[300];
    int _path_len = 0;
    while(*p) {
        if(*p == SEPERATE_FLAG) { // �ҵ����·���е�����һ��(�������һ��)
            strncpy(_path,(p-_path_len),_path_len);
            _path[_path_len] = 0;
            std::string _tmp_item(_path);
            str_path_vector.push_back(_tmp_item);
            _path_len = 0;
        } else {
            _path_len++;
        }
        p++;
    }
    // ���һ��·��,����ֻ��һ��·�������
    {
        strncpy(_path,(p-_path_len),_path_len);
        _path[_path_len] = 0;
        std::string _tmp_item(_path);
        str_path_vector.push_back(_tmp_item);
    }

    delete [] _tmp_lst;
    _tmp_lst = NULL;
}


// ��·�������е�Ŀ¼���ƺ��ļ����Ʒֱ�ȡ����
// path[in]: ·�����ƣ�����:"/20131110/*.txt.gz"
// dir[in/out]: Ŀ¼���ƣ�����:"/20131110/"
// name[in/out]: �ļ����ƣ�����:"*.txt.gz"
void FileParser::splite_path_name(const char* path,char* dir,char* name)
{
    int slen=strlen(path);
    char _dir[300];
    strcpy(_dir,path);
    char *pend = _dir+strlen(_dir)-1;
    int pos = 0;
    while (*pend && *pend != '/') {
        pos++;
        pend--;
    }
    *(pend+1) = 0;
    if(*pend == '/') {// find dir
        strcpy(name,path+(slen-pos));
        strcpy(dir,_dir);
    } else {
        dir[0]=0;
        strcpy(name,path);
    }

    lgprintf("FileParser::splite_path_name: '%s' to '%s','%s'.",path,dir,name);
}


//����������
//  ��һ�ε���frombackup,��������һ��ftp/local�Ѿ�������
int FileParser::commonGetFile(const char *backuppath,bool frombackup,const char *filepatt,SysAdmin &sp,int tabid,int datapartid,bool checkonly)
{
    char localfile[300];
    AutoMt mt(dbc,10);

    // lgprintf("commonGetFile : bgein to list file \n");

    if(frombackup && !fm.listhasopen() ) {
        char backfilepatt[300];
        sprintf(backfilepatt,"%s_%s",fix_cur_pach_md5,filepatt);

        if(strcmp(backfilepatt+strlen(backfilepatt)-4,".ack")==0)
            backfilepatt[strlen(backfilepatt)-4]=0;

        // BEGIN: fix DMA-447,20130109
        if(strcmp(backfilepatt+strlen(backfilepatt)-3,".gz")!=0)
            strcat(backfilepatt,".gz");
        // END : fix DMA-447

        //>> Begin: fix DM-263
        fm.SetIsBackupDir(frombackup);      // ��¼��ǰĿ¼�Ƿ���backupĿ¼
        //<< End:fix Dm-263

        fm.listlocal(backuppath,backfilepatt);
    } else if(!fm.listhasopen() && strlen(pWrapper->getOption("hadoop:namenode",""))>1) {
        //hadoop list file
        fm.listHdfs(pWrapper->getOption("hadoop:namenode","default"),
                    pWrapper->getOption("hadoop:port",0),
                    pWrapper->getOption("hadoop:path","/tmp"),
                    filepatt);

        fm.SetSkipBackup(pWrapper->getOption("files:skipbackup",0)==1);

        //>> Begin: fix DM-263
        fm.SetIsBackupDir(frombackup);      // ��¼��ǰĿ¼�Ƿ���backupĿ¼
        //<< End:fix Dm-263
    } else if(!fm.listhasopen() && strlen(pWrapper->getOption("ftp:host",""))<1) {
        //�����ļ�ģʽ
        fm.listlocal(fix_localpath,filepatt);
        fm.SetSkipPickup(pWrapper->getOption("files:skippickup",0)==1);
        fm.SetSkipBackup(pWrapper->getOption("files:skipbackup",0)==1);

        //>> Begin: fix DM-263
        fm.SetIsBackupDir(frombackup);      // ��¼��ǰĿ¼�Ƿ���backupĿ¼
        //<< End:fix Dm-263
    } else if(!fm.listhasopen()) {
        //>> Begin: fix DM-263
        fm.SetIsBackupDir(frombackup);      // ��¼��ǰĿ¼�Ƿ���backupĿ¼
        //<< End:fix Dm-263

        //FTPģʽ
        fm.SetAscMode(pWrapper->getOption("ftp:textmode",1)==1);
        fm.SetSkipBackup(pWrapper->getOption("files:skipbackup",0)==1);

        //>> Begin: FTP get �ļ�ʧ�ܺ�,��������5�Σ�ÿ�εȴ�2���� ,modify by liujs
        int ftpListTimes = 0;
        while(1) {
            try {
                ftpListTimes++; // FTP List times

                fm.list(pWrapper->getOption("ftp:host",""),
                        pWrapper->getOption("ftp:username",""),
                        pWrapper->getOption("ftp:password",""),
                        fix_ftp_path,filepatt);

                // list �ļ�������δ�׳��쳣
                break;
            } catch(char* e) {
                if(ftpListTimes <= FTP_GET_LIST_RETRY_TIMES) {
                    lgprintf("FTP LIST �ļ�ʧ�ܣ��ȴ�1���Ӻ��������,FTP LISTִ�д���(%d),��ǰ����(%d)",FTP_GET_LIST_RETRY_TIMES,ftpListTimes);
                    sleep(FTP_OPER_FAIL_SLEEP_SEC);// 2 minutes
                    fm.FTPDisconnect();
                    continue;
                } else {
                    throw e;
                }
            } catch(...) {
                ThrowWith("FTP LIST �ļ��б�ʧ�ܣ�δ֪����");
            }
        }// end while
        //<< End: modify by liujs
    }
    fm.SetLocalPath(pWrapper->getOption("files:localpath","./"));

    //lgprintf("commonGetFile : finish to list file \n");

    bool ec=wociIsEcho();
    wociSetEcho(pWrapper->getOption("files:verbose",0)==1);
    while(fm.getnextfile(localfile,fix_filename)) {
        char realfile[300];
        char realfile_bk[300];
        strcpy(realfile,localfile);
        if(strcmp(realfile+strlen(realfile)-4,".ack")==0)
            realfile[strlen(realfile)-4]=0;

        // ��¼���ļ�����,dma-941ʹ�õ�
        if(strcmp(fix_filename+strlen(fix_filename)-4,".ack")==0)
            fix_filename[strlen(fix_filename)-4]=0;

        // �㽭�ƶ�DMA-868
        strcpy(realfile_bk, realfile);
        if(strcmp(realfile+strlen(realfile)-3,".gz")==0) {
            realfile[strlen(realfile)-3] = 0;
        }


        //>> begin: fix dma-1315:��ȡ��·�����ļ�������·��md5ֵ
        if(!fm.GetIsBackupDir()) { // �������backupĿ¼�����ļ�,���ļ�����ǰ��MD5
            char _md5_fn[256];
            sprintf(_md5_fn,"%s_%s",fix_cur_pach_md5,realfile);
            strcpy(realfile,_md5_fn);
        }
        //<< end: fix dma-1315:��ȡ��·�����ļ�������·��md5ֵ


        // crcǰ��ȥ��.gz ��չ����
        fileseq=GetFileSeq(realfile);

        //Begin: crc������ţ�crc��Ų�У��
        if(pWrapper->getOption("files:crcsequence",0)==0) {
            if(fileseq<=0) ThrowWith("�ļ���Ŵ���(%d):%s",fileseq,localfile);
        }
        //End:src�������

        // fix dm-210,add filename query
        if(!GetDumpfileForHoleTable()){
            mt.FetchAll("select filename from dp.dp_filelog where tabid=%d and datapartid=%d and fileseq=%d and filename = '%s'",
                    tabid,datapartid,fileseq,realfile);
        }else{
            mt.FetchAll("select filename from dp.dp_filelog where tabid=%d and fileseq=%d and filename = '%s'",
                    tabid,fileseq,realfile);
        }

        if(mt.Wait()>0) {//�Ѿ��д����¼
            // lgprintf("�ļ����(%d)���������ļ�'%s' ---'%s' ���к��ظ�!",fileseq,mt.PtrStr("filename",0),realfile_bk);
            char _cur_file_name[300];
            sprintf(_cur_file_name,"%s/%s",fm.GetPath(),realfile_bk);
            fm.SetCurrentFile(_cur_file_name);
            if(!fm.GetIsBackupDir()) {
                #ifdef DEBUG
                lgprintf("�ļ����(%d)���������ļ�'%s' ---'%s' ���к��ظ�!",fileseq,mt.PtrStr("filename",0),realfile_bk);
                #endif

                // ADD BY LIUJS
                // ��������ʵ��:�ڵ�����ȡ����ֻ���ڵ���dpadmin���̲���ִ�е�ʱ��,ȷʵ�б�Ҫִ��
                // ��Ϊ����dpadmin�ɼ����ݶ���,�ɼ����������ļ���Ӧ���ڴ�����
                // fm.DeleteSrcFile();  // fix dma-1796: ���dpadmin�ɼ���ͬ����Դ�������,�Ὣ�������̲ɼ����ļ�ɾ����

            }
            
            continue;
        }

        char backfilename[300];
        backfilename[0]=0;
        if(!fm.GetIsBackupDir()) {
            sprintf(backfilename,"%s/%s",backuppath,realfile);
        }
        AutoStmt st(dbc);
        try {
            if(!checkonly) {
                char temp[300] ;
                if(backfilename[0] != 0) {
                    sprintf(temp,"%s.gz",backfilename);
                } else {
                    sprintf(temp,"%s/%s.gz",backuppath,realfile);
                }

                st.Prepare("insert into dp.dp_filelog(tabid,datapartid,slaveridx ,"
                           " filename,bakfilename,status,fileseq ,beginproctime,endproctime,recordnum ,"
                           " filesize,errrows ,errfname,ignorerows) values("
                           "%d,%d,%d,'%s','%s',0,%d,sysdate(),null,0,0,0,'NULL',0)",
                           tabid,datapartid,slaver_index,realfile,temp,fileseq);

                st.Execute(1);
                st.Wait();
            }
        } catch(...) {
            //��һ�������ڴ�������ļ���ֻ����ȷд��dp_filelog�Ľ��̿��Դ����ļ�
            continue;
        }

        bool filesuc=true;
        if(!checkonly) {
            //>> Begin: FTP get �ļ�ʧ�ܺ�,��������5�Σ�ÿ�εȴ�2���� ,modify by liujs
            int ftpGetTimes = 0;
            while(1) {
                try {
                    ftpGetTimes++;
                    // warning : DM-263 , �����ڴ˴�����
                    // fm.SetIsBackupDir(frombackup);      // ��¼��ǰĿ¼�Ƿ���backupĿ¼

                    bool _rm_file = !frombackup;// �Ƿ�ɾ���ļ�����
                    fm.SetBackupDir(backuppath);
                    fm.getfile(localfile,backfilename,_rm_file,sp,tabid,datapartid);
                    // get �ļ�������Ϊ�׳��쳣
                    break;
                } catch(char* e) {
                    if(ftpGetTimes <= FTP_GET_LIST_RETRY_TIMES) {
                        lgprintf("%s�ļ�ʧ�ܣ��ȴ�1���Ӻ��������,%sִ�д���(%d),��ǰ����(%d)",fm._get_file_title().c_str(),
                                 fm._get_file_title().c_str(),FTP_GET_LIST_RETRY_TIMES,ftpGetTimes);
                        sleep(FTP_OPER_FAIL_SLEEP_SEC);// 2 minutes
                        fm.FTPDisconnect();
                        continue;
                    } else {
                        // throw e;
                        break; // fix dma-1196
                    }
                } catch(...) {
                    ThrowWith("%s �ļ�ʧ�ܣ�δ֪����", fm._get_file_title().c_str());
                }
            }// END while
            //<< End: modify by liujs

            // fix dma-1196
            if(ftpGetTimes >=FTP_GET_LIST_RETRY_TIMES) {
                lgprintf("==>ERROR<==��ȡ�ļ�[%s]ʧ��,�������ļ����л�ȡ��һ���ļ�.",localfile);

                // ɾ����Ч��¼,fix dma-1197
                st.DirectExecute("delete from  dp.dp_filelog  where tabid = %d and datapartid =%d and status = 0 and fileseq=%d",
                                 tabid,datapartid,fileseq);

                continue;
            }

            basefilename=localfile;  // ������·�����ļ�����,��������ļ�����ʹ��
            filename=fm.getlocalfile(localfile);
            if(!filesuc) {
                //���Դ���ֱ���޸�Ϊ����������ύ״̬
                st.DirectExecute("update dp.dp_filelog set status=2 where tabid=%d and datapartid=%d and slaveridx = %d and status=0"
                                 ,tabid,datapartid,slaver_index);
            }
        }

        // �����ļ���С
        st.DirectExecute("update dp.dp_filelog set filesize=%ld where tabid=%d and datapartid=%d and fileseq=%d",
                         fm.GetCurFileSize(),tabid,datapartid,fileseq);

        wociSetEcho(ec);
        return filesuc?1:2;
    }
    wociSetEcho(ec);
    return 0;
}


FileParser::FileParser()
{
    fp=NULL;
    fix_ftp_path[0]=0;
    fix_localpath[0]=0;
    fix_cur_pach_md5[0]=0;
    fix_filepatt[0]=0;
    fix_filename[0]=0;
    current_path_index = -1;
    str_path_vector.clear();
    line = NULL;
    line=new char[MAXLINELEN];

    slaver_index=0;             // slaver�ڵ���±�
}
//������������ڶ��������������Ա����֮ǰ����
void FileParser::SetTable(int _dbc,int _tabid,int _datapartid,IDumpFileWrapper *pdfw)
{
    dbc=_dbc;
    tabid=_tabid;
    fp=NULL;
    pWrapper=pdfw;
    datapartid=_datapartid;
    fileseq=0;
}

int FileParser::GetForceEndDump(){
    return pWrapper->getOption("files:forceenddump",0);
}


// ���ö�ڵ�ɼ������õĻỰid,�ڵ�index
void FileParser::SetSession(const int slaverIndex)
{
    slaver_index=slaverIndex;
}


// fill filename member,the wrapper check conflict concurrent process,
// if one has privillege to process current file ,then invoke DoParse
// ����ֵ��
//  -1 ������������������״̬�Ѿ��������������޸�Ϊȡ��������ͣ
//  0�������ļ�
//  1��ȡ����Ч���ļ�
//int  FileParser::GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly) {return 0;};
// if end ,update task to next step(with lock)
// critical function !
// checked every file parser completed.
// �����ǲ������ļ���Ҫ�����ʱ�򣬲��ܵ��������麯��
//bool FileParser::ExtractEnd() {return true;};
int FileParser::GetIdleSeconds()
{
    return pWrapper->getOption("files:seekinterval", 5);
}
// return value:
//  -1: a error occurs.�����ڴ���ֶνṹ��ƥ��
//  0:memory table has been filled full,and need to parse again
//  1:file parse end
//while parser file ended,this function record file process log and
// sequence of file in dp dictions tab.
//int FileParser::DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) {return -1;};
//int FileParser::GetFileSeq(const char *filename) {return 1;}

FileParser::~FileParser()
{
    if(fp) fclose(fp);
    if(line != NULL) {
        delete []line;
        line = NULL;
    }
    fix_ftp_path[0]=0;
    fix_cur_pach_md5[0]=0;
    fix_localpath[0]=0;
    fix_filepatt[0]=0;
    fix_filename[0]=0;
    current_path_index = -1;
    str_path_vector.clear();
}

#ifndef PLUGIN_VERSION
#define PLUGIN_VERSION "plugin 2.0.0"
#endif
void FileParser::PluginPrintVersion(){
    lgprintf("PLUGIN-[%s]",PLUGIN_VERSION);
}

// ����Ƿ�������Ϊͳһ��λ����һ��ɼ�����
// 1> [default: false]���������ɼ�����,dp.dp_filelog�������ǵ��������ֵ�
// 2> [true]����ɼ�����,���DMA-1448����,ͨ��[files::dumpfileforholetable]���л�ȡ
bool FileParser::GetDumpfileForHoleTable(){
    return pWrapper->getOption("files:dumpfileforholetable",0) == 1;
}


