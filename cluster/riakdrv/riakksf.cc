#include <sys/file.h>

#ifndef BUILD_RIAKKSF_TOOL
#include <riakdrv/riakksf.h>
#else

#include "riakksf.h"
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>

#ifndef DPRINTFX
#define DPRINTFX
#endif
#ifndef DPRINTF
#define DPRINTF printf
#endif

#endif

#define KEY_SPACE_ALLOC_LOG     "KeySpaceAlloc.log"

void key_space_file_timeout_recover()
{
    char* p_riak_space_dir = getenv(ENV_KEY_SPACE_LOCK_DIR);
    if(NULL == p_riak_space_dir) {
        DPRINTF("warning: getenv(RIAK_KEY_SPACE_DIR) return NULL \n");
        return ;
    }

    std::string str_path(p_riak_space_dir);
    RiakKSAlloc _rs(str_path);
    _rs.TimeoutKeySpaceRecover();
}

void key_space_alloc_file_export()
{
    char* p_riak_space_dir = getenv(ENV_KEY_SPACE_LOCK_DIR);
    if(NULL == p_riak_space_dir) {
        DPRINTF("warning: getenv(RIAK_KEY_SPACE_DIR) return NULL \n");
        return ;
    }

    std::string str_path(p_riak_space_dir);
    RiakKSAlloc _rs(str_path);
    _rs.AllocFileKeySpaceExport();
}

void key_space_unsolid_file_export()
{
    char* p_riak_space_dir = getenv(ENV_KEY_SPACE_LOCK_DIR);
    if(NULL == p_riak_space_dir) {
        DPRINTF("warning: getenv(RIAK_KEY_SPACE_DIR) return NULL \n");
        return ;
    }

    std::string str_path(p_riak_space_dir);
    RiakKSAlloc _rs(str_path);
    _rs.SolidFileKeySpaceExport();
}


int64_t
key_space_file_alloc(const std::string &bucket,const int tabid,const int attrid)
{
    char* p_riak_space_dir = getenv(ENV_KEY_SPACE_LOCK_DIR);
    if(NULL == p_riak_space_dir) {
        DPRINTF("warning: getenv(RIAK_KEY_SPACE_DIR) return NULL \n");
        return -1;
    }

    std::string str_path(p_riak_space_dir);
    RiakKSAlloc _rs(str_path);
    int64_t ret_riak_ks = _rs.AllocKeySpace(tabid,attrid);

    char key_space_log[300];
    sprintf(key_space_log,"%s/%s",p_riak_space_dir,KEY_SPACE_ALLOC_LOG);

    // log:
    FILE* pfile = NULL;
    pfile = fopen(key_space_log,"a+t");
    assert(pfile != NULL);

    char str_time[32];
    time_t t;
    struct tm *tmp;
    t = time(NULL);
    tmp = localtime(&t);
    strftime(str_time, sizeof(str_time), "%F %T", tmp);

    fprintf(pfile,"%s: [ALLOC_KEY:<tabid:%d><attrid:%d><keyspace:%ld>]\n",str_time,tabid,attrid,(long)ret_riak_ks);
    fclose(pfile);
    pfile=NULL;

    if(RIAK_KS_MAX>ret_riak_ks) {
        return ret_riak_ks;
    }

    return -1;
}

int
key_space_file_free(const std::string &bucket,const int tabid,const int attrid,const int64_t ks_id)
{
    char* p_riak_space_dir = getenv(ENV_KEY_SPACE_LOCK_DIR);
    if(NULL == p_riak_space_dir) {
        DPRINTF("warning: getenv(ENV_KEY_SPACE_LOCK_DIR) return NULL \n");
        return -1;
    }

    std::string str_path(p_riak_space_dir);
    RiakKSAlloc _rs(str_path);
    _rs.FreeKeySpace(tabid,attrid,ks_id);

    char key_space_log[300];
    sprintf(key_space_log,"%s/%s",p_riak_space_dir,KEY_SPACE_ALLOC_LOG);

    // log:
    FILE* pfile = NULL;
    pfile = fopen(key_space_log,"a+t");
    assert(pfile != NULL);

    char str_time[32];
    time_t t;
    struct tm *tmp;
    t = time(NULL);
    tmp = localtime(&t);
    strftime(str_time, sizeof(str_time), "%F %T", tmp);

    fprintf(pfile,"%s: [FREE_KEY:<tabid:%d><attrid:%d><keyspace:%ld>]\n",str_time,tabid,attrid,(long)ks_id);
    fclose(pfile);
    pfile=NULL;

    return 0;
}

int
key_space_file_solidify(const std::string &bucket,const int tabid,const int attrid,const int64_t ks_id)
{
    char* p_riak_space_dir = getenv(ENV_KEY_SPACE_LOCK_DIR);
    if(NULL == p_riak_space_dir) {
        DPRINTF("warning: getenv(ENV_KEY_SPACE_LOCK_DIR) return NULL \n");
        return -1;
    }

    std::string str_path(p_riak_space_dir);
    RiakKSAlloc _rs(str_path);
    _rs.SolidKeySpace(tabid,attrid,ks_id);

    char key_space_log[300];
    sprintf(key_space_log,"%s/%s",p_riak_space_dir,KEY_SPACE_ALLOC_LOG);

    // log:
    FILE* pfile = NULL;
    pfile = fopen(key_space_log,"a+t");
    assert(pfile != NULL);

    char str_time[32];
    time_t t;
    struct tm *tmp;
    t = time(NULL);
    tmp = localtime(&t);
    strftime(str_time, sizeof(str_time), "%F %T", tmp);

    fprintf(pfile,"%s: [SOLID_KEY:<tabid:%d><attrid:%d><keyspace:%ld>]\n",str_time,tabid,attrid,(long)ks_id);
    fclose(pfile);
    pfile=NULL;

    return 0;
}

//------------------------------------------------------------------------------
RiakKSAlloc::RiakKSAlloc(const std::string path)
{
    _riak_ks_file = path;
    _riak_ks_file += "/";
    _riak_ks_file += RIAK_KS_ALLOC_FILE;

    _riak_lock_file = path;
    _riak_lock_file += "/";
    _riak_lock_file += RIAK_KS_LOCK_FILE;

    _riak_solid_file = path;
    _riak_solid_file += "/";
    _riak_solid_file += RIAK_KS_SOLID_FILE;

    _riak_ks_file_bak = _riak_ks_file;
    _riak_ks_file_bak += "_bak";
    _riak_solid_file_bak= _riak_solid_file;
    _riak_solid_file_bak += "_bak";
}

RiakKSAlloc::~RiakKSAlloc()
{
    _riak_ks_file ="";
    _riak_lock_file  = "";
    _riak_solid_file = "";
}

/*
    分配的时候:
    free_node_num >0 , 分配free_nodex
    free_node_num =0 , 分配alloc_max_node+=4096,free_node_num--;

    释放的时候:
    free_node_num ++; 添加free_nodex
*/
int64_t RiakKSAlloc::AllocKeySpace(const int tab_num,const int attr_num)
{
    FILE* pf_lock = NULL;
    FILE* pf_ks = NULL;
    int ret  = 0;
    int64_t ret_riak_ks = 0;
    ret_riak_ks |= RIAK_KS_RESERVE;
    while(1) { // 打开文件
        pf_lock = fopen(_riak_lock_file.c_str(),"wb");
        if(pf_lock == NULL) {
            DPRINTFX(30,"File exist : Open file %s return NULL,please wait .... \n",_riak_lock_file.c_str());
            usleep(10000);
            continue;
        }
        if(flock(fileno(pf_lock), LOCK_EX)==-1) {
            fclose(pf_lock);
            throw "Lock file error!";
        }

        int64_t ks_free_number = 0;
        int64_t ks_max = 0;
        struct stat stat_info;
        if(0 != stat(_riak_ks_file.c_str(), &stat_info)) { // do not exist : RIAK_KS_ALLOC_FILE
            ks_free_number = 0;
            ks_max = 0;

            pf_ks = fopen(_riak_ks_file.c_str(),"wb");
            if(pf_ks == NULL) {
                DPRINTFX(30,"File not exist : Open file %s return NULL\n",_riak_ks_file.c_str());
                assert(0);
            }

            ret_riak_ks = 0;
            ret_riak_ks |= RIAK_KS_RESERVE;
            ks_max = ret_riak_ks;
            ret = fwrite(&ks_max,sizeof(ks_max),1,pf_ks);
            assert(ret == 1);

            ret = fwrite(&ks_free_number,sizeof(ks_free_number),1,pf_ks);
            assert(ret == 1);
        } else { // exist : RIAK_KS_ALLOC_FILE
            pf_ks = fopen(_riak_ks_file.c_str(),"rb+");  // read/write
            if(pf_ks == NULL) {
                DPRINTFX(30,"File exist : Open file %s return NULL\n",_riak_ks_file.c_str());
                assert(0);
            }
            ret = fread(&ks_max,sizeof(int64_t),1,pf_ks);
            assert(ret == 1);

            ret = fread(&ks_free_number,sizeof(int64_t),1,pf_ks);
            assert(ret == 1);

            if(ks_free_number == 0) {
                ks_max += RIAK_KS_ALLOC_STEMP;
                ret_riak_ks = ks_max;
                ret_riak_ks |= RIAK_KS_RESERVE;
                fseek(pf_ks,0,SEEK_SET);
                ret = fwrite(&ks_max,sizeof(int64_t),1,pf_ks);
                assert(ret == 1);
            } else {
                // get last
                fseek(pf_ks,(8+ks_free_number*8),SEEK_SET);

                ret = fread(&ret_riak_ks,sizeof(int64_t),1,pf_ks);
                assert(ret == 1);

                ret_riak_ks |= RIAK_KS_RESERVE;

                ks_free_number = ks_free_number-1;
                assert(ks_free_number>=0);

                // update free_num
                fseek(pf_ks,8,SEEK_SET);
                ret = fwrite(&ks_free_number,sizeof(int64_t),1,pf_ks);
                assert(ret == 1);
            }
        }

        if(pf_ks) {
            fclose(pf_ks);
            pf_ks = NULL;
        }

        if(RIAK_KS_MAX>ret_riak_ks) {
            AddKS2Solid(tab_num,attr_num,ret_riak_ks);
        }

        copy_file(_riak_ks_file.c_str(),_riak_ks_file_bak.c_str());

        flock(fileno(pf_lock),LOCK_UN);
        fclose(pf_lock);
        pf_lock = NULL;


        break;
    }

    assert(ret_riak_ks%RIAK_KS_ALLOC_STEMP ==0);

    return ret_riak_ks;
}

void RiakKSAlloc::FreeKeySpace(const int tab_num,const int attr_num,const int64_t keyspace)
{
    // 老系统分配的keyspace的过滤
    if(keyspace < RIAK_KS_RESERVE || keyspace > RIAK_KS_MAX ) {
        return ;
    }
    assert(keyspace%RIAK_KS_ALLOC_STEMP ==0);
    FILE* pf_lock = NULL;
    FILE* pf_ks = NULL;
    int ret  = 0;
    while(1) { // 打开文件
        pf_lock = fopen(_riak_lock_file.c_str(),"wb");
        if(pf_lock == NULL) {
            DPRINTFX(30,"File exist : Open file %s return NULL,please wait .... \n",_riak_lock_file.c_str());
            usleep(10000);
            continue;
        }
        if(flock(fileno(pf_lock), LOCK_EX)==-1) {
            fclose(pf_lock);
            throw "Lock file error!";
        }
        int64_t ks_free_number = 0;
        int64_t ks_max = 0;
        struct stat stat_info;
        if(0 != stat(_riak_ks_file.c_str(), &stat_info)) { // do not exist : RIAK_KS_ALLOC_FILE
            DPRINTFX(30,"File do not exist : Open file %s return NULL\n",_riak_lock_file.c_str());
            assert(0);
        } else { // exist : RIAK_KS_ALLOC_FILE
            pf_ks = fopen(_riak_ks_file.c_str(),"rb+");  // read/write
            if(pf_ks == NULL) {
                DPRINTFX(30,"File exist : Open file %s return NULL\n",_riak_ks_file.c_str());
                assert(0);
            }

            // update max ks
            ret = fread(&ks_max,sizeof(int64_t),1,pf_ks);
            assert(ret == 1);
            if(keyspace == ks_max) {
                if(ks_max>0) {
                    ks_max = ks_max-RIAK_KS_ALLOC_STEMP;
                }

                ks_max |= RIAK_KS_RESERVE;

                fseek(pf_ks,0,SEEK_SET);
                ret = fwrite(&ks_max,sizeof(int64_t),1,pf_ks);
                assert(ret == 1);

                // 不能在将keyspace 加入free的列表中,否则再次来alloc的时候,会继续分配重复
                // detail : SUSA-216
            } else if(keyspace > ks_max) {
                DPRINTFX(30,"keyspace(%ld) > keyspace_max(%ld)\n",(long)keyspace,(long)ks_max);
                assert(0);
            } else { // keyspace < ks_max
                // update free num
                ret = fread(&ks_free_number,sizeof(int64_t),1,pf_ks);
                assert(ret == 1);

                // 为避免重复Free引入的重复分配BUG,SASU-407问题
                bool  FreeExitKs = false;
                {
                    for(int i=0; i<ks_free_number; i++) {
                        int64_t _tmp_ks;
                        ret = fread(&_tmp_ks,sizeof(int64_t),1,pf_ks);
                        assert(ret == 1);
                        if(_tmp_ks == keyspace) {
                            DPRINTFX(30,"keyspace(%ld) has free\n",(long)keyspace);
                            FreeExitKs = true;
                            break;
                        }
                    }
                }

                if(!FreeExitKs) {
                    ks_free_number = ks_free_number+1;
                    fseek(pf_ks,8,SEEK_SET);
                    ret = fwrite(&ks_free_number,sizeof(int64_t),1,pf_ks);
                    assert(ret == 1);

                    // update last free
                    fseek(pf_ks,8+ks_free_number*8,SEEK_SET);
                    ret = fwrite(&keyspace,sizeof(int64_t),1,pf_ks);
                    assert(ret == 1);
                }
            }
        }

        if(pf_ks) {
            fclose(pf_ks);
            pf_ks = NULL;
        }

        copy_file(_riak_ks_file.c_str(),_riak_ks_file_bak.c_str());

        flock(fileno(pf_lock),LOCK_UN);
        fclose(pf_lock);
        pf_lock = NULL;

        SolidKeySpace(tab_num,attr_num,keyspace);

        break;
    }
}

int RiakKSAlloc::copy_file(char const * src_path,char const * des_path)
{
    FILE *in,*out;
    char buff[1024];
    int len;
    in = fopen(src_path,"r+");
    out = fopen(des_path,"w+");
    while( (len = fread(buff,1,sizeof(buff),in))>0 ) {
        fwrite(buff,1,len,out);
    }
    fclose(out);
    fclose(in);
    return 0;
}

void RiakKSAlloc::SolidKeySpace(const int tab_num,const int attr_num,const int64_t keyspace)
{
    // 老系统分配的keyspace的过滤
    if(keyspace < RIAK_KS_RESERVE || keyspace > RIAK_KS_MAX) {
        return ;
    }
    assert(keyspace%RIAK_KS_ALLOC_STEMP ==0);
    FILE* pf_lock = NULL;
    FILE* pf_solid = NULL;
    int ret  = 0;
    while(1) { // 打开文件
        pf_lock = fopen(_riak_lock_file.c_str(),"wb");
        if(pf_lock == NULL) {
            DPRINTFX(30,"File exist : Open file %s return NULL,please wait .... \n",_riak_lock_file.c_str());
            usleep(10000);
            continue;
        }
        if(flock(fileno(pf_lock), LOCK_EX)==-1) {
            fclose(pf_lock);
            throw "Lock file error!";
        }
        std::set<stru_solid_node*> solid_node_set;
        int64_t solid_node_num = 0;
        pf_solid = fopen(_riak_solid_file.c_str(),"rb+");  // read/write
        if(pf_solid == NULL) {
            DPRINTFX(30,"File exist : Open file %s return NULL\n",_riak_solid_file.c_str());
            assert(0);
        }

        ret = fread(&solid_node_num,sizeof(solid_node_num),1,pf_solid);
        assert(ret == 1);

        int64_t solid_node_index = 0;
        bool should_solid = false;
        while(solid_node_index<solid_node_num) {
            stru_solid_node * st_s_n_item = (stru_solid_node*)malloc(sizeof(stru_solid_node));
            memset(st_s_n_item,0x0,sizeof(stru_solid_node));

            ret = fread(&st_s_n_item->keyspace,sizeof(int64_t),1,pf_solid);
            assert(ret == 1);

            ret = fread(&st_s_n_item->tab_num,sizeof(int),1,pf_solid);
            assert(ret == 1);

            ret = fread(&st_s_n_item->attr_num ,sizeof(int),1,pf_solid);
            assert(ret == 1);

            ret = fread(&st_s_n_item->alloc_time,sizeof(time_t),1,pf_solid);
            assert(ret == 1);

            if(st_s_n_item->keyspace != keyspace) {
                solid_node_set.insert(st_s_n_item);
                /* // 只能手动的完成如下部分功能,不能自动完成!
                time_t tm_now = time(NULL);
                float time_diff = difftime(tm_now,st_s_n_item->alloc_time);
                if(time_diff < RIAK_KS_SOLID_TIME_OUT){ // 24小时内的保留
                    solid_node_set.insert(st_s_n_item);
                }
                else{
                    should_solid = true;
                }
                */
            } else {
                should_solid = true;
                if(st_s_n_item != NULL) {
                    free(st_s_n_item);
                    st_s_n_item= NULL;
                }
            }

            solid_node_index++;
        }

        std::set<stru_solid_node*>::iterator iter ;
        if(should_solid) {
            solid_node_num = solid_node_set.size();
            fseek(pf_solid,0L,SEEK_SET);
            ret =  fwrite(&solid_node_num,sizeof(solid_node_num),1,pf_solid);
            assert(ret == 1);

            for(iter = solid_node_set.begin(); iter!=solid_node_set.end(); iter++) {
                stru_solid_node* pnode = (stru_solid_node*)(*iter);

                ret = fwrite(&(pnode->keyspace),sizeof(int64_t),1,pf_solid);
                assert(ret == 1);

                ret = fwrite(&(pnode->tab_num),sizeof(int),1,pf_solid);
                assert(ret == 1);

                ret = fwrite(&(pnode->attr_num),sizeof(int),1,pf_solid);
                assert(ret == 1);

                ret = fwrite(&(pnode->alloc_time),sizeof(time_t),1,pf_solid);
                assert(ret == 1);
            }
        }

        for(iter = solid_node_set.begin(); iter!=solid_node_set.end(); iter++) {
            stru_solid_node* pnode = (stru_solid_node*)(*iter);
            free(pnode);
            pnode = NULL;
        }
        solid_node_set.clear();


        if(pf_solid) {
            fclose(pf_solid);
            pf_solid = NULL;
        }

        copy_file(_riak_solid_file.c_str(),_riak_solid_file_bak.c_str());

        flock(fileno(pf_lock),LOCK_UN);
        fclose(pf_lock);
        pf_lock = NULL;

        break;
    }

}

// 只能手动的完成如下部分功能,不能自动完成!
void RiakKSAlloc::TimeoutKeySpaceRecover()
{
    char* p_riak_space_dir = getenv(ENV_KEY_SPACE_LOCK_DIR);
    if(NULL == p_riak_space_dir) {
        DPRINTF("warning: getenv(ENV_KEY_SPACE_LOCK_DIR) return NULL \n");
        return;
    }

    FILE* pf_lock = NULL;
    FILE* pf_solid = NULL;
    int ret  = 0;
    while(1) { // 打开文件
        pf_lock = fopen(_riak_lock_file.c_str(),"wb");
        if(pf_lock == NULL) {
            DPRINTF("File exist : Open file %s return NULL,please wait .... \n",_riak_lock_file.c_str());
            usleep(10000);
            continue;
        }
        if(flock(fileno(pf_lock), LOCK_EX)==-1) {
            fclose(pf_lock);
            throw "Lock file error!";
        }
        std::set<stru_solid_node*> solid_node_set;
        std::set<stru_solid_node*> timeout_solid_node_set;  // add by liujs  at 2019/04/11, need free keyspace
        int64_t solid_node_num = 0;
        pf_solid = fopen(_riak_solid_file.c_str(),"rb+");  // read/write
        if(pf_solid == NULL) {
            DPRINTF("File exist : Open file %s return NULL\n",_riak_solid_file.c_str());
            assert(0);
        }

        ret = fread(&solid_node_num,sizeof(solid_node_num),1,pf_solid);
        assert(ret == 1);

        int64_t solid_node_index = 0;
        while(solid_node_index<solid_node_num) {
            stru_solid_node * st_s_n_item = (stru_solid_node*)malloc(sizeof(stru_solid_node));
            memset(st_s_n_item,0x0,sizeof(stru_solid_node));

            ret = fread(&st_s_n_item->keyspace,sizeof(int64_t),1,pf_solid);
            assert(ret == 1);

            ret = fread(&st_s_n_item->tab_num,sizeof(int),1,pf_solid);
            assert(ret == 1);

            ret = fread(&st_s_n_item->attr_num ,sizeof(int),1,pf_solid);
            assert(ret == 1);

            ret = fread(&st_s_n_item->alloc_time,sizeof(time_t),1,pf_solid);
            assert(ret == 1);

            time_t tm_now = time(NULL);
            float time_diff = difftime(tm_now,st_s_n_item->alloc_time);
            if(time_diff < RIAK_KS_SOLID_TIME_OUT) { // 24小时内的保留
                solid_node_set.insert(st_s_n_item);
            } else { // add by liujs ,2019/04/11 , 此部分需要对Keyspace进行释放回收操作
                timeout_solid_node_set.insert(st_s_n_item);
            }
            solid_node_index++;
        }

        std::set<stru_solid_node*>::iterator iter ;
        if(!solid_node_set.empty()) {
            solid_node_num = solid_node_set.size();
            fseek(pf_solid,0L,SEEK_SET);
            ret =  fwrite(&solid_node_num,sizeof(solid_node_num),1,pf_solid);
            assert(ret == 1);

            for(iter = solid_node_set.begin(); iter!=solid_node_set.end(); iter++) {
                stru_solid_node* pnode = (stru_solid_node*)(*iter);

                ret = fwrite(&(pnode->keyspace),sizeof(int64_t),1,pf_solid);
                assert(ret == 1);

                ret = fwrite(&(pnode->tab_num),sizeof(int),1,pf_solid);
                assert(ret == 1);

                ret = fwrite(&(pnode->attr_num),sizeof(int),1,pf_solid);
                assert(ret == 1);

                ret = fwrite(&(pnode->alloc_time),sizeof(time_t),1,pf_solid);
                assert(ret == 1);
            }
        }

        for(iter = solid_node_set.begin(); iter!=solid_node_set.end(); iter++) {
            stru_solid_node* pnode = (stru_solid_node*)(*iter);
            free(pnode);
            pnode = NULL;
        }
        solid_node_set.clear();

        if(pf_solid) {
            fclose(pf_solid);
            pf_solid = NULL;
        }
        flock(fileno(pf_lock),LOCK_UN);
        fclose(pf_lock);
        pf_lock = NULL;

        //  add by liujs ,2019/04/11 , 此部分需要对Keyspace进行释放回收操作
        {

            char key_space_log[300];
            sprintf(key_space_log,"%s/%s",p_riak_space_dir,KEY_SPACE_ALLOC_LOG);
            
            // log:
            FILE* pfile = NULL;
            pfile = fopen(key_space_log,"a+t");
            assert(pfile != NULL);
            
            char str_time[32];
            time_t t;
            struct tm *tmp;
            t = time(NULL);
            tmp = localtime(&t);
            strftime(str_time, sizeof(str_time), "%F %T", tmp);
       
            int _idx = 1;
            for(iter = timeout_solid_node_set.begin(); iter!=timeout_solid_node_set.end(); iter++) {
                stru_solid_node* pnode = (stru_solid_node*)(*iter);

                struct tm tm1;
                localtime_r(&pnode->alloc_time, &tm1 );
                char szTime[64]= {0};
                sprintf( szTime, "%4.4d-%2.2d-%2.2d %2.2d:%2.2d:%2.2d",
                         tm1.tm_year+1900, tm1.tm_mon+1, tm1.tm_mday,
                         tm1.tm_hour, tm1.tm_min,tm1.tm_sec);

                // 逐个超时节点回收
                DPRINTF("%d\tFree timeout solid node  <t,a,k,alloc_time>(%d,%d,%ld[0x%lx],%s)\n",
                         _idx++,pnode->tab_num,pnode->attr_num,pnode->keyspace,pnode->keyspace,szTime);

                fprintf(pfile,"%s: [RECOVER_UNSOLID_KEY:<%d><tabid:%d><attrid:%d><keyspace:%ld[0x%lx]><alloc_time:%s>]\n",
                    str_time,_idx,pnode->tab_num,pnode->attr_num,pnode->keyspace,pnode->keyspace,szTime);
                
                FreeKeySpace(pnode->tab_num,pnode->attr_num,pnode->keyspace);

                free(pnode);
                pnode = NULL;
            }
            timeout_solid_node_set.clear();

            fclose(pfile);
            pfile=NULL;
        }

        break;
    }


}

void RiakKSAlloc:: AllocFileKeySpaceExport()       // 导出Alloc文件
{
    FILE* pf_lock = NULL;
    FILE* pf_ks = NULL;
    int ret  = 0;
    while(1) { // 打开文件
        pf_lock = fopen(_riak_lock_file.c_str(),"wb");
        if(pf_lock == NULL) {
            DPRINTFX(30,"File exist : Open file %s return NULL,please wait .... \n",_riak_lock_file.c_str());
            usleep(10000);
            continue;
        }
        if(flock(fileno(pf_lock), LOCK_EX)==-1) {
            fclose(pf_lock);
            throw "Lock file error!";
        }

        int64_t ks_free_number = 0;
        int64_t ks_max = 0;
        std::set<int64_t>  check_set;
        struct stat stat_info;
        if(0 != stat(_riak_ks_file.c_str(), &stat_info)) { // do not exist : RIAK_KS_ALLOC_FILE
            DPRINTF("File exist : Open file %s return NULL \n",_riak_ks_file.c_str());
        } else { // exist : RIAK_KS_ALLOC_FILE
            pf_ks = fopen(_riak_ks_file.c_str(),"rb+");  // read/write
            ret = fread(&ks_max,sizeof(int64_t),1,pf_ks);
            assert(ret == 1);
            DPRINTF("ks_max = %ld<0x%lx> \n",ks_max,ks_max);

            ret = fread(&ks_free_number,sizeof(int64_t),1,pf_ks);
            assert(ret == 1);
            DPRINTF("ks_free_number = %ld \n",ks_free_number);

            for(int i=0; i<ks_free_number; i++) {
                int64_t _tmp_ks;
                ret = fread(&_tmp_ks,sizeof(int64_t),1,pf_ks);
                check_set.insert(_tmp_ks);
                assert(ret == 1);
                DPRINTF("<%d\t%ld[0x%lx]> \n",i+1,_tmp_ks,_tmp_ks);
            }
            int  check_set_size = check_set.size();
            if(check_set_size!= ks_free_number) {
                DPRINTF("!!!!!!!!!!! : vcheck_set_size = %d , ks_free_number=%ld\n",check_set_size,ks_free_number);
            }
        }
        check_set.clear();
        if(pf_ks) {
            fclose(pf_ks);
            pf_ks = NULL;
        }

        flock(fileno(pf_lock),LOCK_UN);
        fclose(pf_lock);
        pf_lock = NULL;
        break;
    }

}
void RiakKSAlloc:: SolidFileKeySpaceExport()      // 导出Solid文件
{
    FILE* pf_lock = NULL;
    FILE* pf_solid = NULL;
    int ret  = 0;
    while(1) { // 打开文件
        pf_lock = fopen(_riak_lock_file.c_str(),"wb");
        if(pf_lock == NULL) {
            DPRINTF("File exist : Open file %s return NULL,please wait .... \n",_riak_lock_file.c_str());
            usleep(10000);
            continue;
        }
        if(flock(fileno(pf_lock), LOCK_EX)==-1) {
            fclose(pf_lock);
            throw "Lock file error!";
        }
        std::set<stru_solid_node*> solid_node_set;
        int64_t solid_node_num = 0;
        pf_solid = fopen(_riak_solid_file.c_str(),"rb+");  // read/write
        if(pf_solid == NULL) {
            DPRINTF("File exist : Open file %s return NULL\n",_riak_solid_file.c_str());
            return;
        }

        ret = fread(&solid_node_num,sizeof(solid_node_num),1,pf_solid);
        assert(ret == 1);
        DPRINTF("solid_node_num = %ld \n",solid_node_num);

        int64_t solid_node_index = 0;
        //bool should_solid = false;
        while(solid_node_index<solid_node_num) {
            stru_solid_node * st_s_n_item = (stru_solid_node*)malloc(sizeof(stru_solid_node));
            memset(st_s_n_item,0x0,sizeof(stru_solid_node));

            ret = fread(&st_s_n_item->keyspace,sizeof(int64_t),1,pf_solid);
            assert(ret == 1);

            ret = fread(&st_s_n_item->tab_num,sizeof(int),1,pf_solid);
            assert(ret == 1);

            ret = fread(&st_s_n_item->attr_num ,sizeof(int),1,pf_solid);
            assert(ret == 1);

            ret = fread(&st_s_n_item->alloc_time,sizeof(time_t),1,pf_solid);
            assert(ret == 1);

            solid_node_index++;

            solid_node_set.insert(st_s_n_item);

        }

        std::set<stru_solid_node*>::iterator iter ;
        int idx = 0;
        for(iter = solid_node_set.begin(); iter!=solid_node_set.end(); iter++) {
            stru_solid_node* pnode = (stru_solid_node*)(*iter);

            struct tm tm1;
            localtime_r(&pnode->alloc_time, &tm1 );
            char szTime[64]= {0};            
            sprintf( szTime, "%4.4d-%2.2d-%2.2d %2.2d:%2.2d:%2.2d",
                         tm1.tm_year+1900, tm1.tm_mon+1, tm1.tm_mday,
                         tm1.tm_hour, tm1.tm_min,tm1.tm_sec);

            // 逐个超时节点回收
            DPRINTF("%d\t<t,a,k,alloc_time>(%d,%d,%ld[0x%lx],%s)\n",++idx,
                     pnode->tab_num,pnode->attr_num,pnode->keyspace,pnode->keyspace,szTime);

        }

        int  check_set_size = solid_node_set.size();
        if(check_set_size!= solid_node_num) {
            DPRINTF("!!!!!!!!!!! : vcheck_set_size = %d , solid_node_num=%ld\n",check_set_size,solid_node_num);
        }

        for(iter = solid_node_set.begin(); iter!=solid_node_set.end(); iter++) {
            stru_solid_node* pnode = (stru_solid_node*)(*iter);
            free(pnode);
            pnode = NULL;
        }
        solid_node_set.clear();

        if(pf_solid) {
            fclose(pf_solid);
            pf_solid = NULL;
        }

        flock(fileno(pf_lock),LOCK_UN);
        fclose(pf_lock);
        pf_lock = NULL;

        break;
    }


}


void RiakKSAlloc::AddKS2Solid(const int tab_num,const int attr_num,const int64_t keyspace)
{
    assert(keyspace%RIAK_KS_ALLOC_STEMP ==0);
    FILE* pf_solid = NULL;
    int ret  = 0;

    int64_t solid_node_num = 0;
    stru_solid_node st_s_n_item;
    st_s_n_item.keyspace = keyspace;
    st_s_n_item.keyspace |= RIAK_KS_RESERVE;
    st_s_n_item.tab_num = tab_num;
    st_s_n_item.attr_num = attr_num;
    st_s_n_item.alloc_time = time(NULL);

    struct stat stat_info;
    if(0 != stat(_riak_solid_file.c_str(), &stat_info)) { // do not exist : _riak_solid_file
        pf_solid = fopen(_riak_solid_file.c_str(),"wb");
        if(pf_solid == NULL) {
            DPRINTFX(30,"File not exist : Open file %s return NULL\n",_riak_solid_file.c_str());
            assert(0);
        }
        solid_node_num = solid_node_num+1;
        ret =  fwrite(&solid_node_num,sizeof(solid_node_num),1,pf_solid);
        assert(ret == 1);

        ret = fwrite(&st_s_n_item.keyspace,sizeof(int64_t),1,pf_solid);
        assert(ret == 1);

        ret = fwrite(&st_s_n_item.tab_num,sizeof(int),1,pf_solid);
        assert(ret == 1);

        ret = fwrite(&st_s_n_item.attr_num,sizeof(int),1,pf_solid);
        assert(ret == 1);

        ret = fwrite(&st_s_n_item.alloc_time,sizeof(time_t),1,pf_solid);
        assert(ret == 1);
    } else { // exist : RIAK_KS_ALLOC_FILE
        pf_solid = fopen(_riak_solid_file.c_str(),"rb+");  // read/write
        if(pf_solid == NULL) {
            DPRINTFX(30,"File exist : Open file %s return NULL\n",_riak_solid_file.c_str());
            assert(0);
        }

        // solid_node_num
        ret = fread(&solid_node_num,sizeof(int64_t),1,pf_solid);
        assert(ret == 1);
        solid_node_num = solid_node_num+1;
        fseek(pf_solid,0L,SEEK_SET);
        ret = fwrite(&solid_node_num,sizeof(int64_t),1,pf_solid);
        assert(ret == 1);

        // solid node info
        fseek(pf_solid,8+(solid_node_num-1)*sizeof(stru_solid_node),SEEK_SET);

        ret = fwrite(&st_s_n_item.keyspace,sizeof(int64_t),1,pf_solid);
        assert(ret == 1);

        ret = fwrite(&st_s_n_item.tab_num,sizeof(int),1,pf_solid);
        assert(ret == 1);

        ret = fwrite(&st_s_n_item.attr_num,sizeof(int),1,pf_solid);
        assert(ret == 1);

        ret = fwrite(&st_s_n_item.alloc_time,sizeof(time_t),1,pf_solid);
        assert(ret == 1);

    }

    if(pf_solid) {
        fclose(pf_solid);
        pf_solid = NULL;
    }
}
