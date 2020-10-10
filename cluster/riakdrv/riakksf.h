#ifndef __H_RIAKKSF_H__
#define __H_RIAKKSF_H__
#ifndef BUILD_RIAKKSF_TOOL
#include <riakdrv/riakcluster.h>
#else
#include <time.h>
#include <stdint.h>
#include <set>
#include <string>
using namespace std;
#endif

#define ENV_KEY_SPACE_LOCK_DIR "RIAK_KEY_SPACE_DIR"
#define RIAK_KS_LOCK_FILE               "riak_ks_lock"                   // ��ͬ���̽����ļ�
#define RIAK_KS_ALLOC_STEMP         4096                             // ���䲽��
#define RIAK_KS_ALLOC_FILE        "riak_ks_info.bin"                 // �Ѿ����տ��е�key
#ifndef REUSE_CLUSTER_FM_KEYSPACE_IN_FILE_MODE
#define RIAK_KS_RESERVE                  0x1f00000000000000      // ���λΪ0x1F,��������cluster_fm�����keyspace
#else
#define RIAK_KS_RESERVE                  0x0000000000000000      // ���λΪ0x1F,��������cluster_fm�����keyspace
#endif
#define RIAK_KS_RESERVE_XOR         0x00ffffffffffffff      // ���λΪ0x1F,��������cluster_fm�����keyspace
#define RIAK_KS_MAX                         0x1fffffffffffffff      // ���ֵ

/*
    riak_ks_info.bin �ļ��ṹ
    [alloc_max_node:8bytes ����������ֵ]
    [free_node_num:8bytes ���չ��Ľڵ���]
    [free_node1:8bytes]
    [free_node2:8bytes]
    [free_node3:8bytes]
    [free_nodex:8bytes]

    �����ʱ��:
    free_node_num >0 , ����free_nodex
    free_node_num =0 , ����alloc_max_node+=4096,free_node_num--;

    �ͷŵ�ʱ��:
    free_node_num ++; ���free_nodex
*/
#define RIAK_KS_SOLID_FILE "riak_ks_solid.bin"      // �ȴ��̻��Ľڵ��ļ�
#define RIAK_KS_SOLID_TIME_OUT  (7*24*3600)         // 7*24Сʱ��ͽ�δ�̻���ɾ����
/*
    riak_ks_solid.bin �ļ��ṹ
    [solid_node_num:8bytes ��Ҫ�̻��Ľڵ���]
    [<solid_node_1:8bytes><tabid:4bytes><attr:4bytes><alloc_time:8bytes>]
    [<solid_node_2:8bytes><tabid:4bytes><attr:4bytes><alloc_time:8bytes>]
    [<solid_node_3:8bytes><tabid:4bytes><attr:4bytes><alloc_time:8bytes>]
    [<solid_node_x:8bytes><tabid:4bytes><attr:4bytes><alloc_time:8bytes>]
*/

int64_t key_space_file_alloc(const std::string &bucket,const int tabid,const int attrid);
int key_space_file_free(const std::string &bucket,const int tabid,const int attrid,const int64_t ks_id);
int key_space_file_solidify(const std::string &bucket,const int tabid,const int attrid,const int64_t ks_id);

// ��ʱ����
void key_space_file_timeout_recover();
// ����KeySpace�ļ�����
void key_space_alloc_file_export();
// ����SolidKeySpace�ļ�����
void key_space_unsolid_file_export();

class RiakKSAlloc
{
public:
    RiakKSAlloc(const std::string path);
    virtual ~RiakKSAlloc();
public:
    int64_t AllocKeySpace(const int tab_num,const int attr_num);
    void    FreeKeySpace(const int tab_num,const int attr_num,const int64_t keyspace);
    void    SolidKeySpace(const int tab_num,const int attr_num,const int64_t keyspace);

public:  // ֻ���ֶ�����
    void TimeoutKeySpaceRecover();     // ֻ���ֶ���������²��ֹ���,�����Զ����!
    void AllocFileKeySpaceExport();       // ����Alloc�ļ�
    void SolidFileKeySpaceExport();       // ����Solid�ļ�
protected:
    std::string _riak_ks_file;
    std::string _riak_solid_file;
    std::string _riak_lock_file;
private:
    std::string _riak_ks_file_bak;
    std::string _riak_solid_file_bak;

private:
    int copy_file(char const * src_path,char const * des_path);
    void AddKS2Solid(const int tab_num,const int attr_num,const int64_t keyspace);
    typedef struct stru_solid_node {
        int64_t keyspace;
        int tab_num;
        int attr_num;
        time_t  alloc_time;
        stru_solid_node()
        {
            keyspace = -1;
            tab_num = 0;
            attr_num = 0;
            alloc_time = time(NULL);
        }
    } stru_solid_node;
};


#endif /* __H_RIAKKSF_H__ */
