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
#define RIAK_KS_LOCK_FILE               "riak_ks_lock"                   // 不同进程建锁文件
#define RIAK_KS_ALLOC_STEMP         4096                             // 分配步长
#define RIAK_KS_ALLOC_FILE        "riak_ks_info.bin"                 // 已经回收空闲的key
#ifndef REUSE_CLUSTER_FM_KEYSPACE_IN_FILE_MODE
#define RIAK_KS_RESERVE                  0x1f00000000000000      // 最高位为0x1F,用于区分cluster_fm分配的keyspace
#else
#define RIAK_KS_RESERVE                  0x0000000000000000      // 最高位为0x1F,用于区分cluster_fm分配的keyspace
#endif
#define RIAK_KS_RESERVE_XOR         0x00ffffffffffffff      // 最高位为0x1F,用于区分cluster_fm分配的keyspace
#define RIAK_KS_MAX                         0x1fffffffffffffff      // 最大值

/*
    riak_ks_info.bin 文件结构
    [alloc_max_node:8bytes 分配过的最大值]
    [free_node_num:8bytes 回收过的节点数]
    [free_node1:8bytes]
    [free_node2:8bytes]
    [free_node3:8bytes]
    [free_nodex:8bytes]

    分配的时候:
    free_node_num >0 , 分配free_nodex
    free_node_num =0 , 分配alloc_max_node+=4096,free_node_num--;

    释放的时候:
    free_node_num ++; 添加free_nodex
*/
#define RIAK_KS_SOLID_FILE "riak_ks_solid.bin"      // 等待固化的节点文件
#define RIAK_KS_SOLID_TIME_OUT  (7*24*3600)         // 7*24小时候就将未固化的删除掉
/*
    riak_ks_solid.bin 文件结构
    [solid_node_num:8bytes 需要固化的节点数]
    [<solid_node_1:8bytes><tabid:4bytes><attr:4bytes><alloc_time:8bytes>]
    [<solid_node_2:8bytes><tabid:4bytes><attr:4bytes><alloc_time:8bytes>]
    [<solid_node_3:8bytes><tabid:4bytes><attr:4bytes><alloc_time:8bytes>]
    [<solid_node_x:8bytes><tabid:4bytes><attr:4bytes><alloc_time:8bytes>]
*/

int64_t key_space_file_alloc(const std::string &bucket,const int tabid,const int attrid);
int key_space_file_free(const std::string &bucket,const int tabid,const int attrid,const int64_t ks_id);
int key_space_file_solidify(const std::string &bucket,const int tabid,const int attrid,const int64_t ks_id);

// 超时回收
void key_space_file_timeout_recover();
// 导出KeySpace文件数据
void key_space_alloc_file_export();
// 导出SolidKeySpace文件数据
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

public:  // 只能手动运行
    void TimeoutKeySpaceRecover();     // 只能手动的完成如下部分功能,不能自动完成!
    void AllocFileKeySpaceExport();       // 导出Alloc文件
    void SolidFileKeySpaceExport();       // 导出Solid文件
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
