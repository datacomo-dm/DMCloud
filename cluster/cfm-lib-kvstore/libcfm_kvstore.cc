#include "libcfm_kvstore.h"
#include <leveldb/db.h>

////////////////////////////////////

typedef struct {
    leveldb::DB* db;
    std::vector<std::vector<std::string> > cluster_info;
    std::pair<int,int> current_node;
    std::string job_id;
} kvstore_private_data_t;

////////////////////////////////////

static void *lib_on_start(const std::vector<std::vector<std::string> > &cluster_info,
    const std::pair<int,int> &current_node, const std::string &job_id,
    const std::string &extra_param, const std::vector<std::string> &work_path_arr);
static int lib_on_stop(void *proc_data_p, const std::string &data_req);
static int lib_on_query(void *proc_data_p, const std::string &data_req, std::string &data_resp);

////////////////////////////////////

static cfm_lib_kvput_t kvput;
static cfm_lib_kvget_t kvget;

////////////////////////////////////

int
cfm_lib_init(CFMLibInfo *lib_infp_p)
{
    if (!lib_infp_p)
        return EFAULT;

    lib_infp_p->on_start = lib_on_start;
    lib_infp_p->on_stop = lib_on_stop;
    lib_infp_p->on_query = lib_on_query;

    kvput = lib_infp_p->kvput;
    kvget = lib_infp_p->kvget;

    return 0;
}

////////////////////////////////////

static void *
lib_on_start(const std::vector<std::vector<std::string> > &cluster_info,
    const std::pair<int,int> &current_node, const std::string &job_id,
    const std::string &extra_param, const std::vector<std::string> &work_path_arr)
{
    kvstore_private_data_t *private_data_p = NULL;

    std::string data_path = work_path_arr[0] + std::string("/permanent");

    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, data_path, &db);

    if (status.ok()) {
        private_data_p = new kvstore_private_data_t;
        private_data_p->db = db;
        private_data_p->cluster_info = cluster_info;
        private_data_p->current_node = current_node;
        private_data_p->job_id = job_id;
    } else {
        DPRINTF("%s\n", status.ToString().c_str());
    }

    return private_data_p;
}

static int
lib_on_stop(void *proc_data_p, const std::string &data_req)
{
    if (!proc_data_p)
        return EFAULT;

    kvstore_private_data_t *private_data_p = (kvstore_private_data_t*)proc_data_p;

    if (private_data_p->db) {
        delete private_data_p->db;
    }

    delete private_data_p;

    return 0;
}

static int
lib_on_query(void *proc_data_p, const std::string &data_req, std::string &data_resp)
{
    if (!proc_data_p)
        return EFAULT;

    kvstore_private_data_t *private_data_p = (kvstore_private_data_t*)proc_data_p;

    if (!private_data_p->db)
        return EFAULT;

    return 0;
}
