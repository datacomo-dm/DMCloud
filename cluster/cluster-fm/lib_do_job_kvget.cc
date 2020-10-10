#include <mutex>

#include "cluster_fm.h"

int
lib_do_job_kvget_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg)
{
    int ret = 0;
    CFMJobKVGetReq job_kvget_req;

    if (!job_kvget_req.ParseFromString(data_from_client))
        return ENOMSG;

    ret = app_treestorage.Get(job_kvget_req.key(), data_to_client, job_kvget_req.timeout_sec());

    return ret;
}
