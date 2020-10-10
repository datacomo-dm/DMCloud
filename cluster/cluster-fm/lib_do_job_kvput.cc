#include <mutex>

#include "cluster_fm.h"

int
lib_do_job_kvput_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg)
{
    int ret = 0;
    CFMJobKVPutReq job_kvput_req;

    if (!job_kvput_req.ParseFromString(data_from_client))
        return ENOMSG;

    if (job_kvput_req.has_value()) {
        if (job_kvput_req.has_if_none_match() && job_kvput_req.if_none_match()) {
            ret = app_treestorage.Add(job_kvput_req.key(), job_kvput_req.value());
        } else {
            ret = app_treestorage.Set(job_kvput_req.key(), job_kvput_req.value());
        }
    } else {
        ret = app_treestorage.Del(job_kvput_req.key());
    }

    if (!ret)
        data_to_client.clear();

    return ret;
}
