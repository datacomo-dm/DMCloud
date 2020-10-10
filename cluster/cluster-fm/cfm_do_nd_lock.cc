#include "cluster_fm.h"
#include "cfm_jobexecutor.h"

int
do_nd_lock_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg)
{
    std::string job_id;
    std::string data_req;

    int ret = CFM_Basic_NS::exchange_msg_parse(data_from_client, job_id, &data_req);
    if (ret)
        return ret;

    CFMLockReq lock_req;

    if (!lock_req.ParseFromString(data_req))
        return ENOMSG;

    std::string old_lock_value;

    int tree_ret = app_treestorage.Set(lock_req.lock_name(), lock_req.lock_value(), &old_lock_value,
      lock_req.lock_pass(), lock_req.lock_timeout(), lock_req.wait_timeout());

    std::string data_resp = CR_Class_NS::error_encode(tree_ret, old_lock_value);

    CFM_Basic_NS::exchange_msg_merge(job_id, &data_resp, data_to_client);

    return 0;
}

int
do_nd_unlock_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg)
{
    std::string job_id;
    std::string data_req;

    int ret = CFM_Basic_NS::exchange_msg_parse(data_from_client, job_id, &data_req);
    if (ret)
        return ret;

    CFMUnlockReq unlock_req;

    if (!unlock_req.ParseFromString(data_req))
        return ENOMSG;

    int tree_ret = app_treestorage.Del(unlock_req.lock_name(), unlock_req.lock_pass());

    std::string data_resp = CR_Class_NS::error_encode(tree_ret, CR_Class_NS::blank_string);

    CFM_Basic_NS::exchange_msg_merge(job_id, &data_resp, data_to_client);

    return 0;
}
