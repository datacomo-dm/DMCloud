#include "cluster_fm.h"

int
do_nd_ping_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg)
{
    int ret = 0;
    std::string job_id;

    ret = CFM_Basic_NS::exchange_msg_parse(data_from_client, job_id, NULL);
    if (ret)
        return ret;

    if (job_id.size() > 0) {
        ret = do_nd_job_other_req(server_private_p, connect_private_p, client_host, client_srv,
          cmd_code_from_client, data_from_client, cmd_code_to_client, data_to_client, errmsg);
    } else {
        CFM_Basic_NS::exchange_msg_merge(job_id, &app_cluster_str, data_to_client);
    }

    return ret;
}
