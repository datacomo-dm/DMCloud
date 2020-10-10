#include <ctype.h>

#include "cluster_fm.h"
#include "cfm_jobexecutor.h"

int
do_nd_job_stop_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg)
{
    int ret = 0;
    CFMJobExecutorMap *je_map_p = (CFMJobExecutorMap *)server_private_p;
    CFMJobExecutorMap::JobExecutorInfo *je_p;
    std::string job_id;
    std::string data_req;

    ret = CFM_Basic_NS::exchange_msg_parse(data_from_client, job_id, &data_req);
    if (ret)
        return ret;

    std::string data_resp_from_job;

    je_p = je_map_p->del_job(job_id);
    if (je_p) {
        ret = je_p->job_conn.SendAndRecv(
          cmd_code_from_client, &data_req, cmd_code_to_client, &data_resp_from_job);
        je_map_p->del_je_info(je_p);
    } else
        ret = ENOENT;

    if (!ret) {
        if (cmd_code_to_client == CFM_ERROR_RESP) {
            data_to_client = data_resp_from_job;
        } else {
            CFM_Basic_NS::exchange_msg_merge(job_id, &data_resp_from_job, data_to_client);
        }
    }

    return ret;
}
