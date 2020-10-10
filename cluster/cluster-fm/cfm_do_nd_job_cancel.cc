#include <ctype.h>
#include <signal.h>

#include "cluster_fm.h"
#include "cfm_jobexecutor.h"

int
do_nd_job_cancel_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg)
{
    int ret = 0;
    CFMJobExecutorMap *je_map_p = (CFMJobExecutorMap *)server_private_p;
    CFMJobExecutorMap::JobExecutorInfo *je_p;
    std::string job_id;
    std::string data_req;
    std::string job_path;
    std::string cmd_line;

    ret = CFM_Basic_NS::exchange_msg_parse(data_from_client, job_id, &data_req);
    if (ret)
        return ret;

    if (data_req.size() == 0)
        return EINVAL;

    std::string data_resp_from_job;

    je_p = je_map_p->del_job(job_id);
    if (je_p) {
        kill(je_p->job_pid, SIGKILL);
        je_p->kill_count = INT_MAX;
        je_map_p->del_je_info(je_p);
        DPRINTFX(0, "[%s]Job canceled by %s:%s\n", job_id.c_str(), client_host.c_str(), client_srv.c_str());
        DPRINTF("[%s]JobCancel caller info -->\n%s\n<--EOF\n", job_id.c_str(), data_req.c_str());
    }

    DPRINTF("[%s]Remove job data\n", job_id.c_str());

    for (size_t i=0; i<app_work_path_arr.size(); i++) {
        job_path = app_work_path_arr[i];
        job_path += "/";
        job_path += CFM_Basic_NS::make_node_name_prefix(app_current_node);
        job_path += job_id;
        job_path += "*";
        cmd_line = "rm -rf ";
        cmd_line += job_path;
        CR_Class_NS::system(cmd_line.c_str());
    }

    job_path = "/tmp/";
    job_path += CFM_Basic_NS::make_node_name_prefix(app_current_node);
    job_path += job_id;
    job_path += "*";
    cmd_line = "rm -rf ";
    cmd_line += job_path;
    CR_Class_NS::system(cmd_line.c_str());

    if (!ret) {
        CFM_Basic_NS::exchange_msg_merge(job_id, &data_resp_from_job, data_to_client);
    }

    return ret;
}
