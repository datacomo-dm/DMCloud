#include <mutex>

#include "cluster_fm.h"

int
lib_do_job_query_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg)
{
    int ret = 0;

    if (app_job_handle) {
        if (app_lib_info_p->on_query) {
            data_to_client.clear();
            ret = app_lib_info_p->on_query(app_job_handle, data_from_client, data_to_client);
        } else
            ret = ENOTSUP;
    } else
        ret = ECANCELED;

    return ret;
}
