#include <mutex>

#include "cluster_fm.h"

int
lib_do_job_stop_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg)
{
    int ret = 0;

    CR_Class_NS::delay_exit(30, 0);

    if (app_job_handle) {
        if (app_lib_info_p->on_stop) {
            data_to_client.clear();
            std::lock_guard<std::mutex> _lck(app_lib_fm_mtx);
            ret = app_lib_info_p->on_stop(app_job_handle, data_from_client);
            app_job_handle = NULL;
            CR_Class_NS::delay_exit(1, 0);
        } else
            ret = ENOTSUP;
    } else
        ret = ECANCELED;

    if (!ret)
        data_to_client.clear();

    return ret;
}
