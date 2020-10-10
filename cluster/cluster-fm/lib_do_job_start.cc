#include <mutex>

#include "cluster_fm.h"

int
lib_do_job_start_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg)
{
    int ret = 0;
    CFMJobStartReq job_start_req;

    if (!app_job_handle) {
        if (app_lib_info_p->on_start) {
            data_to_client.clear();
            std::lock_guard<std::mutex> _lck(app_lib_fm_mtx);
            app_job_handle = app_lib_info_p->on_start(app_cluster_info, app_current_node,
              app_job_id, data_from_client, app_work_path_arr, app_lib_static_work);
            if (!app_job_handle) {
                if (errno)
                    ret = errno;
                else
                    ret = ECANCELED;
            }
        } else
            ret = ENOTSUP;
    } else
        ret = EALREADY;

    if (!ret)
        data_to_client.clear();

    return ret;
}
