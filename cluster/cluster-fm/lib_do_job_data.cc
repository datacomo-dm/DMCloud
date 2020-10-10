#include "cluster_fm.h"

int
lib_do_job_data_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg)
{
    if (!app_job_handle)
        return ECANCELED;

    if (!app_lib_info_p->on_data)
        return ENOTSUP;

    CFMJobDataReq msg_data_req;

    if (!msg_data_req.ParseFromString(data_from_client))
        return ENOMSG;

    int ret = 0;

    std::string data_req;
    std::string data_resp;

    ret = CR_Class_NS::decompressx(msg_data_req.data_req(), data_req);
    if (ret)
        return ret;

    data_to_client.clear();

    int64_t rowid_left = msg_data_req.rowid_left();
    int64_t rowid_right = msg_data_req.rowid_right();

    ret = app_data_rowid_set.Add(rowid_left, rowid_right);
    if (!ret) {
        ret = app_lib_info_p->on_data(app_job_handle, rowid_left, rowid_right, data_req, data_resp);
        if (ret) {
            app_data_rowid_set.Del(rowid_left, rowid_right);
        }
    }

    if (!ret) {
        CR_Class_NS::compressx_snappy(data_resp, data_to_client);
    }

    return ret;
}
