#ifndef __H_CFM_EXTRA_H__
#define __H_CFM_EXTRA_H__

#include <vector>
#include <set>
#include <mutex>

#include <cr_class/cr_class.h>
#include <cr_class/cr_locked.h>
#include <cr_class/cr_simpleserver.h>
#include <cr_class/cr_bigset.h>
#include <cr_class/cr_treestorage.h>
#include <cr_class/cr_msgcode.h>
#include <cfm_drv/cfm_basic.h>
#include <cfm_drv/cfm_connect.h>

#define CFM_TO_SOCKET_TIMEOUT_SEC	(60)

#define CFM_EN_LIB_CMD_PREFIX		"CFM_LIB_CMD_PREFIX"
#define CFM_EN_LIB_FIRST_WAIT_SECOND	"CFM_LIB_FIRST_WAIT_SECOND"

typedef int(*func_t)(int argc, char **argv);

int do_cfm(int argc, char *argv[]);
int do_lib(int argc, char *argv[]);
int do_replica(int argc, char *argv[]);

int do_req_distributed(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);

int do_nd_ping_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);
int do_nd_job_start_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);
int do_nd_job_stop_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);
int do_nd_job_cancel_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);
int do_nd_job_other_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);
int do_nd_lock_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);
int do_nd_unlock_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);
int do_nd_id_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);

int lib_do_ping_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);
int lib_do_job_start_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);
int lib_do_job_stop_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);
int lib_do_job_query_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);
int lib_do_job_data_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);
int lib_do_job_clear_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);
int lib_do_job_kvput_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);
int lib_do_job_kvget_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);

extern CR_SimpleServer *app_listen_server_p;
extern int app_argc;
extern char **app_argv;
extern std::string app_job_id;
extern int64_t app_job_slave_level;
extern std::string app_cwd;
extern std::string app_lib_dir_path;
extern std::vector<std::string> app_work_path_arr;
extern CR_TreeStorage app_treestorage;

extern bool app_lib_static_work;
extern std::mutex app_lib_fm_mtx;
extern void *app_job_handle;

extern CR_SimpleServer::on_async_recv_error_func_t app_on_async_recv_error;

extern CFMLibInfo *app_lib_info_p;

extern std::string app_cluster_str;
extern CFM_Basic_NS::cluster_info_t app_cluster_info;
extern std::pair<int,int> app_current_node;

extern CR_Class_NS::nn_transport_t app_cur_transport;
extern std::string app_cur_hostname;
extern std::string app_cur_srvname;

extern CR_BigSet<int64_t> app_data_rowid_set;

#endif /* __H_CFM_EXTRA_H__ */
