#include <signal.h>
#include <dirent.h>

#include <cr_class/md5.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_simplecomm.h>

#include "cluster_fm.h"
#include "cfm_jobexecutor.h"

#define _AUTORUN_LIB_PREFIX "libcfm__"
#define _AUTORUN_LIB_SUFFIX ".so"

static void help_out(int err_code=EINVAL);
void *cfm_on_async_recv_init(void *server_private_p,
    const std::string &client_host, const std::string &client_srv, std::string *&chkstr);
void cfm_on_async_recv_clear(void *server_private_p,
    const std::string &client_host, const std::string &client_srv,
    void *connect_private_p, const int last_errno);
int cfm_on_async_recv_error(const std::string &client_host, const std::string &client_srv,
    const int outer_errcode, const std::string &outer_errmsg,
    int8_t &err_code_to_client, std::string &errdata_to_client);

static std::map<int8_t,CR_SimpleServer::on_async_recv_func_t> _recv_func_map;
static CFMJobExecutorMap _je_map;

static void
help_out(int err_code)
{
    fprintf(stderr,
      "Usage : %s %s <cluster_str> <current_node_addr> <lib dir path> <work_path_#0 [work_path_#1 ...]>\n"
      "  cluster_str format : striping#0_node#0_addr[,striping#0_node#1_addr[,...]]\n"
      "                       [;striping#1_node#0_addr[,striping#1_node#1_addr[,...]]]\n"
      "                       [;...]\n"
      "  node_addr format   : tcp://hostname:srvname\n"
      , app_argv[0], app_argv[1]
    );
    exit(err_code);
}

int
do_cfm(int argc, char *argv[])
{
    int fret;

    if (argc < 6)
        help_out();

    app_listen_server_p = new CR_SimpleServer;

    std::set<int> sig_ret;
    std::set<int> sig_set;

    sig_ret.insert(0);
    sig_ret.insert(ETIMEDOUT);
    sig_ret.insert(EADDRINUSE);

    sig_set.insert(SIGSEGV);
    sig_set.insert(SIGTERM);
    sig_set.insert(SIGABRT);

    if (CR_Class_NS::protection_fork(NULL, &sig_ret, &sig_set) != 0) {
        return 0;
    }

    app_cluster_str = argv[2];

    std::set<std::string> _wp_set;
    std::set<std::string>::iterator _wp_set_it;

    std::string current_node_addr = argv[3];

    app_lib_dir_path = argv[4];

    for (int i=5; i<argc; i++) {
        _wp_set.insert(std::string(argv[i]));
    }

    app_work_path_arr.clear();

    for (_wp_set_it=_wp_set.begin(); _wp_set_it!=_wp_set.end(); _wp_set_it++) {
        app_work_path_arr.push_back(*_wp_set_it);
    }

    fret = CR_Class_NS::nn_addr_split(current_node_addr.c_str(),
      app_cur_transport, app_cur_hostname, app_cur_srvname);

    if (fret || (app_cur_transport != CR_Class_NS::NN_TCP) || (app_cur_srvname.size() == 0)) {
        DPRINTF("Current node \"%s\" is invalid\n", current_node_addr.c_str());
        return EINVAL;
    }

    app_current_node = CFM_Basic_NS::parse_cluster_info(
      app_cluster_str, current_node_addr, app_cluster_info);

    if (app_current_node.first < 0) {
        DPRINTF("Cluster info parse failed[%s]\n", CR_Class_NS::strerrno(app_current_node.second));
        return app_current_node.second;
    }

    DPRINTF("striping[%d]->node[%d] is CURRENT\n", app_current_node.first, app_current_node.second);

    fret = app_listen_server_p->Listen(CR_Class_NS::blank_string, app_cur_srvname, 2000, 60, true);

    if (fret) {
        DPRINTF("Listen on port %s failed[%s]\n", app_cur_srvname.c_str(), CR_Class_NS::strerrno(fret));
        return fret;
    }

    _recv_func_map[CFM_PING_REQ] = do_req_distributed;
    _recv_func_map[CFM_JOB_START_REQ] = do_req_distributed;
    _recv_func_map[CFM_JOB_STOP_REQ] = do_req_distributed;
    _recv_func_map[CFM_JOB_CANCEL_REQ] = do_req_distributed;
    _recv_func_map[CFM_JOB_QUERY_REQ] = do_req_distributed;
    _recv_func_map[CFM_JOB_DATA_REQ] = do_req_distributed;
    _recv_func_map[CFM_JOB_CLEAR_REQ] = do_req_distributed;
    _recv_func_map[CFM_JOB_KVPUT_REQ] = do_req_distributed;
    _recv_func_map[CFM_JOB_KVGET_REQ] = do_req_distributed;
    _recv_func_map[CFM_ID_REQ] = do_req_distributed;
    _recv_func_map[LOCK_REQ] = do_req_distributed;
    _recv_func_map[UNLOCK_REQ] = do_req_distributed;

    if (app_listen_server_p->AsyncRecvStart(_recv_func_map, NULL,
      cfm_on_async_recv_init, cfm_on_async_recv_clear, cfm_on_async_recv_error, &_je_map) == 0) {
        std::vector<std::string> autorun_jobid_array;
        CFMConnect self_connect;

        struct dirent **namelist;
        int namelist_count;

        namelist_count = scandir(app_lib_dir_path.c_str(), &namelist, NULL, alphasort);
        if (namelist_count >= 0) {
            CFM_Basic_NS::striping_info_t self_striping_info;
            self_striping_info.push_back(current_node_addr);
            self_connect.Connect(self_striping_info);
            for (int i=0; i<namelist_count; i++) {
                std::string d_name = namelist[i]->d_name;
                if (d_name.compare(0, strlen(_AUTORUN_LIB_PREFIX), _AUTORUN_LIB_PREFIX) == 0) {
                    if (d_name.compare(d_name.size() - strlen(_AUTORUN_LIB_SUFFIX),
                      std::string::npos, _AUTORUN_LIB_SUFFIX) == 0) {
                        std::string tmp_job_id = d_name.substr(strlen(_AUTORUN_LIB_PREFIX),
                          d_name.size() - strlen(_AUTORUN_LIB_PREFIX) - strlen(_AUTORUN_LIB_SUFFIX));
                        self_connect.SetJobID(tmp_job_id);
                        if (self_connect.JobStart(d_name, true) == 0) {
                            autorun_jobid_array.push_back(tmp_job_id);
                        }
                    }
                }
                free(namelist[i]);
            }
            free(namelist);
        }

        while (1) {
            usleep(100000);
            for (size_t i=0; i<autorun_jobid_array.size(); i++) {
                self_connect.SetJobID(autorun_jobid_array[i]);
                self_connect.Ping();
            }
        }
    }

    if (app_listen_server_p) {
        delete app_listen_server_p;
    }

    return 0;
}

int
cfm_on_async_recv_error(const std::string &client_host, const std::string &client_srv,
    const int outer_errcode, const std::string &outer_errmsg,
    int8_t &err_code_to_client, std::string &errdata_to_client)
{
    std::string local_errmsg;

    if (outer_errmsg.size() > 0)
        local_errmsg = outer_errmsg;
    else
        local_errmsg = CR_Class_NS::strerrno(outer_errcode);

    err_code_to_client = CFM_ERROR_RESP;
    errdata_to_client = CR_Class_NS::error_encode(outer_errcode, local_errmsg);

    return 0;
}

void *
cfm_on_async_recv_init(void *server_private_p, const std::string &client_host, const std::string &client_srv,
    std::string *&chkstr)
{
    std::vector<CR_SimpleComm*> *striping_conn_array_p = new std::vector<CR_SimpleComm*>;
    *chkstr = CFM_Basic_NS::get_default_chkstr();
    return striping_conn_array_p;
}

void
cfm_on_async_recv_clear(void *server_private_p, const std::string &client_host, const std::string &client_srv,
    void *connect_private_p, const int last_errno)
{
    std::vector<CR_SimpleComm*> *striping_conn_array_p = (std::vector<CR_SimpleComm*>*)connect_private_p;
    if (striping_conn_array_p)
        delete striping_conn_array_p;
}
