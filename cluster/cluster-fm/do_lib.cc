#include <signal.h>
#include <dlfcn.h>
#include <dirent.h>

#include <cr_class/md5.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_simplecomm.h>

#include "cluster_fm.h"
#include "cfm_jobexecutor.h"

static void help_out(int err_code=EINVAL);
int lib_on_async_recv_error(const std::string &client_host, const std::string &client_srv,
    const int outer_errcode, const std::string &outer_errmsg,
    int8_t &err_code_to_client, std::string &errdata_to_client);
void *lib_on_async_recv_init(void *server_private_p,
    const std::string &client_host, const std::string &client_srv, std::string *&chkstr);
void lib_on_async_recv_clear(void *server_private_p,
    const std::string &client_host, const std::string &client_srv,
    void *connect_private_p, const int last_errno);

void lib_on_app_exit();

static int lib_kvput(const std::string &key, const std::string *value_p, const bool if_none_match);
static int lib_kvget(const std::string &key, std::string &value, const double timeout_sec);

static std::map<int8_t,CR_SimpleServer::on_async_recv_func_t> _recv_func_map;

static void
help_out(int err_code)
{
    fprintf(stderr,
      "Usage : %s %s <cluster_str> <current_node_addr> <lib pathname> <job id> <listen sockname>"
      " <work_path_#0 [work_path_#1 ...]>\n"
      , app_argv[0] , app_argv[1]
    );
    exit(err_code);
}

int
do_lib(int argc, char *argv[])
{
    int fret;
    std::set<std::string> _wp_set;
    std::set<std::string>::iterator _wp_set_it;
    void *dl_handle = NULL;

    if (argc < 8)
        help_out();

    app_listen_server_p = new CR_SimpleServer;

    double first_exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
    char *_lib_first_wait_time_sec_str = getenv(CFM_EN_LIB_FIRST_WAIT_SECOND);
    if (_lib_first_wait_time_sec_str) {
        int _lib_first_wait_time_sec = atoi(_lib_first_wait_time_sec_str);
        if ((_lib_first_wait_time_sec > 0) && (_lib_first_wait_time_sec < CFM_TO_SOCKET_TIMEOUT_SEC)) {
            first_exp_time += _lib_first_wait_time_sec;
        }
    }

    while (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) < first_exp_time)
        usleep(10000);

    if (strcmp(argv[1], "slib") == 0) {
        app_lib_static_work = true;
    } else {
        app_lib_static_work = false;
    }

    app_cluster_str = argv[2];
    std::string current_node_addr = argv[3];
    const char *lib_pathname = argv[4];
    app_job_id = argv[5];
    const char *listen_sockname = argv[6];

    app_current_node = CFM_Basic_NS::parse_cluster_info(
      app_cluster_str, current_node_addr, app_cluster_info);

    if (app_current_node.first < 0) {
        DPRINTF("Cluster info parse failed[%s]\n", CR_Class_NS::strerrno(app_current_node.second));
        return app_current_node.second;
    }

    atexit(lib_on_app_exit);

    for (int i=7; i<argc; i++) {
        _wp_set.insert(std::string(argv[i]));
    }

    app_work_path_arr.clear();

    for (_wp_set_it=_wp_set.begin(); _wp_set_it!=_wp_set.end(); _wp_set_it++) {
        app_work_path_arr.push_back(*_wp_set_it);
    }

    dl_handle = dlopen(lib_pathname, RTLD_NOW | RTLD_GLOBAL);
    if (!dl_handle) {
        DPRINTF("%s\n", dlerror());
        return EINVAL;
    }

    cfm_lib_on_init_t cfm_lib_init =
      (cfm_lib_on_init_t)dlsym(dl_handle, CFM_LIB_ON_INIT_FUNC_NAME);

    if (!cfm_lib_init) {
        DPRINTFX(0, "%s\n", dlerror());
        return EINVAL;
    }

    if (!app_lib_info_p)
        app_lib_info_p = new CFMLibInfo;

    app_lib_info_p->kvput = lib_kvput;
    app_lib_info_p->kvget = lib_kvget;

    fret = cfm_lib_init(app_lib_info_p);
    if (fret) {
        DPRINTFX(0, "%s failed[%s]\n", CFM_LIB_ON_INIT_FUNC_NAME, CR_Class_NS::strerrno(fret));
        return fret;
    }

    if (app_lib_info_p->on_async_recv_error == NULL)
        app_lib_info_p->on_async_recv_error = lib_on_async_recv_error;

    fret = app_listen_server_p->Listen(listen_sockname, 2000, 0.1);

    if (fret) {
        DPRINTF("Listen on unix domain \"%s\" failed[%s]\n",
          listen_sockname, CR_Class_NS::strerrno(fret));
        CR_Class_NS::exit(fret);
    }

    DPRINTFX(20, "Listen on unix domain \"%s\"\n", listen_sockname);

    _recv_func_map[CFM_PING_REQ] = lib_do_ping_req;
    _recv_func_map[CFM_JOB_START_REQ] = lib_do_job_start_req;
    _recv_func_map[CFM_JOB_STOP_REQ] = lib_do_job_stop_req;
    _recv_func_map[CFM_JOB_QUERY_REQ] = lib_do_job_query_req;
    _recv_func_map[CFM_JOB_DATA_REQ] = lib_do_job_data_req;
    _recv_func_map[CFM_JOB_CLEAR_REQ] = lib_do_job_clear_req;
    _recv_func_map[CFM_JOB_KVPUT_REQ] = lib_do_job_kvput_req;
    _recv_func_map[CFM_JOB_KVGET_REQ] = lib_do_job_kvget_req;

    if (app_listen_server_p->AsyncRecvStart(_recv_func_map, NULL, lib_on_async_recv_init,
      lib_on_async_recv_clear, app_lib_info_p->on_async_recv_error) == 0) {
        while (1) {
            sleep(1);
        }
    }

    if (app_listen_server_p) {
        delete app_listen_server_p;
    }

    return 0;
}

int
lib_on_async_recv_error(const std::string &client_host, const std::string &client_srv,
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

    return EAGAIN;
}

void *
lib_on_async_recv_init(void *server_private_p, const std::string &client_host, const std::string &client_srv,
    std::string *&chkstr)
{
    chkstr = NULL;
    DPRINTFX(20, "CFM connected.\n");
    return NULL;
}

void
lib_on_async_recv_clear(void *server_private_p, const std::string &client_host, const std::string &client_srv,
    void *connect_private_p, const int last_errno)
{
    CR_Class_NS::delay_exit(1, last_errno);
}

void
lib_on_app_exit()
{
    DPRINTF("ON APP EXIT\n");
}

static int
lib_kvput(const std::string &key, const std::string *value_p, const bool if_none_match)
{
    int ret = 0;

    if (value_p) {
        if (if_none_match) {
            ret = app_treestorage.Add(key, *value_p);
        } else {
            ret = app_treestorage.Set(key, *value_p);
        }
    } else {
        ret = app_treestorage.Del(key);
    }

    return ret;
}

static int
lib_kvget(const std::string &key, std::string &value, const double timeout_sec)
{
    return app_treestorage.Get(key, value, timeout_sec);
}
