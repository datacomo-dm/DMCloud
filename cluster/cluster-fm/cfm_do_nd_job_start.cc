#include <ctype.h>
#include <signal.h>

#include "cluster_fm.h"
#include "cfm_jobexecutor.h"

static int _job_start(CFMJobExecutorMap *je_map_p, const std::string &rand_id,
    const std::string &job_id, const std::string &lib_name, const bool lib_is_tmp,
    const std::string &extra_param, const bool static_work_path,
    int8_t &cmd_code_from_job, std::string &data_from_job);

int
do_nd_job_start_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg)
{
    int ret = 0;
    int fret;
    CFMJobExecutorMap *je_map_p = (CFMJobExecutorMap *)server_private_p;
    std::string job_id;
    std::string data_req;

    ret = CFM_Basic_NS::exchange_msg_parse(data_from_client, job_id, &data_req);
    if (ret)
        return ret;

    const char *job_id_p = job_id.data();
    size_t job_id_size = job_id.size();

    if (job_id_size == 0)
        return EEXIST;

    for (size_t i=0; i<job_id_size; i++) {
        if (!isalnum(job_id_p[i]) && (job_id_p[i] != '_'))
            return EINVAL;
    }

    CFMJobStartReq job_start_req;

    if (!job_start_req.ParseFromString(data_req))
        return ENOMSG;

    std::string extra_param;
    bool static_work_path = false;
    std::string rand_id;

    rand_id = CR_Class_NS::double2str(CR_Class_NS::clock_gettime());

    if (job_start_req.has_extra_param())
        extra_param = job_start_req.extra_param();

    if (job_start_req.has_static_work_path())
        static_work_path = job_start_req.static_work_path();

    do {
        std::string lib_name;
        bool lib_is_tmp = true;
        std::string lib_info = job_start_req.lib_info();
        std::string libso_data;

        lib_name = "/tmp/";
        lib_name += CFM_Basic_NS::make_node_name_prefix(app_current_node);
        lib_name += job_id;
        lib_name += "-";
        lib_name += rand_id;
        lib_name += ".so";

        if (lib_info.size() < PATH_MAX) {
            std::string local_lib_path = app_lib_dir_path;
            local_lib_path += "/libcfm_";
            local_lib_path += CR_Class_NS::basename(lib_info.c_str());
            local_lib_path += ".so";
            fret = CR_Class_NS::load_string(local_lib_path.c_str(), libso_data);
            if (fret) {
                libso_data = lib_info;
            } else {
                lib_is_tmp = false;
                lib_name = local_lib_path;
            }
        } else {
            libso_data = lib_info;
        }

        if (lib_is_tmp) {
            fret = CR_Class_NS::save_string(lib_name.c_str(), libso_data);
            if (fret) {
                DPRINTF("CR_Class_NS::save_string(\"%s\", ...) == %s\n", lib_name.c_str(),
                  CR_Class_NS::strerrno(fret));
                ret = fret;
                break;
            }
        }

        std::string data_resp_from_job;

        ret = _job_start(je_map_p, rand_id, job_id, lib_name, lib_is_tmp, extra_param,
          static_work_path, cmd_code_to_client, data_resp_from_job);
        if (!ret) {
            if (cmd_code_to_client == CFM_ERROR_RESP) {
                data_to_client = data_resp_from_job;
            } else {
                CFM_Basic_NS::exchange_msg_merge(job_id, &data_resp_from_job, data_to_client);
            }
        }
    } while (0);

    return ret;
}

static int
_job_start(CFMJobExecutorMap *je_map_p, const std::string &rand_id,
    const std::string &job_id, const std::string &lib_name, const bool lib_is_tmp,
    const std::string &extra_param, const bool static_work_path,
    int8_t &cmd_code_from_job, std::string &data_from_job)
{
    int ret = 0;
    int fret;
    pid_t child_pid = -1;
    std::string job_work_path;
    std::string job_sock_path;
    std::vector<std::string> job_work_path_arr;
    CFMJobExecutorMap::JobExecutorInfo *je_p = NULL;
    double _cur_time;
    double _exp_time;
    bool _child_conn_ok;
    char *env_lib_cmd_prefix = getenv(CFM_EN_LIB_CMD_PREFIX);

    job_sock_path = "/tmp/";
    job_sock_path += CFM_Basic_NS::make_node_name_prefix(app_current_node);
    job_sock_path += job_id;
    job_sock_path += "-";
    job_sock_path += rand_id;
    job_sock_path += CFM_LIB_SOCKPATH_SUFFIX;

    std::vector<std::string> cmd_prefix_arr;
    std::vector<char*> child_argv;

    if (env_lib_cmd_prefix) {
        cmd_prefix_arr = CR_Class_NS::str_split(env_lib_cmd_prefix, isblank);
        for (size_t i=0; i<cmd_prefix_arr.size(); i++) {
            child_argv.push_back((char*)(cmd_prefix_arr[i].c_str()));
        }
    }

    child_argv.push_back(app_argv[0]);
    if (static_work_path) {
        child_argv.push_back((char*)"slib");
    } else {
        child_argv.push_back((char*)"lib");
    }
    child_argv.push_back((char*)app_cluster_str.c_str());
    child_argv.push_back((char*)(app_cluster_info[app_current_node.first][app_current_node.second]).c_str());
    child_argv.push_back((char*)lib_name.c_str());
    child_argv.push_back((char*)job_id.c_str());
    child_argv.push_back((char*)job_sock_path.c_str());

    for (size_t i=0; i<app_work_path_arr.size(); i++) {
        job_work_path = app_work_path_arr[i];
        job_work_path += "/";
        job_work_path += CFM_Basic_NS::make_node_name_prefix(app_current_node);
        job_work_path += job_id;
        if (!static_work_path) {
            job_work_path += "-";
            job_work_path += rand_id;
        }
        job_work_path += CFM_LIB_WORKPATH_SUFFIX;

        if (mkdir(job_work_path.c_str(), 0700) < 0) {
            if (chmod(job_work_path.c_str(), 0700) < 0) {
                DPRINTF("Create work path[%s] failed[%s]\n",
                  job_work_path.c_str(), CR_Class_NS::strerrno(errno));
                ret = errno;
                goto errout;
            }
        }

        job_work_path_arr.push_back(job_work_path);

        child_argv.push_back((char*)job_work_path_arr[i].c_str());
    }

    child_argv.push_back(NULL);

    child_pid = vfork();

    if (child_pid == 0) { // child
        if (cmd_prefix_arr.size() > 0) {
            execv(cmd_prefix_arr[0].c_str(), child_argv.data());
        } else {
            execv(app_argv[0], child_argv.data());
        }
        // if exec() failed...
        DPRINTF("execl(...) return [%s]\n", CR_Class_NS::strerrno(errno));
        CR_Class_NS::exit(errno);
    } else if (child_pid < 0) { // failed
        DPRINTF("fork() return [%s]\n", CR_Class_NS::strerrno(errno));
        return errno;
    }

    je_p = new CFMJobExecutorMap::JobExecutorInfo(app_cluster_info, app_current_node);
    _cur_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
    _exp_time = _cur_time + CFM_TO_SOCKET_TIMEOUT_SEC;
    _child_conn_ok = false;

    do {
        fret = je_p->job_conn.Connect(job_sock_path);
        if (!fret) {
            _child_conn_ok = true;
            break;
        }
        usleep(1000);
    } while (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) < _exp_time);

    if (!_child_conn_ok) {
        DPRINTF("Connect to child[\"%s\"] failed[%s], kill it.\n",
          job_sock_path.c_str(), CR_Class_NS::strerrno(fret));

        if (fret)
            ret = fret;
        else
            ret = ETIMEDOUT;

        goto errout;
    }

    fret = je_p->job_conn.SendAndRecv(
      CFM_JOB_START_REQ, &extra_param, cmd_code_from_job, &data_from_job);
    if (fret) {
        DPRINTF("JOB SendAndRecv failed[%s].\n", CR_Class_NS::strerrno(fret));
        ret = fret;
        goto errout;
    }

    if (cmd_code_from_job != CFM_JOB_START_RESP)
        goto errout;

    je_p->job_pid = child_pid;
    je_p->job_id = job_id;
    je_p->job_sock_path = job_sock_path;
    je_p->job_work_path_arr = job_work_path_arr;
    je_p->lib_name = lib_name;
    je_p->lib_is_tmp = lib_is_tmp;
    je_p->static_work_path = static_work_path;
    je_p->kill_count = 0;

    ret = je_map_p->add_job(je_p);

    if (ret)
        goto errout;

    DPRINTFX(5, "Job [%s], lib [%s], listen on [%s], pid [%d] start successed.\n",
      job_id.c_str(), lib_name.c_str(), job_sock_path.c_str(), child_pid);

    return ret;
errout:
    if (je_p)
        delete je_p;

    if (child_pid > 0)
        kill(child_pid, SIGKILL);

    std::string _cmd_line;

    if (!static_work_path) {
        for (size_t i=0; i<job_work_path_arr.size(); i++) {
            _cmd_line = "rm -rf '";
            _cmd_line += job_work_path_arr[i];
            _cmd_line += "'";
            CR_Class_NS::system(_cmd_line.c_str());
        }
    }

    unlink(job_sock_path.c_str());
    if (lib_is_tmp)
        unlink(lib_name.c_str());

    return ret;
}
