#include <signal.h>

#include "cfm_jobexecutor.h"
#include "cluster_fm.h"

static double _cfm_je_info_cleanup(void *p);

static double
_cfm_je_info_cleanup(void *p)
{
    double ret;
    bool do_del = false;
    CFMJobExecutorMap::JobExecutorInfo *je_p = (CFMJobExecutorMap::JobExecutorInfo *)p;

    if (je_p->desc.is_real_closed()) {
        if (kill(je_p->job_pid, 0) == 0) {
            je_p->kill_count++;
            if (je_p->kill_count > 300) {
                kill(je_p->job_pid, SIGKILL);
                do_del = true;
            }
        } else {
            do_del = true;
        }
    }

    if (do_del) {
        std::string _cmd_line;

        if (!je_p->static_work_path) {
            for (size_t i=0; i< je_p->job_work_path_arr.size(); i++) {
                _cmd_line = "rm -rf '";
                _cmd_line += je_p->job_work_path_arr[i];
                _cmd_line += "'";
                CR_Class_NS::system(_cmd_line.c_str());
            }
        }

        unlink(je_p->job_sock_path.c_str());
        if (je_p->lib_is_tmp) {
            unlink(je_p->lib_name.c_str());
        }

        delete je_p;

        ret = 0.0;
    } else {
        ret = 0.2;
    }

    return ret;
}

////////////////////////////
////////////////////////////

CFMJobExecutorMap::JobExecutorInfo::JobExecutorInfo(const CFM_Basic_NS::cluster_info_t &cluster_info,
    const std::pair<int,int> &current_node)
{
    this->job_conn.set_timeout_send(180);
    this->job_conn.set_timeout_recv(8640000);
    this->job_conn.set_tag(this);
}

CFMJobExecutorMap::JobExecutorInfo::~JobExecutorInfo()
{
}

////////////////////////////
////////////////////////////

CFMJobExecutorMap::CFMJobExecutorMap()
{
}

CFMJobExecutorMap::~CFMJobExecutorMap()
{
    std::map<std::string,JobExecutorInfo*>::iterator map_it;

    CANCEL_DISABLE_ENTER();

    this->_cmtx.lock();

    while (!this->je_info_map.empty()) {
        map_it = this->je_info_map.begin();
        JobExecutorInfo *je_p = map_it->second;

        if (je_p) {
            je_p->desc.close();
            CR_Class_NS::set_alarm(_cfm_je_info_cleanup, je_p, 0.0);
            kill(je_p->job_pid, SIGINT);
            delete je_p;
        }

        this->je_info_map.erase(map_it);
    }

    this->_cmtx.unlock();

    CANCEL_DISABLE_LEAVE();
}

void
CFMJobExecutorMap::del_je_info(JobExecutorInfo *je_p)
{
    if (je_p) {
        je_p->desc.close();
        CR_Class_NS::set_alarm(_cfm_je_info_cleanup, je_p, 0.0);
    }
}

int
CFMJobExecutorMap::add_job(JobExecutorInfo *je_p)
{
    int ret = 0;
    pid_t _tmp_job_pid;
    std::string &job_id = je_p->job_id;

    CANCEL_DISABLE_ENTER();
    this->_cmtx.lock();

    JobExecutorInfo *tmp_je_p = this->je_info_map[job_id];
    if (!tmp_je_p) {
        this->je_info_map[job_id] = je_p;
    } else {
        _tmp_job_pid = tmp_je_p->job_pid;
        ret = EEXIST;
    }

    this->_cmtx.unlock();
    CANCEL_DISABLE_LEAVE();

    if (ret)
        DPRINTF("Another job(%d) with same jobid already in job map.\n", _tmp_job_pid);

    return ret;
}

CFMJobExecutorMap::JobExecutorInfo *
CFMJobExecutorMap::del_job(const std::string &job_id)
{
    JobExecutorInfo *ret = NULL;
    std::map<std::string,JobExecutorInfo*>::iterator map_it;

    CANCEL_DISABLE_ENTER();
    this->_cmtx.lock();

    map_it = this->je_info_map.find(job_id);
    if (map_it != this->je_info_map.end()) {
        ret = map_it->second;
        this->je_info_map.erase(map_it);
    }

    this->_cmtx.unlock();
    CANCEL_DISABLE_LEAVE();

    return ret;
}

CFMJobExecutorMap::JobExecutorInfo *
CFMJobExecutorMap::get_job(const std::string &job_id)
{
    JobExecutorInfo *ret = NULL;
    std::map<std::string,JobExecutorInfo*>::iterator map_it;

    CANCEL_DISABLE_ENTER();
    this->_cmtx.lock();

    map_it = this->je_info_map.find(job_id);
    if (map_it != this->je_info_map.end()) {
        ret = map_it->second;
        if (ret) {
            ret->desc.get();
        }
    }

    this->_cmtx.unlock();
    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CFMJobExecutorMap::put_job(JobExecutorInfo *je_p)
{
    int ret = 0;
    std::map<std::string,JobExecutorInfo*>::iterator map_it;

    if (!je_p)
        return EFAULT;

    CANCEL_DISABLE_ENTER();
    this->_cmtx.lock();

    map_it = this->je_info_map.find(je_p->job_id);
    if (map_it == this->je_info_map.end()) {
        ret = ENOENT;
    } else if (map_it->second != je_p) {
        ret = EBADSLT;
    }

    if (!ret)
        je_p->desc.put();

    this->_cmtx.unlock();
    CANCEL_DISABLE_LEAVE();

    return ret;
}
