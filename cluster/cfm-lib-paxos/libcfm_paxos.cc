#include <cr_class/cr_class.h>
#include "libcfm_paxos.h"


void *
lib_on_start(const std::string &job_id, const std::string &extra_param,
    const int64_t slave_level, const std::vector<std::string> &work_path_arr)
{
    DPRINTFX(20, "%s\n", VERSION_STR_CR_CLASS_H);

    return NULL;
}

int
lib_on_stop(void *proc_data_p, const std::string &extra_param)
{
    if (!proc_data_p)
        return EFAULT;

    return 0;
}

int
lib_on_stat(void *proc_data_p, const std::string &extra_param, std::string &proc_stat)
{
    int ret = 0;

    if (!proc_data_p)
        return EFAULT;

    return ret;
}

int
lib_on_raise(void *proc_data_p, const std::string &extra_param, const int64_t slave_level)
{
    int ret = 0;

    if (!proc_data_p)
        return EFAULT;

    return ret;
}

int
lib_on_datareq(void *proc_data_p, const int64_t data_id, const std::string &data_req, std::string &data_resp)
{
    int ret = 0;

    if (!proc_data_p)
        return EFAULT;

    return ret;
}
