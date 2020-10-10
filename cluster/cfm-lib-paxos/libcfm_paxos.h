#ifndef __H_LIBCFM_PAXOS_H__
#define __H_LIBCFM_PAXOS_H__

#include <string>
#include <vector>

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus


void *lib_on_start(const std::string &job_id, const std::string &extra_param,
	const int64_t slave_level, const std::vector<std::string> &work_path_arr);
int lib_on_stop(void *proc_data_p, const std::string &extra_param);
int lib_on_refresh(void *proc_data_p, const std::string &extra_param);
int lib_on_stat(void *proc_data_p, const std::string &extra_param, std::string &proc_stat);
int lib_on_raise(void *proc_data_p, const std::string &extra_param, const int64_t slave_level);
int lib_on_datareq(void *proc_data_p, const int64_t data_id, const std::string &data_req,
	std::string &data_resp);


#ifdef  __cplusplus
}
#endif // __cplusplus

#endif // __H_LIBCFM_PAXOS_H__
