#ifndef __H_CFM_CONNECT_H__
#define __H_CFM_CONNECT_H__

#include <vector>
#include <cr_class/cr_datacontrol.h>
#include <cr_class/cr_simplecomm.h>
#include <cr_class/cr_fixedlinearstorage.h>
#include <cfm_drv/cfm_basic.h>
#include <cfm_drv/cfm.pb.h>

namespace CFMConnect_NS {
	int JobCancel(const CFM_Basic_NS::striping_info_t &striping_info,
		const std::string &job_id, std::string *errmsg_p=NULL);
};

class CFMConnect {
public:
	CFMConnect(const unsigned int proc_timeout=86400, const unsigned int comm_timeout=600,
		const unsigned int max_retry=3);
	~CFMConnect();

	void SetTimeout(const unsigned int proc_timeout, const unsigned int comm_timeout);

	int Connect(const CFM_Basic_NS::striping_info_t &striping_info, const bool stable_mode=true);

	void DisConnect();

	void SetJobID(const std::string &job_id);

	std::string GetJobID();

	int JobStart(const std::string &lib_info, const bool static_work_path,
		const std::string *extra_param_p=NULL, std::string *errmsg_p=NULL);

	int JobStop(const std::string &data_req, std::string *errmsg_p=NULL);

	int JobQuery(const std::string &data_req, std::string *data_resp_p=NULL, std::string *errmsg_p=NULL);

	int JobData(const int64_t rowid_left, const int64_t rowid_right, const std::string &data_req,
		std::string *data_resp_p=NULL, std::string *errmsg_p=NULL);

	int JobClear(const std::string &data_req, std::string *errmsg_p=NULL);

	int JobKVPut(const std::string &key, const std::string *value_p,
		const bool if_none_match=false, std::string *errmsg_p=NULL);

	int JobKVGet(const std::string &key, std::string &value,
		const double timeout_sec=0.0, std::string *errmsg_p=NULL);

	int Ping(std::string *errmsg_p=NULL);

	int Lock(const std::string &lock_name, const std::string &lock_value, std::string *old_lock_value_p,
		const std::string &lock_pass, double lock_timeout_sec, double wait_timeout_sec,
		std::string *errmsg_p=NULL);

	int Unlock(const std::string &lock_name, const std::string &lock_pass, std::string *errmsg_p=NULL);

	int IDAlloc(const std::string &id_name, const size_t alloc_count, const double autofree_timeout,
		std::vector<int64_t> &id_arr, std::string *errmsg_p=NULL);

	int IDFree(const std::string &id_name, const std::vector<int64_t> &id_arr,
		std::string *errmsg_p=NULL);

	int IDSolidify(const std::string &id_name, const std::vector<int64_t> &id_arr,
		std::string *errmsg_p=NULL);

private:
        CR_DataControl::Mutex _mutex;

	std::vector<CR_SimpleComm*> _comm_array;
	unsigned int _proc_timeout;
	unsigned int _comm_timeout;
	std::string _job_id;
	bool _connected;
	size_t _max_retry;
	bool _single_mode;

	void _do_disconnect();
	int _raw_comm(const int8_t code_req, const std::string *data_req_p,
		int8_t &code_resp, std::string *data_resp_p, std::string *errmsg_p);
};

#endif /* __H_CFM_CONNECT_H__ */
