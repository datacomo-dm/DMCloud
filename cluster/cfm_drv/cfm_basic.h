#ifndef __H_CFM_H__
#define __H_CFM_H__

#include <vector>
#include <string>
#include <cr_class/cr_class.h>
#include <cr_class/cr_fixedlinearstorage.h>
#include <cr_class/cr_simpleserver.h>

#define CFM_LIB_ON_INIT_FUNC_NAME	"cfm_lib_init"
#define CFM_LIB_WORKPATH_SUFFIX		".work"
#define CFM_LIB_SOCKPATH_SUFFIX		".sock"

#define CFM_ID_EFFECT_BIT_COUNT		(40)

namespace CFM_Basic_NS {
	typedef std::vector<std::string> striping_info_t;
	typedef std::vector<striping_info_t> cluster_info_t;

	std::string get_default_chkstr();

	int exchange_msg_merge(const std::string &job_id, const std::string *data_p, std::string &s_out);

	int exchange_msg_parse(const std::string &s_in, std::string &job_id, std::string *data_p);

	void exchange_msg_dsbi(std::string &s);
	void exchange_msg_dcli(std::string &s);

	bool exchange_msg_dtst(const std::string &s);

	std::pair<int,int> parse_cluster_info(const std::string &cluster_str,
		const std::string &cur_node_addr, cluster_info_t &cluster_info);

	int parse_cluster_info(const std::string &cluster_str, cluster_info_t &cluster_info);

	std::string make_node_name_prefix(std::pair<int,int> current_node_pos);
};

typedef int (*cfm_lib_kvput_t)(const std::string &key, const std::string *value_p, const bool if_none_match);

typedef int (*cfm_lib_kvget_t)(const std::string &key, std::string &value, const double timeout_sec);

// cfm_lib_on_start_t's return value is other callback func's 'proc_data_p' param
typedef void * (*cfm_lib_on_start_t)(const CFM_Basic_NS::cluster_info_t &cluster_info,
	const std::pair<int,int> &current_node, const std::string &job_id,
	const std::string &extra_param, const std::vector<std::string> &work_path_arr,
	const bool static_work_path);

typedef int (*cfm_lib_on_stop_t)(void *proc_data_p, const std::string &data_req);

typedef int (*cfm_lib_on_query_t)(void *proc_data_p, const std::string &data_req, std::string &data_resp);

typedef int (*cfm_lib_on_data_t)(void *proc_data_p, const int64_t rowid_left,
	const int64_t rowid_right, const std::string &data_req, std::string &data_resp);

typedef int (*cfm_lib_on_clear_t)(void *proc_data_p, const std::string &data_req);

class CFMLibInfo {
public:
	CFMLibInfo();

	cfm_lib_on_start_t on_start;
	cfm_lib_on_stop_t on_stop;
	cfm_lib_on_query_t on_query;
	cfm_lib_on_data_t on_data;
	cfm_lib_on_clear_t on_clear;

	CR_SimpleServer::on_async_recv_error_func_t on_async_recv_error;

	cfm_lib_kvput_t kvput;
	cfm_lib_kvget_t kvget;
};

typedef int (*cfm_lib_on_init_t)(CFMLibInfo *lib_infp_p);

#endif // __H_CFM_H__
