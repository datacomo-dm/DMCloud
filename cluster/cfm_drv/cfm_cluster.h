#ifndef __H_CFM_CLUSTER_H__
#define __H_CFM_CLUSTER_H__

#include <mutex>
#include <vector>
#include <map>
#include <thread>
#include <cr_class/cr_datacontrol.h>
#include <cr_class/cr_autoid.h>
#include <cr_class/cr_blockqueue.h>
#include <cr_class/cr_fixedlinearstorage.h>
#include <cfm_drv/cfm_connect.h>

#define CFMCLUSTER_DEFAULT_SEND_BUF_SIZE	(1024 * 1024 * 1)
#define CFMCLUSTER_MIN_SEND_BUF_SIZE		(1024 * 64)

#define CFMCLUSTER_FLAG_SPLIT_BLOCK		(0x00000001)
#define CFMCLUSTER_FLAG_REFRESH_FIRST		(0x00000002)
#define CFMCLUSTER_FLAG_DROP_IT			(0x00000004)
#define CFMCLUSTER_FLAG_REDO_IT			(0x00000008)

namespace CFMCluster_NS {
	typedef std::map<size_t,std::string> NodeErrorsT;

	std::string ExplainErrors(const NodeErrorsT &errors);

	void MergeError(NodeErrorsT *errors_p, const size_t striping_id,
		const int err_code, const std::string &err_msg);

	int RemoteCancel(const std::string &cluster_str, const std::string &job_id,
		CFMCluster_NS::NodeErrorsT *errors_p=NULL);
};

class CFMCluster {
public:
	typedef int (*result_merge_func_t)(void *merge_arg_p,
		const CFM_Basic_NS::cluster_info_t &cluster_info,
		const std::string &data_req, const std::vector<std::string> &results);

	// return a pos of striping [0 <= ret < striping_count_of_cluster],
	//  ret < 0 means error, and abs(ret) is errno
	//  row_id_in is an auto-increment unique number from 0
	//  can use CFMCLUSTER_FLAG_SPLIT_BLOCK and CFMCLUSTER_FLAG_REFRESH_FIRST flag(s)
	typedef int (*on_splitter_call_func_t)(void *splitter_arg_p,
		const CFM_Basic_NS::cluster_info_t &cluster_info, const int64_t row_id_in,
		const int64_t obj_id, const void *key_p, const uint64_t key_size,
		const void *value_p, const uint64_t value_size,
		int64_t &row_id_out, int64_t &ret_flags);

	// can use CFMCLUSTER_FLAG_DROP_IT flag(s)
	typedef int (*before_buf_refresh_func_t)(void *private_arg_p,
		const CFM_Basic_NS::cluster_info_t &cluster_info, const size_t target_striping_id,
		int64_t &row_id_left, int64_t &row_id_right, CR_FixedLinearStorage &fls_req,
		uintptr_t &pack_arg, int64_t &ret_flags);

	// can use CFMCLUSTER_FLAG_REDO_IT flag(s)
	typedef int (*after_buf_refreshed_func_t)(void *private_arg_p,
		const CFM_Basic_NS::cluster_info_t &cluster_info, const size_t target_striping_id,
		int64_t &row_id_left, int64_t &row_id_right, CR_FixedLinearStorage &fls_req,
		const int jobdata_ret, const std::string &data_resp, const std::string &errmsg,
		uintptr_t &pack_arg, int64_t &ret_flags);

	class SendBuf {
	public:
		class BufBlock {
		public:
			BufBlock(uint64_t fls_size, int64_t fls_size_mode);
			void SetCallbacks(before_buf_refresh_func_t _before_buf_refresh_func,
				void *_before_buf_refresh_arg_p,
				after_buf_refreshed_func_t _after_buf_refreshed_func,
				void *_after_buf_refreshed_arg_p);
			int PushBack(const int64_t row_id, const int64_t obj_id,
				const void *key_p, const uint64_t key_size,
				const void *value_p, const uint64_t value_size);
			int Send(CFMConnect *conn_p, const CFM_Basic_NS::cluster_info_t *cluster_info_p,
				size_t striping_id);

		private:
			int64_t row_id_left;
			int64_t row_id_right;
			CR_FixedLinearStorage buf_fls;
			before_buf_refresh_func_t before_buf_refresh_func;
			void *before_buf_refresh_arg_p;
			after_buf_refreshed_func_t after_buf_refreshed_func;
			void *after_buf_refreshed_arg_p;
		};

		SendBuf(const CFM_Basic_NS::cluster_info_t &cluster_info, const size_t striping_id,
			uint64_t send_buf_size, const int64_t send_buf_size_mode);
		~SendBuf();
		int SendRow(int64_t cur_row_id, const int64_t obj_id,
			const void *key_p, const uint64_t key_size,
			const void *value_p, const uint64_t value_size,
			before_buf_refresh_func_t before_buf_refresh_func, void *before_buf_refresh_arg_p,
			after_buf_refreshed_func_t after_buf_refreshed_func, void *after_buf_refreshed_arg_p);
		int Refresh(before_buf_refresh_func_t before_buf_refresh_func, void *before_buf_refresh_arg_p,
			after_buf_refreshed_func_t after_buf_refreshed_func, void *after_buf_refreshed_arg_p);
		void SetTimeout(const unsigned int proc_timeout, const unsigned int comm_timeout);
		int Connect(unsigned int comm_timeout);
		void SetJobID(const std::string &job_id);
		int JobStart(const std::string &lib_info, const bool static_work_path,
			const std::string *extra_param_p, std::string *errmsg_p);
		int JobStop(const std::string &data_req, std::string *errmsg_p);
		int JobQuery(const std::string &data_req,
			std::string *data_resp_p, std::string *errmsg_p);
		int JobKVPut(const std::string &key, const std::string *value_p,
			const bool if_none_match, std::string *errmsg_p);
		int JobKVGet(const std::string &key, std::string &value,
			const double timeout_sec, std::string *errmsg_p);
		int JobClear(const std::string &data_req, std::string *errmsg_p);
		int IDAlloc(const std::string &id_name, const size_t alloc_count,
			const double autofree_timeout, std::vector<int64_t> &id_arr, std::string *errmsg_p);
		int IDFree(const std::string &id_name, const std::vector<int64_t> &id_arr,
			std::string *errmsg_p);
		int IDSolidify(const std::string &id_name, const std::vector<int64_t> &id_arr,
			std::string *errmsg_p);

	private:
		CR_DataControl::Spin _lck;
		CR_BlockQueue<BufBlock*> _buf_queue;
		std::thread *_send_tp;
		volatile bool _send_th_stop;
		volatile int _send_th_errno;
		CFMConnect *conn_p;
		const CFM_Basic_NS::cluster_info_t *cluster_info_p;
		size_t striping_id;
		uint64_t fls_size;
		int64_t fls_size_mode;
		BufBlock *buf_block_p;

		int do_send(before_buf_refresh_func_t before_buf_refresh_func, void *before_buf_refresh_arg_p,
			after_buf_refreshed_func_t after_buf_refreshed_func, void *after_buf_refreshed_arg_p);
	};

	CFMCluster(const CFM_Basic_NS::cluster_info_t &cluster_info,
		const uint64_t send_buf_size=CFMCLUSTER_DEFAULT_SEND_BUF_SIZE,
		const int64_t send_buf_size_mode=CR_FLS_SIZEMODE_LARGE,
		const unsigned int proc_timeout=86400, const unsigned int comm_timeout=600);

	~CFMCluster();

	void SetTimeout(const unsigned int proc_timeout, const unsigned int comm_timeout);

	void SetRowID(int64_t row_id);

	void SetJobID(const std::string &job_id);

	std::string GetJobID();

	int Connect(CFMCluster_NS::NodeErrorsT *errors_p=NULL);

	int Start(const std::string &lib_info, const bool static_work_path,
		const std::string *param_p=NULL, CFMCluster_NS::NodeErrorsT *errors_p=NULL);

	int Stop(const std::string &data_req, CFMCluster_NS::NodeErrorsT *errors_p=NULL);

	int Query(const std::string &data_req, result_merge_func_t result_merge_func,
		void *merge_arg_p, CFMCluster_NS::NodeErrorsT *errors_p=NULL);

	int KVPut(const std::string &key, const std::string *value_p,
		const bool if_none_match=false, CFMCluster_NS::NodeErrorsT *errors_p=NULL);

	int KVGet(const std::string &key, const double timeout_sec,
		result_merge_func_t result_merge_func, void *merge_arg_p,
		CFMCluster_NS::NodeErrorsT *errors_p=NULL);

	int QueryOne(const size_t striping_id, const std::string &data_req,
		std::string *data_resp_p=NULL, std::string *errmsg_p=NULL);

	int KVPutOne(const size_t striping_id, const std::string &key, const std::string *value_p,
		const bool if_none_match=false, std::string *errmsg_p=NULL);

	int KVGetOne(const size_t striping_id, const std::string &key, std::string &value,
		const double timeout_sec=0.0, std::string *errmsg_p=NULL);

	// if data_send_p == NULL, it means force send all cached data to cluster
	// if on_splitter_call_func == NULL and (uintptr_t)splitter_arg_p < striping_count,
	//   it means target_striping_id = (uintptr_t)splitter_arg_p
	// if on_splitter_call_func == NULL but (uintptr_t)splitter_arg_p >= striping_count,,
	//   it means target_striping_id = rand() % striping_count
	// on_splitter_call_func, before_buf_refresh_func and after_buf_refreshed_func MUST be thread-safe
	int Data(CR_FixedLinearStorage *data_send_p=NULL,
		on_splitter_call_func_t on_splitter_call_func=NULL, void *splitter_arg_p=NULL,
		before_buf_refresh_func_t before_buf_refresh_func=NULL, void *before_buf_refresh_arg_p=NULL,
		after_buf_refreshed_func_t after_buf_refreshed_func=NULL, void *after_buf_refreshed_arg_p=NULL);

	int Clear(const std::string &data_req, CFMCluster_NS::NodeErrorsT *errors_p=NULL);

	int IDAlloc(const std::string &id_name, const size_t alloc_count, const double autofree_timeout,
		std::vector<int64_t> &id_arr, std::string *errmsg_p=NULL);

	int IDFree(const std::string &id_name, const std::vector<int64_t> &id_arr, std::string *errmsg_p=NULL);

	int IDSolidify(const std::string &id_name, const std::vector<int64_t> &id_arr,
		std::string *errmsg_p=NULL);

private:
	std::mutex _mtx;

	CR_AutoID<int64_t> _row_id;
	bool _connected;

	CFM_Basic_NS::cluster_info_t _cluster_info;
	std::vector<SendBuf*> _sendbuf_arr;
	std::string _job_id;

	unsigned int _proc_timeout;
	unsigned int _comm_timeout;
};

#endif /* __H_CFM_CLUSTER_H__ */
