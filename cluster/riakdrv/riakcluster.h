#ifndef __H_RIAKCLUSTER_H__
#define __H_RIAKCLUSTER_H__

#include <vector>
#include <list>
#include <map>
#include <thread>
#include <riakdrv/riakconnect.h>
#include <cr_class/cr_multithread.h>
#include <time.h>
#include <cfm_drv/dm_ib_sort_load.h>
#define KEYSPACE_USE_FILE_MODE                  // keyspce的分配采用文件模式
#define REUSE_CLUSTER_FM_KEYSPACE_IN_FILE_MODE      // 将cluster_fm分配的keyspace也回收到文件模式分配的keyspace中重复使用

class RiakCluster {
public:
	typedef int (*async_callback_t)(const std::string *tag_data, const int op_ret, const std::string &op_data);

	typedef enum {ST_COUNT=1, ST_SIZE=2} status_type_t;
	typedef enum {SR_ALL=(INT_MIN), SR_ALL_FAILED=(INT_MIN+1)} special_retcode_t;

	RiakCluster(size_t timeout=60, size_t max_connect_per_node=2,
		size_t max_retry=3, size_t _rebuild_timeout=3600);
	virtual ~RiakCluster();

	// result > 0 : success connect to (result) nodes
	// result < 0 : error, and (0 - result) is error number
	// result == 0: undefined
	int Rebuild(const char *conn_str=NULL, const char *_srv_name=NULL);

	int64_t QueryStatus(const status_type_t status_type, const int8_t opcode=UNDEFINED_REQ,
		const special_retcode_t retcode=SR_ALL);

	int64_t KeySpaceAlloc(const std::string &bucket, const int32_t tabid, const int32_t attrid,
		const size_t min_alloc_count=128, const double autofree_timeout_sec=(86400*15));

	int KeySpaceFree(const std::string &bucket, const int32_t tabid, const int32_t attrid,
		const std::vector<int64_t> &ks_arr);

    
	int KeySpaceFree(const std::string &bucket, const int32_t tabid, const int32_t attrid,
		const int64_t& ks);

	int KeySpaceSolidify(const std::string &bucket, const int32_t tabid, const int32_t attrid,
		const std::vector<int64_t> &ks_arr);

	int Get(
		const std::string &bucket,
		const std::string &key,
		std::string &value,
		const std::string *queryparams_p=NULL,
		const std::string *spec_connect_id_p=NULL,
		const std::string *never_connect_id_p=NULL
	);

	int Put(
		const std::string &bucket,
		const std::string &key,
		const std::string &value,
		const std::string *queryparams_p=NULL,
		size_t *stored_size=NULL,
		const std::string *spec_connect_id_p=NULL,
		const std::string *never_connect_id_p=NULL
	);

	int Del(
		const std::string &bucket,
		const std::string &key,
		const std::string *queryparams_p=NULL,
		const std::string *spec_connect_id_p=NULL,
		const std::string *never_connect_id_p=NULL
	);

        int GetEx(
		const std::string &bucket,
		const std::string &key,
		const std::string *queryparams_p,
		const uint32_t flags,
		RpbContent *content_out_p,
		std::string *vclock_io_p,
		const std::string *spec_connect_id_p=NULL,
		const std::string *never_connect_id_p=NULL
	);

        int PutEx(
		const std::string &bucket,
		const std::string *key_in_p,
		const std::string *queryparams_p,
		const uint32_t flags,
		RpbContent *content_io_p,
		std::string *vclock_io_p,
		std::string *key_out_p,
		const std::string *spec_connect_id_p=NULL,
		const std::string *never_connect_id_p=NULL
	);

        int DelEx(
		const std::string &bucket,
		const std::string &key,
		const std::string *queryparams_p,
		const std::string *vclock_in_p,
		const std::string *spec_connect_id_p=NULL,
		const std::string *never_connect_id_p=NULL
	);

	int ListBuckets(
		std::vector<std::string> &buckets,
		const std::string *spec_connect_id_p=NULL,
		const std::string *never_connect_id_p=NULL
	);

	int ListKeys(
		const std::string &bucket,
		std::vector<std::string> &keys,
		const std::string *spec_connect_id_p=NULL,
		const std::string *never_connect_id_p=NULL
	);

	int Index(
		const std::string &bucket,
		const std::string &index,
		const std::string &range_min,
		const std::string &range_max,
		std::vector<std::string> *keys_arr_p,
		std::map<std::string,std::string> *kv_map_p,
		const size_t max_results=0,
		std::string *continuation_io_p=NULL,
		const std::string *spec_connect_id_p=NULL,
		const std::string *never_connect_id_p=NULL
	);

	void *AsyncInit(const size_t thread_count, const size_t queue_max_size=64,
		const unsigned int retry_count=10);

	int AsyncGetWithCallback(
		void *handle,
		const std::string &bucket,
		const std::string &key,
		const int64_t query_id,
		const std::string *queryparams_p,
		async_callback_t callback,
		const std::string *tag_data_p
	);

	int AsyncPutWithCallback(
		void *handle,
		const std::string &bucket,
		const std::string &key,
		const std::string &value,
		const std::string *queryparams_p,
		async_callback_t callback,
		const std::string *tag_data_p
	);

	int AsyncDelWithCallback(
		void *handle,
		const std::string &bucket,
		const std::string &key,
		const std::string *queryparams_p,
		async_callback_t callback,
		const std::string *tag_data_p
	);

	int AsyncGet(
		void *handle,
		const std::string &bucket,
		const std::string &key,
		const int64_t query_id,
		const std::string *queryparams_p=NULL
	);

	int AsyncPopValue(
		void *handle,
		const std::string &bucket,
		const std::string &key,
		const int64_t query_id,
		std::string &value
	);

	int AsyncClearValue(void *handle,
		const int64_t query_id
	);

	int AsyncPut(
		void *handle,
		const std::string &bucket,
		const std::string &key,
		const std::string &value,
		const std::string *queryparams_p=NULL
	);

	int AsyncDel(
		void *handle,
		const std::string &bucket,
		const std::string &key,
		const std::string *queryparams_p=NULL
	);

	ssize_t AsyncWait(
		void *handle,
		std::string &errmsg,
		double timeout_sec=0.0
	);

	bool IsReady();

	bool get_first_use() const;
	size_t get_rebuild_timeout() const;
	std::string req_to_key(const std::string &bucket, const std::string &key, const int64_t query_id) const;
	std::string get_clusterinfo_bucket() const;

	bool need_rebuild;
private:
	CR_DataControl::CondMutex _cond_mutex;

	bool _is_ready;
	bool _first_use;
	size_t _rebuild_timeout;
	size_t _timeout;
	size_t _max_connect_per_node;
	size_t _max_retry;
	bool _has_readonly_cluster;
	CR_MultiThread thread_pool;
	std::thread *_rebuilder_thread_p;
	volatile bool _rebuilder_thread_stop;
	std::string _clusterinfo_str;
	std::list<RiakConnect*> _connect_list;
	std::map<std::string,size_t> _node_connects_map;

	CFMCluster *_cfm_cluster_p;

	std::list<int64_t> _alloced_id_list;

	void *stat_data_p;

	RiakConnect *get_connect(double timeout_sec=0, const std::string *spec_connect_id_p=NULL,
		const std::string *never_connect_id_p=NULL);
	void put_connect(RiakConnect *conn, int last_ret,
		int8_t last_code_req=UNKNOWN_REQ, size_t last_op_size=0);
	int parse_cluster_info(const std::string &cluster_str, std::vector<std::string> &cluster_info);
};

#endif /* __H_RIAKCLUSTER_H__ */
