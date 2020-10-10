#ifndef __H_RIAKCONNECT_H__
#define __H_RIAKCONNECT_H__

#include <vector>
#include <map>
#include <cr_class/cr_datacontrol.h>
#include <cr_class/cr_simplecomm.h>
#include <cr_class/cr_msgcode.h>
#include <riakdrv/riak.pb.h>
#include <riakdrv/riak.pb.h>
#include <riakdrv/riak_kv.pb.h>
#include <riakdrv/riak_search.pb.h>

namespace RiakMisc {
	double GetContentTime(const RpbContent &content);
	std::string GetUserMeta(const RpbContent &content, const std::string &name);
	int SetUserMeta(RpbContent &content, const std::string &name, const std::string &value);
	int DelUserMeta(RpbContent &content, const std::string &name);
	int MergeUserMeta(RpbContent &content, const std::map<std::string,std::string> &usermetas);
	int SetIndexes(RpbContent &content, const std::string &name, const std::string &value);
	int MergeIndexes(RpbContent &content, const std::map<std::string,std::string> &indexes);
	std::string BKeyToKIKey(const std::string &bucket, const std::string &key,
		const std::string *write_id_p=NULL);
};

class RiakConnect {
public:
	typedef int(*riak_modify_callback_t)(const std::string& bucket, const std::string& key,
		RpbContent &content, const std::string &callback_params, const size_t retry_count);

	RiakConnect(const std::string &id, const int proc_timeout=30, const int comm_timeout=10);
	virtual ~RiakConnect();

	int Connect(const std::string &hostname, const std::string &srvname);

	int Ping(std::string *errmsg=NULL);

	int GetClientId(std::string &client_id,
		std::string *errmsg=NULL);

	int SetClientId(const std::string &client_id,
		std::string *errmsg=NULL);

	int GetServerInfo(std::string &node,
		std::string &server_version,
		std::string *errmsg=NULL);

	int Get(const std::string &bucket,
		const std::string &key,
		std::string &value,
		const std::string *queryparams=NULL,
		size_t *stored_size=NULL,
		std::string *errmsg=NULL);

	int Put(const std::string &bucket,
		const std::string &key,
		const std::string &value,
		const std::string *queryparams_p=NULL,
		size_t *stored_size=NULL,
		std::string *errmsg=NULL);

	int Del(const std::string &bucket,
		const std::string &key,
		const std::string *queryparams_p=NULL,
		std::string *errmsg=NULL);

	int GetEx(const std::string &bucket,
		const std::string &key,
		const std::string *queryparams_p,
		const uint32_t flags,
		RpbContent *content_out_p,
		std::string *vclock_io_p,
		std::string *errmsg);

	int PutEx(const std::string &bucket,
		const std::string *key_in_p,
		const std::string *queryparams_p,
		const uint32_t flags,
		RpbContent *content_io_p,
		std::string *vclock_io_p,
		std::string *key_out_p,
		std::string *errmsg);

	int DelEx(const std::string &bucket,
		const std::string &key,
		const std::string *queryparams_p,
		const std::string *vclock_in_p,
		std::string *errmsg);

	int ListBuckets(std::vector<std::string> &buckets,
		std::string *errmsg=NULL);

	int ListKeys(const std::string &bucket,
		std::vector<std::string> &keys,
		std::string *errmsg=NULL);

	int GetBucket(const std::string &bucket,
                RpbBucketProps &props,
		std::string *errmsg=NULL);

	int SetBucket(const std::string &bucket,
                const RpbBucketProps &props,
		std::string *errmsg=NULL);

	int Index(const std::string &bucket,
		const std::string &index,
		const std::string &range_min,
		const std::string &range_max,
		std::vector<std::string> *keys_arr_p,
		std::map<std::string,std::string> *kv_map_p,
		const size_t max_results=0,
		std::string *continuation_io_p=NULL,
		std::string *errmsg=NULL);

	double GetTimeOfDay();

	uint32_t get_max_retry();
	void set_max_retry(uint32_t max_retry);

	const std::string &get_id() const;

	void set_tag(void *p);

	void * get_tag();

public: // add by liujs
        void SetLastUseTime(const double lstUseTime){
            this->_last_use_time = lstUseTime;
        }
        double GetLastUseTime(){
            return this->_last_use_time ;
        }
        void DisConnect(){
            if(this->_connected){ 
                    this->_connected = false;                    
                    DPRINTFX(20, "Node %s is DisConnect,add by liujs \n", _id.c_str());
                    this->_comm.DisConnect();
            }             
        }

protected:
	CR_SimpleComm _comm;
	std::string _id;
	std::map<std::string,std::string> _tags;
        double _last_use_time;            

private:
	typedef int(*riak_operate_callback_t)(const int8_t code_req, const int8_t code_resp,
		const std::string& msg_buf, const std::string &callback_params);

	CR_DataControl::Spin _lck;

	bool _connected;

	uint32_t _max_retry;
	std::string _rand_id;
	double _last_client_time;
	double _client_time_bias;

	int  _parse_err(const int, const int8_t, const std::string*, std::string*) const;

	int _raw_comm(const int8_t, const std::string&, int8_t&, std::string&, std::string*);
	int _raw_operate(const int8_t, const void*, void*, const std::string*, std::string*);

	int _raw_get(RpbGetReq&, RpbGetResp&, std::string*);
	int _raw_put(RpbPutReq&, RpbPutResp&, std::string*);
	int _raw_del(RpbDelReq&, std::string*);

	int _get_ex(const std::string &bucket, const std::string &key, const std::string *queryparams_p,
		const uint32_t flags, RpbContent *content_out_p, std::string *vclock_io_p,
		std::string *errmsg);

	int _put_ex(const std::string &bucket, const std::string *key_in_p, const std::string *queryparams_p,
		const uint32_t flags, RpbContent *content_io_p, std::string *vclock_io_p,
		std::string *key_out_p, std::string *errmsg);

	int _del_ex(const std::string &bucket, const std::string &key, const std::string *queryparams_p,
		const std::string *vclock_in_p, std::string *errmsg);
};

#endif /* __H_RIAKCONNECT_H__ */
