#include <stdio.h>
#include <errno.h>
#include <math.h>
#include <list>
#include <vector>

#include <cr_class/cr_datacontrol.h>
#include <riakdrv/riakconnect.h>
#include <riakdrv/riak_common.h>

#define __LIST_KEYS_KEY_ARRAY_PTR__	"LISTKEYS_KEYARR_P"
#define __MAPRED_RESP_LIST_PTR__	"MAPRED_RESPLIST_P"
#define __INDEX_KEY_ARRAY_PTR__		"INDEX_KEYARR_P"
#define __INDEX_KV_MAP_PTR__		"INDEX_KVMAP_P"
#define __INDEX_CONTINUATION_PTR__	"INDEX_CONTINUATION_P"

#define __LOCK_ID_SAVE_NAME__		"LOCK_ID"
#define __LOCK_TIMEOUT_SAVE_NAME__	"LOCK_TIMEOUT"

#define __READABLE_ID_NAME__		"READABLE_ID"
#define __RANDOM_ID_NAME__		"RANDOM_ID"

#define __RIAKC_META_NAME_LOCK_OWNER	"CR-RiakConnect-Lock-Owner"
#define __RIAKC_META_NAME_LOCK_LEVEL	"CR-RiakConnect-Lock-Level"
#define __RIAKC_META_NAME_LOCK_EXPIRE	"CR-RiakConnect-Lock-Expire"

static int _riak_multi_operate_default(const int8_t, const int8_t, const std::string&, const std::string&);

double
RiakMisc::GetContentTime(const RpbContent &content)
{
    double ret = 0.0;
    if (content.has_last_mod()) {
        ret = content.last_mod();
    }
    if (content.has_last_mod_usecs()) {
        ret += ((double)content.last_mod_usecs()) / 1000000;
    }
    return ret;
}

int
RiakMisc::SetUserMeta(RpbContent &content, const std::string &name, const std::string &value)
{
    int ret = 0;
    int find_pos = -1;

    for (int i=0; i<content.usermeta_size(); i++) {
        if (content.usermeta(i).key().compare(name) == 0) {
            find_pos = i;
            break;
        }
    }

    if (find_pos >= 0) {
        content.mutable_usermeta(find_pos)->set_value(value);
    } else {
        RpbPair *one_meta = content.add_usermeta();
        one_meta->set_key(name);
        one_meta->set_value(value);
    }

    return ret;
}

int
RiakMisc::MergeUserMeta(RpbContent &content, const std::map<std::string,std::string> &usermetas)
{
    int ret = 0;
    std::map<std::string,std::string>::const_iterator it;
    for (it=usermetas.begin(); it!=usermetas.end(); it++) {
        ret = SetUserMeta(content, it->first, it->second);
        if (ret)
            break;
    }
    return ret;
}

std::string
RiakMisc::GetUserMeta(const RpbContent &content, const std::string &name)
{
    std::string ret = CR_Class_NS::blank_string;
    errno = ENOENT;

    for (int i=0; i<content.usermeta_size(); i++) {
        const RpbPair &one_meta = content.usermeta(i);
        if (one_meta.key().compare(name) == 0) {
            errno = 0;
            if (one_meta.has_value()) {
                ret = one_meta.value();
            }
        }
    }

    return ret;
}

int
RiakMisc::DelUserMeta(RpbContent &content, const std::string &name)
{
    int ret = ENOENT;
    std::map<std::string,std::string> usermeta_map;
    std::map<std::string,std::string>::iterator it;

    for (int i=0; i<content.usermeta_size(); i++) {
        const RpbPair &one_meta = content.usermeta(i);
        const std::string &meta_name = one_meta.key();
        if (meta_name.compare(name) == 0) {
            ret = 0;
            continue;
        }
        if (usermeta_map.find(meta_name) == usermeta_map.end()) {
            if (one_meta.has_value()) {
                usermeta_map[meta_name] = one_meta.value();
            } else {
                usermeta_map[meta_name] = CR_Class_NS::blank_string;
            }
        }
    }

    if (!ret) {
        content.clear_usermeta();
        for (it=usermeta_map.begin(); it!=usermeta_map.end(); it++) {
            RpbPair *one_meta_p = content.add_usermeta();
            one_meta_p->set_key(it->first);
            one_meta_p->set_value(it->second);
        }
    }

    return ret;
}

int
RiakMisc::SetIndexes(RpbContent &content, const std::string &name, const std::string &value)
{
    int ret = 0;
    int find_pos = -1;

    std::string true_name = name + "_bin";

    for (int i=0; i<content.indexes_size(); i++) {
        if (content.indexes(i).key().compare(true_name) == 0) {
            find_pos = i;
            break;
        }
    }

    if (find_pos >= 0) {
        content.mutable_indexes(find_pos)->set_value(value);
    } else {
        RpbPair *one_index = content.add_indexes();
        one_index->set_key(true_name);
        one_index->set_value(value);
    }

    return ret;
}

int
RiakMisc::MergeIndexes(RpbContent &content, const std::map<std::string,std::string> &indexes)
{
    int ret = 0;
    std::map<std::string,std::string>::const_iterator it;
    for (it=indexes.begin(); it!=indexes.end(); it++) {
        ret = SetIndexes(content, it->first, it->second);
        if (ret)
            break;
    }
    return ret;
}

std::string
RiakMisc::BKeyToKIKey(const std::string &bucket, const std::string &key, const std::string *write_id_p)
{
    std::string ret = RIAK_KEYINFO_PREFIX;
    ret.append("-B[");
    ret.append(bucket);
    ret.append("]K[");
    ret.append(key);
    if (write_id_p) {
        ret.append("]WI[");
        ret.append(*write_id_p);
    }
    ret.append("]");
    return ret;
}

/////////////////////////
/////////////////////////

RiakConnect::RiakConnect(const std::string &id, const int proc_timeout, const int comm_timeout)
{
    this->_id = id;
    this->_rand_id = this->_id + ":" + CR_Class_NS::randstr(4);

    this->_comm.set_timeout_send(comm_timeout);
    this->_comm.set_timeout_recv(proc_timeout);
    this->_connected = false;
    this->_max_retry = 10;
    this->_last_use_time = 0.0;

    this->_last_client_time = 0.0;
    this->_client_time_bias = 0.0;
}

RiakConnect::~RiakConnect()
{
    if(this->_connected){ // fix susa-375,add by liujs
            this->_connected = false;
            this->_comm.DisConnect();
    }
    this->_last_use_time= 0.0;
}

void
RiakConnect::set_tag(void *p)
{
    this->_comm.set_tag(p);
}

void *
RiakConnect::get_tag()
{
    return this->_comm.get_tag();
}

int
RiakConnect::Connect(const std::string &hostname, const std::string &srvname)
{
    int ret;

    ret = this->_comm.Connect(hostname, srvname);

    if (ret == 0) {
        this->_connected = true;

        std::string mixed_id;

        mixed_id = CR_Class_NS::str_setparam(mixed_id, __READABLE_ID_NAME__, this->get_id());
        mixed_id = CR_Class_NS::str_setparam(mixed_id, __RANDOM_ID_NAME__, CR_Class_NS::randstr(1));
        ret = this->SetClientId(mixed_id);

        if (ret) {
            this->_connected = false;
            this->_comm.DisConnect();
        }
    }

    return ret;
}

int
RiakConnect::_parse_err(const int _errno, const int8_t code_req, const std::string *errbuf, std::string *errmsg) const
{
    DPRINTFX(200, "errno=%s, errmsg=%s, code_req=%d, errbuf=%s\n",
      CR_Class_NS::strerrno(_errno), CR_Class_NS::strerror(_errno),
      code_req, (errbuf)?(errbuf->c_str()):("NULL"));

    int ret = ECANCELED;

    if (_errno > 0)
        ret = _errno;

    switch (code_req) {
    case RPB_PUT_REQ :
        if (errbuf) {
            if (errbuf->compare("modified") == 0) {
                ret = ETXTBSY;
            } else if (errbuf->compare("notfound") == 0) {
                ret = ENOENT;
            } else if (errbuf->compare("match_found") == 0) {
                ret = EEXIST;
            }
        }
        break;
    default :
        break;
    }

    if (errmsg) {
        RpbErrorResp msg_syserr;
        msg_syserr.set_errmsg(CR_Class_NS::strerror(_errno));
        msg_syserr.set_errcode(_errno);
        *errmsg = "[System] => ";
        *errmsg += msg_syserr.ShortDebugString();
    }

    if (errbuf) {
        RpbErrorResp msg_resp;
        if (msg_resp.ParseFromString(*errbuf)) {
            if (msg_resp.errcode() > 0) {
                ret = msg_resp.errcode();
            } else {
                switch (code_req) {
                case RPB_PUT_REQ :
                    if (msg_resp.errmsg().compare("modified") == 0) {
                        ret = ETXTBSY;
                    } else if (msg_resp.errmsg().compare("notfound") == 0) {
                        ret = ENOENT;
                    } else if (msg_resp.errmsg().compare("match_found") == 0) {
                        ret = EEXIST;
                    }
                    break;
                default :
                    break;
                }
            }

            if (errmsg) {
                *errmsg += " [Riak] => ";
                *errmsg += msg_resp.ShortDebugString();
            }
        }
    }

    return ret;
}

const std::string &
RiakConnect::get_id() const
{
    return this->_id;
}

uint32_t
RiakConnect::get_max_retry()
{
    uint32_t ret = 0;

    this->_lck.lock();
    ret = this->_max_retry;
    this->_lck.unlock();

    return ret;
}

void
RiakConnect::set_max_retry(uint32_t max_retry)
{
    this->_lck.lock();
    this->_max_retry = max_retry;
    this->_lck.unlock();
}

int
RiakConnect::_raw_comm(const int8_t code_req, const std::string &str_req, int8_t &code_resp,
	std::string &str_resp, std::string *errmsg)
{
    int ret = 0;
    int8_t code_resp_local = code_resp;

    ret = this->_comm.SendAndRecv(code_req, &str_req, code_resp_local, &str_resp, this->_max_retry);

    if (ret == 0) {
        if (code_resp_local != code_resp) {
            code_resp = code_resp_local;
            ret = this->_parse_err(ret, code_req, &str_resp, errmsg);
        }
    } else
        this->_parse_err(ret, code_req, NULL, errmsg);

    return ret;
}

static int
_riak_multi_operate_default(const int8_t code_req, const int8_t code_resp,
	const std::string& msg_buf, const std::string &callback_params)
{
    int ret = -1;

    switch (code_req) {
    case RPB_LIST_KEYS_REQ :
        if (code_resp == RPB_LIST_KEYS_RESP) {
            RpbListKeysResp msg_resp;

            if (msg_resp.ParseFromString(msg_buf)) {
                if (msg_resp.has_done())
                    if (msg_resp.done())
                        ret = 0;

                std::vector<std::string> *key_list_p = (std::vector<std::string>*)CR_Class_NS::str2ptr(
                	CR_Class_NS::str_getparam(callback_params, __LIST_KEYS_KEY_ARRAY_PTR__));

                if (key_list_p) {
                    size_t local_key_count = msg_resp.keys_size();
                    for (size_t i=0; i<local_key_count; i++) {
                        key_list_p->push_back(msg_resp.keys(i));
                    }
                }
            } else
                ret = ENOMSG;
        } else
            ret = ECANCELED;
        break;
    case RPB_MAPRED_REQ :
        if (code_resp == RPB_MAPRED_RESP) {
            RpbMapRedResp *msg_resp_p = new RpbMapRedResp;

            if (msg_resp_p->ParseFromString(msg_buf)) {
                if (msg_resp_p->has_done())
                    if (msg_resp_p->done())
                        ret = 0;

                std::list<RpbMapRedResp*> *mapred_resp_list_p = (std::list<RpbMapRedResp*>*)CR_Class_NS::str2ptr(
                	CR_Class_NS::str_getparam(callback_params, __MAPRED_RESP_LIST_PTR__));

                if (mapred_resp_list_p) {
                    mapred_resp_list_p->push_back(msg_resp_p);
                } else
                    delete msg_resp_p;
            } else
                ret = ENOMSG;
        } else
            ret = ECANCELED;
        break;
    case RPB_INDEX_REQ :
        if (code_resp == RPB_INDEX_RESP) {
            RpbIndexResp msg_resp;

            if (msg_resp.ParseFromString(msg_buf)) {
                if (msg_resp.has_done())
                    if (msg_resp.done())
                        ret = 0;

                std::vector<std::string> *keys_arr_p = (std::vector<std::string>*)CR_Class_NS::str2ptr(
                  CR_Class_NS::str_getparam(callback_params, __INDEX_KEY_ARRAY_PTR__));

                std::map<std::string,std::string> *kv_map_p = (std::map<std::string,std::string>*)
                  CR_Class_NS::str2ptr(CR_Class_NS::str_getparam(callback_params, __INDEX_KV_MAP_PTR__));

                std::string *continuation_io_p = (std::string*)CR_Class_NS::str2ptr(
                  CR_Class_NS::str_getparam(callback_params, __INDEX_CONTINUATION_PTR__));

                if (keys_arr_p) {
                    for (int i=0; i<msg_resp.keys_size(); i++) {
                        keys_arr_p->push_back(msg_resp.keys(i));
                    }
                }

                if (kv_map_p) {
                    for (int i=0; i<msg_resp.results_size(); i++) {
                        const RpbPair &one_pair = msg_resp.results(i);
                        (*kv_map_p)[one_pair.key()] = one_pair.value();
                    }
                }

                if (continuation_io_p) {
                    if (msg_resp.has_continuation()) {
                        *continuation_io_p = msg_resp.continuation();
                    }
                }
            } else
                ret = ENOMSG;
        } else
            ret = ECANCELED;
        break;
    default :
        ret = ENOPROTOOPT;
        break;
    }

    return ret;
}

int
RiakConnect::_raw_operate(const int8_t code_req, const void *req_p, void *resp_or_callback_p,
	const std::string *callback_params_p, std::string *errmsg)
{
    int ret = 0;
    int8_t code_resp = code_req + 1;
    std::string str_req;
    std::string str_resp;
    bool req_conv_ok = true;
    bool resp_conv_ok = true;
    bool do_callback = false;
    const std::string *_local_callparam_p = callback_params_p;

    if (!_local_callparam_p)
        _local_callparam_p = &(CR_Class_NS::blank_string);

    switch (code_req) {
    case RPB_PING_REQ :
        break;
    case RPB_GET_CLIENTID_REQ :
        break;
    case RPB_SET_CLIENTID_REQ :
        req_conv_ok = ((const RpbSetClientIdReq*)req_p)->SerializeToString(&str_req);
        break;
    case RPB_GET_SERVERINFO_REQ :
        break;
    case RPB_GET_REQ :
        req_conv_ok = ((const RpbGetReq*)req_p)->SerializeToString(&str_req);
        break;
    case RPB_PUT_REQ :
        req_conv_ok = ((const RpbPutReq*)req_p)->SerializeToString(&str_req);
        break;
    case RPB_DEL_REQ :
        req_conv_ok = ((const RpbDelReq*)req_p)->SerializeToString(&str_req);
        break;
    case RPB_LIST_BUCKETS_REQ :
        break;
    case RPB_LIST_KEYS_REQ :
        do_callback = true;
        req_conv_ok = ((const RpbListKeysReq*)req_p)->SerializeToString(&str_req);
        break;
    case RPB_GET_BUCKET_REQ :
        req_conv_ok = ((const RpbGetBucketReq*)req_p)->SerializeToString(&str_req);
        break;
    case RPB_SET_BUCKET_REQ :
        req_conv_ok = ((const RpbSetBucketReq*)req_p)->SerializeToString(&str_req);
        break;
    case RPB_MAPRED_REQ :
        do_callback = true;
        req_conv_ok = ((const RpbMapRedReq*)req_p)->SerializeToString(&str_req);
        break;
    case RPB_INDEX_REQ :
        do_callback = true;
        req_conv_ok = ((const RpbIndexReq*)req_p)->SerializeToString(&str_req);
        break;
    case RPB_SEARCH_QUERY_REQ :
        req_conv_ok = ((const RpbSearchQueryReq*)req_p)->SerializeToString(&str_req);
        break;
    default :
        ret = ENOPROTOOPT;
        break;
    }

    if (!req_conv_ok) {
        DPRINTFX(20, "code_req == %d , but req_conv_ok != true\n", code_req);
        ret = EINVAL;
    }

    if (!ret) {
        if (do_callback) {
            bool reset_conn = false;

            this->_lck.lock();

            if (this->_connected) {
                ret = this->_comm.ProtoSend(code_req, &str_req);
                if (ret == 0) {
                    while(1) {
                        ret = this->_comm.ProtoRecv(code_resp, &str_resp);
                        if (ret) {
                            reset_conn = true;
                            break;
                        }

                        if (code_resp == RPB_ERROR_RESP) {
                            reset_conn = true;
                            ret = this->_parse_err(0, code_req, &str_resp, errmsg);
                            break;
                        }

                        if (resp_or_callback_p) {
                            ret = ((riak_operate_callback_t)resp_or_callback_p)(code_req, code_resp,
                            	str_resp, *_local_callparam_p);
                        } else {
                            ret = _riak_multi_operate_default(code_req, code_resp, str_resp, *_local_callparam_p);
                        }

                        if (ret == -1)
                            continue;

                        if (ret != 0)
                            reset_conn = true;

                        break;
                    }
                } else {
                    reset_conn = true;
                }
            } else
                ret = ENOTCONN;

            this->_lck.unlock();

            if (reset_conn) {
                this->_comm.ReConnect();
            }
        } else {
            ret = this->_raw_comm(code_req, str_req, code_resp, str_resp, errmsg);

            if (!ret) {
                if (code_resp == code_req + 1) {
                    switch (code_req) {
                    case RPB_PING_REQ :
                        break;
                    case RPB_GET_CLIENTID_REQ :
                        resp_conv_ok = ((RpbGetClientIdResp*)resp_or_callback_p)->ParseFromString(str_resp);
                        break;
                    case RPB_SET_CLIENTID_REQ :
                        break;
                    case RPB_GET_SERVERINFO_REQ :
                        resp_conv_ok = ((RpbGetServerInfoResp*)resp_or_callback_p)->ParseFromString(str_resp);
                        break;
                    case RPB_GET_REQ :
                        resp_conv_ok = ((RpbGetResp*)resp_or_callback_p)->ParseFromString(str_resp);
                        break;
                    case RPB_PUT_REQ :
                        resp_conv_ok = ((RpbPutResp*)resp_or_callback_p)->ParseFromString(str_resp);
                        break;
                    case RPB_DEL_REQ :
                        break;
                    case RPB_LIST_BUCKETS_REQ :
                        resp_conv_ok = ((RpbListBucketsResp*)resp_or_callback_p)->ParseFromString(str_resp);
                        break;
                    case RPB_GET_BUCKET_REQ :
                        resp_conv_ok = ((RpbGetBucketResp*)resp_or_callback_p)->ParseFromString(str_resp);
                        break;
                    case RPB_SET_BUCKET_REQ :
                        break;
                    case RPB_SEARCH_QUERY_REQ :
                        resp_conv_ok = ((RpbSearchQueryResp*)resp_or_callback_p)->ParseFromString(str_resp);
                        break;
                    default :
                        ret = ENOPROTOOPT;
                        break;
                    }

                    if (!resp_conv_ok)
                        ret = ENOMSG;
                } else {
                    switch (code_req) {
                    default :
                        ret = EPROTONOSUPPORT;
                        break;
                    }
                }
            }
        }
    }

    return ret;
}

int
RiakConnect::Ping(std::string *errmsg)
{
    return this->_raw_operate(RPB_PING_REQ, NULL, NULL, NULL, errmsg);
}

int
RiakConnect::GetClientId(std::string &client_id, std::string *errmsg)
{
    int ret = 0;
    RpbGetClientIdResp msg_resp;

    ret = this->_raw_operate(RPB_PING_REQ, NULL, &msg_resp, NULL, errmsg);

    if (!ret)
        client_id = msg_resp.client_id();

    return ret;
}

int
RiakConnect::SetClientId(const std::string &client_id, std::string *errmsg)
{
    RpbSetClientIdReq msg_req;

    msg_req.set_client_id(client_id);
    return this->_raw_operate(RPB_SET_CLIENTID_REQ, &msg_req, NULL, NULL, errmsg);
}

int
RiakConnect::GetServerInfo(std::string &node, std::string &server_version, std::string *errmsg)
{
    int ret = 0;
    RpbGetServerInfoResp msg_resp;

    ret = this->_raw_operate(RPB_GET_SERVERINFO_REQ, NULL, &msg_resp, NULL, errmsg);

    if (!ret) {
        if (msg_resp.has_node())
            node = msg_resp.node();

        if (msg_resp.has_server_version())
            server_version = msg_resp.server_version();
    }

    return ret;
}

int
RiakConnect::_raw_get(RpbGetReq &get_req, RpbGetResp &get_resp, std::string *errmsg)
{
    int ret = this->_raw_operate(RPB_GET_REQ, &get_req, &get_resp, NULL, errmsg);

    DPRINTFX(300, "\nget_req => %s\nget_resp=> %s\n",
    	get_req.DebugString().c_str(), get_resp.DebugString().c_str());

    return ret;
}

int
RiakConnect::Get(const std::string &bucket, const std::string &key, std::string &value,
	const std::string *queryparams_p, size_t *stored_size, std::string *errmsg)
{
    int ret = 0;
    RpbContent get_content;

    ret = this->GetEx(bucket, key, queryparams_p, 0, &get_content, NULL, errmsg);
    if (!ret) {
        const std::string *value_p = &(get_content.value());
        std::string value_transfer_decompress;
        std::string value_save_decompress;
        if (RiakMisc::GetUserMeta(get_content, RIAK_META_NAME_TRANSFER_COMPRESS) == RIAK_META_VALUE_TRUE) {
            if (CR_Class_NS::decompressx(*value_p, value_transfer_decompress) == 0) {
                value_p = &value_transfer_decompress;
            }
        }
        if (RiakMisc::GetUserMeta(get_content, RIAK_META_NAME_SAVE_COMPRESS) == RIAK_META_VALUE_TRUE) {
            if (CR_Class_NS::decompressx(*value_p, value_save_decompress) == 0) {
                value_p = &value_save_decompress;
            }
        }
        value = *value_p;

        if (stored_size) {
            *stored_size = CR_Class_NS::str2u64(RiakMisc::GetUserMeta(get_content, RIAK_META_NAME_STORED_SIZE));
        }
    }

    return ret;
}

int
RiakConnect::GetEx(const std::string &bucket, const std::string &key, const std::string *queryparams_p,
	const uint32_t flags, RpbContent *content_out_p, std::string *vclock_io_p, std::string *errmsg)
{
    if (key.find(RIAK_LOCK_SUFFIX) != std::string::npos)
        return EPERM;

    return this->_get_ex(bucket, key, queryparams_p, flags, content_out_p, vclock_io_p, errmsg);
}

int
RiakConnect::_get_ex(const std::string &bucket, const std::string &key, const std::string *queryparams_p,
	const uint32_t flags, RpbContent *content_out_p, std::string *vclock_io_p, std::string *errmsg)
{
    int ret = 0;
    RpbGetReq msg_req;
    RpbGetResp msg_resp;

    msg_req.set_bucket(bucket);
    msg_req.set_key(key);
    if (queryparams_p)
        msg_req.set_queryparams(*queryparams_p);
    if (vclock_io_p) {
        if (vclock_io_p->size() > 0) {
            msg_req.set_if_modified(*vclock_io_p);
        }
    }
    if (flags & RIAK_CONNECT_FLAG_RETURN_HEAD)
        msg_req.set_head(true);

    ret = this->_raw_get(msg_req, msg_resp, errmsg);

    if (!ret) {
        if (msg_resp.unchanged()) {
            ret = EEXIST;
        }
    }

    if (!ret) {
        int content_size = msg_resp.content_size();

        if (vclock_io_p) {
            if (msg_resp.has_vclock()) {
                *vclock_io_p = msg_resp.vclock();
            }
        }

        if (content_size == 1) {
            if (content_out_p)
                *content_out_p = msg_resp.content(0);
        } else if (content_size == 0) {
            ret = ENOENT;
        } else {
            ret = EMLINK;
        }
    }

    if (ret) {
        DPRINTFX(50, "ret=%s, msg=%s\n", CR_Class_NS::strerrno(ret), CR_Class_NS::strerror(ret));
    }

    return ret;
}

int
RiakConnect::_raw_put(RpbPutReq &put_req, RpbPutResp &put_resp, std::string *errmsg)
{
    int ret;

    if (!put_req.has_return_body())
        put_req.set_return_head(true);
    else if (!put_req.return_body())
        put_req.set_return_head(true);

    ret  = this->_raw_operate(RPB_PUT_REQ, &put_req, &put_resp, NULL, errmsg);

    DPRINTFX(300, "\nput_req => %s\nput_resp=> %s\n",
    	put_req.DebugString().c_str(), put_resp.DebugString().c_str());

    return ret;
}

int
RiakConnect::Put(const std::string &bucket, const std::string &key, const std::string &value,
	const std::string *queryparams_p, size_t *stored_size, std::string *errmsg)
{
    int ret = 0;

    RpbContent put_content;
    std::string value_compress;
    const std::string *value_p = &value;
    if (value.size() >= RIAK_CONNECT_TRANSFER_COMPRESS_MIN_SIZE) {
        if (CR_Class_NS::compressx_snappy(value, value_compress) == 0) {
            RiakMisc::SetUserMeta(put_content, RIAK_META_NAME_TRANSFER_COMPRESS, RIAK_META_VALUE_TRUE);
            value_p = &value_compress;
        }
    }
    if (queryparams_p) {
        if (CR_Class_NS::str_getparam(*queryparams_p, RIAK_META_NAME_SAVE_COMPRESS) == RIAK_META_VALUE_FALSE) {
            RiakMisc::SetUserMeta(put_content, RIAK_META_NAME_SAVE_COMPRESS, RIAK_META_VALUE_FALSE);
        }
    }

    put_content.set_value(*value_p);

    ret = this->PutEx(bucket, &key, queryparams_p, RIAK_CONNECT_FLAG_PUT_DIRECT, &put_content, NULL, NULL, errmsg);

    if ((!ret) && stored_size) {
        *stored_size = CR_Class_NS::str2u64(
          RiakMisc::GetUserMeta(put_content, RIAK_META_NAME_STORED_SIZE));
    }

    return ret;
}

int
RiakConnect::PutEx(const std::string &bucket, const std::string *key_in_p, const std::string *queryparams_p,
	const uint32_t flags, RpbContent *content_io_p, std::string *vclock_io_p, std::string *key_out_p,
	std::string *errmsg)
{
    if (key_in_p) {
        if (key_in_p->find(RIAK_LOCK_SUFFIX) != std::string::npos)
            return EPERM;
    }

    return this->_put_ex(bucket, key_in_p, queryparams_p, flags, content_io_p, vclock_io_p, key_out_p, errmsg);
}

int
RiakConnect::_put_ex(const std::string &bucket, const std::string *key_in_p, const std::string *queryparams_p,
	const uint32_t flags, RpbContent *content_io_p, std::string *vclock_io_p, std::string *key_out_p,
	std::string *errmsg)
{
    int ret = 0;

    do {
        RpbPutReq msg_req;
        RpbPutResp msg_resp;
        RpbContent *msg_content_p = msg_req.mutable_content();

        std::string put_id = CR_Class_NS::randstr(2);

        if (content_io_p) {
            *msg_content_p = *content_io_p;
        } else {
            msg_content_p->set_value(CR_Class_NS::blank_string);
        }
        msg_req.set_return_head(true);
        msg_req.set_bucket(bucket);
        if (key_in_p)
            msg_req.set_key(*key_in_p);
        if (vclock_io_p)
            msg_req.set_vclock(*vclock_io_p);
        if (queryparams_p)
            msg_req.set_queryparams(*queryparams_p);
        if (flags & RIAK_CONNECT_FLAG_RETURN_BODY) {
            msg_req.set_return_body(true);
            msg_req.set_return_head(false);
        }
        if (flags & RIAK_CONNECT_FLAG_PUT_IF_NONE_MATCH)
            msg_req.set_if_none_match(true);
        if (flags & RIAK_CONNECT_FLAG_PUT_IF_NOT_MODIFIED)
            msg_req.set_if_not_modified(true);

        // set normal key's WRITE ID
        RiakMisc::SetUserMeta(*msg_content_p, RIAK_META_NAME_PRIVATE_PUT_ID, put_id);

        ret = this->_raw_put(msg_req, msg_resp, errmsg);
        if (ret)
            break;

        if (msg_resp.content_size() > 1) {
            ret = EMLINK;
            break;
        }

        if (msg_resp.content_size() == 1) {
            const RpbContent &content_resp = msg_resp.content(0);

            if (RiakMisc::GetUserMeta(content_resp, RIAK_META_NAME_PRIVATE_PUT_ID) != put_id) {
                ret = ETXTBSY;
                break;
            }

            if (content_io_p) {
                *content_io_p = msg_resp.content(0);
            }
        } else {
            ret = EFAULT;
            break;
        }

        if (vclock_io_p) {
            if (msg_resp.has_vclock()) {
                *vclock_io_p = msg_resp.vclock();
            }
        }

        if (key_out_p) {
            if (msg_resp.has_key()) {
                *key_out_p = msg_resp.key();
            }
        }
    } while (0);

    if (ret) {
        DPRINTFX(50, "ret=%s, msg=%s\n", CR_Class_NS::strerrno(ret), CR_Class_NS::strerror(ret));
    }

    return ret;
}

int
RiakConnect::_raw_del(RpbDelReq &del_req, std::string *errmsg)
{
    int ret = this->_raw_operate(RPB_DEL_REQ, &del_req, NULL, NULL, errmsg);

    DPRINTFX(300, "\ndel_req => %s\n", del_req.DebugString().c_str());

    return ret;
}

int
RiakConnect::Del(const std::string &bucket, const std::string &key, const std::string *queryparams_p,
	std::string *errmsg)
{
    return this->DelEx(bucket, key, queryparams_p, NULL, errmsg);
}

int
RiakConnect::DelEx(const std::string &bucket, const std::string &key, const std::string *queryparams_p,
	const std::string *vclock_in_p, std::string *errmsg)
{
    if (key.find(RIAK_LOCK_SUFFIX) != std::string::npos)
        return EPERM;

    return this->_del_ex(bucket, key, queryparams_p, vclock_in_p, errmsg);
}

int
RiakConnect::_del_ex(const std::string &bucket, const std::string &key, const std::string *queryparams_p,
	const std::string *vclock_in_p, std::string *errmsg)
{
    int ret = 0;

    do {
        RpbDelReq msg_req;
        msg_req.set_bucket(bucket);
        msg_req.set_key(key);
        if (queryparams_p)
            msg_req.set_queryparams(*queryparams_p);
        if (vclock_in_p)
            msg_req.set_vclock(*vclock_in_p);

        ret = this->_raw_del(msg_req, errmsg);
    } while (0);

    if (ret) {
        DPRINTFX(50, "ret=%s, msg=%s\n", CR_Class_NS::strerrno(ret), CR_Class_NS::strerror(ret));
    }

    return ret;
}

int
RiakConnect::ListBuckets(std::vector<std::string> &buckets, std::string *errmsg)
{
    int ret = 0;
    RpbListBucketsResp msg_resp;

    buckets.clear();
    ret = this->_raw_operate(RPB_LIST_BUCKETS_REQ, NULL, &msg_resp, NULL, errmsg);

    if (!ret) {
        for (int i=0; i<msg_resp.buckets_size(); i++) {
            buckets.push_back(msg_resp.buckets(i));
        }
    }

    return ret;
}

int
RiakConnect::ListKeys(const std::string &bucket, std::vector<std::string> &keys, std::string *errmsg)
{
    RpbListKeysReq msg_req;
    std::string callback_param = CR_Class_NS::str_setparam("", __LIST_KEYS_KEY_ARRAY_PTR__,
    	CR_Class_NS::ptr2str(&keys));

    keys.clear();
    msg_req.set_bucket(bucket);

    return this->_raw_operate(RPB_LIST_KEYS_REQ, &msg_req, NULL, &callback_param, errmsg);
}

int
RiakConnect::GetBucket(const std::string &bucket, RpbBucketProps &props, std::string *errmsg)
{
    int ret = 0;
    RpbGetBucketReq msg_req;
    RpbGetBucketResp msg_resp;

    msg_req.set_bucket(bucket);

    ret = this->_raw_operate(RPB_GET_BUCKET_REQ, &msg_req, &msg_resp, NULL, errmsg);

    if (ret == 0)
        props = msg_resp.props();

    return ret;
}

int
RiakConnect::SetBucket(const std::string &bucket, const RpbBucketProps &props, std::string *errmsg)
{
    RpbSetBucketReq msg_req;

    msg_req.set_bucket(bucket);
    *(msg_req.mutable_props()) = props;

    return this->_raw_operate(RPB_SET_BUCKET_REQ, &msg_req, NULL, NULL, errmsg);
}

int
RiakConnect::Index(const std::string &bucket, const std::string &index,
	const std::string &range_min, const std::string &range_max,
	std::vector<std::string> *keys_arr_p, std::map<std::string,std::string> *kv_map_p,
	const size_t max_results, std::string *continuation_io_p, std::string *errmsg)
{
    RpbIndexReq msg_req;
    std::string callback_param;

    callback_param = CR_Class_NS::str_setparam(callback_param, __INDEX_KEY_ARRAY_PTR__,
    	CR_Class_NS::ptr2str(keys_arr_p));
    callback_param = CR_Class_NS::str_setparam(callback_param, __INDEX_KV_MAP_PTR__,
    	CR_Class_NS::ptr2str(kv_map_p));
    callback_param = CR_Class_NS::str_setparam(callback_param, __INDEX_CONTINUATION_PTR__,
    	CR_Class_NS::ptr2str(continuation_io_p));

    msg_req.set_bucket(bucket);
    msg_req.set_index(index + "_bin");
    msg_req.set_qtype(RpbIndexReq::range);
    msg_req.set_range_min(range_min);
    msg_req.set_range_max(range_max);
    if (kv_map_p) {
        msg_req.set_return_terms(true);
    }
    msg_req.set_stream(true);
    if (max_results > 0) {
        msg_req.set_max_results(max_results);
    }
    if (continuation_io_p) {
        if (continuation_io_p->size() > 0) {
            msg_req.set_continuation(*continuation_io_p);
        }
    }

    return this->_raw_operate(RPB_INDEX_REQ, &msg_req, NULL, &callback_param, errmsg);
}

double
RiakConnect::GetTimeOfDay()
{
    double ret = 0.0;
    double cur_client_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

    if ((this->_last_client_time == 0.0)
      || (fabs(cur_client_time - this->_last_client_time) > 60.0)) {
        RpbPutReq put_req;
        RpbPutResp put_resp;
        RpbDelReq del_req;

        put_req.set_bucket(RIAK_CONNECT_MEMORY_BUCKET_NAME);
        put_req.set_return_head(true);
        put_req.mutable_content()->set_value("");

        do {
            if (this->_raw_put(put_req, put_resp, NULL) != 0)
                break;

            std::string tmp_key = put_resp.key();

            del_req.set_bucket(RIAK_CONNECT_MEMORY_BUCKET_NAME);
            del_req.set_key(tmp_key);
            if (this->_raw_del(del_req, NULL) != 0)
                break;

            for (int i=0; i<put_resp.content_size(); i++) {
                double tmp_time = RiakMisc::GetContentTime(put_resp.content(i));
                if (tmp_time > ret) {
                    ret = tmp_time;
                }
            }
        } while (0);

        if (ret > 0.0) {
            this->_last_client_time = cur_client_time;
            this->_client_time_bias = ret - cur_client_time;
        }
    } else {
        this->_last_client_time = cur_client_time;
        ret = cur_client_time + this->_client_time_bias;
    }

    return ret;
}
