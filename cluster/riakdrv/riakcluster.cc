#include <stdlib.h>
#include <set>

#include <cr_class/cr_class.h>
#include <cr_class/cr_quickhash.h>
#include <riakdrv/riakcluster.h>
#include <riakdrv/riak_common.h>
#include <riakdrv/riakdrv++.pb.h>
#include <riakdrv/riakksf.h>

#define RIAK_CONNECT_CONTROLLER     "RIAK_CONNECT_CONTROLLER"
#define RIAK_CLUSTER_PTR        "riak_cluster_p"
#define RIAK_NODE_RCCB_PTR      "riak_node_rccb_p"
#define RIAK_NODE_TIMEOUT_STR       "riak_node_timeout"
#define RIAK_NODE_MAX_CONNECT_STR   "riak_node_max_connect"
#define RIAK_NODE_MAX_RETRY_STR     "riak_node_max_retry"
#define RIAK_NODE_INFO_STR      "riak_node_info"

#define ASYNC_THREAD_CLASS      "ASYNC_THREAD"
#define ASYNC_ASCB_PTR          "ascb_ptr"

typedef struct ASCB {
    CR_DataControl::CondMutex condmutex;
    std::set<intptr_t> thread_tid_set;
    unsigned int retry_count;
    size_t total_stored_size;
    bool stop_on_timeout;
    bool stop_now;
    int last_errno;
    std::string last_step;
    std::string last_req_str;
    CR_TreeStorage value_tree;
    std::map<intptr_t,double> clear_timestamp;
} ASCB_t;

typedef struct RSB {
    std::map<int8_t,std::map<int,int64_t> > opcode_retcode_count_map;
    std::map<int8_t,std::map<int,int64_t> > opcode_retcode_size_map;
} RSB_t;

static std::string async_thread_func(CR_Thread::handle_t thread_self);

static void riak_heartbeat(volatile bool *stop, RiakCluster *riak_cluster_p);

static std::string
async_thread_func(CR_Thread::handle_t thread_self)
{
    std::string ret = "";
    CR_MultiThread *thread_pool;
    std::string thread_class;
    std::string thread_name;
    intptr_t thread_id;
    RiakCluster *riak_cluster_p = NULL;
    ASCB_t *ascb_p = NULL;
    msgRiakAsyncReq msgReq;
    std::string msgReq_str;
    msgRiakAsyncResp msgResp;
    std::string msgResp_str;
    int fret;
    bool stop_now = false;
    bool stop_on_timeout = false;
    unsigned int retry_count;
    size_t total_stored_size = 0;
    std::string asyncget_key, asyncget_value;
    char last_cmd_char = '*';
    double ascb_clear_timestamp;
    double msgreq_timestamp;

    CANCEL_DISABLE_ENTER();

    thread_pool = (CR_MultiThread *)CR_Class_NS::str2ptr(
                      CR_Thread::gettag(thread_self, CR_MULTITHREAD_TAG_MULTITHREAD_PTR));
    thread_class = CR_Thread::gettag(thread_self, CR_MULTITHREAD_TAG_THREAD_CLASS);
    thread_name = CR_Thread::gettag(thread_self, CR_MULTITHREAD_TAG_THREAD_NAME);
    thread_id = CR_Thread::gettid(thread_self);

    riak_cluster_p = (RiakCluster *)CR_Class_NS::str2ptr(CR_Thread::gettag(thread_self, RIAK_CLUSTER_PTR));
    ascb_p = (ASCB_t *)CR_Class_NS::str2ptr(CR_Thread::gettag(thread_self, ASYNC_ASCB_PTR));

    ascb_p->condmutex.lock();

    ascb_p->thread_tid_set.insert(thread_id);

    ascb_p->condmutex.lock();

    while (1) {
        if (CR_Thread::teststop(thread_self))
            stop_on_timeout = true;

        ascb_p->condmutex.lock();

        retry_count = ascb_p->retry_count;

        if (ascb_p->stop_now) {
            ret = "[ascb_p->stop_now == true]";
            stop_now = true;
        }

        if (ascb_p->stop_on_timeout)
            stop_on_timeout = true;

        ascb_p->condmutex.unlock();

        if (stop_now)
            break;

        if (stop_on_timeout) {
            fret = thread_pool->QueuePull(thread_class, thread_name, msgReq_str, 0);
            if (fret) {
                ret = "[stop_on_timeout == true && queue is blank]";
                break;
            }
        } else {
            fret = thread_pool->QueuePull(thread_class, thread_name, msgReq_str, 0.1);
            if (fret)
                continue;
        }

        if (msgReq.ParseFromString(msgReq_str)) {
            bool break_out = false;
            int64_t query_id = 0;
            RiakCluster::async_callback_t callback_ptr = NULL;
            size_t stored_size = 0;

            if (msgReq.has_callback_ptr()) {
                callback_ptr = (RiakCluster::async_callback_t)CR_Class_NS::str2ptr(msgReq.callback_ptr());
            }

            for (unsigned int retry=0; retry<retry_count; retry++) {
                switch (msgReq.cmdcode()) {
                    case RIAK_ASYNC_GET :
                        last_cmd_char = 'G';
                        fret = riak_cluster_p->Get(msgReq.bucket(), msgReq.key(), asyncget_value,
                                                   ((msgReq.has_queryparams()) ? (&(msgReq.queryparams())) : (NULL)));
                        if (fret == ENOENT) {
                            break_out = true;
                        }
                        break;
                    case RIAK_ASYNC_PUT :
                        last_cmd_char = 'P';
                        if (msgReq.has_value()) {
                            fret = riak_cluster_p->Put(msgReq.bucket(), msgReq.key(), msgReq.value(),
                                                       ((msgReq.has_queryparams()) ? (&(msgReq.queryparams())) : (NULL)), &stored_size);
                            total_stored_size += stored_size;
                        } else {
                            DPRINTFX(20, "B[%s]K[%s]VS[%lu]PS[%lu], msgReq.has_value() != true\n",
                                     msgReq.bucket().c_str(), msgReq.key().c_str(), msgReq.value().size(),
                                     ((msgReq.has_queryparams()) ? (msgReq.queryparams().size()) : 0));
                            fret = EINVAL;
                            break_out = true;
                        }
                        break;
                    case RIAK_ASYNC_DEL :
                        last_cmd_char = 'D';
                        fret = riak_cluster_p->Del(msgReq.bucket(), msgReq.key(),
                                                   ((msgReq.has_queryparams()) ? (&(msgReq.queryparams())) : (NULL)));
                        break;
                    default :
                        last_cmd_char = '?';
                        fret = EOPNOTSUPP;
                        break_out = true;
                        break;
                }

                if ((break_out) || (fret == 0)) {
                    switch (msgReq.cmdcode()) {
                        case RIAK_ASYNC_GET :
                            msgResp.Clear();
                            msgResp.set_cmdcode(msgReq.cmdcode());
                            msgResp.set_bucket(msgReq.bucket());
                            msgResp.set_key(msgReq.key());
                            msgResp.set_intret(fret);
                            if (msgReq.has_queryid()) {
                                query_id = msgReq.queryid();
                                msgResp.set_queryid(query_id);
                            }
                            if (msgReq.has_queryparams())
                                msgResp.set_queryparams(msgReq.queryparams());

                            if (fret == 0)
                                msgResp.set_value(asyncget_value);

                            if (msgResp.SerializeToString(&msgResp_str)) {
                                if (callback_ptr) {
                                    if (msgReq.has_callback_tag_data())
                                        fret = callback_ptr(&(msgReq.callback_tag_data()), fret, msgResp_str);
                                    else
                                        fret = callback_ptr(NULL, fret, msgResp_str);
                                } else {
                                    asyncget_key = riak_cluster_p->req_to_key(msgReq.bucket(), msgReq.key(),
                                                   (msgReq.has_queryid())?(msgReq.queryid()):(0));

                                    ascb_p->condmutex.lock();

                                    if (msgReq.has_queryid() && msgReq.has_timestamp()) {
                                        ascb_clear_timestamp = ascb_p->clear_timestamp[msgReq.queryid()];
                                        msgreq_timestamp = msgReq.timestamp();
                                    } else {
                                        ascb_clear_timestamp = 0.0;
                                        msgreq_timestamp = 1.0;
                                    }

                                    if (ascb_clear_timestamp < msgreq_timestamp) {
                                        ascb_p->value_tree.Add(asyncget_key, msgResp_str,
                                                               CR_Class_NS::i642str(query_id));
                                    } else {
                                        DPRINTFX(30, "query_id[%ld]'s clear timestamp[%f]"
                                                 " > msgReq.timestamp[%f], drop it.\n",
                                                 (msgReq.has_queryid())?((long)msgReq.queryid()):(0),
                                                 ascb_clear_timestamp, msgreq_timestamp);
                                    }

                                    ascb_p->condmutex.unlock();
                                    fret = 0;
                                }
                            }
                            break;
                        case RIAK_ASYNC_PUT :
                        case RIAK_ASYNC_DEL :
                            if (callback_ptr) {
                                msgResp.Clear();
                                msgResp.set_cmdcode(msgReq.cmdcode());
                                msgResp.set_bucket(msgReq.bucket());
                                msgResp.set_key(msgReq.key());
                                msgResp.set_intret(fret);
                                if (msgReq.has_queryid()) {
                                    msgResp.set_queryid(msgReq.queryid());
                                }
                                if (msgReq.has_queryparams())
                                    msgResp.set_queryparams(msgReq.queryparams());

                                if (fret == 0)
                                    msgResp.set_value_stored_size(stored_size);

                                if (msgResp.SerializeToString(&msgResp_str)) {
                                    if (msgReq.has_callback_tag_data())
                                        fret = callback_ptr(&(msgReq.callback_tag_data()), fret, msgResp_str);
                                    else
                                        fret = callback_ptr(NULL, fret, msgResp_str);
                                }
                            }
                            break;
                        default :
                            break;
                    }
                    break;
                }

                DPRINTFX(20, "last_cmd=%c, bucket=%s, key=%s, fret=%s, msg=%s, retry=%u\n",
                         last_cmd_char, msgReq.bucket().c_str(), msgReq.key().c_str(),
                         CR_Class_NS::strerrno(fret), CR_Class_NS::strerror(fret), retry);
            }
        } else      // (msgReq.ParseFromString(msgReq_str))
            fret = ENOMSG;

        if (fret && (fret != ENOENT)) {
            DPRINTF("last_cmd=%c, bucket=%s, key=%s, fret=%s, msg=%s, FINAL\n",
                    last_cmd_char, msgReq.bucket().c_str(), msgReq.key().c_str(),
                    CR_Class_NS::strerrno(fret), CR_Class_NS::strerror(fret));

            ascb_p->condmutex.lock();

            if (!ascb_p->stop_now) {
                ascb_p->stop_now = true;
                ascb_p->last_errno = fret;
                ascb_p->last_step = riak_cluster_p->req_to_key(msgReq.bucket(), msgReq.key(), 0);
                ascb_p->last_req_str = msgReq_str;
            }

            ascb_p->condmutex.unlock();

            break;
        }
    }

    ascb_p->condmutex.lock();

    ascb_p->total_stored_size += total_stored_size;
    ascb_p->thread_tid_set.erase(thread_id);
    ascb_p->condmutex.broadcast();

    ascb_p->condmutex.unlock();

    CANCEL_DISABLE_LEAVE();

    return ret;
}

static void
riak_heartbeat(volatile bool *stop, RiakCluster *riak_cluster_p)
{
    size_t sec_count = 0;
    size_t rebuild_timeout;
    bool do_rebuild = false;
    int64_t status_last_count = 0;
    int64_t status_last_size = 0;
    int64_t status_cur_count = 0;
    int64_t status_cur_size = 0;
    int64_t status_count_ps = 0;
    int64_t status_size_ps = 0;

    rebuild_timeout = riak_cluster_p->get_rebuild_timeout();

    CANCEL_DISABLE_ENTER();

    while (1) {
        if (*stop){
            break;
        }

        sleep(1);

        sec_count++;
        if ((sec_count % 60) == 0) {
            DPRINTF( "Heartbeat(%lu)\n", sec_count);
        }

        status_cur_count = riak_cluster_p->QueryStatus(RiakCluster::ST_COUNT);
        status_cur_size = riak_cluster_p->QueryStatus(RiakCluster::ST_SIZE);
        if ((status_cur_count - status_last_count) >= 0) {
            status_count_ps = status_cur_count - status_last_count;
        }
        if ((status_cur_size - status_last_size) >= 0) {
            status_size_ps = status_cur_size - status_last_size;
        }

        DPRINTF("Cluster[%lX]->QueryStatus:[ST_COUNT]==%lld, [ST_SIZE]==%lld,"
                 " [ST_COUNT_PS]==%lld, [ST_SIZE_PS]==%lld\n",
                 (intptr_t)riak_cluster_p, (long long)status_cur_count, (long long)status_cur_size,
                 (long long)status_count_ps, (long long)status_size_ps
                );

        status_last_count = status_cur_count;
        status_last_size = status_cur_size;

        if (riak_cluster_p->get_first_use()){
            continue;
        }

        do_rebuild = false;

        if (riak_cluster_p->need_rebuild) {
            do_rebuild = true;
            DPRINTF("Auto rebuilder: Start force rebuild.\n");
        } else if (sec_count % rebuild_timeout == 0) {
            do_rebuild = true;
            DPRINTF("Auto rebuilder: Start auto rebuild.\n");
        }

        if (do_rebuild) {
            DPRINTF("Auto rebuilder: Rebuilding...\n");
            int fret = riak_cluster_p->Rebuild();
            if (fret > 0){
                riak_cluster_p->need_rebuild = false;
            }
            DPRINTF("Auto rebuilder: Rebuild() return %d.\n", fret);
        }
    }

    CANCEL_DISABLE_LEAVE();
}

RiakCluster::RiakCluster(size_t timeout, size_t max_connect_per_node,
                         size_t max_retry, size_t _rebuild_timeout)
{
    char *_clusterinfo_tmp_p;

    this->_rebuild_timeout = _rebuild_timeout;
    this->_timeout = timeout;
    this->_max_connect_per_node = max_connect_per_node;
    this->_max_retry = max_retry;
    this->need_rebuild = false;
    this->_first_use = true;
    this->_has_readonly_cluster = false;
    this->stat_data_p = new RSB_t;
    this->_is_ready = false;

    this->_cfm_cluster_p = NULL;

    _clusterinfo_tmp_p = getenv(RIAK_CLUSTERINFO_STR_ENVNAME);
    if (_clusterinfo_tmp_p) {
        this->_clusterinfo_str = _clusterinfo_tmp_p;
    } else {
        throw "Need " RIAK_CLUSTERINFO_STR_ENVNAME " ENV\n";
    }

    this->_rebuilder_thread_stop = false;
    this->_rebuilder_thread_p = new std::thread(riak_heartbeat, &(this->_rebuilder_thread_stop), this);
}

RiakCluster::~RiakCluster()
{
    if (this->_rebuilder_thread_p) {
        this->_rebuilder_thread_stop = true;
        this->_rebuilder_thread_p->join();
        delete this->_rebuilder_thread_p;
    }

    RSB_t *rsb_p = (RSB_t *)(this->stat_data_p);

    if (rsb_p) {
        delete rsb_p;
    }

    while (!this->_connect_list.empty()) {
        RiakConnect *tmp_conn = this->_connect_list.front();
        this->_connect_list.pop_front();
        if (tmp_conn) {
            // add by liujs                        
            std::string tmp_id = tmp_conn->get_id();
            DPRINTF("RiakCluster::~RiakCluster delete Node %s \n", tmp_id.c_str());
            delete tmp_conn;
        }
    }

    if (this->_cfm_cluster_p)
        delete this->_cfm_cluster_p;
}

bool
RiakCluster::get_first_use() const
{
    return this->_first_use;
}

size_t
RiakCluster::get_rebuild_timeout() const
{
    return this->_rebuild_timeout;
}

int64_t
RiakCluster::QueryStatus(const status_type_t status_type, const int8_t opcode, const special_retcode_t retcode)
{
    int64_t ret = -1;
    int64_t ret_tmp = 0;
    int errno_tmp = 0;

    this->_cond_mutex.lock();

    do {
        RSB_t *rsb_p = (RSB_t *)(this->stat_data_p);

        if (!rsb_p) {
            errno_tmp = EFAULT;
            break;
        }

        if (status_type == ST_COUNT) {
            // collect count status
            if (opcode == UNDEFINED_REQ) {
                // collect all opcode status of count
                std::map<int8_t,std::map<int,int64_t> >::iterator _count_map_it;
                for (_count_map_it=rsb_p->opcode_retcode_count_map.begin();
                     _count_map_it!=rsb_p->opcode_retcode_count_map.end(); _count_map_it++) {
                    int8_t tmp_opcode = _count_map_it->first;
                    if ((tmp_opcode == UNDEFINED_REQ) || (tmp_opcode == UNDEFINED_RESP)) {
                        continue;
                    }

                    std::map<int,int64_t> &_inner_map = _count_map_it->second;
                    std::map<int,int64_t>::iterator _inner_map_it;

                    if ((retcode == SR_ALL) || (retcode == SR_ALL_FAILED)) {
                        // collect all(failed) retcode status of all opcode of count
                        for (_inner_map_it=_inner_map.begin(); _inner_map_it!=_inner_map.end(); _inner_map_it++) {
                            if ((retcode == SR_ALL_FAILED) && (_inner_map_it->first == 0)) {
                                continue;
                            }
                            ret_tmp += _inner_map_it->second;
                        }
                    } else {
                        // collect success retcode status of all opcode of count
                        ret_tmp += _inner_map[retcode];
                    }
                }
            } else {
                // collect one opcode status of count
                std::map<int,int64_t> &_inner_map = rsb_p->opcode_retcode_count_map[opcode];
                std::map<int,int64_t>::iterator _inner_map_it;

                if ((retcode == SR_ALL) || (retcode == SR_ALL_FAILED)) {
                    // collect all(failed) retcode status of one opcode of count
                    for (_inner_map_it=_inner_map.begin(); _inner_map_it!=_inner_map.end(); _inner_map_it++) {
                        if ((retcode == SR_ALL_FAILED) && (_inner_map_it->first == 0)) {
                            continue;
                        }
                        ret_tmp += _inner_map_it->second;
                    }
                } else {
                    // collect success retcode status of one opcode of count
                    ret_tmp += _inner_map[retcode];
                }
            }
            break;
        } else if (status_type == ST_SIZE) {
            // collect size status
            if (opcode == UNDEFINED_REQ) {
                // collect all opcode status of size
                std::map<int8_t,std::map<int,int64_t> >::iterator _size_map_it;
                for (_size_map_it=rsb_p->opcode_retcode_size_map.begin();
                     _size_map_it!=rsb_p->opcode_retcode_size_map.end(); _size_map_it++) {
                    int8_t tmp_opcode = _size_map_it->first;
                    if ((tmp_opcode == UNDEFINED_REQ) || (tmp_opcode == UNDEFINED_RESP)) {
                        continue;
                    }

                    std::map<int,int64_t> &_inner_map = _size_map_it->second;
                    std::map<int,int64_t>::iterator _inner_map_it;

                    if ((retcode == SR_ALL) || (retcode == SR_ALL_FAILED)) {
                        // collect all(failed) retcode status of all opcode of size
                        for (_inner_map_it=_inner_map.begin(); _inner_map_it!=_inner_map.end(); _inner_map_it++) {
                            if ((retcode == SR_ALL_FAILED) && (_inner_map_it->first == 0)) {
                                continue;
                            }
                            ret_tmp += _inner_map_it->second;
                        }
                    } else {
                        // collect success retcode status of all opcode of size
                        ret_tmp += _inner_map[retcode];
                    }
                }
            } else {
                // collect one opcode status of size
                std::map<int,int64_t> &_inner_map = rsb_p->opcode_retcode_size_map[opcode];
                std::map<int,int64_t>::iterator _inner_map_it;

                if ((retcode == SR_ALL) || (retcode == SR_ALL_FAILED)) {
                    // collect all(failed) retcode status of one opcode of size
                    for (_inner_map_it=_inner_map.begin(); _inner_map_it!=_inner_map.end(); _inner_map_it++) {
                        if ((retcode == SR_ALL_FAILED) && (_inner_map_it->first == 0)) {
                            continue;
                        }
                        ret_tmp += _inner_map_it->second;
                    }
                } else {
                    // collect success retcode status of one opcode of size
                    ret_tmp += _inner_map[retcode];
                }
            }
            break;
        } else {
            errno_tmp = EINVAL;
            break;
        }
    } while (0);

    this->_cond_mutex.unlock();

    if (errno_tmp == 0) {
        ret = ret_tmp;
    } else {
        if (errno_tmp < 0) {
            ret = errno_tmp;
        } else {
            ret = (0 - errno_tmp);
        }
    }

    return ret;
}

int
RiakCluster::Rebuild(const char *conn_str, const char *_srv_name)
{
    int ret = 0;
    int fret = ENOENT;
    std::vector<std::string> nodes_arr;
    std::list<RiakConnect*> tmp_conn_p_list;
    std::string one_node_name;
    RpbContent one_node_content;
    CR_Class_NS::nn_transport_t one_node_transport;
    std::string one_node_hostname;
    std::string one_node_srvname;
    std::string tmp_id;
    int node_count = 0;
    int connect_count = 0;
    int wait_timeout;

    wait_timeout = this->_timeout / 2;
    if (wait_timeout > 30)
        wait_timeout = 30;

    fret = this->parse_cluster_info(this->_clusterinfo_str, nodes_arr);

    if (fret == 0) {
        for (size_t i=0; i<nodes_arr.size(); i++) {
            one_node_name = nodes_arr[i];
            DPRINTF("begin connect to node when node name == %s\n", one_node_name.c_str());
            fret = CR_Class_NS::nn_addr_split(nodes_arr[i].c_str(),
              one_node_transport, one_node_hostname, one_node_srvname);

            if ((fret != 0) || (one_node_hostname.size() == 0) || (one_node_srvname.size() == 0)) {
                DPRINTF("Bad hostname or srvname %s\n", nodes_arr[i].c_str());
                continue;
            }

            for (unsigned j=0; j< this->_max_connect_per_node; j++) {
                RiakConnect *tmp_conn_p = new RiakConnect(one_node_name, this->_timeout);

                if (tmp_conn_p) {
                    if (tmp_conn_p->Connect(one_node_hostname, one_node_srvname) != 0) {
                        DPRINTF("Failed connect to node when node name == %s,node index %d\n", one_node_name.c_str(),j);
                        delete tmp_conn_p;
                    } else {
                        DPRINTF("Success connect to node %s, node index %d\n", one_node_name.c_str(),j);
                        tmp_conn_p_list.push_back(tmp_conn_p);
                        connect_count++;
                    }
                }
            }

            node_count++;
        }

        if (!tmp_conn_p_list.empty()) {
            this->_cond_mutex.lock();
           // can not delete,add by liujs
           this->_node_connects_map.clear();

            double _current_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

            // close  not use connection
            const int conCloseWaitConnectTime = 3;
            std::list<RiakConnect*>::iterator iter;
            while(1) {
                bool _has_found_close_conn = false;
                for(iter = this->_connect_list.begin(); iter != this->_connect_list.end(); iter++) {
                    double _exp_time = (*iter)->GetLastUseTime() + conCloseWaitConnectTime * this->_timeout;
                    if(_exp_time<_current_time) {

                        // add by liujs                        
                        tmp_id =  (*iter)->get_id();
                        DPRINTF("Node %s is timeout\n", tmp_id.c_str());
                        if (this->_node_connects_map[tmp_id] > 0) {
                            (this->_node_connects_map[tmp_id])--;
                        }
                        
                        _has_found_close_conn = true;
                        (*iter)->DisConnect();
                        this->_connect_list.erase(iter);
                        break;
                    }
                }
                if(!_has_found_close_conn) {
                    break;
                }
            }

            while (!tmp_conn_p_list.empty()) {
                RiakConnect *tmp_conn_p = tmp_conn_p_list.front();
                if (tmp_conn_p) {
                    tmp_id = tmp_conn_p->get_id();
                    if (this->_node_connects_map[tmp_id] < this->_max_connect_per_node) {
                        (this->_node_connects_map[tmp_id])++;

                        tmp_conn_p->SetLastUseTime(_current_time);  // add by liujs

                        this->_connect_list.push_back(tmp_conn_p);
                        ret++;
                        DPRINTF("Node %s is ready\n", tmp_id.c_str());
                    } else {
                        delete tmp_conn_p;
                        tmp_conn_p = NULL;
                    }
                }
                tmp_conn_p_list.pop_front();
            }

            if (ret > 0){
                this->_cond_mutex.broadcast();
            }
#ifndef KEYSPACE_USE_FILE_MODE
            if (!this->_cfm_cluster_p) {
                do {
                    char *cluster_str_p = getenv(DMIBSORTLOAD_EN_CLUSTER_STR);
                    if (!cluster_str_p) {
                        DPRINTF("Environment variable \"%s\" not set\n", DMIBSORTLOAD_EN_CLUSTER_STR);
                        break;
                    }
                    std::string cluster_str = cluster_str_p;
                    CFM_Basic_NS::cluster_info_t cluster_info;
                    int fret = CFM_Basic_NS::parse_cluster_info(cluster_str, cluster_info);
                    if (fret) {
                        DPRINTF("Parse cluster info failed[%s]\n", CR_Class_NS::strerrno(fret));
                        break;
                    }
                    CFMCluster *new_cluster_p = new CFMCluster(cluster_info);
                    new_cluster_p->SetJobID("PERSIST_ID");
                    new_cluster_p->SetTimeout(this->_timeout, this->_timeout);
                    CFMCluster_NS::NodeErrorsT cluster_error;
                    fret = new_cluster_p->Connect(&cluster_error);
                    if (fret) {
                        DPRINTF("cfm_cluster->Connect() failed, msg => [%s]\n",
                                CFMCluster_NS::ExplainErrors(cluster_error).c_str());
                        delete new_cluster_p;
                        break;
                    }
                    this->_cfm_cluster_p = new_cluster_p;
                } while (0);
            }
#endif 

            this->_cond_mutex.unlock();
        }
    } else {
        if (fret > 0)
            ret = 0 - fret;
        else
            ret = fret;
    }

    if (ret >= 0) {
        if (this->_first_use) {
            this->_first_use = false;
        }

        this->_is_ready = true;

        DPRINTF("Total available node(s) %d, available connect(s) %d, usage connect(s) %d\n",
                node_count, connect_count, ret);
    }

    return ret;
}

bool
RiakCluster::IsReady()
{
    return this->_is_ready;
}

RiakConnect *
RiakCluster::get_connect(double timeout_sec, const std::string *spec_connect_id_p,
                         const std::string *never_connect_id_p)
{
    RiakConnect *conn = NULL;
    std::string tmp_id;

    this->_cond_mutex.lock();

    do {
        if (spec_connect_id_p) {
            if (this->_node_connects_map.find(*spec_connect_id_p) == this->_node_connects_map.end()) {
                break;
            }
        }

        if (this->_connect_list.empty()) {
            this->_cond_mutex.wait(timeout_sec);
        }

        if (!this->_connect_list.empty()) {
            conn = this->_connect_list.front();
            this->_connect_list.pop_front();
            if (conn) {
                tmp_id = conn->get_id();
                if (this->_node_connects_map.find(tmp_id) == this->_node_connects_map.end()) {
                    DPRINTF("get_connect delete1 Node %s \n", tmp_id.c_str());
                    delete conn;
                    conn = NULL;
                } else if (spec_connect_id_p) {
                
                    DPRINTF("get_connect delete2 Node %s \n", tmp_id.c_str());
                    if (tmp_id != *spec_connect_id_p) {
                        this->_connect_list.push_back(conn);
                        this->_cond_mutex.broadcast();
                        conn = NULL;
                    }
                } else if (never_connect_id_p) {
                
                    DPRINTF("get_connect delete3 Node %s \n", tmp_id.c_str());
                    if (tmp_id == *never_connect_id_p) {
                        this->_connect_list.push_back(conn);
                        this->_cond_mutex.broadcast();
                        conn = NULL;
                    }
                }
            }
        }
    } while (0);

    if(conn) {
        conn->SetLastUseTime(CR_Class_NS::clock_gettime(CLOCK_MONOTONIC));
    }else{
       DPRINTF("get_connect get null\n");
    }
    this->_cond_mutex.unlock();

    return conn;
}

void
RiakCluster::put_connect(RiakConnect *conn, int last_ret, int8_t last_code_req, size_t last_op_size)
{
    if (!conn) {
        return;
    }

    std::string tmp_id = conn->get_id();
    RSB_t *rsb_p = (RSB_t *)(this->stat_data_p);
    
    DPRINTF("put_connect A connect of node %s begin\n",tmp_id.c_str());

    if (last_code_req == UNDEFINED_REQ) {
        last_code_req = UNKNOWN_REQ;
    } else if (last_code_req == UNDEFINED_RESP) {
        last_code_req = UNKNOWN_RESP;
    }

    this->_cond_mutex.lock();

    conn->SetLastUseTime(CR_Class_NS::clock_gettime(CLOCK_MONOTONIC));

    if (rsb_p) {
        (rsb_p->opcode_retcode_count_map[last_code_req][last_ret])++;
        (rsb_p->opcode_retcode_size_map[last_code_req][last_ret]) += last_op_size;
    }

    if (this->_node_connects_map.find(tmp_id) != this->_node_connects_map.end()) {
        if ((last_ret) && (last_ret != ENOENT)) {
            delete conn;
            if (last_ret != EPIPE) {
                DPRINTF("A connect of node %s was failed (err=%s, msg=%s), cluster may need rebuild\n",
                        tmp_id.c_str(), CR_Class_NS::strerrno(last_ret), CR_Class_NS::strerror(last_ret));
            }
            DPRINTF("A connect of node %s was not in _node_connects_map  may need rebuild\n",
                        tmp_id.c_str());
            this->need_rebuild = true;
            if (this->_node_connects_map[tmp_id] > 0) {
                (this->_node_connects_map[tmp_id])--;
            }
        } else {
            if (this->_node_connects_map[tmp_id] <= this->_max_connect_per_node) {
                this->_connect_list.push_back(conn);
                this->_cond_mutex.broadcast();
            } else {
                delete conn;
            }
        }
    } else{
        delete conn;
        }

    this->_cond_mutex.unlock();
}

std::string
RiakCluster::req_to_key(const std::string &bucket, const std::string &key, const int64_t query_id) const
{
    std::string ret;

    ret = CR_Class_NS::str_setparam(CR_Class_NS::blank_string, "BUCKET", bucket);
    ret = CR_Class_NS::str_setparam(ret, "KEY", key);
    ret = CR_Class_NS::str_setparam(ret, "QID", CR_Class_NS::i642str(query_id));

    return ret;
}

int
RiakCluster::Get(const std::string &bucket,
                 const std::string &key,
                 std::string &value,
                 const std::string *queryparams_p,
                 const std::string *spec_connect_id_p,
                 const std::string *never_connect_id_p)
{
    int ret = EAGAIN;
    RiakConnect *conn = NULL;
    unsigned int retry = 0;
    double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + this->_timeout;

    do {
        conn = this->get_connect(0.1, spec_connect_id_p, never_connect_id_p);
        if (conn) {
            ret = conn->Get(bucket, key, value, queryparams_p);
            this->put_connect(conn, ret, RPB_GET_REQ, value.size());
            if (ret) {
                if (ret == ENOENT){
                    break;
                }else{
                    retry++;
                }
            } else{
                break;
            }
        }
    } while ((retry < this->_max_retry) && (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) <= exp_time));

    return ret;
}

int
RiakCluster::Put(const std::string &bucket,
                 const std::string &key,
                 const std::string &value,
                 const std::string *queryparams_p,
                 size_t *stored_size,
                 const std::string *spec_connect_id_p,
                 const std::string *never_connect_id_p)
{
    int ret = EAGAIN;
    RiakConnect *conn = NULL;
    unsigned int retry = 0;
    double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + this->_timeout;

    do {
        conn = this->get_connect(0.1, spec_connect_id_p, never_connect_id_p);
        if (conn) {
            ret = conn->Put(bucket, key, value, queryparams_p, stored_size);
            this->put_connect(conn, ret, RPB_PUT_REQ, value.size());
            conn->SetLastUseTime(CR_Class_NS::clock_gettime(CLOCK_MONOTONIC));
            if (ret)
                retry++;
            else
                break;
        }
    } while ((retry < this->_max_retry) && (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) <= exp_time));

    return ret;
}

int
RiakCluster::Del(const std::string &bucket,
                 const std::string &key,
                 const std::string *queryparams_p,
                 const std::string *spec_connect_id_p,
                 const std::string *never_connect_id_p)
{
    int ret = EAGAIN;
    RiakConnect *conn = NULL;
    unsigned int retry = 0;
    double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + this->_timeout;

    do {
        conn = this->get_connect(0.1, spec_connect_id_p, never_connect_id_p);
        if (conn) {
            ret = conn->Del(bucket, key, queryparams_p);
            this->put_connect(conn, ret, RPB_DEL_REQ);
            if (ret)
                retry++;
            else
                break;
        }
    } while ((retry < this->_max_retry) && (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) <= exp_time));

    return ret;
}

int
RiakCluster::GetEx(const std::string &bucket, const std::string &key, const std::string *queryparams_p,
                   const uint32_t flags, RpbContent *content_out_p, std::string *vclock_io_p,
                   const std::string *spec_connect_id_p, const std::string *never_connect_id_p)
{
    int ret = EAGAIN;
    RiakConnect *conn = NULL;
    unsigned int retry = 0;
    double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + this->_timeout;

    do {
        conn = this->get_connect(0.1, spec_connect_id_p, never_connect_id_p);
        if (conn) {
            ret = conn->GetEx(bucket, key, queryparams_p, flags, content_out_p, vclock_io_p, NULL);
            this->put_connect(conn, ret, RPB_GET_REQ);
            if (ret)
                if (ret == ENOENT)
                    break;
                else
                    retry++;
            else
                break;
        }
    } while ((retry < this->_max_retry) && (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) <= exp_time));

    return ret;
}

int
RiakCluster::PutEx(const std::string &bucket, const std::string *key_in_p, const std::string *queryparams_p,
                   const uint32_t flags, RpbContent *content_io_p, std::string *vclock_io_p, std::string *key_out_p,
                   const std::string *spec_connect_id_p, const std::string *never_connect_id_p)
{
    int ret = EAGAIN;
    RiakConnect *conn = NULL;
    unsigned int retry = 0;
    double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + this->_timeout;

    do {
        conn = this->get_connect(0.1, spec_connect_id_p, never_connect_id_p);
        if (conn) {
            ret = conn->PutEx(bucket, key_in_p, queryparams_p, flags, content_io_p, vclock_io_p, key_out_p, NULL);
            this->put_connect(conn, ret, RPB_PUT_REQ);
            if (ret)
                retry++;
            else
                break;
        }
    } while ((retry < this->_max_retry) && (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) <= exp_time));

    return ret;
}

int
RiakCluster::DelEx(const std::string &bucket, const std::string &key,
                   const std::string *queryparams_p, const std::string *vclock_in_p,
                   const std::string *spec_connect_id_p, const std::string *never_connect_id_p)
{
    int ret = EAGAIN;
    RiakConnect *conn = NULL;
    unsigned int retry = 0;
    double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + this->_timeout;

    do {
        conn = this->get_connect(0.1, spec_connect_id_p, never_connect_id_p);
        if (conn) {
            ret = conn->DelEx(bucket, key, queryparams_p, vclock_in_p, NULL);
            this->put_connect(conn, ret, RPB_DEL_REQ);
            if (ret)
                retry++;
            else
                break;
        }
    } while ((retry < this->_max_retry) && (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) <= exp_time));

    return ret;
}

int
RiakCluster::ListBuckets(std::vector<std::string> &buckets,
                         const std::string *spec_connect_id_p, const std::string *never_connect_id_p)
{
    int ret = EAGAIN;
    RiakConnect *conn = NULL;
    unsigned int retry = 0;
    double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + this->_timeout;

    do {
        conn = this->get_connect(0.1, spec_connect_id_p, never_connect_id_p);
        if (conn) {
            ret = conn->ListBuckets(buckets);
            this->put_connect(conn, ret, RPB_LIST_BUCKETS_REQ, 0);
            if (ret) {
                retry++;
            } else
                break;
        }
    } while ((retry < this->_max_retry) && (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) <= exp_time));

    return ret;
}

int
RiakCluster::ListKeys(const std::string &bucket, std::vector<std::string> &keys,
                      const std::string *spec_connect_id_p, const std::string *never_connect_id_p)
{
    int ret = EAGAIN;
    RiakConnect *conn = NULL;
    unsigned int retry = 0;
    double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + this->_timeout;

    do {
        conn = this->get_connect(0.1, spec_connect_id_p, never_connect_id_p);
        if (conn) {
            ret = conn->ListKeys(bucket, keys);
            this->put_connect(conn, ret, RPB_LIST_KEYS_REQ, 0);
            if (ret) {
                retry++;
            } else
                break;
        }
    } while ((retry < this->_max_retry) && (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) <= exp_time));

    return ret;
}

int
RiakCluster::Index(const std::string &bucket, const std::string &index,
                   const std::string &range_min, const std::string &range_max,
                   std::vector<std::string> *keys_arr_p, std::map<std::string,std::string> *kv_map_p,
                   const size_t max_results, std::string *continuation_io_p,
                   const std::string *spec_connect_id_p, const std::string *never_connect_id_p)
{
    int ret = EAGAIN;
    RiakConnect *conn = NULL;
    unsigned int retry = 0;
    double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + this->_timeout;

    do {
        conn = this->get_connect(0.1, spec_connect_id_p, never_connect_id_p);
        if (conn) {
            ret = conn->Index(bucket, index, range_min, range_max,
                              keys_arr_p, kv_map_p, max_results, continuation_io_p);
            this->put_connect(conn, ret, RPB_INDEX_REQ, 0);
            if (ret) {
                retry++;
            } else
                break;
        }
    } while ((retry < this->_max_retry) && (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) <= exp_time));

    return ret;
}

void *
RiakCluster::AsyncInit(const size_t thread_count, const size_t queue_max_size, const unsigned int retry_count)
{
    if (thread_count <= 0)
        return NULL;

    ASCB_t *ascb_p = new ASCB_t;
    std::string thread_name;
    CR_Thread::TDB_t tdb;

    thread_name = CR_Class_NS::ptr2str(ascb_p);

    this->thread_pool.QueueSetMaxSize(ASYNC_THREAD_CLASS, thread_name, queue_max_size);

    ascb_p->stop_now = false;
    ascb_p->stop_on_timeout = false;
    ascb_p->last_errno = 0;
    ascb_p->retry_count = retry_count;
    ascb_p->total_stored_size = 0;

    tdb.thread_func = async_thread_func;
    tdb.default_tags[RIAK_CLUSTER_PTR] = CR_Class_NS::ptr2str(this);
    tdb.default_tags[ASYNC_ASCB_PTR] = CR_Class_NS::ptr2str(ascb_p);

    for (size_t i=0; i<thread_count; i++) {
        if (this->thread_pool.Spawn(ASYNC_THREAD_CLASS, thread_name, tdb)) {
            DPRINTF("SPAWN THREAD FAILED.\n");
            continue;
        }
    }

    return ascb_p;
}

int
RiakCluster::AsyncGet(void *handle, const std::string &bucket, const std::string &key, const int64_t query_id,
                      const std::string *queryparams_p)
{
    return this->AsyncGetWithCallback(handle, bucket, key, query_id, queryparams_p, NULL, NULL);
}

int
RiakCluster::AsyncGetWithCallback(void *handle, const std::string &bucket, const std::string &key,
                                  const int64_t query_id, const std::string *queryparams_p,
                                  async_callback_t callback, const std::string *tag_data_p)
{
    if (!handle)
        return EFAULT;

    int ret = 0;
    ASCB_t *ascb_p = (ASCB_t *)handle;
    std::string thread_name;
    msgRiakAsyncReq msgReq;
    std::string msgReq_str;

    thread_name = CR_Class_NS::ptr2str(ascb_p);

    msgReq.set_cmdcode(RIAK_ASYNC_GET);
    msgReq.set_timestamp(CR_Class_NS::clock_gettime(CLOCK_MONOTONIC));
    msgReq.set_bucket(bucket);
    msgReq.set_key(key);
    msgReq.set_queryid(query_id);
    if (callback) {
        msgReq.set_callback_ptr(CR_Class_NS::ptr2str((void *)callback));
        if (tag_data_p)
            msgReq.set_callback_tag_data(*tag_data_p);
    }
    if (queryparams_p)
        msgReq.set_queryparams(*queryparams_p);

    msgReq.SerializeToString(&msgReq_str);
    ret = this->thread_pool.QueuePushByName(ASYNC_THREAD_CLASS, thread_name, msgReq_str, this->_timeout);

    return ret;
}

int
RiakCluster::AsyncPopValue(void *handle, const std::string &bucket, const std::string &key,
                           const int64_t query_id, std::string &value)
{
    if (!handle)
        return EFAULT;

    int ret = 0;
    ASCB_t *ascb_p = (ASCB_t *)handle;
    msgRiakAsyncResp msgResp;
    std::string msgResp_str;

    ret = ascb_p->value_tree.PopOne(this->req_to_key(bucket, key, query_id),
                                    msgResp_str, CR_Class_NS::i642str(query_id), this->_timeout);
    if (ret == 0) {
        if (msgResp.ParseFromString(msgResp_str)) {
            if (msgResp.cmdcode() == RIAK_ASYNC_GET
                && msgResp.bucket() == bucket
                && msgResp.key() == key
                && (msgResp.has_queryid() && (msgResp.queryid() == query_id))) {
                // new line
                if (msgResp.has_value()) {
                    value = msgResp.value();
                } else if (msgResp.has_intret()) {
                    ret = msgResp.intret();
                    if (ret == 0)
                        ret = ENOENT;
                } else
                    ret = ENOENT;
            } else
                ret = ENOTSUP;
        } else
            ret = ENOMSG;
    }

    return ret;
}

int
RiakCluster::AsyncClearValue(void *handle, const int64_t query_id)
{
    if (!handle)
        return EFAULT;

    int ret = 0;
    ASCB_t *ascb_p = (ASCB_t *)handle;

    ascb_p->condmutex.lock();

    ascb_p->clear_timestamp[query_id] = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
    ret = ascb_p->value_tree.Clear(CR_Class_NS::i642str(query_id));

    ascb_p->condmutex.unlock();

    return ret;
}

int
RiakCluster::AsyncPut(void *handle, const std::string &bucket, const std::string &key,
                      const std::string &value, const std::string *queryparams_p)
{
    return this->AsyncPutWithCallback(handle, bucket, key, value, queryparams_p, NULL, NULL);
}

int
RiakCluster::AsyncPutWithCallback(void *handle, const std::string &bucket, const std::string &key,
                                  const std::string &value, const std::string *queryparams_p,
                                  async_callback_t callback, const std::string *tag_data_p)
{
    if (!handle)
        return EFAULT;

    int ret = 0;
    ASCB_t *ascb_p = (ASCB_t *)handle;
    std::string thread_name;
    msgRiakAsyncReq msgReq;
    std::string msgReq_str;

    thread_name = CR_Class_NS::ptr2str(ascb_p);

    msgReq.set_cmdcode(RIAK_ASYNC_PUT);
    msgReq.set_timestamp(CR_Class_NS::clock_gettime(CLOCK_MONOTONIC));
    msgReq.set_bucket(bucket);
    msgReq.set_key(key);
    msgReq.set_value(value);
    if (callback) {
        msgReq.set_callback_ptr(CR_Class_NS::ptr2str((void *)callback));
        if (tag_data_p)
            msgReq.set_callback_tag_data(*tag_data_p);
    }
    if (queryparams_p)
        msgReq.set_queryparams(*queryparams_p);

    msgReq.SerializeToString(&msgReq_str);
    ret = this->thread_pool.QueuePushByName(ASYNC_THREAD_CLASS, thread_name, msgReq_str, this->_timeout);

    return ret;
}

int
RiakCluster::AsyncDel(void *handle, const std::string &bucket, const std::string &key,
                      const std::string *queryparams_p)
{
    return this->AsyncDelWithCallback(handle, bucket, key, queryparams_p, NULL, NULL);
}

int
RiakCluster::AsyncDelWithCallback(void *handle, const std::string &bucket, const std::string &key,
                                  const std::string *queryparams_p, async_callback_t callback, const std::string *tag_data_p)
{
    if (!handle)
        return EFAULT;

    int ret = 0;
    ASCB_t *ascb_p = (ASCB_t *)handle;
    std::string thread_name;
    msgRiakAsyncReq msgReq;
    std::string msgReq_str;

    thread_name = CR_Class_NS::ptr2str(ascb_p);

    msgReq.set_cmdcode(RIAK_ASYNC_DEL);
    msgReq.set_timestamp(CR_Class_NS::clock_gettime(CLOCK_MONOTONIC));
    msgReq.set_bucket(bucket);
    msgReq.set_key(key);
    if (callback) {
        msgReq.set_callback_ptr(CR_Class_NS::ptr2str((void *)callback));
        if (tag_data_p)
            msgReq.set_callback_tag_data(*tag_data_p);
    }
    if (queryparams_p)
        msgReq.set_queryparams(*queryparams_p);

    msgReq.SerializeToString(&msgReq_str);
    ret = this->thread_pool.QueuePushByName(ASYNC_THREAD_CLASS, thread_name, msgReq_str, this->_timeout);

    return ret;
}

ssize_t
RiakCluster::AsyncWait(void *handle, std::string &errmsg, double timeout_sec)
{
    if (!handle)
        return EFAULT;

    ssize_t ret;
    int fret = 0;
    ASCB_t *ascb_p = (ASCB_t *)handle;
    double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;

    ascb_p->condmutex.lock();

    ascb_p->stop_on_timeout = true;

    while (ascb_p->thread_tid_set.size() > 0) {
        if (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) > exp_time) {
            ret = (0 - ETIMEDOUT);
            break;
        }

        fret = ascb_p->condmutex.wait(0.1);

        if ((fret == 0) || (fret == ETIMEDOUT) || (fret == EINTR)) {
            continue;
        }

        if (fret > 0) {
            ret = 0 - fret;
        } else if (fret < 0) {
            ret = fret;
        }

        break;
    }

    if (ascb_p->last_errno) {
        ret = ascb_p->last_errno;
        if (ret > 0)
            ret = 0 - ret;
        errmsg = ascb_p->last_step;
    } else
        ret = ascb_p->total_stored_size;

    ascb_p->condmutex.unlock();

    if (ret >= 0)
        delete ascb_p;

    return ret;
}

int64_t
RiakCluster::KeySpaceAlloc(const std::string &bucket, const int32_t tabid, const int32_t attrid,
                           const size_t min_alloc_count, const double autofree_timeout_sec)
{
    int64_t ret = -1;

    this->_cond_mutex.lock();

#ifndef KEYSPACE_USE_FILE_MODE

    do {
        if (!this->_cfm_cluster_p) {
            errno = EAGAIN;
            break;
        }

        if (this->_alloced_id_list.empty()) {
            std::vector<int64_t> id_arr;
            std::string errmsg;
            int fret = this->_cfm_cluster_p->IDAlloc(bucket,
                       min_alloc_count, autofree_timeout_sec, id_arr, &errmsg);
            if (fret) {
                DPRINTF("cfm_cluster->IDAlloc() failed, msg => [%s]\n", errmsg.c_str());
                errno = fret;
                break;
            }

            for (size_t i=0; i<id_arr.size(); i++) {
                this->_alloced_id_list.push_back(id_arr[i] << 12);
            }
        }

        if (!this->_alloced_id_list.empty()) {
            ret = this->_alloced_id_list.front();
            this->_alloced_id_list.pop_front();
        }
    } while (0);

#else
    ret = key_space_file_alloc(bucket,tabid,attrid);
#endif

    this->_cond_mutex.unlock();

    return ret;
}


int RiakCluster::KeySpaceFree(const std::string &bucket, const int32_t tabid, const int32_t attrid,
    const int64_t& ks)
{
       int ret = 0;
    
        this->_cond_mutex.lock();
    
#ifndef KEYSPACE_USE_FILE_MODE
        do {
            if (!this->_cfm_cluster_p) {
                ret = EAGAIN;
                break;
            }
    
            std::vector<int64_t> id_arr;
            id_arr.push_back((ks>>12));
    
            std::string errmsg;
            ret = this->_cfm_cluster_p->IDFree(bucket, id_arr, &errmsg);
            if (ret) {
                DPRINTF("cfm_cluster->IDFree() failed, msg => [%s]\n", errmsg.c_str());
                break;
            }
        } while (0);
#else
        ret = key_space_file_free(bucket,tabid,attrid,ks);
#endif
        this->_cond_mutex.unlock();
    
        return ret;


}


int
RiakCluster::KeySpaceFree(const std::string &bucket, const int32_t tabid, const int32_t attrid,
                          const std::vector<int64_t> &ks_arr)
{
    int ret = 0;

    this->_cond_mutex.lock();

#ifndef KEYSPACE_USE_FILE_MODE
    do {
        if (!this->_cfm_cluster_p) {
            ret = EAGAIN;
            break;
        }

        std::vector<int64_t> id_arr = ks_arr;

        for (size_t i=0; i<id_arr.size(); i++) {
            id_arr[i] = id_arr[i] >> 12;
        }

        std::string errmsg;
        ret = this->_cfm_cluster_p->IDFree(bucket, id_arr, &errmsg);
        if (ret) {
            DPRINTF("cfm_cluster->IDFree() failed, msg => [%s]\n", errmsg.c_str());
            break;
        }
    } while (0);
#else
    for (size_t i=0; i<ks_arr.size(); i++) {
        ret = key_space_file_free(bucket,tabid,attrid,ks_arr[i]);
    }
#endif
    this->_cond_mutex.unlock();

    return ret;
}

int
RiakCluster::KeySpaceSolidify(const std::string &bucket, const int32_t tabid, const int32_t attrid,
                              const std::vector<int64_t> &ks_arr)
{
    int ret = 0;

    this->_cond_mutex.lock();

#ifndef KEYSPACE_USE_FILE_MODE

    do {
        if (!this->_cfm_cluster_p) {
            ret = EAGAIN;
            break;
        }

        std::vector<int64_t> id_arr = ks_arr;

        for (size_t i=0; i<id_arr.size(); i++) {
            id_arr[i] = id_arr[i] >> 12;
        }

        std::string errmsg;
        ret = this->_cfm_cluster_p->IDSolidify(bucket, id_arr, &errmsg);
        if (ret) {
            DPRINTF("cfm_cluster->IDSolidify() failed, msg => [%s]\n", errmsg.c_str());
            break;
        }
    } while (0);
#else
    for (size_t i=0; i<ks_arr.size(); i++) {
        ret = key_space_file_solidify(bucket,tabid,attrid,ks_arr[i]);
    }
#endif
    this->_cond_mutex.unlock();

    return ret;
}

int
RiakCluster::parse_cluster_info(const std::string &cluster_str,
    std::vector<std::string> &cluster_info)
{
    int fret;
    std::vector<std::string> tmp_cluster_info;
    const char *cp = cluster_str.c_str();
    const char *last_cp = cp;
    std::set<std::string> node_addr_set;
    CR_Class_NS::nn_transport_t tmp_transport;
    std::string tmp_hostname;
    std::string tmp_srvname;

    while (*cp) {
        if (*cp == ';') {
            std::string tmp_node_addr;
            tmp_node_addr.assign(last_cp, ((uintptr_t)cp - (uintptr_t)last_cp));
            if (tmp_node_addr.size() > 0) {
               if (node_addr_set.find(tmp_node_addr) == node_addr_set.end()) {
                    node_addr_set.insert(tmp_node_addr);
                    fret = CR_Class_NS::nn_addr_split(tmp_node_addr.c_str(),
                      tmp_transport, tmp_hostname, tmp_srvname);
                    if (fret) {
                        DPRINTF("nn_addr_split(\"%s\") == %s\n", tmp_node_addr.c_str(),
                          CR_Class_NS::strerrno(fret));
                        return EINVAL;
                    }
                    if (tmp_transport != CR_Class_NS::NN_TCP) {
                        DPRINTF("transport != TCP\n");
                        return EINVAL;
                    }
                    if ((tmp_hostname.size() == 0) || (tmp_srvname.size() == 0)) {
                        DPRINTF("hostname == \"%s\", srvname == \"%s\"\n",
                          tmp_hostname.c_str(), tmp_srvname.c_str());
                        return EINVAL;
                    }
                    tmp_cluster_info.push_back(tmp_node_addr);
                } else {
                    DPRINTF("dup node: \"%s\"\n",
                      tmp_node_addr.c_str());
                    return EINVAL;
                }
            }
            cp++;
            last_cp = cp;
        } else {
            cp++;
        }
    }

    // cluster_info = tmp_cluster_info;
    cluster_info.clear();
    for(int i=0;i<(int)tmp_cluster_info.size();i++){
           cluster_info.push_back(tmp_cluster_info[i]);
    }

    return 0;
}
