#include <errno.h>
#include <cfm_drv/cfm_cluster.h>

#define _SEND_BUF_COUNT		(4)

/////////////////////////////

static void _buf_block_clear(CFMCluster::SendBuf::BufBlock * & bbp, void *arg_p);

static void _do_send_block(const CFM_Basic_NS::cluster_info_t *cluster_info_p, size_t striping_id,
    CR_BlockQueue<CFMCluster::SendBuf::BufBlock*> *buf_queue_p, CFMConnect *conn_p,
    volatile int *errno_p, volatile bool *stop_p);

static void _do_connect_one(CFMCluster::SendBuf *sendbuf_p, unsigned int comm_timeout, int *fret_p);
static void _do_jobstart_one(CFMCluster::SendBuf *sendbuf_p, int *fret_p,
    const std::string *lib_info_p, const bool static_work_path,
    const std::string *extra_param_p, std::string *errmsg_p);
static void _do_jobstop_one(CFMCluster::SendBuf *sendbuf_p, int *fret_p,
    const std::string *data_req_p, std::string *errmsg_p);
static void _do_jobquery_one(CFMCluster::SendBuf *sendbuf_p, int *fret_p,
    const std::string *data_req_p, std::string *result_p, std::string *errmsg_p);
static void _do_jobkvput_one(CFMCluster::SendBuf *sendbuf_p, int *fret_p,
    const std::string *key_p, const std::string *value_p, const bool if_none_match, std::string *errmsg_p);
static void _do_jobkvget_one(CFMCluster::SendBuf *sendbuf_p, int *fret_p,
    const std::string *key_p, std::string *value_p, const double timeout_sec, std::string *errmsg_p);
static void _do_jobclear_one(CFMCluster::SendBuf *sendbuf_p, int *fret_p,
    const std::string *data_req_p, std::string *errmsg_p);
static void _do_idfree_one(CFMCluster::SendBuf *sendbuf_p, int *fret_p, const std::string *id_name_p,
    std::vector<int64_t> *id_arr_p, std::string *errmsg_p);
static void _do_idsolidify_one(CFMCluster::SendBuf *sendbuf_p, int *fret_p, const std::string *id_name_p,
    std::vector<int64_t> *id_arr_p, std::string *errmsg_p);
static void _do_refresh_one(CFMCluster::SendBuf *sendbuf_p, int *fret_p,
    CFMCluster::before_buf_refresh_func_t before_buf_refresh_func, void *before_buf_refresh_arg_p,
    CFMCluster::after_buf_refreshed_func_t after_buf_refreshed_func, void *after_buf_refreshed_arg_p);

static void _do_cancel_one(int *fret_p, const CFM_Basic_NS::striping_info_t striping_info,
    const std::string job_id, std::string *errmsg_p);

/////////////////////////////

static void
_buf_block_clear(CFMCluster::SendBuf::BufBlock * & bbp, void *arg_p)
{
    if (bbp) {
        delete bbp;
    }
}

static void
_do_send_block(const CFM_Basic_NS::cluster_info_t *cluster_info_p, size_t striping_id,
    CR_BlockQueue<CFMCluster::SendBuf::BufBlock*> *buf_queue_p, CFMConnect *conn_p,
    volatile int *errno_p, volatile bool *stop_p)
{
    int fret = 0;
    CFMCluster::SendBuf::BufBlock *bbp;

    while (1) {
        fret = buf_queue_p->pop_front(bbp, 0.05);
        if (!fret) {
            if (bbp) {
                fret = bbp->Send(conn_p, cluster_info_p, striping_id);
                delete bbp;
                if (fret) {
                    *errno_p = fret;
                    break;
                }
            }
        } else if (*stop_p) {
            break;
        }
    }
}

static void
_do_connect_one(CFMCluster::SendBuf *sendbuf_p, unsigned int comm_timeout, int *fret_p)
{
    *fret_p = sendbuf_p->Connect(comm_timeout);
}

static void
_do_jobstart_one(CFMCluster::SendBuf *sendbuf_p, int *fret_p, const std::string *lib_info_p,
    const bool static_work_path, const std::string *extra_param_p, std::string *errmsg_p)
{
    *fret_p = sendbuf_p->JobStart(*lib_info_p, static_work_path, extra_param_p, errmsg_p);
}

static void
_do_jobstop_one(CFMCluster::SendBuf *sendbuf_p, int *fret_p, const std::string *data_req_p,
    std::string *errmsg_p)
{
    *fret_p = sendbuf_p->JobStop(*data_req_p, errmsg_p);
}

static void
_do_jobquery_one(CFMCluster::SendBuf *sendbuf_p, int *fret_p, const std::string *data_req_p,
    std::string *result_p, std::string *errmsg_p)
{
    *fret_p = sendbuf_p->JobQuery(*data_req_p, result_p, errmsg_p);
}

static void
_do_jobkvput_one(CFMCluster::SendBuf *sendbuf_p, int *fret_p, const std::string *key_p,
    const std::string *value_p, const bool if_none_match, std::string *errmsg_p)
{
    *fret_p = sendbuf_p->JobKVPut(*key_p, value_p, if_none_match, errmsg_p);
}

static void
_do_jobkvget_one(CFMCluster::SendBuf *sendbuf_p, int *fret_p, const std::string *key_p,
    std::string *value_p, const double timeout_sec, std::string *errmsg_p)
{
    *fret_p = sendbuf_p->JobKVGet(*key_p, *value_p, timeout_sec, errmsg_p);
}

static void
_do_jobclear_one(CFMCluster::SendBuf *sendbuf_p, int *fret_p, const std::string *data_req_p,
    std::string *errmsg_p)
{
    *fret_p = sendbuf_p->JobClear(*data_req_p, errmsg_p);
}

static void
_do_refresh_one(CFMCluster::SendBuf *sendbuf_p, int *fret_p,
    CFMCluster::before_buf_refresh_func_t before_buf_refresh_func, void *before_buf_refresh_arg_p,
    CFMCluster::after_buf_refreshed_func_t after_buf_refreshed_func, void *after_buf_refreshed_arg_p)
{
    *fret_p = sendbuf_p->Refresh(before_buf_refresh_func, before_buf_refresh_arg_p,
      after_buf_refreshed_func, after_buf_refreshed_arg_p);
}

static void
_do_cancel_one(int *fret_p, const CFM_Basic_NS::striping_info_t striping_info,
    const std::string job_id, std::string *errmsg_p)
{
    *fret_p = CFMConnect_NS::JobCancel(striping_info, job_id, errmsg_p);
}

static void
_do_idfree_one(CFMCluster::SendBuf *sendbuf_p, int *fret_p, const std::string *id_name_p,
    std::vector<int64_t> *id_arr_p, std::string *errmsg_p)
{
    *fret_p = sendbuf_p->IDFree(*id_name_p, *id_arr_p, errmsg_p);
}

static void
_do_idsolidify_one(CFMCluster::SendBuf *sendbuf_p, int *fret_p, const std::string *id_name_p,
    std::vector<int64_t> *id_arr_p, std::string *errmsg_p)
{
    *fret_p = sendbuf_p->IDSolidify(*id_name_p, *id_arr_p, errmsg_p);
}

/////////////////////////////

CFMCluster::SendBuf::BufBlock::BufBlock(uint64_t fls_size, int64_t fls_size_mode)
{
    this->row_id_left = INT64_MAX;
    this->row_id_right = INT64_MIN;
    this->buf_fls.ReSize(fls_size, fls_size_mode);
    this->before_buf_refresh_func = NULL;
    this->before_buf_refresh_arg_p = NULL;
    this->after_buf_refreshed_func = NULL;
    this->after_buf_refreshed_arg_p = NULL;
}

void
CFMCluster::SendBuf::BufBlock::SetCallbacks(
    before_buf_refresh_func_t _before_buf_refresh_func, void *_before_buf_refresh_arg_p,
    after_buf_refreshed_func_t _after_buf_refreshed_func, void *_after_buf_refreshed_arg_p
)
{
    this->before_buf_refresh_func = _before_buf_refresh_func;
    this->before_buf_refresh_arg_p = _before_buf_refresh_arg_p;
    this->after_buf_refreshed_func = _after_buf_refreshed_func;
    this->after_buf_refreshed_arg_p = _after_buf_refreshed_arg_p;
}

int
CFMCluster::SendBuf::BufBlock::PushBack(const int64_t row_id, const int64_t obj_id,
    const void *key_p, const uint64_t key_size, const void *value_p, const uint64_t value_size)
{
    int ret = this->buf_fls.PushBack(obj_id, key_p, key_size, value_p, value_size);
    if ((ENOBUFS == ret) && (this->buf_fls.GetTotalLines() == 0)) {
        ret = this->buf_fls.ReSize(key_size + value_size + 64);
        if (!ret) {
            ret = this->buf_fls.PushBack(obj_id, key_p, key_size, value_p, value_size);
        }
    }
    if (!ret) {
        this->row_id_left = CR_Class_NS::min(this->row_id_left, row_id);
        this->row_id_right = CR_Class_NS::max(this->row_id_right, row_id);
    }
    return ret;
}

int
CFMCluster::SendBuf::BufBlock::Send(CFMConnect *conn_p,
    const CFM_Basic_NS::cluster_info_t *cluster_info_p, size_t striping_id)
{
    int ret = 0;
    uintptr_t pack_arg = 0;
    int64_t before_buf_refresh_ret_flags = 0;

    if (this->before_buf_refresh_func) {
        ret = this->before_buf_refresh_func(this->before_buf_refresh_arg_p,
          *cluster_info_p, striping_id, this->row_id_left, this->row_id_right,
          this->buf_fls, pack_arg, before_buf_refresh_ret_flags);
    }
    if (!ret) {
        if ((this->buf_fls.GetTotalLines() > 0)
          && ((before_buf_refresh_ret_flags & CFMCLUSTER_FLAG_DROP_IT) == 0)) {
            int64_t after_buf_refresh_ret_flags = 0;
            std::string data_req;
            std::string data_resp;
            std::string errmsg;
            do {
                this->buf_fls.SaveToString(data_req);
                ret = conn_p->JobData(this->row_id_left, this->row_id_right,
                  data_req, &data_resp, &errmsg);
                after_buf_refresh_ret_flags = 0;
                if (this->after_buf_refreshed_func) {
                    ret = this->after_buf_refreshed_func(this->after_buf_refreshed_arg_p,
                      *cluster_info_p, striping_id, this->row_id_left, this->row_id_right,
                      this->buf_fls, ret, data_resp, errmsg, pack_arg, after_buf_refresh_ret_flags);
                }
            } while (after_buf_refresh_ret_flags & CFMCLUSTER_FLAG_REDO_IT);
        }
    }

    return ret;
}

/////////////////////////////

CFMCluster::SendBuf::SendBuf(const CFM_Basic_NS::cluster_info_t &cluster_info,
    const size_t striping_id, uint64_t send_buf_size, const int64_t send_buf_size_mode)
{
    send_buf_size = CR_Class_NS::max<uint64_t>(send_buf_size, CFMCLUSTER_MIN_SEND_BUF_SIZE);

    this->buf_block_p = new BufBlock(send_buf_size, send_buf_size_mode);
    this->cluster_info_p = &cluster_info;
    this->conn_p = new CFMConnect;
    this->striping_id = striping_id;
    this->fls_size = send_buf_size;
    this->fls_size_mode = send_buf_size_mode;
    this->_buf_queue.set_args(_SEND_BUF_COUNT, _buf_block_clear);
    this->_send_th_stop = false;
    this->_send_th_errno = 0;
    this->_send_tp = new std::thread(_do_send_block, this->cluster_info_p, this->striping_id,
      &(this->_buf_queue), this->conn_p, &(this->_send_th_errno), &(this->_send_th_stop));
}

CFMCluster::SendBuf::~SendBuf()
{
    this->Refresh(NULL, NULL, NULL, NULL);
    if (this->_send_tp) {
        this->_send_th_stop = true;
        this->_send_tp->join();
        delete this->_send_tp;
    }
    if (this->conn_p)
        delete this->conn_p;
    if (this->buf_block_p)
        delete this->buf_block_p;
}

int
CFMCluster::SendBuf::do_send(before_buf_refresh_func_t before_buf_refresh_func, void *before_buf_refresh_arg_p,
    after_buf_refreshed_func_t after_buf_refreshed_func, void *after_buf_refreshed_arg_p)
{
    BufBlock *cur_buf_p = this->buf_block_p;

    this->buf_block_p = new BufBlock(this->fls_size, this->fls_size_mode);
    cur_buf_p->SetCallbacks(before_buf_refresh_func, before_buf_refresh_arg_p,
      after_buf_refreshed_func, after_buf_refreshed_arg_p);

    return this->_buf_queue.push_back(cur_buf_p, 600);
}

int
CFMCluster::SendBuf::SendRow(int64_t cur_row_id, const int64_t obj_id,
    const void *key_p, const uint64_t key_size, const void *value_p, const uint64_t value_size,
    before_buf_refresh_func_t before_buf_refresh_func, void *before_buf_refresh_arg_p,
    after_buf_refreshed_func_t after_buf_refreshed_func, void *after_buf_refreshed_arg_p)
{
    int ret = 0;
    int fret;

    this->_lck.lock();

    ret = this->_send_th_errno;
    if (!ret) {
        if (key_p) {
            while (1) {
                fret = this->buf_block_p->PushBack(cur_row_id, obj_id, key_p, key_size, value_p, value_size);
                if (!fret)
                    break;
                if (ENOBUFS == fret) {
                    ret = this->do_send(before_buf_refresh_func, before_buf_refresh_arg_p,
                      after_buf_refreshed_func, after_buf_refreshed_arg_p);
                    if (ret)
                        break;
                } else {
                    ret = fret;
                    break;
                }
            }
        } else {
            ret = this->do_send(before_buf_refresh_func, before_buf_refresh_arg_p,
              after_buf_refreshed_func, after_buf_refreshed_arg_p);
        }
    } else {
        DPRINTF("Striping[%llu].JobData(...) failed[%s]\n",
          (long long unsigned)this->striping_id, CR_Class_NS::strerrno(ret));
    }

    this->_lck.unlock();

    return ret;
}

int
CFMCluster::SendBuf::Refresh(before_buf_refresh_func_t before_buf_refresh_func, void *before_buf_refresh_arg_p,
    after_buf_refreshed_func_t after_buf_refreshed_func, void *after_buf_refreshed_arg_p)
{
    int ret = 0;

    this->_lck.lock();

    ret = this->do_send(before_buf_refresh_func, before_buf_refresh_arg_p,
      after_buf_refreshed_func, after_buf_refreshed_arg_p);

    if (!ret) {
        this->_send_th_stop = true;
        this->_send_tp->join();
        ret = this->_send_th_errno;
        delete this->_send_tp;
        this->_send_th_stop = false;
        this->_send_th_errno = 0;
        this->_send_tp = new std::thread(_do_send_block, this->cluster_info_p, this->striping_id,
          &(this->_buf_queue), this->conn_p, &(this->_send_th_errno), &(this->_send_th_stop));
    }

    this->_lck.unlock();

    return ret;
}

void
CFMCluster::SendBuf::SetJobID(const std::string &job_id)
{
    this->conn_p->SetJobID(job_id);
}

void
CFMCluster::SendBuf::SetTimeout(const unsigned int proc_timeout, const unsigned int comm_timeout)
{
    return this->conn_p->SetTimeout(proc_timeout, comm_timeout);
}

int
CFMCluster::SendBuf::Connect(unsigned int comm_timeout)
{
    return this->conn_p->Connect((*this->cluster_info_p)[this->striping_id], comm_timeout);
}

int
CFMCluster::SendBuf::JobStart(const std::string &lib_info, const bool static_work_path,
    const std::string *extra_param_p, std::string *errmsg_p)
{
    return this->conn_p->JobStart(lib_info, static_work_path, extra_param_p, errmsg_p);
}

int
CFMCluster::SendBuf::JobStop(const std::string &data_req, std::string *errmsg_p)
{
    return this->conn_p->JobStop(data_req, errmsg_p);
}

int
CFMCluster::SendBuf::JobQuery(const std::string &data_req, std::string *data_resp_p,
    std::string *errmsg_p)
{
    return this->conn_p->JobQuery(data_req, data_resp_p, errmsg_p);
}

int
CFMCluster::SendBuf::JobKVPut(const std::string &key, const std::string *value_p,
    const bool if_none_match, std::string *errmsg_p)
{
    return this->conn_p->JobKVPut(key, value_p, if_none_match, errmsg_p);
}

int
CFMCluster::SendBuf::JobKVGet(const std::string &key, std::string &value,
    const double timeout_sec, std::string *errmsg_p)
{
    int ret = 0;
    if (timeout_sec > 0.0) {
        double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;
        do {
            ret = this->conn_p->JobKVGet(key, value, 1, errmsg_p);
            if (!ret)
                break;
            if (ret != ETIMEDOUT)
                break;
        } while (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) < exp_time);
    } else {
        ret = this->conn_p->JobKVGet(key, value, timeout_sec, errmsg_p);
    }
    return ret;
}

int
CFMCluster::SendBuf::JobClear(const std::string &data_req, std::string *errmsg_p)
{
    return this->conn_p->JobClear(data_req, errmsg_p);
}

int
CFMCluster::SendBuf::IDAlloc(const std::string &id_name, const size_t alloc_count,
    const double autofree_timeout, std::vector<int64_t> &id_arr, std::string *errmsg_p)
{
    return this->conn_p->IDAlloc(id_name, alloc_count, autofree_timeout, id_arr, errmsg_p);
}

int
CFMCluster::SendBuf::IDFree(const std::string &id_name, const std::vector<int64_t> &id_arr,
    std::string *errmsg_p)
{
    return this->conn_p->IDFree(id_name, id_arr, errmsg_p);
}

int
CFMCluster::SendBuf::IDSolidify(const std::string &id_name, const std::vector<int64_t> &id_arr,
    std::string *errmsg_p)
{
    return this->conn_p->IDSolidify(id_name, id_arr, errmsg_p);
}

/////////////////////////////

CFMCluster::CFMCluster(const CFM_Basic_NS::cluster_info_t &cluster_info,
    const uint64_t send_buf_size, const int64_t send_buf_size_mode,
    const unsigned int proc_timeout, const unsigned int comm_timeout)
{
    this->_proc_timeout = proc_timeout;
    this->_comm_timeout = comm_timeout;
    this->_connected = false;
    this->_cluster_info = cluster_info;
    size_t cluster_size = this->_cluster_info.size();
    for (size_t i=0; i<cluster_size; i++) {
        SendBuf *sendbuf_p = new SendBuf(this->_cluster_info, i, send_buf_size, send_buf_size_mode);
        sendbuf_p->SetTimeout(this->_proc_timeout, this->_comm_timeout);
        this->_sendbuf_arr.push_back(sendbuf_p);
    }
}

CFMCluster::~CFMCluster()
{
    size_t cluster_size = this->_cluster_info.size();
    for (size_t i=0; i<cluster_size; i++) {
        SendBuf *sendbuf_p = this->_sendbuf_arr[i];
        if (sendbuf_p)
            delete sendbuf_p;
    }
}

void
CFMCluster::SetTimeout(const unsigned int proc_timeout, const unsigned int comm_timeout)
{
    std::lock_guard<std::mutex> _lck(this->_mtx);

    this->_proc_timeout = proc_timeout;
    this->_comm_timeout = comm_timeout;

    for (size_t i=0; i< this->_cluster_info.size(); i++) {
        SendBuf *sendbuf_p = this->_sendbuf_arr[i];
        if (sendbuf_p) {
            sendbuf_p->SetTimeout(this->_proc_timeout, this->_comm_timeout);
        }
    }
}

void
CFMCluster::SetRowID(int64_t row_id)
{
    this->_row_id.Reset(row_id);
}

int
CFMCluster::Connect(CFMCluster_NS::NodeErrorsT *errors_p)
{
    int ret = 0;
    std::vector<int> fret_arr;
    std::vector<std::thread*> tp_arr;

    size_t cluster_size = this->_cluster_info.size();
    if (cluster_size == 0)
        return ERANGE;

    fret_arr.resize(cluster_size);
    tp_arr.resize(cluster_size);

    if (errors_p)
        errors_p->clear();

    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (this->_connected)
        return EALREADY;

    for (size_t i=0; i<cluster_size; i++) {
        tp_arr[i] = new std::thread(_do_connect_one, this->_sendbuf_arr[i],
          this->_comm_timeout, &(fret_arr[i]));
    }

    for (size_t i=0; i<cluster_size; i++) {
        tp_arr[i]->join();
        delete tp_arr[i];
        if (fret_arr[i]) {
            ret = fret_arr[i];
            CFMCluster_NS::MergeError(errors_p, i, fret_arr[i], CR_Class_NS::strerrno(fret_arr[i]));
        }
    }

    if (!ret) {
        this->_connected = true;
        this->_row_id.Reset(0);
    }

    return ret;
}

void
CFMCluster::SetJobID(const std::string &job_id)
{
    std::lock_guard<std::mutex> _lck(this->_mtx);

    size_t cluster_size = this->_cluster_info.size();
    this->_job_id = job_id;
    for (size_t i=0; i<cluster_size; i++) {
        this->_sendbuf_arr[i]->SetJobID(job_id);
    }
}

std::string
CFMCluster::GetJobID()
{
    std::lock_guard<std::mutex> _lck(this->_mtx);
    return this->_job_id;
}

int
CFMCluster::Start(const std::string &lib_info, const bool static_work_path,
    const std::string *param_p, CFMCluster_NS::NodeErrorsT *errors_p)
{
    int ret = 0;
    std::vector<int> fret_arr;
    std::vector<std::thread*> tp_arr;
    std::vector<std::string> errmsg_arr;

    size_t cluster_size = this->_cluster_info.size();
    if (cluster_size == 0)
        return ERANGE;

    fret_arr.resize(cluster_size);
    tp_arr.resize(cluster_size);
    errmsg_arr.resize(cluster_size);

    if (errors_p)
        errors_p->clear();

    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (!this->_connected)
        return ENOTCONN;

    for (size_t i=0; i<cluster_size; i++) {
        tp_arr[i] = new std::thread(_do_jobstart_one, this->_sendbuf_arr[i], &(fret_arr[i]),
          &lib_info, static_work_path, param_p, &(errmsg_arr[i]));
    }

    for (size_t i=0; i<cluster_size; i++) {
        tp_arr[i]->join();
        delete tp_arr[i];
        if (fret_arr[i]) {
            ret = fret_arr[i];
            CFMCluster_NS::MergeError(errors_p, i, fret_arr[i], errmsg_arr[i]);
        }
    }

    return ret;
}

int
CFMCluster::Stop(const std::string &data_req, CFMCluster_NS::NodeErrorsT *errors_p)
{
    int ret = 0;
    std::vector<int> fret_arr;
    std::vector<std::thread*> tp_arr;
    std::vector<std::string> errmsg_arr;

    size_t cluster_size = this->_cluster_info.size();
    if (cluster_size == 0)
        return ERANGE;

    fret_arr.resize(cluster_size);
    tp_arr.resize(cluster_size);
    errmsg_arr.resize(cluster_size);

    if (errors_p)
        errors_p->clear();

    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (!this->_connected)
        return ENOTCONN;

    for (size_t i=0; i<cluster_size; i++) {
        tp_arr[i] = new std::thread(_do_jobstop_one, this->_sendbuf_arr[i], &(fret_arr[i]),
          &data_req, &(errmsg_arr[i]));
    }

    for (size_t i=0; i<cluster_size; i++) {
        tp_arr[i]->join();
        delete tp_arr[i];
        if (fret_arr[i]) {
            ret = fret_arr[i];
            CFMCluster_NS::MergeError(errors_p, i, fret_arr[i], errmsg_arr[i]);
        }
    }

    return ret;
}

int
CFMCluster::Query(const std::string &data_req, result_merge_func_t result_merge_func,
    void *merge_arg_p, CFMCluster_NS::NodeErrorsT *errors_p)
{
    int ret = 0;
    std::vector<int> fret_arr;
    std::vector<std::string> results;
    std::vector<std::string> errmsg_arr;
    std::vector<std::thread*> tp_arr;

    size_t cluster_size = this->_cluster_info.size();
    if (cluster_size == 0)
        return ERANGE;

    fret_arr.resize(cluster_size);
    results.resize(cluster_size);
    errmsg_arr.resize(cluster_size);
    tp_arr.resize(cluster_size);

    if (errors_p)
        errors_p->clear();

    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (!this->_connected)
        return ENOTCONN;

    for (size_t i=0; i<cluster_size; i++) {
        tp_arr[i] = new std::thread(_do_jobquery_one, this->_sendbuf_arr[i], &(fret_arr[i]),
          &data_req, &(results[i]), &(errmsg_arr[i]));
    }

    for (size_t i=0; i<cluster_size; i++) {
        tp_arr[i]->join();
        delete tp_arr[i];
        if (fret_arr[i]) {
            ret = fret_arr[i];
            CFMCluster_NS::MergeError(errors_p, i, fret_arr[i], errmsg_arr[i]);
        }
    }

    if ((!ret) && result_merge_func) {
        ret = result_merge_func(merge_arg_p, this->_cluster_info, data_req, results);
    }

    return ret;
}

int
CFMCluster::KVPut(const std::string &key, const std::string *value_p,
    const bool if_none_match, CFMCluster_NS::NodeErrorsT *errors_p)
{
    int ret = 0;
    std::vector<int> fret_arr;
    std::vector<std::string> errmsg_arr;
    std::vector<std::thread*> tp_arr;

    size_t cluster_size = this->_cluster_info.size();
    if (cluster_size == 0)
        return ERANGE;

    fret_arr.resize(cluster_size);
    errmsg_arr.resize(cluster_size);
    tp_arr.resize(cluster_size);

    if (errors_p)
        errors_p->clear();

    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (!this->_connected)
        return ENOTCONN;

    for (size_t i=0; i<cluster_size; i++) {
        tp_arr[i] = new std::thread(_do_jobkvput_one, this->_sendbuf_arr[i], &(fret_arr[i]),
          &key, value_p, if_none_match, &(errmsg_arr[i]));
    }

    for (size_t i=0; i<cluster_size; i++) {
        tp_arr[i]->join();
        delete tp_arr[i];
        if (fret_arr[i]) {
            ret = fret_arr[i];
            CFMCluster_NS::MergeError(errors_p, i, fret_arr[i], errmsg_arr[i]);
        }
    }

    return ret;
}

int
CFMCluster::KVGet(const std::string &key, const double timeout_sec,
    result_merge_func_t result_merge_func, void *merge_arg_p, CFMCluster_NS::NodeErrorsT *errors_p)
{
    int ret = 0;
    std::vector<int> fret_arr;
    std::vector<std::thread*> tp_arr;
    std::vector<std::string> errmsg_arr;
    std::vector<std::string> values;

    size_t cluster_size = this->_cluster_info.size();
    if (cluster_size == 0)
        return ERANGE;

    fret_arr.resize(cluster_size);
    tp_arr.resize(cluster_size);
    errmsg_arr.resize(cluster_size);
    values.resize(cluster_size);

    if (errors_p)
        errors_p->clear();

    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (!this->_connected)
        return ENOTCONN;

    for (size_t i=0; i<cluster_size; i++) {
        tp_arr[i] = new std::thread(_do_jobkvget_one, this->_sendbuf_arr[i], &(fret_arr[i]),
          &key, &(values[i]), timeout_sec, &(errmsg_arr[i]));
    }

    for (size_t i=0; i<cluster_size; i++) {
        tp_arr[i]->join();
        delete tp_arr[i];
        if (fret_arr[i]) {
            ret = fret_arr[i];
            CFMCluster_NS::MergeError(errors_p, i, fret_arr[i], errmsg_arr[i]);
        }
    }

    if ((!ret) && result_merge_func) {
        ret = result_merge_func(merge_arg_p, this->_cluster_info, key, values);
    }

    return ret;
}

int
CFMCluster::QueryOne(const size_t striping_id, const std::string &data_req,
    std::string *data_resp_p, std::string *errmsg_p)
{
    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (!this->_connected)
        return ENOTCONN;

    if (striping_id >= this->_cluster_info.size())
        return ERANGE;

    return this->_sendbuf_arr[striping_id]->JobQuery(data_req, data_resp_p, errmsg_p);
}

int
CFMCluster::KVPutOne(const size_t striping_id, const std::string &key, const std::string *value_p,
    const bool if_none_match, std::string *errmsg_p)
{
    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (!this->_connected)
        return ENOTCONN;

    if (striping_id >= this->_cluster_info.size())
        return ERANGE;

    return this->_sendbuf_arr[striping_id]->JobKVPut(key, value_p, if_none_match, errmsg_p);
}

int
CFMCluster::KVGetOne(const size_t striping_id, const std::string &key, std::string &value,
    const double timeout_sec, std::string *errmsg_p)
{
    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (!this->_connected)
        return ENOTCONN;

    if (striping_id >= this->_cluster_info.size())
        return ERANGE;

    return this->_sendbuf_arr[striping_id]->JobKVGet(key, value, timeout_sec, errmsg_p);
}

int
CFMCluster::Data(CR_FixedLinearStorage *data_send_p,
    on_splitter_call_func_t on_splitter_call_func, void *splitter_arg_p,
    before_buf_refresh_func_t before_buf_refresh_func, void *before_buf_refresh_arg_p,
    after_buf_refreshed_func_t after_buf_refreshed_func, void *after_buf_refreshed_arg_p)
{
    int ret = 0;
    int fret;
    int64_t row_id_in;
    int64_t row_id_out;
    int64_t obj_id;
    const char *key_p;
    uint64_t key_size;
    const char *value_p;
    uint64_t value_size;
    int target_pos;
    size_t cluster_size = this->_cluster_info.size();

    if (!this->_connected)
        return ENOTCONN;

    if (cluster_size == 0)
        return ERANGE;

    if (!data_send_p) {
        std::vector<int> fret_arr;
        std::vector<std::thread*> tp_arr;

        fret_arr.resize(cluster_size);
        tp_arr.resize(cluster_size);

        std::lock_guard<std::mutex> _lck(this->_mtx);

        for (size_t i=0; i<cluster_size; i++) {
            tp_arr[i] = new std::thread(_do_refresh_one, this->_sendbuf_arr[i], &(fret_arr[i]),
              before_buf_refresh_func, before_buf_refresh_arg_p,
              after_buf_refreshed_func, after_buf_refreshed_arg_p);
        }

        for (size_t i=0; i<cluster_size; i++) {
            tp_arr[i]->join();
            delete tp_arr[i];
            if (fret_arr[i]) {
                DPRINTF("[%s]node_send_buf[%llu]->Refresh() failed[%s]\n", this->_job_id.c_str(),
                  (long long unsigned)i, CR_Class_NS::strerrno(fret_arr[i]));
                ret = fret_arr[i];
            }
        }
    } else {
        uint64_t total_lines = data_send_p->GetTotalLines();
        for (uint64_t line_no=0; line_no<total_lines; line_no++) {
            fret = data_send_p->Query(line_no, obj_id, key_p, key_size, value_p, value_size);
            if (fret) {
                DPRINTF("[%s]data_send_p->Query(%llu, ...) failed[%s]\n", this->_job_id.c_str(),
                  (long long unsigned)line_no, CR_Class_NS::strerrno(fret));
                ret = fret;
                break;
            }
            row_id_in = this->_row_id.Get();
            row_id_out = row_id_in;
            if (on_splitter_call_func) {
                int64_t ret_flags = 0;
                target_pos = on_splitter_call_func(splitter_arg_p, this->_cluster_info,
                  row_id_in, obj_id, key_p, key_size, value_p, value_size, row_id_out, ret_flags);
                if (target_pos < 0) {
                    DPRINTF("[%s]on_splitter_call_func(?, ?, %lld, ...) failed[%s]\n",
                      this->_job_id.c_str(), (long long)row_id_in, CR_Class_NS::strerrno(0 - target_pos));
                    ret = (0 - target_pos);
                    break;
                }
                if (target_pos >= (int)cluster_size) {
                    DPRINTF("[%s]on_splitter_call_func(%lld, \"%s\", ...) == %d out of range[0 - %llu]\n",
                      this->_job_id.c_str(), (long long)row_id_in,
                      std::string((const char*)key_p, key_size).c_str(),
                      target_pos, (long long unsigned)cluster_size - 1);
                    ret = E2BIG;
                    break;
                }
                if (ret_flags & CFMCLUSTER_FLAG_SPLIT_BLOCK) {
                    fret = this->_sendbuf_arr[target_pos]->SendRow(0, 0, NULL, 0, NULL, 0,
                      before_buf_refresh_func, before_buf_refresh_arg_p,
                      after_buf_refreshed_func, after_buf_refreshed_arg_p);
                    if (fret) {
                        ret = fret;
                        break;
                    }
                }
                if (ret_flags & CFMCLUSTER_FLAG_REFRESH_FIRST) {
                    fret = this->_sendbuf_arr[target_pos]->Refresh(before_buf_refresh_func,
                      before_buf_refresh_arg_p, after_buf_refreshed_func, after_buf_refreshed_arg_p);
                    if (fret) {
                        ret = fret;
                        break;
                    }
                }
            } else {
                if ((uintptr_t)splitter_arg_p < cluster_size) {
                    target_pos = (uintptr_t)splitter_arg_p;
                } else {
                    target_pos = row_id_in % cluster_size;
                }
            }
            fret = this->_sendbuf_arr[target_pos]->SendRow(row_id_out,
              obj_id, key_p, key_size, value_p, value_size,
              before_buf_refresh_func, before_buf_refresh_arg_p,
              after_buf_refreshed_func, after_buf_refreshed_arg_p);
            if (fret) {
                ret = fret;
                break;
            }
        }
    }

    return ret;
}

int
CFMCluster::Clear(const std::string &data_req, CFMCluster_NS::NodeErrorsT *errors_p)
{
    int ret = 0;
    std::vector<int> fret_arr;
    std::vector<std::thread*> tp_arr;
    std::vector<std::string> errmsg_arr;

    size_t cluster_size = this->_cluster_info.size();
    if (cluster_size == 0)
        return ERANGE;

    fret_arr.resize(cluster_size);
    tp_arr.resize(cluster_size);
    errmsg_arr.resize(cluster_size);

    if (errors_p)
        errors_p->clear();

    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (!this->_connected)
        return ENOTCONN;

    for (size_t i=0; i<cluster_size; i++) {
        tp_arr[i] = new std::thread(_do_jobclear_one, this->_sendbuf_arr[i], &(fret_arr[i]),
          &data_req, &(errmsg_arr[i]));
    }

    for (size_t i=0; i<cluster_size; i++) {
        tp_arr[i]->join();
        delete tp_arr[i];
        if (fret_arr[i]) {
            ret = fret_arr[i];
            CFMCluster_NS::MergeError(errors_p, i, fret_arr[i], errmsg_arr[i]);
        }
    }

    return ret;
}

std::string
CFMCluster_NS::ExplainErrors(const NodeErrorsT &errors)
{
    std::string ret;
    std::map<size_t,std::string>::const_iterator it;
    size_t striping_id;
    uint32_t err_code;
    std::string err_msg;
    double err_time;
    std::string err_line;

    for (it=errors.cbegin(); it!=errors.cend(); it++) {
        striping_id = it->first;
        err_code = CR_Class_NS::error_decode(it->second, &err_msg, &err_time);
        if (err_code != UINT32_MAX) {
            err_line = CR_Class_NS::u162str(striping_id) + std::string(":");
            err_line += std::string("(") + CR_Class_NS::time2str(NULL, (time_t)err_time) + std::string(")");
            err_line += std::string("[") + CR_Class_NS::strerrno(err_code) + std::string("]");
            err_line += std::string("\"") + err_msg + std::string("\";");
            ret += err_line;
        }
    }

    return ret;
}

void
CFMCluster_NS::MergeError(NodeErrorsT *errors_p, const size_t striping_id,
    const int err_code, const std::string &err_msg)
{
    if (!errors_p)
        return;
    (*errors_p)[striping_id] = CR_Class_NS::error_encode(err_code, err_msg, CR_Class_NS::clock_gettime());
}

int
CFMCluster_NS::RemoteCancel(const std::string &cluster_str, const std::string &job_id,
    CFMCluster_NS::NodeErrorsT *errors_p)
{
    int ret = 0;
    int fret;
    CFM_Basic_NS::cluster_info_t cluster_info;
    std::vector<int> fret_arr;
    std::vector<std::thread*> tp_arr;
    std::vector<std::string> errmsg_arr;

    if (errors_p)
        errors_p->clear();

    fret = CFM_Basic_NS::parse_cluster_info(cluster_str, cluster_info);
    if (fret) {
        return fret;
    }

    size_t cluster_size = cluster_info.size();
    if (cluster_size == 0)
        return ERANGE;

    fret_arr.resize(cluster_size);
    tp_arr.resize(cluster_size);
    errmsg_arr.resize(cluster_size);

    for (size_t i=0; i<cluster_size; i++) {
        tp_arr[i] = new std::thread(_do_cancel_one, &(fret_arr[i]),
          cluster_info[i], job_id, &(errmsg_arr[i]));
    }

    for (size_t i=0; i<cluster_size; i++) {
        tp_arr[i]->join();
        delete tp_arr[i];
        if (fret_arr[i]) {
            ret = fret_arr[i];
            CFMCluster_NS::MergeError(errors_p, i, fret_arr[i], errmsg_arr[i]);
        }
    }

    return ret;
}

int
CFMCluster::IDAlloc(const std::string &id_name, const size_t alloc_count, const double autofree_timeout,
    std::vector<int64_t> &id_arr, std::string *errmsg_p)
{
    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (!this->_connected)
        return ENOTCONN;

    if (this->_cluster_info.size() == 0)
        return ERANGE;

    size_t striping_id = 0;

    if (CR_Class_NS::get_debug_level() <= 15)
        striping_id = rand() % this->_cluster_info.size();

    int fret = this->_sendbuf_arr[striping_id]->IDAlloc(id_name, alloc_count,
      autofree_timeout, id_arr, errmsg_p);

    if (!fret) {
        for (size_t i=0; i<id_arr.size(); i++) {
            id_arr[i] = id_arr[i] | ((int64_t)striping_id << CFM_ID_EFFECT_BIT_COUNT);
        }
    }

    return fret;
}

int
CFMCluster::IDFree(const std::string &id_name, const std::vector<int64_t> &id_arr, std::string *errmsg_p)
{
    int ret = 0;
    std::vector<int> fret_arr;
    std::vector<std::thread*> tp_arr;
    std::vector<std::vector<int64_t> > id_arr_arr;
    std::vector<std::string> errmsg_arr;
    CFMCluster_NS::NodeErrorsT cluster_errors;

    size_t cluster_size = this->_cluster_info.size();
    if (cluster_size == 0)
        return ERANGE;

    fret_arr.resize(cluster_size);
    tp_arr.resize(cluster_size);
    id_arr_arr.resize(cluster_size);
    errmsg_arr.resize(cluster_size);

    for (size_t i=0; i<id_arr.size(); i++) {
        int64_t cur_id = id_arr[i];
        int64_t striping_id = cur_id >> CFM_ID_EFFECT_BIT_COUNT;
        if ((striping_id > (int64_t)cluster_size) || (striping_id < 0)) {
            DPRINTF("[%s]ID %lld has invaild striping id %lld\n", this->_job_id.c_str(),
              (long long)cur_id, (long long)striping_id);
            return ERANGE;
        }
        id_arr_arr[striping_id].push_back(cur_id & ((INT64_C(1) << CFM_ID_EFFECT_BIT_COUNT) - 1));
    }

    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (!this->_connected)
        return ENOTCONN;

    for (size_t i=0; i<cluster_size; i++) {
        if (id_arr_arr[i].size() > 0) {
            tp_arr[i] = new std::thread(_do_idfree_one, this->_sendbuf_arr[i], &(fret_arr[i]),
              &id_name, &(id_arr_arr[i]), &(errmsg_arr[i]));
        } else {
            tp_arr[i] = NULL;
        }
    }

    for (size_t i=0; i<cluster_size; i++) {
        if (tp_arr[i]) {
            tp_arr[i]->join();
            delete tp_arr[i];
            if (fret_arr[i]) {
                ret = fret_arr[i];
                if (errmsg_p)
                    CFMCluster_NS::MergeError(&cluster_errors, i, fret_arr[i], errmsg_arr[i]);
            }
        }
    }

    if (errmsg_p)
        *errmsg_p = CFMCluster_NS::ExplainErrors(cluster_errors);

    return ret;
}

int
CFMCluster::IDSolidify(const std::string &id_name, const std::vector<int64_t> &id_arr, std::string *errmsg_p)
{
    int ret = 0;
    std::vector<int> fret_arr;
    std::vector<std::thread*> tp_arr;
    std::vector<std::vector<int64_t> > id_arr_arr;
    std::vector<std::string> errmsg_arr;
    CFMCluster_NS::NodeErrorsT cluster_errors;

    size_t cluster_size = this->_cluster_info.size();
    if (cluster_size == 0)
        return ERANGE;

    fret_arr.resize(cluster_size);
    tp_arr.resize(cluster_size);
    id_arr_arr.resize(cluster_size);
    errmsg_arr.resize(cluster_size);

    for (size_t i=0; i<id_arr.size(); i++) {
        int64_t cur_id = id_arr[i];
        int64_t striping_id = cur_id >> CFM_ID_EFFECT_BIT_COUNT;
        if ((striping_id > (int64_t)cluster_size) || (striping_id < 0)) {
            DPRINTF("[%s]ID %lld has invaild striping id %lld\n", this->_job_id.c_str(),
              (long long)cur_id, (long long)striping_id);
            return ERANGE;
        }
        id_arr_arr[striping_id].push_back(cur_id & ((INT64_C(1) << CFM_ID_EFFECT_BIT_COUNT) - 1));
    }

    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (!this->_connected)
        return ENOTCONN;

    for (size_t i=0; i<cluster_size; i++) {
        if (id_arr_arr[i].size() > 0) {
            tp_arr[i] = new std::thread(_do_idsolidify_one, this->_sendbuf_arr[i], &(fret_arr[i]),
              &id_name, &(id_arr_arr[i]), &(errmsg_arr[i]));
        } else {
            tp_arr[i] = NULL;
        }
    }

    for (size_t i=0; i<cluster_size; i++) {
        if (tp_arr[i]) {
            tp_arr[i]->join();
            delete tp_arr[i];
            if (fret_arr[i]) {
                ret = fret_arr[i];
                if (errmsg_p)
                    CFMCluster_NS::MergeError(&cluster_errors, i, fret_arr[i], errmsg_arr[i]);
            }
        }
    }

    if (errmsg_p)
        *errmsg_p = CFMCluster_NS::ExplainErrors(cluster_errors);

    return ret;
}
