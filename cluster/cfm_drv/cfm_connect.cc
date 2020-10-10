#include <vector>
#include <cr_class/cr_msgcode.h>
#include <cfm_drv/cfm_connect.h>

static int _do_raw_comm(const std::string &job_id, const std::vector<CR_SimpleComm*> &comm_array,
    size_t max_retry, const int8_t code_req, const std::string *data_req_p,
    int8_t &code_resp, std::string *data_resp_p, std::string *errmsg_p);

/////////////////////////////

CFMConnect::CFMConnect(const unsigned int proc_timeout, const unsigned int comm_timeout,
    const unsigned int max_retry)
{
    this->_proc_timeout = proc_timeout;
    this->_comm_timeout = comm_timeout;
    this->_connected = false;
    this->_max_retry = max_retry;
}

CFMConnect::~CFMConnect()
{
    this->_mutex.lock();

    this->_do_disconnect();

    this->_mutex.unlock();
}

void
CFMConnect::SetTimeout(const unsigned int proc_timeout, const unsigned int comm_timeout)
{
    this->_mutex.lock();

    this->_proc_timeout = proc_timeout;
    this->_comm_timeout = comm_timeout;

    for (size_t i=0; i< this->_comm_array.size(); i++) {
        CR_SimpleComm *comm_p = this->_comm_array[i];
        if (comm_p) {
            comm_p->set_timeout_send(this->_comm_timeout);
            comm_p->set_timeout_recv(this->_proc_timeout);
        }
    }

    this->_mutex.unlock();
}

int
CFMConnect::Connect(const CFM_Basic_NS::striping_info_t &striping_info, const bool stable_mode)
{
    int ret = 0;
    int fret;

    CR_Class_NS::nn_transport_t node_transport;
    std::string node_hostname;
    std::string node_srvname;
    size_t striping_size = striping_info.size();
    std::vector<CR_SimpleComm*> comm_array_tmp;

    if (striping_size == 0)
        return EINVAL;

    for (size_t i=0; i<striping_size; i++) {
        fret = CR_Class_NS::nn_addr_split(striping_info[i].c_str(),
          node_transport, node_hostname, node_srvname);
        if (!fret) {
            CR_SimpleComm *comm_p = new CR_SimpleComm;
            comm_p->set_chkstr(CFM_Basic_NS::get_default_chkstr());
            comm_p->set_timeout_send(this->_comm_timeout);
            comm_p->set_timeout_recv(this->_proc_timeout);
            fret = comm_p->Connect(node_hostname, node_srvname);
            if (!fret) {
                comm_array_tmp.push_back(comm_p);
            } else {
                delete comm_p;
                DPRINTF("Connect to node \"%s\" failed[%s]\n",
                  striping_info[i].c_str(), CR_Class_NS::strerrno(fret));
            }
        } else {
            DPRINTF("Invalid node address \"%s\"\n", striping_info[i].c_str());
        }
    }

    if (comm_array_tmp.size() == 0) {
        DPRINTF("All node connect failed\n");
        if (fret) {
            ret = fret;
        } else {
            ret = ECONNABORTED;
        }
    } else if (comm_array_tmp.size() <= (striping_info.size() / 2)) {
        DPRINTF("Stable striping failed\n");
        if (stable_mode) {
            for (size_t i=0; i<comm_array_tmp.size(); i++) {
                CR_SimpleComm *comm_p = comm_array_tmp[i];
                if (comm_p)
                    delete comm_p;
            }
            ret = ECONNABORTED;
        }
    }

    if (!ret) {
        this->_mutex.lock();
        this->_do_disconnect();
        this->_comm_array = comm_array_tmp;
        this->_mutex.unlock();
    }

    return ret;
}

void
CFMConnect::_do_disconnect()
{
    for (size_t i=0; i< this->_comm_array.size(); i++) {
        CR_SimpleComm *comm_p = this->_comm_array[i];
        if (comm_p)
            delete comm_p;
    }

    this->_comm_array.clear();

    this->_connected = false;
}

void
CFMConnect::DisConnect()
{
    this->_mutex.lock();
    this->_do_disconnect();
    this->_mutex.unlock();
}

void
CFMConnect::SetJobID(const std::string &job_id)
{
    this->_mutex.lock();
    this->_job_id = job_id;
    this->_mutex.unlock();
}

std::string
CFMConnect::GetJobID()
{
    this->_mutex.lock();

    return this->_job_id;

    this->_mutex.unlock();
}

int
CFMConnect::_raw_comm(const int8_t code_req, const std::string *data_req_p,
    int8_t &code_resp, std::string *data_resp_p, std::string *errmsg_p)
{
    int ret = 0;

    this->_mutex.lock();

    ret = _do_raw_comm(this->_job_id, this->_comm_array, this->_max_retry,
      code_req, data_req_p, code_resp, data_resp_p, errmsg_p);

    this->_mutex.unlock();

    return ret;
}

int
CFMConnect::JobStart(const std::string &lib_info, const bool static_work_path,
    const std::string *extra_param_p, std::string *errmsg_p)
{
    CFMJobStartReq msg_req_data;
    std::string msg_req_data_str;
    int8_t code_req = CFM_JOB_START_REQ;
    int8_t code_resp = CFM_JOB_START_RESP;

    msg_req_data.set_lib_info(lib_info);
    if (extra_param_p)
        msg_req_data.set_extra_param(*extra_param_p);
    if (static_work_path)
        msg_req_data.set_static_work_path(true);

    msg_req_data.SerializeToString(&msg_req_data_str);

    return this->_raw_comm(code_req, &msg_req_data_str, code_resp, NULL, errmsg_p);
}

int
CFMConnect::JobStop(const std::string &data_req, std::string *errmsg_p)
{
    int8_t code_req = CFM_JOB_STOP_REQ;
    int8_t code_resp = CFM_JOB_STOP_RESP;

    return this->_raw_comm(code_req, &data_req, code_resp, NULL, errmsg_p);
}

int
CFMConnect::JobQuery(const std::string &data_req, std::string *data_resp_p,
    std::string *errmsg_p)
{
    int8_t code_req = CFM_JOB_QUERY_REQ;
    int8_t code_resp = CFM_JOB_QUERY_RESP;

    return this->_raw_comm(code_req, &data_req, code_resp, data_resp_p, errmsg_p);
}

int
CFMConnect::JobData(const int64_t rowid_left, const int64_t rowid_right, const std::string &data_req,
    std::string *data_resp_p, std::string *errmsg_p)
{
    int ret = 0;

    CFMJobDataReq msg_data_req;
    std::string msg_data_req_str;
    std::string compressed_data;
    int8_t code_req = CFM_JOB_DATA_REQ;
    int8_t code_resp = CFM_JOB_DATA_RESP;

    ret = CR_Class_NS::compressx_snappy(data_req, compressed_data);
    if (ret)
        return ret;

    msg_data_req.set_data_req(compressed_data);
    msg_data_req.set_rowid_left(rowid_left);
    msg_data_req.set_rowid_right(rowid_right);

    msg_data_req.SerializeToString(&msg_data_req_str);

    ret = this->_raw_comm(code_req, &msg_data_req_str, code_resp, &compressed_data, errmsg_p);

    if (!ret && data_resp_p) {
        CR_Class_NS::decompressx(compressed_data, *data_resp_p);
    }

    return ret;
}

int
CFMConnect::JobClear(const std::string &data_req, std::string *errmsg_p)
{
    int8_t code_req = CFM_JOB_CLEAR_REQ;
    int8_t code_resp = CFM_JOB_CLEAR_RESP;

    return this->_raw_comm(code_req, &data_req, code_resp, NULL, errmsg_p);
}

int
CFMConnect::JobKVPut(const std::string &key, const std::string *value_p,
    const bool if_none_match, std::string *errmsg_p)
{
    CFMJobKVPutReq msg_req_data;
    std::string msg_req_data_str;
    int8_t code_req = CFM_JOB_KVPUT_REQ;
    int8_t code_resp = CFM_JOB_KVPUT_RESP;

    msg_req_data.set_key(key);
    if (value_p)
        msg_req_data.set_value(*value_p);
    if (if_none_match)
        msg_req_data.set_if_none_match(true);

    msg_req_data.SerializeToString(&msg_req_data_str);

    return this->_raw_comm(code_req, &msg_req_data_str, code_resp, NULL, errmsg_p);
}

int
CFMConnect::JobKVGet(const std::string &key, std::string &value,
    const double timeout_sec, std::string *errmsg_p)
{
    CFMJobKVGetReq msg_req_data;
    std::string msg_req_data_str;
    int8_t code_req = CFM_JOB_KVGET_REQ;
    int8_t code_resp = CFM_JOB_KVGET_RESP;

    msg_req_data.set_key(key);
    msg_req_data.set_timeout_sec(timeout_sec);

    msg_req_data.SerializeToString(&msg_req_data_str);

    return this->_raw_comm(code_req, &msg_req_data_str, code_resp, &value, errmsg_p);
}

int
CFMConnect::Ping(std::string *errmsg_p)
{
    int8_t code_req = CFM_PING_REQ;
    int8_t code_resp = CFM_PING_RESP;

    return this->_raw_comm(code_req, NULL, code_resp, NULL, errmsg_p);
}

int
CFMConnect::Lock(const std::string &lock_name, const std::string &lock_value, std::string *old_lock_value_p,
    const std::string &lock_pass, double lock_timeout_sec, double wait_timeout_sec, std::string *errmsg_p)
{
    CFMLockReq msg_req_data;
    std::string msg_req_data_str;
    std::string msg_resp_data_str;
    int8_t code_req = LOCK_REQ;
    int8_t code_resp = LOCK_RESP;

    msg_req_data.set_lock_name(lock_name);
    msg_req_data.set_lock_value(lock_value);
    msg_req_data.set_lock_pass(lock_pass);
    msg_req_data.set_lock_timeout(lock_timeout_sec);
    msg_req_data.set_wait_timeout(wait_timeout_sec);

    msg_req_data.SerializeToString(&msg_req_data_str);

    int fret = this->_raw_comm(code_req, &msg_req_data_str, code_resp, &msg_resp_data_str, errmsg_p);

    if (fret)
        return fret;

    return CR_Class_NS::error_decode(msg_resp_data_str, old_lock_value_p);
}

int
CFMConnect::Unlock(const std::string &lock_name, const std::string &lock_pass, std::string *errmsg_p)
{
    CFMUnlockReq msg_req_data;
    std::string msg_req_data_str;
    std::string msg_resp_data_str;
    int8_t code_req = UNLOCK_REQ;
    int8_t code_resp = UNLOCK_RESP;

    msg_req_data.set_lock_name(lock_name);
    msg_req_data.set_lock_pass(lock_pass);

    msg_req_data.SerializeToString(&msg_req_data_str);

    int fret = this->_raw_comm(code_req, &msg_req_data_str, code_resp, &msg_resp_data_str, errmsg_p);

    if (fret)
        return fret;

    return CR_Class_NS::error_decode(msg_resp_data_str);
}

int
CFMConnect::IDAlloc(const std::string &id_name, const size_t alloc_count,
    const double autofree_timeout, std::vector<int64_t> &id_arr, std::string *errmsg_p)
{
    CFMIDReq msg_req_data;
    std::string msg_req_data_str;
    int8_t code_req = CFM_ID_REQ;
    int8_t code_resp = CFM_ID_RESP;

    msg_req_data.set_cmd_code(CFM_ID_ALLOC);
    msg_req_data.set_id_name(id_name);
    msg_req_data.set_alloc_count(alloc_count);
    msg_req_data.set_autofree_timeout(autofree_timeout);

    msg_req_data.SerializeToString(&msg_req_data_str);

    std::string msg_resp_data_str;

    int fret = this->_raw_comm(code_req, &msg_req_data_str, code_resp, &msg_resp_data_str, errmsg_p);
    if (fret)
        return fret;

    CFMIDResp msg_resp_data;

    if (!msg_resp_data.ParseFromString(msg_resp_data_str))
        return ENOMSG;

    id_arr.clear();

    for (int i=0; i<msg_resp_data.id_list_size(); i++) {
        id_arr.push_back(msg_resp_data.id_list(i));
    }

    return 0;
}

int
CFMConnect::IDFree(const std::string &id_name, const std::vector<int64_t> &id_arr, std::string *errmsg_p)
{
    CFMIDReq msg_req_data;
    std::string msg_req_data_str;
    int8_t code_req = CFM_ID_REQ;
    int8_t code_resp = CFM_ID_RESP;

    msg_req_data.set_cmd_code(CFM_ID_FREE);
    msg_req_data.set_id_name(id_name);

    for (size_t i=0; i<id_arr.size(); i++) {
        msg_req_data.add_id_list(id_arr[i]);
    }

    msg_req_data.SerializeToString(&msg_req_data_str);

    return this->_raw_comm(code_req, &msg_req_data_str, code_resp, NULL, errmsg_p);
}

int
CFMConnect::IDSolidify(const std::string &id_name, const std::vector<int64_t> &id_arr, std::string *errmsg_p)
{
    CFMIDReq msg_req_data;
    std::string msg_req_data_str;
    int8_t code_req = CFM_ID_REQ;
    int8_t code_resp = CFM_ID_RESP;

    msg_req_data.set_cmd_code(CFM_ID_SOLIDIFY);
    msg_req_data.set_id_name(id_name);

    for (size_t i=0; i<id_arr.size(); i++) {
        msg_req_data.add_id_list(id_arr[i]);
    }

    msg_req_data.SerializeToString(&msg_req_data_str);

    return this->_raw_comm(code_req, &msg_req_data_str, code_resp, NULL, errmsg_p);
}

/////////////////////////////

static int
_do_raw_comm(const std::string &job_id, const std::vector<CR_SimpleComm*> &comm_array,
    size_t max_retry, const int8_t code_req, const std::string *data_req_p,
    int8_t &code_resp, std::string *data_resp_p, std::string *errmsg_p)
{
    int ret = 0;
    int8_t code_resp_local = code_resp;
    std::string req_str;
    std::string resp_str;
    std::string job_id_resp;

    if (max_retry < 1)
        max_retry = 1;

    ret = CFM_Basic_NS::exchange_msg_merge(job_id, data_req_p, req_str);
    if (!ret) {
        CFM_Basic_NS::exchange_msg_dsbi(req_str);
        for (size_t i=0; i<comm_array.size(); i++) {
            CR_SimpleComm *comm_p = comm_array[i];
            for (size_t retry=0; retry<=max_retry; retry++) {
                ret = comm_p->SendAndRecv(code_req, &req_str, code_resp_local, &resp_str);
                if (!ret) {
                    break;
                } else if (ret == EEXIST) {
                    break;
                }
            }
            if (!ret) {
                if (code_resp_local == code_resp) {
                    ret = CFM_Basic_NS::exchange_msg_parse(resp_str, job_id_resp, data_resp_p);
                    if (!ret) {
                        if (job_id_resp != job_id) {
                            ret = EPROTOTYPE;
                        }
                    }
                } else {
                    code_resp = code_resp_local;
                    ret = CR_Class_NS::error_decode(resp_str, errmsg_p);
                }
                break;
            }
        }
    }

    return ret;
}

/////////////////////////////

int
CFMConnect_NS::JobCancel(const CFM_Basic_NS::striping_info_t &striping_info,
    const std::string &job_id, std::string *errmsg_p)
{
    int ret = 0;
    int fret;
    int8_t code_req = CFM_JOB_CANCEL_REQ;
    int8_t code_resp = CFM_JOB_CANCEL_RESP;
    char caller_cmd[64];
    std::string caller_info;
    char caller_info_buf[4096];
    CR_Class_NS::nn_transport_t node_transport;
    std::string node_hostname;
    std::string node_srvname;
    size_t striping_size = striping_info.size();
    CR_SimpleComm comm_tmp;
    std::vector<CR_SimpleComm*> comm_array_tmp;

    if (striping_size == 0)
        return EINVAL;

    comm_array_tmp.push_back(&comm_tmp);
    comm_tmp.set_chkstr(CFM_Basic_NS::get_default_chkstr());

    snprintf(caller_cmd, sizeof(caller_cmd), "ps -ewwu -p %d 2> /dev/null", getpid());

    FILE *fp = popen(caller_cmd, "r");
    while (fp) {
        size_t read_count = fread(caller_info_buf, 1, sizeof(caller_info_buf), fp);
        if (read_count > 0) {
            caller_info.append(caller_info_buf, read_count);
        } else {
            pclose(fp);
            break;
        }
    }

    for (size_t i=0; i<striping_size; i++) {
        fret = CR_Class_NS::nn_addr_split(striping_info[i].c_str(),
          node_transport, node_hostname, node_srvname);
        if (!fret) {
            fret = comm_tmp.Connect(node_hostname, node_srvname, 10);
            if (!fret) {
                fret = _do_raw_comm(job_id, comm_array_tmp, 1,
                  code_req, &caller_info, code_resp, NULL, errmsg_p);
            } else {
                ret = fret;
                DPRINTF("Connect to node \"%s\" failed[%s]\n",
                  striping_info[i].c_str(), CR_Class_NS::strerrno(fret));
            }
        } else {
            ret = fret;
            DPRINTF("Invalid node address \"%s\"\n", striping_info[i].c_str());
        }
    }

    return ret;
}
