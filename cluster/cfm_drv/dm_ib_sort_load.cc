#include <cr_class/cr_class.h>
#include <cr_class/cr_class.pb.h>
#include <cfm_drv/dm_ib_sort_load.h>

#define IBSORT_SLAVE_CLIENT_PREFIX		"slave_client-"
#define IBSORT_LIB_SORT_NAME			"sort"
#define IBSORT_LIB_IBLOAD_NAME			"ibload"

/////////////////////////////////

static int _make_splitter(void *merge_arg_p, const CFM_Basic_NS::cluster_info_t &cluster_info,
    const std::string &data_req, const std::vector<std::string> &results);

static int _set_line_count_arr(void *merge_arg_p, const CFM_Basic_NS::cluster_info_t &cluster_info,
    const std::string &data_req, const std::vector<std::string> &results);

static int _merge_result(void *merge_arg_p, const CFM_Basic_NS::cluster_info_t &cluster_info,
    const std::string &data_req, const std::vector<std::string> &results);

static int on_splitter(void *splitter_arg_p, const CFM_Basic_NS::cluster_info_t &cluster_info,
    const int64_t row_id_in, const int64_t obj_id, const void *key_p, const uint64_t key_size,
    const void *value_p, const uint64_t value_size, int64_t &row_id_out, int64_t &ret_flags);

static int before_buf_refresh(void *private_arg_p,
    const CFM_Basic_NS::cluster_info_t &cluster_info, const size_t target_striping_id,
    int64_t &row_id_left, int64_t &row_id_right, CR_FixedLinearStorage &fls_req,
    uintptr_t &pack_arg, int64_t &ret_flags);

static int after_buf_refreshed(void *refreshed_arg_p,
    const CFM_Basic_NS::cluster_info_t &cluster_info, const size_t target_striping_id,
    int64_t &row_id_left, int64_t &row_id_right, CR_FixedLinearStorage &fls_req,
    const int jobdata_ret, const std::string &data_resp, const std::string &errmsg,
    uintptr_t &pack_arg, int64_t &ret_flags);

/////////////////////////////////

DMIBSortLoad::DMIBSortLoad(const std::string &job_id, const bool is_master_client,
    const std::string *sort_param_p)
{
    int fret;

    this->_splitter_p = NULL;
    this->_job_id = job_id;
    this->_sort_job_id = job_id + std::string("S");
    this->_load_job_id = job_id + std::string("L");
    this->_is_master = is_master_client;
    this->_total_lines = 0;
    this->_crcsum_out = 0;

    DPRINTF("[%s]Init cluster connect ...\n", this->_job_id.c_str());

    char *cluster_str_p = getenv(DMIBSORTLOAD_EN_CLUSTER_STR);
    if (!cluster_str_p) {
        THROWF("[%s]Environment variable \"%s\" not set\n",
          this->_job_id.c_str(), DMIBSORTLOAD_EN_CLUSTER_STR);
    }
    this->_cluster_str = cluster_str_p;

    CFM_Basic_NS::cluster_info_t cluster_info;

    fret = CFM_Basic_NS::parse_cluster_info(this->_cluster_str, cluster_info);
    if (fret)
        THROWF("[%s]Parse cluster info failed[%s]\n", this->_job_id.c_str(), CR_Class_NS::strerrno(fret));

    this->_cluster_info = cluster_info;

    size_t send_buf_size = 1024 * 1024;
    uint64_t proc_timeout = 86400;
    uint64_t comm_timeout = 600;
    std::string last_job_id;

    if (sort_param_p) {
        send_buf_size = CR_Class_NS::str_getparam_u64(*sort_param_p,
          DMIBSORTLOAD_PN_SEND_BUF_SIZE, 1024 * 1024);
        proc_timeout = CR_Class_NS::str_getparam_u64(*sort_param_p,
          DMIBSORTLOAD_PN_TIMEOUT_PROC, 86400);
        comm_timeout = CR_Class_NS::str_getparam_u64(*sort_param_p,
          DMIBSORTLOAD_PN_TIMEOUT_COMM, 600);
        last_job_id = CR_Class_NS::str_getparam(*sort_param_p, DMIBSORTLOAD_PN_LAST_JOB_ID);
    }

    DPRINTF("[%s]send_buf_size == %llu, timeout_proc == %u, timeout_comm == %u\n",
      this->_job_id.c_str(), (long long)send_buf_size, (unsigned)proc_timeout, (unsigned)comm_timeout);

    this->_sort_cluster_p = new CFMCluster(this->_cluster_info,
      send_buf_size, CR_FLS_SIZEMODE_SMALL, proc_timeout, comm_timeout);
    this->_load_cluster_p = new CFMCluster(this->_cluster_info,
      send_buf_size, CR_FLS_SIZEMODE_SMALL, proc_timeout, comm_timeout);

    this->_wait_timeoutsec = proc_timeout;

    this->_sort_cluster_p->SetJobID(this->_sort_job_id);
    this->_load_cluster_p->SetJobID(this->_load_job_id);

    CFMCluster_NS::NodeErrorsT cluster_error;

    fret = this->_sort_cluster_p->Connect(&cluster_error);
    if (fret)
        THROWF("[%s]sort_cluster->Connect failed, msg => [%s]\n", this->_job_id.c_str(),
          CFMCluster_NS::ExplainErrors(cluster_error).c_str());

    fret = this->_load_cluster_p->Connect(&cluster_error);
    if (fret)
        THROWF("[%s]load_cluster->Connect failed, msg => [%s]\n", this->_job_id.c_str(),
          CFMCluster_NS::ExplainErrors(cluster_error).c_str());

    DPRINTF("[%s]Init cluster connect OK\n", this->_job_id.c_str());

    std::string client_reg_key;

    if (is_master_client) {
        this->_client_id = 0;

        DPRINTF("[%s]Start cluster job ...\n", this->_job_id.c_str());

        std::string libsort_name = IBSORT_LIB_SORT_NAME;
        std::string libibload_name = IBSORT_LIB_IBLOAD_NAME;
        bool keep_sort_tmp = false;
        if (sort_param_p) {
            keep_sort_tmp = CR_Class_NS::str_getparam_bool(*sort_param_p,
              DMIBSORTLOAD_PN_KEEP_SORT_TMP, false);
        }

        fret = this->_sort_cluster_p->Start(libsort_name, keep_sort_tmp, sort_param_p, &cluster_error);
        if (fret) {
            this->_sort_cluster_p->Stop(CR_Class_NS::blank_string, NULL);
            this->_load_cluster_p->Stop(CR_Class_NS::blank_string, NULL);
            THROWF("[%s]sort_cluster->Start failed, msg => [%s]\n", this->_job_id.c_str(),
              CFMCluster_NS::ExplainErrors(cluster_error).c_str());
        }

        fret = this->_load_cluster_p->Start(libibload_name, false, sort_param_p, &cluster_error);
        if (fret) {
            this->_sort_cluster_p->Stop(CR_Class_NS::blank_string, NULL);
            this->_load_cluster_p->Stop(CR_Class_NS::blank_string, NULL);
            THROWF("[%s]load_cluster->Start failed, msg => [%s]\n", this->_job_id.c_str(),
              CFMCluster_NS::ExplainErrors(cluster_error).c_str());
        }

        DPRINTF("[%s]Start cluster job OK\n", this->_job_id.c_str());

        client_reg_key = std::string(DMIBSORTLOAD_KN_CLIENT_REG_PREFIX);
        client_reg_key += CR_Class_NS::u642str(this->_client_id);
        fret = this->_sort_cluster_p->KVPutOne(0, client_reg_key, &client_reg_key, true);
        if (fret) {
            THROWF("[%s]Master client register failed[%s]\n",
              this->_job_id.c_str(), CR_Class_NS::strerrno(fret));
        }

        DPRINTF("[%s]Master client register OK, client id is %llu\n",
          this->_job_id.c_str(), (long long unsigned)this->_client_id);

        std::string query_cmd_str;
        query_cmd_str = CR_Class_NS::str_setparam(query_cmd_str,
          DMIBSORTLOAD_PN_QUERY_CMD, LIBSORT_PV_QUERY_KEY_SAMPLE);
        query_cmd_str = CR_Class_NS::str_setparam(query_cmd_str,
          DMIBSORTLOAD_PN_LAST_JOB_ID, last_job_id);
        fret = this->_sort_cluster_p->Query(query_cmd_str, _make_splitter,
          &(this->_splitter_p), &cluster_error);

        if (this->_splitter_p) {
            std::string splitter_str = this->_splitter_p->Save();
            fret = this->_sort_cluster_p->KVPutOne(0, std::string(LIBSORT_KN_MASTER_STARTED),
              &splitter_str, true);
        } else {
            fret = this->_sort_cluster_p->KVPutOne(0, std::string(LIBSORT_KN_MASTER_STARTED),
              &(CR_Class_NS::blank_string), true);
        }
        if (fret)
            THROWF("[%s]Save MASTER_START failed[%s]\n", this->_job_id.c_str(), CR_Class_NS::strerrno(fret));
    } else {
        for (this->_client_id=1; ; ) {
            client_reg_key = std::string(DMIBSORTLOAD_KN_CLIENT_REG_PREFIX);
            client_reg_key += CR_Class_NS::u642str(this->_client_id);
            fret = this->_sort_cluster_p->KVPutOne(0, client_reg_key, &client_reg_key, true);
            if (!fret) {
                DPRINTF("[%s]Slave client register OK, client id is %llu\n",
                  this->_job_id.c_str(), (long long unsigned)this->_client_id);
                break;
            } else if (fret == EEXIST) {
                DPRINTF("[%s]Client id %llu already exist, try next\n",
                  this->_job_id.c_str(), (long long unsigned)this->_client_id);
                this->_client_id++;
            } else if (fret == ENOENT) {
                DPRINTF("[%s]Waiting job start ...\n", this->_job_id.c_str());
                sleep(1);
            } else {
                THROWF("[%s]Slave client register failed[%s]\n",
                  this->_job_id.c_str(), CR_Class_NS::strerrno(fret));
            }
        }

        this->_sort_cluster_p->SetRowID(this->_client_id * (UINT64_C(1) << 48));
        std::string splitter_str;

        while (1) {
            fret = this->_sort_cluster_p->KVGetOne(0,
              std::string(LIBSORT_KN_MASTER_STARTED), splitter_str, 1);
            if (!fret) {
                if (splitter_str.size() > 0) {
                    this->_splitter_p = new CFMSplitter;
                    fret = this->_splitter_p->Load(splitter_str);
                    if (fret) {
                        DPRINTF("[%s]Load splitter info, %s\n", this->_job_id.c_str(),
                          CR_Class_NS::strerrno(fret));
                        delete this->_splitter_p;
                        this->_splitter_p = NULL;
                    }
                }
                break;
            } else if (fret == ETIMEDOUT) {
                DPRINTF("[%s]Waiting master client...\n", this->_job_id.c_str());
            } else {
                THROWF("[%s]Wait MASTER_STAT failed[%s]\n",
                  this->_job_id.c_str(), CR_Class_NS::strerrno(fret));
            }
        }
    }
}

DMIBSortLoad::~DMIBSortLoad()
{
    if (this->_sort_cluster_p)
        delete this->_sort_cluster_p;

    if (this->_load_cluster_p)
        delete this->_load_cluster_p;
}

size_t
DMIBSortLoad::GetClientID()
{
    size_t ret = 0;

    this->_lck.lock();
    ret = this->_client_id;
    this->_lck.unlock();

    return ret;
}

std::string
DMIBSortLoad::GetJobID()
{
    std::string ret;

    this->_lck.lock();
    ret = this->_job_id;
    this->_lck.unlock();

    return ret;
}

int
DMIBSortLoad::PushLines(CR_FixedLinearStorage &lines, std::string *err_msg)
{
    int ret = 0;
    std::string tag_tmp = lines.GetTag();
    CFMCluster_NS::NodeErrorsT cluster_error;

    if (CR_Class_NS::str_getparam(tag_tmp, DMIBSORTLOAD_PN_QUERY_CMD) == LIBSORT_PV_APPEND_PART_FILES) {
        std::vector<std::string> target_jobid_array;
        int64_t line_count_tmp;
        std::string job_id_tmp;
        std::string _value_tmp;
        bool _value_is_null_tmp;
        std::vector<uint64_t> result_arr;
        int64_t total_lines_want = 0;

        result_arr.push_back(0);
        result_arr.push_back(0);

        for (size_t i=0; i<lines.GetTotalLines(); i++) {
            lines.Query(i, line_count_tmp, job_id_tmp, _value_tmp, _value_is_null_tmp);
            if ((line_count_tmp >= 0) && (total_lines_want >= 0)) {
                total_lines_want += line_count_tmp;
            } else {
                total_lines_want = -1;
            }
            target_jobid_array.push_back(job_id_tmp);
        }

        tag_tmp = CR_Class_NS::str_setparam_strarr(tag_tmp, LIBSORT_PN_TARGET_JOB_ID, target_jobid_array);
        ret = this->_sort_cluster_p->Query(tag_tmp, _merge_result, &result_arr, &cluster_error);

        if (!ret) {
            std::vector<uint64_t> client_info;
            std::string client_info_key;
            std::string client_info_value;

            if (total_lines_want >= 0) {
                if (result_arr[0] != (uint64_t)total_lines_want) {
                    DPRINTF("[%s]Want merge %llu lines, but got %llu lines, CRCSUM[0x%016llX]\n",
                      this->_job_id.c_str(), (long long unsigned)total_lines_want,
                      (long long unsigned)result_arr[0], (long long)result_arr[1]);
                    ret = EMSGSIZE;
                }
            }

            if (!ret) {
                client_info_key = std::string(DMIBSORTLOAD_KN_CLIENT_INFO_PREFIX);
                client_info_key += CR_Class_NS::u642str(this->_client_id);

                this->_lck.lock();
                this->_total_lines += result_arr[0];
                this->_crcsum_out += result_arr[1];
                client_info.push_back(this->_total_lines);
                client_info.push_back(this->_crcsum_out);
                this->_lck.unlock();

                DPRINTF("[%s]Total %llu lines merged, CRCSUM[0x%016llX]\n",
                  this->_job_id.c_str(), (long long unsigned)result_arr[0], (long long)result_arr[1]);

                client_info_value = CR_Class_NS::u64arr2str(client_info);

                ret = this->_sort_cluster_p->KVPutOne(0, client_info_key, &client_info_value);

                DPRINTF("[%s]Save checkpoint, %s\n", this->_job_id.c_str(), CR_Class_NS::strerrno(ret));
            }
        } else {
            DPRINTF("[%s]sort_cluster->Query(APPEND_PART_FILES) failed, msg => [%s]\n",
              this->_job_id.c_str(), CFMCluster_NS::ExplainErrors(cluster_error).c_str());
        }
    } else {
        ret = this->_sort_cluster_p->Data(&lines, on_splitter, this->_splitter_p,
          before_buf_refresh, &(this->_job_id), after_buf_refreshed, &(this->_job_id));
        if (!ret) {
            this->_lck.lock();
            this->_total_lines += lines.GetTotalLines();
            this->_crcsum_out += lines.GetObjIDCKSUM() + lines.GetValueCKSUM();
            this->_lck.unlock();
        }
    }

    if (ret) {
        if (err_msg) {
            *err_msg = CFMCluster_NS::ExplainErrors(cluster_error);
        }
    }

    return ret;
}

int
DMIBSortLoad::Finish(const std::string &load_param, std::string *err_msg, std::string *result_msg_p,
    const bool turn_master_flag, const double clients_wait_timeout)
{
    int ret = 0;
    int fret;
    std::string client_info_key;
    std::string client_info_value;
    std::vector<uint64_t> client_info;
    CFMCluster_NS::NodeErrorsT cluster_error;

    this->_lck.lock();
    client_info.push_back(this->_total_lines);
    client_info.push_back(this->_crcsum_out);
    this->_lck.unlock();

    client_info_value = CR_Class_NS::u64arr2str(client_info);

    DPRINTF("[%s]Refresh data send cache\n", this->_job_id.c_str());

    ret = this->_sort_cluster_p->Data(NULL, on_splitter, this->_splitter_p,
      before_buf_refresh, &(this->_job_id), after_buf_refreshed, &(this->_job_id));

    DPRINTF("[%s]Refresh data send cache, %s\n", this->_job_id.c_str(), CR_Class_NS::strerrno(ret));

    if (!ret) {
        client_info_key = std::string(DMIBSORTLOAD_KN_CLIENT_INFO_PREFIX);
        client_info_key += CR_Class_NS::u642str(this->_client_id);

        ret = this->_sort_cluster_p->KVPutOne(0, client_info_key, &client_info_value);

        DPRINTF("[%s]Save checkpoint, %s\n", this->_job_id.c_str(), CR_Class_NS::strerrno(ret));
    }

    if (turn_master_flag)
        this->_is_master = !(this->_is_master);

    if (!ret && this->_is_master) {
        std::string query_cmd_str;
        std::vector<uint64_t> line_count_arr;

        this->_lck.lock();

        do {
            std::string table_info_str = CR_Class_NS::str_getparam(load_param, LIBIBLOAD_PN_TABLE_INFO);
            if (table_info_str.size() == 0) {
                DPRINTF("table_info_str.size() == 0\n");
                ret = EINVAL;
                break;
            }

            std::string save_pathname = CR_Class_NS::str_getparam(load_param, LIBIBLOAD_PN_SAVE_PATHNAME);
            if (save_pathname.size() == 0) {
                DPRINTF("save_pathname.size() == 0\n");
                ret = EINVAL;
                break;
            }

            int64_t lineid_begin = CR_Class_NS::str_getparam_i64(load_param, DMIBSORTLOAD_PN_LINEID_BEGIN, -1);
            if (lineid_begin < 0) {
                DPRINTF("lineid_begin < 0\n");
                ret = EINVAL;
                break;
            }

            size_t block_lines = CR_Class_NS::str_getparam_u64(load_param, DMIBSORTLOAD_PN_BLOCK_LINES, 0);
            if (block_lines == 0) {
                DPRINTF("block_lines == 0\n");
                ret = EINVAL;
                break;
            }

            std::vector<uint64_t> packindex_arr = CR_Class_NS::str_getparam_u64arr(load_param,
              LIBIBLOAD_PN_PACKINDEX_INFO);
            bool pki_multimode = CR_Class_NS::str_getparam_bool(load_param, LIBIBLOAD_PN_PACKINDEX_MULTIMODE);
            size_t filethread_count = CR_Class_NS::str_getparam_u64(load_param, LIBIBLOAD_PN_FILETHREAD_COUNT);

            size_t total_clients = 0;

            for (size_t target_client_id=0; ; target_client_id++) {
                std::string client_reg_key, client_reg_value;
                client_reg_key = std::string(DMIBSORTLOAD_KN_CLIENT_REG_PREFIX);
                client_reg_key += CR_Class_NS::u642str(target_client_id);
                fret = this->_sort_cluster_p->KVGetOne(0, client_reg_key, client_reg_value);
                if (fret) {
                    if (fret == ENOENT) {
                        DPRINTF("[%s]Total %llu clients in job detected\n", this->_job_id.c_str(),
                          (long long unsigned)target_client_id);
                        total_clients = target_client_id;
                    } else {
                        DPRINTF("[%s]Detect clients failed[%s]\n",
                          this->_job_id.c_str(), CR_Class_NS::strerrno(fret));
                        ret = fret;
                    }
                    break;
                }
            }

            if (ret)
                break;

            uint64_t job_total_lines = 0;
            uint64_t job_crcsum_out = 0;

            double clients_wait_exptime = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + clients_wait_timeout;

            DPRINTF("[%s]Query job total lines ...\n", this->_job_id.c_str());
            for (size_t target_client_id=0; target_client_id<total_clients; ) {
                client_info_key = std::string(DMIBSORTLOAD_KN_CLIENT_INFO_PREFIX);
                client_info_key += CR_Class_NS::u642str(target_client_id);
                fret = this->_sort_cluster_p->KVGetOne(0, client_info_key, client_info_value, 10);
                if (!fret) {
                    std::vector<uint64_t> client_info = CR_Class_NS::str2u64arr(client_info_value);
                    job_total_lines += client_info[0];
                    job_crcsum_out += client_info[1];
                    target_client_id++;
                } else if ((fret == ENOENT) || (fret == ETIMEDOUT)) {
                    if (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) >= clients_wait_exptime) {
                        DPRINTF("[%s]Client %llu info not ready and timedout\n", this->_job_id.c_str(),
                          (long long unsigned)target_client_id);
                        ret = ETIMEDOUT;
                        break;
                    } else {
                        DPRINTF("[%s]Client %llu info not ready, continue wait ...\n", this->_job_id.c_str(),
                          (long long unsigned)target_client_id);
                    }
                } else {
                    DPRINTF("[%s]Client %llu info get failed[%s]\n", this->_job_id.c_str(),
                      (long long unsigned)target_client_id, CR_Class_NS::strerrno(fret));
                    ret = fret;
                    break;
                }
            }

            if (ret)
                break;

            DPRINTF("[%s]Total %llu lines report by all clients, CRCSUM[0x%016llX]\n",
              this->_job_id.c_str(), (long long unsigned)job_total_lines, (long long)job_crcsum_out);

            query_cmd_str = CR_Class_NS::str_setparam(query_cmd_str,
              DMIBSORTLOAD_PN_QUERY_CMD, DMIBSORTLOAD_PV_DATA_END);

            ret = this->_sort_cluster_p->Query(query_cmd_str, NULL, NULL, &cluster_error);
            if (ret) {
                DPRINTF("[%s]sort_cluster->Query(LIBSORT_PV_DATA_END) failed, msg => [%s]\n",
                  this->_job_id.c_str(), CFMCluster_NS::ExplainErrors(cluster_error).c_str());
                break;
            }

            DPRINTF("[%s]Send DATA_END to sort cluster OK\n", this->_job_id.c_str());

            query_cmd_str.clear();

            query_cmd_str = CR_Class_NS::str_setparam(query_cmd_str,
              DMIBSORTLOAD_PN_QUERY_CMD, DMIBSORTLOAD_PV_QUERY_STAT);

            ret = this->_sort_cluster_p->Query(query_cmd_str, _set_line_count_arr,
              &line_count_arr, &cluster_error);
            if (ret) {
                DPRINTF("[%s]sort_cluster->Query(DMIBSORTLOAD_PV_QUERY_STAT) failed, msg => [%s]\n",
                  this->_job_id.c_str(), CFMCluster_NS::ExplainErrors(cluster_error).c_str());
                break;
            }

            uint64_t sort_total_lines = 0;

            for (size_t i=0; i<line_count_arr.size(); i++) {
                sort_total_lines += line_count_arr[i];
            }

            DPRINTF("[%s]Sort cluster receive %llu lines\n", this->_job_id.c_str(),
              (long long unsigned)sort_total_lines);

            if (sort_total_lines != job_total_lines) {
                DPRINTF("[%s]Total lines between sort cluster and clients not equal!\n",
                  this->_job_id.c_str());
                ret = ERANGE;
                break;
            }

            query_cmd_str.clear();

            query_cmd_str = CR_Class_NS::str_setparam(query_cmd_str,
              DMIBSORTLOAD_PN_QUERY_CMD, LIBIBLOAD_PV_WORK_START);
            query_cmd_str = CR_Class_NS::str_setparam(query_cmd_str,
              LIBIBLOAD_PN_TABLE_INFO, table_info_str);
            query_cmd_str = CR_Class_NS::str_setparam(query_cmd_str,
              LIBIBLOAD_PN_SAVE_PATHNAME, save_pathname);
            query_cmd_str = CR_Class_NS::str_setparam_u64(query_cmd_str,
              DMIBSORTLOAD_PN_BLOCK_LINES, block_lines);
            query_cmd_str = CR_Class_NS::str_setparam_i64(query_cmd_str,
              DMIBSORTLOAD_PN_LINEID_BEGIN, lineid_begin);
            query_cmd_str = CR_Class_NS::str_setparam_u64arr(query_cmd_str,
              DMIBSORTLOAD_PN_LINE_COUNT_ARRAY, line_count_arr);
            query_cmd_str = CR_Class_NS::str_setparam_u64arr(query_cmd_str,
              LIBIBLOAD_PN_PACKINDEX_INFO, packindex_arr);
            query_cmd_str = CR_Class_NS::str_setparam_bool(query_cmd_str,
              LIBIBLOAD_PN_PACKINDEX_MULTIMODE, pki_multimode);
            query_cmd_str = CR_Class_NS::str_setparam_u64(query_cmd_str,
              LIBIBLOAD_PN_FILETHREAD_COUNT, filethread_count);

            ret = this->_load_cluster_p->Query(query_cmd_str, NULL, NULL, &cluster_error);
            if (ret) {
                DPRINTF("[%s]load_cluster->Query(LIBIBLOAD_PV_WORK_START) failed, msg => [%s]\n",
                  this->_job_id.c_str(), CFMCluster_NS::ExplainErrors(cluster_error).c_str());
                break;
            }

            DPRINTF("[%s]Send WORK_START to load cluster OK\n", this->_job_id.c_str());

            query_cmd_str.clear();

            query_cmd_str = CR_Class_NS::str_setparam(query_cmd_str,
              DMIBSORTLOAD_PN_QUERY_CMD, LIBSORT_PV_SET_TARGET);
            query_cmd_str = CR_Class_NS::str_setparam(query_cmd_str,
              LIBSORT_PN_TARGET_JOB_ID, this->_load_job_id);
            query_cmd_str = CR_Class_NS::str_setparam_u64(query_cmd_str,
              DMIBSORTLOAD_PN_BLOCK_LINES, block_lines);
            query_cmd_str = CR_Class_NS::str_setparam_i64(query_cmd_str,
              DMIBSORTLOAD_PN_LINEID_BEGIN, lineid_begin);
            query_cmd_str = CR_Class_NS::str_setparam_u64arr(query_cmd_str,
              DMIBSORTLOAD_PN_LINE_COUNT_ARRAY, line_count_arr);

            ret = this->_sort_cluster_p->Query(query_cmd_str, NULL, NULL, &cluster_error);
            if (ret) {
                DPRINTF("[%s]sort_cluster->Query(LIBSORT_PV_SET_TARGET) failed, msg => [%s]\n",
                  this->_job_id.c_str(), CFMCluster_NS::ExplainErrors(cluster_error).c_str());
                break;
            }

            DPRINTF("[%s]Send SET_TARGET to sort cluster OK\n", this->_job_id.c_str());

            std::string job_done_key;
            std::string job_done_value;

            std::vector<uint64_t> result_arr_sort;
            std::vector<uint64_t> result_arr_load;

            result_arr_sort.push_back(0);
            result_arr_sort.push_back(0);

            result_arr_load.push_back(0);
            result_arr_load.push_back(0);

            job_done_key = std::string(DMIBSORTLOAD_KN_JOB_DONE_PREFIX);
            job_done_key += this->_sort_job_id;

            DPRINTF("[%s]Waiting sort cluster job done ...\n", this->_job_id.c_str());

            ret = this->_sort_cluster_p->KVGet(job_done_key, this->_wait_timeoutsec, _merge_result,
              &result_arr_sort, &cluster_error);
            if (ret) {
                DPRINTF("[%s]sort_cluster->Query(JOB_DONE) failed, msg => [%s]\n",
                  this->_job_id.c_str(), CFMCluster_NS::ExplainErrors(cluster_error).c_str());
                break;
            }

            DPRINTF("[%s]Sort cluster finish %llu lines, CRCSUM[0x%016llX]\n",
              this->_job_id.c_str(), (long long unsigned)result_arr_sort[0], (long long)result_arr_sort[1]);

            if (job_total_lines != result_arr_sort[0]) {
                DPRINTF("[%s]Total lines between sort cluster and clients not equal!\n",
                  this->_job_id.c_str());
                ret = ERANGE;
                break;
            }

            if (job_crcsum_out != result_arr_sort[1]) {
                DPRINTF("[%s]CRCSUM between sort cluster and clients not equal!\n", this->_job_id.c_str());
                ret = ERANGE;
                break;
            }

            job_done_key = std::string(DMIBSORTLOAD_KN_JOB_DONE_PREFIX);
            job_done_key += this->_load_job_id;

            DPRINTF("[%s]Waiting load cluster job done ...\n", this->_job_id.c_str());

            ret = this->_load_cluster_p->KVGet(job_done_key, this->_wait_timeoutsec, _merge_result,
              &result_arr_load, &cluster_error);
            if (ret) {
                DPRINTF("[%s]load_cluster->Query(JOB_DONE) failed, msg => [%s]\n",
                  this->_job_id.c_str(), CFMCluster_NS::ExplainErrors(cluster_error).c_str());
                break;
            }

            DPRINTF("[%s]Load cluster finish %llu lines, CRCSUM[0x%016llX]\n",
              this->_job_id.c_str(), (long long unsigned)result_arr_load[0], (long long)result_arr_load[1]);

            if (job_total_lines != result_arr_load[0]) {
                DPRINTF("[%s]Total lines between load cluster and clients not equal!\n",
                  this->_job_id.c_str());
                ret = ERANGE;
                break;
            }

            if (job_crcsum_out != result_arr_load[1]) {
                DPRINTF("[%s]CRCSUM between load cluster and clients not equal!\n", this->_job_id.c_str());
                ret = ERANGE;
                break;
            }

            if (result_msg_p) {
                *result_msg_p = CR_Class_NS::str_setparam_u64arr(*result_msg_p,
                  DMIBSORTLOAD_PN_LIBLOAD_RESULT_ARRAY, result_arr_load);
            }

            this->_sort_cluster_p->Stop(CR_Class_NS::blank_string);
            this->_load_cluster_p->Stop(CR_Class_NS::blank_string);
        } while (0);

        this->_lck.unlock();
    }

    DPRINTF("[%s]%s\n", this->_job_id.c_str(), CR_Class_NS::strerrno(ret));

    if (ret) {
        if (err_msg) {
            *err_msg = CFMCluster_NS::ExplainErrors(cluster_error);
        }
        errno = ret;
        return -1;
    } else {
        return this->_cluster_info.size();
    }
}

/////////////////////////////////

static int
_make_splitter(void *merge_arg_p, const CFM_Basic_NS::cluster_info_t &cluster_info,
    const std::string &data_req, const std::vector<std::string> &results)
{
    CFMSplitter **_splitter_pp = (CFMSplitter **)merge_arg_p;
    CFMSplitter *splitter_p = new CFMSplitter;
    int fret = splitter_p->Merge(results);

    if (fret) {
        delete splitter_p;
        return fret;
    }

    *_splitter_pp = splitter_p;

    return 0;
}

static int
_set_line_count_arr(void *merge_arg_p, const CFM_Basic_NS::cluster_info_t &cluster_info,
    const std::string &data_req, const std::vector<std::string> &results)
{
    std::vector<uint64_t> *line_count_arr_p = (std::vector<uint64_t>*)merge_arg_p;

    for (size_t i=0; i<cluster_info.size(); i++) {
        line_count_arr_p->push_back(CR_Class_NS::str_getparam_u64(results[i], LIBSORT_STAT_LINES_IN));
    }

    return 0;
}

static int
_merge_result(void *merge_arg_p, const CFM_Basic_NS::cluster_info_t &cluster_info,
    const std::string &data_req, const std::vector<std::string> &results)
{
    std::vector<uint64_t> *result_arr_p = (std::vector<uint64_t> *)merge_arg_p;
    std::vector<uint64_t> result_tmp;

    for (size_t i=0; i<results.size(); i++) {
        result_tmp = CR_Class_NS::str2u64arr(results[i]);
        if (result_arr_p->size() < result_tmp.size())
            result_arr_p->resize(result_tmp.size(), 0);
        for (size_t j=0; j<result_tmp.size(); j++)
            (*result_arr_p)[j] += result_tmp[j];
    }

    return 0;
}

static int
on_splitter(void *splitter_arg_p, const CFM_Basic_NS::cluster_info_t &cluster_info,
    const int64_t row_id_in, const int64_t obj_id, const void *key_p, const uint64_t key_size,
    const void *value_p, const uint64_t value_size, int64_t &row_id_out, int64_t &ret_flags)
{
    CFMSplitter *splitter_p = (CFMSplitter *)splitter_arg_p;

    if (splitter_p) {
        std::string key((const char*)key_p, key_size);
        return splitter_p->KeySplit(key);
    } else {
        return row_id_in % cluster_info.size();
    }
}

static int
before_buf_refresh(void *private_arg_p,
    const CFM_Basic_NS::cluster_info_t &cluster_info, const size_t target_striping_id,
    int64_t &row_id_left, int64_t &row_id_right, CR_FixedLinearStorage &fls_req,
    uintptr_t &pack_arg, int64_t &ret_flags)
{
    return 0;
}

static int
after_buf_refreshed(void *refreshed_arg_p,
    const CFM_Basic_NS::cluster_info_t &cluster_info, const size_t target_striping_id,
    int64_t &row_id_left, int64_t &row_id_right, CR_FixedLinearStorage &fls_req,
    const int jobdata_ret, const std::string &data_resp, const std::string &errmsg,
    uintptr_t &pack_arg, int64_t &ret_flags)
{
    const std::string *job_id_p = (const std::string*)refreshed_arg_p;

    if (jobdata_ret) {
        if ((jobdata_ret != EALREADY) && (pack_arg < 3)) {
            pack_arg++;
            ret_flags |= CFMCLUSTER_FLAG_REDO_IT;
            DPRINTF("[%s]Send data block [%lld ~ %lld] to striping[%u], %s[%s], retry %llu\n",
              job_id_p->c_str(), (long long)row_id_left, (long long)row_id_right,
              (unsigned)target_striping_id, CR_Class_NS::strerrno(jobdata_ret), errmsg.c_str(),
              (long long unsigned)pack_arg);
        } else {
            DPRINTF("[%s]Send data block [%lld ~ %lld] to striping[%u], %s[%s], FIN\n",
              job_id_p->c_str(), (long long)row_id_left, (long long)row_id_right,
              (unsigned)target_striping_id, CR_Class_NS::strerrno(jobdata_ret), errmsg.c_str());
        }
    } else {
        DPRINTFX(20, "[%s]Send data block [%lld ~ %lld] to striping[%u], SUCCESS\n",
          job_id_p->c_str(), (long long)row_id_left, (long long)row_id_right,
          (unsigned)target_striping_id);
    }

    return jobdata_ret;
}

/////////////////////////////////

int
DMIBSortLoad_NS::EmergencyStop(const std::string &job_id)
{
    char *cluster_str_p = getenv(DMIBSORTLOAD_EN_CLUSTER_STR);
    if (!cluster_str_p) {
        DPRINTF("[%s]Environment variable \"%s\" not set\n", job_id.c_str(), DMIBSORTLOAD_EN_CLUSTER_STR);
        return EINVAL;
    }

    int ret = 0;
    int fret;
    std::string cluster_str = cluster_str_p;
    std::string jobid_sort = job_id + std::string("S");
    std::string jobid_load = job_id + std::string("L");
    CFMCluster_NS::NodeErrorsT cluster_error;

    fret = CFMCluster_NS::RemoteCancel(cluster_str, jobid_sort, &cluster_error);
    if (fret) {
        DPRINTF("[%s]RemoteCancel() failed, msg => [%s]\n",
          jobid_sort.c_str(), CFMCluster_NS::ExplainErrors(cluster_error).c_str());
        ret = fret;
    }

    fret = CFMCluster_NS::RemoteCancel(cluster_str, jobid_load, &cluster_error);
    if (fret) {
        DPRINTF("[%s]RemoteCancel() failed, msg => [%s]\n",
          jobid_sort.c_str(), CFMCluster_NS::ExplainErrors(cluster_error).c_str());
        ret = fret;
    }

    return ret;
}
