#include <string.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_addon.h>
#include <cr_class/cr_quickhash.h>
#include <cr_class/cr_timer.h>
#include <cr_class/cr_externalsort.h>
#include <cr_class/cr_blockqueue.h>
#include <cr_class/cr_unlimitedfifo.h>
#include <cr_class/cr_class.pb.h>
#include <cfm_drv/cfm_cluster.h>
#include <cfm_drv/dm_ib_sort_load.h>

#include <vector>
#include <list>
#include <mutex>
#include <thread>

#include "libcfm_sort.h"

#define LIBSORT_KN_DATASUMMARY_FILENAME			"datasummary.info"

#define LIBSORT_INPUT_FIFO_FILENAME_PREFIX		"LIBSORT-IN-"
#define LIBSORT_OUTPUT_FIFO_FILENAME_PREFIX		"LIBSORT-OUT-"
#define LIBSORT_FIFO_MIN_FILESIZE			(UINTMAX_C(0x100000) * 128)
#define LIBSORT_FIFO_MAX_FILESIZE			(UINTMAX_C(0x100000) * 1024)

#define LIBSORT_SORTER_MIN_FILESIZE			(UINTMAX_C(0x100000) * 128)
#define LIBSORT_SORTER_MAX_FILESIZE			(UINTMAX_C(0x100000) * 4096)
#define LIBSORT_SORTER_MIN_SLICESIZE			(UINTMAX_C(0x100000) * 1)
#define LIBSORT_SORTER_MAX_SLICESIZE			(UINTMAX_C(0x100000) * 32)
#define LIBSORT_SORTER_FILENAME_PREFIX			"LIBSORT-DATA-"

////////////////////////////////////

typedef struct {
    uint64_t lines_in;
    uint64_t lines_out;
    uint64_t lines_crcsum_in;
    uint64_t lines_crcsum_out;
    msgCRPairList key_samples;
} libsort_stat_t;

typedef struct {
    CR_DataControl::CondMutex _cmtx;
    libsort_stat_t stat;
    std::string job_id;
    std::string sorter_args;
    CR_ExternalSort sorter;
    CR_UnlimitedFifo in_fifo_disk;
    CR_BlockQueue<std::string> in_fifo_mem;
    bool in_fifo_use_disk;
    size_t slice_size;
    bool sort_end;
    std::thread *from_infifo_to_sorter_tp;
    volatile bool from_infifo_to_sorter_stop;
    std::thread *from_sorter_to_target_tp;
    volatile bool from_sorter_to_target_start;
    volatile bool from_sorter_to_target_stop;
    volatile bool from_sorter_to_target_done;

    std::vector<std::string> work_path_arr;

    std::vector<std::string> merged_job_path_arr;

    CFM_Basic_NS::cluster_info_t cluster_info;
    std::pair<int,int> current_node;
    size_t current_node_id;

    size_t block_lines;

    CFMCluster::on_splitter_call_func_t on_splitter_call_func;

    std::vector<uint64_t> line_target_array;
    size_t current_target_striping_id;

#ifdef DEBUG
    std::vector<uint64_t> target_line_count;
#endif

    CFMCluster * cfmc_next_p;
} sort_private_data_t;

////////////////////////////////////

static void *lib_on_start(const CFM_Basic_NS::cluster_info_t &cluster_info,
    const std::pair<int,int> &current_node, const std::string &job_id,
    const std::string &extra_param, const std::vector<std::string> &work_path_arr,
    const bool static_work_path);
static int lib_on_stop(void *proc_data_p, const std::string &data_req);
static int lib_on_query(void *proc_data_p, const std::string &data_req, std::string &data_resp);
static int lib_on_data(void *proc_data_p, const int64_t rowid_left, const int64_t rowid_right,
    const std::string &data_req, std::string &data_resp);

static void _from_infifo_to_sorter_func(sort_private_data_t *private_data_p);
static void _from_sorter_to_loader_func(sort_private_data_t *private_data_p);

static int _on_splitter_call_noalign(void *arg_p, const CFM_Basic_NS::cluster_info_t &cluster_info,
    const int64_t row_id_in, const int64_t obj_id, const void *key_p, const uint64_t key_size,
    const void *value_p, const uint64_t value_size, int64_t &row_id_out, int64_t &splitter_func_ret2);

static int _on_splitter_call_align(void *arg_p, const CFM_Basic_NS::cluster_info_t &cluster_info,
    const int64_t row_id_in, const int64_t obj_id, const void *key_p, const uint64_t key_size,
    const void *value_p, const uint64_t value_size, int64_t &row_id_out, int64_t &splitter_func_ret2);

static int _after_buf_refreshed_func(void *refreshed_arg_p,
    const CFM_Basic_NS::cluster_info_t &cluster_info, const size_t target_striping_id,
    int64_t &row_id_left, int64_t &row_id_right, CR_FixedLinearStorage &fls_req,
    const int jobdata_ret, const std::string &data_resp, const std::string &errmsg,
    uintptr_t &pack_arg, int64_t &ret_flags);

static void _do_init(sort_private_data_t *private_data_p);
static void _do_stop(sort_private_data_t *private_data_p);

static int _lib_do_query_stat_nolock(sort_private_data_t *private_data_p,
    const std::string &data_req, std::string &data_resp);

static int _lib_do_query_stat(sort_private_data_t *private_data_p,
    const std::string &data_req, std::string &data_resp);
static int _lib_do_set_target(sort_private_data_t *private_data_p,
    const std::string &data_req, std::string &data_resp);
static int _lib_do_data_end(sort_private_data_t *private_data_p,
    const std::string &data_req, std::string &data_resp);
static int _lib_do_wait_send_done(sort_private_data_t *private_data_p,
    const std::string &data_req, std::string &data_resp);
static int _lib_do_append_part_files(sort_private_data_t *private_data_p,
    const std::string &data_req, std::string &data_resp);
static int _lib_do_query_key_sample(sort_private_data_t *private_data_p,
    const std::string &data_req, std::string &data_resp);

////////////////////////////////////

static cfm_lib_kvput_t kvput;

static cfm_lib_kvget_t kvget;

////////////////////////////////////

static void
_from_infifo_to_sorter_func(sort_private_data_t *private_data_p)
{
    int fret;
    CR_FixedLinearStorage slice_fls;
    std::string slice_fls_str;
    int64_t slice_fls_id;
    size_t slice_fls_total_lines;
    std::string _value;
    bool _null_value;

    while (1) {
        if (private_data_p->from_infifo_to_sorter_stop)
            break;

        if (private_data_p->in_fifo_use_disk) {
            fret = private_data_p->in_fifo_disk.PopFront(slice_fls_id, slice_fls_str, _value, _null_value, 0.5);
        } else {
            fret = private_data_p->in_fifo_mem.pop_front(slice_fls_str, 0.5);
        }
        if (fret == EAGAIN) {
            continue;
        } else if (fret == ETIMEDOUT) {
            continue;
        } else if (fret) {
            DPRINTF("in_fifo.PopFront, %s\n", CR_Class_NS::strerrno(fret));
            break;
        }

        fret = slice_fls.MapFromArray(slice_fls_str.data(), slice_fls_str.size());
        if (fret) {
            DPRINTF("slice_fls.MapFromArray, %s\n", CR_Class_NS::strerrno(fret));
            break;
        }

        slice_fls_total_lines = slice_fls.GetTotalLines();
        if (slice_fls_total_lines > 0) {
            fret = private_data_p->sorter.Push(slice_fls);
            if (fret) {
                DPRINTF("sorter.Push, %s\n", CR_Class_NS::strerrno(fret));
                break;
            }
        } else {
            break;
        }
    }

    private_data_p->sorter.PushOne(0, NULL, 0, NULL, 0);
}

static void
_from_sorter_to_loader_func(sort_private_data_t *private_data_p)
{
    CR_FixedLinearStorage slice_fls;
    int kv_fret;
    int sorter_fret;
    int cluster_fret;
    bool do_stop = false;
    bool no_error = true;
    std::string sample_key;
    std::vector<uint64_t> result_arr;

    slice_fls.ReSize(private_data_p->slice_size);

    while (!private_data_p->from_sorter_to_target_start) {
        if (private_data_p->from_sorter_to_target_stop) {
            do_stop = true;
            break;
        } else {
            usleep(10000);
            continue;
        }
    }

    while (!do_stop) {
        if (private_data_p->from_sorter_to_target_stop) {
            do_stop = true;
            break;
        }

        slice_fls.Clear();
        sorter_fret = private_data_p->sorter.Pop(slice_fls);
        if (sorter_fret == 0) {
            cluster_fret = private_data_p->cfmc_next_p->Data(&slice_fls,
              private_data_p->on_splitter_call_func, private_data_p,
              NULL, NULL, _after_buf_refreshed_func, private_data_p);
            if (cluster_fret) {
                DPRINTF("next_p->Data(...), %s\n", CR_Class_NS::strerrno(cluster_fret));
                no_error = false;
                break;
            }
        } else if (sorter_fret == EINPROGRESS) {
            usleep(10000);
            continue;
        } else if (sorter_fret == EAGAIN) {
            private_data_p->_cmtx.lock();
            private_data_p->from_sorter_to_target_done = true;
            private_data_p->_cmtx.broadcast();
            private_data_p->_cmtx.unlock();
            break;
        } else {
            DPRINTF("sorter.Pop() == %s\n", CR_Class_NS::strerrno(sorter_fret));
            no_error = false;
            break;
        }
    }

    if (no_error && private_data_p->cfmc_next_p) {
        private_data_p->cfmc_next_p->Data(NULL, NULL, NULL, NULL, NULL,
          _after_buf_refreshed_func, private_data_p);
    }

    DPRINTF("Prepare key samples and job done info\n");

    std::string samples_str;
    std::string samples_str_c;
    int samples_count;

    private_data_p->_cmtx.lock();
    while (no_error && (private_data_p->stat.lines_out < private_data_p->stat.lines_in)) {
        private_data_p->_cmtx.wait(1);
    }
    samples_count = private_data_p->stat.key_samples.pair_list_size();
    private_data_p->stat.key_samples.SerializeToString(&samples_str);
    result_arr.push_back(private_data_p->stat.lines_out);
    result_arr.push_back(private_data_p->stat.lines_crcsum_out);
    result_arr.push_back(private_data_p->stat.lines_in);
    result_arr.push_back(private_data_p->stat.lines_crcsum_in);
    private_data_p->_cmtx.unlock();

    CR_Class_NS::compressx(samples_str, samples_str_c, 1);

    std::string key_samples_savepath;

    key_samples_savepath = CR_Class_NS::dirname(private_data_p->work_path_arr[0].c_str());
    key_samples_savepath += "/";
    key_samples_savepath += CFM_Basic_NS::make_node_name_prefix(private_data_p->current_node);
    key_samples_savepath += private_data_p->job_id;
    key_samples_savepath += LIBSORT_KN_KEY_SAMPLES_SUFFIX;

    kv_fret = CR_Class_NS::save_string(key_samples_savepath.c_str(), samples_str_c);

    DPRINTF("%d lines key-sample save to \"%s\", %s\n", samples_count,
      key_samples_savepath.c_str(), CR_Class_NS::strerrno(kv_fret));

    std::string job_done_key, job_done_value;
    job_done_key = std::string(DMIBSORTLOAD_KN_JOB_DONE_PREFIX);
    job_done_key += private_data_p->job_id;
    job_done_value = CR_Class_NS::u64arr2str(result_arr);

    kv_fret = kvput(job_done_key, &job_done_value, false);

#ifdef DEBUG
    if (_on_splitter_call_align == private_data_p->on_splitter_call_func) {
        std::string str_out;
        for (size_t i=0; i< private_data_p->target_line_count.size(); i++) {
            str_out += CR_Class_NS::str_printf("%lu, ", (long unsigned)private_data_p->target_line_count[i]);
        }
        DPRINTF("TARGET_LINE_COUNT = {%s}\n", str_out.c_str());
    }
#endif

    DPRINTF("%llu lines in, CRCSUM-IN[0x%016llX], %llu lines out, CRCSUM-OUT[0x%016llX]\n",
      (long long unsigned)result_arr[2], (long long)result_arr[3],
      (long long unsigned)result_arr[0], (long long)result_arr[1]);

    DPRINTF("Save job done info, %s\n", CR_Class_NS::strerrno(kv_fret));
}

////////////////////////////////////

int
cfm_lib_init(CFMLibInfo *lib_infp_p)
{
    if (!lib_infp_p)
        return EFAULT;

    lib_infp_p->on_start = lib_on_start;
    lib_infp_p->on_stop = lib_on_stop;
    lib_infp_p->on_query = lib_on_query;
    lib_infp_p->on_data = lib_on_data;

    kvput = lib_infp_p->kvput;
    kvget = lib_infp_p->kvget;

    return 0;
}

////////////////////////////////////

static void *
lib_on_start(const CFM_Basic_NS::cluster_info_t &cluster_info,
    const std::pair<int,int> &current_node, const std::string &job_id,
    const std::string &extra_param, const std::vector<std::string> &work_path_arr,
    const bool static_work_path)
{
    std::vector<std::string> sort_filename_prefix_arr;
    std::string sort_filename_prefix;

    CR_Class_NS::dprintfx_line_prefix = std::string("[") + job_id + std::string("]");

    for (size_t i=0; i<work_path_arr.size(); i++) {
        sort_filename_prefix = work_path_arr[i];
        sort_filename_prefix += "/";
        sort_filename_prefix += LIBSORT_SORTER_FILENAME_PREFIX;
        sort_filename_prefix_arr.push_back(sort_filename_prefix);
    }

    sort_private_data_t *private_data_p = new sort_private_data_t;

    private_data_p->cluster_info = cluster_info;
    private_data_p->current_node = current_node;
    private_data_p->current_node_id = current_node.first;
    private_data_p->job_id = job_id;
    private_data_p->cfmc_next_p = NULL;

    std::string riak_connect_str = CR_Class_NS::str_getparam(extra_param, DMIBSORTLOAD_PN_RIAK_CONNECT_STR);
    std::string riak_bucket = CR_Class_NS::str_getparam(extra_param, DMIBSORTLOAD_PN_RIAK_BUCKET);

    if ((riak_connect_str.size() != 0) && (riak_bucket.size() != 0)) {
        DPRINTF("Line send mode: align\n");
        private_data_p->on_splitter_call_func = _on_splitter_call_align;
    } else {
        DPRINTF("Line send mode: not align\n");
        private_data_p->on_splitter_call_func = _on_splitter_call_noalign;
    }

    std::string esort_args = extra_param;

    size_t rough_file_size = CR_Class_NS::str_getparam_u64(
      esort_args, EXTERNALSORT_ARGNAME_ROUGHFILESIZE);
    if (rough_file_size < LIBSORT_SORTER_MIN_FILESIZE)
        rough_file_size = LIBSORT_SORTER_MIN_FILESIZE;
    if (rough_file_size > LIBSORT_SORTER_MAX_FILESIZE)
        rough_file_size = LIBSORT_SORTER_MAX_FILESIZE;
    esort_args = CR_Class_NS::str_setparam_u64(esort_args,
      EXTERNALSORT_ARGNAME_ROUGHFILESIZE, rough_file_size);

    size_t rough_slice_size = CR_Class_NS::str_getparam_u64(
      esort_args, EXTERNALSORT_ARGNAME_ROUGHSLICESIZE);
    if (rough_slice_size < LIBSORT_SORTER_MIN_SLICESIZE)
        rough_slice_size = LIBSORT_SORTER_MIN_SLICESIZE;
    if (rough_slice_size > LIBSORT_SORTER_MAX_SLICESIZE)
        rough_slice_size = LIBSORT_SORTER_MAX_SLICESIZE;
    esort_args = CR_Class_NS::str_setparam_u64(esort_args,
      EXTERNALSORT_ARGNAME_ROUGHSLICESIZE, rough_slice_size);

    size_t max_slice_size = CR_Class_NS::str_getparam_u64(
      esort_args, EXTERNALSORT_ARGNAME_MAXSLICESIZE);
    if (max_slice_size > LIBSORT_SORTER_MAX_SLICESIZE)
        max_slice_size = LIBSORT_SORTER_MAX_SLICESIZE;
    esort_args = CR_Class_NS::str_setparam_u64(esort_args,
      EXTERNALSORT_ARGNAME_MAXSLICESIZE, max_slice_size);

    private_data_p->sorter.SetArgs(esort_args, sort_filename_prefix_arr, static_work_path);

    size_t fifo_slice_size = CR_Class_NS::str_getparam_u64(esort_args, DMIBSORTLOAD_PN_FIFO_SIZE);
    if (fifo_slice_size < LIBSORT_FIFO_MIN_FILESIZE)
        fifo_slice_size = LIBSORT_FIFO_MIN_FILESIZE;
    if (fifo_slice_size > LIBSORT_FIFO_MAX_FILESIZE)
        fifo_slice_size = LIBSORT_FIFO_MAX_FILESIZE;

    // input fifo always use first path
    std::string in_fifo_filename_prefix = work_path_arr[0];
    in_fifo_filename_prefix += "/";
    in_fifo_filename_prefix += LIBSORT_INPUT_FIFO_FILENAME_PREFIX;
    private_data_p->in_fifo_disk.SetArgs(in_fifo_filename_prefix, fifo_slice_size);

    int memfifo_size = 0;
    char *memfifo_size_str = getenv(DMIBSORTLOAD_EN_LIB_MEMFIFO_SIZE);
    if (memfifo_size_str)
        memfifo_size = atoi(memfifo_size_str);
    if (memfifo_size < 0)
        memfifo_size = 0;

    if (memfifo_size > 0) {
        private_data_p->in_fifo_use_disk = false;
        private_data_p->in_fifo_mem.set_args(memfifo_size);
        DPRINTF("FIFO use memory(%d)\n", memfifo_size);
    } else {
        private_data_p->in_fifo_use_disk = true;
        DPRINTF("FIFO use disk\n");
    }

    private_data_p->slice_size = rough_slice_size;
    private_data_p->work_path_arr = work_path_arr;

    _do_init(private_data_p);

    return private_data_p;
}

static void
_do_init(sort_private_data_t *private_data_p)
{
    private_data_p->sort_end = false;

    private_data_p->stat.lines_in = 0;
    private_data_p->stat.lines_out = 0;
    private_data_p->stat.lines_crcsum_in = 0;
    private_data_p->stat.lines_crcsum_out = 0;
    private_data_p->stat.key_samples.Clear();

    private_data_p->from_infifo_to_sorter_tp = NULL;
    private_data_p->from_sorter_to_target_tp = NULL;

    private_data_p->from_infifo_to_sorter_stop = false;
    private_data_p->from_sorter_to_target_stop = false;
    private_data_p->from_sorter_to_target_start = false;
    private_data_p->from_sorter_to_target_done = false;

    private_data_p->from_infifo_to_sorter_tp = new std::thread(_from_infifo_to_sorter_func, private_data_p);
    private_data_p->from_sorter_to_target_tp = new std::thread(_from_sorter_to_loader_func, private_data_p);
}

static void
_do_stop(sort_private_data_t *private_data_p)
{
    std::thread *from_infifo_to_sorter_tp;
    std::thread *from_sorter_to_target_tp;
    std::string _cmd_line;

    private_data_p->_cmtx.lock();

    private_data_p->from_infifo_to_sorter_stop = true;
    private_data_p->from_sorter_to_target_stop = true;

    from_infifo_to_sorter_tp = private_data_p->from_infifo_to_sorter_tp;
    from_sorter_to_target_tp = private_data_p->from_sorter_to_target_tp;

    private_data_p->from_infifo_to_sorter_tp = NULL;
    private_data_p->from_sorter_to_target_tp = NULL;

    if (private_data_p->merged_job_path_arr.size() == 0) {
        int fret;
        CR_FixedLinearStorage_NS::fls_infos_t sorter_info;
        std::string sorter_info_str;
        std::string sorter_info_str_c;
        std::string sorter_info_savepath;

        private_data_p->sorter.GetInfos(sorter_info);
        sorter_info_str.assign((const char*)(&sorter_info), sizeof(sorter_info));
        CR_Class_NS::compressx(sorter_info_str, sorter_info_str_c, 1);

        sorter_info_savepath = private_data_p->work_path_arr[0];
        sorter_info_savepath += "/";
        sorter_info_savepath += LIBSORT_KN_DATASUMMARY_FILENAME;

        fret = CR_Class_NS::save_string(sorter_info_savepath.c_str(), sorter_info_str_c);
        DPRINTF("Save data summary to \"%s\", %s\n",
          sorter_info_savepath.c_str(), CR_Class_NS::strerrno(fret));
    }

    for (size_t path_id=0; path_id< private_data_p->merged_job_path_arr.size(); path_id++) {
        _cmd_line = "rm -rf '";
        _cmd_line += private_data_p->merged_job_path_arr[path_id];
        _cmd_line += "'";
        CR_Class_NS::system(_cmd_line.c_str());
    }

    private_data_p->_cmtx.broadcast();

    private_data_p->_cmtx.unlock();

    if (from_infifo_to_sorter_tp) {
        from_infifo_to_sorter_tp->join();
        delete from_infifo_to_sorter_tp;
    }

    if (from_sorter_to_target_tp) {
        from_sorter_to_target_tp->join();
        delete from_sorter_to_target_tp;
    }
}

static int
lib_on_stop(void *proc_data_p, const std::string &data_req)
{
    if (!proc_data_p)
        return EFAULT;

    sort_private_data_t *private_data_p = (sort_private_data_t *)proc_data_p;

    _do_stop(private_data_p);
    delete private_data_p;

    return 0;
}

static int
lib_on_query(void *proc_data_p, const std::string &data_req, std::string &data_resp)
{
    if (!proc_data_p)
        return EFAULT;

    int ret = 0;
    std::string query_cmd_str = CR_Class_NS::str_getparam(data_req, DMIBSORTLOAD_PN_QUERY_CMD);

    if (query_cmd_str == DMIBSORTLOAD_PV_QUERY_STAT) {
        ret = _lib_do_query_stat((sort_private_data_t*)proc_data_p, data_req, data_resp);
    } else if (query_cmd_str == LIBSORT_PV_SET_TARGET) {
        ret = _lib_do_set_target((sort_private_data_t*)proc_data_p, data_req, data_resp);
    } else if (query_cmd_str == DMIBSORTLOAD_PV_DATA_END) {
        ret = _lib_do_data_end((sort_private_data_t*)proc_data_p, data_req, data_resp);
    } else if (query_cmd_str == LIBSORT_PV_WAIT_SEND_DONE) {
        ret = _lib_do_wait_send_done((sort_private_data_t*)proc_data_p, data_req, data_resp);
    } else if (query_cmd_str == LIBSORT_PV_APPEND_PART_FILES) {
        ret = _lib_do_append_part_files((sort_private_data_t*)proc_data_p, data_req, data_resp);
    } else if (query_cmd_str == LIBSORT_PV_QUERY_KEY_SAMPLE) {
        ret = _lib_do_query_key_sample((sort_private_data_t*)proc_data_p, data_req, data_resp);
    } else {
        ret = EINVAL;
    }

    return ret;
}

static int
lib_on_data(void *proc_data_p, const int64_t rowid_left, const int64_t rowid_right,
    const std::string &data_req, std::string &data_resp)
{
    int ret = 0;

    if (!proc_data_p)
        return EFAULT;

    CR_FixedLinearStorage_NS::fls_infos_t fls_infos;
    if (!CR_FixedLinearStorage_NS::VerifyData(data_req.data(), data_req.size(), &fls_infos))
        return EBADMSG;

    sort_private_data_t *private_data_p = (sort_private_data_t *)proc_data_p;

    private_data_p->_cmtx.lock();

    if (!private_data_p->sort_end) {
        private_data_p->stat.lines_crcsum_in += fls_infos.objid_cksum + fls_infos.value_cksum;
        if (fls_infos.total_lines > 0) {
            if (private_data_p->in_fifo_use_disk) {
                ret = private_data_p->in_fifo_disk.PushBack(0, data_req, NULL);
            } else {
                ret = private_data_p->in_fifo_mem.push_back(data_req, 86400);
            }
            if (!ret) {
                private_data_p->stat.lines_in += fls_infos.total_lines;
            }
        }
        private_data_p->_cmtx.broadcast();
    } else {
        DPRINTF("Push data after data end\n");
        ret = EALREADY;
    }

    private_data_p->_cmtx.unlock();

    return ret;
}

////////////////////////////////////

static int
_lib_do_query_stat_nolock(sort_private_data_t *private_data_p,
    const std::string &data_req, std::string &data_resp)
{
    int ret = 0;

    std::string proc_stat;

    proc_stat = CR_Class_NS::str_setparam_u64(
      proc_stat, LIBSORT_STAT_LINES_IN, private_data_p->stat.lines_in);

    proc_stat = CR_Class_NS::str_setparam_u64(
      proc_stat, LIBSORT_STAT_LINES_OUT, private_data_p->stat.lines_out);

    proc_stat = CR_Class_NS::str_setparam_u64(
      proc_stat, LIBSORT_STAT_LINES_CRCSUM_IN, private_data_p->stat.lines_crcsum_in);

    proc_stat = CR_Class_NS::str_setparam_u64(
      proc_stat, LIBSORT_STAT_LINES_CRCSUM_OUT, private_data_p->stat.lines_crcsum_out);

    data_resp = proc_stat;

    return ret;
}

static int
_lib_do_query_stat(sort_private_data_t *private_data_p,
    const std::string &data_req, std::string &data_resp)
{
    int ret = 0;

    private_data_p->_cmtx.lock();

    ret = _lib_do_query_stat_nolock(private_data_p, data_req, data_resp);

    private_data_p->_cmtx.unlock();

    return ret;
}

static int
_lib_do_set_target(sort_private_data_t *private_data_p,
    const std::string &data_req, std::string &data_resp)
{
    int ret = 0;

    std::string target_job_id = CR_Class_NS::str_getparam(data_req, LIBSORT_PN_TARGET_JOB_ID);
    if (target_job_id.size() == 0) {
        DPRINTF("LIBSORT_PN_TARGET_JOB_ID not set\n");
        return EINVAL;
    }

    int64_t lineid_begin = CR_Class_NS::str_getparam_i64(data_req, DMIBSORTLOAD_PN_LINEID_BEGIN, -1);
    if (lineid_begin < 0) {
        DPRINTF("DMIBSORTLOAD_PN_LINEID_BEGIN{%lld} not set or wrong\n", (long long)lineid_begin);
        return EINVAL;
    }

    size_t block_lines = CR_Class_NS::str_getparam_u64(data_req, DMIBSORTLOAD_PN_BLOCK_LINES, 0);
    if (block_lines == 0) {
        DPRINTF("DMIBSORTLOAD_PN_BLOCK_LINES not set\n");
        return EINVAL;
    } else if (block_lines > DM_IB_MAX_BLOCKLINES) {
        block_lines = DM_IB_MAX_BLOCKLINES;
    }

    std::vector<uint64_t> line_count_array =
      CR_Class_NS::str_getparam_u64arr(data_req, DMIBSORTLOAD_PN_LINE_COUNT_ARRAY, -1);

    if (line_count_array.size() != private_data_p->cluster_info.size()) {
        DPRINTF("line_count_array.size(),%lld != cluster_info.size(),%lld\n",
          (long long)line_count_array.size(), (long long)private_data_p->cluster_info.size());
        return EINVAL;
    }

    const size_t cur_node_id = private_data_p->current_node_id;

    uint64_t before_node_line_count = 0;

    for (size_t i=0; i<line_count_array.size(); i++) {
        if (i < cur_node_id) {
            before_node_line_count += line_count_array[i];
        }
    }

    int64_t local_lineid_begin = lineid_begin + before_node_line_count;
    std::vector<uint64_t> line_target_array = line_count_array;

    if (_on_splitter_call_align == private_data_p->on_splitter_call_func) {
        line_target_array[0] += lineid_begin;
        CR_Class_NS::align_uintarr(line_target_array.data(), line_target_array.size(), block_lines);
        line_target_array[0] -= lineid_begin;
        CR_Class_NS::select_uintarr(line_target_array.data(), line_target_array.size(),
          before_node_line_count, line_count_array[cur_node_id]);
#ifdef DEBUG
        std::string str_out;
        for (size_t i=0; i<line_target_array.size(); i++) {
            str_out += CR_Class_NS::str_printf("%lu, ", (long unsigned)line_target_array[i]);
        }
        DPRINTF("LINE_TARGET_ARR = {%s}\n", str_out.c_str());
#endif
    }

    private_data_p->_cmtx.lock();

    if (!private_data_p->sort_end) {
        DPRINTF("Set target before data end\n");
        ret = EINPROGRESS;
    } else if (private_data_p->from_sorter_to_target_start) {
        DPRINTF("Double set target\n");
        ret = EALREADY;
    } else {
        if (private_data_p->stat.lines_in != line_count_array[private_data_p->current_node_id]) {
            DPRINTF("Existing lines{%lld} does not match the expected{%lld}\n",
              (long long)private_data_p->stat.lines_in,
              (long long)line_count_array[private_data_p->current_node_id]);
            ret = EINVAL;
        }
        if (!ret) {
            CFMCluster_NS::NodeErrorsT errors;
            CFMCluster *tmp_cfm_p = NULL;
            int fret = 0;
            for (int retry_count=0; retry_count<3; retry_count++) {
                tmp_cfm_p = new CFMCluster(private_data_p->cluster_info,
                  (private_data_p->sorter.GetLongestLineSize() + 32) * block_lines);
                if (tmp_cfm_p) {
                    fret = tmp_cfm_p->Connect(&errors);
                    if (!fret)
                       break;
                    DPRINTF("Connect to next cluster failed(%d):%s\n",
                      retry_count, CFMCluster_NS::ExplainErrors(errors).c_str());
                    delete tmp_cfm_p;
                    tmp_cfm_p = NULL;
                }
                usleep(rand() % 400000 + 100000);
            }
            if (tmp_cfm_p && !fret) {
                private_data_p->cfmc_next_p = tmp_cfm_p;
                private_data_p->block_lines = block_lines;
                if (_on_splitter_call_align == private_data_p->on_splitter_call_func) {
                    private_data_p->line_target_array = line_target_array;
#ifdef DEBUG
                    private_data_p->target_line_count.resize(private_data_p->line_target_array.size());
#endif
                    private_data_p->current_target_striping_id = 0;
                }
                private_data_p->cfmc_next_p->SetRowID(local_lineid_begin);
                private_data_p->cfmc_next_p->SetJobID(target_job_id);
                private_data_p->from_sorter_to_target_done = false;
                private_data_p->from_sorter_to_target_start = true;
                private_data_p->_cmtx.broadcast();
            } else {
                ret = fret;
            }
        }
    }

    private_data_p->_cmtx.unlock();

    return ret;
}

static int
_lib_do_data_end(sort_private_data_t *private_data_p,
    const std::string &data_req, std::string &data_resp)
{
    int ret = 0;
    CR_FixedLinearStorage blank_fls(0);
    std::string blank_fls_data;

    blank_fls.SaveToString(blank_fls_data);

    private_data_p->_cmtx.lock();

    if (!private_data_p->sort_end) {
        private_data_p->sort_end = true;
        if (private_data_p->in_fifo_use_disk) {
            ret = private_data_p->in_fifo_disk.PushBack(0, blank_fls_data, NULL);
        } else {
            ret = private_data_p->in_fifo_mem.push_back(blank_fls_data, 86400);
        }
    } else {
        DPRINTF("Double data end\n");
        ret = EALREADY;
    }

    ret = _lib_do_query_stat_nolock(private_data_p, data_req, data_resp);

    private_data_p->_cmtx.unlock();

    return ret;
}

static int
_lib_do_wait_send_done(sort_private_data_t *private_data_p,
    const std::string &data_req, std::string &data_resp)
{
    int ret = 0;

    private_data_p->_cmtx.lock();

    while (!private_data_p->from_sorter_to_target_done) {
        if (private_data_p->from_sorter_to_target_stop)
            break;
        private_data_p->_cmtx.wait(0.1);
    }

    ret = _lib_do_query_stat_nolock(private_data_p, data_req, data_resp);

    private_data_p->_cmtx.unlock();

    return ret;
}

static int
_lib_do_append_part_files(sort_private_data_t *private_data_p,
    const std::string &data_req, std::string &data_resp)
{
    int ret = 0;

    CR_FixedLinearStorage_NS::fls_infos_t part_info;
    std::vector<std::string> job_id_array =
      CR_Class_NS::str_getparam_strarr(data_req, LIBSORT_PN_TARGET_JOB_ID);
    std::vector<std::string> job_paths;
    std::string path_tmp;
    std::vector<uint64_t> result_arr;
    std::string node_name_prefix = CFM_Basic_NS::make_node_name_prefix(private_data_p->current_node);
    bool data_summary_get_filed = false;

    CR_FixedLinearStorage_NS::ClearInfos(part_info);

    for (size_t job_id_id=0; job_id_id<job_id_array.size(); job_id_id++) {
        if (!data_summary_get_filed) {
            int fret;
            CR_FixedLinearStorage_NS::fls_infos_t sorter_info;
            std::string sorter_info_str;
            std::string sorter_info_str_c;
            std::string sorter_info_savepath;

            sorter_info_savepath = CR_Class_NS::dirname(private_data_p->work_path_arr[0].c_str());
            sorter_info_savepath += "/";
            sorter_info_savepath += CFM_Basic_NS::make_node_name_prefix(private_data_p->current_node);
            sorter_info_savepath += job_id_array[job_id_id];
            sorter_info_savepath += "S";
            sorter_info_savepath += CFM_LIB_WORKPATH_SUFFIX;
            sorter_info_savepath += "/";
            sorter_info_savepath += LIBSORT_KN_DATASUMMARY_FILENAME;
            fret = CR_Class_NS::load_string(sorter_info_savepath.c_str(), sorter_info_str_c);
            if (!fret)
                fret = CR_Class_NS::decompressx(sorter_info_str_c, sorter_info_str);
            if (!fret) {
                if (sorter_info_str.size() == sizeof(sorter_info))
                    memcpy(&sorter_info, sorter_info_str.data(), sizeof(sorter_info));
                else
                    fret = ENOMSG;
            }
            if (!fret) {
                CR_FixedLinearStorage_NS::MergeInfo(part_info, sorter_info);
            }
            if (fret) {
                data_summary_get_filed = true;
            }
            DPRINTF("Read job \"%s\" data summary file \"%s\", %s\n", job_id_array[job_id_id].c_str(),
              sorter_info_savepath.c_str(), CR_Class_NS::strerrno(fret));
        }

        for (size_t work_path_id=0; work_path_id< private_data_p->work_path_arr.size(); work_path_id++) {
            path_tmp = private_data_p->work_path_arr[work_path_id];
            path_tmp = path_tmp.substr(0, path_tmp.find(node_name_prefix));
            path_tmp.append(node_name_prefix);
            path_tmp.append(job_id_array[job_id_id]);
            path_tmp.append("S");
            path_tmp.append(CFM_LIB_WORKPATH_SUFFIX);
            path_tmp.append("/");
            job_paths.push_back(path_tmp);
        }
    }

    private_data_p->_cmtx.lock();

    private_data_p->merged_job_path_arr.insert(
      private_data_p->merged_job_path_arr.end(), job_paths.begin(), job_paths.end());
    if (data_summary_get_filed) {
        ret = private_data_p->sorter.MergePartPaths(job_paths, &part_info);
    } else {
        ret = private_data_p->sorter.MergePartPaths(job_paths, &part_info, true);
    }

    private_data_p->stat.lines_in += part_info.total_lines;
    private_data_p->stat.lines_crcsum_in += part_info.objid_cksum + part_info.value_cksum;

    private_data_p->_cmtx.unlock();

    result_arr.push_back(part_info.total_lines);
    result_arr.push_back(part_info.objid_cksum + part_info.value_cksum);

    data_resp = CR_Class_NS::u64arr2str(result_arr);

    return ret;
}

static int
_lib_do_query_key_sample(sort_private_data_t *private_data_p,
    const std::string &data_req, std::string &data_resp)
{
    std::string last_job_id = CR_Class_NS::str_getparam(data_req, DMIBSORTLOAD_PN_LAST_JOB_ID);

    std::string key_samples_savepath;

    key_samples_savepath = CR_Class_NS::dirname(private_data_p->work_path_arr[0].c_str());
    key_samples_savepath += "/";
    key_samples_savepath += CFM_Basic_NS::make_node_name_prefix(private_data_p->current_node);
    key_samples_savepath += last_job_id;
    key_samples_savepath += "S";
    key_samples_savepath += LIBSORT_KN_KEY_SAMPLES_SUFFIX;

    return CR_Class_NS::load_string(key_samples_savepath.c_str(), data_resp);
}

////////////////////////////////////

static int
_on_splitter_call_noalign(void *arg_p, const CFM_Basic_NS::cluster_info_t &cluster_info,
    const int64_t row_id_in, const int64_t obj_id, const void *key_p, const uint64_t key_size,
    const void *value_p, const uint64_t value_size, int64_t &row_id_out, int64_t &splitter_func_ret2)
{
    sort_private_data_t *private_data_p = (sort_private_data_t *)arg_p;

    if (row_id_in % private_data_p->block_lines == 0) {
        splitter_func_ret2 |= CFMCLUSTER_FLAG_SPLIT_BLOCK;
    }

    return private_data_p->current_node_id;
}

static int
_on_splitter_call_align(void *arg_p, const CFM_Basic_NS::cluster_info_t &cluster_info,
    const int64_t row_id_in, const int64_t obj_id, const void *key_p, const uint64_t key_size,
    const void *value_p, const uint64_t value_size, int64_t &row_id_out, int64_t &splitter_func_ret2)
{
    sort_private_data_t *private_data_p = (sort_private_data_t *)arg_p;
    size_t &target_id = private_data_p->current_target_striping_id;

    for (; target_id< private_data_p->line_target_array.size(); target_id++) {
        if (private_data_p->line_target_array[target_id] > 0) {
            private_data_p->line_target_array[target_id]--;
            break;
        }
    }

    if (row_id_in % private_data_p->block_lines == 0) {
        splitter_func_ret2 |= CFMCLUSTER_FLAG_SPLIT_BLOCK;
    }

#ifdef DEBUG
    private_data_p->target_line_count[target_id]++;
#endif

    return target_id;
}

static int
_after_buf_refreshed_func(void *refreshed_arg_p,
    const CFM_Basic_NS::cluster_info_t &cluster_info, const size_t target_striping_id,
    int64_t &row_id_left, int64_t &row_id_right, CR_FixedLinearStorage &fls_req,
    const int jobdata_ret, const std::string &data_resp, const std::string &errmsg,
    uintptr_t &pack_arg, int64_t &ret_flags)
{
    sort_private_data_t *private_data_p = (sort_private_data_t *)refreshed_arg_p;

    DPRINTF("[%u]Send data block [%lld ~ %lld] to striping[%u], %s\n", (unsigned)pack_arg,
      (long long)row_id_left, (long long)row_id_right, (unsigned)target_striping_id,
      CR_Class_NS::strerrno(jobdata_ret));

    if (!jobdata_ret) {
        uint64_t fls_total_lines = fls_req.GetTotalLines();
        uint64_t cksum_out = fls_req.GetObjIDCKSUM() + fls_req.GetValueCKSUM();
        std::string sample_key;
        fls_req.Query(fls_total_lines - 1, sample_key);
        private_data_p->_cmtx.lock();
        private_data_p->stat.lines_out += fls_total_lines;
        private_data_p->stat.lines_crcsum_out += cksum_out;
        msgCRPair *one_sample_p = private_data_p->stat.key_samples.add_pair_list();
        one_sample_p->set_key(sample_key);
        if (private_data_p->stat.lines_out == private_data_p->stat.lines_in)
            private_data_p->_cmtx.broadcast();
        private_data_p->_cmtx.unlock();
    } else {
        if (jobdata_ret != EALREADY) {
            if (pack_arg < 3) {
                pack_arg++;
                ret_flags |= CFMCLUSTER_FLAG_REDO_IT;
            }
        }
    }

    return jobdata_ret;
}
