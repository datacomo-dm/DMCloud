#include <thread>
#include <cr_class/cr_class.h>
#include <cr_class/cr_addon.h>
#include <cr_class/cr_locked.h>
#include <cr_class/cr_blockqueue.h>
#include <cr_class/cr_unlimitedfifo.h>
#include <cr_class/cr_quickhash.h>
#include <cfm_drv/dm_ib_sort_load.h>
#include <IBCompress.h>
#include "libcfm_ibload.h"
#include "dm_ib_block_converter.h"

#define LIBLOAD_IN_FIFO_FILENAME_PREFIX		"LIBLOAD-IN-"
#define LIBLOAD_FIFO_MIN_FILESIZE		(UINTMAX_C(0x100000) * 128)
#define LIBLOAD_FIFO_MAX_FILESIZE		(UINTMAX_C(0x100000) * 1024)

#define LIBLOAD_LASTPACK_STORAGE_SIZE		(1024 * 1024 * 32)

#define LIBLOAD_KS_DEFAULT_BUCKET		"RIAK_PACKET_PAT"

////////////////////////////////////

typedef struct {
    int64_t rowid_left;
    std::string data;
} ibload_fifo_block_t;

typedef struct {
    CR_DataControl::CondMutex _cmtx;
    CFM_Basic_NS::cluster_info_t cluster_info;
    std::pair<int,int> current_node;
    std::string job_id;
    std::string real_job_id;
    CR_UnlimitedFifo in_fifo_disk;
    CR_BlockQueue<ibload_fifo_block_t> in_fifo_mem;
    bool in_fifo_use_disk;

    std::string work_path;

    bool work_start;
    bool load_end;

    std::thread *key_alloc_tp;
    volatile bool key_alloc_stop;

    std::thread *block_converter_tp;
    volatile bool block_converter_stop;

    DM_IB_BlockConverter block_converter;

    uint64_t total_lines_in;
    uint64_t total_lines_out;
    uint64_t crcsum_in;
    uint64_t crcsum_out;

    std::string table_info_str;
    std::string save_pathname;
    int64_t lineid_begin;
    size_t block_lines;
    size_t longest_line_size;
    int64_t local_lineid_begin;
    int64_t local_lineid_end;
    int64_t lastpack_lineid_begin;

    CR_FixedLinearStorage lastpack_fls;
    int64_t lastpack_linecount;

    bool use_riak;

    RiakCluster riakc;
    CR_BlockQueue<int64_t> riak_ks_pool;
    CR_Locked<void*> riak_handle;

    std::string riak_connect_str;
    std::string riak_bucket;
    std::string ks_bucket;
    std::vector<int64_t> ksi_data;
} ibload_private_data_t;

////////////////////////////////////

static void *lib_on_start(const CFM_Basic_NS::cluster_info_t &cluster_info,
    const std::pair<int,int> &current_node, const std::string &job_id,
    const std::string &extra_param, const std::vector<std::string> &work_path_arr,
    const bool static_work_path);
static int lib_on_stop(void *proc_data_p, const std::string &data_req);
static int lib_on_query(void *proc_data_p, const std::string &data_req, std::string &data_resp);
static int lib_on_data(void *proc_data_p, const int64_t rowid_left, const int64_t rowid_right,
    const std::string &data_req, std::string &data_resp);

static void _key_alloc(ibload_private_data_t *private_data_p);
static void _block_converter(ibload_private_data_t *private_data_p);
static void _async_riak_rebuild(ibload_private_data_t *private_data_p);

static int _lib_do_query_stat(ibload_private_data_t *private_data_p,
    const std::string &data_req, std::string &data_resp);
static int _lib_do_work_start(ibload_private_data_t *private_data_p,
    const std::string &data_req, std::string &data_resp);

////////////////////////////////////

static cfm_lib_kvput_t kvput;

static cfm_lib_kvget_t kvget;

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
    CR_Class_NS::dprintfx_line_prefix = std::string("[") + job_id + std::string("]");

    DPRINTFX(20, "%s\n", VERSION_STR_CR_CLASS_H);

    ibload_private_data_t *private_data_p = new ibload_private_data_t;

    private_data_p->cluster_info = cluster_info;
    private_data_p->current_node = current_node;
    private_data_p->job_id = job_id;
    private_data_p->real_job_id = job_id.substr(0, job_id.size() - 1);
    private_data_p->total_lines_in = 0;
    private_data_p->total_lines_out = 0;
    private_data_p->crcsum_in = 0;
    private_data_p->crcsum_out = 0;
    private_data_p->longest_line_size = 0;
    private_data_p->work_start = false;
    private_data_p->load_end = false;
    private_data_p->use_riak = false;
    private_data_p->work_path = work_path_arr[0];
    private_data_p->riak_ks_pool.set_args(SIZE_MAX);
    private_data_p->riak_handle = NULL;
    private_data_p->ksi_data.resize(16);

    std::string riak_connect_str = CR_Class_NS::str_getparam(extra_param, DMIBSORTLOAD_PN_RIAK_CONNECT_STR);

    if (riak_connect_str.size() > 0) {
        private_data_p->use_riak = true;
        private_data_p->riak_connect_str = riak_connect_str;
        private_data_p->riak_bucket = CR_Class_NS::str_getparam(extra_param, DMIBSORTLOAD_PN_RIAK_BUCKET);
        private_data_p->ks_bucket = CR_Class_NS::str_getparam(extra_param, DMIBSORTLOAD_PN_KS_BUCKET);
        if (private_data_p->ks_bucket.size() == 0)
            private_data_p->ks_bucket = LIBLOAD_KS_DEFAULT_BUCKET;
        private_data_p->key_alloc_stop = false;
        private_data_p->key_alloc_tp = new std::thread(_key_alloc, private_data_p);

        std::thread t(_async_riak_rebuild, private_data_p);
        t.detach();
    }

    return private_data_p;
}

static int
lib_on_stop(void *proc_data_p, const std::string &data_req)
{
    if (!proc_data_p)
        return EFAULT;

    ibload_private_data_t *private_data_p = (ibload_private_data_t*)proc_data_p;

    private_data_p->_cmtx.lock();

    private_data_p->key_alloc_stop = true;
    if (private_data_p->key_alloc_tp) {
        private_data_p->key_alloc_tp->join();
        delete private_data_p->key_alloc_tp;
        private_data_p->key_alloc_tp = NULL;
    }

    private_data_p->block_converter_stop = true;
    if (private_data_p->block_converter_tp) {
        private_data_p->block_converter_tp->join();
        delete private_data_p->block_converter_tp;
        private_data_p->block_converter_tp = NULL;
    }

    private_data_p->_cmtx.unlock();

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
        ret = _lib_do_query_stat((ibload_private_data_t*)proc_data_p, data_req, data_resp);
    } else if (query_cmd_str == LIBIBLOAD_PV_WORK_START) {
        ret = _lib_do_work_start((ibload_private_data_t*)proc_data_p, data_req, data_resp);
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

    ibload_private_data_t *private_data_p = (ibload_private_data_t*)proc_data_p;

    if (private_data_p->work_start) {
        int64_t need_pack_lines = rowid_right - rowid_left + 1;
        if (fls_infos.total_lines == (uint64_t)need_pack_lines) {
            if (private_data_p->in_fifo_use_disk) {
                private_data_p->in_fifo_disk.PushBack(rowid_left, data_req, NULL);
            } else {
                ibload_fifo_block_t tmp_block;
                tmp_block.rowid_left = rowid_left;
                tmp_block.data = data_req;
                private_data_p->in_fifo_mem.push_back(tmp_block, 86400);
            }
            private_data_p->_cmtx.lock();
            private_data_p->total_lines_in += fls_infos.total_lines;
            private_data_p->crcsum_in += fls_infos.objid_cksum + fls_infos.value_cksum;
            private_data_p->_cmtx.unlock();
        } else {
            DPRINTF("%lld - %lld + 1 != %llu\n", (long long)rowid_right,
              (long long)rowid_left, (long long unsigned)fls_infos.total_lines);
            ret = ERANGE;
        }
    } else {
        ret = EINPROGRESS;
    }

    return ret;
}

////////////////////////////////////

static int
_lib_do_query_stat(ibload_private_data_t *private_data_p, const std::string &data_req, std::string &data_resp)
{
    return 0;
}

static int
_lib_do_work_start(ibload_private_data_t *private_data_p, const std::string &data_req, std::string &data_resp)
{
    int ret = 0;
    std::string table_info_str;
    std::string save_pathname;
    int64_t lineid_begin;
    size_t block_lines;

    std::string in_fifo_filename_prefix = private_data_p->work_path;
    in_fifo_filename_prefix += "/";
    in_fifo_filename_prefix += LIBLOAD_IN_FIFO_FILENAME_PREFIX;
    size_t fifo_slice_size = CR_Class_NS::str_getparam_u64(data_req, DMIBSORTLOAD_PN_FIFO_SIZE);
    if (fifo_slice_size < LIBLOAD_FIFO_MIN_FILESIZE)
        fifo_slice_size = LIBLOAD_FIFO_MIN_FILESIZE;
    if (fifo_slice_size > LIBLOAD_FIFO_MAX_FILESIZE)
        fifo_slice_size = LIBLOAD_FIFO_MAX_FILESIZE;
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

    table_info_str = CR_Class_NS::str_getparam(data_req, LIBIBLOAD_PN_TABLE_INFO);
    if (table_info_str.size() == 0) {
        DPRINTF("LIBIBLOAD_PN_TABLE_INFO not set or wrong\n");
        return EINVAL;
    }

    save_pathname = CR_Class_NS::str_getparam(data_req, LIBIBLOAD_PN_SAVE_PATHNAME);
    if (save_pathname.size() == 0) {
        DPRINTF("LIBIBLOAD_PN_SAVE_PATHNAME not set or wrong\n");
        return EINVAL;
    }

    lineid_begin = CR_Class_NS::str_getparam_i64(data_req, DMIBSORTLOAD_PN_LINEID_BEGIN, -1);
    if (lineid_begin < 0) {
        DPRINTF("DMIBSORTLOAD_PN_LINEID_BEGIN{%lld} not set or wrong\n", (long long)lineid_begin);
        return EINVAL;
    }

    block_lines = CR_Class_NS::str_getparam_u64(data_req, DMIBSORTLOAD_PN_BLOCK_LINES, 0);
    if (block_lines == 0) {
        DPRINTF("DMIBSORTLOAD_PN_BLOCK_LINES{%llu} not set or wrong\n", (long long unsigned)block_lines);
        return EINVAL;
    } else if (block_lines > DM_IB_MAX_BLOCKLINES) {
        block_lines = DM_IB_MAX_BLOCKLINES;
    }

    std::vector<uint64_t> packindex_req =
      CR_Class_NS::str_getparam_u64arr(data_req, LIBIBLOAD_PN_PACKINDEX_INFO);

    bool packindex_multimode =
      CR_Class_NS::str_getparam_bool(data_req, LIBIBLOAD_PN_PACKINDEX_MULTIMODE);

    std::vector<uint64_t> line_count_array =
      CR_Class_NS::str_getparam_u64arr(data_req, DMIBSORTLOAD_PN_LINE_COUNT_ARRAY, -1);

    if (line_count_array.size() != private_data_p->cluster_info.size()) {
        DPRINTF("line_count_array.size(),%lld != cluster_info.size(),%lld\n",
          (long long)line_count_array.size(), (long long)private_data_p->cluster_info.size());
        return EINVAL;
    }

    size_t filethread_count = CR_Class_NS::str_getparam_u64(data_req, LIBIBLOAD_PN_FILETHREAD_COUNT);

    if (private_data_p->use_riak) {
        line_count_array[0] += lineid_begin;
        CR_Class_NS::align_uintarr(line_count_array.data(), line_count_array.size(), block_lines);
        line_count_array[0] -= lineid_begin;
    }

    int64_t local_lineid_begin = lineid_begin;
    for (int i=0; i< private_data_p->current_node.first; i++) {
        local_lineid_begin += line_count_array[i];
    }

    int64_t local_lineid_end = local_lineid_begin + line_count_array[private_data_p->current_node.first] - 1;

    int64_t lastpack_lineid_begin = local_lineid_end / block_lines * block_lines;

    if (lastpack_lineid_begin < local_lineid_begin)
        lastpack_lineid_begin = local_lineid_begin;

    private_data_p->_cmtx.lock();

    if (!private_data_p->work_start) {
        RiakCluster *riakp = NULL;
        if (private_data_p->use_riak) {
            riakp = &(private_data_p->riakc);
            double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + 3600;
            DPRINTF("Waiting riak cluster...\n");
            while (private_data_p->riak_handle == NULL) {
                usleep(50000);
                if (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) > exp_time) {
                    DPRINTF("Wait riak cluster timeout\n");
                    ret = ETIMEDOUT;
                    break;
                }
            }
            if (!ret)
                DPRINTF("Waiting riak cluster OK\n");
        }
        if (!ret) {
            ret = private_data_p->block_converter.SetArgs(save_pathname, private_data_p->work_path,
              private_data_p->real_job_id, private_data_p->current_node.first, packindex_multimode,
              packindex_req, table_info_str, local_lineid_begin, local_lineid_end, block_lines,
              riakp, private_data_p->riak_bucket, private_data_p->riak_handle.value(),
              &(private_data_p->riak_ks_pool));
        }
        if (!ret) {
            ret = private_data_p->block_converter.SetFileThreadCount(filethread_count);
        }
        if (!ret) {
            private_data_p->table_info_str = table_info_str;
            private_data_p->save_pathname = save_pathname;
            private_data_p->lineid_begin = lineid_begin;
            private_data_p->block_lines = block_lines;
            private_data_p->local_lineid_begin = local_lineid_begin;
            private_data_p->local_lineid_end = local_lineid_end;
            if (private_data_p->use_riak) {
                private_data_p->lastpack_lineid_begin = lastpack_lineid_begin;
                private_data_p->lastpack_fls.ReSize(LIBLOAD_LASTPACK_STORAGE_SIZE);
                private_data_p->lastpack_linecount = 0;
            }
            private_data_p->work_start = true;
            private_data_p->block_converter_stop = false;
            private_data_p->block_converter_tp = new std::thread(_block_converter, private_data_p);
        }
    } else {
        ret = EALREADY;
    }

    private_data_p->_cmtx.unlock();

    return ret;
}

////////////////////////////////////

static void
_key_alloc(ibload_private_data_t *private_data_p)
{
    while (!private_data_p->key_alloc_stop) {
        usleep(10000);

        if (!private_data_p->riakc.IsReady())
            continue;

        size_t cur_alloc_count = 0;

        while (private_data_p->riak_ks_pool.size() < 64) {
            int64_t new_ks = private_data_p->riakc.KeySpaceAlloc(private_data_p->ks_bucket, 0, 0);
            if (new_ks < 0) {
                DPRINTF("Alloc key space failed[%s]\n", CR_Class_NS::strerrno(0 - new_ks));
                continue;
            } else {
                DPRINTFX(15, "Key space 0x%016llX(%lld) alloc SUCCESS\n",
                  (long long)new_ks, (long long)(new_ks >> 12));
                private_data_p->riak_ks_pool.push_back(new_ks, 1);
                private_data_p->ksi_data.push_back(new_ks);
                cur_alloc_count++;
            }
        }
    }
}

////////////////////////////////////

static void
_block_converter(ibload_private_data_t *private_data_p)
{
    int fret = 0;
    CR_FixedLinearStorage slice_fls;
    int64_t rowid_left;
    int64_t rowid_right;
    uint64_t fls_lines;
    std::string slice_fls_str;
    std::string _value;
    bool _value_is_null;
    uint64_t pack_cksum = 0;
    const int64_t total_lines_need =
      private_data_p->local_lineid_end - private_data_p->local_lineid_begin + 1;
    int64_t total_lines = 0;
    std::vector<uint64_t> result_arr;
    std::string job_done_key, job_done_value;

    while (total_lines < total_lines_need) {
        if (private_data_p->block_converter_stop)
            break;

        if (private_data_p->in_fifo_use_disk) {
            fret = private_data_p->in_fifo_disk.PopFront(
              rowid_left, slice_fls_str, _value, _value_is_null, 0.1);
        } else {
            ibload_fifo_block_t tmp_block;
            fret = private_data_p->in_fifo_mem.pop_front(tmp_block, 0.1);
            if (!fret) {
                rowid_left = tmp_block.rowid_left;
                slice_fls_str = tmp_block.data;
            }
        }

        if (!fret) fret = slice_fls.MapFromArray(slice_fls_str.data(), slice_fls_str.size());

        if (!fret) {
            fls_lines = slice_fls.GetTotalLines();
            if (fls_lines > 0) {
                rowid_right = rowid_left + fls_lines - 1;

                if (private_data_p->use_riak && (rowid_left >= private_data_p->lastpack_lineid_begin)) {
                    CR_FixedLinearStorage_NS::line_info_t line_info;

                    for (uint64_t i=0; i<fls_lines; i++) {
                        int local_fret = slice_fls.Query(i, line_info);
                        int64_t lastpack_lid = rowid_left - private_data_p->lastpack_lineid_begin + i;
                        do {
                            local_fret = private_data_p->lastpack_fls.Set(lastpack_lid, line_info);
                            if (local_fret == ENOBUFS) {
                                local_fret = private_data_p->lastpack_fls.Rebuild(
                                  private_data_p->lastpack_fls.GetStorageSize()
                                  + LIBLOAD_LASTPACK_STORAGE_SIZE);
                            }
                        } while (local_fret == ENOBUFS);
                    }

                    private_data_p->lastpack_linecount += fls_lines;

                    DPRINTF("Save[%lld ~ %lld](alloc==%llu,unused==%llu) to last pack[%lld ~ %lld]\n",
                      (long long)rowid_left, (long long)rowid_right,
                      (long long unsigned)slice_fls_str.size(), (long long unsigned)slice_fls.GetFreeSpace(),
                      (long long)private_data_p->lastpack_lineid_begin,
                      (long long)private_data_p->local_lineid_end);

                    int64_t lastpack_need_count = private_data_p->local_lineid_end
                      - private_data_p->lastpack_lineid_begin + 1;

                    if (private_data_p->lastpack_linecount == lastpack_need_count) {
                        fret = private_data_p->block_converter.BlockConvert(
                          private_data_p->lastpack_lineid_begin, private_data_p->local_lineid_end,
                          private_data_p->lastpack_fls);
                        DPRINTF("Convert[%lld ~ %lld] LAST, %s\n",
                          (long long)private_data_p->lastpack_lineid_begin,
                          (long long)private_data_p->local_lineid_end, CR_Class_NS::strerrno(fret));
                    } else if (private_data_p->lastpack_linecount > lastpack_need_count) {
                        DPRINTF("Last pack[%lld ~ %lld] got %lu lines\n",
                          (long long)private_data_p->lastpack_lineid_begin,
                          (long long)private_data_p->local_lineid_end,
                          (long unsigned)private_data_p->lastpack_linecount);
                        fret = ERANGE;
                    }
                } else {
                    fret = private_data_p->block_converter.BlockConvert(rowid_left, rowid_right, slice_fls);
                    DPRINTF("Convert[%lld ~ %lld](alloc==%llu,unused==%llu), %s\n",
                      (long long)rowid_left, (long long)rowid_right,
                      (long long unsigned)slice_fls_str.size(),
                      (long long unsigned)slice_fls.GetFreeSpace(),
                      CR_Class_NS::strerrno(fret));
                }
            }

            if (fret)
                break;

            pack_cksum = slice_fls.GetObjIDCKSUM() + slice_fls.GetValueCKSUM();

            private_data_p->_cmtx.lock();
            private_data_p->total_lines_out += fls_lines;
            private_data_p->crcsum_out += pack_cksum;
            private_data_p->_cmtx.unlock();

            total_lines += fls_lines;
        }
    }

    if (total_lines_need != total_lines) {
        DPRINTF("Total %lld lines, but %lld converted.\n",
          (long long)total_lines_need, (long long)total_lines);
        fret = ERANGE;
        goto errout;
    }

    private_data_p->_cmtx.lock();
    result_arr.push_back(private_data_p->total_lines_out);
    result_arr.push_back(private_data_p->crcsum_out);
    result_arr.push_back(private_data_p->total_lines_in);
    result_arr.push_back(private_data_p->crcsum_in);

    private_data_p->key_alloc_stop = true;
    if (private_data_p->key_alloc_tp) {
        private_data_p->key_alloc_tp->join();
        delete private_data_p->key_alloc_tp;
        private_data_p->key_alloc_tp = NULL;
    }

    if ((!fret) && private_data_p->riak_handle.value()) {
        std::string errmsg;
        DPRINTF("AsyncWait start\n");
        int64_t riak_total_size =
          private_data_p->riakc.AsyncWait(private_data_p->riak_handle.value(), errmsg, 36000);
        if (riak_total_size >= 0) {
            private_data_p->ksi_data[0] = private_data_p->ksi_data.size() - 16;
            private_data_p->ksi_data[1] = riak_total_size;
            private_data_p->ksi_data[2] = CR_QuickHash::CRC64Hash(&(private_data_p->ksi_data[3]),
              (private_data_p->ksi_data.size() - 3) * sizeof(int64_t),
              CR_QuickHash::CRC64Hash(&(private_data_p->ksi_data[0]), sizeof(int64_t) * 2));
            DPRINTF("AsyncWait OK, total_size == %lld\n", (long long)riak_total_size);
        } else {
            fret = 0 - riak_total_size;
            DPRINTF("AsyncWait failed[%s]\n", CR_Class_NS::strerrno(fret));
        }
        private_data_p->riak_handle = NULL;
    }

    private_data_p->_cmtx.unlock();

    if (!fret) {
        fret = private_data_p->block_converter.Finish(result_arr, &(private_data_p->ksi_data));
        if (fret)
            goto errout;
    }

    DPRINTF("%llu lines in, CRCSUM-IN[0x%016llX], %llu lines out, CRCSUM-OUT[0x%016llX]\n",
      (long long unsigned)result_arr[2], (long long)result_arr[3],
      (long long unsigned)result_arr[0], (long long)result_arr[1]);

    job_done_key = std::string(DMIBSORTLOAD_KN_JOB_DONE_PREFIX);
    job_done_key += private_data_p->job_id;
    job_done_value = CR_Class_NS::u64arr2str(result_arr);

    fret = kvput(job_done_key, &job_done_value, false);

    DPRINTF("Save job done value, %s\n", CR_Class_NS::strerrno(fret));

    return;

errout:
    CR_Class_NS::delay_exit(0, fret);

    return;
}

////////////////////////////////////

static void
_async_riak_rebuild(ibload_private_data_t *private_data_p)
{
    DPRINTF("Start riak cluster\n");
    int fret = private_data_p->riakc.Rebuild(private_data_p->riak_connect_str.c_str());
    if (fret < 0) {
        DPRINTF("Connect to riak cluster failed[%s]\n", CR_Class_NS::strerrno(0 - fret));
        return;
    }
    void *riak_handle = private_data_p->riakc.AsyncInit(16, 128);
    if (!riak_handle) {
        DPRINTF("Async init failed\n");
        return;
    }
    private_data_p->riak_handle = riak_handle;
}
