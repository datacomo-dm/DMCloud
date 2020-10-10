#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <unordered_set>
#include <cr_class/cr_file.h>
#include <cr_class/cr_compress.h>
#include <cfm_drv/dm_ib_rowdata.h>
#include <cfm_drv/dm_ib_sort_load.h>
#include "dm_ib_block_converter.h"
#include "dm_ib_quick_pack.h"

#define _MOUNT_TYPE_UNKNOWN		(0)
#define _MOUNT_TYPE_LOCAL		(1)
#define _MOUNT_TYPE_SSH			(2)
#define _MOUNT_TYPE_NFS			(3)

///////////////////////////////////

static void
_do_save_pack(DM_IB_QuickPack *pack_p, int *fret_p, std::list<int64_t> *pack_id_list_p,
    int fd, int file_id, CR_DataControl::Sem *comp_sem_p, CR_DataControl::Sem *file_sem_p);

static int
_push_pki_line(CR_FixedLinearStorage &sort_out_fls, std::vector<uint32_t> &pki_idx_array,
    const std::string &pki_last_key, const std::string &job_id, int pki_fd, uint64_t &pki_line_count);

///////////////////////////////////

static void
_do_save_pack(DM_IB_QuickPack *pack_p, int *fret_p, std::list<int64_t> *pack_id_list_p,
    int fd, int file_id, CR_DataControl::Sem *comp_sem_p, CR_DataControl::Sem *file_sem_p)
{
    if (comp_sem_p)
        comp_sem_p->wait();

    *fret_p = pack_p->SavePack(pack_id_list_p, fd, file_id, file_sem_p);

    if (comp_sem_p)
        comp_sem_p->post();
}

///////////////////////////////////

DM_IB_BlockConverter::DM_IB_BlockConverter()
{
    this->_pki_array = NULL;
    this->_mount_type = _MOUNT_TYPE_UNKNOWN;
    this->_file_sem.post();
    this->_riakp = NULL;
    this->_riak_handle = NULL;
    this->_ks_pool_p = NULL;
    this->_ksi_fd = -1;
}

DM_IB_BlockConverter::~DM_IB_BlockConverter()
{
    if (this->_pki_array) {
        delete []this->_pki_array;
    }
    this->close_all_fd();
    CR_Class_NS::system("sync");
    std::string cmd_str;
    switch (this->_mount_type) {
    case _MOUNT_TYPE_SSH :
        cmd_str = "fusermount -uz '";
        cmd_str += this->_dst_pathname;
        cmd_str += "'";
        CR_Class_NS::system(cmd_str.c_str());
        break;
    case _MOUNT_TYPE_NFS :
#if defined (__OpenBSD__)
        cmd_str = "umount -f '";
        cmd_str += this->_dst_pathname;
        cmd_str += "'";
#else
        cmd_str = "sudo umount.nfs '";
        cmd_str += this->_dst_pathname;
        cmd_str += "' -l";
#endif
        CR_Class_NS::system(cmd_str.c_str());
        break;
    default :
        break;
    }
}

void
DM_IB_BlockConverter::close_all_fd()
{
    for (size_t i=0; i<_dpn_fd_array.size(); i++) {
        if (this->_dpn_fd_array[i] >= 0)
            CR_File::close(this->_dpn_fd_array[i]);
    }
    for (size_t i=0; i<_rsi_fd_array.size(); i++) {
        if (this->_rsi_fd_array[i] >= 0)
            CR_File::close(this->_rsi_fd_array[i]);
    }
    for (size_t i=0; i<_pki_fd_array.size(); i++) {
        if (this->_pki_fd_array[i] >= 0)
            CR_File::close(this->_pki_fd_array[i]);
    }
    for (size_t i=0; i<_pak_fd_array.size(); i++) {
        if (this->_pak_fd_array[i] >= 0)
            CR_File::close(this->_pak_fd_array[i]);
    }
    if (this->_ksi_fd >= 0)
        CR_File::close(this->_ksi_fd);
    this->_dpn_fd_array.clear();
    this->_rsi_fd_array.clear();
    this->_pki_fd_array.clear();
    this->_pak_fd_array.clear();
}

int
DM_IB_BlockConverter::SetArgs(const std::string &save_pathname, const std::string &work_pathname,
    const std::string &job_id, const size_t striping_id, const bool pki_use_multimode,
    const std::vector<uint64_t> &packindex_req, const std::string &tbl_info_str,
    const int64_t lineid_begin, const int64_t lineid_end, const size_t block_lines,
    RiakCluster *riakp, const std::string &riak_bucket, void *riak_handle,
    CR_BlockQueue<int64_t> *ks_pool_p)
{
    int fret;

    std::string tmp_pathname = work_pathname + std::string("/tmp");
    if (mkdir(tmp_pathname.c_str(), 0700) < 0) {
        DPRINTF("mkdir(\"%s\"), %s\n", tmp_pathname.c_str(), CR_Class_NS::strerrno(errno));
        return errno;
    }

    std::string dst_pathname = work_pathname + std::string("/dst");
    if (strncmp(save_pathname.c_str(), "/", 1) == 0) {			// local path
        DIR *tmp_dp = opendir(save_pathname.c_str());
        if (!tmp_dp) {
            DPRINTF("opendir(\"%s\"), %s\n", save_pathname.c_str(), CR_Class_NS::strerrno(errno));
            return errno;
        }
        closedir(tmp_dp);
        fret = symlink(save_pathname.c_str(), dst_pathname.c_str());
        this->_mount_type = _MOUNT_TYPE_LOCAL;
    } else if (strncmp(save_pathname.c_str(), "ssh://", 6) == 0) {	// ssh path
        if (mkdir(dst_pathname.c_str(), 0700) < 0) {
            DPRINTF("mkdir(\"%s\"), %s\n", dst_pathname.c_str(), CR_Class_NS::strerrno(errno));
            return errno;
        }
        std::string true_save_pathname = save_pathname.substr(6);
        std::string cmd_str = "sshfs '";
        cmd_str += true_save_pathname;
        cmd_str += "' '";
        cmd_str += dst_pathname;
        cmd_str += "'";
        fret = CR_Class_NS::system(cmd_str.c_str());
        if (fret == -1) {
            DPRINTF("system(\"%s\"), %s\n", cmd_str.c_str(), CR_Class_NS::strerrno(errno));
            return errno;
        } else {
            int child_ret = WEXITSTATUS(fret);
            if (child_ret) {
                DPRINTF("system(\"%s\") == 0, but child return %s\n",
                  cmd_str.c_str(), CR_Class_NS::strerrno(child_ret));
                return child_ret;
            }
        }
        this->_mount_type = _MOUNT_TYPE_SSH;
    } else if (strncmp(save_pathname.c_str(), "nfs://", 6) == 0) {	// nfs path
        if (mkdir(dst_pathname.c_str(), 0700) < 0) {
            DPRINTF("mkdir(\"%s\"), %s\n", dst_pathname.c_str(), CR_Class_NS::strerrno(errno));
            return errno;
        }
        std::string true_save_pathname = save_pathname.substr(6);
#if defined (__OpenBSD__)
        std::string cmd_str = "mount_nfs -s -i '";
        cmd_str += true_save_pathname;
        cmd_str += "' '";
        cmd_str += dst_pathname;
        cmd_str += "'";
#else
        std::string cmd_str = "sudo mount.nfs '";
        cmd_str += true_save_pathname;
        cmd_str += "' '";
        cmd_str += dst_pathname;
        cmd_str += "' -o soft,intr";
#endif
        fret = CR_Class_NS::system(cmd_str.c_str());
        if (fret == -1) {
            DPRINTF("system(\"%s\"), %s\n", cmd_str.c_str(), CR_Class_NS::strerrno(errno));
            return errno;
        } else {
            int child_ret = WEXITSTATUS(fret);
            if (child_ret) {
                DPRINTF("system(\"%s\") == 0, but child return %s\n",
                  cmd_str.c_str(), CR_Class_NS::strerrno(child_ret));
                return child_ret;
            }
        }
        this->_mount_type = _MOUNT_TYPE_NFS;
    } else {
        DPRINTF("Save path [%s] not support\n", save_pathname.c_str());
        return ENOTDIR;
    }

    fret = this->_table_info.ParseFromString(tbl_info_str);
    if (fret) {
        std::string job_id_out = std::string("[") + job_id + std::string("]");
        DPRINTF("Parse table info, %s\n", CR_Class_NS::strerrno(fret));
        CR_Class_NS::hexdump(stderr, tbl_info_str.data(), tbl_info_str.size(), 16, job_id_out.c_str());
        return fret;
    }

    DPRINTF("Table (%s);\n", this->_table_info.Print().c_str());

    this->_tmp_pathname = tmp_pathname;
    this->_dst_pathname = dst_pathname;
    this->_job_id = job_id;
    this->_striping_id = striping_id;
    this->_pki_use_multimode = pki_use_multimode;
    this->_packindex_req = packindex_req;
    this->_lineid_begin = lineid_begin;
    this->_lineid_end = lineid_end;
    this->_block_lines = block_lines;
    this->_riakp = riakp;
    this->_riak_bucket = riak_bucket;
    this->_riak_handle = riak_handle;
    this->_ks_pool_p = ks_pool_p;

    this->close_all_fd();

    size_t col_count = this->_table_info.size();
    size_t pki_count = packindex_req.size();

    this->_dpn_array.clear();
    this->_dpn_array.resize(col_count);

    this->_rsi_array.clear();
    this->_rsi_array.resize(col_count);

    this->_compress_level_array.clear();
    this->_compress_level_array.resize(col_count);

    this->_pack_file_id_array.resize(col_count);

    this->_pack_id_list_arr.resize(col_count);

    char str_buf[PATH_MAX];
    int fd_tmp;
    std::string compress_mode;

    if (riakp) {
        snprintf(str_buf, sizeof(str_buf), "%s/S%s-N%05u.ksi",
          this->_tmp_pathname.c_str(), job_id.c_str(), (unsigned)striping_id);
        fd_tmp = CR_File::open(str_buf, O_WRONLY|O_CREAT|O_TRUNC, 0644);
        if (fd_tmp < 0) {
            DPRINTF("open(\"%s\"), %s\n", str_buf, CR_Class_NS::strerrno(errno));
            return errno;
        }
        this->_ksi_fd = fd_tmp;
    }

    for (size_t column_id=0; column_id<col_count; column_id++) {
        const DM_IB_TableInfo::column_info_t &cur_column_info = this->_table_info[column_id];
        this->_dpn_array[column_id].SetArgs(cur_column_info.column_type, lineid_begin, lineid_end);
        this->_rsi_array[column_id].SetArgs(cur_column_info.column_type,
          cur_column_info.column_scale, lineid_begin, lineid_end);

        compress_mode = CR_Class_NS::str_getparam(cur_column_info.column_comment, "compressmode");

        if (compress_mode == "snappy") {
            this->_compress_level_array[column_id] = CR_Compress::CT_SNAPPY;
        } else if (compress_mode == "zlib") {
            this->_compress_level_array[column_id] = CR_Compress::CT_ZLIB;
        } else if (compress_mode == "lzma") {
            this->_compress_level_array[column_id] = CR_Compress::CT_LZMA;
        } else if (compress_mode == "lz4") {
            this->_compress_level_array[column_id] = CR_Compress::CT_LZ4;
        } else {
            this->_compress_level_array[column_id] = -1;
        }

        snprintf(str_buf, sizeof(str_buf), "%s/S%s-N%05u-A%05u.dpn",
          this->_tmp_pathname.c_str(), job_id.c_str(), (unsigned)striping_id, (unsigned)column_id);
        fd_tmp = CR_File::open(str_buf, O_WRONLY|O_CREAT|O_TRUNC, 0644);
        if (fd_tmp < 0) {
            DPRINTF("open(\"%s\"), %s\n", str_buf, CR_Class_NS::strerrno(errno));
            return errno;
        }
        this->_dpn_fd_array.push_back(fd_tmp);

        snprintf(str_buf, sizeof(str_buf), "%s/S%s-N%05u-A%05u.rsi",
          this->_tmp_pathname.c_str(), job_id.c_str(), (unsigned)striping_id, (unsigned)column_id);
        fd_tmp = CR_File::open(str_buf, O_WRONLY|O_CREAT|O_TRUNC, 0644);
        if (fd_tmp < 0) {
            DPRINTF("open(\"%s\"), %s\n", str_buf, CR_Class_NS::strerrno(errno));
            return errno;
        }
        this->_rsi_fd_array.push_back(fd_tmp);

        this->_pack_file_id_array[column_id] = 0;

        if (!this->_riakp) {
            snprintf(str_buf, sizeof(str_buf), "%s/S%s-N%05u-A%05u-B%05u.pck",
              this->_tmp_pathname.c_str(), job_id.c_str(), (unsigned)striping_id, (unsigned)column_id,
              (unsigned)this->_pack_file_id_array[column_id]);
            fd_tmp = CR_File::open(str_buf, O_WRONLY|O_CREAT|O_TRUNC, 0644);
            if (fd_tmp < 0) {
                DPRINTF("open(\"%s\"), %s\n", str_buf, CR_Class_NS::strerrno(errno));
                return errno;
            }
            this->_pak_fd_array.push_back(fd_tmp);
        }
    }

    std::string sort_args;

    sort_args = CR_Class_NS::str_setparam_u64(sort_args,
      EXTERNALSORT_ARGNAME_ROUGHFILESIZE, (1024 * 1024 * 256));
    sort_args = CR_Class_NS::str_setparam_u64(sort_args,
      EXTERNALSORT_ARGNAME_ROUGHSLICESIZE, (1024 * 1024));
    sort_args = CR_Class_NS::str_setparam_bool(sort_args,
      EXTERNALSORT_ARGNAME_ENABLECOMPRESS, true);
    sort_args = CR_Class_NS::str_setparam_bool(sort_args,
      EXTERNALSORT_ARGNAME_ENABLEPRELOAD, true);
    sort_args = CR_Class_NS::str_setparam_u64(sort_args,
      EXTERNALSORT_ARGNAME_MAXSORTTHREADS, 1);
    sort_args = CR_Class_NS::str_setparam_u64(sort_args,
      EXTERNALSORT_ARGNAME_MAXDISKTHREADS, 1);
    sort_args = CR_Class_NS::str_setparam_i64(sort_args,
      EXTERNALSORT_ARGNAME_DEFAULTSIZEMODE, CR_FLS_SIZEMODE_SMALL);

    if (this->_pki_array) {
        delete []this->_pki_array;
    }
    this->_pki_array = new CR_ExternalSort[pki_count];

    for (size_t index_id=0; index_id<pki_count; index_id++) {
        if (packindex_req[index_id] >= col_count) {
            DPRINTF("pack-index[%llu] == %llu out of range\n", (long long unsigned)index_id,
              (long long unsigned)packindex_req[index_id]);
            return ERANGE;
        }
        snprintf(str_buf, sizeof(str_buf), "%s/S%s-N%05u-A%05u.pki",
          this->_tmp_pathname.c_str(), job_id.c_str(),
          (unsigned)striping_id, (unsigned)packindex_req[index_id]);
        fd_tmp = CR_File::open(str_buf, O_WRONLY|O_CREAT|O_TRUNC, 0644);
        if (fd_tmp < 0) {
            DPRINTF("open(\"%s\"), %s\n", str_buf, CR_Class_NS::strerrno(errno));
            return errno;
        }
        this->_pki_fd_array.push_back(fd_tmp);

        std::vector<std::string> sort_filename_prefix_arr;
        std::string sort_filename_prefix = work_pathname;
        sort_filename_prefix += "/PKI-A";
        sort_filename_prefix += CR_Class_NS::u642str(index_id);
        sort_filename_prefix += "-";
        sort_filename_prefix_arr.push_back(sort_filename_prefix);
        this->_pki_array[index_id].SetArgs(sort_args, sort_filename_prefix_arr);
    }

    DPRINTF("Striping[%u]: save_path==\"%s\", lineid_begin==%lld, lineid_end==%lld\n",
      (unsigned)striping_id, save_pathname.c_str(), (long long)this->_lineid_begin,
      (long long)this->_lineid_end);

    return 0;
}

int
DM_IB_BlockConverter::BlockConvert(const int64_t lineid_left, const int64_t lineid_right,
    CR_FixedLinearStorage &block_fls, const size_t max_work_threads, const size_t max_disk_threads)
{
    if ((lineid_left < this->_lineid_begin) || (lineid_right > this->_lineid_end)) {
        DPRINTF("Block [%lld ~ %lld] not in [%lld ~ %lld]\n",
          (long long)lineid_left, (long long)lineid_right,
          (long long)this->_lineid_begin, (long long)this->_lineid_end);
        return ERANGE;
    }

    size_t block_rows = block_fls.GetTotalLines();
    if (block_rows != (size_t)(lineid_right - lineid_left + 1)) {
        DPRINTF("Need %llu rows, got %llu rows\n", (long long unsigned)(lineid_right - lineid_left + 1),
          (long long unsigned)block_rows);
        return EMSGSIZE;
    }

    int ret = 0;
    int fret;
    DM_IB_RowData cur_row;

    cur_row.BindTableInfo(this->_table_info);

    size_t col_count = this->_table_info.size();
    size_t pki_count = this->_packindex_req.size();

    DM_IB_QuickPack packs[col_count];

    std::unordered_set<std::string> pki_key_set[pki_count];

    for (size_t i=0; i<col_count; i++) {
        packs[i].SetArgs(this->_job_id, i, this->_table_info[i].column_type, this->_compress_level_array[i],
          lineid_left, lineid_right, this->_dpn_array[i].LineToDPN(lineid_left), this->_rsi_array[i],
          this->_riakp, this->_riak_bucket, this->_riak_handle, this->_ks_pool_p);
    }

    int64_t orig_lineid;
    const char *_key_p;
    uint64_t _key_size;
    const char *line_data_p;
    uint64_t line_data_size;

    std::string pki_key;
    int64_t cur_pack_id = lineid_left / this->_block_lines;

    int64_t int_v;
    double double_v;
    const char *str_v;
    size_t str_vlen;

    for (int64_t lineid=lineid_left; lineid<=lineid_right; lineid++) {
        fret = block_fls.Query(
          lineid - lineid_left, orig_lineid, _key_p, _key_size, line_data_p, line_data_size);
        if (fret) {
            DPRINTF("block_fls.Query(%ld), %s\n",
              (long)(lineid - lineid_left), CR_Class_NS::strerrno(fret));
            return fret;
        }
        fret = cur_row.MapFromArray(line_data_p, line_data_size);
        if (fret) {
            DPRINTF("Parse line %lld, %s\n", (long long)lineid, CR_Class_NS::strerrno(fret));
            std::string _table_info_str;
            this->_table_info.SerializeToString(_table_info_str);
            DPRINTFX(0, "Table info : -->\n");
            CR_Class_NS::hexdump(stderr, _table_info_str.data(), _table_info_str.size(), 16);
            DPRINTFX(0, "<-- EOF Table info\n");
            DPRINTFX(0, "Line data : -->\n");
            CR_Class_NS::hexdump(stderr, line_data_p, line_data_size, 16);
            DPRINTFX(0, "<-- EOF Line data\n");
            return fret;
        }
        for (size_t col_id=0; col_id<col_count; col_id++) {
            if (cur_row.IsNull(col_id)) {
                fret = packs[col_id].PutNull(lineid);
            } else {
                switch (this->_table_info[col_id].column_type) {
                case DM_IB_TABLEINFO_CT_INT :
                case DM_IB_TABLEINFO_CT_BIGINT :
                    fret = cur_row.GetInt64(col_id, int_v);
                    if (!fret) {
                        fret = packs[col_id].PutValueI(lineid, int_v);
                    }
                    break;
                case DM_IB_TABLEINFO_CT_FLOAT :
                case DM_IB_TABLEINFO_CT_DOUBLE :
                    fret = cur_row.GetDouble(col_id, double_v);
                    if (!fret) {
                        fret = packs[col_id].PutValueD(lineid, double_v);
                    }
                    break;
                case DM_IB_TABLEINFO_CT_CHAR :
                case DM_IB_TABLEINFO_CT_VARCHAR :
                    fret = cur_row.GetString(col_id, str_v, str_vlen);
                    if (!fret) {
                        fret = packs[col_id].PutValueS(lineid, str_v, str_vlen);
                    }
                    break;
                case DM_IB_TABLEINFO_CT_DATETIME :
                    fret = cur_row.GetDateTime(col_id, int_v);
                    if (!fret) {
                        fret = packs[col_id].PutValueI(lineid, int_v);
                    }
                    break;
                default :
                    return EPROTOTYPE;
                }
            }
            if (fret) {
                DPRINTF("Save line %lld column %llu, %s\n", (long long)lineid,
                  (long long unsigned)col_id, CR_Class_NS::strerrno(fret));
                return fret;
            }
        }
        for (size_t i=0; i<pki_count; i++) {
            uint64_t index_col_id = this->_packindex_req[i];
            if (!cur_row.IsNull(index_col_id)) {
                pki_key_set[i].insert(cur_row.GetSortAbleColKey(index_col_id, false));
            }
        }
    }

    for (size_t i=0; i<pki_count; i++) {
        for (auto it=pki_key_set[i].cbegin(); it!=pki_key_set[i].cend(); it++) {
            const std::string &key_tmp = *it;
            this->_pki_array[i].PushOne(cur_pack_id, key_tmp.data(), key_tmp.size(), NULL, 0);
        }
    }

    std::vector<std::thread*> tp_arr;
    std::vector<int> fret_arr;

    tp_arr.resize(col_count);
    fret_arr.resize(col_count);

    if (!this->_riakp) {
        size_t cpu_idle_count = CR_Class_NS::get_cpu_count() + 0.5;
        if (cpu_idle_count < 1)
            cpu_idle_count = 1;
        this->_compress_sem.setmaxvalue(cpu_idle_count);
        for (size_t i=0; i<col_count; i++) {
            tp_arr[i] = new std::thread(_do_save_pack, &(packs[i]), &(fret_arr[i]),
              &(this->_pack_id_list_arr[i]), this->_pak_fd_array[i], this->_pack_file_id_array[i],
              &(this->_compress_sem), &(this->_file_sem));
        }
    } else {
        this->_compress_sem.setmaxvalue(col_count);
        for (size_t i=0; i<col_count; i++) {
            tp_arr[i] = new std::thread(_do_save_pack, &(packs[i]), &(fret_arr[i]),
              &(this->_pack_id_list_arr[i]), -1, 0, &(this->_compress_sem), &(this->_file_sem));
        }
    }

    for (size_t i=0; i<col_count; i++) {
        tp_arr[i]->join();
        delete tp_arr[i];
        if (fret_arr[i]) {
            DPRINTF("Save pack [%lld ~ %lld] column %llu, %s\n",
              (long long)lineid_left, (long long)lineid_right,
              (long long unsigned)i, CR_Class_NS::strerrno(fret_arr[i]));
            ret = fret_arr[i];
        }
        if (!this->_riakp) {
            if (!ret && (CR_File::lseek(this->_pak_fd_array[i], 0, SEEK_CUR) > INT32_MAX)) {
                char str_buf[PATH_MAX];
                CR_File::close(this->_pak_fd_array[i]);
                this->_pack_file_id_array[i]++;
                snprintf(str_buf, sizeof(str_buf), "%s/S%s-N%05u-A%05u-B%05u.pck",
                  this->_tmp_pathname.c_str(), this->_job_id.c_str(), (unsigned)this->_striping_id,
                  (unsigned)i, (unsigned)this->_pack_file_id_array[i]);
                int fd_tmp = CR_File::open(str_buf, O_WRONLY|O_CREAT|O_TRUNC, 0644);
                if (fd_tmp < 0) {
                    DPRINTF("open(\"%s\"), %s\n", str_buf, CR_Class_NS::strerrno(errno));
                    return errno;
                }
                this->_pak_fd_array[i] = fd_tmp;
            }
        }
    }

    return ret;
}

int
DM_IB_BlockConverter::Finish(std::vector<uint64_t> &result_arr, const std::vector<int64_t> *ksi_data_p)
{
    int fret = 0;
    size_t col_count = this->_table_info.size();
    size_t pki_count = this->_packindex_req.size();
    std::vector<uint64_t> tmp_result_arr;
    char str_buf[PATH_MAX];
    CR_FixedLinearStorage_NS::fls_infos_t sorter_infos;
    uint64_t sorter_lines_poped;

    tmp_result_arr.resize(col_count, 0);

    DPRINTF("Save Pack-Index files\n");

    for (size_t i=0; i<pki_count; i++) {
        CR_FixedLinearStorage_NS::ClearInfos(sorter_infos);
        this->_pki_array[i].PushOne(0, NULL, 0, NULL, 0);
        this->_pki_array[i].GetInfos(sorter_infos);
        if (this->_pki_use_multimode) {
            CR_FixedLinearStorage sort_out_fls(1024 * 1024 * 32, CR_FLS_SIZEMODE_MIDDLE);
            int64_t pack_id;
            std::string pki_key;
            std::string _value_tmp;
            bool _bool_tmp;
            bool is_first_key = true;
            std::string pki_last_key;
            uint32_t pki_last_packid = UINT32_C(0xFFFFFFFF);
            std::vector<uint32_t> pki_idx_array;
            uint64_t pki_line_count = 0;
            while (1) {
                fret = this->_pki_array[i].PopOne(pack_id, pki_key, _value_tmp, _bool_tmp);
                if (fret == EAGAIN) {
                    fret = _push_pki_line(sort_out_fls, pki_idx_array, pki_last_key,
                      this->_job_id, this->_pki_fd_array[i], pki_line_count);
                    if (fret)
                        return fret;
                    if (sort_out_fls.GetTotalLines() > 0) {
                        fret = sort_out_fls.SaveToFileDescriptor(this->_pki_fd_array[i]);
                        if (fret) {
                            DPRINTF("sort_out_fls.SaveToFileDescriptor, %s\n", CR_Class_NS::strerrno(fret));
                            return fret;
                        }
                        sort_out_fls.Clear();
                    }
                    tmp_result_arr[this->_packindex_req[i]] = pki_line_count;
                    snprintf(str_buf, sizeof(str_buf), "%s/S%s-N%05u-A%05u.pkiv",
                      this->_tmp_pathname.c_str(), this->_job_id.c_str(),
                      (unsigned)this->_striping_id, (unsigned)this->_packindex_req[i]);
                    CR_Class_NS::save_string(str_buf, CR_Class_NS::u642str(pki_line_count), 0644);
                    break;
                } else if (fret) {
                    DPRINTF("_pki_array[%u].PopOne, %s\n", (unsigned)i, CR_Class_NS::strerrno(fret));
                    return fret;
                }
                if (is_first_key || (pki_key == pki_last_key)) {
                    if ((uint32_t)pack_id != pki_last_packid) {
                        pki_idx_array.push_back((uint32_t)pack_id);
                    }
                    if (is_first_key) {
                        is_first_key = false;
                    }
                    pki_last_packid = pack_id;
                } else {
                    fret = _push_pki_line(sort_out_fls, pki_idx_array, pki_last_key,
                      this->_job_id, this->_pki_fd_array[i], pki_line_count);
                    if (fret)
                        return fret;
                    pki_idx_array.push_back((uint32_t)pack_id);
                }
                pki_last_key = pki_key;
            }
        } else {
            CR_FixedLinearStorage sort_out_fls(1024 * 1024 * 32, CR_FLS_SIZEMODE_SMALL);
            while (1) {
                sort_out_fls.Clear(true);
                fret = this->_pki_array[i].Pop(sort_out_fls);
                if (!fret) {
                    sort_out_fls.SaveToFileDescriptor(this->_pki_fd_array[i]);
                } else {
                    break;
                }
            }
        }
        sorter_lines_poped = this->_pki_array[i].GetTotalLinesPoped();
        DPRINTF("Total %lu lines in Pack-Index sorter, %lu lines poped, %s\n",
          (long unsigned)sorter_lines_poped, (long unsigned)sorter_infos.total_lines,
          (sorter_lines_poped==sorter_infos.total_lines)?("EQ"):("NE"));
    }

    DPRINTF("Save RSI files\n");

    for (size_t i=0; i<col_count; i++) {
        fret = this->_rsi_array[i].Save(this->_rsi_fd_array[i]);
        if (fret) {
            DPRINTF("Save column %llu RSI, %s\n", (long long unsigned)i, CR_Class_NS::strerrno(fret));
            return fret;
        }
    }

    DPRINTF("Save DPN files\n");

    for (size_t i=0; i<col_count; i++) {
        fret = this->_dpn_array[i].Save(this->_dpn_fd_array[i]);
        if (fret) {
            DPRINTF("Save column %llu DPN, %s\n", (long long unsigned)i, CR_Class_NS::strerrno(fret));
            return fret;
        }
    }

    if (this->_riakp && ksi_data_p) {
        DPRINTF("Save KSI file\n");
        fret = CR_File::write(this->_ksi_fd, ksi_data_p->data(), sizeof(int64_t) * ksi_data_p->size());
    }

    this->close_all_fd();

    DPRINTF("Copy files to dst-path\n");

    std::string cmd_str;
    cmd_str = "yes n 2> /dev/null | mv -i ";
    cmd_str += this->_tmp_pathname;
    cmd_str += "/* ";
    cmd_str += this->_dst_pathname;
    cmd_str += "/ > /dev/null 2>&1";

    fret = CR_Class_NS::system(cmd_str.c_str());
    if (fret == -1) {
        DPRINTF("system(\"%s\"), %s\n", cmd_str.c_str(), CR_Class_NS::strerrno(errno));
        return errno;
    } else {
        int child_ret = WEXITSTATUS(fret);
        if (child_ret) {
            DPRINTF("system(\"%s\") == 0, but child return %s\n",
              cmd_str.c_str(), CR_Class_NS::strerrno(child_ret));
            return child_ret;
        }
    }

    result_arr.insert(result_arr.end(), tmp_result_arr.begin(), tmp_result_arr.end());

    DPRINTF("Copy files to dst-path done\n");

    return 0;
}

int
DM_IB_BlockConverter::SetFileThreadCount(size_t tc)
{
    if (tc < 1)
        tc = 1;

    return this->_file_sem.setmaxvalue(tc);
}

int
_push_pki_line(CR_FixedLinearStorage &sort_out_fls, std::vector<uint32_t> &pki_idx_array,
    const std::string &pki_last_key, const std::string &job_id, int pki_fd, uint64_t &pki_line_count)
{
    while (pki_idx_array.size() > 0) {
        int fret = sort_out_fls.PushBack(pki_idx_array[0], pki_last_key.data(),
          pki_last_key.size(), pki_idx_array.data(), (pki_idx_array.size() * sizeof(uint32_t)));
        if (fret == ENOBUFS) {
            fret = sort_out_fls.SaveToFileDescriptor(pki_fd);
            if (fret) {
                DPRINTF("sort_out_fls.SaveToFileDescriptor, %s\n", CR_Class_NS::strerrno(fret));
                return fret;
            }
            sort_out_fls.Clear();
        } else if (fret) {
            DPRINTF("sort_out_fls.PushBack, %s\n", CR_Class_NS::strerrno(fret));
            return fret;
        } else {
            pki_idx_array.clear();
            pki_line_count++;
            break;
        }
    }

    return 0;
}
