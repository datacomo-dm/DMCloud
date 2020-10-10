#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <thread>
#include <cr_class/cr_class.h>
#include <cr_class/cr_addon.h>
#include <cr_class/cr_autoid.h>
#include <cr_class/cr_class.pb.h>
#include <cr_class/cr_quickhash.h>
#include <cr_class/cr_externalsort.h>
#include <cr_class/cr_blockqueue.h>
#include <cfm_drv/dm_ib_sort_load.h>
#include <cfm_drv/dm_ib_tableinfo.h>
#include <cfm_drv/dm_ib_rowdata.h>
#include "cfm-toolbox.h"

//////////////////////

static void help_out(int argc, char **argv);
static void _key_maker(DM_IB_TableInfo *table_info_p,
    CR_BlockQueue<CR_FixedLinearStorage*> *queue_p, int64_t line_count);

//////////////////////

const char *string_arr[] = {
    "123456", "234567", "345678", "456789", "567890",
    "Qwertyui", "Wertyuio", "Ertyuiop", "Rtyuiop[", "Tyuiop[]",
    "Asdfghjkl;", "Sdfghjkl;'", "Dfghjkl;'a", "Fghjkl;'as", "Ghjkl;'asd",
    "Zxcvbnm,./zx", "Xcvbnm,./zxc", "Cvbnm,./zxcv", "Vbnmzxcvbnm,", "Bnmzxcvbnm,."
};

//////////////////////

static void
help_out(int argc, char **argv)
{
    fprintf(stderr,
      "Usage: %s %s"
      " <last_job_id> <job_id> <col_def_str> <line_count> <lineid_begin|-1==slave>"
      " <block_lines> <key_maker_thread_count>"
      " [sort_part_file_size] [slice_size] [max_slice_size] [sort_thread_count] [disk_thread_count]"
      " [merge_thread_count] [merge_part_count] [compress_data_file] [enable_preload] [column_comment]"
      "\n"
      , argv[0], argv[1]
    );

    exit(1);
}

static void
_key_maker(DM_IB_TableInfo *table_info_p, CR_BlockQueue<CR_FixedLinearStorage*> *queue_p, int64_t line_count)
{
    int fret = 0;
    int rand_val;
    std::string k_tmp, v_tmp;
    DM_IB_RowData row_data;
    int col_count = table_info_p->size();
    CR_FixedLinearStorage *slice_fls_p = NULL;

    slice_fls_p = new CR_FixedLinearStorage(1000000);

    row_data.BindTableInfo(*table_info_p);

    DPRINTFX(15, "Make %lld lines data\n", (long long)line_count);
    for (int64_t line_id=0; line_id<line_count; line_id++) {
        rand_val = rand();
        k_tmp = CR_Class_NS::i322str(rand_val);
        for (int col_id=0; col_id<col_count; col_id++) {
            switch ((*table_info_p)[col_id].column_type) {
            case DM_IB_TABLEINFO_CT_BIGINT :
                fret = row_data.SetInt64(col_id, line_id + col_id);
                break;
            case DM_IB_TABLEINFO_CT_DOUBLE :
                fret = row_data.SetDouble(col_id, (double)(line_id + col_id));
                break;
            case DM_IB_TABLEINFO_CT_DATETIME :
                fret = row_data.SetDateTime(col_id, DM_IB_RowData_NS::mkibdate(
                  2000 + col_id, (line_id + col_id) % 12, (line_id + col_id) % 28,
                  (line_id + col_id) % 24, (line_id + col_id) % 60, (line_id + col_id) % 60));
                break;
            case DM_IB_TABLEINFO_CT_VARCHAR :
              {
                const char *s_tmp = string_arr[(line_id + col_id) % 20];
                fret = row_data.SetString(col_id, s_tmp, strlen(s_tmp));
                break;
              }
            case DM_IB_TABLEINFO_CT_CHAR :
                v_tmp = CR_Class_NS::i322str(rand());
                fret = row_data.SetString(col_id, v_tmp.data(), v_tmp.size());
                break;
            case DM_IB_TABLEINFO_CT_FLOAT :
                fret = row_data.SetNull(col_id);
                break;
            default :
                fret = row_data.SetNull(col_id);
                break;
            }
            if (fret) {
                DPRINTF("row[%ld]col[%d] set failed[%s]\n",
                  (long)line_id, col_id, CR_Class_NS::strerrno(fret));
                exit(fret);
            }
        }
        if (fret) {
            DPRINTF("row_data.Set failed[%s]\n", CR_Class_NS::strerrno(fret));
            break;
        }
        const void *data_p;
        size_t data_size;
        fret = row_data.Data(data_p, data_size);
        if (fret) {
            DPRINTF("row_data.SerializeToString failed[%s]\n", CR_Class_NS::strerrno(fret));
            break;
        }
        fret = slice_fls_p->PushBack(line_id, k_tmp.data(), k_tmp.size(), data_p, data_size);
        if (fret == ENOBUFS) {
            fret = queue_p->push_back(slice_fls_p, 3600);
            if (fret) {
                DPRINTF("queue_p->push_back failed [%s]\n", CR_Class_NS::strerrno(fret));
                break;
            }
            slice_fls_p = new CR_FixedLinearStorage(1000000);
            fret = slice_fls_p->PushBack(line_id, k_tmp.data(), k_tmp.size(), data_p, data_size);
        } else if (fret) {
            DPRINTF("slice_fls_p->PushBack failed[%s]\n", CR_Class_NS::strerrno(fret));
            break;
        }
        row_data.Clear();
        if (line_id && (line_id % 0x10000 == 0))
            DPRINTFX(15, "Total 0x%016llX lines made\n", (long long)line_id);
    }
    if (line_count % 0x10000 != 0)
        DPRINTFX(15, "Total 0x%016llX lines made\n", (long long)line_count);
    if (slice_fls_p->GetTotalLines() > 0) {
        fret = queue_p->push_back(slice_fls_p, 3600);
        if (fret) {
            DPRINTF("queue_p->push_back failed [%s]\n", CR_Class_NS::strerrno(fret));
        }
    }
}

int
do_sltest(int argc, char *argv[])
{
    if (argc < 9)
        help_out(argc, argv);

    int fret;
    std::string last_job_id = argv[2];
    std::string job_id = argv[3];
    std::string col_def_str = argv[4];
    int64_t line_count = atoll(argv[5]);
    int64_t lineid_begin = atoll(argv[6]);
    size_t block_lines = atoll(argv[7]);
    int key_maker_thread_count = atoi(argv[8]);

    size_t memory_size = (1024 * 1024 * 100);
    if (argc > 9)
        memory_size = atoll(argv[9]);
    if (memory_size < (1024 * 1024 * 100))
        memory_size = (1024 * 1024 * 100);
    size_t slice_size = (1024 * 1024);
    if (argc > 10)
        slice_size = atoll(argv[10]);
    if (slice_size < (1024 * 1024))
        slice_size = (1024 * 1024);
    size_t max_slice_size = (1024 * 1024 * 16);
    if (argc > 11)
        max_slice_size = atoll(argv[11]);
    if (max_slice_size < (1024 * 1024 * 16))
        max_slice_size = (1024 * 1024 * 16);
    int sort_thread_count = 1;
    if (argc > 12)
        sort_thread_count = atoi(argv[12]);
    int disk_thread_count = 1;
    if (argc > 13)
        disk_thread_count = atoi(argv[13]);
    int merge_thread_count = 1;
    if (argc > 14)
        merge_thread_count = atoi(argv[14]);
    size_t merge_part_count = 16;
    if (argc > 15)
        merge_part_count = atoi(argv[15]);
    int compress_data_file = 1;
    if (argc > 16)
        compress_data_file = atoi(argv[16]);
    int enable_preload = 1;
    if (argc > 17)
        enable_preload = atoi(argv[17]);
    std::string col_comment;
    if (argc > 18)
        col_comment = argv[18];
    std::string sort_args;
    std::string load_args;
    std::string query_cmd_str;
    std::vector<int64_t> line_count_arr;
    std::string table_info_str;
    std::string save_pathname_remote;
    bool do_keep_partfile = false;
    char *target_path_p = getenv(CFM_TB_EN_LOAD_SORTED_DATA_DIR);
    char *riak_connect_str_p = getenv("RIAK_HOSTNAME");
    std::string riak_connect_str;
    std::string riak_bucket = "IB_DATAPACKS";

    if (riak_connect_str_p)
        riak_connect_str = riak_connect_str_p;

    if (job_id[0] == '_') {
        do_keep_partfile = true;
    }

    if (target_path_p) {
        save_pathname_remote = target_path_p;
    } else {
        DPRINTFX(0, "Must set env-var %s\n", CFM_TB_EN_LOAD_SORTED_DATA_DIR);
        return EINVAL;
    }

    CR_BlockQueue<CR_FixedLinearStorage*> queue;
    DM_IB_TableInfo table_info;

    queue.set_args(128);

    for (size_t col_id=0; col_id<col_def_str.size(); col_id++) {
        switch (col_def_str[col_id]) {
        case 'I' :
            table_info.push_back(std::string("C_BIGINT") + CR_Class_NS::u162str(col_id),
              DM_IB_TABLEINFO_CT_BIGINT, 8, 0, col_comment);
            break;
        case 'F' :
            table_info.push_back(std::string("C_DOUBLE_") + CR_Class_NS::u162str(col_id),
              DM_IB_TABLEINFO_CT_DOUBLE, 8, 0, col_comment);
            break;
        case 'D' :
            table_info.push_back(std::string("C_DATETIME_") + CR_Class_NS::u162str(col_id),
              DM_IB_TABLEINFO_CT_DATETIME, 8, 0, col_comment);
            break;
        case 'S' :
            table_info.push_back(std::string("C_VARCHAR_") + CR_Class_NS::u162str(col_id),
              DM_IB_TABLEINFO_CT_VARCHAR, 256, 0, col_comment);
            break;
        case 'R' :
            table_info.push_back(std::string("C_RANDOM_") + CR_Class_NS::u162str(col_id),
              DM_IB_TABLEINFO_CT_CHAR, 8, 0, col_comment);
            break;
        case 'N' :
            table_info.push_back(std::string("C_NULL_") + CR_Class_NS::u162str(col_id),
              DM_IB_TABLEINFO_CT_FLOAT, 4, 0, col_comment);
            break;
        default :
            table_info.push_back(std::string("C_INT") + CR_Class_NS::u162str(col_id),
              DM_IB_TABLEINFO_CT_INT, 4, 0, col_comment);
            break;
        }
    }

    table_info.SerializeToString(table_info_str);

    if (key_maker_thread_count < 1)
        key_maker_thread_count = 1;

    if (key_maker_thread_count > 8)
        key_maker_thread_count = 8;

    DPRINTF("Enter\n");

    std::list<std::thread*> km_tp_list;

    std::vector<uint64_t> packindex_arr;
    packindex_arr.clear();
    packindex_arr.push_back(0);

    CR_FixedLinearStorage *slice_fls_p = NULL;

    sort_args = CR_Class_NS::str_setparam(sort_args,
      DMIBSORTLOAD_PN_LAST_JOB_ID, last_job_id);
    sort_args = CR_Class_NS::str_setparam_u64(sort_args,
      EXTERNALSORT_ARGNAME_ROUGHFILESIZE, memory_size);
    sort_args = CR_Class_NS::str_setparam_u64(sort_args,
      EXTERNALSORT_ARGNAME_ROUGHSLICESIZE, slice_size);
    sort_args = CR_Class_NS::str_setparam_u64(sort_args,
      EXTERNALSORT_ARGNAME_MAXSLICESIZE, max_slice_size);
    sort_args = CR_Class_NS::str_setparam_bool(sort_args,
      EXTERNALSORT_ARGNAME_ENABLECOMPRESS, compress_data_file);
    sort_args = CR_Class_NS::str_setparam_bool(sort_args,
      EXTERNALSORT_ARGNAME_ENABLEPRELOAD, enable_preload);
    sort_args = CR_Class_NS::str_setparam_u64(sort_args,
      EXTERNALSORT_ARGNAME_MAXSORTTHREADS, sort_thread_count);
    sort_args = CR_Class_NS::str_setparam_u64(sort_args,
      EXTERNALSORT_ARGNAME_MAXDISKTHREADS, disk_thread_count);
    sort_args = CR_Class_NS::str_setparam_u64(sort_args,
      EXTERNALSORT_ARGNAME_MAXMERGETHREADS, merge_thread_count);
    sort_args = CR_Class_NS::str_setparam_u64(sort_args,
      EXTERNALSORT_ARGNAME_MERGECOUNTMIN, merge_part_count);
    sort_args = CR_Class_NS::str_setparam_u64(sort_args,
      EXTERNALSORT_ARGNAME_MERGECOUNTMAX, merge_part_count);
    sort_args = CR_Class_NS::str_setparam_i64(sort_args,
      EXTERNALSORT_ARGNAME_DEFAULTSIZEMODE, CR_FLS_SIZEMODE_LARGE);
    sort_args = CR_Class_NS::str_setparam_u64(sort_args,
      DMIBSORTLOAD_PN_FIFO_SIZE, 1024 * 1024 * 256);
    sort_args = CR_Class_NS::str_setparam_bool(sort_args,
      DMIBSORTLOAD_PN_KEEP_SORT_TMP, do_keep_partfile);

    sort_args = CR_Class_NS::str_setparam(sort_args, DMIBSORTLOAD_PN_RIAK_CONNECT_STR, riak_connect_str);
    sort_args = CR_Class_NS::str_setparam(sort_args, DMIBSORTLOAD_PN_RIAK_BUCKET, riak_bucket);

    double enter_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

    DMIBSortLoad *sortload_p = NULL;

    if (!sortload_p)
        sortload_p = new DMIBSortLoad(job_id, (lineid_begin >= 0)?true:false, &sort_args);

    for (int i=0; i<key_maker_thread_count; i++) {
        int64_t cur_line_count = line_count / key_maker_thread_count;
        if (i == 0) {
            cur_line_count += line_count % key_maker_thread_count;
        }
        std::thread *new_tp = new std::thread(_key_maker, &table_info, &queue, cur_line_count);
        km_tp_list.push_back(new_tp);
    }

    int64_t total_lines = 0;
    int64_t total_lines_next_step = 0x100000;

    while (1) {
        fret = queue.pop_front(slice_fls_p);
        if (!fret) {
            total_lines += slice_fls_p->GetTotalLines();
            fret = sortload_p->PushLines(*slice_fls_p);
            delete slice_fls_p;
            if (fret) {
                DPRINTF("sortload_p->PushLines failed [%s]\n", CR_Class_NS::strerrno(fret));
                break;
            }
            if (total_lines > total_lines_next_step) {
                DPRINTF("Total 0x%016llX lines pushed\n", (long long)total_lines_next_step);
                total_lines_next_step += 0x100000;
            }
        } else if (fret == EAGAIN) {
            if (total_lines >= line_count) {
                DPRINTF("Total 0x%016llX lines pushed\n", (long long)total_lines);
                fret = 0;
                break;
            } else {
                usleep(1000);
                continue;
            }
        } else {
            break;
        }
    }

    if (!fret) {
        DPRINTF("%llu lines push ok\n", (long long unsigned)total_lines);

        if (riak_connect_str.size() > 0) {
            DPRINTF("riak connect string detected, data will save to riak\n");
        }

        load_args.clear();
        load_args = CR_Class_NS::str_setparam(load_args, LIBIBLOAD_PN_TABLE_INFO, table_info_str);
        load_args = CR_Class_NS::str_setparam(load_args, LIBIBLOAD_PN_SAVE_PATHNAME, save_pathname_remote);
        load_args = CR_Class_NS::str_setparam_u64(load_args, DMIBSORTLOAD_PN_BLOCK_LINES, block_lines);
        load_args = CR_Class_NS::str_setparam_i64(load_args, DMIBSORTLOAD_PN_LINEID_BEGIN, lineid_begin);
        load_args = CR_Class_NS::str_setparam_u64arr(load_args, LIBIBLOAD_PN_PACKINDEX_INFO, packindex_arr);
        load_args = CR_Class_NS::str_setparam_bool(load_args, LIBIBLOAD_PN_PACKINDEX_MULTIMODE, true);
        load_args = CR_Class_NS::str_setparam_u64(load_args, DMIBSORTLOAD_PN_FIFO_SIZE, 1024 * 1024 * 256);
        load_args = CR_Class_NS::str_setparam_u64(load_args, LIBIBLOAD_PN_FILETHREAD_COUNT, disk_thread_count);

        fret = sortload_p->Finish(load_args);
        if (fret > 0) {
            DPRINTF("sortload_p->Finish() OK[%d]\n", fret);
            fret = 0;
        } else if (fret < 0) {
            DPRINTF("sortload_p->Finish() failed[%s]\n", CR_Class_NS::strerrno(errno));
            fret = errno;
        }
    } else {
        DMIBSortLoad_NS::EmergencyStop(job_id);
    }

    if (sortload_p) {
        delete sortload_p;
        sortload_p = NULL;
    }

    if (lineid_begin >= 0) {
        printf("SORT/LOAD usage time: %f\n", CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - enter_time);
    }

    while (!km_tp_list.empty()) {
        std::thread *tp = km_tp_list.front();
        km_tp_list.pop_front();
        tp->join();
        delete tp;
    }

    return fret;
}
