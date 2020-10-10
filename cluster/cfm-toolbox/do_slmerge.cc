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
#include <cr_class/cr_unlimitedfifo.h>
#include <cfm_drv/dm_ib_sort_load.h>
#include <cfm_drv/dm_ib_tableinfo.h>
#include <cfm_drv/dm_ib_rowdata.h>
#include "cfm-toolbox.h"

//////////////////////

static void help_out(int argc, char **argv);

//////////////////////

static void
help_out(int argc, char **argv)
{
    fprintf(stderr,
      "Usage: %s %s"
      " <job_id> <col_def_str> <lineid_begin> <block_lines>"
      " [<load_job_id#0> <total_lines#0> [<load_job_id#1> <total_lines#1> ...]]\n"
      , argv[0], argv[1]
    );

    exit(1);
}

int
do_slmerge(int argc, char *argv[])
{
    if (argc < 6)
        help_out(argc, argv);

    int fret = 0;
    std::string job_id = argv[2];
    std::string col_def_str = argv[3];
    int64_t lineid_begin = atoll(argv[4]);
    int64_t block_lines = atoll(argv[5]);
    int64_t total_lines;

    std::string sort_args;
    std::string load_args;
    std::string query_cmd_str;
    std::string table_info_str;
    std::string save_pathname_remote;
    char *target_path_p = getenv(CFM_TB_EN_LOAD_SORTED_DATA_DIR);

    if (target_path_p) {
        save_pathname_remote = target_path_p;
    } else {
        DPRINTFX(0, "Must set env-var %s\n", CFM_TB_EN_LOAD_SORTED_DATA_DIR);
        return EINVAL;
    }

    DM_IB_TableInfo table_info;

    for (size_t col_id=0; col_id<col_def_str.size(); col_id++) {
        switch (col_def_str[col_id]) {
        case 'I' :
            table_info.push_back(std::string("C_BIGINT") + CR_Class_NS::u162str(col_id),
              DM_IB_TABLEINFO_CT_BIGINT, 8);
            break;
        case 'F' :
            table_info.push_back(std::string("C_DOUBLE_") + CR_Class_NS::u162str(col_id),
              DM_IB_TABLEINFO_CT_DOUBLE, 8);
            break;
        case 'D' :
            table_info.push_back(std::string("C_DATETIME_") + CR_Class_NS::u162str(col_id),
              DM_IB_TABLEINFO_CT_DATETIME, 8);
            break;
        case 'S' :
            table_info.push_back(std::string("C_VARCHAR_") + CR_Class_NS::u162str(col_id),
              DM_IB_TABLEINFO_CT_VARCHAR, 256);
            break;
        default :
            table_info.push_back(std::string("C_INT") + CR_Class_NS::u162str(col_id),
              DM_IB_TABLEINFO_CT_INT, 4);
            break;
        }
    }

    table_info.SerializeToString(table_info_str);

    DPRINTF("Enter\n");

    std::vector<uint64_t> packindex_arr;
    packindex_arr.clear();
    packindex_arr.push_back(1);

    sort_args = CR_Class_NS::str_setparam_bool(sort_args,
      EXTERNALSORT_ARGNAME_ENABLECOMPRESS, true);
    sort_args = CR_Class_NS::str_setparam_bool(sort_args,
      EXTERNALSORT_ARGNAME_ENABLEPRELOAD, false);
    sort_args = CR_Class_NS::str_setparam_u64(sort_args,
      EXTERNALSORT_ARGNAME_MAXMERGETHREADS, 0);
    sort_args = CR_Class_NS::str_setparam_u64(sort_args,
      DMIBSORTLOAD_PN_FIFO_SIZE, 1024 * 1024 * 256);
    sort_args = CR_Class_NS::str_setparam_u64(sort_args,
      DMIBSORTLOAD_PN_TIMEOUT_PROC, 86400);

    double enter_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

    DMIBSortLoad *sortload_p = NULL;

    if (!sortload_p)
        sortload_p = new DMIBSortLoad(job_id, true, &sort_args);

    CR_FixedLinearStorage tmp_fls(65536);

    for (int i=6; i<argc; i+=2) {
        tmp_fls.Clear();
        tmp_fls.SetTag(CR_Class_NS::str_setparam("", DMIBSORTLOAD_PN_QUERY_CMD, LIBSORT_PV_APPEND_PART_FILES));
        total_lines = atoll(argv[i+1]);
        tmp_fls.PushBack(total_lines, std::string(argv[i]), NULL);
        fret = sortload_p->PushLines(tmp_fls);
        DPRINTF("Merge job [\"%s\"], %s\n", argv[i], CR_Class_NS::strerrno(fret));
        if (fret) {
            break;
        }
    }

    if (!fret) {
        load_args.clear();
        load_args = CR_Class_NS::str_setparam(load_args, LIBIBLOAD_PN_TABLE_INFO, table_info_str);
        load_args = CR_Class_NS::str_setparam(load_args, LIBIBLOAD_PN_SAVE_PATHNAME, save_pathname_remote);
        load_args = CR_Class_NS::str_setparam_u64(load_args, DMIBSORTLOAD_PN_BLOCK_LINES, block_lines);
        load_args = CR_Class_NS::str_setparam_i64(load_args, DMIBSORTLOAD_PN_LINEID_BEGIN, lineid_begin);
        load_args = CR_Class_NS::str_setparam_u64arr(load_args, LIBIBLOAD_PN_PACKINDEX_INFO, packindex_arr);
        load_args = CR_Class_NS::str_setparam_bool(load_args, LIBIBLOAD_PN_PACKINDEX_MULTIMODE, true);
        load_args = CR_Class_NS::str_setparam_u64(load_args, DMIBSORTLOAD_PN_FIFO_SIZE, 1024 * 1024 * 256);

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

    return fret;
}
