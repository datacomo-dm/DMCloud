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
static void _key_maker(DM_IB_TableInfo *table_info_p, int64_t line_count);

//////////////////////

static void
help_out(int argc, char **argv)
{
    fprintf(stderr,
      "Usage: %s %s <line_count> <col_def_str> <key_maker_thread_count>\n"
      , argv[0], argv[1]
    );

    exit(1);
}

static void
_key_maker(DM_IB_TableInfo *table_info_p, int64_t line_count)
{
    int fret;
    std::string v_tmp, l_tmp;
    DM_IB_RowData row_data;
    int col_count = table_info_p->size();
    CR_FixedLinearStorage slice_fls;

    slice_fls.ReSize(16000000);

    row_data.BindTableInfo(*table_info_p);
    v_tmp.reserve(32);

    DPRINTF("Make %lld lines (%d columns) data\n", (long long)line_count, col_count);
    for (int64_t line_id=0; line_id<line_count; line_id++) {
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
                size_t s_len = strlen(s_tmp);
                fret = row_data.SetString(col_id, s_tmp, s_len);
                break;
              }
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
        const void *data_p;
        size_t data_size;
        row_data.Data(data_p, data_size);
        fret = slice_fls.PushBack(line_id, data_p, data_size, NULL, 0);
        if (fret) {
            slice_fls.Clear();
            slice_fls.PushBack(line_id, data_p, data_size, NULL, 0);
        }
        row_data.Clear();
    }
    DPRINTF("Total %lld lines made\n", (long long)line_count);
}

int
do_perftest(int argc, char *argv[])
{
    if (argc < 5)
        help_out(argc, argv);

    int64_t line_count = atoll(argv[2]);
    std::string col_def_str = argv[3];
    int key_maker_thread_count = atoi(argv[4]);

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

    if (key_maker_thread_count < 1)
        key_maker_thread_count = 1;

    if (key_maker_thread_count > 8)
        key_maker_thread_count = 8;

    DPRINTF("Enter\n");

    std::list<std::thread*> km_tp_list;

    for (int i=0; i<key_maker_thread_count; i++) {
        int64_t cur_line_count = line_count / key_maker_thread_count;
        if (i == 0) {
            cur_line_count += line_count % key_maker_thread_count;
        }
        std::thread *new_tp = new std::thread(_key_maker, &table_info, cur_line_count);
        km_tp_list.push_back(new_tp);
    }

    while (!km_tp_list.empty()) {
        std::thread *tp = km_tp_list.front();
        km_tp_list.pop_front();
        tp->join();
        delete tp;
    }

    return 0;
}
