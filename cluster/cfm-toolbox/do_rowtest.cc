#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_addon.h>
#include <cr_class/cr_timer.h>
#include <cr_class/cr_fixedlinearstorage.h>
#include <cfm_drv/dm_ib_tableinfo.h>
#include <cfm_drv/dm_ib_rowdata.h>
#include "cfm-toolbox.h"

//////////////////////

static void help_out(int argc, char **argv);
static void _row_encoder(int fls_fd, DM_IB_TableInfo *table_info_p, int64_t line_count);
static void _row_decoder(int fls_fd, DM_IB_TableInfo *table_info_p);

//////////////////////

static void
help_out(int argc, char **argv)
{
    fprintf(stderr,
      "Usage: %s %s <col_def_str> <line_count>\n"
      , argv[0], argv[1]
    );

    exit(1);
}

static void
_row_encoder(int fls_fd, DM_IB_TableInfo *table_info_p, int64_t line_count)
{
    int fret = 0;
    DM_IB_RowData row_data;
    std::string s_tmp;
    std::string v_tmp;
    CR_FixedLinearStorage slice_fls(16000000);
    int col_count = table_info_p->size();
    uint64_t total_row_size = 0;

    row_data.BindTableInfo(*table_info_p);
    s_tmp.reserve(100);

    CR_Timer set_row_timer;
    CR_Timer serialize_timer;

    DPRINTF("Make %lld lines data\n", (long long)line_count);
    for (int64_t line_id=0; line_id<line_count; line_id++) {
        set_row_timer.Start();
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
        set_row_timer.Stop();
        const void *data_p;
        size_t data_size;
        serialize_timer.Start();
        fret = row_data.Data(data_p, data_size);
        serialize_timer.Stop();
        if (fret) {
            DPRINTF("row_data.SerializeToString failed[%s]\n", CR_Class_NS::strerrno(fret));
            exit(fret);
        }
        total_row_size += v_tmp.size();
        fret = slice_fls.PushBack(line_id, data_p, data_size, NULL, 0);
        if (fret == ENOBUFS) {
            fret = slice_fls.SaveToFileDescriptor(fls_fd);
            if (fret) {
                DPRINTF("slice_fls.SaveToFileDescriptor failed [%s]\n", CR_Class_NS::strerrno(fret));
                exit(fret);
            }
            slice_fls.Clear();
            fret = slice_fls.PushBack(line_id, data_p, data_size, NULL, 0);
        } else if (fret) {
            DPRINTF("slice_fls.PushBack failed[%s]\n", CR_Class_NS::strerrno(fret));
            exit(fret);
        }
        row_data.Clear();
        if (line_id && (line_id % 0x10000 == 0))
            DPRINTF("Total 0x%016llX lines made\n", (long long)line_id);
    }

    slice_fls.SaveToFileDescriptor(fls_fd);

    if (line_count % 0x10000 != 0) {
        DPRINTF("Total 0x%016llX lines made, avg row size %lu\n", (long long)line_count,
          (unsigned long)(total_row_size / line_count));
    }

    fprintf(stderr, "set_row_timer  : %s\n", set_row_timer.DebugString().c_str());
    fprintf(stderr, "serialize_timer: %s\n", serialize_timer.DebugString().c_str());
}

static void
_row_decoder(int fls_fd, DM_IB_TableInfo *table_info_p)
{
    int fret = 0;
    DM_IB_RowData row_data;
    CR_FixedLinearStorage slice_fls;
    std::string v_tmp;
    int col_count = table_info_p->size();
    int64_t line_id = 0;
    int64_t no_bound_line_count = 0;
    int64_t no_bound_str_count = 0;

    row_data.BindTableInfo(*table_info_p);

    CR_Timer get_row_timer;
    CR_Timer parse_timer;

    while (1) {
        fret = slice_fls.LoadFromFileDescriptor(fls_fd);
        if (fret)
            break;

        size_t total_lines = slice_fls.GetTotalLines();

        for (size_t i=0; i<total_lines; i++) {
            int64_t _obj_id;
            const char *k_p;
            uint64_t k_size;
            const char *_v_p;
            uint64_t _v_size;
            fret = slice_fls.Query(i, _obj_id, k_p, k_size, _v_p, _v_size);
            if (fret) {
                DPRINTF("[%u]slice_fls.Query() failed, [%s]\n",
                  (unsigned)i, CR_Class_NS::strerrno(fret));
                exit(fret);
            }
            if (((uint64_t)k_p % 8) != 0)
                no_bound_line_count++;
            parse_timer.Start();
            fret = row_data.MapFromArray(k_p, k_size);
            parse_timer.Stop();
            if (fret) {
                DPRINTF("[%u]row_data.ParseFromString() failed, [%s]\n",
                  (unsigned)i, CR_Class_NS::strerrno(fret));
                exit(fret);
            }
            get_row_timer.Start();
            for (int col_id=0; col_id<col_count; col_id++) {
                switch ((*table_info_p)[col_id].column_type) {
                case DM_IB_TABLEINFO_CT_BIGINT :
                  {
                    int64_t int_v;
                    fret = row_data.GetInt64(col_id, int_v);
                    if (int_v != (line_id + col_id)) {
                        DPRINTF("R[%ld]C[%d] == %ld != %ld\n", (long)line_id, col_id,
                          (long)int_v, (long)(line_id + col_id));
                        exit(EBADSLT);
                    }
                    break;
                  }
                case DM_IB_TABLEINFO_CT_DOUBLE :
                  {
                    double double_v;
                    fret = row_data.GetDouble(col_id, double_v);
                    if (double_v != (double)(line_id + col_id)) {
                        DPRINTF("R[%ld]C[%d] == %f != %f\n", (long)line_id, col_id,
                          double_v, (double)(line_id + col_id));
                        exit(EBADSLT);
                    }
                    break;
                  }
                case DM_IB_TABLEINFO_CT_DATETIME :
                  {
                    int64_t ibdate_get;
                    int64_t ibdate_tm = DM_IB_RowData_NS::mkibdate(
                      2000 + col_id, (line_id + col_id) % 12, (line_id + col_id) % 28,
                      (line_id + col_id) % 24, (line_id + col_id) % 60, (line_id + col_id) % 60);
                    fret = row_data.GetDateTime(col_id, ibdate_get);
                    if (ibdate_tm != ibdate_get) {
                        char datetime_buf_get[15];
                        char datetime_buf_tm[15];
                        DM_IB_RowData_NS::ibdate2str(ibdate_get, datetime_buf_get);
                        DM_IB_RowData_NS::ibdate2str(ibdate_tm, datetime_buf_tm);
                        DPRINTF("R[%ld]C[%d] == %s != %s\n", (long)line_id, col_id,
                          datetime_buf_get, datetime_buf_tm);
                        exit(EBADSLT);
                    }
                    break;
                  }
                case DM_IB_TABLEINFO_CT_VARCHAR :
                  {
                    const char *str_v;
                    size_t str_len;
                    fret = row_data.GetString(col_id, str_v, str_len);
                    if (((uint64_t)str_v % 8) != 0)
                        no_bound_str_count++;
                    if (strncmp(string_arr[(line_id + col_id) % 20], str_v, str_len) != 0) {
                        DPRINTF("R[%ld]C[%d] == %s != %s\n", (long)line_id, col_id,
                          str_v, string_arr[(line_id + col_id) % 20]);
                        exit(EBADSLT);
                    }
                    break;
                  }
                default :
                    if (!row_data.IsNull(col_id)) {
                        DPRINTF("R[%ld]C[%d] is not null\n", (long)line_id, col_id);
                        exit(EBADSLT);
                    }
                }
                if (fret) {
                    DPRINTF("col[%d] get failed[%s]\n", col_id, CR_Class_NS::strerrno(fret));
                    exit(fret);
                }
            }
            get_row_timer.Stop();
            line_id++;
        }
    }

    DPRINTF("no_bound_line_count == %ld, no_bound_str_count == %ld\n",
      (long)no_bound_line_count, (long)no_bound_str_count);

    fprintf(stderr, "get_row_timer  : %s\n", get_row_timer.DebugString().c_str());
    fprintf(stderr, "parse_timer    : %s\n", parse_timer.DebugString().c_str());
}

int
do_rowtest(int argc, char *argv[])
{
    if (argc < 4)
        help_out(argc, argv);

    int fret = 0;
    std::string col_def_str = argv[2];
    int64_t line_count = atoll(argv[3]);
    DM_IB_TableInfo table_info;
    char fls_filename[256];

    CR_Class_NS::strlcpy(fls_filename, "/tmp/cfm-tb-fls-XXXXXX", sizeof(fls_filename));
    int fls_fd = mkstemp(fls_filename);

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

    DPRINTF("Enter\n");

    _row_encoder(fls_fd, &table_info, line_count);

    ::lseek(fls_fd, SEEK_SET, 0);

    _row_decoder(fls_fd, &table_info);

    ::unlink(fls_filename);

    return fret;
}
