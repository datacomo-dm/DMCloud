#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <thread>
#include <cr_class/cr_class.h>
#include <cr_class/cr_addon.h>
#include <cr_class/cr_timer.h>
#include <cr_class/cr_blockqueue.h>
#include <cr_class/cr_fixedlinearstorage.h>
#include <cfm_drv/dm_ib_tableinfo.h>
#include <cfm_drv/dm_ib_rowdata.h>

//////////////////////

static void help_out(int argc, char **argv);
static void _row_encoder(CR_BlockQueue<CR_FixedLinearStorage*> *queue_p,
    DM_IB_TableInfo *table_info_p, int64_t line_count);
static void _row_decoder(CR_BlockQueue<CR_FixedLinearStorage*> *queue_p,
    DM_IB_TableInfo *table_info_p);

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
      "Usage: %s <column_define_string> <line_count>\n"
      "  column_define_string:\n"
      "    I -> bigint\n"
      "    F -> double\n"
      "    D -> datetime\n"
      "    S -> string\n"
      "    other -> nul\n"
      "  example : \"IIFDSSABC\" is bigint, bigint, double, datetime, string, string, nul, nul, nul\n"
      , argv[0]
    );

    exit(1);
}

static void
_row_encoder(CR_BlockQueue<CR_FixedLinearStorage*> *queue_p, DM_IB_TableInfo *table_info_p, int64_t line_count)
{
    int fret = 0;
    DM_IB_RowData row_data;
    CR_FixedLinearStorage *slice_fls_p = NULL;
    int col_count = table_info_p->size();
    uint64_t total_data_size = 0;

    slice_fls_p = new CR_FixedLinearStorage(1000000);
    row_data.BindTableInfo(*table_info_p);

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
                fret = row_data.SetDateTime(col_id, DM_IB_RowData_NS::mkibdate(2001, 2, 3, 4, 5, 6));
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
        const void *row_data_data;
        size_t row_data_size;
        serialize_timer.Start();
        fret = row_data.Data(row_data_data, row_data_size);
        serialize_timer.Stop();
        total_data_size += row_data_size;
        if (fret) {
            DPRINTF("row_data.Data failed[%s]\n", CR_Class_NS::strerrno(fret));
            exit(fret);
        }
        if (line_count <= 10) {
            CR_Class_NS::hexdump(stderr, row_data_data, row_data_size, 16);
        }
        fret = slice_fls_p->PushBack(line_id, row_data_data, row_data_size, NULL, 0);
        if (fret == ENOBUFS) {
            fret = queue_p->push_back(slice_fls_p, 3600);
            if (fret) {
                DPRINTF("queue_p->push_back failed [%s]\n", CR_Class_NS::strerrno(fret));
                exit(fret);
            }
            slice_fls_p = new CR_FixedLinearStorage(1000000);
            fret = slice_fls_p->PushBack(line_id, row_data_data, row_data_size, NULL, 0);
        } else if (fret) {
            DPRINTF("slice_fls_p->PushBack failed[%s]\n", CR_Class_NS::strerrno(fret));
            exit(fret);
        }
        row_data.Clear();
        if (line_id && (line_id % 0x10000 == 0))
            DPRINTF("Total 0x%016llX lines made\n", (long long)line_id);
    }

    queue_p->push_back(slice_fls_p, 3600);

    if (line_count % 0x10000 != 0) {
        DPRINTF("Total 0x%016llX lines made, avg row size %lu\n", (long long)line_count,
          (long unsigned)(total_data_size / line_count));
    }

    fprintf(stderr, "set_row_timer  : %s\n", set_row_timer.DebugString().c_str());
    fprintf(stderr, "serialize_timer: %s\n", serialize_timer.DebugString().c_str());

    queue_p->push_back(NULL, 3600);
}

static void
_row_decoder(CR_BlockQueue<CR_FixedLinearStorage*> *queue_p, DM_IB_TableInfo *table_info_p)
{
    int fret = 0;
    DM_IB_RowData row_data;
    CR_FixedLinearStorage *slice_fls_p = NULL;
    int col_count = table_info_p->size();
    int64_t line_id = 0;
    int64_t no_bound_line_count = 0;
    int64_t no_bound_str_count = 0;

    row_data.BindTableInfo(*table_info_p);

    CR_Timer get_row_timer;
    CR_Timer parse_timer;

    while (1) {
        fret = queue_p->pop_front(slice_fls_p, 3600);
        if (!fret) {
            if (slice_fls_p == NULL)
                break;

            size_t total_lines = slice_fls_p->GetTotalLines();

            for (size_t i=0; i<total_lines; i++) {
                int64_t _obj_id;
                const char *k_p;
                uint64_t k_size;
                const char *_v_p;
                uint64_t _v_size;
                fret = slice_fls_p->Query(i, _obj_id, k_p, k_size, _v_p, _v_size);
                if (fret) {
                    DPRINTF("[%u]slice_fls_p->Query() failed, [%s]\n",
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
                        char datetime_v[32];
                        fret = row_data.GetDateTime(col_id, datetime_v);
                        if (strcmp(datetime_v, "20010203040506") != 0) {
                            DPRINTF("R[%ld]C[%d] == %s != \"20010203040506\"\n", (long)line_id, col_id,
                              datetime_v);
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
            delete slice_fls_p;
        } else {
            DPRINTF("queue.pop_front() failed, [%s]\n", CR_Class_NS::strerrno(fret));
            exit(fret);
        }
    }

    DPRINTF("no_bound_line_count == %ld, no_bound_str_count == %ld\n",
      (long)no_bound_line_count, (long)no_bound_str_count);

    fprintf(stderr, "get_row_timer  : %s\n", get_row_timer.DebugString().c_str());
    fprintf(stderr, "parse_timer    : %s\n", parse_timer.DebugString().c_str());
}

int
main(int argc, char *argv[])
{
    if (argc < 3)
        help_out(argc, argv);

    int fret = 0;
    std::string col_def_str = argv[1];
    int64_t line_count = atoll(argv[2]);
    DM_IB_TableInfo table_info;
    CR_BlockQueue<CR_FixedLinearStorage*> queue;
    std::string col_name_str;
    std::string col_name_str_out;

    queue.set_args(128);

    for (size_t col_id=0; col_id<col_def_str.size(); col_id++) {
        switch (col_def_str[col_id]) {
        case 'I' :
            col_name_str = std::string("C_BIGINT") + CR_Class_NS::u162str(col_id);
            table_info.push_back(col_name_str, DM_IB_TABLEINFO_CT_BIGINT, 8);
            break;
        case 'F' :
            col_name_str = std::string("C_DOUBLE_") + CR_Class_NS::u162str(col_id);
            table_info.push_back(col_name_str, DM_IB_TABLEINFO_CT_DOUBLE, 8);
            break;
        case 'D' :
            col_name_str = std::string("C_DATETIME_") + CR_Class_NS::u162str(col_id);
            table_info.push_back(col_name_str, DM_IB_TABLEINFO_CT_DATETIME, 8);
            break;
        case 'S' :
            col_name_str = std::string("C_VARCHAR_") + CR_Class_NS::u162str(col_id);
            table_info.push_back(col_name_str, DM_IB_TABLEINFO_CT_VARCHAR, 256);
            break;
        default :
            col_name_str = std::string("C_INT") + CR_Class_NS::u162str(col_id);
            table_info.push_back(col_name_str, DM_IB_TABLEINFO_CT_INT, 4);
            break;
        }
        col_name_str_out += col_name_str;
        col_name_str_out += ", ";
    }

    DPRINTF("%s\n", col_name_str_out.c_str());

    std::thread encoder_th(_row_encoder, &queue, &table_info, line_count);
    std::thread decoder_th(_row_decoder, &queue, &table_info);

    decoder_th.join();
    encoder_th.join();

    return fret;
}
