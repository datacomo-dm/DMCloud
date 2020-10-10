#include <cr_class/cr_class.h>
#include <cr_class/cr_fixedlinearstorage.h>

#define _FLS_FILENAME_ENV	"FLS_FILENAME"

void
help_out(char *argv[], int retcode)
{
    fprintf(stderr, "Usage: %s resize <max_data_size> [size_mode]\n", argv[0]);
    fprintf(stderr, "       %s clear [do_bzero]\n", argv[0]);
    fprintf(stderr, "       %s rebuild [max_data_size] [size_mode]\n", argv[0]);
    fprintf(stderr, "       %s set <line_no> <obj_id> <key> [value]\n", argv[0]);
    fprintf(stderr, "       %s pushback <obj_id> <key> [value]\n", argv[0]);
    fprintf(stderr, "       %s query <line_no>\n", argv[0]);
    fprintf(stderr, "       %s sort\n", argv[0]);
    fprintf(stderr, "       %s setflags <flags>\n", argv[0]);
    fprintf(stderr, "       %s savetofile <save_filename> [slice_size]\n", argv[0]);

    exit(retcode);
}

int
main(int argc, char *argv[])
{
    char *fls_filename_p = getenv(_FLS_FILENAME_ENV);
    if (!fls_filename_p) {
        fprintf(stderr, "Must set %s env-arg\n", _FLS_FILENAME_ENV);
        exit(EINVAL);
    }

    if (argc < 2)
        help_out(argv, EINVAL);

    int ret = 0;
    CR_FixedLinearStorage fls;
    std::string fls_filename = fls_filename_p;
    std::string cmd_str = argv[1];

    fls.LoadFromFile(fls_filename);

    if (cmd_str == "resize") {
        if (argc < 3)
            help_out(argv, EINVAL);
        uint64_t max_data_size = atoll(argv[2]);
        uint32_t size_mode = CR_FLS_SIZEMODE_LARGE;
        if (argc > 3)
            size_mode = atoi(argv[3]);
        ret = fls.ReSize(max_data_size, size_mode);
    } else if (cmd_str == "clear") {
        bool do_bzero = false;
        if (argc > 2)
            do_bzero = atoi(argv[2]);
        ret = fls.Clear(do_bzero);
    } else if (cmd_str == "rebuild") {
        uint64_t max_data_size = 0;
        uint32_t size_mode = 0;
        if (argc > 2)
            max_data_size = atoll(argv[2]);
        if (argc > 3)
            size_mode = atoi(argv[3]);
        ret = fls.Rebuild(max_data_size, size_mode);
    } else if (cmd_str == "set") {
        if (argc < 5)
            help_out(argv, EINVAL);
        uint64_t line_no = atoll(argv[2]);
        int64_t obj_id = atoll(argv[3]);
        std::string key = argv[4];
        std::string value;
        std::string *value_p=NULL;
        uint64_t free_space_left;
        if (argc > 5) {
            value = argv[5];
            value_p = &value;
        }
        ret = fls.Set(line_no, obj_id, key, value_p, &free_space_left);
        if (!ret)
            fprintf(stderr, "free_space_left == %llu\n", (long long unsigned)free_space_left);
    } else if (cmd_str == "pushback") {
        if (argc < 4)
            help_out(argv, EINVAL);
        int64_t obj_id = atoll(argv[2]);
        std::string key = argv[3];
        std::string value;
        std::string *value_p=NULL;
        uint64_t free_space_left;
        if (argc > 4) {
            value = argv[4];
            value_p = &value;
        }
        ret = fls.PushBack(obj_id, key, value_p, &free_space_left);
        if (!ret)
            fprintf(stderr, "free_space_left == %llu\n", (long long unsigned)free_space_left);
    } else if (cmd_str == "query") {
        if (argc < 3)
            help_out(argv, EINVAL);
        uint64_t line_no = atoll(argv[2]);
        int64_t obj_id;
        std::string key, value;
        bool value_is_null, is_last_line;
        ret = fls.Query(line_no, obj_id, key, value, value_is_null, &is_last_line);
        if (!ret) {
            std::string str_out = CR_Class_NS::str_printf("obj_id == %lld, key == \"%s\"",
              (long long)obj_id, key.c_str());
            if (value_is_null) {
                str_out.append(CR_Class_NS::str_printf(", value IS NULL"));
            } else {
                str_out.append(CR_Class_NS::str_printf(", value == \"%s\"", value.c_str()));
            }
            if (is_last_line) {
                str_out.append(CR_Class_NS::str_printf(", LAST_LINE"));
            }
            fprintf(stderr, "%s\n", str_out.c_str());
        }
    } else if (cmd_str == "sort") {
        ret = fls.Sort();
    } else if (cmd_str == "setflags") {
        if (argc < 2)
            help_out(argv, EINVAL);
        uint64_t flags = atoll(argv[2]);
        fls.SetFlags(flags);
    } else if (cmd_str == "savetofile") {
        if (argc < 3)
            help_out(argv, EINVAL);
        std::string save_filename = argv[2];
        uint64_t slice_size = 0;
        if (argc > 3)
            slice_size = atoll(argv[3]);
        bool do_compress = true;
        if (argc > 4)
            do_compress = atoi(argv[4]);
        uint64_t slice_count = 0;
        if (fls_filename != save_filename) {
            ret = fls.SaveToFile(save_filename, slice_size, &slice_count, do_compress);
            if (!ret)
                fprintf(stderr, "slice_count == %llu\n", (long long unsigned)slice_count);
        }
    } else {
        help_out(argv, EINVAL);
    }

    CR_FixedLinearStorage_NS::fls_infos_t fls_infos;
    fls.GetInfos(fls_infos);
    fprintf(stderr,
            "\nsize_mode == %u, flags == 0X%08X\n"
            "min_obj_id == %lld, max_obj_id == %lld\n"
            "storage_size == %llu, total_lines == %llu\n"
            "longest_key_size == %llu, longest_value_size == %llu, longest_line_size == %llu\n"
            "next_line_data_pos == %llu, free_space_left == %llu\n"
            "objid_cksum == 0X%016llX, key_cksum == 0X%016llX, value_cksum == 0X%016llX\n",
            fls_infos.size_mode, fls_infos.flags,
            (long long)fls_infos.min_obj_id, (long long)fls_infos.max_obj_id,
            (long long unsigned)fls.GetStorageSize(), (long long unsigned)fls_infos.total_lines,
            (long long unsigned)fls_infos.longest_key_size,
            (long long unsigned)fls_infos.longest_value_size,
            (long long unsigned)fls_infos.longest_line_size,
            (long long unsigned)fls_infos.next_line_data_pos,
            (long long unsigned)fls_infos.free_space_left, (long long unsigned)fls_infos.objid_cksum,
            (long long unsigned)fls_infos.key_cksum, (long long unsigned)fls_infos.value_cksum
    );

    if (fls.SaveToFile(fls_filename)) {
        fprintf(stderr, "fls.Save failed\n");
    }

    fprintf(stderr, "%s\n", CR_Class_NS::strerrno(ret));

    return ret;
}
