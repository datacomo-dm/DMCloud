#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_externalsort.h>
#include <cr_class/cr_quickhash.h>

void
help_out(int argc, char **argv)
{
    fprintf(stderr,
      "Usage: %s <data_filename_prefix> <output_filename>"
        " <memory_size> <slice_size> <max_slice_size> <line_count>"
        " [sort_thread_count] [write_thread_count]"
        " [merge_thread_count] [merge_part_count]"
        " [1=compress_data_file] [1=enable_resualt_preload]"
        " [loop_count] [extra_data_filename_prefix_#1] ...\n"
      , argv[0]
    );

    exit(1);
}

int
main(int argc, char *argv[]) {
    if (argc < 7)
        help_out(argc, argv);

    int fret;
    std::string data_filename_prefix = argv[1];
    std::string output_filename = argv[2];
    size_t memory_size = atoll(argv[3]);
    size_t slice_size = atoll(argv[4]);
    size_t max_slice_size = atoll(argv[5]);
    size_t line_count = atoll(argv[6]);
    int sort_thread_count = 1;
    int disk_thread_count = 1;
    int merge_thread_count = 0;
    size_t merge_part_count = 0;
    int compress_data_file = 0;
    int enable_preload = 0;
    CR_ExternalSort esort;
    std::string esort_args;
    int loop_count = 1;
    std::vector<std::string> data_filename_prefix_arr;

    if (argc > 7)
        sort_thread_count = atoi(argv[7]);

    if (argc > 8)
        disk_thread_count = atoi(argv[8]);

    if (argc > 9)
        merge_thread_count = atoi(argv[9]);

    if (argc > 10)
        merge_part_count = atoi(argv[10]);

    if (argc > 11)
        compress_data_file = atoi(argv[11]);

    if (argc > 12)
        enable_preload = atoi(argv[12]);

    if (argc > 13)
        loop_count = atoi(argv[13]);

    if (loop_count < 0)
        loop_count = 0;

    data_filename_prefix_arr.push_back(data_filename_prefix);

    for (int i=14; i<argc; i++) {
        data_filename_prefix_arr.push_back(std::string(argv[i]));
    }

    size_t line_id, next_step;

    std::string k_tmp, v_tmp, k_last;

    size_t k_crc, v_crc;
    size_t k_crc_save, v_crc_save;
    int64_t obj_id;

    double begin_time, count_time;
    int save_fd;

//    msgCRPairList msg_data;
    CR_FixedLinearStorage fls_data;

    fls_data.ReSize(max_slice_size, CR_FLS_SIZEMODE_SMALL);

    save_fd = open(output_filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (save_fd < 0) {
        DPRINTF("Open file [%s] failed[%s]\n", output_filename.c_str(), CR_Class_NS::strerrno(errno));
        return errno;
    }

    esort_args = CR_Class_NS::str_setparam_uint(esort_args,
      EXTERNALSORT_ARGNAME_ROUGHFILESIZE, memory_size);
    esort_args = CR_Class_NS::str_setparam_uint(esort_args,
      EXTERNALSORT_ARGNAME_ROUGHSLICESIZE, slice_size);
    esort_args = CR_Class_NS::str_setparam_uint(esort_args,
      EXTERNALSORT_ARGNAME_MAXSLICESIZE, max_slice_size);
    esort_args = CR_Class_NS::str_setparam_bool(esort_args,
      EXTERNALSORT_ARGNAME_ENABLECOMPRESS, compress_data_file);
    esort_args = CR_Class_NS::str_setparam_bool(esort_args,
      EXTERNALSORT_ARGNAME_ENABLEPRELOAD, enable_preload);
    esort_args = CR_Class_NS::str_setparam_uint(esort_args,
      EXTERNALSORT_ARGNAME_MAXSORTTHREADS, sort_thread_count);
    esort_args = CR_Class_NS::str_setparam_uint(esort_args,
      EXTERNALSORT_ARGNAME_MAXDISKTHREADS, disk_thread_count);
    esort_args = CR_Class_NS::str_setparam_uint(esort_args,
      EXTERNALSORT_ARGNAME_MAXMERGETHREADS, merge_thread_count);
    esort_args = CR_Class_NS::str_setparam_uint(esort_args,
      EXTERNALSORT_ARGNAME_MERGECOUNTMIN, merge_part_count);
    esort_args = CR_Class_NS::str_setparam_uint(esort_args,
      EXTERNALSORT_ARGNAME_MERGECOUNTMAX, merge_part_count);
    esort_args = CR_Class_NS::str_setparam_int(esort_args,
      EXTERNALSORT_ARGNAME_DEFAULTSIZEMODE, CR_FLS_SIZEMODE_LARGE);

    esort.SetArgs(esort_args, data_filename_prefix_arr);

    double enter_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

    for (int looploop=0; looploop<loop_count; looploop++) {

        count_time = 0;

        k_crc = 0;
        v_crc = 0;

        next_step = 0x100000UL;

        DPRINTF("Enter\n");

        fls_data.Clear();

        for (line_id=0; line_id<line_count; line_id++) {
            k_tmp = CR_Class_NS::uint2str(CR_Class_NS::rand());
            v_tmp = CR_Class_NS::uint2str(line_id);

            fret = fls_data.PushBack(line_id, k_tmp, &v_tmp);
            if (fret) {
                begin_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
                fret = esort.Push(fls_data);
                count_time += CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - begin_time;

                if (fret) {
                    DPRINTF("Push failed[%s] at last lines\n",
                      CR_Class_NS::strerrno(fret));
                    return fret;
                }

                fls_data.Clear();
                line_id--;

                continue;
            }

            k_crc += CR_Class_NS::crc32(k_tmp);
            v_crc += CR_Class_NS::crc32(v_tmp);

            if (line_id >= next_step) {
                DPRINTF("Total 0x%016llX lines push, total usage time %f\n",
                  (long long)line_id, count_time);
                next_step += 0x100000;
            }
        }

        begin_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
        fret = esort.Push(fls_data, true);
        count_time += CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - begin_time;

        if (fret) {
            DPRINTF("Push failed[%s] at last lines\n",
              CR_Class_NS::strerrno(fret));
            return fret;
        }

        DPRINTF("PUSH Time Usage:%f\n", count_time);

        DPRINTF("%llu lines push ok\n", (long long unsigned)line_id);

        k_crc_save = k_crc;
        v_crc_save = v_crc;

        line_id = 0;
        count_time = 0;

        k_crc = 0;
        v_crc = 0;

        next_step = 0x100000UL;

        k_last.clear();

        bool is_done = false;

        while (1) {
            fls_data.Clear();

            begin_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
            fret = esort.Pop(fls_data);
            count_time += CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - begin_time;

            if (fret == EAGAIN) {
                is_done = true;
            } else if (fret) {
                DPRINTF("Pop failed[%s]\n", CR_Class_NS::strerrno(fret));
                return fret;
            }

            for (size_t line_no=0; ; line_no++) {
                bool value_is_null;

                fret = fls_data.Query(line_no, obj_id, k_tmp, v_tmp, value_is_null);
                if (fret == ERANGE) {
                    fret = 0;
                    break;
                } else if (fret) {
                    DPRINTF("fls_data.Query failed[%s]\n", CR_Class_NS::strerrno(fret));
                    return fret;
                }

                k_crc += CR_Class_NS::crc32(k_tmp);
                v_crc += CR_Class_NS::crc32(v_tmp);

                if (CR_Class_NS::uint2str(obj_id) != v_tmp) {
                    DPRINTF("Value error, [%s] != [%s]\n",
                      CR_Class_NS::uint2str(obj_id).c_str(), v_tmp.c_str());
                }

                if (k_last > k_tmp) {
                    DPRINTF("Order fault, [%s] > [%s]\n", k_last.c_str(), k_tmp.c_str());
                }

                k_last = k_tmp;
                line_id++;
            }

            if (line_id >= next_step) {
                DPRINTF("Total 0x%016llX lines pop, total usage time %f\n",
                  (long long)line_id, count_time);
                next_step += 0x100000;
            }

            if (fls_data.SaveToFileDescriptor(save_fd, 0, NULL, compress_data_file)) {
                DPRINTF("Save to file failed\n");
                return 1;
            }

            if (is_done)
                break;
        }

        DPRINTF("POP Time Usage:%f\n", count_time);

        if ((k_crc != k_crc_save) || (v_crc != v_crc_save)) {
            DPRINTF("%llu lines pop, CRCSUM failed:"
              " KPUSH[0x%016llX], KPOP[0x%016llX], VPUSH[0x%016llX], VPOP[0x%016llX]\n",
              (long long unsigned)line_id, (long long)k_crc_save, (long long)k_crc,
              (long long)v_crc_save, (long long)v_crc);
        } else {
            DPRINTF("%llu lines pop and check ok\n", (long long unsigned)line_id);
        }

        DPRINTF("%s\n", CR_Class_NS::strerrno(fret));

        esort.Clear();
    }

    printf("PROG usage time: %f\n", CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - enter_time);

    return 0;
}
