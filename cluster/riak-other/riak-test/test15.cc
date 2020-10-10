#include <cr_class/cr_buddyalloc.h>
#include <cr_class/cr_quickhash.h>

void
help_out(int argc, char *argv[])
{
    fprintf(stderr, "Usage: %s <memory_size> <chunk_size> <loop_count>\n", argv[0]);
    exit(EINVAL);
}

int
main(int argc, char *argv[])
{
    if (argc < 4) {
        help_out(argc, argv);
    }

    size_t mem_size = atoll(argv[1]);
    size_t chunk_size = atoll(argv[2]);
    int loop_count = atoi(argv[3]);

    CR_BuddyAlloc alloc;
    int fret;

    fret = alloc.Init(mem_size, chunk_size);

    DPRINTF("alloc.Init(%lu, %lu) == %s\n", (long unsigned)mem_size, (long unsigned)chunk_size,
      CR_Class_NS::strerrno(fret));

    if (fret) {
        return fret;
    }

    std::vector<void*> p_arr2, p_arr3;
    double enter_time, usage_time;
    size_t alloc_total;

    p_arr2.reserve(10000000);
    p_arr3.reserve(10000000);

    alloc_total = 0;

    enter_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

    for (size_t i=0; ; i++) {
        size_t alloc_size = i % 30 + 1;
        void *p = alloc.Alloc(alloc_size);
        if (!p)
            break;
        alloc_total += alloc_size;
        p_arr2.push_back(p);
    }

    usage_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - enter_time;

    DPRINTF("alloc %ld loop, usage time %f, avg %f, total %lu\n", (long)p_arr2.size(),
      usage_time, usage_time / p_arr2.size(), (long unsigned)alloc_total);

    enter_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

    for (size_t i=0; i<p_arr2.size(); i++) {
        alloc.Free(p_arr2[i]);
    }

    usage_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - enter_time;

    DPRINTF("free %ld loop, usage time %f, avg %f\n", (long)p_arr2.size(),
      usage_time, usage_time / p_arr2.size());

    alloc_total = 0;

    enter_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

    for (size_t i=0; i<p_arr2.size(); i++) {
        size_t alloc_size = i % 30 + 1;
        void *p = calloc(1, alloc_size);
        alloc_total += alloc_size;
        p_arr3.push_back(p);
    }

    usage_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - enter_time;

    DPRINTF("calloc %ld loop, usage time %f, avg %f, total %lu\n", (long)p_arr2.size(),
      usage_time, usage_time / p_arr2.size(), (long unsigned)alloc_total);

    enter_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

    for (size_t i=0; i<p_arr3.size(); i++) {
        free(p_arr3[i]);
    }

    usage_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - enter_time;

    DPRINTF("free %ld loop, usage time %f, avg %f\n", (long)p_arr2.size(),
      usage_time, usage_time / p_arr2.size());

    for (int loop=0; loop<loop_count; loop++) {
        size_t total_alloc = 0;
        std::string data_str;
        std::vector<void*> p_arr;

        for (size_t i=0; ; i++) {
            size_t line_size = CR_QuickHash::CRC32Hash(&i, sizeof(i)) % 128;
            if (line_size == 0) {
                p_arr.push_back(NULL);
            } else {
                void *p = alloc.Alloc(line_size);
                if (!p) {
                    DPRINTF("%lu: Alloc failed[%s], already alloced %lu, avg size %lu\n",
                      (long unsigned)i, CR_Class_NS::strerrno(errno),
                      (long unsigned)total_alloc, (long unsigned)(total_alloc / i));
                    break;
                }
                data_str = CR_QuickHash::sha512(&i, sizeof(i));
                memcpy(p, data_str.data(), line_size);
                if (i % 17 == 0) {
                    void *lp = alloc.ReAlloc(p, data_str.size());
                    memcpy(lp, data_str.data(), data_str.size());
                    p_arr.push_back(lp);
                    total_alloc += data_str.size();
                } else {
                    p_arr.push_back(p);
                    total_alloc += line_size;
                }
            }
        }

        DPRINTF("L-%d: Alloc and write OK\n", loop);

        for (size_t i=0; i<p_arr.size(); i++) {
            size_t line_size = CR_QuickHash::CRC32Hash(&i, sizeof(i)) % 128;
            if (line_size != 0) {
                data_str = CR_QuickHash::sha512(&i, sizeof(i));
                if (i % 17 == 0) {
                    if (memcmp(p_arr[i], data_str.data(), data_str.size()) != 0) {
                        DPRINTF("%lu: verify failed\n", (long unsigned)i);
                    }
                } else {
                    if (memcmp(p_arr[i], data_str.data(), line_size) != 0) {
                        DPRINTF("%lu: verify failed\n", (long unsigned)i);
                    }
                }
            }
        }

        DPRINTF("L-%d: Verify OK\n", loop);

        for (size_t i=0; i<p_arr.size(); i++) {
            alloc.Free(p_arr[i]);
        }

        p_arr.clear();

        DPRINTF("L-%d: Free OK\n", loop);
    }

    return 0;
}
