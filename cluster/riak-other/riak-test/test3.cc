#include <cr_class/cr_class.h>
#include <cr_class/cr_trie.h>
#include <cr_class/cr_fixedlinearstorage.h>
#include <map>

void help_out(int argc, char **argv);

void
help_out(int argc, char **argv)
{
    fprintf(stderr,
      "Usage: %s <key_len> <total_keys> <key_save_size> [--verbose]\n"
      , argv[0]
    );
    exit(1);
}

int main(int argc, char *argv[])
{
    if (argc < 4)
        help_out(argc, argv);

    int key_len = atoi(argv[1]);
    ssize_t total_keys = atoll(argv[2]);
    ssize_t key_save_size = atoll(argv[3]);

    if (key_len < 0)
        key_len = 0;

    if (total_keys < 1)
        total_keys = 1;

    if (key_save_size < 1)
        key_save_size = 1;

    std::string k_tmp, k_tmp_0, v_tmp;
    bool null_value;
    double begin_time;
    ssize_t node_count;
    uintptr_t i_tmp;
    bool do_test_display = false;
    CR_FixedLinearStorage key_store(key_save_size, CR_FLS_SIZEMODE_TINY);
    int64_t obj_id;

    if (argc >= 5) {
        if (strcmp(argv[4], "--verbose") == 0) {
            do_test_display = true;
        }
    }

    CR_Trie<uintptr_t> *trie = new CR_Trie<uintptr_t>;
    CR_Trie<uintptr_t>::iterator node_it;
    std::map<std::string,uintptr_t> *map = new std::map<std::string,uintptr_t>;
    std::map<std::string,uintptr_t>::iterator map_it;

    DPRINTF("TRIE ENTER\n");

    trie->insert("", 0, 1);
    (*map)[""] = 1;

    begin_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

    for (ssize_t i=0; i<total_keys; i++) {
        i_tmp = CR_Class_NS::rand();
        k_tmp_0 = CR_Class_NS::u642str(i_tmp) + "-" + CR_Class_NS::randstr(key_len);
        k_tmp = k_tmp_0;
        k_tmp.assign(k_tmp_0.data(), CR_Class_NS::rand() % k_tmp_0.size());
        trie->insert(k_tmp.data(), k_tmp.size(), i_tmp);
        (*map)[k_tmp] = i_tmp;
    }

    DPRINTF("TRIE SET USAGE:%f\n", CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - begin_time);

    std::string k_save;

    node_count = 0;
    begin_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

    DPRINTF("TRIE 0 -> 1\n");

    node_it = trie->begin();
    while (node_it) {
        k_tmp = node_it.key();
        if (do_test_display)
            printf("TRIE: K[%s] == %016llX\n", k_tmp.c_str(), (long long unsigned)(*node_it));
        if (k_tmp < k_save)
            DPRINTF("ORDER FAILED, K[%s], ORIG_K[%s]\n", k_tmp.c_str(), k_save.c_str());
        ++node_it;
        node_count++;
    }

    DPRINTF("TRIE PLAN USAGE:%f\n", CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - begin_time);

    for (map_it=map->begin(); map_it!=map->end(); map_it++) {
        if (do_test_display)
            printf("MAP: K[%s] == %016llX\n", map_it->first.c_str(), (long long unsigned)(map_it->second));
    }

    begin_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

    while (1) {
        k_tmp = CR_Class_NS::u642str(CR_Class_NS::rand());
        if (key_store.PushBack(0, k_tmp, NULL) != 0)
            break;
    }

    size_t total_key_count = key_store.GetTotalLines();

    DPRINTF("KEY-ARRAY GEN USAGE:%f, TOTAL %lu LINES\n",
      CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - begin_time, total_key_count);

    DPRINTF("TRIE RANDOM GET\n");

    begin_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

    for (size_t i=0; i<total_key_count; i++) {
        key_store.Query(i, obj_id, k_tmp, v_tmp, null_value);
        i_tmp = trie->get(k_tmp, 0, 1);
        if (do_test_display)
            printf("TRIE: K[%s] => %016llX\n", k_tmp.c_str(), (long long unsigned)i_tmp);
    }

    DPRINTF("TRIE INDEXED GET USAGE:%f\n", CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - begin_time);

    DPRINTF("MAP RANDOM GET\n");

    begin_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

    for (size_t i=0; i<total_key_count; i++) {
        key_store.Query(i, obj_id, k_tmp, v_tmp, null_value);
        map_it = map->lower_bound(k_tmp);
        if (map_it == map->end())
            i_tmp = 0;
        else
            i_tmp = map_it->second;
        if (do_test_display)
            printf("MAP: K[%s] => %016llX\n", k_tmp.c_str(), (long long unsigned)i_tmp);
    }

    DPRINTF("MAP GET USAGE:%f\n", CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - begin_time);

    delete map;

    delete trie;

    return 0;
}
