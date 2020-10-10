#include "riak-toolbox.h"
#include <thread>

void
help_out(int argc, char *argv[])
{
    fprintf(stderr, "Usage: %s %s ALLOC <bucket> <alloc_count> <autofree_timeout_sec:86400*15>\n", argv[0], argv[1]);
    fprintf(stderr, "Usage: %s %s FREE <bucket> <ks_id> [<ks_id> ...]\n", argv[0], argv[1]);
    fprintf(stderr, "Usage: %s %s SOLID <bucket> <ks_id> [<ks_id> ...]\n", argv[0], argv[1]);
    fprintf(stderr, "Usage: %s %s AUTO <bucket> <alloc_count> [thread_count] [loop_count]\n", argv[0], argv[1]);
    exit(EINVAL);
}

void
_do_auto(RiakCluster *riak_p, const std::string *bucket_p, size_t alloc_count, size_t loop_count)
{
    int fret = 0;

    for (size_t i=0; i<loop_count; i++) {
        std::vector<int64_t> id_arr;
        DPRINTF("[%u]alloc_count == %lu\n", (unsigned)i, (long unsigned)alloc_count);
        for (size_t j=0; j<alloc_count; j++) {
            int64_t ks_id = riak_p->KeySpaceAlloc(*bucket_p, 0, 0, 1024);
            if (ks_id < 0) {
                DPRINTF("[%u]alloc at %lu failed[%s]\n", (unsigned)i, (long unsigned)j,
                        CR_Class_NS::strerrno(errno));
                return;
            }
            id_arr.push_back(ks_id);
        }
        DPRINTF("[%u]alloc %lu ks ok, [0x%016lX(%ld) ~ 0x%016lX(%ld)]\n",
                (unsigned)i, id_arr.size(), id_arr[0], (id_arr[0] >> 12),
                id_arr[id_arr.size()-1], (id_arr[id_arr.size()-1] >> 12));
        if (rand() % 10 >= 3) {
            fret = riak_p->KeySpaceSolidify(*bucket_p, 0, 0, id_arr);
            DPRINTF("[%u]solidify %lu ks, %s\n", (unsigned)i, (long unsigned)id_arr.size(),
                    CR_Class_NS::strerrno(fret));
            if (fret)
                break;
        } else {
            DPRINTF("[%u]no solidify\n", (unsigned)i);
        }
        if (rand() % 10 >= 3) {
            fret = riak_p->KeySpaceFree(*bucket_p, 0, 0, id_arr);
            DPRINTF("[%u]free %lu ks, %s\n", (unsigned)i, (long unsigned)id_arr.size(),
                    CR_Class_NS::strerrno(fret));
            if (fret)
                break;
        } else {
            DPRINTF("[%u]no free\n", (unsigned)i);
        }
        DPRINTF("[%u]\n", (unsigned)i);
    }
}

int
do_kstest(int argc, char *argv[])
{
    int fret = 0;

    if (argc < 5)
        help_out(argc, argv);

    std::string riak_bucket;
    std::string errmsg;
    std::vector<int64_t> id_arr;
    int64_t ks_id_tmp;

    RiakCluster _cluster_client(180, 5);

    riak_bucket = argv[3];

    for (int i=4; i<argc; i++) {
        if(strlen(argv[i])>2) {
            const char* p=argv[i];
            if(p[0]=='0' && p[1] == 'x') {
                sscanf(p,"0x%016lX",&ks_id_tmp);
            } else {
                ks_id_tmp = atoll(argv[i]);
            }
            id_arr.push_back(ks_id_tmp);
        }
    }

    int nodes = _cluster_client.Rebuild(riak_hostname, riak_port_str);

    if (nodes < 0) {
        DPRINTF("Rebuild failed!, ret=%d, msg=%s\n", (0-nodes), CR_Class_NS::strerror(0-nodes));
    }

    if (strcmp(argv[2], "ALLOC") == 0) {
        size_t autofree_timeout_sec=86400*15;
        if(argc >5) {
            autofree_timeout_sec=atoi(argv[5]);
        }
        size_t alloc_count = atoi(argv[4]);
        int64_t ks_id;
        printf("alloced:\n");
        for (size_t i=0; i<alloc_count; i++) {
            ks_id = _cluster_client.KeySpaceAlloc(riak_bucket, 0, 0,1,autofree_timeout_sec);
            printf("0x%016llX(%lld) ", (long long)ks_id, (long long)(ks_id>>12));
        }
        printf("\n");
    } else if (strcmp(argv[2], "FREE") == 0) {
        fret = _cluster_client.KeySpaceFree(riak_bucket, 0, 0, id_arr);
    } else if (strcmp(argv[2], "SOLID") == 0) {
        fret = _cluster_client.KeySpaceSolidify(riak_bucket, 0, 0, id_arr);
    } else if (strcmp(argv[2], "AUTO") == 0) {
        size_t alloc_count = atoi(argv[4]);
        size_t thread_count = 1;
        size_t loop_count = 1;
        if (argc > 5)
            thread_count = atoi(argv[5]);
        if (argc > 6)
            loop_count = atoi(argv[6]);
        std::vector<std::thread*> tp_arr;
        for (size_t i=0; i<thread_count; i++) {
            tp_arr.push_back(new std::thread(_do_auto, &_cluster_client,
                                             &riak_bucket, alloc_count, loop_count));
        }
        for (size_t i=0; i<tp_arr.size(); i++) {
            tp_arr[i]->join();
            delete tp_arr[i];
        }
    }

    DPRINTF("%s\n", CR_Class_NS::strerror(fret));

    return fret;
}
