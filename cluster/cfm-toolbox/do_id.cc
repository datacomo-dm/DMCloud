#include <cr_class/cr_class.h>
#include <cfm_drv/dm_ib_sort_load.h>

//////////////////////

static void help_out(int argc, char **argv);

//////////////////////

static void
help_out(int argc, char **argv)
{
    fprintf(stderr, "Usage: %s %s alloc <id_name> <alloc_count> [autofree_timeout]\n", argv[0], argv[1]);
    fprintf(stderr, "       %s %s free <id_name> <id#0> [<id#1> ...]\n", argv[0], argv[1]);
    fprintf(stderr, "       %s %s solidify <id_name> <id#0> [<id#1> ...]\n", argv[0], argv[1]);

    exit(EINVAL);
}

//////////////////////

int
do_id(int argc, char *argv[])
{
    if (argc < 5)
        help_out(argc, argv);

    int fret;
    std::string cmd_str = argv[2];
    std::string id_name = argv[3];
    std::vector<int64_t> id_arr;

    char *cluster_str_p = getenv(DMIBSORTLOAD_EN_CLUSTER_STR);
    if (!cluster_str_p)
        return EINVAL;

    std::string job_id = "IDTEST";
    std::string cluster_str = cluster_str_p;

    CFM_Basic_NS::cluster_info_t cluster_info;

    fret = CFM_Basic_NS::parse_cluster_info(cluster_str, cluster_info);
    if (fret) {
        DPRINTF("[%s]Parse cluster info failed[%s]\n", job_id.c_str(), CR_Class_NS::strerrno(fret));
        return fret;
    }

    CFMCluster id_cluster(cluster_info, 1000000, CR_FLS_SIZEMODE_SMALL, 60, 10);

    id_cluster.SetJobID(job_id);

    std::string errmsg;

    fret = id_cluster.Connect();
    if (fret) {
        DPRINTF("[%s]id_cluster->Connect failed\n", job_id.c_str());
        return fret;
    }

    if (cmd_str == "alloc") {
        size_t alloc_count = atoll(argv[4]);
        double autofree_timeout = 86400;

        if (argc > 5)
            autofree_timeout = atof(argv[5]);

        fret = id_cluster.IDAlloc(id_name, alloc_count, autofree_timeout, id_arr, &errmsg);
        if (fret) {
            DPRINTF("[%s]Alloc sample failed, msg => [%s]\n", job_id.c_str(), errmsg.c_str());
            return fret;
        }

        fprintf(stderr, "alloc %lu id : ", (long unsigned)id_arr.size());

        for (size_t i=0; i<id_arr.size(); i++) {
            fprintf(stderr, "%lld(0x%016llX), ", (long long)id_arr[i], (long long)id_arr[i]);
        }

        fprintf(stderr, "\n");
    } else if (cmd_str == "free") {
        for (int i=4; i<argc; i++) {
            id_arr.push_back(atoll(argv[i]));
        }

        fret = id_cluster.IDFree(id_name, id_arr, &errmsg);
        if (fret) {
            DPRINTF("[%s]Free failed, msg => [%s]\n", job_id.c_str(), errmsg.c_str());
            return fret;
        }
    } else if (cmd_str == "solidify") {
        for (int i=4; i<argc; i++) {
            id_arr.push_back(atoll(argv[i]));
        }

        fret = id_cluster.IDSolidify(id_name, id_arr, &errmsg);
        if (fret) {
            DPRINTF("[%s]Solidify failed, msg => [%s]\n", job_id.c_str(), errmsg.c_str());
            return fret;
        }
    } else {
        help_out(argc, argv);
    }

    return 0;
}
