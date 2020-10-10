#include <cr_class/cr_class.h>
#include <cfm_drv/dm_ib_sort_load.h>

//////////////////////

static void help_out(int argc, char **argv);

//////////////////////

static void
help_out(int argc, char **argv)
{
    fprintf(stderr,
      "Usage: %s %s <lock_name> <lock_pass>\n"
      , argv[0], argv[1]
    );

    exit(1);
}

//////////////////////

int
do_unlock(int argc, char *argv[])
{
    if (argc < 4)
        help_out(argc, argv);

    int fret;
    std::string lock_name = argv[2];
    std::string lock_pass = argv[3];

    char *cluster_str_p = getenv(DMIBSORTLOAD_EN_CLUSTER_STR);
    if (!cluster_str_p) {
        DPRINTF("Environment variable \"%s\" not set\n", DMIBSORTLOAD_EN_CLUSTER_STR);
        return EINVAL;
    }

    std::string cluster_str = cluster_str_p;

    CFM_Basic_NS::cluster_info_t cluster_info;

    fret = CFM_Basic_NS::parse_cluster_info(cluster_str, cluster_info);
    if (fret)
        DPRINTF("Parse cluster info failed[%s]\n", CR_Class_NS::strerrno(fret));

    CFMConnect node_conn;

    fret = node_conn.Connect(cluster_info[0]);
    if (fret)
        DPRINTF("Connect to node failed[%s]\n", CR_Class_NS::strerrno(fret));

    fret = node_conn.Unlock(lock_name, lock_pass);

    DPRINTF("Unlock() == %s\n", CR_Class_NS::strerrno(fret));

    return 0;
}
