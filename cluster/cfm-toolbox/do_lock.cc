#include <cr_class/cr_class.h>
#include <cfm_drv/dm_ib_sort_load.h>

//////////////////////

static void help_out(int argc, char **argv);

//////////////////////

static void
help_out(int argc, char **argv)
{
    fprintf(stderr,
      "Usage: %s %s <lock_name> <lock_value> <lock_pass> <lock_timeout_sec> <wait_timeout_sec>\n"
      , argv[0], argv[1]
    );

    exit(1);
}

//////////////////////

int
do_lock(int argc, char *argv[])
{
    if (argc < 7)
        help_out(argc, argv);

    int fret;
    std::string lock_name = argv[2];
    std::string lock_value = argv[3];
    std::string lock_pass = argv[4];
    double lock_timeout_sec = atof(argv[5]);
    double wait_timeout_sec = atof(argv[6]);
    std::string old_lock_value;

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

    fret = node_conn.Lock(lock_name, lock_value, &old_lock_value,
      lock_pass, lock_timeout_sec, wait_timeout_sec);

    DPRINTF("Lock() == %s, old_lock_value == %s\n", CR_Class_NS::strerrno(fret),
      old_lock_value.c_str());

    return 0;
}
