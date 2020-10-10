#include <cr_class/cr_class.h>
#include <cfm_drv/dm_ib_sort_load.h>

//////////////////////

static void help_out(int argc, char **argv);
static int _make_splitter(void *merge_arg_p, const CFM_Basic_NS::cluster_info_t &cluster_info,
    const std::string &data_req, const std::vector<std::string> &results);

//////////////////////

static void
help_out(int argc, char **argv)
{
    fprintf(stderr,
      "Usage: %s %s <job_id>\n"
      , argv[0], argv[1]
    );

    exit(1);
}

//////////////////////

int
do_keysampletest(int argc, char *argv[])
{
    if (argc < 3)
        help_out(argc, argv);

    int fret = 0;
    char *cluster_str_p = getenv(DMIBSORTLOAD_EN_CLUSTER_STR);
    if (!cluster_str_p)
        return EINVAL;

    std::string job_id = argv[2];
    std::string job_id_s = job_id + "S";
    std::string cluster_str = cluster_str_p;

    CFM_Basic_NS::cluster_info_t cluster_info;

    fret = CFM_Basic_NS::parse_cluster_info(cluster_str, cluster_info);
    if (fret) {
        DPRINTF("[%s]Parse cluster info failed[%s]\n", job_id.c_str(), CR_Class_NS::strerrno(fret));
        return fret;
    }

    CFMCluster sort_cluster(cluster_info, 1000000, CR_FLS_SIZEMODE_SMALL, 60, 10);
    sort_cluster.SetJobID(job_id_s);

    CFMCluster_NS::NodeErrorsT cluster_error;

    fret = sort_cluster.Connect(&cluster_error);
    if (fret) {
        DPRINTF("[%s]sort_cluster->Connect failed, msg => [%s]\n", job_id.c_str(),
          CFMCluster_NS::ExplainErrors(cluster_error).c_str());
        return fret;
    }

    sort_cluster.Start(std::string("sort"), false, NULL, NULL);

    std::string query_cmd_str;
    query_cmd_str = CR_Class_NS::str_setparam(query_cmd_str,
      DMIBSORTLOAD_PN_QUERY_CMD, LIBSORT_PV_QUERY_KEY_SAMPLE);
    query_cmd_str = CR_Class_NS::str_setparam(query_cmd_str,
      DMIBSORTLOAD_PN_LAST_JOB_ID, job_id);
    fret = sort_cluster.Query(query_cmd_str, _make_splitter, NULL, &cluster_error);

    if (fret) {
        DPRINTF("[%s]Query key sample failed, msg => [%s]\n", job_id.c_str(),
          CFMCluster_NS::ExplainErrors(cluster_error).c_str());
        return fret;
    }

    return 0;
}

static int
_make_splitter(void *merge_arg_p, const CFM_Basic_NS::cluster_info_t &cluster_info,
    const std::string &data_req, const std::vector<std::string> &results)
{
    std::vector<std::string> samples;
    CFMSplitter splitter;
    char str_buf[256];

    splitter.Merge(results, &samples);

    for (size_t i=0; i<samples.size(); i++) {
        snprintf(str_buf, sizeof(str_buf), "S[%lu]:", (long unsigned)i);
        CR_Class_NS::hexdump(stdout, samples[i].data(), samples[i].size(), 16, str_buf);
    }

    return 0;
}
