#include <cr_class/cr_class.h>
#include <cfm_drv/dm_ib_sort_load.h>

//////////////////////

static void help_out(int argc, char **argv);

//////////////////////

static void
help_out(int argc, char **argv)
{
    fprintf(stderr,
      "Usage: %s %s"
      " <job_id>\n"
      , argv[0], argv[1]
    );

    exit(1);
}

//////////////////////

int
do_slcancel(int argc, char *argv[])
{
    if (argc < 3)
        help_out(argc, argv);

    std::string job_id = argv[2];

    return DMIBSortLoad_NS::EmergencyStop(job_id);
}
