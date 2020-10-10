#include <stdio.h>
#include <set>
#include <map>
#include <cr_class/cr_class.h>
#include <cr_class/cr_file.h>
#include <cr_class/cr_addon.h>
#include <cr_class/cr_autoid.h>
#include <cr_class/cr_class.pb.h>
#include <cr_class/cr_quickhash.h>
#include <cr_class/cr_fixedlinearstorage.h>
#include "libibcompress/IBCompress.h"

//////////////////////

static void help_out(int argc, char **argv);

//////////////////////

static void
help_out(int argc, char **argv)
{
    fprintf(stderr,
      "Usage: %s %s"
      " <data_path> <job_id> <column_id> <node_count>"
      " <packn | packs> <ib | zip>"
      " <packid_begin> <packid_end> [key_like_str]\n"
      , argv[0], argv[1]
    );

    exit(1);
}

//////////////////////

int
do_slpkchk(int argc, char *argv[])
{
    if (argc < 10)
        help_out(argc, argv);

    std::string data_path = argv[2];
    std::string job_id = argv[3];
    unsigned int column_id = atoi(argv[4]);
    unsigned int node_count = atoi(argv[5]);
    std::string pack_type = argv[6];
    std::string compress_type = argv[7];
    int64_t packid_begin = atoll(argv[8]); if (packid_begin < 0) packid_begin = 0;
    int64_t packid_end = atoll(argv[9]); if (packid_end < 0) packid_end = INT64_MAX;
    std::string key_like_str; if (argc > 10) key_like_str = argv[10];

    int fret = 0;
    char str_buf[PATH_MAX];
    std::map<std::string,std::set<uint32_t> > pack_line_store;
    int fd;

    for (unsigned node_id=0; node_id<node_count; node_id++) {
        snprintf(str_buf, sizeof(str_buf), "%s/S%s-N%05u-A%05u.pki",
          data_path.c_str(), job_id.c_str(), node_id, column_id);
        fd = CR_File::open(str_buf, O_RDONLY);
        if (fd >= 0) {
            do {
                break;
            } while (1);
        } else {
            return errno;
        }
    }

    for (auto map_it=pack_line_store.cbegin(); map_it!=pack_line_store.cend(); map_it++) {
        const std::set<uint32_t> &cur_key_set = map_it->second;
        printf("K[\"%s\"](%lu)->{", map_it->first.c_str(), (long unsigned)cur_key_set.size());
        for (auto set_it=cur_key_set.cbegin(); set_it!=cur_key_set.cend(); set_it++) {
            if (set_it!=cur_key_set.cbegin()) {
                printf(", %u", *set_it);
            } else {
                printf("%u", *set_it);
            }
        }
        printf("}\n");
    }

    return fret;
}
