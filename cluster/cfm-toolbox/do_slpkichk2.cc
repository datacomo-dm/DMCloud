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

//////////////////////

static void help_out(int argc, char **argv);

//////////////////////

static void
help_out(int argc, char **argv)
{
    fprintf(stderr,
      "Usage: %s %s"
      " <data_path> <job_id> <column_id> <node_count> [key_like_str]\n"
      , argv[0], argv[1]
    );

    exit(1);
}

//////////////////////

int
do_slpkichk2(int argc, char *argv[])
{
    if (argc < 6)
        help_out(argc, argv);

    int fret = 0;
    std::string data_path = argv[2];
    std::string job_id = argv[3];
    unsigned int column_id = atoi(argv[4]);
    unsigned int node_count = atoi(argv[5]);
    std::string key_like_str;
    char str_buf[PATH_MAX];
    CR_FixedLinearStorage fls_tmp;
    int fls_fd;
    size_t fls_total_lines;
    int64_t fls_obj_id;
    std::string fls_key;
    std::string fls_value;
    bool fls_value_is_null;
    std::vector<uint32_t> pki_idx_array;
    std::map<std::string,std::set<uint32_t> > pki_idx_store;

    if (argc > 6) {
        key_like_str = argv[6];
    }

    for (unsigned node_id=0; node_id<node_count; node_id++) {
        snprintf(str_buf, sizeof(str_buf), "%s/S%s-N%05u-A%05u.pki",
          data_path.c_str(), job_id.c_str(), node_id, column_id);
        fls_fd = CR_File::open(str_buf, O_RDONLY);
        if (fls_fd >= 0) {
            do {
                fprintf(stderr, "Read \"%s\":%lld ...", str_buf,
                  (long long)CR_File::lseek(fls_fd, 0, SEEK_CUR));
                fret = fls_tmp.LoadFromFileDescriptor(fls_fd);
                fprintf(stderr, " %s\n", CR_Class_NS::strerrno(fret));
                if (fret == ENOMSG) {
                    fret = 0;
                    break;
                } else if (fret) {
                    int8_t tmp_code;
                    uint32_t tmp_size;
                    void * tmp_p = CR_Class_NS::fget_block(fls_fd, tmp_code,
                      NULL, tmp_size, CR_BLOCKOP_FLAG_IGNORECRCERR);
                    if (tmp_p) {
                        fprintf(stderr, "bad pack(data_size = %u)\n", tmp_size);
                        CR_FixedLinearStorage_NS::fls_infos_t tmp_fls_info;
                        CR_Class_NS::set_debug_level(20);
                        if (CR_FixedLinearStorage_NS::VerifyData(tmp_p, tmp_size, &tmp_fls_info)) {
                            fprintf(stderr, "but data vrify OK\n");
                        }
                        free(tmp_p);
                    } else {
                        fret = errno;
                        break;
                    }
                    continue;
                }
                fls_total_lines = fls_tmp.GetTotalLines();
                for (size_t i=0; i<fls_total_lines; i++) {
                    fret = fls_tmp.Query(i, fls_obj_id, fls_key, fls_value, fls_value_is_null);
                    if (fret)
                        return fret;;
                    if (CR_Class_NS::str_like_cmp(fls_key, key_like_str)) {
                        CR_Class_NS::vector_assign(pki_idx_array, fls_value.data(), fls_value.size());
                        fprintf(stderr, "K[\"%s\"](%lu)->{", fls_key.c_str(),
                          (long unsigned)pki_idx_array.size());
                        for (size_t i=0; i<pki_idx_array.size(); i++) {
                            uint32_t cur_pki_idx = pki_idx_array[i];
                            pki_idx_store[fls_key].insert(cur_pki_idx);
                            if (i) {
                                fprintf(stderr, ", %u", cur_pki_idx);
                            } else {
                                fprintf(stderr, "%u", cur_pki_idx);
                            }
                        }
                        fprintf(stderr, "}\n");
                    }
                }
                if (fret)
                    return fret;
            } while (1);
        } else {
            return errno;
        }
    }

    for (auto map_it=pki_idx_store.cbegin(); map_it!=pki_idx_store.cend(); map_it++) {
        const std::set<uint32_t> &cur_key_set = map_it->second;
        printf("KEY[\"%s\"](%lu)->{", map_it->first.c_str(), (long unsigned)cur_key_set.size());
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
