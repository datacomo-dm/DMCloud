#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <thread>
#include <cr_class/cr_class.h>
#include <cr_class/cr_addon.h>
#include <cr_class/cr_autoid.h>
#include <cr_class/cr_class.pb.h>
#include <cr_class/cr_quickhash.h>
#include <cr_class/cr_externalsort.h>
#include <cr_class/cr_unlimitedfifo.h>
#include <cfm_drv/dm_ib_sort_load.h>
#include <cfm_drv/dm_ib_tableinfo.h>
#include <cfm_drv/dm_ib_rowdata.h>

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
do_slpkichk(int argc, char *argv[])
{
    if (argc < 6)
        help_out(argc, argv);

    int fret;
    std::string data_path = argv[2];
    std::string job_id = argv[3];
    unsigned int column_id = atoi(argv[4]);
    unsigned int node_count = atoi(argv[5]);
    std::string key_like_str;
    bool do_like_cmp = false;
    char str_buf[PATH_MAX];
    CR_ExternalSort::PopOnly pop_only;
    std::vector<std::string> part_filename_array;
    int64_t first_pack;
    std::string pki_key;
    std::string pki_packno_str;
    bool _bool_tmp;
    std::string pki_last_key = CR_Class_NS::randstr(16);
    std::string pki_last_key_rand = pki_last_key;
    std::vector<uint32_t> pki_packno_array_key;
    std::vector<uint32_t> pki_packno_array_tmp;
    int64_t pki_last_pack = -1;
    int64_t pki_last_pack0 = -1;
    size_t pki_key_count = 0;
    size_t pki_line_count = 0;
    size_t pki_skipped_line_count = 0;
    size_t pki_key_packs_total = 0;
    size_t pki_key_packs_cur = SIZE_MAX;
    size_t pki_min_key_packs = SIZE_MAX;
    size_t pki_max_key_packs = 0;

    if (argc > 6) {
        key_like_str = argv[6];
        do_like_cmp = true;
    }

    for (unsigned node_id=0; node_id<node_count; node_id++) {
        snprintf(str_buf, sizeof(str_buf), "%s/S%s-N%05u-A%05u.pki",
          data_path.c_str(), job_id.c_str(), node_id, column_id);
        part_filename_array.push_back(std::string(str_buf));
    }

    pop_only.LoadFiles(part_filename_array);

    uint64_t pki_line_count_verify = 0;

    for (size_t i=0; i<part_filename_array.size(); i++) {
        std::string pkiv_filename = part_filename_array[i] + "v";
        pki_line_count_verify += CR_Class_NS::str2u64(CR_Class_NS::load_string(pkiv_filename.c_str()));
    }

    while (1) {
        fret = pop_only.PopOne(first_pack, pki_key, pki_packno_str, _bool_tmp);
        if (fret == EAGAIN) {
            if (pki_key_packs_cur != SIZE_MAX)
                pki_key_packs_total += pki_key_packs_cur;
            fret = 0;
            break;
        } else if (fret) {
            DPRINTF("pop_only.PopOne failed[%s]\n", CR_Class_NS::strerrno(fret));
            break;
        } else {
            if (pki_last_key != pki_key) {
                if ((pki_key < pki_last_key) && (pki_last_key != pki_last_key_rand)) {
                    fprintf(stderr, "K[\"%s\"] < K[\"%s\"], ERROR!\n",
                      pki_key.c_str(), pki_last_key.c_str());
                    fret = EILSEQ;
                    break;
                }
                if (pki_key_packs_cur != SIZE_MAX) {
                    if (pki_min_key_packs > pki_key_packs_cur)
                        pki_min_key_packs = pki_key_packs_cur;
                    if (pki_max_key_packs < pki_key_packs_cur)
                        pki_max_key_packs = pki_key_packs_cur;
                    pki_key_packs_total += pki_key_packs_cur;
                }
                pki_last_key = pki_key;
                pki_last_pack = -1;
                pki_last_pack0 = -1;
                pki_packno_array_key.clear();
                pki_key_packs_cur = 0;
                pki_key_count++;
            }

            CR_Class_NS::vector_assign<uint32_t>(pki_packno_array_tmp,
              pki_packno_str.data(), pki_packno_str.size());

            pki_key_packs_cur += pki_packno_array_tmp.size();
            if (pki_packno_array_tmp[0] == pki_last_pack)
                pki_key_packs_cur--;

            if (do_like_cmp && CR_Class_NS::str_like_cmp(pki_key, key_like_str)) {
                printf("K[\"%s\"](%lu)->{", pki_key.c_str(), (long unsigned)pki_packno_array_tmp.size());
                for (size_t i=0; i<pki_packno_array_tmp.size(); i++) {
                    if (i) {
                        printf(", %u", pki_packno_array_tmp[i]);
                    } else {
                        printf("%u", pki_packno_array_tmp[i]);
                    }
                }
                if ((pki_packno_array_tmp.size() == 1) && (pki_packno_array_tmp[0] == pki_last_pack0)) {
                    printf("}, SKIPPED\n");
                } else {
                    printf("}\n");
                }
            }

            if (pki_packno_array_tmp[0] < pki_last_pack) {
                if ((pki_packno_array_tmp.size() == 1) && (pki_packno_array_tmp[0] == pki_last_pack0)) {
                    pki_skipped_line_count++;
                } else {
                    fprintf(stderr, "K[\"%s\"]->{", pki_key.c_str());
                    for (size_t i=0; i<pki_packno_array_key.size(); i++) {
                        if (i) {
                            fprintf(stderr, ", %u", pki_packno_array_key[i]);
                        } else {
                            fprintf(stderr, "%u", pki_packno_array_key[i]);
                        }
                    }
                    fprintf(stderr, "}\n");

                    fprintf(stderr, "K[\"%s\"]->{", pki_key.c_str());
                    for (size_t i=0; i<pki_packno_array_tmp.size(); i++) {
                        if (i) {
                            fprintf(stderr, ", %u", pki_packno_array_tmp[i]);
                        } else {
                            fprintf(stderr, "%u", pki_packno_array_tmp[i]);
                        }
                    }
                    fprintf(stderr, "} ERROR!\n");
                    fret = EILSEQ;
                    break;
                }
            }

            CR_Class_NS::vector_append<uint32_t>(pki_packno_array_key,
              pki_packno_str.data(), pki_packno_str.size());
            pki_last_pack0 = pki_packno_array_tmp[0];
            pki_last_pack = pki_packno_array_tmp[pki_packno_array_tmp.size() - 1];
            pki_line_count++;
        }
    }

    if (!fret) {
        printf("Total %u keys in %u lines(verify %u), and %u lines skipped\n"
               "key_packs_min = %u, key_packs_max = %u, key_packs_avg = %f\n",
               (unsigned)pki_key_count, (unsigned)pki_line_count, (unsigned)pki_line_count_verify,
               (unsigned)pki_skipped_line_count,
               (unsigned)pki_min_key_packs, (unsigned)pki_max_key_packs,
               (double)pki_key_packs_total / pki_key_count
        );
    }

    return fret;
}
