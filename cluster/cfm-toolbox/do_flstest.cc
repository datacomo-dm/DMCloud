#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <thread>
#include <list>
#include <cr_class/cr_class.h>
#include <cr_class/cr_addon.h>
#include <cr_class/cr_quickhash.h>
#include <cr_class/cr_fixedlinearstorage.h>
#include <cr_class/cr_timer.h>
#include "cfm-toolbox.h"

//////////////////////

static void help_out(int argc, char **argv);
static void _key_maker(CR_FixedLinearStorage *slice_fls_p);

//////////////////////

static void
help_out(int argc, char **argv)
{
    fprintf(stderr,
      "Usage: %s %s <fls_size> <line_maker_thread_count>"
      "\n"
      , argv[0], argv[1]
    );

    exit(1);
}

static void
_key_maker(CR_FixedLinearStorage *slice_fls_p)
{
    int fret = 0;
    size_t line_count = 0;
    std::string v_tmp;

    v_tmp.reserve(1024);

    CR_Timer fls_push_timer;

    while (1) {
        v_tmp.assign(string_arr[(line_count) % 20], 10);
        for (size_t i=0; i<24; i++) {
            v_tmp.append(string_arr[(line_count + i) % 20], 10);
        }
        fls_push_timer.Start();
        fret = slice_fls_p->PushBack(line_count, string_arr[line_count % 20], 3,
          v_tmp.data(), v_tmp.size());
        fls_push_timer.Stop();
        if (fret == ENOBUFS)
            break;
        line_count++;
    }

    DPRINTF("%s\n", fls_push_timer.DebugString().c_str());
}

int
do_flstest(int argc, char *argv[])
{
    if (argc < 4)
        help_out(argc, argv);

    size_t fls_size = atoll(argv[2]);
    int key_maker_thread_count = atoi(argv[3]);

    if (key_maker_thread_count < 1)
        key_maker_thread_count = 1;

    if (key_maker_thread_count > 8)
        key_maker_thread_count = 8;

    CR_FixedLinearStorage slice_fls(fls_size);
    std::list<std::thread*> km_tp_list;

    for (int i=0; i<key_maker_thread_count; i++) {
        std::thread *new_tp = new std::thread(_key_maker, &slice_fls);
        km_tp_list.push_back(new_tp);
    }

    while (!km_tp_list.empty()) {
        std::thread *tp = km_tp_list.front();
        km_tp_list.pop_front();
        tp->join();
        delete tp;
    }

    return 0;
}
