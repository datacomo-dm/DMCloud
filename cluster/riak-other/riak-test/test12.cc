#include <stdio.h>
#include <stdlib.h>
#include <thread>
#include <cr_class/cr_unlimitedfifo.h>
#include <cr_class/cr_fixedqueue.h>

void help_out(int argc, char **argv);

void
help_out(int argc, char **argv)
{
    fprintf(stderr,
      "Usage: %s <filename_prefix> <file_size>\n"
      , argv[0]
    );

    exit(1);
}

double
alarm_func(void *alarm_arg)
{
    double next = (double)(CR_Class_NS::rand() % 100) / 10 + 0.1;
    DPRINTF("[0x%016llX]next=%f\n", (long long)alarm_arg, next);
    return next;
}

void
thread_push(CR_UnlimitedFifo *fifo_p, size_t loop_count)
{
    int fret;
    std::string k_tmp, v_tmp;
    double begin_time = CR_Class_NS::clock_gettime();

    for (size_t i=0; i<loop_count; i++) {
        k_tmp = CR_Class_NS::i642str(i);
        v_tmp = CR_Class_NS::randstr(7);

        fret = fifo_p->PushBack(i, k_tmp, &v_tmp);
        if (fret) {
            DPRINTF("PushOne failed[%s]\n", CR_Class_NS::strerrno(fret));
            return;
        }
    }

    printf("  thread push %llu lines, usage %f second(s)\n",
      (long long unsigned)loop_count, CR_Class_NS::clock_gettime() - begin_time);
}

void
thread_pop(CR_UnlimitedFifo *fifo_p, size_t loop_count)
{
    int fret;
    int64_t obj_id;
    std::string k_tmp, v_tmp;
    bool value_is_null;
    double begin_time = CR_Class_NS::clock_gettime();

    for (size_t i=0; i<loop_count; i++) {
        fret = fifo_p->PopFront(obj_id, k_tmp, v_tmp, value_is_null, 30);
        if (fret) {
            DPRINTF("PopOne failed[%s]\n", CR_Class_NS::strerrno(fret));
            return;
        }

        if ((size_t)obj_id != i) {
            DPRINTF("obj_id failed, want [%lu], got [%lu]\n", i, (size_t)obj_id);
        }

        if (k_tmp != CR_Class_NS::i642str(i)) {
            DPRINTF("Key failed, want [%s], got [%s]\n",
              CR_Class_NS::i642str(i).c_str(), k_tmp.c_str());
        }
    }

    printf("  thread pop %llu lines, usage %f second(s)\n",
      (long long unsigned)loop_count, CR_Class_NS::clock_gettime() - begin_time);
}

void
queue_push(CR_FixedQueue *queue_p, size_t loop_count)
{
    int fret;
    std::string k_tmp, v_tmp;
    double begin_time = CR_Class_NS::clock_gettime();

    for (size_t i=0; i<loop_count; i++) {
        k_tmp = CR_Class_NS::i642str(i);
        v_tmp = CR_Class_NS::randstr(7);

        fret = queue_p->PushBack(i, k_tmp, &v_tmp, 1);
        if (fret) {
            DPRINTF("PushBack failed[%s]\n", CR_Class_NS::strerrno(fret));
            return;
        }
    }

    printf("  thread push %llu lines, usage %f second(s)\n",
      (long long unsigned)loop_count, CR_Class_NS::clock_gettime() - begin_time);
}

void
queue_pop(CR_FixedQueue *queue_p, size_t loop_count)
{
    int fret;
    std::string k_tmp, v_tmp;
    bool value_is_null;
    double begin_time = CR_Class_NS::clock_gettime();
    int64_t obj_id;

    for (size_t i=0; i<loop_count; i++) {
        fret = queue_p->PopFront(obj_id, k_tmp, v_tmp, value_is_null, 30);
        if (fret) {
            DPRINTF("PopOne failed[%s]\n", CR_Class_NS::strerrno(fret));
            return;
        }

        if (obj_id != (int64_t)i) {
            DPRINTF("line[%lu] obj_id failed, want [%lu], got [%lu]\n",
              i, (long unsigned)i, (long unsigned)obj_id);
        }

        if (k_tmp != CR_Class_NS::i642str(i)) {
            DPRINTF("line[%lu] key failed, want [%s], got [%s]\n",
              i, CR_Class_NS::i642str(i).c_str(), k_tmp.c_str());
        }
    }

    printf("  thread pop %llu lines, usage %f second(s)\n",
      (long long unsigned)loop_count, CR_Class_NS::clock_gettime() - begin_time);
}

int main(int argc, char *argv[])
{
    if (argc < 3)
        help_out(argc, argv);

    int fret = 0;
    std::string filename_prefix = argv[1];
    size_t file_size = atoll(argv[2]);
    std::string k_tmp, v_tmp;
    size_t next_push = 0;
    size_t next_pop = 0;
    CR_UnlimitedFifo fifo;
    CR_FixedQueue queue;
    CR_FixedLinearStorage fls;

    size_t free_space_left;

    char cmd_buf[128];
    char arg_buf[128];

    std::string cmd_str;
    std::string arg_str;

    for (int i=0; i<1; i++) {
        CR_Class_NS::set_alarm(alarm_func, (void*)CR_Class_NS::rand(), 1);
    }

    fifo.SetArgs(filename_prefix, file_size);
    queue.ReSize(file_size);
    fls.ReSize(file_size, CR_FLS_SIZEMODE_SMALL);

    k_tmp = CR_Class_NS::i642str(0);
    v_tmp = CR_Class_NS::randstr(0);

    DPRINTF("FLS freespace:%lu\n", fls.GetFreeSpace());

    while (1) {
        printf(
          "CMD: push <string>\n"
          "     push <block-size>\n"
          "     pop <timeout>\n"
          "     pushq <block-size>\n"
          "     popq <timeout>\n"
          "     pushf <block-size>\n"
          "     popf <timeout>\n"
          "     loop <loop-count>\n"
          "     loopq <loop-count>\n"
          "     loopf <loop-count>\n"
          "     thread <loop-count>\n"
          "     threadq <loop-count>\n"
          "     quit <return-value>\n"
        );
        scanf("%s %s", cmd_buf, arg_buf);

        cmd_str = cmd_buf;
        arg_str = arg_buf;

        if (cmd_str == "push") {
            k_tmp = CR_Class_NS::i642str(next_push);

            size_t push_size = atoll(arg_buf);

            if (push_size > 0) {
                void *p_tmp = malloc(push_size);
                if (!p_tmp) {
                    DPRINTF("[%s]\n", CR_Class_NS::strerrno(errno));
                    continue;
                }

                memset(p_tmp, (int)next_push, push_size);

                fret = fifo.PushBack(next_push, k_tmp.data(), k_tmp.size(), p_tmp, push_size);
                if (fret) {
                    DPRINTF("PushOne failed[%s]\n", CR_Class_NS::strerrno(fret));
                    continue;
                }

                free(p_tmp);
            } else {
                fret = fifo.PushBack(next_push, k_tmp, &arg_str);
                if (fret) {
                    DPRINTF("PushOne failed[%s]\n", CR_Class_NS::strerrno(fret));
                    continue;
                }
            }

            next_push++;
        } else if (cmd_str == "pop") {
            int64_t obj_id;
            bool value_is_null;
            int pop_timeout = atoi(arg_buf);

            fret = fifo.PopFront(obj_id, k_tmp, v_tmp, value_is_null, pop_timeout);
            if (fret) {
                DPRINTF("PopOne failed[%s]\n", CR_Class_NS::strerrno(fret));
                continue;
            }

            if (v_tmp.size() < 64) {
                printf("I[%s], K[%s], V[%s]\n", CR_Class_NS::i642str(next_pop).c_str(),
                  k_tmp.c_str(), v_tmp.c_str());
            } else {
                printf("I[%s], K[%s], V.size()[%llu]\n", CR_Class_NS::i642str(next_pop).c_str(),
                  k_tmp.c_str(), (long long unsigned)v_tmp.size());
            }

            next_pop++;
        } else if (cmd_str == "pushf") {
            k_tmp = CR_Class_NS::i642str(next_push);

            size_t push_size = atoll(arg_buf);

            void *p_tmp = malloc(push_size);
            if (!p_tmp) {
                DPRINTF("[%s]\n", CR_Class_NS::strerrno(errno));
                continue;
            }

            memset(p_tmp, (int)next_push, push_size);

            fret = fls.PushBack(next_push, k_tmp.data(), k_tmp.size(), p_tmp, push_size, &free_space_left);
            if (fret) {
                DPRINTF("PushOne failed[%s]\n", CR_Class_NS::strerrno(fret));
                continue;
            }

            free(p_tmp);

            printf("FLS: space-left [%lu]\n", free_space_left);

            next_push++;
        } else if (cmd_str == "popf") {
            int64_t obj_id;
            bool value_is_null;
            size_t line_no = atoll(arg_buf);

            fret = fls.Query(line_no, obj_id, k_tmp, v_tmp, value_is_null);
            if (fret) {
                DPRINTF("PopOne failed[%s]\n", CR_Class_NS::strerrno(fret));
                continue;
            }

            printf("I[%s], K[%s], V.size()[%llu]\n", CR_Class_NS::int2str(obj_id).c_str(),
              k_tmp.c_str(), (long long unsigned)v_tmp.size());
        } else if (cmd_str == "pushq") {
            k_tmp = CR_Class_NS::i642str(next_push);
            size_t push_size = atoll(arg_buf);

            if (push_size > 0) {
                void *p_tmp = malloc(push_size);
                if (!p_tmp) {
                    DPRINTF("[%s]\n", CR_Class_NS::strerrno(errno));
                    continue;
                }

                memset(p_tmp, (int)next_push, push_size);

                fret = queue.PushBack(next_push, k_tmp.data(), k_tmp.size(), p_tmp, push_size, 1);
                if (fret) {
                    DPRINTF("PushBack failed[%s]\n", CR_Class_NS::strerrno(fret));
                    continue;
                }

                free(p_tmp);
            } else {
                fret = queue.PushBack(next_push, k_tmp, &arg_str);
                if (fret) {
                    DPRINTF("PushOne failed[%s]\n", CR_Class_NS::strerrno(fret));
                    continue;
                }
            }

            next_pop++;
        } else if (cmd_str == "popq") {
            int64_t obj_id;
            bool value_is_null;
            int pop_timeout = atoi(arg_buf);

            fret = queue.PopFront(obj_id, k_tmp, v_tmp, value_is_null, pop_timeout);
            if (fret) {
                DPRINTF("PopFront failed[%s]\n", CR_Class_NS::strerrno(fret));
                continue;
            }

            if (v_tmp.size() < 64) {
                printf("I[%s], K[%s], V[%s]\n", CR_Class_NS::i642str(next_pop).c_str(),
                  k_tmp.c_str(), v_tmp.c_str());
            } else {
                printf("I[%s], K[%s], V.size()[%llu]\n", CR_Class_NS::i642str(next_pop).c_str(),
                  k_tmp.c_str(), (long long unsigned)v_tmp.size());
            }

            next_pop++;
        } else if (cmd_str == "loop") {
            size_t loop_count = atoll(arg_buf);
            double begin_time;
            int64_t last_k = -1;

            printf("start loop test:\n");

            begin_time = CR_Class_NS::clock_gettime();

            for (size_t i=0; i<loop_count; i++) {
//                k_tmp = CR_Class_NS::i642str(i);
//                v_tmp = CR_Class_NS::randstr(7);

                fret = fifo.PushBack(i, k_tmp, &v_tmp);
                if (fret) {
                    DPRINTF("PushOne failed[%s]\n", CR_Class_NS::strerrno(fret));
                    break;
                }
            }

            if (fret)
                continue;

            printf("  push %llu lines, usage %f second(s)\n",
              (long long unsigned)loop_count, CR_Class_NS::clock_gettime() - begin_time);

            begin_time = CR_Class_NS::clock_gettime();

            for (size_t i=0; i<loop_count; i++) {
                int64_t obj_id;
                bool value_is_null;
                fret = fifo.PopFront(obj_id, k_tmp, v_tmp, value_is_null);
                if (fret) {
                    DPRINTF("PopOne failed[%s]\n", CR_Class_NS::strerrno(fret));
                    break;
                }

                if ((size_t)obj_id != i) {
                    DPRINTF("obj_id failed, want [%lu], got [%lu]\n", i, (size_t)obj_id);
                }

/*
                if (CR_Class_NS::str2i64(k_tmp) != (last_k + 1)) {
                    DPRINTF("Want [%s], got [%s]\n", CR_Class_NS::i642str(last_k + 1).c_str(),
                      k_tmp.c_str());
                    fret = EBADMSG;
                    break;
                }
*/

                last_k++;
            }

            if (fret)
                continue;

            printf("  pop %llu lines, usage %f second(s)\n",
              (long long unsigned)loop_count, CR_Class_NS::clock_gettime() - begin_time);
        } else if (cmd_str == "loopq") {
            size_t loop_count = atoll(arg_buf);
            double begin_time;
            int64_t last_k = -1;

            printf("start loop test:\n");

            begin_time = CR_Class_NS::clock_gettime();

            for (size_t i=0; i<loop_count; i++) {
//                k_tmp = CR_Class_NS::i642str(i);
//                v_tmp = CR_Class_NS::randstr(7);

                fret = queue.PushBack(i, k_tmp, &v_tmp);
                if (fret) {
                    DPRINTF("PushBack failed[%s] at [%lu]\n", CR_Class_NS::strerrno(fret), i);
                    queue.Clear();
                    break;
                }
            }

            if (fret)
                continue;

            printf("  push %llu lines, usage %f second(s)\n",
              (long long unsigned)loop_count, CR_Class_NS::clock_gettime() - begin_time);

            begin_time = CR_Class_NS::clock_gettime();

            for (size_t i=0; i<loop_count; i++) {
                int64_t obj_id;
                bool value_is_null;
                fret = queue.PopFront(obj_id, k_tmp, v_tmp, value_is_null);
                if (fret) {
                    DPRINTF("PopFront failed[%s] at [%lu]\n", CR_Class_NS::strerrno(fret), i);
                    queue.Clear();
                    break;
                }

                if ((size_t)obj_id != i) {
                    DPRINTF("obj_id failed, want [%lu], got [%lu]\n", i, (size_t)obj_id);
                }
/*
                if (CR_Class_NS::str2i64(k_tmp) != (last_k + 1)) {
                    DPRINTF("Want [%s], got [%s]\n", CR_Class_NS::i642str(last_k + 1).c_str(),
                      k_tmp.c_str());
                    fret = EBADMSG;
                    break;
                }
*/
                last_k++;
            }

            if (fret)
                continue;

            printf("  pop %llu lines, usage %f second(s)\n",
              (long long unsigned)loop_count, CR_Class_NS::clock_gettime() - begin_time);
        } else if (cmd_str == "loopf") {
            size_t loop_count = atoll(arg_buf);
            double begin_time;
            int64_t last_k = -1;

            printf("start loop test:\n");

            begin_time = CR_Class_NS::clock_gettime();

            for (size_t i=0; i<loop_count; i++) {
//                k_tmp = CR_Class_NS::i642str(i);
//                v_tmp = CR_Class_NS::randstr(7);

                fret = fls.PushBack(i, k_tmp, &v_tmp, &free_space_left);
                if (fret) {
                    DPRINTF("PushBack failed[%s] at [%lu]\n", CR_Class_NS::strerrno(fret), i);
                    queue.Clear();
                    break;
                }
                DPRINTF("FLS freespace:%lu\n", free_space_left);
            }

            if (fret)
                continue;

            printf("  push %llu lines, usage %f second(s)\n",
              (long long unsigned)loop_count, CR_Class_NS::clock_gettime() - begin_time);

            begin_time = CR_Class_NS::clock_gettime();

            for (size_t i=0; i<loop_count; i++) {
                int64_t obj_id;
                bool value_is_null;
                fret = fls.Query(i, obj_id, k_tmp, v_tmp, value_is_null);
                if (fret) {
                    DPRINTF("PopOne failed[%s] at [%lu]\n", CR_Class_NS::strerrno(fret), i);
                    queue.Clear();
                    break;
                }

                if ((size_t)obj_id != i) {
                    DPRINTF("obj_id failed, want [%lu], got [%lu]\n", i, (size_t)obj_id);
                }
/*
                if (CR_Class_NS::str2i64(k_tmp) != (last_k + 1)) {
                    DPRINTF("Want [%s], got [%s]\n", CR_Class_NS::i642str(last_k + 1).c_str(),
                      k_tmp.c_str());
                    fret = EBADMSG;
                    break;
                }
*/
                last_k++;
            }

            if (fret)
                continue;

            printf("  pop %llu lines, usage %f second(s)\n",
              (long long unsigned)loop_count, CR_Class_NS::clock_gettime() - begin_time);
        } else if (cmd_str == "thread") {
            size_t loop_count = atoll(arg_buf);

            std::thread push_th(thread_push, &fifo, loop_count);
            std::thread pop_th(thread_pop, &fifo, loop_count);

            push_th.join();
            pop_th.join();
        } else if (cmd_str == "threadq") {
            size_t loop_count = atoll(arg_buf);

            std::thread push_th(queue_push, &queue, loop_count);
            std::thread pop_th(queue_pop, &queue, loop_count);

            push_th.join();
            pop_th.join();
        } else if (cmd_str == "quit") {
            int ret_val = atoi(arg_buf);

            return ret_val;
        }
    }

    return 0;
}

