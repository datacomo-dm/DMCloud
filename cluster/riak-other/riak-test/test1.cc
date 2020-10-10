#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <cr_class/cr_class.h>
#include <cr_class/cr_addon.h>

void
help_out(int argc, char *argv[])
{
    fprintf(stderr,
      "Usage: %s <heap_size> <item_count>\n"
      , argv[0]
    );
    exit(1);
}

void
heap_pushback(std::vector<int> &heap, int value)
{
    size_t cur_pos = heap.size();
    size_t parent_pos;

    heap.resize(cur_pos + 1);

    while(cur_pos > 0){
        parent_pos = BINTREE_PARENT(cur_pos);
        if (value < heap[parent_pos]) {
            heap[cur_pos] = heap[parent_pos];
            cur_pos = parent_pos;
        } else {
            heap[cur_pos] = value;
            return;
        }
    }
    heap[0] = value;
}

inline int
heap_front(const std::vector<int> &heap)
{
    return heap[0];
}

void
heap_popfront(std::vector<int> &heap)
{
    size_t last_pos = heap.size() - 1;
    heap[0] = heap[last_pos];
    heap[last_pos] = 0;
    size_t tmp_pos = 1;
    int cur_value = heap[0];
    while (tmp_pos < last_pos) {
        if (((tmp_pos + 1) < last_pos) && (heap[tmp_pos] > heap[tmp_pos + 1]))
            tmp_pos++;
        if (heap[tmp_pos] < cur_value) {
            heap[BINTREE_PARENT(tmp_pos)] = heap[tmp_pos];
            heap[tmp_pos] = cur_value;
            tmp_pos = BINTREE_LEFT_LEAF(tmp_pos);
        } else {
            heap.resize(last_pos);
            return;
        }
    }
    heap.resize(last_pos);
}

int
main(int argc, char *argv[])
{
    if (argc < 3)
        help_out(argc, argv);

    std::vector<int> heap;
    size_t heap_size = atoll(argv[1]);
    size_t item_count = atoll(argv[2]);
    int tmp_value;
    double begin_time;

    srand(1);

    begin_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

    for (size_t i=0; i<heap_size; i++) {
        tmp_value = rand();
        heap_pushback(heap, tmp_value);
    }

    for (size_t i=0; i<item_count; i++) {
        tmp_value = heap_front(heap);
        heap_popfront(heap);
        tmp_value = rand();
        heap_pushback(heap, tmp_value);
    }

    DPRINTF("HEAP Time Usage:%f\n", CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - begin_time);

    heap.clear();

    std::multiset<int> value_set;
    std::multiset<int>::iterator value_set_it;

    srand(1);

    begin_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

    for (size_t i=0; i<heap_size; i++) {
        value_set.insert(rand());
    }

    for (size_t i=0; i<item_count; i++) {
        value_set_it = value_set.begin();
        tmp_value = *value_set_it;
        value_set.erase(value_set_it);
        value_set.insert(rand());
    }

    DPRINTF("SET Time Usage:%f\n", CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - begin_time);

    return 0;
}
