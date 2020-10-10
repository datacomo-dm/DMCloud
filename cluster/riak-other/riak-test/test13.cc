#include <stdio.h>
#include <stdint.h>
#include <cr_class/cr_class.h>

int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <align_size> <lineid_begin> [val0 [val1 [...]]]\n", argv[0]);
        exit(EINVAL);
    }

    size_t align_size = atoll(argv[1]);
    int64_t lineid_begin = atoll(argv[2]);
    std::vector<uint64_t> arr;
    uint64_t total_val_b = 0;
    uint64_t total_val_e = 0;

    for (int i=3; i<argc; i++) {
        arr.push_back(atoll(argv[i]));
    }

    printf("old_arr[%lu] = {", (long unsigned)arr.size());
    for (size_t i=0; i<arr.size(); i++) {
        printf("%llu(%u, %u), ", (long long unsigned)arr[i], (unsigned)(arr[i] / align_size),
          (unsigned)(arr[i] % align_size));
        total_val_b += arr[i];
    }
    printf("}\n");

    arr[0] += lineid_begin;

    CR_Class_NS::align_uintarr(arr.data(), arr.size(), align_size);

    arr[0] -= lineid_begin;

    printf("new_arr[%lu] = {", (long unsigned)arr.size());
    for (size_t i=0; i<arr.size(); i++) {
        printf("%llu(%u, %u), ", (long long unsigned)arr[i], (unsigned)(arr[i] / align_size),
          (unsigned)(arr[i] % align_size));
        total_val_e += arr[i];
    }
    printf("}\n");

    printf("TB == %llu, TE == %llu, (TB == TE) == %d\n",
      (long long unsigned)total_val_b, (long long unsigned)total_val_e,
      (total_val_b == total_val_e));

    return 0;
}
