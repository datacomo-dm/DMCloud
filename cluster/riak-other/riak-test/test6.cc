#include <cr_class/cr_class.h>
#include <cr_class/cr_quickhash.h>

int
main(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <loop_count> <str_in>\n", argv[0]);
        return EINVAL;
    }

    size_t loop_count = atoll(argv[1]);
    std::string str_in = argv[2];
    uint32_t crcsum;
    CR_QuickHash::hash_func_t hash;

    DPRINTF("Enter\n");

    hash = CR_QuickHash::CRC32Hash;
    crcsum = 0;
    for (size_t i=0; i<loop_count; i++) {
        crcsum = hash(str_in.data(), str_in.size(), crcsum);
    }
    DPRINTF("CRC32Hash(str_in) == 0x%08X, crcsum == 0x%08X\n",
      hash(str_in.data(), str_in.size(), 0), crcsum);

    hash = CR_QuickHash::SDBMHash;
    crcsum = 0;
    for (size_t i=0; i<loop_count; i++) {
        crcsum = hash(str_in.data(), str_in.size(), crcsum);
    }
    DPRINTF("SDBMHash(str_in) == 0x%08X, crcsum == 0x%08X\n",
      hash(str_in.data(), str_in.size(), 0), crcsum);

    hash = CR_QuickHash::RSHash;
    crcsum = 0;
    for (size_t i=0; i<loop_count; i++) {
        crcsum = hash(str_in.data(), str_in.size(), crcsum);
    }
    DPRINTF("RSHash(str_in) == 0x%08X, crcsum == 0x%08X\n",
      hash(str_in.data(), str_in.size(), 0), crcsum);

    hash = CR_QuickHash::PJWHash;
    crcsum = 0;
    for (size_t i=0; i<loop_count; i++) {
        crcsum = hash(str_in.data(), str_in.size(), crcsum);
    }
    DPRINTF("PJWHash(str_in) == 0x%08X, crcsum == 0x%08X\n",
      hash(str_in.data(), str_in.size(), 0), crcsum);

    hash = CR_QuickHash::ELFHash;
    crcsum = 0;
    for (size_t i=0; i<loop_count; i++) {
        crcsum = hash(str_in.data(), str_in.size(), crcsum);
    }
    DPRINTF("ELFHash(str_in) == 0x%08X, crcsum == 0x%08X\n",
      hash(str_in.data(), str_in.size(), 0), crcsum);

    hash = CR_QuickHash::BKDRHash;
    crcsum = 0;
    for (size_t i=0; i<loop_count; i++) {
        crcsum = hash(str_in.data(), str_in.size(), crcsum);
    }
    DPRINTF("BKDRHash(str_in) == 0x%08X, crcsum == 0x%08X\n",
      hash(str_in.data(), str_in.size(), 0), crcsum);

    hash = CR_QuickHash::APHash;
    crcsum = 0;
    for (size_t i=0; i<loop_count; i++) {
        crcsum = hash(str_in.data(), str_in.size(), crcsum);
    }
    DPRINTF("APHash(str_in) == 0x%08X, crcsum == 0x%08X\n",
      hash(str_in.data(), str_in.size(), 0), crcsum);

    return 0;
}
