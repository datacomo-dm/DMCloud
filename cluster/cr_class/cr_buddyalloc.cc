#include <cr_class/cr_addon.h>
#include <cr_class/cr_buddyalloc.h>
#include <string.h>
#include <errno.h>

CR_BuddyAlloc::BuddyAlloc::BuddyAlloc(size_t total_blocks)
{
    if ((total_blocks < 1) || (!CR_IS_POW_OF_2(total_blocks))) {
        DPRINTF("total_blocks == %llu\n", (long long unsigned)total_blocks);
        throw EINVAL;
    }

    int8_t log2_node_size;

    this->log2_size = CR_LOG_2_CEIL(total_blocks);
    this->log2_longest = new int8_t[total_blocks * 2];

    log2_node_size = this->log2_size + 1;

    for (size_t i = 0; i < total_blocks * 2 - 1; i++) {
        if (CR_IS_POW_OF_2(i+1))
            log2_node_size--;
        this->log2_longest[i] = log2_node_size;
    }
}

CR_BuddyAlloc::BuddyAlloc::~BuddyAlloc()
{
    delete []this->log2_longest;
}

ssize_t
CR_BuddyAlloc::BuddyAlloc::Alloc(size_t required_blocks)
{
    size_t index = 0;
    size_t tmp_index;
    size_t offset = 0;
    int8_t log2_node_size;
    int8_t log2_l_longest, log2_r_longest;

    if (required_blocks < 1)
        required_blocks = 1;

    required_blocks = CR_ROUND_TO_POW_OF_2(required_blocks);

    if (CR_POW_OF_2(this->log2_longest[index]) < required_blocks) {
        errno = ENOMEM;
        return -1;
    }

    for(log2_node_size = this->log2_size; CR_POW_OF_2(log2_node_size) != required_blocks; log2_node_size--) {
        tmp_index = BINTREE_LEFT_LEAF(index);
        if (CR_POW_OF_2(this->log2_longest[tmp_index]) >= required_blocks)
            index = tmp_index;
        else
            index = BINTREE_RIGHT_LEAF(index);
    }

    this->log2_longest[index] = -1;

    offset = (index + 1) * CR_POW_OF_2(log2_node_size) - CR_POW_OF_2(this->log2_size);

    while (index) {
        index = BINTREE_PARENT(index);

        log2_l_longest = this->log2_longest[BINTREE_LEFT_LEAF(index)];
        log2_r_longest = this->log2_longest[BINTREE_RIGHT_LEAF(index)];

        this->log2_longest[index] = MAX(log2_l_longest, log2_r_longest);
    }

    return offset;
}

int
CR_BuddyAlloc::BuddyAlloc::Free(size_t block_offset)
{
    int8_t log2_node_size;
    int8_t log2_l_longest, log2_r_longest;
    size_t index = 0;

    if (block_offset >= CR_POW_OF_2(this->log2_size))
        return ERANGE;

    log2_node_size = 0;
    index = block_offset + CR_POW_OF_2(this->log2_size) - 1;

    for (; CR_POW_OF_2(this->log2_longest[index]) != 0 ; index = BINTREE_PARENT(index)) {
        log2_node_size++;
        if (index == 0) {
            return EALREADY;
        }
    }

    this->log2_longest[index] = log2_node_size;

    while (index) {
        index = BINTREE_PARENT(index);
        log2_node_size++;

        log2_l_longest = this->log2_longest[BINTREE_LEFT_LEAF(index)];
        log2_r_longest = this->log2_longest[BINTREE_RIGHT_LEAF(index)];

        if ((log2_l_longest + 1 == log2_node_size) && (log2_l_longest == log2_r_longest))
            this->log2_longest[index] = log2_node_size;
        else
            this->log2_longest[index] = MAX(log2_l_longest, log2_r_longest);
    }

    return 0;
}

size_t
CR_BuddyAlloc::BuddyAlloc::BlockSize(size_t block_offset)
{
    int8_t log2_node_size;
    size_t index = 0;

    if (block_offset >= CR_POW_OF_2(this->log2_size)) {
        errno = ERANGE;
        return 0;
    }

    log2_node_size = 0;

    for (
      index = block_offset + CR_POW_OF_2(this->log2_size) - 1;
      CR_POW_OF_2(this->log2_longest[index]) != 0 ;
      index = BINTREE_PARENT(index)
    ) {
        log2_node_size++;
    }

    return CR_POW_OF_2(log2_node_size);
}

std::string
CR_BuddyAlloc::BuddyAlloc::UsageInfo()
{
    char *canvas;
    size_t i,j;
    int8_t log2_node_size;
    size_t offset;
    std::string ret;
    size_t canvas_size = CR_POW_OF_2(this->log2_size);

    canvas = new char[canvas_size];

    memset(canvas,'.', canvas_size);
    log2_node_size = this->log2_size + 1;

    for (i = 0; i < 2 * canvas_size - 1; ++i) {
        if (CR_IS_POW_OF_2(i+1))
            log2_node_size--;

        if (CR_POW_OF_2(this->log2_longest[i]) == 0 ) {
            if (i >= (canvas_size - 1)) {
                canvas[i - canvas_size + 1] = '+';
            } else if (CR_POW_OF_2(this->log2_longest[BINTREE_LEFT_LEAF(i)])
              && CR_POW_OF_2(this->log2_longest[BINTREE_RIGHT_LEAF(i)])) {
                offset = (i+1) * CR_POW_OF_2(log2_node_size) - canvas_size;

                for (j = offset; j < offset + CR_POW_OF_2(log2_node_size); ++j) {
                    canvas[j] = '+';
                }
            }
        }
    }

    ret.assign(canvas, canvas_size);

    delete []canvas;

    return ret;
}

//////////////////////////

CR_BuddyAlloc::CR_BuddyAlloc()
{
    this->_allocer = NULL;
    this->_data = NULL;
}

CR_BuddyAlloc::~CR_BuddyAlloc()
{
    this->Clear();
}

void
CR_BuddyAlloc::Clear()
{
    this->_lck.lock();

    if (this->_allocer) {
        delete this->_allocer;
        this->_allocer = NULL;
    }

    if (this->_data) {
        ::free(this->_data);
        this->_data = NULL;
    }

    this->_lck.unlock();
}

int
CR_BuddyAlloc::Init(size_t total_size, size_t chunk_size)
{
    int ret = 0;

    if (total_size < 4096)
        total_size = 4096;

    if (chunk_size < sizeof(void*))
        chunk_size = sizeof(void*);

    this->_lck.lock();

    do {
        if (this->_allocer) {
            ret = EALREADY;
            break;
        }

        total_size = CR_ROUND_TO_POW_OF_2(total_size);
        chunk_size = CR_ROUND_TO_POW_OF_2(chunk_size);

        try {
            this->_data = ::calloc(total_size / chunk_size, chunk_size);
            if (!this->_data) {
                ret = ENOMEM;
                break;
            }

            this->_allocer = new BuddyAlloc(total_size / chunk_size);
        } catch (...) {
            ret = ENOMEM;
            break;
        }

        this->_total_size = total_size;
        this->_chunk_size = chunk_size;
    } while (0);

    this->_lck.unlock();

    return ret;
}

inline void *
CR_BuddyAlloc::_do_alloc(size_t size, bool do_clear)
{
    if (!this->_allocer) {
        errno = EAGAIN;
        return NULL;
    }

    size_t alloc_chunks = (size - 1 + this->_chunk_size) / this->_chunk_size;

    ssize_t chunk_pos = this->_allocer->Alloc(alloc_chunks);
    if (chunk_pos < 0)
        return NULL;

    void *ret = (void *)((uintptr_t)(this->_data) + (this->_chunk_size * chunk_pos));

    if (do_clear) {
        size_t chunk_count = this->_allocer->BlockSize(chunk_pos);
        memset(ret, 0, (this->_chunk_size * chunk_count));
    }

    return ret;
}

void *
CR_BuddyAlloc::Alloc(size_t size, bool do_clear)
{
    if (size == 0) {
        errno = 0;
        return NULL;
    }

    void *ret;

    this->_lck.lock();

    ret = this->_do_alloc(size, do_clear);

    this->_lck.unlock();

    return ret;
}

inline int
CR_BuddyAlloc::_do_free(void *p)
{
    if (!this->_allocer)
        return EAGAIN;

    size_t addr_bias = (uintptr_t)p - (uintptr_t)(this->_data);
    if ((addr_bias % this->_chunk_size) != 0)
        return EINVAL;

    return this->_allocer->Free(addr_bias / this->_chunk_size);
}

int
CR_BuddyAlloc::Free(void *p)
{
    if (!p)
        return 0;

    int ret;

    this->_lck.lock();

    ret = this->_do_free(p);

    this->_lck.unlock();

    return ret;
}

void *
CR_BuddyAlloc::ReAlloc(void *p, size_t size)
{
    if (!p)
        return this->Alloc(size, false);

    if (size == 0) {
        this->Free(p);
        errno = 0;
        return NULL;
    }

    void *ret = NULL;

    this->_lck.lock();

    do {
        if (!this->_allocer) {
            errno = EAGAIN;
            break;
        }

        size_t addr_bias = (uintptr_t)p - (uintptr_t)(this->_data);
        if ((addr_bias % this->_chunk_size) != 0) {
            errno = EINVAL;
            break;
        }

        size_t old_chunk_pos = addr_bias / this->_chunk_size;
        size_t old_block_size = this->_allocer->BlockSize(old_chunk_pos);
        if (old_block_size == 0)
            break;

        size_t new_block_size = (size - 1 + this->_chunk_size) / this->_chunk_size;

        if (new_block_size <= old_block_size) {
            ret = p;
            break;
        }

        void *new_p = this->_do_alloc(size, false);
        if (!new_p)
            break;

        ::memcpy(new_p, p, this->_chunk_size * old_block_size);

        int fret = this->_do_free(p);
        if (fret) {
            errno = fret;
            break;
        }

        ret = new_p;
    } while (0);

    this->_lck.unlock();

    return ret;
}

std::string
CR_BuddyAlloc::UsageInfo()
{
    std::string ret;

    this->_lck.lock();

    if (this->_allocer) {
        ret = this->_allocer->UsageInfo();
    }

    this->_lck.unlock();

    return ret;
}
