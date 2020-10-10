#include <cr_class/cr_addon.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <zlib.h>
#include <cr_class/cr_pool.h>

///////////////////////////////////////////////
//                CR_QueuePool               //
///////////////////////////////////////////////

CR_QueuePool::CR_QueuePool(size_t obj_size, size_t mb_nchunks,
    const bool do_mem_check, const bool do_round_mb_size)
{
    if (obj_size < 1) {
        obj_size = 1;
    }

    if (mb_nchunks < 1) {
        mb_nchunks = 1;
    }

    this->_mem_check = do_mem_check;
    this->_mb_count = 0;

    LIST_INIT(&(this->_mb_all_list_head));
    LIST_INIT(&(this->_mb_usable_list_head));

    this->_gap_value = (void *)(CR_Class_NS::rand());

    if (this->_mem_check) {
        this->_mc_gape_pos_of_ptr_arr = (obj_size - 1) / sizeof(void *) + 3;
        this->_mc_size_of_ptr_arr = (obj_size - 1) / sizeof(void *) + 4;
    } else {
        this->_mc_size_of_ptr_arr = (obj_size - 1) / sizeof(void *) + 2;
    }

    size_t mc_size = this->_mc_size_of_ptr_arr * sizeof(void *);

    if (do_round_mb_size) {
        size_t mb_alloc_size_tmp = CR_ROUND_TO_POW_OF_2(
          CR_ALIGN(sizeof(memblock_t)) + (mc_size * mb_nchunks) + (sizeof(void*) << 1));
        this->_mb_nchunks =
          (mb_alloc_size_tmp - CR_ALIGN(sizeof(memblock_t)) - (sizeof(void*) << 1)) / mc_size;
    } else {
        this->_mb_nchunks = mb_nchunks;
    }

    this->_mc_last_pos_of_ptr_arr = this->_mc_size_of_ptr_arr * this->_mb_nchunks;
    this->_mb_alloc_size = CR_ALIGN(sizeof(memblock_t)) + (mc_size * this->_mb_nchunks);
}

CR_QueuePool::~CR_QueuePool()
{
    this->Clear();
}

void
CR_QueuePool::Clear()
{
    memblock_t *mb_p;

    this->_spin.lock();

    while (!LIST_EMPTY(&(this->_mb_all_list_head))) {
        mb_p = LIST_FIRST(&(this->_mb_all_list_head));
        LIST_REMOVE(mb_p, list_entry);
        free(mb_p);
    }

    LIST_INIT(&(this->_mb_usable_list_head));

    this->_mb_count = 0;

    this->_spin.unlock();
}

void *
CR_QueuePool::Get()
{
    memblock_t *mb_p;
    void **mc_p = NULL;
    void *ret = NULL;
    bool mem_check = this->_mem_check;
    bool mc_add = false;

    this->_spin.lock();

    mb_p = LIST_FIRST(&(this->_mb_usable_list_head));

    if (!mb_p) {
        this->_spin.unlock();

        mb_p = (memblock_t *)malloc(this->_mb_alloc_size);

        if (mb_p) {
            mb_p->mb_gap_data = (uintptr_t)(this->_gap_value) ^ (uintptr_t)mb_p;
            mb_p->_gap_data = this->_gap_value;
            mb_p->mc_freed_list_head = NULL;
            mb_p->mc_unused_count = this->_mb_nchunks;
            mb_p->mc_next_pos_of_ptr_arr = 0;
        }

        this->_spin.lock();

        if (mb_p) {
            LIST_INSERT_HEAD(&(this->_mb_all_list_head), mb_p, list_entry);
            LIST_INSERT_HEAD(&(this->_mb_usable_list_head), mb_p, usable_list_entry);
            this->_mb_count++;
        }
    }

    if (mb_p) {
        mc_p = mb_p->mc_freed_list_head;
        if (mc_p) {
            mb_p->mc_freed_list_head = (void **)(mc_p[0]);
            mb_p->mc_unused_count--;
            if (mb_p->mc_unused_count == 0) {
                LIST_REMOVE(mb_p, usable_list_entry);
            }
        } else if (mb_p->mc_next_pos_of_ptr_arr < this->_mc_last_pos_of_ptr_arr) {
            mc_p = &(mb_p->mc_data[mb_p->mc_next_pos_of_ptr_arr]);
            mb_p->mc_next_pos_of_ptr_arr += this->_mc_size_of_ptr_arr;
            mb_p->mc_unused_count--;
            mc_add = true;
            if (mb_p->mc_unused_count == 0) {
                LIST_REMOVE(mb_p, usable_list_entry);
            }
        }
    }

    this->_spin.unlock();

    if (mc_p) {
        mc_p[0] = mb_p;

        if (mem_check) {
            if (mc_add) {
                mc_p[1] = this->_gap_value;
                mc_p[_mc_gape_pos_of_ptr_arr] = this->_gap_value;
            }
            ret = &(mc_p[2]);
        } else {
            ret = &(mc_p[1]);
        }
    }

    return ret;
}

int
CR_QueuePool::Put(void *p)
{
    int ret = 0;
    memblock_t *mb_p;
    void **mc_p;
    bool do_free;
    bool mem_check = this->_mem_check;

    if (!p) {
        return 0;
    }

    if (mem_check) {
        mc_p = &(((void **)p)[-2]);
        if (mc_p[1] != this->_gap_value) {
            THROWF("memchunk[%p]->gap_begin[%p] == %p\n",
              mc_p[0], (&(mc_p[1])), (mc_p[1]));
            return EOVERFLOW;
        }
        if (mc_p[this->_mc_gape_pos_of_ptr_arr] != this->_gap_value) {
            THROWF("memchunk[%p]->gap_end[%p] == %p\n",
              mc_p[0], (&(mc_p[_mc_gape_pos_of_ptr_arr])),
              (mc_p[this->_mc_gape_pos_of_ptr_arr]));
            return EOVERFLOW;
        }
    } else {
        mc_p = &(((void **)p)[-1]);
    }

    mb_p = (memblock_t *)(mc_p[0]);
    if ((!mb_p) || (mb_p->mb_gap_data != ((uintptr_t)(this->_gap_value) ^ (uintptr_t)mb_p))) {
        THROWF("Invaild memblock point[%p] of memchunk[%p], maybe double free.\n", mb_p, mc_p);
        return EFAULT;
    }

    do_free = false;

    this->_spin.lock();

    mb_p->mc_unused_count++;
    if (mb_p->mc_unused_count == 1) {
        LIST_INSERT_HEAD(&(this->_mb_usable_list_head), mb_p, usable_list_entry);
    } else if ((mb_p->mc_unused_count == this->_mb_nchunks)
      && ((LIST_FIRST(&(this->_mb_usable_list_head)) != mb_p) || (LIST_NEXT(mb_p, usable_list_entry) != NULL))) {
        LIST_REMOVE(mb_p, list_entry);
        LIST_REMOVE(mb_p, usable_list_entry);
        this->_mb_count--;
        do_free = true;
    }

    if (!do_free) {
        mc_p[0] = mb_p->mc_freed_list_head;
        mb_p->mc_freed_list_head = mc_p;
    }

    this->_spin.unlock();

    if (do_free) {
        free(mb_p);
    }

    return ret;
}

size_t
CR_QueuePool::get_mc_size()
{
    size_t ret;

    this->_spin.lock();

    ret = this->_mc_size_of_ptr_arr * sizeof(void *);

    this->_spin.unlock();

    return ret;
}

size_t
CR_QueuePool::get_mb_nchunks()
{
    size_t ret;

    this->_spin.lock();

    ret = this->_mb_nchunks;

    this->_spin.unlock();

    return ret;
}

size_t
CR_QueuePool::get_mb_count()
{
    size_t ret;

    this->_spin.lock();

    ret = this->_mb_count;

    this->_spin.unlock();

    return ret;
}

size_t
CR_QueuePool::get_mb_total_size()
{
    size_t ret;

    this->_spin.lock();

    ret = this->_mb_alloc_size * this->_mb_count;

    this->_spin.unlock();

    return ret;
}
