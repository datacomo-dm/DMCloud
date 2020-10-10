#ifndef __CR_POOL_H__
#define __CR_POOL_H__

#include <cr_class/cr_addon.h>
#include <unistd.h>
#include <cr_class/queue.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_datacontrol.h>

class CR_QueuePool {
public:
	CR_QueuePool(size_t obj_size, size_t mb_nchunks=1024,
		const bool do_mem_check=false, const bool do_round_mb_size=false);
	~CR_QueuePool();

	void *Get();
	int Put(void *p);

	void Clear();

	size_t get_mc_size();
	size_t get_mb_nchunks();
	size_t get_mb_count();
	size_t get_mb_total_size();
private:
	typedef struct memblock {
		LIST_ENTRY(memblock) list_entry;
		LIST_ENTRY(memblock) usable_list_entry;
		size_t mc_unused_count;
		void **mc_freed_list_head;
		size_t mc_next_pos_of_ptr_arr;
		uintptr_t mb_gap_data;
		void *_gap_data;
		void *mc_data[];
	} memblock_t;

	CR_DataControl::Spin _spin;

	bool _mem_check;

	LIST_HEAD(memblock_list, memblock) _mb_all_list_head;
	LIST_HEAD(memblock_usable_list, memblock) _mb_usable_list_head;
	memblock_t *_mb_last_usable;

	size_t _mc_size_of_ptr_arr;
	size_t _mc_gape_pos_of_ptr_arr;
	size_t _mc_last_pos_of_ptr_arr;

	void *_gap_value;

	size_t _mb_alloc_size;
	size_t _mb_nchunks;

	size_t _mb_count;

	void *_do_get();
};

template<typename T>
class CR_Pool {
public:
	CR_Pool(const size_t mb_nchunks=1024, const bool do_mem_check=false, const bool do_round_mb_size=false)
	{
		this->_pool_p = new CR_QueuePool(sizeof(T), mb_nchunks, do_mem_check, do_round_mb_size);
	}

	~CR_Pool()
	{
		delete this->_pool_p;
	}

	template<typename... Args>
	T *Get(Args... args)
	{
		void *_tmp_p = this->_pool_p->Get();
		if (_tmp_p)
			return new (_tmp_p) T(args...);
		return NULL;
	}

	void Put(T *p)
	{
		if (p) {
			p->~T();
			this->_pool_p->Put(p);
		}
	}

	void Clear()
	{
		this->_pool_p->Clear();
	}

private:
	CR_QueuePool *_pool_p;
};

#endif /* __CR_POOL_H__ */
