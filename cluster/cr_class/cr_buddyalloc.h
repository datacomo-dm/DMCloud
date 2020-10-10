#ifndef __H_CR_BUDDYALLOC_H__
#define __H_CR_BUDDYALLOC_H__

#include <cr_class/cr_class.h>
#include <cr_class/cr_datacontrol.h>

class CR_BuddyAlloc {
public:
	CR_BuddyAlloc();
	~CR_BuddyAlloc();

	void Clear();

	int Init(size_t total_size=65536, size_t chunk_size=16);

	void *Alloc(size_t size, bool do_clear=true);

	int Free(void *p);

	void *ReAlloc(void *p, size_t size);

	std::string UsageInfo();

private:
	class BuddyAlloc {
	public:
		BuddyAlloc(size_t total_blocks);
		~BuddyAlloc();

		ssize_t Alloc(size_t required_blocks);
		int Free(size_t block_offset);

		size_t BlockSize(size_t block_offset);

		std::string UsageInfo();

	private:
		int8_t log2_size;
		int8_t *log2_longest;
	};

        CR_DataControl::Spin _lck;

	BuddyAlloc *_allocer;

	size_t _total_size;
	size_t _chunk_size;

	void *_data;

	inline void *_do_alloc(size_t size, bool do_clear);
	inline int _do_free(void *p);
};

#endif /* __H_CR_BUDDYALLOC_H__ */
