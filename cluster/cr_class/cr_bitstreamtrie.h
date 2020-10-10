#ifndef __H_CR_BITSTREAMTRIE_H__
#define __H_CR_BITSTREAMTRIE_H__

#include <sys/types.h>
#include <string>
#include <cr_class/cr_datacontrol.h>
#include <cr_class/cr_pool.h>

class CR_BitStreamTrie {
public:
	typedef const void * node_t;

	CR_BitStreamTrie();
	~CR_BitStreamTrie();

	void BuildIndex();

	int Set(const void * key_p, const size_t key_size, const uintptr_t v);
	uintptr_t Get(const void * key_p, const size_t key_size, const uintptr_t dv=0,
		const bool lower_bound=false);

	node_t Find(const void * key_p, const size_t key_size, const bool lower_bound=false);
	node_t Begin();
	node_t End() const;
	node_t Prev(node_t node_p);
	node_t Next(node_t node_p);
	std::string Key(node_t node_p);
	uintptr_t Value(node_t node_p, const uintptr_t dv=0) const;
private:
	CR_DataControl::Spin _lck;

	void *pool_p;
	void *root_p;

	bool is_indexed;

	int do_set(const void * key_p, const size_t key_size, uintptr_t v);
	uintptr_t do_get(const void * key_p, const size_t key_size, const uintptr_t dv,
		const bool lower_bound) const;

	node_t do_find(const void * key_p, const size_t key_size, const bool lower_bound) const;
	node_t do_begin() const;
	node_t do_prev(node_t node_p) const;
	node_t do_next(node_t node_p) const;

	std::string do_key(node_t node_p) const;

	node_t _prev_node(node_t node_p) const;
	node_t _next_node(node_t node_p) const;
	node_t _valued_prev_node(node_t node_p) const;
	node_t _valued_next_node(node_t node_p) const;
};

#endif /* __H_CR_BITSTREAMTRIE_H__ */
