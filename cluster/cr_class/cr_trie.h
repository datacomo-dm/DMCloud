#ifndef __H_CR_TRIE_H__
#define __H_CR_TRIE_H__

#include <sys/types.h>
#include <string>
#include <list>
#include <cr_class/cr_datacontrol.h>
#include <cr_class/cr_pool.h>

template<typename T, const uint8_t L=0U, const uint8_t R=UINT8_MAX>
class CR_Trie {
private:
	struct node {
		node *_parent_p;
		uint8_t _key_byte;
		bool _has_value;
		node *_children[(int)R-L+1];
		node *_valued_prev_p;
		node *_valued_next_p;
		size_t _ref_count;
		size_t _key_size;
		T _value;

		node(node *parent_p=NULL, uint8_t key_byte='\0')
		{
			this->_parent_p = parent_p;
			if (_parent_p)
				this->_key_size = _parent_p->_key_size + 1;
			else
				this->_key_size = 0;
			this->_valued_prev_p = NULL;
			this->_valued_next_p = NULL;
			this->_key_byte = key_byte;
			this->_ref_count = 0;
			this->_has_value = false;
			for (size_t i=0; i<=(int)R-L; i++) {
				this->_children[i] = NULL;
			}
		}

		~node()
		{
			for (size_t i=0; i<=(int)R-L; i++) {
				if (this->_children[i]) {
					delete this->_children[i];
				}
			}
		}

		std::string key() const
		{
			char key_buf[this->_key_size];
			const node *np = this;
			ssize_t key_pos = this->_key_size - 1;
			while (np && (key_pos >= 0)) {
				key_buf[key_pos] = (char)np->_key_byte;
				np = np->_parent_p;
				key_pos--;
			}
			return std::string(key_buf, this->_key_size);
		}
	};

public:
	class iterator {
	friend CR_Trie;
	public:
		iterator()
		{
			this->_np = NULL;
		}

		inline operator bool()
		{
			return (this->_np != NULL);
		}

		inline T & operator *()
		{
			return this->_np->_value;
		}

		inline void operator =(const T &d)
		{
			this->_np->_value = d;
		}

		inline iterator & operator ++()
		{
			if (this->_np)
				this->_np = this->_np->_valued_next_p;
			return *this;
		}

		inline iterator & operator --()
		{
			if (this->_np)
				this->_np = this->_np->_valued_prev_p;
			return *this;
		}

		std::string key()
		{
			std::string ret;
			if (this->_np)
				ret = this->_np->key();
			return ret;
		}

	private:
		node *_np;
	};

	CR_Trie()
	{
		this->_size = 0;
	}

	~CR_Trie()
	{
		this->clear();
	}

	inline size_t size()
	{
		return this->_size;
	}

	inline iterator begin()
	{
		iterator ret;
		ret._np = &(this->_root);
		return ret;
	}

	inline iterator end()
	{
		iterator ret;
		return ret;
	}

	inline void clear()
	{
		for(size_t i=0; i<=(int)R-L; i++) {
			if(this->_root._children[i]){
				delete this->_root._children[i];
				this->_root._children[i] = NULL;
			}
		}
	}

	inline iterator find(const std::string &key)
	{
		return this->find(key.data(), key.size());
	}

	// find_mode = 0 : it.key() == key
	// find_mode < 0 : it.key() <= key
	// find_mode > 0 : it.key() >= key
	inline iterator find(const void *key_p, const size_t key_size, const int find_mode=0)
	{
		iterator it;
		node *np = &(this->_root);
		for (size_t i=0; i<key_size; i++){
			uint8_t key_byte = ((const uint8_t *)key_p)[i];
#if defined(DIAGNOSTIC)
			if (((int)key_byte < (int)L) || (key_byte > R))
				THROWF("Illegal byte '%c' at key[%u]\n", key_byte, (unsigned)i);
#endif // DIAGNOSTIC
			node *nnp = np->_children[(int)key_byte-L];
			if(!nnp) {
				if (find_mode != 0) {
					if (find_mode < 0) {
						it._np = np->_valued_prev_p;
					} else {
						it._np = np->_valued_next_p;
					}
				}
				return it;
			}
			np = nnp;
		}
		if(np->_has_value) {
			it._np = np;
		} else {
			if (find_mode < 0) {
				it._np = np->_valued_prev_p;
			} else if (find_mode > 0) {
				it._np = np->_valued_next_p;
			}
		}
		return it;
	}

	inline const T & get(const std::string &key, const T & default_v, const int find_mode=0)
	{
		return this->get(key.data(), key.size(), default_v, find_mode);
	}

	inline const T & get(const void *key_p, const size_t key_size,
		const T & default_v, const int find_mode=0)
	{
		iterator it = this->find(key_p, key_size, find_mode);
		if (it)
			return *it;
		else
			return default_v;
	}

	inline iterator insert(const std::string &key, const T & v)
	{
		return this->insert(key.data(), key.size(), v);
	}

	inline iterator insert(const void *key_p, const size_t key_size, const T & v)
	{
		iterator it = this->find(key_p, key_size);
		if (it) {
			it = v;
			return it;
		}
		node *np = &(this->_root);
		for (size_t i=0; i<key_size; i++){
			uint8_t key_byte = ((const uint8_t *)key_p)[i];
#if defined(DIAGNOSTIC)
			if (((int)key_byte < (int)L) || (key_byte > R))
				THROWF("Illegal byte '%c' at key[%u]\n", key_byte, (unsigned)i);
#endif // DIAGNOSTIC
			node *nnp = np->_children[(int)key_byte-L];
			if(!nnp) {
				nnp = new node(np, key_byte);
				np->_children[(int)key_byte-L] = nnp;
			}
			np = nnp;
			np->_ref_count++;
		}
		np->_value = v;
		np->_has_value = true;
		this->_size++;
		this->_index();
		return it;
	}

	inline T &operator[](const std::string &key)
	{
		iterator it = this->find(key);
		if (!it)
			it = this->insert(key, T());
		return it._np->_value;
	}

	inline ssize_t erase(const std::string &key)
	{
		return this->erase(key.data(), key.size());
	}

	inline ssize_t erase(const void *key_p, const size_t key_size)
	{
		if (!this->find(key_p, key_size))
			return 0;
		node *np = &(this->_root);
		this->_size--;
		for (size_t i=0; i<key_size; i++) {
			uint8_t key_byte = ((const uint8_t *)key_p)[i];
#if defined(DIAGNOSTIC)
			if (((int)key_byte < (int)L) || (key_byte > R))
				THROWF("Illegal byte '%c' at key[%u]\n", key_byte, (unsigned)i);
#endif // DIAGNOSTIC
			if(i && !np->_ref_count) {
				np->_parent_p->_children[(int)(np->_key_byte)-L]=NULL;
				delete np;
				return 1;
			}
			np = np->_children[(int)key_byte-L];
			np->_ref_count--;
		}
		np->_has_value = false;
		this->_index();
		return 1;
	}

private:
	inline node *_left_child(node *np, uint8_t start_key_byte) const
	{
		if (np) {
			for (int i=((int)start_key_byte-L-1); i>=0; i--) {
				if (np->_children[i])
					return np->_children[i];
			}
		}
		return NULL;
	}

	inline node *_right_child(node *np, uint8_t start_key_byte) const
	{
		if (np) {
			for (int i=((int)start_key_byte-L+1); i<=(int)R-L; i++) {
				if (np->_children[i])
					return np->_children[i];
			}
		}
		return NULL;
	}

	inline node *_left_sibling(node *np) const
	{
		if (np)
			return this->_left_child(np->_parent_p, np->_key_byte);
		return NULL;
	}

	inline node *_right_sibling(node *np) const
	{
		if (np)
			return this->_right_child(np->_parent_p, np->_key_byte);
		return NULL;
	}

	inline node *_leftmost_child(node *np) const
	{
		if (np) {
			for (int i=0; i<=(int)R-L; i++) {
				if (np->_children[i])
					return np->_children[i];
			}
		}
		return NULL;
	}

	inline node *_rightmost_child(node *np) const
	{
		if (np) {
			for (int i=(int)R-L; i>=0; i--) {
				if (np->_children[i])
					return np->_children[i];
			}
		}
		return NULL;
	}

	inline node *_tree_prev(node *np) const
	{
		if (!np)
			return NULL;
		node *cur_node = this->_left_sibling(np);
		if (!cur_node)
			return np->_parent_p;
		node *rightmost;
		while (cur_node) {
			rightmost = this->_rightmost_child(cur_node);
			if (!rightmost)
				break;
			cur_node = rightmost;
		}
		return cur_node;
	}

	inline node *_tree_next(node *np) const
	{
		if (!np)
			return NULL;
		node *cur_node = this->_leftmost_child(np);
		if (cur_node)
			return cur_node;
		node *right_sibling;
		cur_node = np;
		while (cur_node) {
			right_sibling = this->_right_sibling(cur_node);
			if (right_sibling) {
				cur_node = right_sibling;
				break;
			}
			cur_node = cur_node->_parent_p;
		}
		return cur_node;
	}

	inline node *_valued_prev(node *np) const
	{
		while (np) {
			np = this->_tree_prev(np);
			if (!np || np->_has_value)
				break;
		}
		return np;
	}

	inline node *_valued_next(node *np) const
	{
		while (np) {
			np = this->_tree_next(np);
			if (!np || np->_has_value)
				break;
		}
		return np;
	}

	void _index()
	{
		node * valued_prev_node = NULL;
		std::list<node*> no_value_prev_nodes;
		node * tmp_node;

		node * cur_node = &(this->_root);
		while (cur_node) {
			cur_node->_valued_prev_p = valued_prev_node;
			if (cur_node->_has_value) {
				while (!no_value_prev_nodes.empty()) {
					tmp_node = no_value_prev_nodes.front();
					no_value_prev_nodes.pop_front();
					tmp_node->_valued_next_p = cur_node;
				}
				valued_prev_node = cur_node;
			} else {
				cur_node->_valued_next_p = NULL;
				no_value_prev_nodes.push_back(cur_node);
			}
			cur_node = this->_tree_next(cur_node);
		}
	}

	node _root;
	size_t _size;
};

#endif /* __H_CR_TRIE_H__ */
