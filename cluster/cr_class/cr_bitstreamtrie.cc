#include <list>
#include <cr_class/cr_bitstreamtrie.h>

#define THIS_ROOT_P				((_NodeT*)(this->root_p))
#define THIS_POOL_P				((CR_Pool<_NodeT>*)(this->pool_p))

//////////////////////
//////////////////////

class _NodeT {
public:
    _NodeT(CR_BitStreamTrie::node_t parent_p=NULL,
      const bool is_right_leaf=false, const bool is_fake_node=false);

    const _NodeT *_parent_p;
    _NodeT *_leftleaf_p;
    _NodeT *_rightleaf_p;
    _NodeT *_cached_valued_prev_p;
    _NodeT *_cached_valued_next_p;
    uint32_t _key_bit_len;
    bool _is_right_leaf;
    bool _has_value;
    bool _is_fake_node;
    uintptr_t _value;
};

//////////////////////

_NodeT::_NodeT(CR_BitStreamTrie::node_t parent_p, const bool is_right_leaf, const bool is_fake_node)
{
    this->_parent_p = (const _NodeT*)parent_p;
    this->_leftleaf_p = NULL;
    this->_rightleaf_p = NULL;
    this->_cached_valued_prev_p = NULL;
    this->_cached_valued_next_p = NULL;
    this->_has_value = false;
    this->_is_right_leaf = is_right_leaf;
    this->_is_fake_node = is_fake_node;
    if (this->_parent_p)
        this->_key_bit_len = this->_parent_p->_key_bit_len + 1;
    else
        this->_key_bit_len = 0;
}

//////////////////////
//////////////////////

CR_BitStreamTrie::CR_BitStreamTrie()
{
    this->pool_p = new CR_Pool<_NodeT>(8192);
    this->root_p = THIS_POOL_P->Get();
    this->is_indexed = false;
}

CR_BitStreamTrie::~CR_BitStreamTrie()
{
    delete THIS_POOL_P;
}

CR_BitStreamTrie::node_t
CR_BitStreamTrie::_prev_node(node_t node_p) const
{
    const _NodeT *cur_node = (const _NodeT*)node_p;
    const _NodeT *parent_node = cur_node->_parent_p;
    const _NodeT *rightmost_node;
    const _NodeT *left_sibling;
    if (!parent_node)
        return NULL;
    if (!cur_node->_is_right_leaf)
        return parent_node;
    left_sibling = parent_node->_leftleaf_p;
    if (!left_sibling)
        return parent_node;
    cur_node = left_sibling;
    while (cur_node) {
        rightmost_node = cur_node->_rightleaf_p;
        if (!rightmost_node) {
            rightmost_node = cur_node->_leftleaf_p;
            if (!rightmost_node)
                return cur_node;
        }
        cur_node = rightmost_node;
    }
    return NULL;
}

CR_BitStreamTrie::node_t
CR_BitStreamTrie::_next_node(node_t node_p) const
{
    const _NodeT *cur_node = (const _NodeT*)node_p;
    const _NodeT *parent_node;
    const _NodeT *right_sibling;
    if (cur_node->_leftleaf_p)
        return cur_node->_leftleaf_p;
    if (cur_node->_rightleaf_p)
        return cur_node->_rightleaf_p;
    while (cur_node) {
        parent_node = cur_node->_parent_p;
        if (!parent_node)
            break;
        if (!(cur_node->_is_right_leaf)) {
            right_sibling = parent_node->_rightleaf_p;
            if (right_sibling)
                return right_sibling;
        }
        cur_node = parent_node;
    }
    return NULL;
}

CR_BitStreamTrie::node_t
CR_BitStreamTrie::_valued_prev_node(node_t node_p) const
{
    const _NodeT * cur_node = (const _NodeT *)node_p;
    if (cur_node->_is_fake_node)
        cur_node = (const _NodeT *)this->_prev_node(cur_node);
    while (cur_node) {
        if (this->is_indexed)
            return cur_node->_cached_valued_prev_p;
        cur_node = (const _NodeT *)this->_prev_node(cur_node);
        if (!cur_node)
            break;
        if (cur_node->_has_value)
            break;
    }
    return cur_node;
}

CR_BitStreamTrie::node_t
CR_BitStreamTrie::_valued_next_node(node_t node_p) const
{
    const _NodeT * cur_node = (const _NodeT *)node_p;
    if (cur_node->_is_fake_node)
        cur_node = (const _NodeT *)this->_next_node(cur_node);
    while (cur_node) {
        if (this->is_indexed)
            return cur_node->_cached_valued_next_p;
        cur_node = (const _NodeT *)this->_next_node(cur_node);
        if (!cur_node)
            break;
        if (cur_node->_has_value)
            break;
    }
    return cur_node;
}

int
CR_BitStreamTrie::do_set(const void * key_p, const size_t key_size, uintptr_t v)
{
    _NodeT *cur_node = THIS_ROOT_P;
    _NodeT *nxt_node;
    uint8_t one_byte;

    for (size_t i=0; i<key_size; i++) {
        one_byte = ((uint8_t*)key_p)[i];

        for (int j=7; j>=0; j--) {
            if (one_byte & (1 << j)) {
                nxt_node = cur_node->_rightleaf_p;
                if (!nxt_node) {
                    nxt_node = THIS_POOL_P->Get(cur_node, true);
                    cur_node->_rightleaf_p = nxt_node;
                    this->is_indexed = false;
                }
            } else {
                nxt_node = cur_node->_leftleaf_p;
                if (!nxt_node) {
                    nxt_node = THIS_POOL_P->Get(cur_node, false);
                    cur_node->_leftleaf_p = nxt_node;
                    this->is_indexed = false;
                }
            }

            cur_node = nxt_node;
        }
    }
    cur_node->_has_value = true;
    cur_node->_value = v;

    return 0;
}

CR_BitStreamTrie::node_t
CR_BitStreamTrie::do_find(const void * key_p, const size_t key_size, const bool lower_bound) const
{
    node_t cur_node = THIS_ROOT_P;
    node_t nxt_node;
    uint8_t one_byte;

    for (size_t i=0; i<key_size; i++) {
        one_byte = ((uint8_t*)key_p)[i];

        for (int j=7; j>=0; j--) {
            if (one_byte & (1 << j)) {
                nxt_node = ((const _NodeT*)cur_node)->_rightleaf_p;
                if (!nxt_node) {
                    if (lower_bound) {
                        _NodeT fake_node(cur_node, true, true);
                        cur_node = this->_valued_next_node(&fake_node);
                        return cur_node;
                    }
                    return NULL;
                }
            } else {
                nxt_node = ((const _NodeT*)cur_node)->_leftleaf_p;
                if (!nxt_node) {
                    if (lower_bound) {
                        _NodeT fake_node(cur_node, false, true);
                        cur_node = this->_valued_next_node(&fake_node);
                        return cur_node;
                    }
                    return NULL;
                }
            }
            cur_node = nxt_node;
        }
    }
    return cur_node;
}

uintptr_t
CR_BitStreamTrie::do_get(const void * key_p, const size_t key_size, const uintptr_t dv,
    const bool lower_bound) const
{
    const _NodeT *find_node = (const _NodeT*)this->do_find(key_p, key_size, lower_bound);

    if (find_node)
        if (find_node->_has_value)
            return find_node->_value;

    return dv;
}

CR_BitStreamTrie::node_t
CR_BitStreamTrie::do_begin() const
{
    node_t cur_node = THIS_ROOT_P;
    while (cur_node) {
        if (((const _NodeT*)cur_node)->_has_value)
            break;
        cur_node = this->_next_node(cur_node);
    }
    return cur_node;
}

CR_BitStreamTrie::node_t
CR_BitStreamTrie::do_prev(node_t node_p) const
{
    return this->_valued_prev_node(node_p);
}

CR_BitStreamTrie::node_t
CR_BitStreamTrie::do_next(node_t node_p) const
{
    return this->_valued_next_node(node_p);
}

int
CR_BitStreamTrie::Set(const void * key_p, const size_t key_size, uintptr_t v)
{
    int ret;

    this->_lck.lock();
    ret = this->do_set(key_p, key_size, v);
    this->_lck.unlock();

    return ret;
}

uintptr_t
CR_BitStreamTrie::Get(const void * key_p, const size_t key_size, const uintptr_t dv, const bool lower_bound)
{
    uintptr_t ret;

    this->_lck.lock();
    ret = this->do_get(key_p, key_size, dv, lower_bound);
    this->_lck.unlock();

    return ret;
}

CR_BitStreamTrie::node_t
CR_BitStreamTrie::Find(const void * key_p, const size_t key_size, const bool lower_bound)
{
    CR_BitStreamTrie::node_t ret;

    this->_lck.lock();
    ret = this->do_find(key_p, key_size, lower_bound);
    this->_lck.unlock();

    return ret;
}

CR_BitStreamTrie::node_t
CR_BitStreamTrie::Begin()
{
    CR_BitStreamTrie::node_t ret;

    this->_lck.lock();
    ret = this->do_begin();
    this->_lck.unlock();

    return ret;
}

CR_BitStreamTrie::node_t
CR_BitStreamTrie::End() const
{
    return NULL;
}

CR_BitStreamTrie::node_t
CR_BitStreamTrie::Prev(node_t node_p)
{
    CR_BitStreamTrie::node_t ret;

    this->_lck.lock();
    ret = this->do_prev(node_p);
    this->_lck.unlock();

    return ret;
}

CR_BitStreamTrie::node_t
CR_BitStreamTrie::Next(node_t node_p)
{
    CR_BitStreamTrie::node_t ret;

    this->_lck.lock();
    ret = this->do_next(node_p);
    this->_lck.unlock();

    return ret;
}

uintptr_t
CR_BitStreamTrie::Value(node_t node_p, const uintptr_t dv) const
{
    const _NodeT *cur_node = (const _NodeT*)node_p;
    if (!cur_node)
        return dv;
    return cur_node->_value;
}

std::string
CR_BitStreamTrie::do_key(node_t node_p) const
{
    const _NodeT *cur_node = (const _NodeT*)node_p;

    if (!cur_node)
        return std::string("");

    std::string ret;
    size_t bit_len = cur_node->_key_bit_len;
    size_t key_len = bit_len / 8;
    char *buf = new char[key_len];

    for (ssize_t i=(bit_len-1); i>=0; i--) {
        if (cur_node->_is_right_leaf) {
            CR_Class_NS::BMP_BITSET_N(buf, key_len, i);
        } else {
            CR_Class_NS::BMP_BITCLEAR_N(buf, key_len, i);
        }
        cur_node = cur_node->_parent_p;
    }

    ret.assign(buf, key_len);

    delete []buf;

    return ret;
}

std::string
CR_BitStreamTrie::Key(node_t node_p)
{
    std::string ret;

    this->_lck.lock();
    ret = this->do_key(node_p);
    this->_lck.unlock();

    return ret;
}

void
CR_BitStreamTrie::BuildIndex()
{
    _NodeT * valued_prev_node = NULL;
    std::list<_NodeT *> no_value_prev_nodes;
    _NodeT * tmp_node;

    this->_lck.lock();

    _NodeT * cur_node = THIS_ROOT_P;
    while (cur_node) {
        cur_node->_cached_valued_prev_p = valued_prev_node;
        if (cur_node->_has_value) {
            while (!no_value_prev_nodes.empty()) {
                tmp_node = no_value_prev_nodes.front();
                no_value_prev_nodes.pop_front();
                tmp_node->_cached_valued_next_p = cur_node;
            }
            valued_prev_node = cur_node;
        } else {
            cur_node->_cached_valued_next_p = NULL;
            no_value_prev_nodes.push_back(cur_node);
        }
        cur_node = (_NodeT*)this->_next_node(cur_node);
    }

    this->is_indexed = true;

    this->_lck.unlock();
}
