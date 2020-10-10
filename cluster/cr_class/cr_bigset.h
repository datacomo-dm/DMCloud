#ifndef __H_CR_BIGSET_H__
#define __H_CR_BIGSET_H__

#include <cr_class/cr_addon.h>
#include <vector>
#include <cr_class/tree.h>
#include <cr_class/cr_datacontrol.h>
#include <cr_class/cr_pool.h>

template<typename T>
class CR_BigSet {
public:
	CR_BigSet()
	{
		this->_pool_p = new CR_Pool<_node_t>;
		RB_INIT(&(this->_rb_head));
	}

	~CR_BigSet()
	{
		delete this->_pool_p;
	}

	int Add(const T &v)
	{
		return this->Add(v, v);
	}

	int Add(const T &vl, const T &vr)
	{
		int ret = 0;
		this->_spin.lock();
		ret = this->do_add(vl, vr);
		this->_spin.unlock();
		return ret;
	}

	int Del(const T &v)
	{
		return this->Del(v, v);
	}

	int Del(const T &vl, const T &vr)
	{
		int ret = 0;
		this->_spin.lock();
		ret = this->do_del(vl, vr);
		this->_spin.unlock();
		return ret;
	}

	int PopFirst(T &vout)
	{
		int ret = 0;
		this->_spin.lock();
		ret = this->do_pop_first(vout);
		this->_spin.unlock();
		return ret;
	}

	int PopLast(T &vout)
	{
		int ret = 0;
		this->_spin.lock();
		ret = this->do_pop_last(vout);
		this->_spin.unlock();
		return ret;
	}

	int Test(const T &v, const bool test_add)
	{
		return this->Test(v, v, test_add);
	}

	int Test(const T &vl, const T &vr, const bool test_add)
	{
		int ret = 0;
		this->_spin.lock();
		ret = this->do_test(vl, vr, test_add);
		this->_spin.unlock();
		return ret;
	}

	void Clear()
	{
		this->_spin.lock();
		this->do_clear();
		this->_spin.unlock();
	}

	int SaveToArray(std::vector<T> &usage_arr, const T &vl, const T &vr)
	{
		if (vl > vr)
			return EINVAL;
		usage_arr.clear();
		int ret = 0;
		this->_spin.lock();
		ret = this->do_info(usage_arr, vl, vr);
		this->_spin.unlock();
		return ret;
	}

	int LoadFromArray(const T *arr, const size_t arr_size)
	{
		if ((arr_size % 2) != 0)
			return EINVAL;
		for (size_t i=1; i<arr_size; i++) {
			if ((i % 2) == 0) {
				if (arr[i] <= arr[i-1])
					return EINVAL;
			} else {
				if (arr[i] < arr[i-1])
					return EINVAL;
			}
		}
		int ret = 0;
		this->_spin.lock();
		this->do_clear();
		for (size_t i=0; i<arr_size; i+=2) {
			ret = this->do_add(arr[i], arr[i+1]);
			if (ret)
				break;
		}
		this->_spin.unlock();
		return ret;
	}

private:
	typedef struct _node_st {
		_node_st(const T &_vl)
		{
			this->vl = _vl;
		}

		_node_st(const T &_vl, const T &_vr)
		{
			this->vl = _vl;
			this->vr = _vr;
		}

		RB_ENTRY(_node_st) entry;
		T vl;
		T vr;
	} _node_t;

	int _node_cmp(_node_t *npl, _node_t *npr)
	{
		if (npl->vl < npr->vl)
			return -1;
		else if (npl->vl > npr->vl)
			return 1;
		else
			return 0;
	}

	typedef RB_HEAD(_rb_tree, _node_st) _rb_head_t;

	RB_GENERATE(_rb_tree, _node_st, entry, _node_cmp)

	void do_clear()
	{
		RB_INIT(&(this->_rb_head));
		this->_pool_p->Clear();
	}

	int do_info(std::vector<T> &usage_arr, const T &vl, const T &vr)
	{
		_node_t ntmp(vl);
		_node_t *np = RB_NFIND(_rb_tree, &(this->_rb_head), &ntmp);
		usage_arr.clear();
		while (np && (np->vr <= vr)) {
			usage_arr.push_back(np->vl);
			usage_arr.push_back(np->vr);
			np = RB_NEXT(_rb_tree, &(this->_rb_head), np);
		}
		return 0;
	}

	int do_add(const T &vl, const T &vr)
	{
		if (vl > vr)
			return EINVAL;
		bool cl = false;
		bool cr = false;
		_node_t ntmp(vl);
		_node_t *nlp = NULL;
		_node_t *nrp = RB_NFIND(_rb_tree, &(this->_rb_head), &ntmp);
		if (nrp) {
			if (nrp->vl <= vr)
				return EEXIST;
			nlp = RB_PREV(_rb_tree, &(this->_rb_head), nrp);
			T vtmp = vr;
			vtmp++;
			if (nrp->vl == vtmp)
				cr = true;
		} else
			nlp = RB_MAX(_rb_tree, &(this->_rb_head));
		if (nlp) {
			if (nlp->vr >= vl)
				return EEXIST;
			T vtmp = vl;
			vtmp--;
			if (nlp->vr == vtmp)
				cl = true;
		}
		if (cl) {
			if (cr) {
				nlp->vr = nrp->vr;
				RB_REMOVE(_rb_tree, &(this->_rb_head), nrp);
				this->_pool_p->Put(nrp);
			} else
				nlp->vr = vr;
		} else if (cr) {
			nrp->vl = vl;
		} else {
			_node_t *nnp = this->_pool_p->Get(vl, vr);
			if (!nnp)
				return ENOMEM;
			RB_INSERT(_rb_tree, &(this->_rb_head), nnp);
		}
		return 0;
	}

	int do_del(const T &vl, const T &vr)
	{
		if (vl > vr)
			return EINVAL;
		_node_t ntmp(vl);
		_node_t *np = RB_NFIND(_rb_tree, &(this->_rb_head), &ntmp);
		if (np) {
			if (np->vl > vl)
				np = RB_PREV(_rb_tree, &(this->_rb_head), np);
		} else
			np = RB_MAX(_rb_tree, &(this->_rb_head));
		if (!np)
			return ERANGE;
		if ((np->vl > vl) || (np->vr < vr))
			return ERANGE;
		if (np->vl == vl) {
			RB_REMOVE(_rb_tree, &(this->_rb_head), np);
			if (np->vr == vr) {
				this->_pool_p->Put(np);
			} else {
				np->vl = vr;
				np->vl++;
				RB_INSERT(_rb_tree, &(this->_rb_head), np);
			}
		} else {
			if (np->vr > vr) {
				_node_t *nnp = this->_pool_p->Get(vr, np->vr);
				if (!nnp)
					return ENOMEM;
				nnp->vl++;
				RB_INSERT(_rb_tree, &(this->_rb_head), nnp);
			}
			np->vr = vl;
			np->vr--;
		}
		return 0;
	}

	int do_pop_first(T &vout)
	{
		_node_t *np = RB_MIN(_rb_tree, &(this->_rb_head));
		if (!np)
			return ENOENT;
		T vtmp = np->vl;
		int ret = this->do_del(vtmp, vtmp);
		if (!ret)
			vout = vtmp;
		return ret;
	}

	int do_pop_last(T &vout)
	{
		_node_t *np = RB_MAX(_rb_tree, &(this->_rb_head));
		if (!np)
			return ENOENT;
		T vtmp = np->vr;
		int ret = this->do_del(vtmp, vtmp);
		if (!ret)
			vout = vtmp;
		return ret;
	}

	int do_test(const T &vl, const T &vr, const bool test_add)
	{
		if (vl > vr)
			return EINVAL;
		_node_t ntmp(vl);
		if (test_add) {
			_node_t *nlp = NULL;
			_node_t *nrp = RB_NFIND(_rb_tree, &(this->_rb_head), &ntmp);
			if (nrp) {
				if (nrp->vl <= vr)
					return EEXIST;
				nlp = RB_PREV(_rb_tree, &(this->_rb_head), nrp);
			} else
				nlp = RB_MAX(_rb_tree, &(this->_rb_head));
			if (nlp) {
				if (nlp->vr >= vl)
					return EEXIST;
			}
		} else {
			_node_t *np = RB_NFIND(_rb_tree, &(this->_rb_head), &ntmp);
			if (np) {
				if (np->vl > vl)
					np = RB_PREV(_rb_tree, &(this->_rb_head), np);
			} else
				np = RB_MAX(_rb_tree, &(this->_rb_head));
			if (!np)
				return ERANGE;
			if ((np->vl > vl) || (np->vr < vr))
				return ERANGE;
		}
		return 0;
	}

	CR_DataControl::Spin _spin;
	_rb_head_t _rb_head;
	CR_Pool<_node_t> *_pool_p;
};

#endif /* __H_CR_BIGSET_H__ */
