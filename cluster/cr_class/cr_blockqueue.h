#ifndef __H_CR_BLOCKQUEUE_H__
#define __H_CR_BLOCKQUEUE_H__

#include <list>
#include <cr_class/cr_datacontrol.h>
#include <cr_class/cr_class.h>

template <typename T>
class CR_BlockQueue {
public:
	typedef void (*on_element_clear_t)(T &v, void *arg_p);

	CR_BlockQueue()
	{
		this->_max_size = 1;
	}

	~CR_BlockQueue()
	{
		this->clear();
	}

	void set_args(size_t max_size, on_element_clear_t on_element_clear=NULL,
		void *on_element_clear_arg_p=NULL)
	{
		this->_cmtx.lock();
		this->_max_size = max_size;
		this->_on_element_clear = on_element_clear;
		this->_on_element_clear_arg_p = on_element_clear_arg_p;
		this->_cmtx.unlock();
	}

	void clear()
	{
		this->_cmtx.lock();
		while (!this->_list.empty()) {
			if (this->_on_element_clear) {
				this->_on_element_clear(this->_list.front(),
				  this->_on_element_clear_arg_p);
			}
			this->_list.pop_front();
		}
		this->_cmtx.unlock();
	}

	void set_max_size(const size_t max_size)
	{
		this->set_args(max_size);
	}

	int push_back(const T &v, const double timeout_sec=0.0)
	{
		int ret = 0;
		double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;
		double wait_gap = (timeout_sec <= 0.1) ? timeout_sec : 0.1;

		this->_cmtx.lock();
		if (this->_max_size > 0) {
			if (timeout_sec > 0.0) {
				while (this->_list.size() >= this->_max_size) {
					this->_cmtx.wait(wait_gap);
					if (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) >= exp_time) {
						ret = ETIMEDOUT;
						break;
					}
				}
			} else {
				if (this->_list.size() >= this->_max_size)
					ret = EAGAIN;
			}
			if (!ret) {
				this->_list.push_back(v);
				this->_cmtx.broadcast();
			}
		} else
			ret = ENOSPC;
		this->_cmtx.unlock();

		return ret;
	}

	void push_front(const T &v)
	{
		this->_cmtx.lock();
		this->_list.push_front(v);
		this->_cmtx.broadcast();
		this->_cmtx.unlock();
	}

	int pop_front(T &v, const double timeout_sec=0.0)
	{
		int ret = 0;
		double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;
		double wait_gap = (timeout_sec <= 0.1) ? timeout_sec : 0.1;

		this->_cmtx.lock();
		if (timeout_sec > 0.0) {
			while (this->_list.size() == 0) {
				this->_cmtx.wait(wait_gap);
				if (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) > exp_time) {
					ret = ETIMEDOUT;
					break;
				}
			}
		} else {
			if (this->_list.size() == 0)
				ret = EAGAIN;
		}
		if (!ret) {
			v = this->_list.front();
			this->_list.pop_front();
			this->_cmtx.broadcast();
		}
		this->_cmtx.unlock();

		return ret;
	}

	void pop_front_multi(std::list<T> &v_list, const size_t count=SIZE_MAX)
	{
		this->_cmtx.lock();
		for (size_t i=0; i<count; i++) {
			if (this->_list.empty())
				break;
			v_list.push_back(this->_list.front());
			this->_list.pop_front();
		}
		if (count > 0)
			this->_cmtx.broadcast();
		this->_cmtx.unlock();
	}

	size_t size()
	{
		size_t ret;

		this->_cmtx.lock();
		ret = this->_list.size();
		this->_cmtx.unlock();

		return ret;
	}

	size_t max_size()
	{
		size_t ret;

		this->_cmtx.lock();
		ret = this->_max_size;
		this->_cmtx.unlock();

		return ret;
	}

	int wait_empty(const double timeout_sec=0.0)
	{
		int ret = 0;
		double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;
		double wait_gap = (timeout_sec <= 0.1) ? timeout_sec : 0.1;

		this->_cmtx.lock();
		while (this->_list.size() > 0) {
			this->_cmtx.wait(wait_gap);
			if (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) > exp_time) {
				ret = ETIMEDOUT;
				break;
			}
		}
		this->_cmtx.unlock();

		return ret;
	}

protected:
	size_t _max_size;
	std::list<T> _list;
	on_element_clear_t _on_element_clear;
	void * _on_element_clear_arg_p;

private:
	CR_DataControl::CondMutex _cmtx;
};

#endif /* __H_CR_BLOCKQUEUE_H__ */
