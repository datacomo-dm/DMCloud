#ifndef __H_CR_AUTOID_H__
#define __H_CR_AUTOID_H__

#include <cr_class/cr_datacontrol.h>

template<typename T>
class CR_AutoID {
public:
	T Get()
	{
		T ret;
		this->_spin.lock();
		ret = this->_v;
		(this->_v)++;
		this->_spin.unlock();
		return ret;
	}

	T Query()
	{
		T ret;
		this->_spin.lock();
		ret = this->_v;
		this->_spin.unlock();
		return ret;
	}

	T Add(const T &v)
	{
		T ret;
		this->_spin.lock();
		this->_v += v;
		ret = this->_v;
		this->_spin.unlock();
		return ret;
	}

	void Reset(const T &new_v)
	{
		this->_spin.lock();
		this->_v = new_v;
		this->_spin.unlock();
	}

private:
	CR_DataControl::Spin _spin;
	T _v;
};

#endif /* __H_CR_AUTOID_H__ */
