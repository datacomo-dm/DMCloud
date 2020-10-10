#ifndef __H_CR_LOCKED_H__
#define __H_CR_LOCKED_H__

#include <cr_class/cr_datacontrol.h>

template<typename T>
class CR_Locked {
public:
	CR_Locked() { };
	CR_Locked(const T &v) { this->_v = v; };

	inline T value()
	{
		T ret;
		this->_lck.lock();
		ret = this->_v;
		this->_lck.unlock();
		return ret;
	};

	inline CR_Locked<T> & operator = (const T &r_v)
	{
		this->_lck.lock();
		this->_v = r_v;
		this->_lck.unlock();
		return *this;
	};

	inline CR_Locked<T> & operator = (CR_Locked<T> &r_v)
	{
		this->_lck.lock();
		this->_v = r_v.value();
		this->_lck.unlock();
		return *this;
	};

	inline bool operator == (const T &r_v) { return (this->value() == r_v); };
	inline bool operator != (const T &r_v) { return (this->value() != r_v); };
	inline bool operator > (const T &r_v) { return (this->value() > r_v); };
	inline bool operator >= (const T &r_v) { return (this->value() >= r_v); };
	inline bool operator < (const T &r_v) { return (this->value() < r_v); };
	inline bool operator <= (const T &r_v) { return (this->value() <= r_v); };

	inline friend bool operator == (const T &l_v, CR_Locked<T> &r_v) { return (l_v == r_v.value()); };
	inline friend bool operator != (const T &l_v, CR_Locked<T> &r_v) { return (l_v != r_v.value()); };
	inline friend bool operator > (const T &l_v, CR_Locked<T> &r_v) { return (l_v > r_v.value()); };
	inline friend bool operator >= (const T &l_v, CR_Locked<T> &r_v) { return (l_v >= r_v.value()); };
	inline friend bool operator < (const T &l_v, CR_Locked<T> &r_v) { return (l_v < r_v.value()); };
	inline friend bool operator <= (const T &l_v, CR_Locked<T> &r_v) { return (l_v <= r_v.value()); };

private:
	CR_DataControl::Spin _lck;
	T _v;
};

#endif /* __H_CR_LOCKED_H__ */
