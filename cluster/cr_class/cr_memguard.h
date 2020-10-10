#ifndef __H_CR_MEMGUARD_H__
#define __H_CR_MEMGUARD_H__

#include <cr_class/cr_class.h>

template <typename T>
class CR_MemGuard;

namespace CR_MemGuard_NS
{
	template<typename T>
	struct debunk_void
	{
		typedef T& type;
	};

	template<>
	struct debunk_void<void>
	{
		typedef void* type;
	};

	template <typename T>
	class Ref
	{
	private:
		explicit Ref(T *data, bool call_delete=true)
			: _data(data), _call_delete(call_delete)
		{
		}

		T *_data;
		bool _call_delete;
		friend class CR_MemGuard<T>;
	};
}

template <typename T>
class CR_MemGuard
{
public:
	CR_MemGuard()
		: _data(NULL), _count(0), _call_delete(false)
	{
	}

	explicit CR_MemGuard(T *data, bool call_delete=true, size_t count=SIZE_MAX)
		: _data(data), _count(count), _call_delete(call_delete)
	{
	}

	CR_MemGuard(const size_t count)
		: _call_delete(true)
	{
		this->_count = count;
		this->_data = new T[count];
	}

	CR_MemGuard(const size_t count, const T &def_v)
		: _call_delete(true)
	{
		this->_count = count;
		this->_data = new T[count];
		for (size_t i=0; i<count; i++)
			this->_data[i] = def_v;
	}

	CR_MemGuard(CR_MemGuard &mg)
		: _data(mg._data), _count(mg._count), _call_delete(mg._call_delete)
	{
		mg._call_delete = false;
	}

	virtual ~CR_MemGuard()
	{
		this->reset();
	}

	CR_MemGuard& operator=(CR_MemGuard_NS::Ref<T> const &mmr)
	{
		this->reset();
		this->_data = mmr._data;
		this->_count = SIZE_MAX;
		this->_call_delete = mmr._call_delete;
		return *this;
        }

	CR_MemGuard& operator=(CR_MemGuard<T> &mg)
	{
		if(&mg != this) {
			this->reset();
			this->_data = mg._data;
			this->_count = mg._count;
			this->_call_delete = mg._call_delete;
			mg._call_delete = false;
		}
		return *this;
	}

	operator CR_MemGuard_NS::Ref<T>()
	{
		CR_MemGuard_NS::Ref<T> mmr(this->_data, this->_call_delete);
		this->_call_delete = false;
		return (mmr);
	}

	typename CR_MemGuard_NS::debunk_void<T>::type operator*() const
	{
		return *this->_data;
	}

	T * get() const
	{
		return this->_data;
	}

	T * operator->() const
	{
		return this->_data;
	}

	size_t count() const
	{
		return this->_count;
	}

	typename CR_MemGuard_NS::debunk_void<T>::type operator[] (const size_t i) const
	{
		if (i >= this->_count)
			THROWF("out of range(%llu >= %llu)\n",
				(long long unsigned)i, (long long unsigned)this->_count);
		return this->_data[i];
	}

	T * release()
	{
		T *tmp = this->_data;
		this->_data = NULL;
		this->_count = 0;
		this->_call_delete = false;
		return tmp;
	}

	void reset()
	{
		if(this->_call_delete && this->_data)
			delete [](this->_data);
		this->_data = NULL;
		this->_count = 0;
		this->_call_delete = false;
	}

	void reset(const size_t count)
	{
		if(this->_call_delete && this->_data)
			delete [](this->_data);
		this->_call_delete = true;
		this->_count = count;
		this->_data = new T[count];
	}

	void reset(const size_t count, const T &def_v)
	{
		if(this->_call_delete && this->_data)
			delete [](this->_data);
		this->_call_delete = true;
		this->_count = count;
		this->_data = new T[count];
		for (size_t i=0; i<count; i++)
			this->_data[i] = def_v;
	}

	void resize(const size_t count)
	{
		T *old_data = this->_data;
		size_t old_count = this->_count;
		bool old_call_delete = this->_call_delete;
		this->_call_delete = true;
		this->_count = count;
		this->_data = new T[count];
		for (size_t i=0; i<old_count; i++)
			this->_data[i] = old_data[i];
		if(old_call_delete && old_data)
			delete []old_data;
	}

	void resize(const size_t count, const T &def_v)
	{
		T *old_data = this->_data;
		size_t old_count = this->_count;
		bool old_call_delete = this->_call_delete;
		this->_call_delete = true;
		this->_count = count;
		this->_data = new T[count];
		for (size_t i=0; i<old_count; i++)
			this->_data[i] = old_data[i];
		if(old_call_delete && old_data)
			delete []old_data;
		for (size_t i=old_count; i<count; i++)
			this->_data[i] = def_v;
	}

private:
	T *_data;
	size_t _count;
	bool _call_delete;
};

#endif /* __H_CR_MEMGUARD_H__ */
