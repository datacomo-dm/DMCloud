#ifndef __H_CR_AUTOMSG_H__
#define __H_CR_AUTOMSG_H__

#include <cr_class/cr_addon.h>
#include <unistd.h>
#include <functional>
#include <utility>

class CR_AutoMsg {
public:
	typedef enum {
		DT_INT			= 10,
		DT_DOUBLE		= 20,
		DT_LOCAL_CACHED		= 30,
		DT_STD_STRING		= 40,
		DT_STD_FUNCTION		= 50,
		DT_MEMBLOCK_INFO	= 60,
		DT_UNKNOWN		= 255
	} data_t;

	typedef std::pair<void*,size_t> mb_desc_t;

	typedef std::function<intmax_t(void*)> function_t;

	CR_AutoMsg();
	CR_AutoMsg(const CR_AutoMsg& msg);
	CR_AutoMsg(const intmax_t intmax_v);
	CR_AutoMsg(const int int_v);
	CR_AutoMsg(const double double_v);
	CR_AutoMsg(const char * char_p);
	CR_AutoMsg(const std::string& string_v);
	CR_AutoMsg(const mb_desc_t& memblock_info);
	CR_AutoMsg(function_t function_v);

	~CR_AutoMsg();

	CR_AutoMsg &operator = (const CR_AutoMsg& r_msg);
	CR_AutoMsg &operator = (const intmax_t r_intmax_t);
	CR_AutoMsg &operator = (const int r_int);
	CR_AutoMsg &operator = (const double r_double);
	CR_AutoMsg &operator = (const char * r_char_p);
	CR_AutoMsg &operator = (const std::string& r_string);
	CR_AutoMsg &operator = (const mb_desc_t& memblock_info);
	CR_AutoMsg &operator = (function_t r_function);

	bool operator == (const CR_AutoMsg& r_msg) const;
	inline bool operator != (const CR_AutoMsg& r_msg) const { return !(*this == r_msg); };

	data_t type() const;

	intmax_t as_intmax_t(const intmax_t default_val=0) const;
	int as_int(const int default_val=0) const { return this->as_intmax_t(default_val); };
	double as_double(const double default_val=0.0) const;
	const char* as_c_str(size_t *len_p=NULL) const;
	std::string as_string() const;
	void* as_memblock(size_t &size) const;
	function_t as_function() const;

	size_t size() const;

	void clear(bool clear_cache=true);

	bool is_null() const;

protected:
	union {
		uintptr_t cache_len;
		void* mb_p;
		std::string* str_p;
		function_t* func_p;
	} _data_info;

	data_t _type;

	union {
		intmax_t intmax_v;
		double double_v;
		size_t mb_size;
		char cache_data[sizeof(function_t)];
	} _data_store;

private:
};

#endif /* __H_CR_AUTOMSG_H__ */
