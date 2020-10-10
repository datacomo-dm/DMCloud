#ifndef __H_CR_UNLIMITEDFIFO_H__
#define __H_CR_UNLIMITEDFIFO_H__

#include <mutex>
#include <cr_class/cr_class.h>
#include <cr_class/cr_class.pb.h>
#include <cr_class/cr_datacontrol.h>
#include <cr_class/cr_fixedlinearstorage.h>

class CR_UnlimitedFifo {
public:
	CR_UnlimitedFifo(const std::string &file_name_prefix="", size_t rough_file_size=0,
		const bool save_compress=true);
	~CR_UnlimitedFifo();

	void Clear();

	int SetArgs(const std::string &file_name_prefix="", const size_t rough_file_size=0,
		const bool save_compress=true);

	int PushBack(const int64_t obj_id, const void *key_p, const size_t key_size,
		const void *value_p, const size_t value_size);

	int PushBack(const int64_t obj_id, const std::string &key, const std::string *value_p);

	int PushBack(CR_FixedLinearStorage &data_in, uint64_t *err_lineno_p=NULL);

	int PopFront(int64_t &obj_id, std::string &key, std::string &value,
		bool &value_is_null, const double timeout_sec=0.0);

	int PopFront(CR_FixedLinearStorage &data_out, const size_t max_line_count=SIZE_MAX,
		double timeout_sec=0);

private:
	CR_DataControl::CondMutex _cmtx;

	std::vector<CR_FixedLinearStorage*> _fls_arr;

	size_t _fls_head_size;

	size_t _next_fls_id;

	size_t _push_fls_pos;
	size_t _pop_fls_pos;

	size_t _next_pop_line_no;

	size_t _rough_file_size;
	std::string _file_name_prefix;
	bool _save_compress;

	void do_init();

	void do_clear();

	int do_set_args(const std::string &file_name_prefix, size_t rough_file_size, const bool save_compress);

	std::string do_gen_filename();

	int do_pushback(const int64_t obj_id, const void *key_p, const size_t key_size,
		const void *value_p, const size_t value_size);

	int do_popfront(int64_t &obj_id, std::string &key, std::string &value, bool &value_is_null);
};

#endif /* __H_CR_UNLIMITEDFIFO_H__ */
