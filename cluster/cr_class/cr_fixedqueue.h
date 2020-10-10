#ifndef __H_CR_FIXEDQUEUE_H__
#define __H_CR_FIXEDQUEUE_H__

#include <pthread.h>
#include <string>

#include <cr_class/cr_datacontrol.h>
#include <cr_class/cr_class.pb.h>

// Large mode means: int64_t obj_id; size_t key_length, value_length;
#define CR_FQ_SIZEMODE_LARGE		(40)
// Middle mode means: int64_t obj_id; uint32_t key_length, value_length;
#define CR_FQ_SIZEMODE_MIDDLE		(30)
// Small mode means: int64_t obj_id; uint16_t key_length, value_length;
#define CR_FQ_SIZEMODE_SMALL		(20)
// Tiny mode means: uint16_t key_length, value_length; // and no obj_id
#define CR_FQ_SIZEMODE_TINY		(10)

class CR_FixedQueue {
public:
	CR_FixedQueue(const size_t max_data_size=0, const int size_mode=CR_FQ_SIZEMODE_LARGE);
	~CR_FixedQueue();

	void ReSize(size_t max_data_size, const int size_mode=CR_FQ_SIZEMODE_LARGE);

	void Clear();

	int PushBack(const int64_t obj_id, const void *key_p, const size_t key_size,
		const void *value_p, const size_t value_size, const double timeout_sec=0.0);

	int PushBack(const int64_t obj_id, const std::string &key, const std::string *value_p,
		const double timeout_sec=0.0);

	int PopFront(int64_t &obj_id, std::string &key, std::string &value,
		bool &value_is_null, const double timeout_sec=0.0);

	size_t GetHeadSize();

	int GetSizeMode();

	int SetTag(const std::string &s);

	std::string GetTag();

	int SetLocalTag(const std::string &s);

	std::string GetLocalTag();

	int SaveToFileDescriptor(int fd, bool do_compress=true);

	int SaveToFile(const std::string &filename, bool do_compress=true);

	int LoadFromFileDescriptor(int fd);

	int LoadFromFile(const std::string &filename);

private:
	CR_DataControl::CondMutex _cmtx;

	std::string _local_tag;

	void *_private_data;
	size_t _private_size;

	int (*_do_pushback)(void *data_p, const int64_t obj_id, const void *key_p, const size_t key_size,
		const void *value_p, const size_t value_size);

	int (*_do_popfront)(void *data_p, int64_t &obj_id, std::string &key, std::string &value,
		bool &value_is_null);

	int timed_pushback(const int64_t obj_id, const void *key_p, const size_t key_size,
		const void *value_p, const size_t value_size, double timeout_sec);

	int timed_popfront(int64_t &obj_id, std::string &key, std::string &value,
		bool &value_is_null, double timeout_sec);

	int do_set_sizemode(const int size_mode);
	int do_get_sizemode();

	void do_clear();
};

#endif /* __H_CR_FIXEDQUEUE_H__ */
