#ifndef __H_CR_FIXEDLINEARSTORAGE_H__
#define __H_CR_FIXEDLINEARSTORAGE_H__

#include <string>
#include <tuple>
#include <cr_class/cr_datacontrol.h>
#include <cr_class/cr_class.pb.h>

// Large mode means: file.length, key.length, value.length are all use uint64_t,
//   and with a 64bit obj-id every line, in amd64-gcc, it's use 32 bytes per line
#define CR_FLS_SIZEMODE_LARGE			(32)
// Middle mode means: file.length is uint64_t, but key.length and value.length use uint32_t,
//   and with a 64bit obj-id every line, in amd64-gcc, it's use 24 bytes per line
#define CR_FLS_SIZEMODE_MIDDLE			(24)
// Small mode means: file.length is uint32_t, and key.length and value.length use uint16_t,
//   and with a 64bit obj-id every line, in any type of gcc, it's use 16 bytes per line
#define CR_FLS_SIZEMODE_SMALL			(16)

#define CR_FLS_FLAG_QUERYMODE_IGNORE_OBJID	(0x1)
#define CR_FLS_FLAG_QUERYMODE_IGNORE_KEY	(0x2)
#define CR_FLS_FLAG_QUERYMODE_IGNORE_VALUE	(0x4)

namespace CR_FixedLinearStorage_NS {
	typedef struct {
		uint32_t size_mode;
		uint32_t flags;
		int64_t min_obj_id;
		int64_t max_obj_id;
		uint64_t total_lines;
		uint64_t longest_key_size;
		uint64_t longest_value_size;
		uint64_t longest_line_size;
		uint64_t next_line_data_pos;
		uint64_t free_space_left;
		uint64_t objid_cksum;
		uint64_t key_cksum;
		uint64_t value_cksum;
	} fls_infos_t;

	struct line_info_t {
		int64_t obj_id;
		const char *key_p;
		uint64_t key_size;
		const char *value_p;
		uint64_t value_size;

		line_info_t()
			: obj_id(0), key_p(NULL), key_size(0), value_p(NULL), value_size(0)
		{
		}
	};

	void ClearInfos(fls_infos_t &infos);

	bool VerifyData(const void *buf, const uint64_t buf_size, fls_infos_t *infos_out_p=NULL);

	bool VerifyFile(const char *fname, fls_infos_t *infos_out_p=NULL);

	bool VerifyFileDescriptor(int fd, fls_infos_t *infos_out_p=NULL);

	uint32_t CalcSizeMode(const uint64_t longest_line_size, const uint64_t longest_key_size,
		const uint64_t longest_value_size, const uint64_t max_lines);

	void MergeInfo(fls_infos_t &dst, const fls_infos_t &src);
}

class CR_FixedLinearStorage {
public:
	CR_FixedLinearStorage(const uint64_t max_data_size=0, const uint32_t size_mode=CR_FLS_SIZEMODE_LARGE);
	~CR_FixedLinearStorage();

	int ReSize(uint64_t max_data_size, const uint32_t size_mode=CR_FLS_SIZEMODE_LARGE);

	int Clear(const bool do_bzero=false);

	int Rebuild(uint64_t new_data_size=0, uint32_t size_mode=0);

	// result:
	//  EPERM -> object is read-only (MapFromArray)
	//  EINVAL -> key_p is NULL
	//  ENOBUFS -> not enough space to save
	int Set(const uint64_t line_no, const int64_t obj_id, const void *key_p, const uint64_t key_size,
		const void *value_p, const uint64_t value_size, uint64_t *free_space_left_p=NULL);

	int Set(const uint64_t line_no, const CR_FixedLinearStorage_NS::line_info_t &line_info,
		uint64_t *free_space_left_p=NULL);

	int Set(const uint64_t line_no, const int64_t obj_id, const std::string &key,
		const std::string *value_p, uint64_t *free_space_left_p=NULL);

	int PushBack(const CR_FixedLinearStorage_NS::line_info_t &line_info,
		uint64_t *free_space_left_p=NULL);

	int PushBack(const int64_t obj_id, const void *key_p, const uint64_t key_size,
		const void *value_p, const uint64_t value_size, uint64_t *free_space_left_p=NULL);

	int PushBack(const int64_t obj_id, const std::string &key, const std::string *value_p,
		uint64_t *free_space_left_p=NULL);

	int Query(const uint64_t line_no, int64_t &obj_id,
		const char *&key_p, uint64_t &key_size,
		const char *&value_p, uint64_t &value_size, bool *is_last_line_p=NULL);

	int Query(const uint64_t line_no, CR_FixedLinearStorage_NS::line_info_t &line_info,
		bool *is_last_line_p=NULL);

	int Query(const uint64_t line_no, const char *&key_p, uint64_t &key_size, bool *is_last_line_p=NULL);

	int Query(const uint64_t line_no, std::string &key, bool *is_last_line_p=NULL);

	int Query(const uint64_t line_no, int64_t &obj_id, std::string &key,
		std::string &value, bool &value_is_null, bool *is_last_line_p=NULL);

	void SetLocalTag(const std::string &local_id_str);

	std::string GetLocalTag();

	int SetTag(const std::string &s);

	std::string GetTag();

	int Sort();

	void MergeInfos(CR_FixedLinearStorage_NS::fls_infos_t &fls_infos);

	void GetInfos(CR_FixedLinearStorage_NS::fls_infos_t &fls_infos);

	void SetFlags(const uint64_t flags);

	uint64_t GetFlags();

	uint32_t GetSizeMode();

	uint64_t GetHeadSize();

	uint64_t GetStorageSize();

	uint64_t GetFreeSpace();

	uint64_t GetObjIDCKSUM();

	uint64_t GetKeyCKSUM();

	uint64_t GetValueCKSUM();

	uint64_t GetCKSUM();

	uint64_t GetTotalLines();

	uint64_t GetLongestKeySize();

	uint64_t GetLongestValueSize();

	uint64_t GetLongestLineSize();

	uint64_t GetLongestLineStoreSize();

	int64_t GetMinObjID();

	int64_t GetMaxObjID();

	int Data(const void *&data, uint64_t &size);

	int SaveToFileDescriptor(int fd, uint64_t slice_size=0,
		uint64_t *slice_count_out_p=NULL, const bool do_compress=true,
		const bool is_sock_fd=false, const double timeout_sec=0.0);

	int SaveToFile(const std::string &filename, const uint64_t slice_size=0,
		uint64_t *slice_count_out_p=NULL, const bool do_compress=true);

	int SaveToString(std::string &str_out, const bool do_compat=true);

	int LoadFromFileDescriptor(int fd, const bool is_sock_fd=false, const double timeout_sec=0.0);

	int LoadFromFile(const std::string &filename);

	int LoadFromArray(const void * buf, const uint64_t buf_size);

	int LoadFromString(const std::string &str_in);

	int MapFromArray(const void * buf, const uint64_t buf_size);

private:
	CR_DataControl::Spin _spin;

	std::string _local_id;

	void *_private_data;
	uint64_t _private_size;
	bool _private_data_is_mapped;

	int (*do_set)(void *data_p, const uint64_t line_no, const int64_t obj_id,
		const void *key_p, const uint64_t key_size,
		const void *value_p, const uint64_t value_size);

	int (*do_query)(void *data_p, const uint64_t line_no, int64_t &obj_id,
		const char *&key_p, uint64_t &key_size,
		const char *&value_p, uint64_t &value_size, bool *is_last_line_p);

	int (*do_sort)(void *data_p);

	uint32_t do_set_sizemode(const uint32_t size_mode);
	uint32_t do_get_sizemode();

	int do_clear(const bool do_bzero);

	inline int64_t do_get_min_obj_id();
	inline int64_t do_get_max_obj_id();
	inline uint64_t do_get_total_lines();
	inline uint64_t do_get_free_space();
	inline uint64_t do_get_objid_cksum();
	inline uint64_t do_get_key_cksum();
	inline uint64_t do_get_value_cksum();
	inline uint64_t do_get_longest_key_size();
	inline uint64_t do_get_longest_value_size();
	inline uint64_t do_get_longest_line_size();
	inline uint64_t do_get_longest_line_store_size();

	int do_save_to_string(std::string &str_out, const bool do_compat);
};

#endif /* __H_CR_FIXEDLINEARSTORAGE_H__ */
