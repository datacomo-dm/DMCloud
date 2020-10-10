#ifndef __H_CR_EXTERNALSORT_H__
#define __H_CR_EXTERNALSORT_H__

#include <string>
#include <mutex>
#include <vector>
#include <list>
#include <map>
#include <tuple>
#include <thread>

#include <cr_class/cr_class.pb.h>
#include <cr_class/cr_datacontrol.h>
#include <cr_class/cr_blockqueue.h>
#include <cr_class/cr_fixedlinearstorage.h>

#define EXTERNALSORT_ARGNAME_ROUGHFILESIZE	"CRES_AN_ROUGHFILESIZE"
#define EXTERNALSORT_ARGNAME_ROUGHSLICESIZE	"CRES_AN_ROUGHSLICESIZE"
#define EXTERNALSORT_ARGNAME_MAXSLICESIZE	"CRES_AN_MAXSLICESIZE"
#define EXTERNALSORT_ARGNAME_PRELOADCACHESIZE	"CRES_AN_PRELOADCACHESIZE"
#define EXTERNALSORT_ARGNAME_ENABLECOMPRESS	"CRES_AN_ENABLECOMPRESS"
#define EXTERNALSORT_ARGNAME_MAXSORTTHREADS	"CRES_AN_MAXSORTTHREADS"
#define EXTERNALSORT_ARGNAME_MAXDISKTHREADS	"CRES_AN_MAXDISKTHREADS"
#define EXTERNALSORT_ARGNAME_MAXMERGETHREADS	"CRES_AN_MAXMERGETHREADS"
#define EXTERNALSORT_ARGNAME_DEFAULTSIZEMODE	"CRES_AN_DEFAULTSIZEMODE"
#define EXTERNALSORT_ARGNAME_MERGECOUNTMIN	"CRES_AN_MERGECOUNTMIN"
#define EXTERNALSORT_ARGNAME_MERGECOUNTMAX	"CRES_AN_MERGECOUNTMAX"
#define EXTERNALSORT_ARGNAME_ENABLEPRELOAD	"CRES_AN_ENABLEPRELOAD"

class CR_ExternalSort {
public:
	class PartInfo {
	public:
		PartInfo(const std::string &file_name);

		PartInfo(CR_BlockQueue<std::string> &fifo_in, const double pop_timeout=86400.0);

		PartInfo(const int64_t part_id_in,
			const std::string &pathname,
			const size_t rough_file_size,
			const size_t rough_slice_size,
			const bool save_compress,
			const int size_mode,
			CR_DataControl::Sem *_disk_sem_p
		);

		~PartInfo();

		int GetDataInfos(CR_FixedLinearStorage_NS::fls_infos_t &fls_info);

		int SortAndSave();
		int StartBGSortAndSave(CR_DataControl::Sem *_sort_sem_p);
		void WaitBGSort();

		int Front(int64_t &obj_id, std::string *key_str_p=NULL,
			std::string *value_str_p=NULL, bool *value_is_null_p=NULL);

		int PopFront();

		CR_DataControl::Sem *disk_sem_p;
		CR_FixedLinearStorage fls;
		std::string file_name;
		int fd;
		size_t slice_size;
		volatile bool is_sorted;
		int sort_errno;
		size_t next_pop_line_no;
		bool compress;
		int fls_size_mode;
		int merge_level;
		bool keep_file;

	private:
		std::mutex _mtx;
		std::thread *_sort_thread_p;

		CR_BlockQueue<std::string> *_fifo_in_p;

		double _pop_timeout;

		bool _read_only;
	};

	class PopMap {
	public:
		PopMap();
		~PopMap();
		int Init(std::list<CR_ExternalSort::PartInfo*> &partinfo_list);
		void Clear();
		int PopOne(int64_t &obj_id, std::string &key, std::string &value, bool &value_is_null);
	private:
		std::multimap<std::tuple<std::string,int64_t>,CR_ExternalSort::PartInfo*> _order_map;
	};

	class PopOnly {
	public:
		PopOnly();
		~PopOnly();
		void Clear();
		int LoadFiles(const std::vector<std::string> &part_filename_array);
		int PopOne(int64_t &obj_id, std::string &key, std::string &value, bool &value_is_null);
	private:
		CR_DataControl::Spin _lck;
		std::list<PartInfo*> _part_info_list;
		PopMap _pop_map;
	private:
		void do_clear();
	};

	CR_ExternalSort();

	~CR_ExternalSort();

	int SetArgs(const std::string &arg_param_str, const std::vector<std::string> &path_names,
		const bool keep_part_file=false);

	void Clear();

	int MergePartFiles(const std::vector<std::string> &file_names,
		CR_FixedLinearStorage_NS::fls_infos_t *part_info_p=NULL, bool use_extrnal_info=false);

	int MergePartPaths(const std::vector<std::string> &path_names,
		CR_FixedLinearStorage_NS::fls_infos_t *part_info_p=NULL, bool use_extrnal_info=false);

	// if (key_p == NULL), it means data transfer finished
	int PushOne(const int64_t obj_id, const void *key_p, const size_t key_size,
		const void *value_p, const size_t value_size);

	int Push(const msgCRPairList &data_in);

	int Push(CR_FixedLinearStorage &data_in, const bool is_done=false);

	// if result == EAGAIN, it means all data poped
	int PopOne(int64_t &obj_id, std::string &key, std::string &value, bool &value_is_null);

	int Pop(msgCRPairList &data_out, const size_t max_line_count=SIZE_MAX);

	// if result == EAGAIN, it means all data poped
	int Pop(CR_FixedLinearStorage &data_out, const size_t max_line_count=SIZE_MAX);

	void GetInfos(CR_FixedLinearStorage_NS::fls_infos_t &fls_infos);

	size_t GetLongestKeySize();

	size_t GetLongestValueSize();

	size_t GetLongestLineSize();

	uint64_t GetTotalLinesPoped();

private:
	std::mutex _mtx;

	CR_FixedLinearStorage_NS::fls_infos_t _infos;

	bool _is_setarg_ok;
	std::string _last_arg_param_str;

	CR_DataControl::Sem *_sort_sem_p;
	CR_DataControl::Sem *_merge_sem_p;

	size_t _rough_file_size;
	size_t _rough_slice_size;
	size_t _max_slice_size;
	size_t _preload_cache_size;
	std::vector<std::pair<std::string,CR_DataControl::Sem*> > _disk_info_arr;
	bool _save_compress;
	int _size_mode;
	unsigned int _max_sort_threads;
	unsigned int _max_disk_threads;
	unsigned int _max_merge_threads;

	unsigned int _merge_count_min;
	unsigned int _merge_count_max;

	bool _merge_thread_stop;

	uint64_t _lines_poped;

	bool _keep_part_file;
	bool _sorted;

	int64_t _next_part_id;
	std::list<PartInfo*> _part_info_list;
	PartInfo *_cur_part_info;

	PopMap _pop_map;

	std::list<std::thread*> _merge_threads;

	bool _enable_preload;
	CR_BlockQueue<CR_FixedLinearStorage*> _preload_fifo;
	std::thread *_preload_thread_p;
	bool _preload_thread_stop;
	CR_FixedLinearStorage *_preload_slice_fls_p;
	size_t _preload_slice_fls_next_line_no;

	void do_clear();

	void del_disk_sem();

	int64_t alloc_partid();

	PartInfo *add_part(const bool set_current=true);

	void wait_all_thread();

	int push_one(const int64_t obj_id, const void *key_p, const size_t key_size,
		const void *value_p, const size_t value_size);

	int pop_one(int64_t &obj_id, std::string &key, std::string &value, bool &value_is_null);

	int pop_low_level_parts(std::list<CR_ExternalSort::PartInfo*> &partinfo_list);

	void try_bg_merge();
};

#endif /* __H_CR_EXTERNALSORT_H__ */
