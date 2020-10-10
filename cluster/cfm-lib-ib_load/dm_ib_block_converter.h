#ifndef __H_DM_IB_BLOCK_CONVERTER_H__
#define __H_DM_IB_BLOCK_CONVERTER_H__

#include <vector>
#include <cr_class/cr_datacontrol.h>
#include <cr_class/cr_blockqueue.h>
#include <cr_class/cr_externalsort.h>
#include <cfm_drv/dm_ib_tableinfo.h>
#include <riakdrv/riakcluster.h>
#include "dm_ib_quick_dpn.h"
#include "dm_ib_quick_rsi.h"

class DM_IB_BlockConverter {
public:
	DM_IB_BlockConverter();
	~DM_IB_BlockConverter();

	int SetArgs(const std::string &save_pathname, const std::string &work_pathname,
		const std::string &job_id, const size_t striping_id, const bool pki_use_multimode,
		const std::vector<uint64_t> &packindex_req, const std::string &tbl_info_str,
		const int64_t lineid_begin, const int64_t lineid_end, const size_t block_lines,
		RiakCluster *riakp, const std::string &riak_bucket,
		void *riak_handle, CR_BlockQueue<int64_t> *key_pool_p);

	int BlockConvert(const int64_t lineid_left, const int64_t lineid_right,
		CR_FixedLinearStorage &block_fls, const size_t max_work_threads=2,
		const size_t max_disk_threads=1);

	int Finish(std::vector<uint64_t> &result_arr, const std::vector<int64_t> *ksi_data_p);

	int SetFileThreadCount(size_t tc);

private:
	DM_IB_TableInfo _table_info;
        std::string _dst_pathname;
        std::string _tmp_pathname;
	std::string _job_id;
	size_t _striping_id;
	std::vector<uint64_t> _packindex_req;
	int64_t _lineid_begin;
	int64_t _lineid_end;
	size_t _block_lines;
	std::vector<DM_IB_QuickDPN> _dpn_array;
	std::vector<DM_IB_QuickRSI> _rsi_array;
	std::vector<int> _compress_level_array;
	bool _pki_use_multimode;
	CR_ExternalSort *_pki_array;
	std::vector<int> _dpn_fd_array;
	std::vector<int> _rsi_fd_array;
	std::vector<int> _pak_fd_array;
	std::vector<int> _pki_fd_array;
	std::vector<size_t> _pack_file_id_array;
	int _ksi_fd;
	int _mount_type;
	CR_DataControl::Sem _file_sem;
	CR_DataControl::Sem _compress_sem;

	RiakCluster *_riakp;
	void *_riak_handle;
	std::string _riak_bucket;
	CR_BlockQueue<int64_t> *_ks_pool_p;
	std::vector<std::list<int64_t> > _pack_id_list_arr;

	void close_all_fd();
};

#endif /* __H_DM_IB_BLOCK_CONVERTER_H__ */
