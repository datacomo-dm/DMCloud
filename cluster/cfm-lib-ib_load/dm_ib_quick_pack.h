#ifndef __H_DM_IB_QUICK_PACK_H__
#define __H_DM_IB_QUICK_PACK_H__

#include <cr_class/cr_addon.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_automsg.h>
#include <cr_class/cr_datacontrol.h>
#include <cfm_drv/dm_ib_sort_load.h>
#include <cfm_drv/dm_ib_tableinfo.h>
#include <riakdrv/riakcluster.h>
#include "dm_ib_quick_dpn.h"
#include "dm_ib_quick_rsi.h"

class DM_IB_QuickPack {
public:
	DM_IB_QuickPack();

	~DM_IB_QuickPack();

	int SetArgs(const std::string &job_id, const size_t column_id,
		const uint8_t column_type, const int compress_level,
		const int64_t lineid_begin, const int64_t lineid_end,
		DM_IB_QuickDPN::QDPN *dpn_p, DM_IB_QuickRSI &rsis,
		RiakCluster *riakp, const std::string &riak_bucket,
		void *_riak_handle, CR_BlockQueue<int64_t> *ks_pool_p);

	int PutNull(const int64_t lineid);
	int PutValueI(const int64_t lineid, const int64_t v);
	int PutValueD(const int64_t lineid, const double v);
	int PutValueS(const int64_t lineid, const char *v, const size_t vlen);

        int SavePack(std::list<int64_t> *pack_id_list_p, int fd, int file_id,
		CR_DataControl::Sem *file_sem_p=NULL);

private:
	uint32_t nulls[DM_IB_MAX_BLOCKLINES / 8 / sizeof(uint32_t)];

	uint8_t _column_type;
	size_t _column_id;
	int64_t _lineid_begin;
	int64_t _lineid_end;

	std::string _job_id;

	DM_IB_QuickDPN::QDPN *_dpn_p;
	DM_IB_QuickRSI *_rsis;

	std::vector<CR_Class_NS::union64_t> _packn_data_array;
	std::vector<const char*> _packs_index_array;
	std::vector<uint16_t> _packs_lens_array;

	uint32_t _packs_data_full_byte_size;

	int _compress_level;

	RiakCluster *_riakp;
	void *_riak_handle;
	std::string _riak_bucket;
	CR_BlockQueue<int64_t> *_ks_pool_p;

	void clear();
};

#endif /* __H_DM_IB_QUICK_PACK_H__ */
