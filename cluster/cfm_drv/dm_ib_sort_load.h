#ifndef __H_DM_IB_SORT_LOAD_H__
#define __H_DM_IB_SORT_LOAD_H__

#include <cr_class/cr_class.h>
#include <cr_class/cr_fixedlinearstorage.h>
#include <cfm_drv/cfm_cluster.h>
#include <cfm_drv/cfm_splitter.h>

#define DM_IB_MAX_BLOCKLINES			(65536)

#define DMIBSORTLOAD_EN_CLUSTER_STR		"CFM_CLUSTER_STR"
#define DMIBSORTLOAD_EN_LIB_MEMFIFO_SIZE	"CFM_LIB_MEMFIFO_SIZE"

#define DMIBSORTLOAD_PN_SEND_BUF_SIZE		"send_buf_size"
#define DMIBSORTLOAD_PN_TIMEOUT_PROC		"timeout_proc"
#define DMIBSORTLOAD_PN_TIMEOUT_COMM		"timeout_comm"
#define DMIBSORTLOAD_PN_LAST_JOB_ID		"last_job_id"
#define DMIBSORTLOAD_PN_KEEP_SORT_TMP		"keep_sort_tmp"
#define DMIBSORTLOAD_PN_BLOCK_LINES		"block_lines"
#define DMIBSORTLOAD_PN_LINEID_BEGIN		"lineid_begin"
#define DMIBSORTLOAD_PN_QUERY_CMD		"QUERY_CMD"
#define DMIBSORTLOAD_PN_LINE_COUNT_ARRAY	"line_count_array"
#define DMIBSORTLOAD_PN_FIFO_SIZE		"FIFO_SIZE"

#define DMIBSORTLOAD_PN_LIBLOAD_RESULT_ARRAY	"libload_result_array"

#define DMIBSORTLOAD_PV_QUERY_STAT		"query_stat"
#define DMIBSORTLOAD_PV_DATA_END		"data_end"

#define DMIBSORTLOAD_KN_JOB_DONE_PREFIX		"job_done-"

#define DMIBSORTLOAD_KN_CLIENT_REG_PREFIX	"client_reg-"
#define DMIBSORTLOAD_KN_CLIENT_INFO_PREFIX	"client_info-"

#define LIBSORT_PN_TARGET_JOB_ID		"target_job_id"

#define LIBSORT_PV_SET_TARGET			"set_target"
#define LIBSORT_PV_WAIT_SEND_DONE		"wait_send_done"
#define LIBSORT_PV_APPEND_PART_FILES		"append_part_files"
#define LIBSORT_PV_QUERY_KEY_SAMPLE		"query_key_sample"

#define LIBSORT_STAT_LINES_IN			"stat_lines_in"
#define LIBSORT_STAT_LINES_OUT			"stat_lines_out"
#define LIBSORT_STAT_LINES_CRCSUM_IN		"stat_lines_crcsum_in"
#define LIBSORT_STAT_LINES_CRCSUM_OUT		"stat_lines_crcsum_out"

#define LIBSORT_KN_TRIE_DATA			"trie_data"
#define LIBSORT_KN_KEY_SAMPLES_SUFFIX		".keysamples"

#define LIBSORT_KN_MASTER_STARTED		"master_started"

#define LIBIBLOAD_PV_WORK_START			"work_start"

#define LIBIBLOAD_PN_TABLE_INFO			"table_info"
#define LIBIBLOAD_PN_SAVE_PATHNAME		"save_pathname"
#define LIBIBLOAD_PN_PACKINDEX_INFO		"packindex_info"
#define LIBIBLOAD_PN_PACKINDEX_MULTIMODE	"packindex_multimode"
#define LIBIBLOAD_PN_FILETHREAD_COUNT		"filethread_count"

#define DMIBSORTLOAD_PN_RIAK_CONNECT_STR	"riak_connect_str"
#define DMIBSORTLOAD_PN_RIAK_BUCKET		"riak_bucket"
#define DMIBSORTLOAD_PN_KS_BUCKET		"ks_bucket"

namespace DMIBSortLoad_NS {
	int EmergencyStop(const std::string &job_id);
};

class DMIBSortLoad {
public:
#if 0
	/* We need 3 environment variables to start work
	//  CFM_CLUSTER_STR is cluster's define string, use `cluster_fm -h` view it's format
	//  CFM_LIBSORT_NAME is sort lib name, usually "libcfm_sort.so"
	//  CFM_LIBIBLOAD_NAME is ib-load lib name, usually "libcfm_ibload.so"

	// if is_master_client == true && sample_count == 0, then MUST set last_job_id to sort_param:
	//  Data definition of last_job_id's task must be the same for this task
	// Example:
	sort_param = CR_Class_NS::str_setparam(sort_param, DMIBSORTLOAD_PN_LAST_JOB_ID,
	  last_job_id);

	// both master and slver, we can set send_buf_size for cluster connect,
	//  min value -> low speed and low error rate, max value -> high speed and high error rate
	sort_param = CR_Class_NS::str_setparam_u64(sort_param, DMIBSORTLOAD_PN_SEND_BUF_SIZE,
	  send_buf_size); // min 64K, max 16M

	sort_param = CR_Class_NS::str_setparam_bool(sort_param, DMIBSORTLOAD_PN_KEEP_SORT_TMP,
	  (bool)keep_sort_tmp); // if keep_sort_tmp == true, it means sort temp data
	                        //  will KEEP after sort
	...
	DMIBSortLoad *dmibsl_short_p = new DMIBSortLoad(job_id, true, &sort_param);
	DMIBSortLoad *dmibsl_long_p = new DMIBSortLoad(job_id, true, &sort_param);
	...
	*/
#endif
	DMIBSortLoad(const std::string &job_id, const bool is_master_client=false,
		const std::string *sort_param_p=NULL);

	~DMIBSortLoad();

	size_t GetClientID();

	std::string GetJobID();

#if 0
	/* How to restore sort tmp files:
	CR_FixedLinearStorage tmp_fls(65536);
	...
	tmp_fls.SetTag(CR_Class_NS::str_setparam("",
	  DMIBSORTLOAD_PN_QUERY_CMD, LIBSORT_PV_APPEND_PART_FILES));
	for (int i=0; i<need_restore_job_count; i++) {
	    tmp_fls.PushBack(i, need_restore_jobid_array[i], NULL);
	}
	fret = dmibsl_long_p->PushLines(tmp_fls);
	...
	*/
#endif
	int PushLines(CR_FixedLinearStorage &lines, std::string *err_msg=NULL);

#if 0
	/* How to make load_param:
	load_param = CR_Class_NS::str_setparam(load_param, LIBIBLOAD_PN_TABLE_INFO
	  table_info);
	load_param = CR_Class_NS::str_setparam_u64(load_param, DMIBSORTLOAD_PN_BLOCK_LINES,
	  (unsigned)block_lines); // block_lines == 65536 for BRIGHTHOUSE_DATALOAD
	load_param = CR_Class_NS::str_setparam_i64(load_param, DMIBSORTLOAD_PN_LINEID_BEGIN,
	  (int64_t)lineid_begin);
	load_param = CR_Class_NS::str_setparam(load_param, LIBIBLOAD_PN_SAVE_PATHNAME,
	  save_pathname);
	// pack-index define example
	//  col[0], col[2], col[3] need pack index
	std::vector<uint64_t> packindex_arr;
	packindex_arr.push_back(0);
	packindex_arr.push_back(2);
	packindex_arr.push_back(3);
	// save index info to load param
	load_param = CR_Class_NS::str_setparam_u64arr(load_param, LIBIBLOAD_PN_PACKINDEX_INFO,
	  packindex_arr);

	// pki file use multi-mode?
	load_param = CR_Class_NS::str_setparam_bool(load_param, LIBIBLOAD_PN_PACKINDEX_MULTIMODE,
	  (bool)packindex_multimode);

	// Note:
	//  save_pathname can be
	//   "/local/path/to/dir" -> node local path (or mounted network path, all node use same path)
	//   "ssh://<username>@<hostname>:/server/path/to/dir" -> SSH server path
	//   "nfs://<hostname>:/server/path/to/dir" -> NFS server path

	// return value :
	//  >  0 : how many LOAD NODE in current cluster
	//  == 0 : NEVER use
	//  <  0 : check errno
	*/
#endif
	int Finish(const std::string &load_param, std::string *err_msg=NULL, std::string *result_msg_p=NULL,
		const bool turn_master_flag=false, const double clients_wait_timeout=86400.0);

#if 0
	/* Example:

	int fret;
	DMIBSortLoad *sortloader_p = new DMIBSortLoad(...);
	CR_FixedLinearStorage lines(1024 * 1024 * 128); // 128MB block
	int64_t line_no = 0;
	std::string errmsg;
	...
	while (line_no < row_data.size()) {
	  fret = lines.PushBack(line_no, key[line_no], &(row_data[line_no]));
	  if (fret == ENOBUFS) {
            fret = sortloader_p->PushLines(lines, &errmsg);
            if (fret)
              throw fret;
            lines.Clear();
            continue;
          } else (fret)
            throw(fret);
          line_no++;
	}
	...
	fret = sortloader_p->Finish(load_param, &errmsg);
	...
	*/

	/* Example for a column's pack-index:

	#include <cr_class/cr_externalsort.h>

        char str_buf[PATH_MAX];

        for (unsigned i=0; i<packindex_arr.size(); i++) {
            CR_ExternalSort::PopOnly pop_only;
            std::vector<std::string> part_filename_array;
            std::string pki_last_key;
            uint32_t pki_last_packid = UINT32_C(0xFFFFFFFF);
            std::vector<uint32_t> pki_idx_array;

            int64_t pack_id;
            std::string pki_key;
            std::string _value_tmp;
            bool _value_is_null_tmp;

            for (unsigned j=0; j<result_part_count; j++) {
                snprintf(str_buf, sizeof(str_buf), "%s/S%s-N%05u-A%05u.pki",
                  save_pathname.c_str(), job_id.c_str(),
                  j, (unsigned)packindex_arr[i]);
                part_filename_array.push_back(std::string(str_buf));
            }

            pop_only.LoadFiles(part_filename_array);

            while (1) {
                fret = pop_only.PopOne(pack_id, pki_key, _value_tmp, _value_is_null_tmp);
                if (fret == EAGAIN) {
                    DPRINTF("Key[%s]'s pack-id array == [%s]\n",
                      pki_last_key.c_str(), CR_Class_NS::u32arr2str(pki_idx_array).c_str());
                    break;
                } else if (fret) {
                    DPRINTF("pop_only.PopOne failed[%s]\n", CR_Class_NS::strerrno(fret));
                    break;
                }
                if (pki_key == pki_last_key) {
                    if ((uint32_t)pack_id != pki_last_packid) {
                        pki_idx_array.push_back((uint32_t)pack_id);
                    }
                    pki_last_packid = pack_id;
                } else {
                    DPRINTF("Key[%s]'s pack-id array == [%s]\n",
                      pki_last_key.c_str(), CR_Class_NS::u32arr2str(pki_idx_array).c_str());
                    pki_idx_array.clear();
                    pki_idx_array.push_back((uint32_t)pack_id);
                }
                pki_last_key = pki_key;
            }
        }
	*/
#endif

private:
        CR_DataControl::Mutex _lck;

	std::string _job_id;
	std::string _sort_job_id;
	std::string _load_job_id;
	std::string _cluster_str;
	bool _is_master;
	size_t _client_id;
	uint64_t _total_lines;
	uint64_t _crcsum_out;

	uint64_t _wait_timeoutsec;

	CFM_Basic_NS::cluster_info_t _cluster_info;

	CFMCluster *_sort_cluster_p;
	CFMCluster *_load_cluster_p;

	CFMSplitter *_splitter_p;
};

#endif /* __H_DM_IB_SORT_LOAD_H__ */
