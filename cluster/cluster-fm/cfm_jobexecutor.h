#ifndef __H_CFM_JOBEXECUROT_H__
#define __H_CFM_JOBEXECUROT_H__

#include <vector>
#include <map>
#include <string>
#include <cr_class/cr_datacontrol.h>
#include <cr_class/cr_simplecomm.h>
#include <cfm_drv/cfm_basic.h>

class CFMJobExecutorMap {
public:
	class JobExecutorInfo {
	public:
		JobExecutorInfo(const CFM_Basic_NS::cluster_info_t &cluster_info,
			const std::pair<int,int> &current_node);
		~JobExecutorInfo();

		CR_DataControl::Descriptor desc;
		pid_t job_pid;
		std::string job_id;
		std::string lib_name;
		bool lib_is_tmp;
		std::string job_sock_path;
		std::vector<std::string> job_work_path_arr;
		bool static_work_path;
		double del_time;
		size_t kill_count;
		CR_SimpleComm job_conn;
	};

        CFMJobExecutorMap();
	~CFMJobExecutorMap();

	int add_job(JobExecutorInfo *je_p);
	JobExecutorInfo * del_job(const std::string &job_id);

	JobExecutorInfo * get_job(const std::string &job_id);
	int put_job(JobExecutorInfo *je_p);

	void del_je_info(JobExecutorInfo *p);

private:
	CR_DataControl::CondMutex _cmtx;
	std::map<std::string,JobExecutorInfo*> je_info_map;
};

#endif /* __H_CFM_JOBEXECUROT_H__ */
