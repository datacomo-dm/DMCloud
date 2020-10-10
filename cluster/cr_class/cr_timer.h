#ifndef __H_CR_TIMER_H__
#define __H_CR_TIMER_H__

#include <unistd.h>
#include <string>
#include <cr_class/cr_datacontrol.h>

class CR_Timer {
public:
	CR_Timer();
	CR_Timer(const std::string &name, const void *p=NULL);
	~CR_Timer();

	void Init(const std::string &name, const void *p=NULL);

	void Start(double &time_tmp) const;
	double Stop(const double &time_tmp);

	void Start();
	double Stop();

	const std::string &GetName() const;
	const void *GetP() const;
	double GetCreateTime() const;

	void GetUsage(double &usage_time, size_t &stop_count);

	std::string DebugString();

private:
	CR_DataControl::Spin _spin;
	pthread_key_t _time_tmp_store;

	std::string _name;
	const void *_p;

	double _create_time;
	double _usage_time;
	size_t _stop_count;
};

#endif /* __H_CR_TIMER_H__ */
