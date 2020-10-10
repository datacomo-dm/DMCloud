#ifndef __H_CR_PERSISTENCEID_H__
#define __H_CR_PERSISTENCEID_H__

#include <cr_class/cr_addon.h>
#include <mutex>

#define CR_PERSISTENCEID_DATAFILE_SUFFIX	".crpiddata"
#define CR_PERSISTENCEID_LOCKFILE_SUFFIX	".crpidlock"

class CR_PersistenceID {
public:
	CR_PersistenceID();
	~CR_PersistenceID();

	int SetArgs(const std::string &filename, int64_t id_range_min, int64_t id_range_max,
		double autofree_timeout_sec=(60 * 60 * 24 * 30));

	int Alloc(size_t alloc_count, std::vector<int64_t> &id_arr);

	int Free(const std::vector<int64_t> &id_arr);

	int Solidify(const std::vector<int64_t> &id_arr);

private:
	std::mutex _mtx;

	std::string _filename;
	int64_t _id_range_min;
	int64_t _id_range_max;
	double _autofree_timeout_sec;
};

#endif /* __H_CR_PERSISTENCEID_H__ */
