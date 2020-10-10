#ifndef __H_CR_TREESTORAGE_H_
#define __H_CR_TREESTORAGE_H_

#include <unistd.h>

#include <string>
#include <vector>
#include <map>

#include <cr_class/cr_datacontrol.h>

class CR_TreeStorage {
public:
	CR_TreeStorage();
	~CR_TreeStorage();

	size_t count();

	int close(std::map<std::string,std::string> *overstocks=NULL);
	int reopen();

	void Touch();

	int Solidify();

	int Add(const std::string &key, const std::string &value, const std::string &lock_pass="",
		double locktimeout_sec=0.0, double waittimeout_sec=0.0);
	int Get(const std::string &key, std::string &value, double timeout_sec=0.0);
	int Set(const std::string &key, const std::string &newvalue, std::string *oldvalue=NULL,
		const std::string &lock_pass="", double locktimeout_sec=0.0, double waittimeout_sec=0.0);
	int Del(const std::string &key, const std::string &lock_pass="");

	int GetAll(std::map<std::string,std::string> &goods,
		const std::string *key_like_pattern=NULL, char escape_character='\\');

	int Clear(const std::string &lock_pass="");

	int Min(std::string &minkey, std::string *value=NULL);
	int Max(std::string &maxkey, std::string *value=NULL);

	int Prev(const std::string &key, std::string &prevkey, std::string *value=NULL);
	int Next(const std::string &key, std::string &nextkey, std::string *value=NULL);

	typedef enum {
		STAY_NONE	= 0,
		STAY_MIN	= 1,
		STAY_MAX	= 2
	} stay_mode_t;

	int FindNear(const std::string &key, std::string *foundkey, std::string *value,
		const stay_mode_t stay_mode=STAY_NONE);
	int FindNear(const std::string &key, std::string &foundkey, std::string &value,
		const stay_mode_t stay_mode=STAY_NONE);
	int FindNearByTree(const std::string &key, std::string &foundkey, std::string &value);

	int ListKeys(std::vector<std::string> &keys);
	int ListKeysRange(const std::string &lkey, const std::string &rkey,
		std::vector<std::string> &keys, const std::string &lock_pass="", double locktimeout_sec=0.0);

	int PopOne(const std::string &key, std::string &value, const std::string &lock_pass="",
		double timeout_sec=0.0);
	int PopOneScoped(const std::string &lkey, const std::string &rkey, std::string &foundkey,
		std::string &value, const std::string &lock_pass="", double timeout_sec=0.0);
	int PopOneScopedReverse(const std::string &lkey, const std::string &rkey, std::string &foundkey,
		std::string &value, const std::string &lock_pass="", double timeout_sec=0.0);

	int LockKey(const std::string &key, const std::string &lock_pass, double locktimeout_sec);
	ssize_t LockRange(const std::string &lkey, const std::string &rkey, const std::string &lock_pass,
		double locktimeout_sec);

	int UnlockKey(const std::string &key, const std::string &lock_pass);
	ssize_t UnlockRange(const std::string &lkey, const std::string &rkey, const std::string &lock_pass);
private:
	CR_DataControl::Descriptor _cr_desc;

	bool _is_static;

	void *_head_p;
	size_t _count;
};

#endif /* __H_CR_TREESTORAGE_H_ */
