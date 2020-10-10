#ifndef __H_CFM_SPLITTER_H__
#define __H_CFM_SPLITTER_H__

#include <string>
#include <vector>
#include <map>

class CFMSplitter {
public:
	int Merge(const std::vector<std::string> &samples_str_array,
		std::vector<std::string> *samples_p=NULL);

	int Load(const std::string &s);
	std::string Save();

	size_t KeySplit(const std::string &key);

private:
	std::map<std::string,size_t> _split_map;
};

#endif /* __H_CFM_SPLITTER_H__ */
