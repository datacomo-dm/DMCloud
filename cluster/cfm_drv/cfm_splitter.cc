#include <cfm_drv/cfm_splitter.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_class.pb.h>

int
CFMSplitter::Merge(const std::vector<std::string> &samples_str_array, std::vector<std::string> *samples_p)
{
    msgCRPairList key_samples;
    std::string samples_str;
    std::vector<std::string> samples;
    std::map<std::string,size_t> local_map;

    for (size_t i=0; i<samples_str_array.size(); i++) {
        int fret = CR_Class_NS::decompressx(samples_str_array[i], samples_str);
        if (fret)
            return fret;
        if (!key_samples.ParseFromString(samples_str))
            return ENOMSG;
        for (int i=0; i<key_samples.pair_list_size(); i++) {
            samples.push_back(key_samples.pair_list(i).key());
        }
    }

    if (samples.size() == 0)
        return ENOMSG;

    std::sort(samples.begin(), samples.end());

    for (size_t i=0; i<samples_str_array.size(); i++) {
        size_t pos = samples.size() / samples_str_array.size() * (i + 1);
        if (pos >= samples.size())
            pos = samples.size() - 1;
        local_map[samples[pos]] = i;
    }

    if (samples_p) {
        *samples_p = samples;
    }

    this->_split_map = local_map;

    return 0;
}

int
CFMSplitter::Load(const std::string &s)
{
    int fret;
    std::map<std::string,std::string> save_map;
    std::map<std::string,size_t> local_map;

    fret = CR_Class_NS::str_param_parse(s, &save_map);
    if (fret < 0)
        return (0 - fret);

    for (auto map_it=save_map.cbegin(); map_it!=save_map.cend(); map_it++) {
        local_map[map_it->first] = CR_Class_NS::str2u64(map_it->second);
    }

    this->_split_map = local_map;

    return 0;
}

std::string
CFMSplitter::Save()
{
    std::map<std::string,std::string> save_map;

    for (auto map_it=this->_split_map.cbegin(); map_it!=this->_split_map.cend(); map_it++) {
        save_map[map_it->first] = CR_Class_NS::u642str(map_it->second);
    }

    return CR_Class_NS::str_param_merge(save_map);
}

size_t
CFMSplitter::KeySplit(const std::string &key)
{
    auto map_it = this->_split_map.lower_bound(key);
    if (map_it == this->_split_map.end())
        return this->_split_map.size() - 1;
    else
        return map_it->second;
}
