#include <cr_class/cr_class.h>
#include <cr_class/cr_persistenceid.h>
#include <cr_class/cr_bigset.h>
#include <cr_class/cr_file.h>
#include <cr_class/cr_msgcode.h>
#include <cr_class/cr_compress.h>
#include <algorithm>

#define _CR_PERID_KN_FREE_ID_SET    "free_id_set"
#define _CR_PERID_KN_NEW_ID_ARRAY   "new_id_array"
#define _CR_PERID_MAX_NEW_ID_COUNT  ((1024 * 1024))

////////////////////////////

typedef struct id_record {
    int64_t id;
    double autofree_time;

    id_record(int64_t _id, double _autofree_time)
    {
        this->id = _id;
        this->autofree_time = _autofree_time;
    }
} id_record_t;

static bool
id_record_timeout_cmp(const id_record &a, const id_record &b)
{
    if (a.autofree_time < b.autofree_time) {
        return true;
    } else {
        return false;
    }
}

////////////////////////////

class id_data_t
{
public:
    id_data_t()
    {
        this->lock_fd = -1;
        this->free_id_set_p = NULL;
        this->new_id_array_p = NULL;
    }

    ~id_data_t()
    {
        if (this->lock_fd >= 0) {
            CR_File::flock(this->lock_fd, LOCK_UN);
            CR_File::close(this->lock_fd);
        }
        if (this->free_id_set_p)
            delete this->free_id_set_p;
        if (this->new_id_array_p)
            delete this->new_id_array_p;
    }

    int get(const std::string &filename, int64_t id_range_min, int64_t id_range_max,
            bool do_sort=false)
    {
        int ret = 0;
        std::string _filename_data = filename + std::string(CR_PERSISTENCEID_DATAFILE_SUFFIX);
        std::string _filename_lock = filename + std::string(CR_PERSISTENCEID_LOCKFILE_SUFFIX);
        int _lock_fd = -1;
        CR_BigSet<int64_t> *_free_id_set_p = new CR_BigSet<int64_t>;
        std::vector<id_record_t> *_new_id_array_p = new std::vector<id_record_t>;
        msgCRPairList msg_id;
        std::string msg_id_str;
        std::string msg_id_str_cmp;

        _lock_fd = CR_File::openlock(_filename_lock.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0640);
        if (_lock_fd < 0) {
            ret = errno;
            goto errout;
        }

        msg_id_str_cmp = CR_Class_NS::load_string(_filename_data.c_str());
        if (msg_id_str_cmp.size() > 0)
            msg_id_str = CR_Compress::decompress(msg_id_str_cmp);

        if (msg_id_str.size() > 0) {
            if (!msg_id.ParseFromString(msg_id_str)) {
                ret = ENOMSG;
                goto errout;
            }
            if (msg_id.pair_list_size() < 2) {
                ret = ENOMSG;
                goto errout;
            }
            if ((msg_id.pair_list(0).key() != _CR_PERID_KN_FREE_ID_SET)
                ||(msg_id.pair_list(1).key() != _CR_PERID_KN_NEW_ID_ARRAY)) {
                ret = ENOMSG;
                goto errout;
            }
            if (!msg_id.pair_list(0).has_value() || !msg_id.pair_list(1).has_value()) {
                ret = ENOMSG;
                goto errout;
            }

            ret = _free_id_set_p->LoadFromArray((const int64_t*)msg_id.pair_list(0).value().data(),
                                                msg_id.pair_list(0).value().size() / sizeof(int64_t));
            if (ret)
                goto errout;
            CR_Class_NS::vector_assign(*_new_id_array_p, msg_id.pair_list(1).value().data(),
                                       msg_id.pair_list(1).value().size());

            if (do_sort) {
                std::stable_sort(_new_id_array_p->begin(), _new_id_array_p->end(), id_record_timeout_cmp);
            }
        } else {
            ret = _free_id_set_p->Add(id_range_min, id_range_max);
            if (ret)
                goto errout;
        }

        this->filename_data = _filename_data;
        this->lock_fd = _lock_fd;
        this->free_id_set_p = _free_id_set_p;
        this->new_id_array_p = _new_id_array_p;

        return ret;

    errout:
        if (_lock_fd >= 0) {
            CR_File::flock(_lock_fd, LOCK_UN);
            CR_File::close(_lock_fd);
        }
        if (_free_id_set_p)
            delete _free_id_set_p;
        if (_new_id_array_p)
            delete _new_id_array_p;

        return ret;
    }

    int put()
    {
        if (this->lock_fd < 0)
            return EINVAL;

        int ret = 0;
        std::vector<int64_t> set_usage_arr;
        std::string msg_id_str;
        std::string msg_id_str_cmp;
        msgCRPairList msg_id;
        msgCRPair *msg_freeidset_p = msg_id.add_pair_list();
        msgCRPair *msg_newidarr_p = msg_id.add_pair_list();

        if (!msg_freeidset_p || !msg_newidarr_p)
            return ENOMEM;

        ret = this->free_id_set_p->SaveToArray(set_usage_arr, 0, INT64_MAX);
        if (ret)
            return ret;

        msg_freeidset_p->set_key(_CR_PERID_KN_FREE_ID_SET);
        msg_freeidset_p->set_value(set_usage_arr.data(), set_usage_arr.size() * sizeof(int64_t));
        msg_newidarr_p->set_key(_CR_PERID_KN_NEW_ID_ARRAY);
        msg_newidarr_p->set_value(this->new_id_array_p->data(),
                                  this->new_id_array_p->size() * sizeof(id_record_t));

        if (!msg_id.SerializeToString(&msg_id_str))
            return EFAULT;

        msg_id_str_cmp = CR_Compress::compress(msg_id_str, CR_Compress::CT_SNAPPY);

        ret = CR_Class_NS::save_string(this->filename_data.c_str(), msg_id_str_cmp);
        if (ret)
            return ret;

        return 0;
    }

    CR_BigSet<int64_t> *free_id_set_p;
    std::vector<id_record_t> *new_id_array_p;

private:
    int lock_fd;
    std::string filename_data;
};

////////////////////////////

CR_PersistenceID::CR_PersistenceID()
{
    this->_id_range_min = INT64_MAX;
    this->_id_range_max = 0;
    this->_autofree_timeout_sec = 0;
}

CR_PersistenceID::~CR_PersistenceID()
{
}

int
CR_PersistenceID::SetArgs(const std::string &filename, int64_t id_range_min, int64_t id_range_max,
                          double autofree_timeout_sec)
{
    if ((id_range_max < 0) || (id_range_min > id_range_max))
        return EINVAL;

    std::lock_guard<std::mutex> _lck(this->_mtx);

    this->_filename = filename;
    this->_id_range_min = id_range_min;
    this->_id_range_max = id_range_max;
    this->_autofree_timeout_sec = autofree_timeout_sec;

    return 0;
}

int
CR_PersistenceID::Alloc(size_t alloc_count, std::vector<int64_t> &id_arr)
{
    int ret = 0;
    double cur_time = CR_Class_NS::clock_gettime();
    double autofree_timeout = cur_time + this->_autofree_timeout_sec;
    size_t newid_arr_pos = 0;
    std::vector<int64_t> id_arr_local;
    std::vector<id_record_t> new_new_id_array;

    std::lock_guard<std::mutex> _lck(this->_mtx);

    id_data_t id_data;

    if (alloc_count > _CR_PERID_MAX_NEW_ID_COUNT)  // syncode from huangwei's  git
        return ENOSPC;

    ret = id_data.get(this->_filename, this->_id_range_min, this->_id_range_max, true);
    if (ret)
        return ret;

    // for protobuf size limit, avoid data error,syncode from huangwei's git
    if (id_data.new_id_array_p->size() + alloc_count > _CR_PERID_MAX_NEW_ID_COUNT)
        return ENOSPC;

    while ((alloc_count > 0) && (newid_arr_pos < id_data.new_id_array_p->size())) {
        id_record_t &newidinfo_tmp = (*(id_data.new_id_array_p))[newid_arr_pos];
        if (newidinfo_tmp.autofree_time <= cur_time) {
            newidinfo_tmp.autofree_time = autofree_timeout;
            id_arr_local.push_back(newidinfo_tmp.id);
            alloc_count--;
            newid_arr_pos++;
        } else
            break;
    }

    for (size_t i=0; i<alloc_count; i++) {
        int64_t tmp_id;
        ret = id_data.free_id_set_p->PopFirst(tmp_id);
        if (ret)
            return ret;
        id_arr_local.push_back(tmp_id);
        id_data.new_id_array_p->push_back(id_record_t(tmp_id, autofree_timeout));
    }

    for (size_t i=0; i< id_data.new_id_array_p->size(); i++) {
        const id_record_t &newid_tmp = (*(id_data.new_id_array_p))[i];
        if (newid_tmp.autofree_time <= cur_time) {
            id_data.free_id_set_p->Add(newid_tmp.id);
        } else {
            new_new_id_array.push_back(newid_tmp);
        }
    }

    id_data.new_id_array_p->clear();

    *id_data.new_id_array_p = new_new_id_array;

    if (!ret) {
        id_data.put();
        id_arr = id_arr_local;
    }

    return ret;
}

int
CR_PersistenceID::Free(const std::vector<int64_t> &id_arr)
{
    int ret = 0;
    std::map<int64_t,double> newid_map;

    std::lock_guard<std::mutex> _lck(this->_mtx);

    for (size_t i=0; i<id_arr.size(); i++) {
        if ((id_arr[i] < this->_id_range_min) || (id_arr[i] > this->_id_range_max))
            return ERANGE;
    }

    id_data_t id_data;

    ret = id_data.get(this->_filename, this->_id_range_min, this->_id_range_max);
    if (ret)
        return ret;

    for (size_t i=0; i< id_data.new_id_array_p->size(); i++) {
        const id_record_t &newid_tmp = (*(id_data.new_id_array_p))[i];
        newid_map[newid_tmp.id] = newid_tmp.autofree_time;
    }

    for (size_t i=0; i<id_arr.size(); i++) {
        ret = id_data.free_id_set_p->Add(id_arr[i]);
        if (ret) {
            if (ret == EEXIST)
                ret = EALREADY;
            return ret;
        }
        newid_map.erase(id_arr[i]);
    }

    id_data.new_id_array_p->clear();

    for (auto it=newid_map.cbegin(); it!=newid_map.cend(); it++) {
        id_data.new_id_array_p->push_back(id_record_t(it->first, it->second));
    }

    if (!ret)
        id_data.put();

    return ret;
}

int
CR_PersistenceID::Solidify(const std::vector<int64_t> &id_arr)
{
    int ret = 0;
    std::map<int64_t,double> newid_map;

    std::lock_guard<std::mutex> _lck(this->_mtx);

    for (size_t i=0; i<id_arr.size(); i++) {
        if ((id_arr[i] < this->_id_range_min) || (id_arr[i] > this->_id_range_max))
            return ERANGE;
    }

    id_data_t id_data;

    ret = id_data.get(this->_filename, this->_id_range_min, this->_id_range_max);
    if (ret)
        return ret;

    for (size_t i=0; i< id_data.new_id_array_p->size(); i++) {
        const id_record_t &newid_tmp = (*(id_data.new_id_array_p))[i];
        newid_map[newid_tmp.id] = newid_tmp.autofree_time;
    }

    for (size_t i=0; i<id_arr.size(); i++) {
        if (newid_map.find(id_arr[i]) == newid_map.end()) {
            ret = id_data.free_id_set_p->Test(id_arr[i], true);
            if (ret == EEXIST)
                ret = ENOENT;
            else
                ret = EALREADY;
            return ret;
        }
        newid_map.erase(id_arr[i]);
    }

    id_data.new_id_array_p->clear();

    for (auto it=newid_map.cbegin(); it!=newid_map.cend(); it++) {
        id_data.new_id_array_p->push_back(id_record_t(it->first, it->second));
    }

    if (!ret)
        id_data.put();

    return ret;
}
