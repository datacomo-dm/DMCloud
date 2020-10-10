#include <cr_class/cr_class.h>
#include <cr_class/cr_persistenceid.h>
#include <cr_class/cr_bigset.h>
#include <cr_class/cr_file.h>
#include <cr_class/cr_msgcode.h>
#include <cr_class/cr_compress.h>
#include <algorithm>

#define _CR_PERID_KN_FREE_ID_SET	"free_id_set"
#define _CR_PERID_KN_NEW_ID_ARRAY	"new_id_array"

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
id_record_cmp(const id_record &a, const id_record &b)
{
    if (a.id < b.id) {
        return true;
    } else {
        return false;
    }
}

static bool
id_record_time_cmp(const id_record &a, const id_record &b)
{
    if (a.autofree_time < b.autofree_time) {
        return true;
    } else {
        return false;
    }
}

////////////////////////////

class id_data_t {
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

    int read(const std::string &filename, int64_t id_range_min, int64_t id_range_max,
        bool sort_by_id=false)
    {
        int ret = 0;
        CR_BigSet<int64_t> *_free_id_set_p = new CR_BigSet<int64_t>;
        std::vector<id_record_t> *_new_id_array_p = new std::vector<id_record_t>;
        msgCRPairList msg_id;
        std::string msg_id_str;
        std::string msg_id_str_cmp;

        msg_id_str_cmp = CR_Class_NS::load_string(filename.c_str());
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

            if (sort_by_id) {
                std::stable_sort(_new_id_array_p->begin(), _new_id_array_p->end(), id_record_cmp);
            } else {
                std::stable_sort(_new_id_array_p->begin(), _new_id_array_p->end(), id_record_time_cmp);
            }
        } else {
            ret = _free_id_set_p->Add(id_range_min, id_range_max);
            if (ret)
                goto errout;
        }

        this->filename_data = filename;
        this->free_id_set_p = _free_id_set_p;
        this->new_id_array_p = _new_id_array_p;

        return ret;

    errout:
        if (_free_id_set_p)
            delete _free_id_set_p;
        if (_new_id_array_p)
            delete _new_id_array_p;

        return ret;
    }

    CR_BigSet<int64_t> *free_id_set_p;
    std::vector<id_record_t> *new_id_array_p;

private:
    int lock_fd;
    std::string filename_data;
};

////////////////////////////

int
main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <pid_data_file> [sort_by_id]\n", argv[0]);
        return EINVAL;
    }

    int ret;
    double cur_time = CR_Class_NS::clock_gettime();
    std::string pid_filename = argv[1];
    id_data_t id_data;
    bool sort_by_id = false;

    if (argc >= 3) {
        if (atoi(argv[2]))
            sort_by_id = true;
    }

    ret = id_data.read(pid_filename, 0, ((INT64_C(1) << 40) - 1), sort_by_id);
    if (ret) {
        fprintf(stderr, "id_data.read(\"%s\") == %s\n", pid_filename.c_str(), CR_Class_NS::strerrno(ret));
        return ret;
    }

    std::vector<int64_t> free_id_set_arr;

    ret = id_data.free_id_set_p->SaveToArray(free_id_set_arr, 0, INT64_MAX);
    if (ret) {
        fprintf(stderr, "id_data.idset2arr() == %s\n", CR_Class_NS::strerrno(ret));
        return ret;
    }

    printf("Free ID ranges:\n");
    for (size_t i=0; i<free_id_set_arr.size(); i+=2) {
        printf("%ld(0x%016lX) -> %ld(0x%016lX)\n", free_id_set_arr[i], free_id_set_arr[i],
          free_id_set_arr[i+1], free_id_set_arr[i+1]);
    }

    printf("NOT solidify ID:\n");
    for (size_t i=0; i< id_data.new_id_array_p->size(); i++) {
        id_record_t &newidinfo_tmp = (*(id_data.new_id_array_p))[i];
        printf("%ld(0x%016lX) expired at %f seconds\n", newidinfo_tmp.id, newidinfo_tmp.id,
          newidinfo_tmp.autofree_time - cur_time);
    }

    return 0;
}
