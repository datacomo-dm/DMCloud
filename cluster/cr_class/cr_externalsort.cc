#include <cr_class/cr_addon.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <cr_class/cr_externalsort.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_file.h>

#define _ESORT_MIN_FILESIZE		(1024 * 256)
#define _ESORT_MIN_SLICESIZE		(1024 * 32)

#define _ESORT_PART_FILENAME_SUFFIX	".sortpart"

static void _part_sort_func(CR_ExternalSort::PartInfo *part_p, CR_DataControl::Sem *_sort_sem_p);

static void _part_merge_func(std::list<CR_ExternalSort::PartInfo*> *src_partinfo_list_p,
    CR_ExternalSort::PartInfo* dst_partinfo_p, const size_t max_slice_size,
    CR_DataControl::Sem *_merge_sem_p);

static void _preload_func(CR_ExternalSort::PopMap *pop_map_p,
    CR_BlockQueue<CR_FixedLinearStorage*> *_preload_fifo_p,
    const size_t slice_size, volatile bool *force_quit_p);

static int _part_file_name_filter(const struct dirent *dp);

static void _do_fls_delete(CR_FixedLinearStorage *&fls_p, void *arg);

//////////////////////////////

static int
_part_file_name_filter(const struct dirent *dp)
{
    const static size_t _suffix_len = strlen(_ESORT_PART_FILENAME_SUFFIX);
    const char *sub_p = strstr(dp->d_name, _ESORT_PART_FILENAME_SUFFIX);

    if (!sub_p)
        return 0;

    if (strlen(sub_p) != _suffix_len)
        return 0;

    return 1;
}

static void
_do_fls_delete(CR_FixedLinearStorage *&fls_p, void *arg)
{
    if (fls_p)
        delete fls_p;
}

//////////////////////////////

static void
_preload_func(CR_ExternalSort::PopMap *pop_map_p, CR_BlockQueue<CR_FixedLinearStorage*> *_preload_fifo_p,
    const size_t slice_size, volatile bool *force_quit_p)
{
    int fret;

    int64_t obj_id;
    std::string key_tmp;
    std::string value_tmp;
    bool value_is_null_tmp;

    size_t kv_size;

    uint64_t last_fls_space;
    int64_t fls_id = 0;
    const void *fls_data;
    uint64_t fls_size;
    CR_FixedLinearStorage *fls_p = new CR_FixedLinearStorage(slice_size);

    last_fls_space = fls_p->GetFreeSpace();

    while (1) {
        if (*force_quit_p)
            goto out;

        fret = pop_map_p->PopOne(obj_id, key_tmp, value_tmp, value_is_null_tmp);
        if (fret == EAGAIN) {
            fls_p->Data(fls_data, fls_size);
            while (1) {
                if (*force_quit_p)
                    goto out;
                fret = _preload_fifo_p->push_back(fls_p, 1);
                if (!fret) {
                    fls_p = new CR_FixedLinearStorage(slice_size);
                    break;
                }
            }
            break;
        } else if (fret) {
            DPRINTF("MapFront failed[%s]\n", CR_Class_NS::strerrno(fret));
            goto out;
        } else {
            kv_size = key_tmp.size();
            if (!value_is_null_tmp)
                kv_size += value_tmp.size();

            if (kv_size > last_fls_space) {
                fls_p->Data(fls_data, fls_size);
                while (1) {
                    if (*force_quit_p)
                        goto out;
                    fret = _preload_fifo_p->push_back(fls_p, 1);
                    if (!fret) {
                        fls_p = new CR_FixedLinearStorage(slice_size);
                        break;
                    }
                }
                fls_p->Clear();
                fls_id++;
            }

            fret = fls_p->PushBack(obj_id, key_tmp,
              (value_is_null_tmp) ? (NULL) : (&value_tmp), &last_fls_space);
            if (fret) {
                DPRINTF("fls_p->PushBack failed[%s]\n", CR_Class_NS::strerrno(fret));
                goto out;
            }
        }
    }

out:
    if (fls_p)
        delete fls_p;

    return;
}

static void
_part_merge_func(std::list<CR_ExternalSort::PartInfo*> *src_partinfo_list_p,
    CR_ExternalSort::PartInfo* dst_partinfo_p, const size_t max_slice_size,
    CR_DataControl::Sem *_merge_sem_p)
{
    int fret;

    int64_t obj_id;
    std::string key_tmp;
    std::string value_tmp;
    bool value_is_null_tmp;

    size_t new_slice_size;
    int max_merge_level = 0;

    CR_ExternalSort::PartInfo *part_p;

    std::list<CR_ExternalSort::PartInfo*>::iterator partinfo_list_it;
    CR_ExternalSort::PopMap pop_map;

    if (!dst_partinfo_p) {
        DPRINTF("dst_partinfo_p == NULL\n");
        goto out;
    }

    if (!src_partinfo_list_p) {
        DPRINTF("src_partinfo_list_p == NULL\n");
        goto out;
    }

    if (src_partinfo_list_p->size() == 0) {
        DPRINTF("src_partinfo_list_p->size() == 0\n");
        goto out;
    }

    fret = pop_map.Init(*src_partinfo_list_p);
    if (fret) {
        DPRINTF("MapMake failed[%s]\n", CR_Class_NS::strerrno(fret));
        goto out;
    }

    new_slice_size = 0;

    for (partinfo_list_it=src_partinfo_list_p->begin();
      partinfo_list_it!=src_partinfo_list_p->end(); partinfo_list_it++) {

        part_p = *partinfo_list_it;
        if (part_p) {
            new_slice_size += part_p->slice_size;
            max_merge_level = CR_Class_NS::max(max_merge_level, part_p->merge_level);
        }
    }

    new_slice_size = CR_Class_NS::min(new_slice_size, max_slice_size);
    new_slice_size = CR_ROUND_TO_POW_OF_2(new_slice_size);
    dst_partinfo_p->merge_level = max_merge_level + CR_LOG_2_CEIL(src_partinfo_list_p->size());
    dst_partinfo_p->fd = CR_File::open(dst_partinfo_p->file_name.c_str(),
      O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (dst_partinfo_p->fd < 0) {
        DPRINTF("open file \"%s\" failed[%s]\n",
          dst_partinfo_p->file_name.c_str(), CR_Class_NS::strerrno(errno));
        goto out;
    }

    dst_partinfo_p->fls.ReSize(new_slice_size);

    while (1) {
        fret = pop_map.PopOne(obj_id, key_tmp, value_tmp, value_is_null_tmp);
        if (fret == EAGAIN) {
            if (dst_partinfo_p->disk_sem_p)
                dst_partinfo_p->disk_sem_p->wait();
            dst_partinfo_p->fls.SaveToFileDescriptor(dst_partinfo_p->fd,
              0, NULL, dst_partinfo_p->compress);
            if (dst_partinfo_p->disk_sem_p)
                dst_partinfo_p->disk_sem_p->post();
            dst_partinfo_p->fls.ReSize(0);
            CR_File::lseek(dst_partinfo_p->fd, 0, SEEK_SET);
            while (!src_partinfo_list_p->empty()) {
                part_p = src_partinfo_list_p->front();
                src_partinfo_list_p->pop_front();
                delete part_p;
            }
            dst_partinfo_p->is_sorted = true;
            break;
        } else if (fret) {
            DPRINTF("MapPopOne failed[%s]\n", CR_Class_NS::strerrno(fret));
            goto out;
        }

        do {
            if (value_is_null_tmp)
                fret = dst_partinfo_p->fls.PushBack(obj_id, key_tmp, NULL);
            else
                fret = dst_partinfo_p->fls.PushBack(obj_id, key_tmp, &value_tmp);
            if (fret == ENOBUFS) {
                if (dst_partinfo_p->disk_sem_p)
                    dst_partinfo_p->disk_sem_p->wait();
                dst_partinfo_p->fls.SaveToFileDescriptor(dst_partinfo_p->fd,
                  0, NULL, dst_partinfo_p->compress);
                if (dst_partinfo_p->disk_sem_p)
                    dst_partinfo_p->disk_sem_p->post();
                dst_partinfo_p->fls.Clear();
            } else if (fret) {
                DPRINTF("fls.PushBack failed[%s]\n", CR_Class_NS::strerrno(fret));
                goto out;
            }
        } while (fret == ENOBUFS);
    }

out:
    if (_merge_sem_p)
        _merge_sem_p->post();

    if (src_partinfo_list_p)
        delete src_partinfo_list_p;

    return;
}

//////////////////////////////

static void
_part_sort_func(CR_ExternalSort::PartInfo *part_p, CR_DataControl::Sem *_sort_sem_p)
{
    int ret = 0;

    if (!part_p)
        return;

    do {
        part_p->fd = CR_File::open(part_p->file_name.c_str(),
          O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (part_p->fd < 0) {
            DPRINTF("open file \"%s\" failed[%s]\n",
              part_p->file_name.c_str(), CR_Class_NS::strerrno(errno));
            ret = errno;
            break;
        }

        if (!part_p->is_sorted)
            part_p->fls.Sort();

        if (part_p->disk_sem_p)
            part_p->disk_sem_p->wait();

        ret = part_p->fls.SaveToFileDescriptor(part_p->fd, part_p->slice_size, NULL, part_p->compress);

        if (part_p->disk_sem_p)
            part_p->disk_sem_p->post();

        if (ret) {
            DPRINTF("save part failed[%s]\n", CR_Class_NS::strerrno(ret));
            break;
        }

        part_p->fls.ReSize(0);

        CR_File::lseek(part_p->fd, 0, SEEK_SET);
    } while (0);

    part_p->sort_errno = ret;

    if (!ret) {
        part_p->is_sorted = true;
    }

    if (_sort_sem_p)
        _sort_sem_p->post();

    return;
}

//////////////////////////////
//////////////////////////////

CR_ExternalSort::PartInfo::PartInfo(const std::string &file_name)
{
    this->disk_sem_p = NULL;
    this->fls.ReSize(0);
    this->file_name = file_name;
    this->fd = CR_File::open(file_name.c_str(), O_RDONLY);
    this->slice_size = 1024 * 1024;
    this->is_sorted = true;
    this->sort_errno = 0;
    this->next_pop_line_no = 0;
    this->compress = true;
    this->fls_size_mode = CR_FLS_SIZEMODE_LARGE;
    this->merge_level = 0;
    this->keep_file = false;
    this->_sort_thread_p = NULL;
    this->_fifo_in_p = NULL;
    this->_pop_timeout = 0;
    this->_read_only = true;
}

CR_ExternalSort::PartInfo::PartInfo(CR_BlockQueue<std::string> &fifo_in, const double pop_timeout)
{
    this->disk_sem_p = NULL;
    this->fls.ReSize(0);
    this->file_name.clear();
    this->fd = -1;
    this->slice_size = 1024 * 1024;
    this->is_sorted = true;
    this->sort_errno = 0;
    this->next_pop_line_no = 0;
    this->compress = true;
    this->fls_size_mode = CR_FLS_SIZEMODE_LARGE;
    this->merge_level = 0;
    this->keep_file = false;
    this->_sort_thread_p = NULL;
    this->_fifo_in_p = &fifo_in;
    this->_pop_timeout = pop_timeout;
    this->_read_only = true;
}

CR_ExternalSort::PartInfo::PartInfo(const int64_t part_id_in,
    const std::string &pathname, const size_t rough_file_size,
    const size_t rough_slice_size, const bool save_compress,
    const int size_mode, CR_DataControl::Sem *_disk_sem_p)
{
    this->disk_sem_p = _disk_sem_p;
    this->fls.ReSize(rough_file_size, size_mode);
    this->file_name = pathname;
    this->file_name += CR_Class_NS::i642str(part_id_in);
    this->file_name += _ESORT_PART_FILENAME_SUFFIX;
    this->fd = -1;
    this->slice_size = rough_slice_size;
    this->is_sorted = false;
    this->sort_errno = EINPROGRESS;
    this->next_pop_line_no = 0;
    this->compress = save_compress;
    this->fls_size_mode = size_mode;
    this->merge_level = 0;
    this->keep_file = false;
    this->_sort_thread_p = NULL;
    this->_fifo_in_p = NULL;
    this->_pop_timeout = 0;
    this->_read_only = false;
}

CR_ExternalSort::PartInfo::~PartInfo()
{
    this->WaitBGSort();

    if (this->fd >= 0) {
        CR_File::close(this->fd);
        this->fd = -1;
    }

    if (!this->_read_only && !this->keep_file)
        CR_File::unlink(this->file_name.c_str());
}

int
CR_ExternalSort::PartInfo::GetDataInfos(CR_FixedLinearStorage_NS::fls_infos_t &fls_info)
{
    if (!CR_FixedLinearStorage_NS::VerifyFileDescriptor(this->fd, &fls_info)) {
        return errno;
    }

    return 0;
}

void
CR_ExternalSort::PartInfo::WaitBGSort()
{
    std::lock_guard<std::mutex> _lck(this->_mtx);
    if (this->_read_only)
        return;
    if (this->_sort_thread_p) {
        this->_sort_thread_p->join();
        delete this->_sort_thread_p;
        this->_sort_thread_p = NULL;
    }
}

int
CR_ExternalSort::PartInfo::StartBGSortAndSave(CR_DataControl::Sem *_sort_sem_p)
{
    if (this->_read_only)
        return EPERM;
    if (!this->_sort_thread_p) {
        if (_sort_sem_p)
            _sort_sem_p->wait();
        this->_sort_thread_p = new std::thread(_part_sort_func, this, _sort_sem_p);
        return 0;
    } else {
        return EINPROGRESS;
    }
}

int
CR_ExternalSort::PartInfo::SortAndSave()
{
    if (this->_read_only)
        return EPERM;
    _part_sort_func(this, NULL);
    return this->sort_errno;
}

int
CR_ExternalSort::PartInfo::Front(int64_t &obj_id, std::string *key_str_p,
    std::string *value_str_p, bool *value_is_null_p)
{
    int ret = 0;

    const char *key_p;
    uint64_t key_size;
    const char *value_p;
    uint64_t value_size;

    while (1) {
        ret = this->fls.Query(this->next_pop_line_no,
          obj_id, key_p, key_size, value_p, value_size);

        if (ret == ERANGE) {
            if (this->fd >= 0) {
                ret = this->fls.LoadFromFileDescriptor(this->fd);
                if ((ret != 0) && (ret != ENOMSG)) {
                    DPRINTF("Query data from \"%s\"(%d:%ld) failed, %s\n", this->file_name.c_str(),
                      this->fd, (long)CR_File::lseek(this->fd, 0, SEEK_CUR), CR_Class_NS::strerrno(ret));
                }
            } else {
                std::string fls_data;
                if (this->_fifo_in_p) {
                    ret = this->_fifo_in_p->pop_front(fls_data, this->_pop_timeout);
                } else {
                    ret = ENOTSUP;
                }
                if (!ret) {
                    if (fls_data.size() > 0) {
                        ret = this->fls.LoadFromString(fls_data);
                    } else {
                        ret = EAGAIN;
                    }
                } else {
                    DPRINTF("Pop data failed[%s]\n", CR_Class_NS::strerrno(ret));
                }
            }
            if (!ret) {
                this->next_pop_line_no = 0;
                continue;
            } else if (ret == ENOMSG) {
                ret = EAGAIN;
            }
        }
        break;
    }

    if (!ret) {
        if (key_str_p) {
            key_str_p->assign(key_p, key_size);
        }

        if (value_p) {
            if (value_str_p) {
                value_str_p->assign(value_p, value_size);
            }
            if (value_is_null_p)
                *value_is_null_p = false;
        } else {
            if (value_str_p)
                value_str_p->clear();
            if (value_is_null_p)
                *value_is_null_p = true;
        }
    }

    return ret;
}

int
CR_ExternalSort::PartInfo::PopFront()
{
    this->next_pop_line_no++;
    return 0;
}

//////////////////////////////
//////////////////////////////

CR_ExternalSort::PopMap::PopMap()
{
}

CR_ExternalSort::PopMap::~PopMap()
{
}

int
CR_ExternalSort::PopMap::Init(std::list<CR_ExternalSort::PartInfo*> &partinfo_list)
{
    int ret = 0;
    int fret;
    int64_t first_obj_id;
    std::string first_key;
    CR_ExternalSort::PartInfo *part_p;
    std::list<CR_ExternalSort::PartInfo*>::iterator it;

    this->_order_map.clear();

    for (it=partinfo_list.begin(); it!=partinfo_list.end(); it++) {
        part_p = *it;
        if (part_p) {
            fret = part_p->Front(first_obj_id, &first_key);
            if (fret == EAGAIN) {
                continue;
            } else if (fret) {
                ret = fret;
                break;
            }
            this->_order_map.insert(std::pair<std::tuple<std::string,int64_t>,CR_ExternalSort::PartInfo*>
              (std::tuple<std::string,int64_t>(first_key, first_obj_id), part_p));
        }
    }

    return ret;
}

void
CR_ExternalSort::PopMap::Clear()
{
    this->_order_map.clear();
}

int
CR_ExternalSort::PopMap::PopOne(int64_t &obj_id, std::string &key, std::string &value, bool &value_is_null)
{
    int fret;

    std::multimap<std::tuple<std::string,int64_t>,CR_ExternalSort::PartInfo*>::iterator it;
    CR_ExternalSort::PartInfo *part_p;
    int64_t next_obj_id;
    std::string next_key;

    while (1) {
        if (this->_order_map.size() == 0)
            return EAGAIN;

        it = this->_order_map.begin();
        part_p = (*it).second;
        fret = part_p->Front(obj_id, &key, &value, &value_is_null);
        if (fret == EAGAIN) {
            this->_order_map.erase(it);
            continue;
        } else if (fret)
            return fret;
        fret = part_p->PopFront();
        fret = part_p->Front(next_obj_id, &next_key);
        if (!fret) {
            if (next_key != key) {
                this->_order_map.erase(it);
                this->_order_map.insert(std::pair<std::tuple<std::string,int64_t>,CR_ExternalSort::PartInfo*>
                  (std::tuple<std::string,int64_t>(next_key, next_obj_id), part_p));
            }
        }
        break;
    }

    return 0;
}

//////////////////////////////
//////////////////////////////

CR_ExternalSort::CR_ExternalSort()
{
    this->_max_sort_threads = 0;
    this->_max_disk_threads = 1;
    this->_max_merge_threads = 0;

    this->_sort_sem_p = new CR_DataControl::Sem(this->_max_sort_threads);
    this->_merge_sem_p = new CR_DataControl::Sem(this->_max_merge_threads);

    this->_merge_count_min = 2;
    this->_merge_count_max = 2;

    this->_sorted = false;
    this->_cur_part_info = NULL;
    this->_next_part_id = 0;
    this->_rough_file_size = 0;
    this->_size_mode = CR_FLS_SIZEMODE_LARGE;
    this->_merge_thread_stop = false;

    this->_lines_poped = 0;

    this->_preload_thread_p = NULL;
    this->_preload_thread_stop = true;
    this->_preload_slice_fls_p = new CR_FixedLinearStorage;
    this->_preload_slice_fls_next_line_no = 0;

    this->_keep_part_file = false;

    CR_FixedLinearStorage_NS::ClearInfos(this->_infos);

    this->_is_setarg_ok = false;
}

CR_ExternalSort::~CR_ExternalSort()
{
    this->do_clear();

    this->del_disk_sem();

    delete this->_preload_slice_fls_p;
    delete this->_merge_sem_p;
    delete this->_sort_sem_p;
}

void
CR_ExternalSort::Clear()
{
    std::lock_guard<std::mutex> _lck(this->_mtx);
    this->do_clear();
}

void
CR_ExternalSort::del_disk_sem()
{
    for (size_t i=0; i< this->_disk_info_arr.size(); i++) {
        std::pair<std::string,CR_DataControl::Sem*> &one_info = this->_disk_info_arr[i];
        if (one_info.second != NULL) {
            delete one_info.second;
            one_info.second = NULL;
        }
    }
}

void
CR_ExternalSort::do_clear()
{
    this->wait_all_thread();

    while (!this->_part_info_list.empty()) {
        PartInfo *tmp_part_info = this->_part_info_list.front();
        this->_part_info_list.pop_front();
        tmp_part_info->keep_file = this->_keep_part_file;
        delete tmp_part_info;
    }

    this->_pop_map.Clear();

    this->_sorted = false;
    this->_cur_part_info = NULL;
    this->_merge_thread_stop = false;

    this->_lines_poped = 0;

    this->_preload_fifo.clear();
    this->_preload_slice_fls_p->ReSize(0);
    this->_preload_slice_fls_next_line_no = 0;

    CR_FixedLinearStorage_NS::ClearInfos(this->_infos);
}

int
CR_ExternalSort::SetArgs(const std::string &arg_param_str, const std::vector<std::string> &path_names,
    const bool keep_part_file)
{
    if (path_names.size() == 0)
        return EINVAL;

    size_t rough_file_size =
      CR_Class_NS::str_getparam_u64(arg_param_str, EXTERNALSORT_ARGNAME_ROUGHFILESIZE);
    size_t rough_slice_size =
      CR_Class_NS::str_getparam_u64(arg_param_str, EXTERNALSORT_ARGNAME_ROUGHSLICESIZE);
    size_t max_slice_size =
      CR_Class_NS::str_getparam_u64(arg_param_str, EXTERNALSORT_ARGNAME_MAXSLICESIZE);
    size_t preload_cache_size =
      CR_Class_NS::str_getparam_u64(arg_param_str, EXTERNALSORT_ARGNAME_PRELOADCACHESIZE);
    unsigned int max_sort_threads =
      CR_Class_NS::str_getparam_u64(arg_param_str, EXTERNALSORT_ARGNAME_MAXSORTTHREADS);
    unsigned int max_disk_threads =
      CR_Class_NS::str_getparam_u64(arg_param_str, EXTERNALSORT_ARGNAME_MAXDISKTHREADS);
    unsigned int max_merge_threads =
      CR_Class_NS::str_getparam_u64(arg_param_str, EXTERNALSORT_ARGNAME_MAXMERGETHREADS);
    int default_size_mode =
      CR_Class_NS::str_getparam_i64(arg_param_str, EXTERNALSORT_ARGNAME_DEFAULTSIZEMODE);
    bool save_compress =
      CR_Class_NS::str_getparam_bool(arg_param_str, EXTERNALSORT_ARGNAME_ENABLECOMPRESS);
    unsigned int merge_count_min =
      CR_Class_NS::str_getparam_u64(arg_param_str, EXTERNALSORT_ARGNAME_MERGECOUNTMIN);
    unsigned int merge_count_max =
      CR_Class_NS::str_getparam_u64(arg_param_str, EXTERNALSORT_ARGNAME_MERGECOUNTMAX);
    bool enable_preload =
      CR_Class_NS::str_getparam_bool(arg_param_str, EXTERNALSORT_ARGNAME_ENABLEPRELOAD);

    rough_file_size = CR_Class_NS::max<size_t>(rough_file_size, _ESORT_MIN_FILESIZE);
    rough_slice_size = CR_Class_NS::max<size_t>(rough_slice_size, _ESORT_MIN_SLICESIZE);
    max_slice_size = CR_Class_NS::max(max_slice_size, rough_slice_size);
    max_slice_size = CR_Class_NS::min(max_slice_size, rough_file_size);
    merge_count_min = CR_Class_NS::max<unsigned int>(merge_count_min, 2);
    merge_count_max = CR_Class_NS::max(merge_count_max, merge_count_min);
    max_sort_threads = CR_Class_NS::min<unsigned int>(max_sort_threads, 256);
    max_disk_threads = CR_Class_NS::max<unsigned int>(max_disk_threads, 1);
    max_disk_threads = CR_Class_NS::min<unsigned int>(max_disk_threads, 256);
    max_merge_threads = CR_Class_NS::min<unsigned int>(max_merge_threads, 256);

    rough_file_size = CR_Class_NS::max(rough_file_size, (rough_slice_size * 16));
    preload_cache_size = CR_Class_NS::max(preload_cache_size, (rough_slice_size * 16));

    merge_count_min = CR_ROUND_TO_POW_OF_2(merge_count_min);
    merge_count_max = CR_ROUND_TO_POW_OF_2(merge_count_max);

    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (this->_part_info_list.size() > 0)
        return EINPROGRESS;

    this->_rough_file_size = rough_file_size;
    this->_rough_slice_size = rough_slice_size;
    this->_max_slice_size = max_slice_size;
    this->_preload_cache_size = preload_cache_size;
    this->_save_compress = save_compress;
    this->_size_mode = default_size_mode;
    this->_merge_count_min = merge_count_min;
    this->_merge_count_max = merge_count_max;
    this->_enable_preload = enable_preload;
    this->_keep_part_file = keep_part_file;

    this->_max_disk_threads = max_disk_threads;

    this->del_disk_sem();

    this->_disk_info_arr.clear();

    for (size_t i=0; i<path_names.size(); i++) {
        std::pair<std::string,CR_DataControl::Sem*> one_info;
        one_info.first = path_names[i];
        one_info.second = new CR_DataControl::Sem(this->_max_disk_threads);
        this->_disk_info_arr.push_back(one_info);
    }

    while (this->_max_sort_threads != max_sort_threads) {
        if (this->_max_sort_threads < max_sort_threads) {
            this->_sort_sem_p->post();
            this->_max_sort_threads++;
        } else {
            this->_sort_sem_p->wait();
            this->_max_sort_threads--;
        }
    }

    while (this->_max_merge_threads != max_merge_threads) {
        if (this->_max_merge_threads < max_merge_threads) {
            this->_merge_sem_p->post();
            this->_max_merge_threads++;
        } else {
            this->_merge_sem_p->wait();
            this->_max_merge_threads--;
        }
    }

    this->_last_arg_param_str = arg_param_str;

    this->_is_setarg_ok = true;

    return 0;
}

void
CR_ExternalSort::try_bg_merge()
{
    int fret;
    std::list<CR_ExternalSort::PartInfo*> *src_partinfo_list_p;
    CR_ExternalSort::PartInfo* dst_partinfo_p;

    fret = this->_merge_sem_p->trywait();
    if (fret)
        return;

    src_partinfo_list_p = new std::list<CR_ExternalSort::PartInfo*>;
    fret = this->pop_low_level_parts(*src_partinfo_list_p);
    if (fret) {
        delete src_partinfo_list_p;
        this->_merge_sem_p->post();
        return;
    }

    dst_partinfo_p = this->add_part(false);

    std::thread *thread_p = new std::thread(_part_merge_func, src_partinfo_list_p,
      dst_partinfo_p, this->_max_slice_size, this->_merge_sem_p);

    this->_merge_threads.push_back(thread_p);
}

int
CR_ExternalSort::pop_low_level_parts(std::list<CR_ExternalSort::PartInfo*> &partinfo_list)
{
    if (this->_sorted)
        return EALREADY;

    std::list<PartInfo*>::iterator it;
    std::multimap<int,std::list<PartInfo*>::iterator> it_map;
    std::multimap<int,std::list<PartInfo*>::iterator>::iterator it_map_it;
    std::pair<std::multimap<int,std::list<PartInfo*>::iterator>::iterator,
      std::multimap<int,std::list<PartInfo*>::iterator>::iterator> it_map_range;
    std::vector<std::list<PartInfo*>::iterator> it_vector;
    PartInfo *part_p;
    int min_merge_level;
    int max_merge_level;
    int find_merge_level = -1;
    unsigned int min_parts = this->_merge_count_min;
    unsigned int max_parts = this->_merge_count_max;

    if (min_parts < 2)
        min_parts = 2;

    if (max_parts < 2)
        max_parts = 2;

    for (it=this->_part_info_list.begin(); it!=this->_part_info_list.end(); it++) {
        part_p = *it;
        if (part_p) {
            if (part_p->is_sorted) {
                it_map.insert(std::pair<int,std::list<PartInfo*>::iterator>( part_p->merge_level, it));
            }
        }
    }

    if (it_map.size() < min_parts)
        return ENOENT;

    min_merge_level = it_map.begin()->first;
    max_merge_level = it_map.rbegin()->first;

    for (int i=min_merge_level; i<=max_merge_level; i++) {
        if (it_map.count(i) >= min_parts) {
            find_merge_level = i;
            break;
        }
    }

    if (find_merge_level < 0)
        return ENOENT;

    it_map_range = it_map.equal_range(find_merge_level);

    partinfo_list.clear();

    for (it_map_it=it_map_range.first; it_map_it!=it_map_range.second; it_map_it++) {
        if (max_parts == 0)
            break;
        it = it_map_it->second;
        part_p = *it;
        this->_part_info_list.erase(it);
        partinfo_list.push_back(part_p);
        max_parts--;
    }

    return 0;
}

int64_t
CR_ExternalSort::alloc_partid()
{
    int64_t ret = this->_next_part_id;
    this->_next_part_id++;
    return ret;
}

CR_ExternalSort::PartInfo *
CR_ExternalSort::add_part(const bool set_current)
{
    if (this->_rough_file_size == 0)
        return NULL;

    int64_t new_part_id = this->alloc_partid();
    std::pair<std::string,CR_DataControl::Sem*> &one_disk =
      this->_disk_info_arr[new_part_id % this->_disk_info_arr.size()];

    PartInfo *new_part_p = new PartInfo(new_part_id,
      one_disk.first, this->_rough_file_size, this->_rough_slice_size,
      this->_save_compress, this->_size_mode, one_disk.second);

    if (set_current)
        this->_cur_part_info = new_part_p;

    this->_part_info_list.push_back(new_part_p);

    return new_part_p;
}

void
CR_ExternalSort::wait_all_thread()
{
    std::list<PartInfo*>::iterator it;

    if (this->_preload_thread_p) {
        this->_preload_thread_stop = true;
        this->_preload_thread_p->join();
        delete this->_preload_thread_p;
        this->_preload_thread_p = NULL;
    }

    for (it=this->_part_info_list.begin(); it!=this->_part_info_list.end(); it++) {
        PartInfo *part_p = *it;
        if (part_p) {
            part_p->WaitBGSort();
        }
    }

    this->_merge_thread_stop = true;

    while (!this->_merge_threads.empty()) {
        std::thread *tmp_thread_p = this->_merge_threads.front();
        this->_merge_threads.pop_front();
        if (tmp_thread_p) {
            tmp_thread_p->join();
            delete tmp_thread_p;
        }
    }
}

int
CR_ExternalSort::push_one(const int64_t obj_id, const void *key_p, const size_t key_size,
    const void *value_p, const size_t value_size)
{
    int fret;
    int sort_ret;

    if (!this->_is_setarg_ok)
        return EINVAL;

    if (!this->_cur_part_info) {
        this->add_part();
        if (!this->_cur_part_info)
            return EFAULT;
    }

    if (key_p) {
        do {
            fret = this->_cur_part_info->fls.PushBack(obj_id, key_p, key_size, value_p, value_size);
            if (fret == ENOBUFS) {
                this->_cur_part_info->fls.MergeInfos(this->_infos);

                if (this->_max_sort_threads > 0)
                    sort_ret = this->_cur_part_info->StartBGSortAndSave(this->_sort_sem_p);
                else
                    sort_ret = this->_cur_part_info->SortAndSave();

                if (sort_ret)
                    return sort_ret;

                this->try_bg_merge();
                this->add_part();
            } else if (fret) {
                return fret;
            }
        } while (fret == ENOBUFS);
    } else {
        this->_cur_part_info->fls.MergeInfos(this->_infos);

        if (this->_max_sort_threads > 0)
            sort_ret = this->_cur_part_info->StartBGSortAndSave(this->_sort_sem_p);
        else
            sort_ret = this->_cur_part_info->SortAndSave();

        if (sort_ret)
            return sort_ret;

        this->wait_all_thread();

        fret = this->_pop_map.Init(this->_part_info_list);
        if (fret)
            return fret;

        if (this->_enable_preload) {
            size_t preload_fifo_level = this->_preload_cache_size / this->_rough_slice_size;
            if (preload_fifo_level < 1)
                preload_fifo_level = 1;
            this->_preload_fifo.set_args(preload_fifo_level, _do_fls_delete);
            this->_preload_thread_stop = false;
            this->_preload_thread_p = new std::thread(_preload_func, &(this->_pop_map),
              &(this->_preload_fifo), this->_rough_slice_size, &(this->_preload_thread_stop));
        }

        this->_sorted = true;
    }

    return 0;
}

int
CR_ExternalSort::PushOne(const int64_t obj_id, const void *key_p, const size_t key_size,
    const void *value_p, const size_t value_size)
{
    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (this->_sorted)
        return EALREADY;

    return this->push_one(obj_id, key_p, key_size, value_p, value_size);
}

int
CR_ExternalSort::pop_one(int64_t &obj_id, std::string &key, std::string &value, bool &value_is_null)
{
    int ret = 0;
    int fret;

    if (this->_lines_poped >= this->_infos.total_lines)
        return EAGAIN;

    if (this->_preload_thread_p) {
        while (1) {
            fret = this->_preload_slice_fls_p->Query(this->_preload_slice_fls_next_line_no,
              obj_id, key, value, value_is_null);
            if (fret == ERANGE) {
                delete this->_preload_slice_fls_p;
                this->_preload_slice_fls_p = NULL;
                while (1) {
                    fret = this->_preload_fifo.pop_front(this->_preload_slice_fls_p, 1);
                    if (!fret) {
                        break;
                    } else if (fret != ETIMEDOUT) {
                        DPRINTF("cache->pop_front failed[%s]\n", CR_Class_NS::strerrno(fret));
                        ret = fret;
                        goto out;
                    }
                }
                this->_preload_slice_fls_next_line_no = 0;
                continue;
            } else if (fret) {
                DPRINTF("cache_fls->Query failed[%s]\n", CR_Class_NS::strerrno(fret));
                ret = fret;
                goto out;
            }
            this->_preload_slice_fls_next_line_no++;
            break;
        }
    } else {
        ret = this->_pop_map.PopOne(obj_id, key, value, value_is_null);
    }

    if (!ret) {
        this->_lines_poped++;
    }

out:

    return ret;
}

int
CR_ExternalSort::PopOne(int64_t &obj_id, std::string &key, std::string &value, bool &value_is_null)
{
    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (!this->_sorted) {
        return EINPROGRESS;
    }

    if (!this->_is_setarg_ok) {
        return EINVAL;
    }

    return this->pop_one(obj_id, key, value, value_is_null);
}

int
CR_ExternalSort::Push(const msgCRPairList &data_in)
{
    int ret = 0;
    int64_t obj_id;
    const char *value_p;
    size_t value_size;
    int data_in_size = data_in.pair_list_size();
    bool is_done = false;

    if (data_in.has_done()) {
        is_done = data_in.done();
    }

    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (this->_sorted)
        return EALREADY;

    for (int i=0; i<data_in_size; i++) {
        const msgCRPair &cur_line = data_in.pair_list(i);

        const std::string &cur_key = cur_line.key();

        if (cur_line.has_obj_id()) {
            obj_id = cur_line.obj_id();
        } else {
            obj_id = 0;
        }

        if (cur_line.has_value()) {
            value_p = cur_line.value().data();
            value_size = cur_line.value().size();
        } else {
            value_p = NULL;
            value_size = 0;
        }

        ret = this->push_one(obj_id, cur_key.data(), cur_key.size(), value_p, value_size);
        if (ret) {
            break;
        }
    }

    if (!ret && is_done) {
        ret = this->push_one(0, NULL, 0, NULL, 0);
    }

    return ret;
}

int
CR_ExternalSort::Pop(msgCRPairList &data_out, const size_t max_line_count)
{
    int fret;

    int64_t obj_id;
    std::string key_tmp;
    std::string value_tmp;
    bool value_is_null;

    msgCRPair *one_line_p;

    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (!this->_sorted) {
        return EINPROGRESS;
    }

    if (!this->_is_setarg_ok) {
        return EINVAL;
    }

    for (size_t i=0; i<max_line_count; i++) {
        fret = this->pop_one(obj_id, key_tmp, value_tmp, value_is_null);
        if (fret == EAGAIN) {
            data_out.set_done(true);
            return 0;
        }
        one_line_p = data_out.add_pair_list();
        one_line_p->set_key(key_tmp);
        if (!value_is_null)
            one_line_p->set_value(value_tmp);
        one_line_p->set_obj_id(obj_id);
    }

    return 0;
}

int
CR_ExternalSort::Push(CR_FixedLinearStorage &data_in, const bool is_done)
{
    int ret = 0;

    int fret;
    int64_t obj_id;
    const char *key_p;
    uint64_t key_size;
    const char *value_p;
    uint64_t value_size;

    size_t data_line_no;

    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (this->_sorted)
        return EALREADY;

    for (data_line_no=0; ; data_line_no++) {
        fret = data_in.Query(data_line_no, obj_id, key_p, key_size, value_p, value_size);
        if (fret)
            break;
        ret = this->push_one(obj_id, key_p, key_size, value_p, value_size);
        if (ret) {
            break;
        }
    }

    if (!ret && is_done) {
        ret = this->push_one(0, NULL, 0, NULL, 0);
    }

    return ret;
}

int
CR_ExternalSort::Pop(CR_FixedLinearStorage &data_out, const size_t max_line_count)
{
    int ret = 0;

    int64_t obj_id;
    std::string key_tmp;
    std::string value_tmp;
    bool value_is_null;
    uint64_t data_out_space;

    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (!this->_sorted) {
        return EINPROGRESS;
    }

    if (!this->_is_setarg_ok) {
        return EINVAL;
    }

    data_out_space = data_out.GetFreeSpace();

    for (size_t i=0; i<max_line_count; i++) {
        if (data_out_space < this->_infos.longest_line_size)
            break;
        ret = this->pop_one(obj_id, key_tmp, value_tmp, value_is_null);
        if (ret) {
            if ((ret==EAGAIN) && (i!=0)) {
                ret = 0;
            }
            break;
        }
        ret = data_out.PushBack(obj_id, key_tmp, (value_is_null) ? (NULL) : (&value_tmp), &data_out_space);
        if (ret)
            break;
    }

    return ret;
}

int
CR_ExternalSort::MergePartPaths(const std::vector<std::string> &path_names,
    CR_FixedLinearStorage_NS::fls_infos_t *part_info_p, bool use_extrnal_info)
{
    std::string filename_tmp;
    struct dirent **names = NULL;
    struct dirent *cur_dp = NULL;
    int names_size;
    std::vector<std::string> file_names;

    for (size_t filename_id=0; filename_id<path_names.size(); filename_id++) {
        const std::string &cur_pathname = path_names[filename_id];
        names_size = ::scandir(cur_pathname.c_str(), &names, _part_file_name_filter, alphasort);
        if (names_size < 0) {
            return errno;
        }
        for (int name_id=0; name_id<names_size; name_id++) {
            cur_dp = names[name_id];
            filename_tmp = cur_pathname;
            filename_tmp.append("/");
            filename_tmp.append(cur_dp->d_name);
            file_names.push_back(filename_tmp);

            ::free(cur_dp);
        }
        ::free(names);
    }

    return this->MergePartFiles(file_names, part_info_p, use_extrnal_info);
}

int
CR_ExternalSort::MergePartFiles(const std::vector<std::string> &file_names,
    CR_FixedLinearStorage_NS::fls_infos_t *part_info_p, bool use_extrnal_info)
{
    int fret = 0;
    CR_FixedLinearStorage_NS::fls_infos_t fls_info_one;
    CR_FixedLinearStorage_NS::fls_infos_t fls_info_all;
    PartInfo *new_part_p;
    std::vector<PartInfo*> new_parts_array;

    if (!part_info_p)
        use_extrnal_info = false;

    std::lock_guard<std::mutex> _lck(this->_mtx);

    if (this->_sorted)
        return EALREADY;

    CR_FixedLinearStorage_NS::ClearInfos(fls_info_one);
    CR_FixedLinearStorage_NS::ClearInfos(fls_info_all);

    for (size_t file_name_id=0; file_name_id<file_names.size(); file_name_id++) {
        new_part_p = new PartInfo(file_names[file_name_id]);
        new_parts_array.push_back(new_part_p);
        if (!use_extrnal_info) {
            fret = new_part_p->GetDataInfos(fls_info_one);
            if (fret)
                break;
            CR_FixedLinearStorage_NS::MergeInfo(fls_info_all, fls_info_one);
        }
    }

    if (!fret) {
        if (use_extrnal_info) {
            CR_FixedLinearStorage_NS::MergeInfo(this->_infos, *part_info_p);
        } else {
            CR_FixedLinearStorage_NS::MergeInfo(this->_infos, fls_info_all);
            if (part_info_p) {
                memcpy(part_info_p, &fls_info_all, sizeof(fls_info_all));
            }
        }
        for (size_t new_part_id=0; new_part_id<new_parts_array.size(); new_part_id++) {
            this->_part_info_list.push_back(new_parts_array[new_part_id]);
        }
        this->try_bg_merge();
    } else {
        for (size_t new_part_id=0; new_part_id<new_parts_array.size(); new_part_id++) {
            delete new_parts_array[new_part_id];
        }
    }

    return fret;
}

void
CR_ExternalSort::GetInfos(CR_FixedLinearStorage_NS::fls_infos_t &fls_infos)
{
    std::lock_guard<std::mutex> _lck(this->_mtx);
    memcpy(&fls_infos, &(this->_infos), sizeof(this->_infos));
}

size_t
CR_ExternalSort::GetLongestKeySize()
{
    std::lock_guard<std::mutex> _lck(this->_mtx);
    return this->_infos.longest_key_size;
}

size_t
CR_ExternalSort::GetLongestValueSize()
{
    std::lock_guard<std::mutex> _lck(this->_mtx);
    return this->_infos.longest_value_size;
}

size_t
CR_ExternalSort::GetLongestLineSize()
{
    std::lock_guard<std::mutex> _lck(this->_mtx);
    return this->_infos.longest_line_size;
}

uint64_t
CR_ExternalSort::GetTotalLinesPoped()
{
    std::lock_guard<std::mutex> _lck(this->_mtx);
    return this->_lines_poped;
}

//////////////////////////////
//////////////////////////////

CR_ExternalSort::PopOnly::PopOnly()
{
}

CR_ExternalSort::PopOnly::~PopOnly()
{
    this->do_clear();
}

void
CR_ExternalSort::PopOnly::do_clear()
{
    while (!this->_part_info_list.empty()) {
        CR_ExternalSort::PartInfo *tmp_part_info = this->_part_info_list.front();
        this->_part_info_list.pop_front();
        delete tmp_part_info;
    }
}

void
CR_ExternalSort::PopOnly::Clear()
{
    this->_lck.lock();

    this->do_clear();

    this->_lck.unlock();
}

int
CR_ExternalSort::PopOnly::LoadFiles(const std::vector<std::string> &part_filename_array)
{
    int ret = 0;

    this->_lck.lock();

    if (this->_part_info_list.empty()) {
        for (size_t i=0; i<part_filename_array.size(); i++) {
            PartInfo *new_part_p = new PartInfo(part_filename_array[i]);
            this->_part_info_list.push_back(new_part_p);
        }
        ret = this->_pop_map.Init(this->_part_info_list);
    } else {
        ret = ENOTEMPTY;
    }

    this->_lck.unlock();

    return ret;
}

int
CR_ExternalSort::PopOnly::PopOne(int64_t &obj_id, std::string &key, std::string &value, bool &value_is_null)
{
    int ret = 0;

    this->_lck.lock();

    ret = this->_pop_map.PopOne(obj_id, key, value, value_is_null);

    this->_lck.unlock();

    return ret;
}
