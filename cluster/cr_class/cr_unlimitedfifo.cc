#include <cr_class/cr_addon.h>
#include <fcntl.h>
#include <cr_class/cr_unlimitedfifo.h>
#include <cr_class/cr_fixedlinearstorage.h>

#define UNLIMITEDFIFO_MIN_FILESIZE	(65536)

CR_UnlimitedFifo::CR_UnlimitedFifo(const std::string &file_name_prefix, size_t rough_file_size,
    const bool save_compress)
{
    this->do_set_args(file_name_prefix, rough_file_size, save_compress);
    this->do_init();
}

CR_UnlimitedFifo::~CR_UnlimitedFifo()
{
    this->do_clear();
}

void
CR_UnlimitedFifo::Clear()
{
    CANCEL_DISABLE_ENTER();

    this->_cmtx.lock();

    this->do_clear();
    this->do_init();

    this->_cmtx.unlock();

    CANCEL_DISABLE_LEAVE();
}

void
CR_UnlimitedFifo::do_init()
{
    CR_FixedLinearStorage *tmp_fls_p = new
      CR_FixedLinearStorage(this->_rough_file_size, CR_FLS_SIZEMODE_MIDDLE);

    this->_fls_head_size = tmp_fls_p->GetHeadSize();

    this->_fls_arr.push_back(tmp_fls_p);

    this->_next_fls_id = 0;

    this->_push_fls_pos = 0;
    this->_pop_fls_pos = 0;

    this->_next_pop_line_no = 0;
}

void
CR_UnlimitedFifo::do_clear()
{
    for (size_t i=0; i< this->_fls_arr.size(); i++) {
        CR_FixedLinearStorage *tmp_fls_p = this->_fls_arr[i];
        if (tmp_fls_p) {
            ::unlink(tmp_fls_p->GetLocalTag().c_str());
            delete tmp_fls_p;
        }
    }

    this->_fls_arr.clear();
}

int
CR_UnlimitedFifo::do_set_args(const std::string &file_name_prefix, size_t rough_file_size,
    const bool save_compress)
{
    if (rough_file_size < UNLIMITEDFIFO_MIN_FILESIZE)
        rough_file_size = UNLIMITEDFIFO_MIN_FILESIZE;

    this->_rough_file_size = rough_file_size;
    this->_file_name_prefix = file_name_prefix;
    this->_save_compress = save_compress;

    return 0;
}

int
CR_UnlimitedFifo::SetArgs(const std::string &file_name_prefix, const size_t rough_file_size,
    const bool save_compress)
{
    int ret;

    CANCEL_DISABLE_ENTER();

    this->_cmtx.lock();

    ret = this->do_set_args(file_name_prefix, rough_file_size, save_compress);

    this->_cmtx.unlock();

    CANCEL_DISABLE_LEAVE();

    return ret;
}

std::string
CR_UnlimitedFifo::do_gen_filename()
{
    std::string ret;

    ret = this->_file_name_prefix;
    ret += CR_Class_NS::u642str(this->_next_fls_id);
    ret += "_";
    ret += CR_Class_NS::u642str(CR_Class_NS::rand());

    this->_next_fls_id++;

    return ret;
}

int
CR_UnlimitedFifo::do_pushback(const int64_t obj_id, const void *key_p, const size_t key_size,
    const void *value_p, const size_t value_size)
{
    int fret;

    while (1) {
        CR_FixedLinearStorage *push_fls_p = this->_fls_arr[this->_push_fls_pos];

        if (!push_fls_p)
            return EFAULT;

        fret = push_fls_p->PushBack(obj_id, key_p, key_size, value_p, value_size);
        if (fret == ENOBUFS) {
            std::string saveto_filename = this->do_gen_filename();

            push_fls_p->SetTag(saveto_filename);
            push_fls_p->SetLocalTag(saveto_filename);

            fret = push_fls_p->SaveToFile(saveto_filename, 0, NULL, this->_save_compress);
            if (fret) {
                DPRINTF("SaveToFile(\"%s\") failed[%s]\n",
                  saveto_filename.c_str(), CR_Class_NS::strerrno(fret));
                return fret;
            }

            if (this->_push_fls_pos > this->_pop_fls_pos) {
                push_fls_p->ReSize(0);
            }

            size_t alloc_size = key_size + this->_fls_head_size;
            if (value_p)
                alloc_size += value_size;

            CR_FixedLinearStorage *new_fls_p = new CR_FixedLinearStorage(
              MAX(this->_rough_file_size, alloc_size), CR_FLS_SIZEMODE_MIDDLE);

            this->_fls_arr.push_back(new_fls_p);
            this->_push_fls_pos++;

            continue;
        } else if (fret) {
            return fret;
        }
        break;
    }

    return 0;
}

int
CR_UnlimitedFifo::do_popfront(int64_t &obj_id, std::string &key, std::string &value, bool &value_is_null)
{
    int fret;

    const char *key_p;
    uint64_t key_size;
    const char *value_p;
    uint64_t value_size;

    while (1) {
        CR_FixedLinearStorage *pop_fls_p = this->_fls_arr[this->_pop_fls_pos];

        if (!pop_fls_p)
            return EFAULT;

        if (pop_fls_p->GetTotalLines() == 0) {
            std::string loadfrom_filename = pop_fls_p->GetLocalTag();
            fret = pop_fls_p->LoadFromFile(loadfrom_filename);
            if (fret == ENOENT) {
                return EAGAIN;
            } else if (fret) {
                DPRINTF("LoadFromFile(\"%s\") failed[%s]\n",
                  loadfrom_filename.c_str(), CR_Class_NS::strerrno(fret));
                return fret;
            }
        }

        fret = pop_fls_p->Query(this->_next_pop_line_no, obj_id, key_p, key_size, value_p, value_size);
        if (fret == ERANGE) {
#if defined(DIAGNOSTIC)
            if (this->_pop_fls_pos > this->_push_fls_pos)
                THROWF("_pop_fls_pos{%llu} > _push_fls_pos{%llu}\n",
                  (long long unsigned)this->_pop_fls_pos,
                  (long long unsigned)this->_push_fls_pos);
#endif // DIAGNOSTIC

            if (this->_pop_fls_pos == this->_push_fls_pos)
                return EAGAIN;

            ::unlink(pop_fls_p->GetLocalTag().c_str());
            delete pop_fls_p;
            this->_fls_arr[this->_pop_fls_pos] = NULL;

            this->_pop_fls_pos++;
            this->_next_pop_line_no = 0;

            continue;
        } else if (fret) {
            return fret;
        }

        this->_next_pop_line_no++;

        break;
    }

    key.assign(key_p, key_size);

    if (value_p) {
        value.assign(value_p, value_size);
        value_is_null = false;
    } else {
        value_is_null = true;
    }

    return 0;
}

int
CR_UnlimitedFifo::PushBack(const int64_t obj_id, const void *key_p, const size_t key_size,
    const void *value_p, const size_t value_size)
{
    int ret;

    CANCEL_DISABLE_ENTER();
    this->_cmtx.lock();

    ret = this->do_pushback(obj_id, key_p, key_size, value_p, value_size);
    if (!ret)
        this->_cmtx.broadcast();

    this->_cmtx.unlock();
    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_UnlimitedFifo::PushBack(const int64_t obj_id, const std::string &key, const std::string *value_p)
{
    int ret;

    if (value_p)
        ret = this->PushBack(obj_id, key.data(), key.size(), value_p->data(), value_p->size());
    else
        ret = this->PushBack(obj_id, key.data(), key.size(), NULL, 0);

    return ret;
}

int
CR_UnlimitedFifo::PushBack(CR_FixedLinearStorage &data_in, uint64_t *err_lineno_p)
{
    int ret = 0;
    int64_t obj_id;
    const char *key_p;
    uint64_t key_size;
    const char *value_p;
    uint64_t value_size;
    uint64_t data_lines = data_in.GetTotalLines();

    CANCEL_DISABLE_ENTER();
    this->_cmtx.lock();

    for (uint64_t i=0; i<data_lines; i++) {
        ret = data_in.Query(i, obj_id, key_p, key_size, value_p, value_size);
        if (!ret)
            ret = this->do_pushback(obj_id, key_p, key_size, value_p, value_size);
        if (ret) {
            if (err_lineno_p) {
                *err_lineno_p = i;
            }
            break;
        }
    }

    this->_cmtx.unlock();
    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_UnlimitedFifo::PopFront(int64_t &obj_id, std::string &key, std::string &value,
    bool &value_is_null, const double timeout_sec)
{
    int ret = 0;
    double exit_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;
    double wait_gap = (timeout_sec <= 0.1) ? timeout_sec : 0.1;

    CANCEL_DISABLE_ENTER();
    this->_cmtx.lock();

    do {
        ret = this->do_popfront(obj_id, key, value, value_is_null);
        if (ret == EAGAIN) {
            if (wait_gap > 0.0)
                this->_cmtx.wait(wait_gap);
            else
                break;
        } else
            break;
    } while (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) < exit_time);

    this->_cmtx.unlock();
    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_UnlimitedFifo::PopFront(CR_FixedLinearStorage &data_out, const size_t max_line_count, double timeout_sec)
{
    int ret = 0;
    int fret;
    size_t line_count = 0;
    int64_t objid_tmp;
    std::string key_tmp;
    std::string value_tmp;
    bool value_is_null;
    double exit_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;
    double wait_gap = (timeout_sec <= 0.1) ? timeout_sec : 0.1;

    CANCEL_DISABLE_ENTER();
    this->_cmtx.lock();

    while (line_count < max_line_count) {
        value_tmp.clear();
        fret = this->do_popfront(objid_tmp, key_tmp, value_tmp, value_is_null);
        if (!fret) {
            fret = data_out.PushBack(objid_tmp, key_tmp, (value_is_null)?(NULL):(&value_tmp));
            if (fret == ENOBUFS) {
                std::string old_data_str;
                CR_FixedLinearStorage tmp_fls;
                uint64_t old_total_lines;
                fret = data_out.SaveToString(old_data_str);
                if (fret) {
                    ret = fret;
                    break;
                }
                tmp_fls.MapFromArray(old_data_str.data(), old_data_str.size());
                old_total_lines = tmp_fls.GetTotalLines();
                data_out.ReSize(old_data_str.size() + key_tmp.size() + value_tmp.size());
                for (uint64_t i=0; i<old_total_lines; i++) {
                    int64_t obj_id;
                    const char *key_p;
                    uint64_t key_size;
                    const char *value_p;
                    uint64_t value_size;
                    tmp_fls.Query(i, obj_id, key_p, key_size, value_p, value_size);
                    data_out.PushBack(obj_id, key_p, key_size, value_p, value_size);
                }
                ret = data_out.PushBack(objid_tmp, key_tmp, (value_is_null)?(NULL):(&value_tmp));
                break;
            }
            line_count++;
        } else {
            if (fret == EAGAIN) {
                if (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) >= exit_time) {
                    break;
                } else {
                    this->_cmtx.wait(wait_gap);
                }
            } else {
                ret = fret;
                break;
            }
        }
    }

    this->_cmtx.unlock();
    CANCEL_DISABLE_LEAVE();

    return ret;
}
