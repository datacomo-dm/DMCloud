#include <cr_class/cr_addon.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>

#include <cr_class/cr_fixedqueue.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_file.h>
#include <cr_class/cr_compress.h>

#define CR_MSG_QUEUESTORAGE		(85)
#define CR_MSG_QUEUESTORAGE_SNAPPY	(87)

#define _FQ_MAX_TAG_LEN			((1UL<<7) - sizeof(uint16_t))

#define THIS_FQ_DATA			((fq_data_t*)(this->_private_data))
#define FQ_DATA				((fq_data_t*)(data_p))

typedef struct {
    int64_t obj_id;
    size_t k_len;
    size_t v_len;
    int8_t data[0];
} line_data_large_t;

typedef struct {
    int64_t obj_id;
    uint32_t k_len;
    uint32_t v_len;
    int8_t data[0];
} line_data_middle_t;

typedef struct {
    int64_t obj_id;
    uint16_t k_len;
    uint16_t v_len;
    int8_t data[0];
} line_data_small_t;

typedef struct {
    uint16_t k_len;
    uint16_t v_len;
    int8_t data[0];
} line_data_tiny_t;

typedef struct {
    uint16_t tag_size;
    char tag_data[_FQ_MAX_TAG_LEN];
    struct {
        int size_mode;
        size_t buf_size;
        size_t next_read_pos;
        size_t next_write_pos;
        size_t loop_write_pos;
    } infos;
    int8_t buf[0];
} fq_data_t;

static int _do_pushback_large(void *data_p, const int64_t obj_id,
    const void *key_p, size_t key_size, const void *value_p, size_t value_size);
static int _do_popfront_large(void *data_p, int64_t &obj_id,
    std::string &key, std::string &value, bool &value_is_null);

static int _do_pushback_middle(void *data_p, const int64_t obj_id,
    const void *key_p, size_t key_size, const void *value_p, size_t value_size);
static int _do_popfront_middle(void *data_p, int64_t &obj_id,
    std::string &key, std::string &value, bool &value_is_null);

static int _do_pushback_small(void *data_p, const int64_t obj_id,
    const void *key_p, size_t key_size, const void *value_p, size_t value_size);
static int _do_popfront_small(void *data_p, int64_t &obj_id,
    std::string &key, std::string &value, bool &value_is_null);

static int _do_pushback_tiny(void *data_p, const int64_t obj_id,
    const void *key_p, size_t key_size, const void *value_p, size_t value_size);
static int _do_popfront_tiny(void *data_p, int64_t &obj_id,
    std::string &key, std::string &value, bool &value_is_null);

///////////////
// large --> //
///////////////

static int
_do_pushback_large(void *data_p, const int64_t obj_id,
    const void *key_p, size_t key_size, const void *value_p, size_t value_size)
{
    if ((!data_p) || (!key_p))
        return EINVAL;

    if (!value_p)
        value_size = 0;

    size_t line_size = CR_ALIGN(sizeof(line_data_large_t) + key_size + value_size);

    if (line_size > FQ_DATA->infos.buf_size)
        return EMSGSIZE;

    size_t next_write_pos = FQ_DATA->infos.next_write_pos;
    bool do_loop_write = false;

    if (FQ_DATA->infos.loop_write_pos > 0) {
        if ((next_write_pos + line_size) > FQ_DATA->infos.next_read_pos)
            return EAGAIN;
    } else {
        if ((next_write_pos + line_size) > FQ_DATA->infos.buf_size) {
            if (line_size > FQ_DATA->infos.next_read_pos)
                return EAGAIN;
            do_loop_write = true;
            next_write_pos = 0;
        }
    }

    line_data_large_t *line_data_p = (line_data_large_t *)(&(FQ_DATA->buf[next_write_pos]));

    line_data_p->obj_id = obj_id;

    line_data_p->k_len = key_size;
    memcpy(line_data_p->data, key_p, key_size);

    if (value_p) {
        line_data_p->v_len = value_size;
        memcpy(&(line_data_p->data[key_size]), value_p, value_size);
    } else {
        line_data_p->v_len = (size_t)-1ULL;
    }

    size_t new_next_write_pos = next_write_pos + line_size;

    if (do_loop_write)
        FQ_DATA->infos.loop_write_pos = FQ_DATA->infos.next_write_pos;

    FQ_DATA->infos.next_write_pos = new_next_write_pos;

    return 0;
}

static int
_do_popfront_large(void *data_p, int64_t &obj_id,
    std::string &key, std::string &value, bool &value_is_null)
{
    if (!data_p)
        return EINVAL;

    int64_t obj_id_local;
    const char *key_p_local;
    size_t key_size_local;
    const char *value_p_local;
    size_t value_size_local;
    size_t next_read_pos = FQ_DATA->infos.next_read_pos;
    bool reset_loop_write = false;

    if (FQ_DATA->infos.loop_write_pos == 0) {
        if (next_read_pos >= FQ_DATA->infos.next_write_pos)
            return EAGAIN;
    } else {
        if (next_read_pos >= FQ_DATA->infos.loop_write_pos) {
            reset_loop_write = true;
            next_read_pos = 0;
        }
    }

    line_data_large_t *line_data_p = (line_data_large_t *)(&(FQ_DATA->buf[next_read_pos]));

    obj_id_local = line_data_p->obj_id;

    key_p_local = (const char *)line_data_p->data;
    key_size_local = line_data_p->k_len;

    if (line_data_p->v_len == (size_t)-1ULL) {
        value_p_local = NULL;
        value_size_local = 0;
    } else {
        value_p_local = (const char *)(&(line_data_p->data[key_size_local]));
        value_size_local = line_data_p->v_len;
    }

    size_t line_size = CR_ALIGN(sizeof(line_data_large_t) + key_size_local + value_size_local);

    if (reset_loop_write)
        FQ_DATA->infos.loop_write_pos = 0;

    FQ_DATA->infos.next_read_pos = next_read_pos + line_size;

    obj_id = obj_id_local;
    key.assign(key_p_local, key_size_local);
    if (value_p_local) {
        value.assign(value_p_local, value_size_local);
        value_is_null = false;
    } else
        value_is_null = true;

    return 0;
}

///////////////
// <-- large //
///////////////

////////////////
// middle --> //
////////////////

static int
_do_pushback_middle(void *data_p, const int64_t obj_id,
    const void *key_p, size_t key_size, const void *value_p, size_t value_size)
{
    if ((!data_p) || (!key_p))
        return EINVAL;

    if (!value_p)
        value_size = 0;

    size_t line_size = CR_ALIGN(sizeof(line_data_middle_t) + key_size + value_size);

    if (line_size > FQ_DATA->infos.buf_size)
        return EMSGSIZE;

    size_t next_write_pos = FQ_DATA->infos.next_write_pos;
    bool do_loop_write = false;

    if (FQ_DATA->infos.loop_write_pos > 0) {
        if ((next_write_pos + line_size) > FQ_DATA->infos.next_read_pos)
            return EAGAIN;
    } else {
        if ((next_write_pos + line_size) > FQ_DATA->infos.buf_size) {
            if (line_size > FQ_DATA->infos.next_read_pos)
                return EAGAIN;
            do_loop_write = true;
            next_write_pos = 0;
        }
    }

    line_data_middle_t *line_data_p = (line_data_middle_t *)(&(FQ_DATA->buf[next_write_pos]));

    line_data_p->obj_id = obj_id;

    line_data_p->k_len = key_size;
    memcpy(line_data_p->data, key_p, key_size);

    if (value_p) {
        line_data_p->v_len = value_size;
        memcpy(&(line_data_p->data[key_size]), value_p, value_size);
    } else {
        line_data_p->v_len = (uint32_t)-1ULL;
    }

    size_t new_next_write_pos = next_write_pos + line_size;

    if (do_loop_write)
        FQ_DATA->infos.loop_write_pos = FQ_DATA->infos.next_write_pos;

    FQ_DATA->infos.next_write_pos = new_next_write_pos;

    return 0;
}

static int
_do_popfront_middle(void *data_p, int64_t &obj_id,
    std::string &key, std::string &value, bool &value_is_null)
{
    if (!data_p)
        return EINVAL;

    int64_t obj_id_local;
    const char *key_p_local;
    size_t key_size_local;
    const char *value_p_local;
    size_t value_size_local;
    size_t next_read_pos = FQ_DATA->infos.next_read_pos;
    bool reset_loop_write = false;

    if (FQ_DATA->infos.loop_write_pos == 0) {
        if (next_read_pos >= FQ_DATA->infos.next_write_pos)
            return EAGAIN;
    } else {
        if (next_read_pos >= FQ_DATA->infos.loop_write_pos) {
            reset_loop_write = true;
            next_read_pos = 0;
        }
    }

    line_data_middle_t *line_data_p = (line_data_middle_t *)(&(FQ_DATA->buf[next_read_pos]));

    obj_id_local = line_data_p->obj_id;

    key_p_local = (const char *)line_data_p->data;
    key_size_local = line_data_p->k_len;

    if (line_data_p->v_len == (uint32_t)-1ULL) {
        value_p_local = NULL;
        value_size_local = 0;
    } else {
        value_p_local = (const char *)(&(line_data_p->data[key_size_local]));
        value_size_local = line_data_p->v_len;
    }

    size_t line_size = CR_ALIGN(sizeof(line_data_middle_t) + key_size_local + value_size_local);

    if (reset_loop_write)
        FQ_DATA->infos.loop_write_pos = 0;

    FQ_DATA->infos.next_read_pos = next_read_pos + line_size;

    obj_id = obj_id_local;
    key.assign(key_p_local, key_size_local);
    if (value_p_local) {
        value.assign(value_p_local, value_size_local);
        value_is_null = false;
    } else
        value_is_null = true;

    return 0;
}

////////////////
// <-- middle //
////////////////

///////////////
// small --> //
///////////////

static int
_do_pushback_small(void *data_p, const int64_t obj_id,
    const void *key_p, size_t key_size, const void *value_p, size_t value_size)
{
    if ((!data_p) || (!key_p))
        return EINVAL;

    if (!value_p)
        value_size = 0;

    size_t line_size = CR_ALIGN(sizeof(line_data_small_t) + key_size + value_size);

    if (line_size > FQ_DATA->infos.buf_size)
        return EMSGSIZE;

    size_t next_write_pos = FQ_DATA->infos.next_write_pos;
    bool do_loop_write = false;

    if (FQ_DATA->infos.loop_write_pos > 0) {
        if ((next_write_pos + line_size) > FQ_DATA->infos.next_read_pos)
            return EAGAIN;
    } else {
        if ((next_write_pos + line_size) > FQ_DATA->infos.buf_size) {
            if (line_size > FQ_DATA->infos.next_read_pos)
                return EAGAIN;
            do_loop_write = true;
            next_write_pos = 0;
        }
    }

    line_data_small_t *line_data_p = (line_data_small_t *)(&(FQ_DATA->buf[next_write_pos]));

    line_data_p->obj_id = obj_id;

    line_data_p->k_len = key_size;
    memcpy(line_data_p->data, key_p, key_size);

    if (value_p) {
        line_data_p->v_len = value_size;
        memcpy(&(line_data_p->data[key_size]), value_p, value_size);
    } else {
        line_data_p->v_len = (uint16_t)-1ULL;
    }

    size_t new_next_write_pos = next_write_pos + line_size;

    if (do_loop_write)
        FQ_DATA->infos.loop_write_pos = FQ_DATA->infos.next_write_pos;

    FQ_DATA->infos.next_write_pos = new_next_write_pos;

    return 0;
}

static int
_do_popfront_small(void *data_p, int64_t &obj_id,
    std::string &key, std::string &value, bool &value_is_null)
{
    if (!data_p)
        return EINVAL;

    int64_t obj_id_local;
    const char *key_p_local;
    size_t key_size_local;
    const char *value_p_local;
    size_t value_size_local;
    size_t next_read_pos = FQ_DATA->infos.next_read_pos;
    bool reset_loop_write = false;

    if (FQ_DATA->infos.loop_write_pos == 0) {
        if (next_read_pos >= FQ_DATA->infos.next_write_pos)
            return EAGAIN;
    } else {
        if (next_read_pos >= FQ_DATA->infos.loop_write_pos) {
            reset_loop_write = true;
            next_read_pos = 0;
        }
    }

    line_data_small_t *line_data_p = (line_data_small_t *)(&(FQ_DATA->buf[next_read_pos]));

    obj_id_local = line_data_p->obj_id;

    key_p_local = (const char *)line_data_p->data;
    key_size_local = line_data_p->k_len;

    if (line_data_p->v_len == (uint16_t)-1ULL) {
        value_p_local = NULL;
        value_size_local = 0;
    } else {
        value_p_local = (const char *)(&(line_data_p->data[key_size_local]));
        value_size_local = line_data_p->v_len;
    }

    size_t line_size = CR_ALIGN(sizeof(line_data_small_t) + key_size_local + value_size_local);

    if (reset_loop_write)
        FQ_DATA->infos.loop_write_pos = 0;

    FQ_DATA->infos.next_read_pos = next_read_pos + line_size;

    obj_id = obj_id_local;
    key.assign(key_p_local, key_size_local);
    if (value_p_local) {
        value.assign(value_p_local, value_size_local);
        value_is_null = false;
    } else
        value_is_null = true;

    return 0;
}

///////////////
// <-- small //
///////////////

//////////////
// tiny --> //
//////////////

static int
_do_pushback_tiny(void *data_p, const int64_t obj_id,
    const void *key_p, size_t key_size, const void *value_p, size_t value_size)
{
    if ((!data_p) || (!key_p))
        return EINVAL;

    if (!value_p)
        value_size = 0;

    size_t line_size = CR_ALIGN(sizeof(line_data_tiny_t) + key_size + value_size);

    if (line_size > FQ_DATA->infos.buf_size)
        return EMSGSIZE;

    size_t next_write_pos = FQ_DATA->infos.next_write_pos;
    bool do_loop_write = false;

    if (FQ_DATA->infos.loop_write_pos > 0) {
        if ((next_write_pos + line_size) > FQ_DATA->infos.next_read_pos)
            return EAGAIN;
    } else {
        if ((next_write_pos + line_size) > FQ_DATA->infos.buf_size) {
            if (line_size > FQ_DATA->infos.next_read_pos)
                return EAGAIN;
            do_loop_write = true;
            next_write_pos = 0;
        }
    }

    line_data_tiny_t *line_data_p = (line_data_tiny_t *)(&(FQ_DATA->buf[next_write_pos]));

    line_data_p->k_len = key_size;
    memcpy(line_data_p->data, key_p, key_size);

    if (value_p) {
        line_data_p->v_len = value_size;
        memcpy(&(line_data_p->data[key_size]), value_p, value_size);
    } else {
        line_data_p->v_len = (uint16_t)-1ULL;
    }

    size_t new_next_write_pos = next_write_pos + line_size;

    if (do_loop_write)
        FQ_DATA->infos.loop_write_pos = FQ_DATA->infos.next_write_pos;

    FQ_DATA->infos.next_write_pos = new_next_write_pos;

    return 0;
}

static int
_do_popfront_tiny(void *data_p, int64_t &obj_id,
    std::string &key, std::string &value, bool &value_is_null)
{
    if (!data_p)
        return EINVAL;

    const char *key_p_local;
    size_t key_size_local;
    const char *value_p_local;
    size_t value_size_local;
    size_t next_read_pos = FQ_DATA->infos.next_read_pos;
    bool reset_loop_write = false;

    if (FQ_DATA->infos.loop_write_pos == 0) {
        if (next_read_pos >= FQ_DATA->infos.next_write_pos)
            return EAGAIN;
    } else {
        if (next_read_pos >= FQ_DATA->infos.loop_write_pos) {
            reset_loop_write = true;
            next_read_pos = 0;
        }
    }

    line_data_tiny_t *line_data_p = (line_data_tiny_t *)(&(FQ_DATA->buf[next_read_pos]));

    key_p_local = (const char *)line_data_p->data;
    key_size_local = line_data_p->k_len;

    if (line_data_p->v_len == (uint16_t)-1ULL) {
        value_p_local = NULL;
        value_size_local = 0;
    } else {
        value_p_local = (const char *)(&(line_data_p->data[key_size_local]));
        value_size_local = line_data_p->v_len;
    }

    size_t line_size = CR_ALIGN(sizeof(line_data_tiny_t) + key_size_local + value_size_local);

    if (reset_loop_write)
        FQ_DATA->infos.loop_write_pos = 0;

    FQ_DATA->infos.next_read_pos = next_read_pos + line_size;

    obj_id = 0;
    key.assign(key_p_local, key_size_local);
    if (value_p_local) {
        value.assign(value_p_local, value_size_local);
        value_is_null = false;
    } else
        value_is_null = true;

    return 0;
}

//////////////
// <-- tiny //
//////////////

CR_FixedQueue::CR_FixedQueue(const size_t max_data_size, const int size_mode)
{
    this->_private_data = NULL;
    this->ReSize(max_data_size, size_mode);
}

CR_FixedQueue::~CR_FixedQueue()
{
    ::free(this->_private_data);
}

void
CR_FixedQueue::ReSize(size_t max_data_size, const int size_mode)
{
    void *old_data_p;
    void *new_data_p;
    size_t new_private_size;

    switch (size_mode) {
    case CR_FQ_SIZEMODE_SMALL:
    case CR_FQ_SIZEMODE_TINY :
        if (max_data_size > 0xFFFFF000UL)
            max_data_size = 0xFFFFF000UL;
        break;
    }

    if (max_data_size > 0) {
        DPRINTFX(20, "max_data_size[0x%016llX], size_mode[%d]\n",
          (long long unsigned)max_data_size, size_mode);
    }

    max_data_size = CR_ALIGN(max_data_size);
    new_private_size = sizeof(fq_data_t) + max_data_size;

    CANCEL_DISABLE_ENTER();

    new_data_p = ::calloc(1, new_private_size);

    this->_cmtx.lock();

    old_data_p = this->_private_data;

    this->_private_data = new_data_p;
    this->_private_size = new_private_size;

    THIS_FQ_DATA->infos.buf_size = max_data_size;

    this->do_set_sizemode(size_mode);
    this->do_clear();

    this->_cmtx.unlock();

    if (old_data_p)
        ::free(old_data_p);

    CANCEL_DISABLE_LEAVE();
}

int
CR_FixedQueue::do_get_sizemode()
{
    if (this->_do_pushback == _do_pushback_middle)
        return CR_FQ_SIZEMODE_MIDDLE;
    else if (this->_do_pushback == _do_pushback_small)
        return CR_FQ_SIZEMODE_SMALL;
    else if (this->_do_pushback == _do_pushback_tiny)
        return CR_FQ_SIZEMODE_TINY;
    else
        return CR_FQ_SIZEMODE_LARGE;
}

int
CR_FixedQueue::do_set_sizemode(const int size_mode)
{
    int ret_size_mode = CR_FQ_SIZEMODE_LARGE;

    switch (size_mode) {
    case CR_FQ_SIZEMODE_MIDDLE :
        this->_do_pushback = _do_pushback_middle;
        this->_do_popfront = _do_popfront_middle;
        ret_size_mode = CR_FQ_SIZEMODE_MIDDLE;
        break;
    case CR_FQ_SIZEMODE_SMALL :
        this->_do_pushback = _do_pushback_small;
        this->_do_popfront = _do_popfront_small;
        ret_size_mode = CR_FQ_SIZEMODE_SMALL;
        break;
    case CR_FQ_SIZEMODE_TINY :
        this->_do_pushback = _do_pushback_tiny;
        this->_do_popfront = _do_popfront_tiny;
        ret_size_mode = CR_FQ_SIZEMODE_TINY;
        break;
    default :
        this->_do_pushback = _do_pushback_large;
        this->_do_popfront = _do_popfront_large;
        break;
    }

    return ret_size_mode;
}

void
CR_FixedQueue::do_clear()
{
    THIS_FQ_DATA->tag_size = 0;
    THIS_FQ_DATA->infos.next_read_pos = 0;
    THIS_FQ_DATA->infos.next_write_pos = 0;
    THIS_FQ_DATA->infos.loop_write_pos = 0;
}

int
CR_FixedQueue::timed_pushback(const int64_t obj_id, const void *key_p, const size_t key_size,
    const void *value_p, const size_t value_size, const double timeout_sec)
{
    int ret = 0;
    double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;
    double wait_gap = (timeout_sec <= 0.1) ? timeout_sec : 0.1;

    CANCEL_DISABLE_ENTER();
    this->_cmtx.lock();

    if (timeout_sec == 0.0) {
        ret = this->_do_pushback(this->_private_data, obj_id, key_p, key_size, value_p, value_size);
    } else {
        do {
            ret = this->_do_pushback(this->_private_data, obj_id, key_p, key_size, value_p, value_size);
            if (ret == EAGAIN) {
                this->_cmtx.wait(wait_gap);
            } else
                break;
        } while (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) < exp_time);
    }

    if (!ret)
        this->_cmtx.broadcast();

    this->_cmtx.unlock();
    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_FixedQueue::timed_popfront(int64_t &obj_id, std::string &key, std::string &value,
    bool &value_is_null, const double timeout_sec)
{
    int ret = 0;
    double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;
    double wait_gap = (timeout_sec <= 0.1) ? timeout_sec : 0.1;

    CANCEL_DISABLE_ENTER();
    this->_cmtx.lock();

    if (timeout_sec == 0.0) {
        ret = this->_do_popfront(this->_private_data, obj_id, key, value, value_is_null);
    } else {
        do {
            ret = this->_do_popfront(this->_private_data, obj_id, key, value, value_is_null);
            if (ret == EAGAIN) {
                this->_cmtx.wait(wait_gap);
            } else
                break;
        } while (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) < exp_time);
    }

    if (!ret)
        this->_cmtx.broadcast();

    this->_cmtx.unlock();
    CANCEL_DISABLE_LEAVE();

    return ret;
}

void
CR_FixedQueue::Clear()
{
    CANCEL_DISABLE_ENTER();
    this->_cmtx.lock();

    this->do_clear();

    this->_cmtx.unlock();
    CANCEL_DISABLE_LEAVE();
}

int
CR_FixedQueue::PushBack(const int64_t obj_id, const void *key_p, const size_t key_size,
    const void *value_p, const size_t value_size, double timeout_sec)
{
    int ret;

    ret = this->timed_pushback(obj_id, key_p, key_size, value_p, value_size, timeout_sec);

    return ret;
}

int
CR_FixedQueue::PushBack(const int64_t obj_id, const std::string &key,
    const std::string *value_p, double timeout_sec)
{
    int ret;

    const char *value_data_p;
    size_t value_size;

    if (value_p) {
        value_data_p = value_p->data();
        value_size = value_p->size();
    } else {
        value_data_p = NULL;
        value_size = 0;
    }

    ret = this->timed_pushback(obj_id, key.data(), key.size(), value_data_p, value_size, timeout_sec);

    return ret;
}

int
CR_FixedQueue::PopFront(int64_t &obj_id,
    std::string &key, std::string &value, bool &value_is_null, double timeout_sec)
{
    int ret;

    ret = this->timed_popfront(obj_id, key, value, value_is_null, timeout_sec);

    return ret;
}

size_t
CR_FixedQueue::GetHeadSize()
{
    return CR_ROUND_TO_POW_OF_2(sizeof(fq_data_t));
}

int
CR_FixedQueue::GetSizeMode()
{
    int ret;

    CANCEL_DISABLE_ENTER();
    this->_cmtx.lock();

    ret = this->do_get_sizemode();

    this->_cmtx.unlock();
    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_FixedQueue::SetTag(const std::string &s)
{
    size_t s_size = s.size();

    if (s_size > sizeof(THIS_FQ_DATA->tag_data))
        return EMSGSIZE;

    CANCEL_DISABLE_ENTER();
    this->_cmtx.lock();

    memcpy(THIS_FQ_DATA->tag_data, s.data(), s_size);
    THIS_FQ_DATA->tag_size = s_size;

    this->_cmtx.unlock();
    CANCEL_DISABLE_LEAVE();

    return 0;
}

std::string
CR_FixedQueue::GetTag()
{
    std::string ret;

    CANCEL_DISABLE_ENTER();
    this->_cmtx.lock();

    if (THIS_FQ_DATA->tag_size <= sizeof(THIS_FQ_DATA->tag_data))
        ret.assign(THIS_FQ_DATA->tag_data, THIS_FQ_DATA->tag_size);

    this->_cmtx.unlock();
    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_FixedQueue::SetLocalTag(const std::string &s)
{
    CANCEL_DISABLE_ENTER();
    this->_cmtx.lock();

    this->_local_tag = s;

    this->_cmtx.unlock();
    CANCEL_DISABLE_LEAVE();

    return 0;
}

std::string
CR_FixedQueue::GetLocalTag()
{
    std::string ret;

    CANCEL_DISABLE_ENTER();
    this->_cmtx.lock();

    ret = this->_local_tag;

    this->_cmtx.unlock();
    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_FixedQueue::SaveToFileDescriptor(int fd, bool do_compress)
{
    int ret = 0;

    CANCEL_DISABLE_ENTER();

    THIS_FQ_DATA->infos.size_mode = this->do_get_sizemode();

    if (do_compress) {
        void *snappy_data_p;
        uint64_t snappy_data_size;

        this->_cmtx.lock();

        snappy_data_p = CR_Compress::compress(this->_private_data,
          this->_private_size, NULL, &snappy_data_size, CR_Compress::CT_SNAPPY);

        this->_cmtx.unlock();

        if (snappy_data_p) {
            ret = CR_Class_NS::fput_block(fd, CR_MSG_QUEUESTORAGE_SNAPPY,
              snappy_data_p, snappy_data_size, CR_BLOCKOP_FLAG_ERRCHECK);

            ::free(snappy_data_p);
        } else
            ret = errno;
    } else {
        this->_cmtx.lock();

        ret = CR_Class_NS::fput_block(fd, CR_MSG_QUEUESTORAGE, this->_private_data,
          this->_private_size, CR_BLOCKOP_FLAG_ERRCHECK);

        this->_cmtx.unlock();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_FixedQueue::SaveToFile(const std::string &filename, bool do_compress)
{
    int ret = 0;
    int saveto_fd;

    CANCEL_DISABLE_ENTER();

    saveto_fd = CR_File::open(filename.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0600);

    if (saveto_fd < 0) {
        return errno;
    }

    ret = this->SaveToFileDescriptor(saveto_fd, do_compress);

    CR_File::close(saveto_fd);

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_FixedQueue::LoadFromFileDescriptor(int fd)
{
    int8_t msg_code;
    void *fgot_data;
    uint32_t fgot_size;
    void *old_private_data;

    fgot_data = CR_Class_NS::fget_block(fd, msg_code, NULL, fgot_size);

    if (!fgot_data) {
        if (errno)
            return errno;
        else
            return EFAULT;
    }

    CANCEL_DISABLE_ENTER();

    switch (msg_code) {
    case CR_MSG_QUEUESTORAGE :
        this->_cmtx.lock();

        old_private_data = this->_private_data;
        this->_private_data = fgot_data;
        this->_private_size = fgot_size;
        this->do_set_sizemode(THIS_FQ_DATA->infos.size_mode);

        this->_cmtx.unlock();
        break;
    case CR_MSG_QUEUESTORAGE_SNAPPY :
        {
            void *decompress_data;
            uint64_t decompress_size;

            decompress_data = CR_Compress::decompress(fgot_data, fgot_size, NULL, &decompress_size);

            if (!decompress_data) {
                ::free(fgot_data);
                return errno;
            }

            this->_cmtx.lock();

            old_private_data = this->_private_data;
            this->_private_data = decompress_data;
            this->_private_size = decompress_size;
            this->do_set_sizemode(THIS_FQ_DATA->infos.size_mode);

            this->_cmtx.unlock();

            ::free(fgot_data);
        }
        break;
    default :
        return EPROTONOSUPPORT;
    }

    if (old_private_data)
        ::free(old_private_data);

    CANCEL_DISABLE_LEAVE();

    return 0;
}

int
CR_FixedQueue::LoadFromFile(const std::string &filename)
{
    int ret = 0;
    int loadfrom_fd;

    CANCEL_DISABLE_ENTER();

    loadfrom_fd = CR_File::open(filename.c_str(), O_RDONLY);

    if (loadfrom_fd < 0) {
        return errno;
    }

    ret = this->LoadFromFileDescriptor(loadfrom_fd);

    CR_File::close(loadfrom_fd);

    CANCEL_DISABLE_LEAVE();

    return ret;
}
