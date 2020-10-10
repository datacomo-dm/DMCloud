#include <cr_class/cr_addon.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>

#include <cr_class/cr_fixedlinearstorage.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_file.h>
#include <cr_class/cr_quickhash.h>
#include <cr_class/cr_compress.h>

#define CR_MSG_LINEARSTORAGE            (81)
#define CR_MSG_LINEARSTORAGE_COMPRESS   (83)

#define _FLS_MAX_TAG_LEN		((1UL<<7) - sizeof(uint16_t))

#define THIS_FLS_DATA	((fls_data_t*)(this->_private_data))
#define FLS_DATA	((fls_data_t*)(data_p))

typedef struct {
    int64_t obj_id;
    uint64_t line_pos;
    uint64_t k_len;
    uint64_t v_len;
} line_info_large_t;

typedef struct {
    int64_t obj_id;
    uint64_t line_pos;
    uint32_t k_len;
    uint32_t v_len;
} line_info_middle_t;

typedef struct {
    int64_t obj_id;
    uint32_t line_pos;
    uint16_t k_len;
    uint16_t v_len;
} line_info_small_t;

typedef struct {
    uint16_t tag_size;
    char tag_data[_FLS_MAX_TAG_LEN];
    CR_FixedLinearStorage_NS::fls_infos_t infos;
    char lines_data[0];
} fls_data_t;

static int _line_compare_large(const void *l, const void *r, void *arg);
static int _do_set_large(void *data_p, const uint64_t line_no, const int64_t obj_id,
    const void *key_p, const uint64_t key_size, const void *value_p, const uint64_t value_size);
static int _do_query_large(void *data_p, const uint64_t line_no,
    int64_t &obj_id, const char *&key_p, uint64_t &key_size,
    const char *&value_p, uint64_t &value_size, bool *is_last_line_p);
static int _do_sort_large(void *data_p);

static int _line_compare_middle(const void *l, const void *r, void *arg);
static int _do_set_middle(void *data_p, const uint64_t line_no, const int64_t obj_id,
    const void *key_p, const uint64_t key_size, const void *value_p, const uint64_t value_size);
static int _do_query_middle(void *data_p, const uint64_t line_no,
    int64_t &obj_id, const char *&key_p, uint64_t &key_size,
    const char *&value_p, uint64_t &value_size, bool *is_last_line_p);
static int _do_sort_middle(void *data_p);

static int _line_compare_small(const void *l, const void *r, void *arg);
static int _do_set_small(void *data_p, const uint64_t line_no, const int64_t obj_id,
    const void *key_p, const uint64_t key_size, const void *value_p, const uint64_t value_size);
static int _do_query_small(void *data_p, const uint64_t line_no,
    int64_t &obj_id, const char *&key_p, uint64_t &key_size,
    const char *&value_p, uint64_t &value_size, bool *is_last_line_p);
static int _do_sort_small(void *data_p);

///////////////
// templates //
///////////////

template<typename line_info_t>
int
_line_compare(const void *l, const void *r, void *arg)
{
    const line_info_t *line_l = (const line_info_t*)l;
    const line_info_t *line_r = (const line_info_t*)r;
    const char *data_p = (const char*)arg;
#if defined(DIAGNOSTIC)
    if (!line_l)
        THROWF("line_l == NULL\n");
    if (!line_r)
        THROWF("line_r == NULL\n");
    if (!data_p)
        THROWF("data_p == NULL\n");
#endif // DIAGNOSTIC
    int key_cmp_ret = CR_Class_NS::memlcmp(data_p + line_l->line_pos, line_l->k_len,
      data_p + line_r->line_pos, line_r->k_len);
    if (key_cmp_ret == 0) {
        const line_info_t *line_l = (const line_info_t*)l;
        const line_info_t *line_r = (const line_info_t*)r;
        if (line_l->obj_id > line_r->obj_id) {
            return 1;
        } else if (line_l->obj_id < line_r->obj_id) {
            return -1;
        } else {
            return 0;
        }
    } else {
        return key_cmp_ret;
    }
}

template<typename line_info_t>
int
_do_set(void *data_p, const uint64_t line_no,
    const int64_t obj_id, const void *key_p, const uint64_t key_size,
    const void *value_p, const uint64_t value_size)
{
    if ((!key_p) || (!data_p))
        return EINVAL;

    bool new_line = false;
    uint64_t kv_size;
    uint64_t objid_cksum = 0;
    uint64_t key_cksum = 0;
    uint64_t value_cksum = 0;
    char *clear_p = NULL;
    size_t clear_size = 0;
    int64_t obj_id_old;
    char *key_p_old = NULL;
    uint64_t key_size_old = 0;
    char *value_p_old = NULL;
    uint64_t value_size_old = 0;
    uint64_t _next_line_data_pos_after = 0;
    uint64_t total_lines_after = FLS_DATA->infos.total_lines;
    line_info_t *line_info_p = &(((line_info_t*)(FLS_DATA->lines_data))[line_no]);

    if (value_p)
        kv_size = key_size + value_size;
    else
        kv_size = key_size;

    if (line_no >= total_lines_after) {
        total_lines_after = line_no + 1;
        new_line = true;
    } else {
        obj_id_old = line_info_p->obj_id;
        key_p_old = &(FLS_DATA->lines_data[line_info_p->line_pos]);
        key_size_old = line_info_p->k_len;
        if (line_info_p->v_len != CR_Class_NS::uint_max(sizeof(line_info_p->v_len))) {
            value_p_old = key_p_old + line_info_p->k_len;
            value_size_old = line_info_p->v_len;
        }
        uint64_t kv_size_old = key_size_old + value_size_old;
        if (kv_size_old < kv_size) {
            new_line = true;
            clear_p = key_p_old;
            clear_size = kv_size_old;
        } else {
            clear_p = key_p_old + kv_size;
            clear_size = kv_size_old - kv_size;
        }
    }

    if (new_line) {
        if (FLS_DATA->infos.next_line_data_pos > kv_size)
            _next_line_data_pos_after = FLS_DATA->infos.next_line_data_pos - kv_size;
        if ((sizeof(line_info_t) * total_lines_after) > _next_line_data_pos_after)
            return ENOBUFS;
    }

    if (key_p_old) {
        objid_cksum -= CR_QuickHash::CRC32Hash(&obj_id_old, sizeof(obj_id_old));
        key_cksum -= CR_QuickHash::CRC32Hash(key_p_old, key_size_old);
        if (value_p_old)
            value_cksum -= CR_QuickHash::CRC32Hash(value_p_old, value_size_old);
    }

    objid_cksum += CR_QuickHash::CRC32Hash(&obj_id, sizeof(obj_id));
    key_cksum += CR_QuickHash::CRC32Hash(key_p, key_size);
    if (value_p)
        value_cksum += CR_QuickHash::CRC32Hash(value_p, value_size);

    line_info_p->obj_id = obj_id;
    FLS_DATA->infos.min_obj_id = CR_Class_NS::min(FLS_DATA->infos.min_obj_id, obj_id);
    FLS_DATA->infos.max_obj_id = CR_Class_NS::max(FLS_DATA->infos.max_obj_id, obj_id);
    line_info_p->k_len = key_size;
    if (value_p) {
        line_info_p->v_len = value_size;
    } else {
        line_info_p->v_len = CR_Class_NS::uint_max(sizeof(line_info_p->v_len));
    }
    if (new_line) {
        if (value_p) {
            uint64_t tmp_value_pos = FLS_DATA->infos.next_line_data_pos - value_size;
            line_info_p->line_pos = tmp_value_pos - key_size;
            memcpy(&(FLS_DATA->lines_data[line_info_p->line_pos]), key_p, key_size);
            memcpy(&(FLS_DATA->lines_data[tmp_value_pos]), value_p, value_size);
        } else {
            line_info_p->line_pos = FLS_DATA->infos.next_line_data_pos - key_size;
            memcpy(&(FLS_DATA->lines_data[line_info_p->line_pos]), key_p, key_size);
        }
        FLS_DATA->infos.next_line_data_pos = _next_line_data_pos_after;
        uint64_t line_info_arr_size = sizeof(line_info_t) * (total_lines_after + 1);
        if (_next_line_data_pos_after > line_info_arr_size) {
            FLS_DATA->infos.free_space_left = _next_line_data_pos_after - line_info_arr_size;
        } else
            FLS_DATA->infos.free_space_left = 0;
    } else { // if (new_line)
        memcpy(key_p_old, key_p, key_size);
        if (value_p)
            memcpy(key_p_old + key_size, value_p, value_size);
    }

    FLS_DATA->infos.longest_line_size = CR_Class_NS::max(FLS_DATA->infos.longest_line_size, kv_size);
    FLS_DATA->infos.longest_key_size = CR_Class_NS::max(FLS_DATA->infos.longest_key_size, key_size);
    if (value_p)
        FLS_DATA->infos.longest_value_size = CR_Class_NS::max(FLS_DATA->infos.longest_value_size, value_size);
    FLS_DATA->infos.objid_cksum += objid_cksum;
    FLS_DATA->infos.key_cksum += key_cksum;
    FLS_DATA->infos.value_cksum += value_cksum;
    FLS_DATA->infos.total_lines = total_lines_after;

    if (clear_p)
        bzero(clear_p, clear_size);

    return 0;
}

template<typename line_info_t>
int
_do_query(void *data_p, const uint64_t line_no,
    int64_t &obj_id, const char *&key_p, uint64_t &key_size,
    const char *&value_p, uint64_t &value_size, bool *is_last_line_p)
{
    uint64_t total_lines_tmp = FLS_DATA->infos.total_lines;

    if (line_no >= total_lines_tmp)
        return ERANGE;

    if (is_last_line_p) {
        if ((line_no + 1) == total_lines_tmp)
            *is_last_line_p = true;
        else
            *is_last_line_p = false;
    }

    const line_info_t *line_info_p = &(((const line_info_t*)(FLS_DATA->lines_data))[line_no]);
    if ((FLS_DATA->infos.flags & CR_FLS_FLAG_QUERYMODE_IGNORE_OBJID) == 0) {
        obj_id = line_info_p->obj_id;
    }
    key_p = &(FLS_DATA->lines_data[line_info_p->line_pos]);
    if ((FLS_DATA->infos.flags & CR_FLS_FLAG_QUERYMODE_IGNORE_KEY) == 0) {
        key_size = line_info_p->k_len;
    } else {
        key_size = 0;
    }
    if (line_info_p->v_len != CR_Class_NS::uint_max(sizeof(line_info_p->v_len))) {
        value_p = key_p + line_info_p->k_len;
        if ((FLS_DATA->infos.flags & CR_FLS_FLAG_QUERYMODE_IGNORE_VALUE) == 0) {
            value_size = line_info_p->v_len;
        } else {
            value_size = 0;
        }
    } else {
        value_p = NULL;
        value_size = 0;
    }

    return 0;
}

template<typename line_info_t>
bool
_verify_fls(const void *buf, const uint64_t buf_size,
    CR_FixedLinearStorage_NS::fls_infos_t *fls_infos_p)
{
    if (!buf) {
        DPRINTFX(20, "!buf\n");
        return false;
    }
    if (buf_size < sizeof(fls_data_t)) {
        DPRINTFX(20, "buf_size < sizeof(fls_data_t)\n");
        return false;
    }
    const fls_data_t *fls_data_p = (const fls_data_t*)buf;
    if (fls_data_p->tag_size > sizeof(fls_data_p->tag_data)) {
        DPRINTFX(20, "tag_size > sizeof(tag_data)\n");
        return false;
    }
    uint64_t data_size = buf_size - sizeof(fls_data_t);
    uint64_t info_size;
    const char *str_p;
    uint64_t str_size;
    const char *key_p;
    uint64_t key_len;
    const char *value_p;
    uint64_t value_len;
    uint64_t line_len;
    int64_t min_obj_id = INT64_MAX;
    int64_t max_obj_id = INT64_MIN;
    uint64_t total_lines = fls_data_p->infos.total_lines;
    uint64_t objid_cksum = 0;
    uint64_t key_cksum = 0;
    uint64_t value_cksum = 0;

    if ((data_size / sizeof(line_info_t)) < total_lines) {
        DPRINTFX(20, "data_size < line_infos_size\n");
        return false;
    }
    info_size = sizeof(line_info_t) * total_lines;
    str_p = &(fls_data_p->lines_data[info_size]);
    str_size = data_size - info_size;
    if (!CR_BOUNDARY_CHECK(str_p, str_size, buf, buf_size)) {
        DPRINTFX(20, "str_p boundary check failed\n");
        return false;
    }
    for (uint64_t i=0; i< total_lines; i++) {
        const line_info_t *cur_line_info = &(((const line_info_t*)(fls_data_p->lines_data))[i]);
        min_obj_id = CR_Class_NS::min(min_obj_id, cur_line_info->obj_id);
        max_obj_id = CR_Class_NS::max(max_obj_id, cur_line_info->obj_id);
        objid_cksum += CR_QuickHash::CRC32Hash(&(cur_line_info->obj_id), sizeof(cur_line_info->obj_id));
        key_len = cur_line_info->k_len;
        if (key_len > fls_data_p->infos.longest_key_size) {
            DPRINTFX(20, "key_len > longest_key_size at line %llu\n", (long long unsigned)i);
            return false;
        }
        key_p = &(fls_data_p->lines_data[cur_line_info->line_pos]);
        if (!CR_BOUNDARY_CHECK(key_p, key_len, str_p, str_size)) {
            DPRINTFX(20, "key_p boundary check failed at line %llu\n", (long long unsigned)i);
            return false;
        }
        key_cksum += CR_QuickHash::CRC32Hash(key_p, key_len);
        if (cur_line_info->v_len != CR_Class_NS::uint_max(sizeof(cur_line_info->v_len))) {
            value_len = cur_line_info->v_len;
            if (value_len > fls_data_p->infos.longest_value_size) {
                DPRINTFX(20, "value_len(%llu) > longest_value_size(%llu) at line %llu\n",
                  (long long unsigned)value_len, (long long unsigned)fls_data_p->infos.longest_value_size,
                  (long long unsigned)i);
                return false;
            }
            value_p = key_p + key_len;
            if (!CR_BOUNDARY_CHECK(value_p, value_len, str_p, str_size)) {
                DPRINTFX(20, "value_p boundary check failed at line %llu\n", (long long unsigned)i);
                return false;
            }
            value_cksum += CR_QuickHash::CRC32Hash(value_p, value_len);
            line_len = key_len + value_len;
        } else
            line_len = key_len;
        if (line_len > fls_data_p->infos.longest_line_size) {
            DPRINTFX(20, "line_len > longest_line_size at line %llu\n", (long long unsigned)i);
            return false;
        }
    }

    if (min_obj_id != fls_data_p->infos.min_obj_id) {
        DPRINTFX(20, "min_obj_id not EQ\n");
        return false;
    }

    if (max_obj_id != fls_data_p->infos.max_obj_id) {
        DPRINTFX(20, "max_obj_id not EQ\n");
        return false;
    }

    if (objid_cksum != fls_data_p->infos.objid_cksum) {
        DPRINTFX(20, "objid_cksum not EQ\n");
        return false;
    }

    if (key_cksum != fls_data_p->infos.key_cksum) {
        DPRINTFX(20, "key_cksum not EQ\n");
        return false;
    }

    if (value_cksum != fls_data_p->infos.value_cksum) {
        DPRINTFX(20, "value_cksum not EQ\n");
        return false;
    }

    if (fls_infos_p) {
        memcpy(fls_infos_p, &(fls_data_p->infos), sizeof(CR_FixedLinearStorage_NS::fls_infos_t));
        fls_infos_p->next_line_data_pos = 0;
        fls_infos_p->free_space_left = 0;
    }

    return true;
}

///////////
// large //
///////////

static int
_line_compare_large(const void *l, const void *r, void *arg)
{
    return _line_compare<line_info_large_t>(l, r, arg);
}

static int
_do_set_large(void *data_p, const uint64_t line_no, const int64_t obj_id,
    const void *key_p, const uint64_t key_size,
    const void *value_p, const uint64_t value_size)
{
    return _do_set<line_info_large_t>(data_p, line_no, obj_id, key_p, key_size, value_p, value_size);
}

static int
_do_query_large(void *data_p, const uint64_t line_no,
    int64_t &obj_id, const char *&key_p, uint64_t &key_size,
    const char *&value_p, uint64_t &value_size, bool *is_last_line_p)
{
    return _do_query<line_info_large_t>(data_p, line_no, obj_id, key_p, key_size,
                                        value_p, value_size, is_last_line_p);
}

static int
_do_sort_large(void *data_p)
{
    CR_Class_NS::heapsort_r(FLS_DATA->lines_data, FLS_DATA->infos.total_lines,
      sizeof(line_info_large_t), _line_compare_large, FLS_DATA->lines_data);

    return 0;
}

////////////
// middle //
////////////

static int
_line_compare_middle(const void *l, const void *r, void *arg)
{
    return _line_compare<line_info_middle_t>(l, r, arg);
}

static int
_do_set_middle(void *data_p, const uint64_t line_no, const int64_t obj_id,
    const void *key_p, const uint64_t key_size,
    const void *value_p, const uint64_t value_size)
{
    if (key_size > 0xFFFFFFF0UL)
        return EMSGSIZE;

    if (value_p && (value_size > 0xFFFFFFF0UL))
        return EMSGSIZE;

    return _do_set<line_info_middle_t>(data_p, line_no, obj_id, key_p, key_size, value_p, value_size);
}

static int
_do_query_middle(void *data_p, const uint64_t line_no,
    int64_t &obj_id, const char *&key_p, uint64_t &key_size,
    const char *&value_p, uint64_t &value_size, bool *is_last_line_p)
{
    return _do_query<line_info_middle_t>(data_p, line_no, obj_id, key_p, key_size,
                                        value_p, value_size, is_last_line_p);
}

static int
_do_sort_middle(void *data_p)
{
    CR_Class_NS::heapsort_r(FLS_DATA->lines_data, FLS_DATA->infos.total_lines,
      sizeof(line_info_middle_t), _line_compare_middle, FLS_DATA->lines_data);

    return 0;
}

///////////
// small //
///////////

static int
_line_compare_small(const void *l, const void *r, void *arg)
{
    return _line_compare<line_info_small_t>(l, r, arg);
}

static int
_do_set_small(void *data_p, const uint64_t line_no, const int64_t obj_id,
    const void *key_p, const uint64_t key_size,
    const void *value_p, const uint64_t value_size)
{
    if (key_size > 0xFFF0U)
        return EMSGSIZE;

    if (value_p && (value_size > 0xFFF0U))
        return EMSGSIZE;

    return _do_set<line_info_small_t>(data_p, line_no, obj_id, key_p, key_size, value_p, value_size);
}

static int
_do_query_small(void *data_p, const uint64_t line_no,
    int64_t &obj_id, const char *&key_p, uint64_t &key_size,
    const char *&value_p, uint64_t &value_size, bool *is_last_line_p)
{
    return _do_query<line_info_small_t>(data_p, line_no, obj_id, key_p, key_size,
                                        value_p, value_size, is_last_line_p);
}

static int
_do_sort_small(void *data_p)
{
    CR_Class_NS::heapsort_r(FLS_DATA->lines_data, FLS_DATA->infos.total_lines,
      sizeof(line_info_small_t), _line_compare_small, FLS_DATA->lines_data);

    return 0;
}

//////////////

bool
CR_FixedLinearStorage_NS::VerifyData(const void *buf, const uint64_t buf_size, fls_infos_t *fls_infos_p)
{
    if (!buf) {
        DPRINTFX(20, "!buf\n");
        return false;
    }
    const fls_data_t *fls_data_p = (const fls_data_t*)buf;
    switch (fls_data_p->infos.size_mode) {
    case CR_FLS_SIZEMODE_LARGE :
        return _verify_fls<line_info_large_t>(buf, buf_size, fls_infos_p);
    case CR_FLS_SIZEMODE_MIDDLE :
        return _verify_fls<line_info_middle_t>(buf, buf_size, fls_infos_p);
    case CR_FLS_SIZEMODE_SMALL :
        return _verify_fls<line_info_small_t>(buf, buf_size, fls_infos_p);
    default :
        DPRINTFX(20, "size_mode 0x%08X not support\n", fls_data_p->infos.size_mode);
        return false;
    }
}

uint32_t
CR_FixedLinearStorage_NS::CalcSizeMode(const uint64_t longest_line_size, const uint64_t longest_key_size,
    const uint64_t longest_value_size, const uint64_t max_lines)
{
    uint32_t sub_size_mode = CR_FLS_SIZEMODE_LARGE;

    if ((longest_key_size <= UINT64_C(0xFFFFFFF0)) && (longest_value_size <= UINT64_C(0xFFFFFFF0))) {
        sub_size_mode = CR_FLS_SIZEMODE_MIDDLE;
    }

    if ((longest_key_size <= UINT64_C(0xFFF0)) && (longest_value_size <= UINT64_C(0xFFF0))
      && ((longest_line_size * (uint64_t)max_lines) <= UINT64_C(0xFFFFFFF0))) {
        sub_size_mode = CR_FLS_SIZEMODE_SMALL;
    }

    return sub_size_mode;
}

void
CR_FixedLinearStorage_NS::MergeInfo(fls_infos_t &dst, const fls_infos_t &src)
{
    dst.size_mode = CR_Class_NS::max(dst.size_mode, src.size_mode);
    dst.min_obj_id = CR_Class_NS::min(dst.min_obj_id, src.min_obj_id);
    dst.max_obj_id = CR_Class_NS::max(dst.max_obj_id, src.max_obj_id);
    dst.total_lines += src.total_lines;
    dst.longest_key_size = CR_Class_NS::max(dst.longest_key_size, src.longest_key_size);
    dst.longest_value_size = CR_Class_NS::max(dst.longest_value_size, src.longest_value_size);
    dst.longest_line_size = CR_Class_NS::max(dst.longest_line_size, src.longest_line_size);
    dst.free_space_left += src.free_space_left;
    dst.objid_cksum += src.objid_cksum;
    dst.key_cksum += src.key_cksum;
    dst.value_cksum += src.value_cksum;
}

void
CR_FixedLinearStorage_NS::ClearInfos(fls_infos_t &infos)
{
    infos.size_mode = 0;
    infos.min_obj_id = INT64_MAX;
    infos.max_obj_id = INT64_MIN;
    infos.total_lines = 0;
    infos.longest_key_size = 0;
    infos.longest_value_size = 0;
    infos.longest_line_size = 0;
    infos.objid_cksum = 0;
    infos.key_cksum = 0;
    infos.value_cksum = 0;
    infos.flags = 0;
}

//////////////

CR_FixedLinearStorage::CR_FixedLinearStorage(const uint64_t max_data_size, const uint32_t size_mode)
{
    this->_private_data = NULL;
    this->_private_data_is_mapped = false;

    this->ReSize(max_data_size, size_mode);
}

CR_FixedLinearStorage::~CR_FixedLinearStorage()
{
    if ((!this->_private_data_is_mapped) && (this->_private_data))
        ::free(this->_private_data);
}

int
CR_FixedLinearStorage::Rebuild(uint64_t new_data_size, uint32_t size_mode)
{
    int ret = 0;
    void *old_data_p = NULL;
    int64_t oid;
    const char *kp;
    uint64_t klen;
    const char *vp;
    uint64_t vlen;

    this->_spin.lock();

    if (new_data_size == 0)
        new_data_size = this->_private_size - sizeof(fls_data_t);

    if (size_mode == 0)
        size_mode = this->do_get_sizemode();

    CR_FixedLinearStorage new_fls(new_data_size, size_mode);
    uint64_t total_lines = this->do_get_total_lines();

    for (uint64_t lid=0; lid<total_lines; lid++) {
        ret = this->do_query(this->_private_data, lid, oid, kp, klen, vp, vlen, NULL);
        if (ret)
            break;
        ret = new_fls.Set(lid, oid, kp, klen, vp, vlen);
        if (ret)
            break;
    }

    if (!ret) {
        if (!this->_private_data_is_mapped)
            old_data_p = this->_private_data;
        this->_private_data = new_fls._private_data;
        this->_private_size = new_fls._private_size;
        this->_private_data_is_mapped = false;
        new_fls._private_data = NULL;
    }

    this->_spin.unlock();

    if (old_data_p)
        ::free(old_data_p);

    return ret;
}

int
CR_FixedLinearStorage::ReSize(uint64_t max_data_size, const uint32_t size_mode)
{
    void *old_data_p = NULL;
    void *new_data_p;
    uint64_t new_private_size;

    switch (size_mode) {
    case CR_FLS_SIZEMODE_SMALL:
        if (max_data_size > 0xFFFFF000UL)
            max_data_size = 0xFFFFF000UL;
        break;
    }

    new_private_size = sizeof(fls_data_t) + CR_ALIGN(max_data_size);

    switch (size_mode) {
    case CR_FLS_SIZEMODE_MIDDLE:
        new_private_size += sizeof(line_info_middle_t);
        break;
    case CR_FLS_SIZEMODE_SMALL:
        new_private_size += sizeof(line_info_small_t);
        break;
    default :
        new_private_size += sizeof(line_info_large_t);
        break;
    }

    new_data_p = ::calloc(1, new_private_size);
    if (!new_data_p)
        return errno;

    this->_spin.lock();

    if (!this->_private_data_is_mapped)
        old_data_p = this->_private_data;

    this->_private_size = new_private_size;
    this->_private_data = new_data_p;
    this->_private_data_is_mapped = false;

    this->do_set_sizemode(size_mode);
    this->do_clear(false);

    this->_spin.unlock();

    if (old_data_p)
        ::free(old_data_p);

    return 0;
}

uint32_t
CR_FixedLinearStorage::do_get_sizemode()
{
    if (this->do_set == _do_set_middle)
        return CR_FLS_SIZEMODE_MIDDLE;
    else if (this->do_set == _do_set_small)
        return CR_FLS_SIZEMODE_SMALL;
    else
        return CR_FLS_SIZEMODE_LARGE;
}

uint32_t
CR_FixedLinearStorage::do_set_sizemode(const uint32_t size_mode)
{
    uint32_t ret_size_mode;

    switch (size_mode) {
    case CR_FLS_SIZEMODE_MIDDLE :
        this->do_set = _do_set_middle;
        this->do_query = _do_query_middle;
        this->do_sort = _do_sort_middle;
        ret_size_mode = CR_FLS_SIZEMODE_MIDDLE;
        break;
    case CR_FLS_SIZEMODE_SMALL :
        this->do_set = _do_set_small;
        this->do_query = _do_query_small;
        this->do_sort = _do_sort_small;
        ret_size_mode = CR_FLS_SIZEMODE_SMALL;
        break;
    default :
        this->do_set = _do_set_large;
        this->do_query = _do_query_large;
        this->do_sort = _do_sort_large;
        ret_size_mode = CR_FLS_SIZEMODE_LARGE;
        break;
    }

    return ret_size_mode;
}

int
CR_FixedLinearStorage::do_clear(const bool do_bzero)
{
    if (this->_private_data_is_mapped)
        return EPERM;

    uint64_t data_size = this->_private_size - sizeof(fls_data_t);
    uint64_t line_info_size;

    THIS_FLS_DATA->tag_size = 0;

    CR_FixedLinearStorage_NS::ClearInfos(THIS_FLS_DATA->infos);

    THIS_FLS_DATA->infos.next_line_data_pos = data_size;
    switch (this->do_get_sizemode()) {
    case CR_FLS_SIZEMODE_MIDDLE:
        THIS_FLS_DATA->infos.free_space_left = data_size - sizeof(line_info_middle_t);
        line_info_size = sizeof(line_info_middle_t);
        break;
    case CR_FLS_SIZEMODE_SMALL:
        THIS_FLS_DATA->infos.free_space_left = data_size - sizeof(line_info_small_t);
        line_info_size = sizeof(line_info_small_t);
        break;
    default :
        THIS_FLS_DATA->infos.free_space_left = data_size - sizeof(line_info_large_t);
        line_info_size = sizeof(line_info_large_t);
        break;
    }

    if (do_bzero) {
        bzero(THIS_FLS_DATA->lines_data, data_size);
    } else {
        bzero(THIS_FLS_DATA->lines_data, line_info_size * THIS_FLS_DATA->infos.total_lines);
    }

    return 0;
}

int
CR_FixedLinearStorage::Clear(const bool do_bzero)
{
    int ret;
    this->_spin.lock();
    ret = this->do_clear(do_bzero);
    this->_spin.unlock();
    return ret;
}

void
CR_FixedLinearStorage::SetFlags(const uint64_t flags)
{
    this->_spin.lock();
    THIS_FLS_DATA->infos.flags = flags;
    this->_spin.unlock();
}

uint64_t
CR_FixedLinearStorage::GetFlags()
{
    uint64_t ret;
    this->_spin.lock();
    ret = THIS_FLS_DATA->infos.flags;
    this->_spin.unlock();
    return ret;
}

void
CR_FixedLinearStorage::SetLocalTag(const std::string &local_id_str)
{
    this->_spin.lock();
    this->_local_id = local_id_str;
    this->_spin.unlock();
}

std::string
CR_FixedLinearStorage::GetLocalTag()
{
    std::string ret;
    this->_spin.lock();
    ret = this->_local_id;
    this->_spin.unlock();
    return ret;
}

int
CR_FixedLinearStorage::SetTag(const std::string &s)
{
    int ret = 0;
    uint64_t s_size = s.size();

    if (s_size > sizeof(THIS_FLS_DATA->tag_data))
        return EMSGSIZE;

    this->_spin.lock();

    if (!this->_private_data_is_mapped) {
        memcpy(THIS_FLS_DATA->tag_data, s.data(), s_size);
        THIS_FLS_DATA->tag_size = s_size;
    } else
        ret = EPERM;

    this->_spin.unlock();

    return ret;
}

std::string
CR_FixedLinearStorage::GetTag()
{
    std::string ret;

    this->_spin.lock();

    if (THIS_FLS_DATA->tag_size <= sizeof(THIS_FLS_DATA->tag_data))
        ret.assign(THIS_FLS_DATA->tag_data, THIS_FLS_DATA->tag_size);
    else
        ret.clear();

    this->_spin.unlock();

    return ret;
}

int
CR_FixedLinearStorage::Set(const uint64_t line_no,
    const int64_t obj_id, const void *key_p, const uint64_t key_size,
    const void *value_p, const uint64_t value_size, uint64_t *free_space_left_p)
{
    int ret;

    this->_spin.lock();

    if (!this->_private_data_is_mapped) {
        ret = this->do_set(this->_private_data, line_no, obj_id, key_p, key_size, value_p, value_size);
        if (!ret && free_space_left_p)
            *free_space_left_p = this->do_get_free_space();
    } else
        ret = EPERM;

    this->_spin.unlock();

    return ret;
}

int
CR_FixedLinearStorage::Set(const uint64_t line_no, const CR_FixedLinearStorage_NS::line_info_t &line_info,
    uint64_t *free_space_left_p)
{
    return this->Set(line_no, line_info.obj_id, line_info.key_p, line_info.key_size,
      line_info.value_p, line_info.value_size, free_space_left_p);
}

int
CR_FixedLinearStorage::Set(const uint64_t line_no,
    const int64_t obj_id, const std::string &key,
    const std::string *value_p, uint64_t *free_space_left_p)
{
    int ret;

    this->_spin.lock();

    if (!this->_private_data_is_mapped) {
        if (value_p) {
            ret = this->do_set(this->_private_data, line_no, obj_id,
              key.data(), key.size(), value_p->data(), value_p->size());
        } else
            ret = this->do_set(this->_private_data, line_no, obj_id, key.data(), key.size(), NULL, 0);
        if (!ret && free_space_left_p)
            *free_space_left_p = this->do_get_free_space();
    } else
        ret = EPERM;

    this->_spin.unlock();

    return ret;
}

int
CR_FixedLinearStorage::PushBack(const int64_t obj_id, const void *key_p, const uint64_t key_size,
    const void *value_p, const uint64_t value_size, uint64_t *free_space_left_p)
{
    int ret;

    this->_spin.lock();

    if (!this->_private_data_is_mapped) {
        ret = this->do_set(this->_private_data, THIS_FLS_DATA->infos.total_lines,
          obj_id, key_p, key_size, value_p, value_size);
        if (!ret && free_space_left_p)
            *free_space_left_p = this->do_get_free_space();
    } else
        ret = EPERM;

    this->_spin.unlock();

    return ret;
}

int
CR_FixedLinearStorage::PushBack(const CR_FixedLinearStorage_NS::line_info_t &line_info,
    uint64_t *free_space_left_p)
{
    return this->PushBack(line_info.obj_id, line_info.key_p, line_info.key_size,
      line_info.value_p, line_info.value_size, free_space_left_p);
}

int
CR_FixedLinearStorage::PushBack(const int64_t obj_id, const std::string &key,
    const std::string *value_p, uint64_t *free_space_left_p)
{
    int ret;

    this->_spin.lock();

    if (!this->_private_data_is_mapped) {
        if (value_p) {
            ret = this->do_set(this->_private_data, THIS_FLS_DATA->infos.total_lines,
              obj_id, key.data(), key.size(), value_p->data(), value_p->size());
        } else {
            ret = this->do_set(this->_private_data, THIS_FLS_DATA->infos.total_lines,
              obj_id, key.data(), key.size(), NULL, 0);
        }
        if (!ret && free_space_left_p)
            *free_space_left_p = this->do_get_free_space();
    } else
        ret = EPERM;

    this->_spin.unlock();

    return ret;
}

int
CR_FixedLinearStorage::Query(const uint64_t line_no,
    const char *&key_p, uint64_t &key_size, bool *is_last_line_p)
{
    int ret;
    const char *_value_p;
    uint64_t _value_size;
    int64_t _obj_id;

    this->_spin.lock();

    ret = this->do_query(this->_private_data, line_no, _obj_id, key_p, key_size,
      _value_p, _value_size, is_last_line_p);

    this->_spin.unlock();

    return ret;
}

int
CR_FixedLinearStorage::Query(const uint64_t line_no, std::string &key, bool *is_last_line_p)
{
    int ret;
    const char *key_p;
    uint64_t key_size;
    const char *_value_p;
    uint64_t _value_size;
    int64_t _obj_id;

    this->_spin.lock();

    ret = this->do_query(this->_private_data, line_no, _obj_id, key_p, key_size,
      _value_p, _value_size, is_last_line_p);

    if (!ret) {
        key.assign(key_p, key_size);
    }

    this->_spin.unlock();

    return ret;
}

int
CR_FixedLinearStorage::Query(const uint64_t line_no,
    int64_t &obj_id, const char *&key_p, uint64_t &key_size,
    const char *&value_p, uint64_t &value_size, bool *is_last_line_p)
{
    int ret;

    this->_spin.lock();

    ret = this->do_query(this->_private_data, line_no, obj_id, key_p, key_size,
      value_p, value_size, is_last_line_p);

    this->_spin.unlock();

    return ret;
}

int
CR_FixedLinearStorage::Query(const uint64_t line_no,
    CR_FixedLinearStorage_NS::line_info_t &line_info, bool *is_last_line_p)
{
    return this->Query(line_no, line_info.obj_id, line_info.key_p, line_info.key_size,
      line_info.value_p, line_info.value_size, is_last_line_p);
}

int
CR_FixedLinearStorage::Query(const uint64_t line_no, int64_t &obj_id, std::string &key,
    std::string &value, bool &value_is_null, bool *is_last_line_p)
{
    int ret;
    const char *key_p;
    uint64_t key_size;
    const char *value_p;
    uint64_t value_size;

    this->_spin.lock();

    ret = this->do_query(this->_private_data, line_no, obj_id, key_p, key_size,
      value_p, value_size, is_last_line_p);

    if (!ret) {
        key.assign(key_p, key_size);

        if (value_p) {
            value.assign(value_p, value_size);
            value_is_null = false;
        } else {
            value_is_null = true;
        }
    }

    this->_spin.unlock();

    return ret;
}

inline int64_t
CR_FixedLinearStorage::do_get_min_obj_id()
{
    return THIS_FLS_DATA->infos.min_obj_id;
}

inline int64_t
CR_FixedLinearStorage::do_get_max_obj_id()
{
    return THIS_FLS_DATA->infos.max_obj_id;
}

inline uint64_t
CR_FixedLinearStorage::do_get_total_lines()
{
    return THIS_FLS_DATA->infos.total_lines;
}

inline uint64_t
CR_FixedLinearStorage::do_get_free_space()
{
    return THIS_FLS_DATA->infos.free_space_left;
}

inline uint64_t
CR_FixedLinearStorage::do_get_objid_cksum()
{
    return THIS_FLS_DATA->infos.objid_cksum;
}

inline uint64_t
CR_FixedLinearStorage::do_get_key_cksum()
{
    return THIS_FLS_DATA->infos.key_cksum;
}

inline uint64_t
CR_FixedLinearStorage::do_get_value_cksum()
{
    return THIS_FLS_DATA->infos.value_cksum;
}

inline uint64_t
CR_FixedLinearStorage::do_get_longest_key_size()
{
    return THIS_FLS_DATA->infos.longest_key_size;
}

inline uint64_t
CR_FixedLinearStorage::do_get_longest_value_size()
{
    return THIS_FLS_DATA->infos.longest_value_size;
}

inline uint64_t
CR_FixedLinearStorage::do_get_longest_line_size()
{
    return THIS_FLS_DATA->infos.longest_line_size;
}

inline uint64_t
CR_FixedLinearStorage::do_get_longest_line_store_size()
{
    uint64_t add_size;

    switch (this->do_get_sizemode()) {
    case CR_FLS_SIZEMODE_MIDDLE :
        add_size = sizeof(line_info_middle_t);
        break;
    case CR_FLS_SIZEMODE_SMALL :
        add_size = sizeof(line_info_small_t);
        break;
    default :
        add_size = sizeof(line_info_large_t);
        break;
    }

    return this->do_get_longest_line_size() + add_size;
}

int
CR_FixedLinearStorage::Sort()
{
    int ret = 0;

    this->_spin.lock();

    if (!this->_private_data_is_mapped)
        ret = this->do_sort(this->_private_data);
    else
        ret = EPERM;

    this->_spin.unlock();

    return ret;
}

void
CR_FixedLinearStorage::MergeInfos(CR_FixedLinearStorage_NS::fls_infos_t &fls_infos)
{
    this->_spin.lock();

    CR_FixedLinearStorage_NS::MergeInfo(fls_infos, THIS_FLS_DATA->infos);

    this->_spin.unlock();
}

void
CR_FixedLinearStorage::GetInfos(CR_FixedLinearStorage_NS::fls_infos_t &fls_infos)
{
    this->_spin.lock();

    memcpy(&fls_infos, &(THIS_FLS_DATA->infos), sizeof(CR_FixedLinearStorage_NS::fls_infos_t));

    this->_spin.unlock();
}

uint32_t
CR_FixedLinearStorage::GetSizeMode()
{
    int ret;

    this->_spin.lock();

    ret = this->do_get_sizemode();

    this->_spin.unlock();

    return ret;
}

uint64_t
CR_FixedLinearStorage::GetHeadSize()
{
    return sizeof(fls_data_t);
}

uint64_t
CR_FixedLinearStorage::GetTotalLines()
{
    uint64_t ret;

    this->_spin.lock();

    ret = this->do_get_total_lines();

    this->_spin.unlock();

    return ret;
}

uint64_t
CR_FixedLinearStorage::GetStorageSize()
{
    uint64_t ret;

    this->_spin.lock();

    ret = this->_private_size - sizeof(fls_data_t);

    this->_spin.unlock();

    return ret;
}

uint64_t
CR_FixedLinearStorage::GetFreeSpace()
{
    uint64_t ret;

    this->_spin.lock();

    ret = this->do_get_free_space();

    this->_spin.unlock();

    return ret;
}

uint64_t
CR_FixedLinearStorage::GetObjIDCKSUM()
{
    uint64_t ret;
    this->_spin.lock();
    ret = this->do_get_objid_cksum();
    this->_spin.unlock();
    return ret;
}

uint64_t
CR_FixedLinearStorage::GetKeyCKSUM()
{
    uint64_t ret;
    this->_spin.lock();
    ret = this->do_get_key_cksum();
    this->_spin.unlock();
    return ret;
}

uint64_t
CR_FixedLinearStorage::GetValueCKSUM()
{
    uint64_t ret;
    this->_spin.lock();
    ret = this->do_get_value_cksum();
    this->_spin.unlock();
    return ret;
}

uint64_t
CR_FixedLinearStorage::GetCKSUM()
{
    uint64_t ret = 0;
    this->_spin.lock();
    ret += this->do_get_objid_cksum();
    ret += this->do_get_key_cksum();
    ret += this->do_get_value_cksum();
    this->_spin.unlock();
    return ret;
}

uint64_t
CR_FixedLinearStorage::GetLongestKeySize()
{
    uint64_t ret;

    this->_spin.lock();

    ret = this->do_get_longest_key_size();

    this->_spin.unlock();

    return ret;
}

uint64_t
CR_FixedLinearStorage::GetLongestValueSize()
{
    uint64_t ret;

    this->_spin.lock();

    ret = this->do_get_longest_value_size();

    this->_spin.unlock();

    return ret;
}

uint64_t
CR_FixedLinearStorage::GetLongestLineSize()
{
    uint64_t ret;

    this->_spin.lock();

    ret = this->do_get_longest_line_size();

    this->_spin.unlock();

    return ret;
}

uint64_t
CR_FixedLinearStorage::GetLongestLineStoreSize()
{
    uint64_t ret;

    this->_spin.lock();

    ret = this->do_get_longest_line_store_size();

    this->_spin.unlock();

    return ret;
}

int64_t
CR_FixedLinearStorage::GetMinObjID()
{
    int64_t ret;

    this->_spin.lock();

    ret = this->do_get_min_obj_id();

    this->_spin.unlock();

    return ret;
}

int64_t
CR_FixedLinearStorage::GetMaxObjID()
{
    int64_t ret;

    this->_spin.lock();

    ret = this->do_get_max_obj_id();

    this->_spin.unlock();

    return ret;
}

int
CR_FixedLinearStorage::Data(const void *&data, uint64_t &size)
{
    int ret = 0;

    this->_spin.lock();

    if (this->_private_data) {
        THIS_FLS_DATA->infos.size_mode = this->do_get_sizemode();

        data = this->_private_data;
        size = this->_private_size;
    } else
        ret = EFAULT;

    this->_spin.unlock();

    return ret;
}

int
CR_FixedLinearStorage::SaveToFileDescriptor(int fd, uint64_t slice_size, uint64_t *slice_count_out_p,
    const bool do_compress, const bool is_sock_fd, const double timeout_sec)
{
    int ret = 0;
    uint64_t slice_count = 0;
    int fd_flags = CR_BLOCKOP_FLAG_ERRCHECK;

    if (is_sock_fd)
        fd_flags |= CR_BLOCKOP_FLAG_ISSOCKFD;

    this->_spin.lock();

    if ((slice_size == 0) || (slice_size > this->_private_size)) {
        THIS_FLS_DATA->infos.size_mode = this->do_get_sizemode();

        if (do_compress) {
            uint64_t compress_size;
            void *compress_data = CR_Compress::compress(
              this->_private_data, this->_private_size, NULL, &compress_size, CR_Compress::CT_SNAPPY);
            if (compress_data) {
                ret = CR_Class_NS::fput_block(fd, CR_MSG_LINEARSTORAGE_COMPRESS,
                  compress_data, compress_size, fd_flags, timeout_sec);
                ::free(compress_data);
            } else
                ret = errno;
        } else {
            ret = CR_Class_NS::fput_block(fd, CR_MSG_LINEARSTORAGE,
              this->_private_data, this->_private_size, fd_flags, timeout_sec);
        }

        if (!ret) {
            slice_count = 1;
        }
    } else {
        do {
            if (slice_size < this->do_get_longest_line_store_size())
                slice_size = this->do_get_longest_line_store_size();

            int64_t obj_id;
            const char *key_p;
            uint64_t key_size;
            const char *value_p;
            uint64_t value_size;

            int sub_size_mode = CR_FixedLinearStorage_NS::CalcSizeMode(
              this->do_get_longest_line_size(), this->do_get_longest_key_size(),
              this->do_get_longest_value_size(), this->do_get_total_lines());

            CR_FixedLinearStorage tmp_fls(slice_size, sub_size_mode);

            uint64_t line_no = 0;
            uint64_t total_lines_tmp = this->do_get_total_lines();

            while (line_no < total_lines_tmp) {
                ret = this->do_query(this->_private_data, line_no, obj_id,
                  key_p, key_size, value_p, value_size, NULL);
                if (ret) {
                    DPRINTF("Error[%s] at query line[%llu]\n",
                      CR_Class_NS::strerrno(ret), (long long unsigned)line_no);
                    break;
                }

                ret = tmp_fls.PushBack(obj_id, key_p, key_size, value_p, value_size);
                if (ret == ENOBUFS) {
                    ret = tmp_fls.SaveToFileDescriptor(fd, 0, NULL, do_compress);
                    if (ret)
                        break;
                    tmp_fls.Clear();
                    slice_count++;
                    continue;
                } else if (ret) {
                    DPRINTF("Error[%s] at insert line[%llu]\n",
                      CR_Class_NS::strerrno(ret), (long long unsigned)line_no);
                    break;
                }

                line_no++;
            }

            if (!ret && (tmp_fls.GetTotalLines() > 0)) {
                ret = tmp_fls.SaveToFileDescriptor(fd, 0, NULL, do_compress);
                if (ret)
                    break;
                tmp_fls.Clear();
                slice_count++;
            }
        } while(0);
    }

    this->_spin.unlock();

    if (slice_count_out_p)
        *slice_count_out_p = slice_count;

    if (!ret && !is_sock_fd) {
        fsync(fd);
    }

    return ret;
}

int
CR_FixedLinearStorage::SaveToFile(const std::string &filename, const uint64_t slice_size,
    uint64_t *slice_count_out_p, const bool do_compress)
{
    int saveto_fd = CR_File::open(filename.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0600);

    if (saveto_fd < 0) {
        return errno;
    }

    int ret = this->SaveToFileDescriptor(saveto_fd, slice_size, slice_count_out_p, do_compress);

    ::close(saveto_fd);

    return ret;
}

int
CR_FixedLinearStorage::do_save_to_string(std::string &str_out, const bool do_compat)
{
    if (do_compat && (this->do_get_free_space() >= 1024)) {
        int fret;
        int sub_size_mode = CR_FixedLinearStorage_NS::CalcSizeMode(
          this->do_get_longest_line_size(), this->do_get_longest_key_size(),
          this->do_get_longest_value_size(), this->do_get_total_lines());
        CR_FixedLinearStorage tmp_fls;
        fret = tmp_fls.ReSize(this->_private_size - this->do_get_free_space(), sub_size_mode);
        if (!fret) {
            int64_t obj_id;
            const char *key_p;
            uint64_t key_size;
            const char *value_p;
            uint64_t value_size;
            uint64_t line_count = this->do_get_total_lines();
            for (uint64_t line_no=0; line_no<line_count; line_no++) {
                fret = this->do_query(this->_private_data, line_no, obj_id, key_p, key_size,
                  value_p, value_size, NULL);
                if (fret)
                    break;
                fret = tmp_fls.PushBack(obj_id, key_p, key_size, value_p, value_size);
                if (fret)
                    break;
            }
        }

        if (!fret) {
            return tmp_fls.SaveToString(str_out, false);
        } else {
            return this->do_save_to_string(str_out, false);
        }
    } else {
        THIS_FLS_DATA->infos.size_mode = this->do_get_sizemode();
        str_out.assign((const char*)(this->_private_data), this->_private_size);
        return 0;
    }
}

int
CR_FixedLinearStorage::SaveToString(std::string &str_out, const bool do_compat)
{
    int ret = 0;

    this->_spin.lock();
    ret = this->do_save_to_string(str_out, do_compat);
    this->_spin.unlock();

    return ret;
}

int
CR_FixedLinearStorage::LoadFromFileDescriptor(int fd, const bool is_sock_fd, const double timeout_sec)
{
    int8_t msg_code;
    void *fgot_data;
    uint32_t fgot_size;
    void *old_private_data = NULL;
    int fd_flags = 0;

    if (is_sock_fd)
        fd_flags |= CR_BLOCKOP_FLAG_ISSOCKFD;

    fgot_data = CR_Class_NS::fget_block(fd, msg_code, NULL, fgot_size, fd_flags, timeout_sec);

    if (!fgot_data) {
        if (errno)
            return errno;
        else
            return EFAULT;
    }

    switch (msg_code) {
    case CR_MSG_LINEARSTORAGE :
        this->_spin.lock();

        if (!this->_private_data_is_mapped)
            old_private_data = this->_private_data;
        this->_private_data = fgot_data;
        this->_private_size = fgot_size;
        this->_private_data_is_mapped = false;
        this->do_set_sizemode(THIS_FLS_DATA->infos.size_mode);

        this->_spin.unlock();
        break;
    case CR_MSG_LINEARSTORAGE_COMPRESS :
        {
            void *decompress_data;
            uint64_t decompress_size;

            decompress_data = CR_Compress::decompress(fgot_data, fgot_size, NULL, &decompress_size);
            ::free(fgot_data);
            if (!decompress_data)
                return errno;

            this->_spin.lock();

            if (!this->_private_data_is_mapped)
                old_private_data = this->_private_data;
            this->_private_data = decompress_data;
            this->_private_size = decompress_size;
            this->_private_data_is_mapped = false;
            this->do_set_sizemode(THIS_FLS_DATA->infos.size_mode);

            this->_spin.unlock();
        }
        break;
    default :
        return EPROTONOSUPPORT;
    }

    if (old_private_data)
        ::free(old_private_data);

    return 0;
}

int
CR_FixedLinearStorage::LoadFromFile(const std::string &filename)
{
    int loadfrom_fd = CR_File::open(filename.c_str(), O_RDONLY);

    if (loadfrom_fd < 0) {
        return errno;
    }

    int ret = this->LoadFromFileDescriptor(loadfrom_fd);

    ::close(loadfrom_fd);

    return ret;
}

int
CR_FixedLinearStorage::LoadFromArray(const void * buf, const uint64_t buf_size)
{
    if ((!buf) || (buf_size < sizeof(fls_data_t)))
        return EINVAL;

    void *old_private_data = NULL;
    void *new_private_data = malloc(buf_size);

    if (!new_private_data)
        return errno;

    memcpy(new_private_data, buf, buf_size);

    this->_spin.lock();

    if (!this->_private_data_is_mapped)
        old_private_data = this->_private_data;
    this->_private_data = new_private_data;
    this->_private_size = buf_size;
    this->_private_data_is_mapped = false;
    this->do_set_sizemode(THIS_FLS_DATA->infos.size_mode);

    this->_spin.unlock();

    if (old_private_data)
        ::free(old_private_data);

    return 0;
}

int
CR_FixedLinearStorage::LoadFromString(const std::string &str_in)
{
    return this->LoadFromArray(str_in.data(), str_in.size());
}

int
CR_FixedLinearStorage::MapFromArray(const void * buf, const uint64_t buf_size)
{
    if (buf_size < sizeof(fls_data_t))
        return EINVAL;

    void *old_private_data = NULL;

    this->_spin.lock();

    if (!this->_private_data_is_mapped)
        old_private_data = this->_private_data;
    this->_private_data = (void *)buf;
    this->_private_size = buf_size;
    this->_private_data_is_mapped = true;
    this->do_set_sizemode(THIS_FLS_DATA->infos.size_mode);

    this->_spin.unlock();

    if (old_private_data)
        ::free(old_private_data);

    return 0;
}

bool
CR_FixedLinearStorage_NS::VerifyFile(const char *fname, fls_infos_t *infos_out_p)
{
    int fd = CR_File::open(fname, O_RDONLY, 0);
    int ret = CR_FixedLinearStorage_NS::VerifyFileDescriptor(fd, infos_out_p);
    if (fd >= 0)
        CR_File::close(fd);
    return ret;
}

bool
CR_FixedLinearStorage_NS::VerifyFileDescriptor(int fd, fls_infos_t *infos_out_p)
{
    if (fd < 0)
        return false;
    off_t cur_fpos = CR_File::lseek(fd, 0, SEEK_CUR);
    if (cur_fpos < 0)
        return false;
    if (infos_out_p)
        bzero(infos_out_p, sizeof(fls_infos_t));
    CR_FixedLinearStorage tmp_fls;
    fls_infos_t tmp_fls_info;
    int fret;
    while (1) {
        fret = tmp_fls.LoadFromFileDescriptor(fd);
        if (fret)
            break;
        if (infos_out_p) {
            tmp_fls.GetInfos(tmp_fls_info);
            CR_FixedLinearStorage_NS::MergeInfo(*infos_out_p, tmp_fls_info);
        }
    }
    CR_File::lseek(fd, cur_fpos, SEEK_SET);
    return true;
}
