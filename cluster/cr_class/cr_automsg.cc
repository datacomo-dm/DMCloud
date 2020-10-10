#include <string.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_automsg.h>

static intmax_t
_cr_automsg_nop(void *p)
{
    return (intmax_t)p;
}

CR_AutoMsg::CR_AutoMsg()
{
    this->_type = DT_UNKNOWN;
}

CR_AutoMsg::CR_AutoMsg(const CR_AutoMsg &msg)
{
    this->_type = msg._type;
    memcpy(&(this->_data_store), &(msg._data_store), sizeof(this->_data_store));

    switch (this->_type) {
    case DT_STD_STRING :
        this->_data_info.str_p = new (this->_data_store.cache_data) std::string(*(msg._data_info.str_p));
        break;
    case DT_STD_FUNCTION :
        this->_data_info.func_p = new (this->_data_store.cache_data) function_t(*(msg._data_info.func_p));
        break;
    default :
        this->_data_info = msg._data_info;
        break;
    }
}

CR_AutoMsg::CR_AutoMsg(const intmax_t intmax_v)
{
    this->_type = DT_INT;
    this->_data_store.intmax_v = intmax_v;
}

CR_AutoMsg::CR_AutoMsg(const int int_v)
{
    this->_type = DT_INT;
    this->_data_store.intmax_v = int_v;
}

CR_AutoMsg::CR_AutoMsg(const double double_v)
{
    this->_type = DT_DOUBLE;
    this->_data_store.double_v = double_v;
}

CR_AutoMsg::CR_AutoMsg(const char *char_p)
{
    size_t str_size = CR_Class_NS::strlcpy(this->_data_store.cache_data, char_p,
      sizeof(this->_data_store.cache_data));
    if (str_size >= sizeof(this->_data_store.cache_data)) {
        this->_type = DT_STD_STRING;
        this->_data_info.str_p = new (this->_data_store.cache_data) std::string(char_p);
    } else {
        this->_type = DT_LOCAL_CACHED;
        this->_data_info.cache_len = str_size;
    }
}

CR_AutoMsg::CR_AutoMsg(const std::string &string_v)
{
    size_t str_size = string_v.size();

    if (str_size >= sizeof(this->_data_store.cache_data)) {
        this->_type = DT_STD_STRING;
        this->_data_info.str_p = new (this->_data_store.cache_data) std::string(string_v);
    } else {
        this->_type = DT_LOCAL_CACHED;
        this->_data_info.cache_len = str_size;
        memcpy(this->_data_store.cache_data, string_v.data(), str_size);
        this->_data_store.cache_data[str_size] = '\0';
    }
}

CR_AutoMsg::CR_AutoMsg(const mb_desc_t& memblock_info)
{
    this->_type = DT_MEMBLOCK_INFO;
    this->_data_info.mb_p = memblock_info.first;
    this->_data_store.mb_size = memblock_info.second;
}

CR_AutoMsg::CR_AutoMsg(function_t function_v)
{
    this->_type = DT_STD_FUNCTION;
    this->_data_info.func_p = new (this->_data_store.cache_data) function_t(function_v);
}

CR_AutoMsg::~CR_AutoMsg()
{
    this->clear(false);
}

void
CR_AutoMsg::clear(bool clear_cache)
{
    switch (this->_type) {
    case DT_STD_STRING :
        if (this->_data_info.str_p) {
            this->_data_info.str_p->~basic_string();
            this->_data_info.str_p = NULL;
        }
        break;
    case DT_STD_FUNCTION :
        if (this->_data_info.func_p) {
            this->_data_info.func_p->~function();
            this->_data_info.func_p = NULL;
        }
        break;
    default :
        break;
    }

    if (clear_cache)
        memset(this->_data_store.cache_data, 0, sizeof(this->_data_store.cache_data));

    this->_type = DT_UNKNOWN;
}

bool
CR_AutoMsg::is_null() const
{
    return (this->_type == DT_UNKNOWN);
}

bool
CR_AutoMsg::operator == (const CR_AutoMsg& r_msg) const
{
    bool ret = false;

    if (this == &r_msg) {
        ret = true;
    } else {
        switch (this->_type) {
        case DT_INT :
            switch (r_msg._type) {
            case DT_INT :
                ret = (this->_data_store.intmax_v == r_msg._data_store.intmax_v);
                break;
            case DT_DOUBLE :
                ret = ((double)(this->_data_store.intmax_v) == r_msg._data_store.double_v);
                break;
            case DT_LOCAL_CACHED:
                ret = (this->_data_store.intmax_v ==
                  CR_Class_NS::str2i64(std::string(r_msg._data_store.cache_data, r_msg._data_info.cache_len)));
                break;
            case DT_STD_STRING:
                ret = (this->_data_store.intmax_v ==
                  CR_Class_NS::str2i64(*(r_msg._data_info.str_p)));
                break;
            default :
                break;
            }
            break;
        case DT_DOUBLE :
            switch (r_msg._type) {
            case DT_INT :
                ret = (this->_data_store.double_v == (double)(r_msg._data_store.intmax_v));
                break;
            case DT_DOUBLE :
                ret = (this->_data_store.double_v == r_msg._data_store.double_v);
                break;
            case DT_LOCAL_CACHED:
                ret = (this->_data_store.double_v ==
                  CR_Class_NS::str2double(std::string(r_msg._data_store.cache_data, r_msg._data_info.cache_len)));
                break;
            case DT_STD_STRING:
                ret = (this->_data_store.double_v ==
                  CR_Class_NS::str2double(*(r_msg._data_info.str_p)));
                break;
            default :
                break;
            }
            break;
        case DT_LOCAL_CACHED :
            switch (r_msg._type) {
            case DT_INT :
                ret = (CR_Class_NS::str2i64(std::string(this->_data_store.cache_data, this->_data_info.cache_len))
                  == r_msg._data_store.intmax_v);
                break;
            case DT_DOUBLE :
                ret = (CR_Class_NS::str2double(std::string(this->_data_store.cache_data, this->_data_info.cache_len))
                  == r_msg._data_store.double_v);
                break;
            case DT_LOCAL_CACHED:
                if (this->_data_info.cache_len == r_msg._data_info.cache_len) {
                    ret = (CR_Class_NS::memlcmp(this->_data_store.cache_data, this->_data_info.cache_len,
                      r_msg._data_store.cache_data, r_msg._data_info.cache_len) == 0);
                }
                break;
            case DT_STD_STRING:
                if (this->_data_info.cache_len == r_msg._data_info.str_p->size()) {
                    ret = (CR_Class_NS::memlcmp(this->_data_store.cache_data, this->_data_info.cache_len,
                      r_msg._data_info.str_p->data(), r_msg._data_info.str_p->size()) == 0);
                }
                break;
            default :
                break;
            }
            break;
        case DT_STD_STRING :
            switch (r_msg._type) {
            case DT_INT :
                ret = (CR_Class_NS::str2i64(*(this->_data_info.str_p)) == r_msg._data_store.intmax_v);
                break;
            case DT_DOUBLE :
                ret = (CR_Class_NS::str2double(*(this->_data_info.str_p)) == r_msg._data_store.double_v);
                break;
            case DT_LOCAL_CACHED:
                if (this->_data_info.str_p->size() == r_msg._data_info.cache_len) {
                    ret = (CR_Class_NS::memlcmp(this->_data_info.str_p->data(), this->_data_info.str_p->size(),
                      r_msg._data_store.cache_data, r_msg._data_info.cache_len) == 0);
                }
                break;
            case DT_STD_STRING:
                ret = (this->_data_info.str_p->compare(*(r_msg._data_info.str_p)) == 0);
                break;
            default :
                break;
            }
            break;
        case DT_MEMBLOCK_INFO :
            if (r_msg._type == DT_MEMBLOCK_INFO) {
                ret = ((this->_data_info.mb_p == r_msg._data_info.mb_p) &&
                  (this->_data_store.mb_size == r_msg._data_store.mb_size));
            }
            break;
        case DT_STD_FUNCTION :
        default :
            break;
        }
    }

    return ret;
}

CR_AutoMsg &
CR_AutoMsg::operator=(const CR_AutoMsg &r_msg)
{
    if (this != &r_msg) {
        this->clear();
        this->_type = r_msg._type;
        memcpy(&(this->_data_store), &(r_msg._data_store), sizeof(this->_data_store));

        switch (this->_type) {
        case DT_STD_STRING :
            this->_data_info.str_p = new (this->_data_store.cache_data) std::string(*(r_msg._data_info.str_p));
            break;
        case DT_STD_FUNCTION :
            this->_data_info.func_p = new (this->_data_store.cache_data) function_t(*(r_msg._data_info.func_p));
            break;
        default :
            this->_data_info = r_msg._data_info;
            break;
        }
    }
    return *this;
}

CR_AutoMsg &
CR_AutoMsg::operator=(const intmax_t r_intmax_t)
{
    this->clear();
    this->_type = DT_INT;
    this->_data_store.intmax_v = r_intmax_t;
    return *this;
}

CR_AutoMsg &
CR_AutoMsg::operator=(const int r_int)
{
    this->clear();
    this->_type = DT_INT;
    this->_data_store.intmax_v = r_int;
    return *this;
}

CR_AutoMsg &
CR_AutoMsg::operator=(const double r_double)
{
    this->clear();
    this->_type = DT_DOUBLE;
    this->_data_store.double_v = r_double;
    return *this;
}

CR_AutoMsg &
CR_AutoMsg::operator=(const char *r_char_p)
{
    if (r_char_p) {
        if (r_char_p != this->as_c_str()) {
            if (this->_type == DT_STD_STRING) {
                *(this->_data_info.str_p) = r_char_p;
            } else {
                this->clear();
                size_t str_size = CR_Class_NS::strlcpy(this->_data_store.cache_data, r_char_p,
                  sizeof(this->_data_store.cache_data));
                if (str_size >= sizeof(this->_data_store.cache_data)) {
                    this->_type = DT_STD_STRING;
                    this->_data_info.str_p = new (this->_data_store.cache_data) std::string(r_char_p);
                } else {
                    this->_type = DT_LOCAL_CACHED;
                    this->_data_info.cache_len = str_size;
                }
            }
        }
    } else {
        this->clear();
        this->_type = DT_LOCAL_CACHED;
        this->_data_info.cache_len = 0;
    }
    return *this;
}

CR_AutoMsg &
CR_AutoMsg::operator=(const std::string &r_string)
{
    if (this->_type == DT_STD_STRING) {
        *(this->_data_info.str_p) = r_string;
    } else {
        this->clear();
        size_t str_size = r_string.size();
        if (str_size >= sizeof(this->_data_store.cache_data)) {
            this->_type = DT_STD_STRING;
            this->_data_info.str_p = new (this->_data_store.cache_data) std::string(r_string);
        } else {
            this->_type = DT_LOCAL_CACHED;
            this->_data_info.cache_len = str_size;
            memcpy(this->_data_store.cache_data, r_string.data(), str_size);
        }
    }
    return *this;
}

CR_AutoMsg &
CR_AutoMsg::operator=(const mb_desc_t& memblock_info)
{
    this->clear();
    this->_type = DT_MEMBLOCK_INFO;
    this->_data_info.mb_p = memblock_info.first;
    this->_data_store.mb_size = memblock_info.second;
    return *this;
}

CR_AutoMsg &
CR_AutoMsg::operator=(function_t r_function)
{
    if (this->_type == DT_STD_FUNCTION) {
        *(this->_data_info.func_p) = r_function;
    } else {
        this->clear();
        this->_type = DT_STD_FUNCTION;
        this->_data_info.func_p = new (this->_data_store.cache_data) function_t(r_function);
    }
    return *this;
}

CR_AutoMsg::data_t
CR_AutoMsg::type() const
{
    if (this->_type == DT_LOCAL_CACHED) {
        return DT_STD_STRING;
    }

    return this->_type;
}

intmax_t
CR_AutoMsg::as_intmax_t(const intmax_t default_val) const
{
    intmax_t ret;

    switch (this->_type) {
    case DT_LOCAL_CACHED:
        ret = CR_Class_NS::str2i64(std::string(this->_data_store.cache_data, this->_data_info.cache_len));
        break;
    case DT_INT:
        ret = this->_data_store.intmax_v;
        break;
    case DT_DOUBLE:
        ret = (intmax_t)(this->_data_store.double_v);
        break;
    case DT_STD_STRING:
        ret = CR_Class_NS::str2i64(*(this->_data_info.str_p));
        break;
    default:
        ret = default_val;
        break;
    }

    return ret;
}

double
CR_AutoMsg::as_double(const double default_val) const
{
    double ret;

    switch (this->_type) {
    case DT_LOCAL_CACHED:
        ret = CR_Class_NS::str2double(std::string(this->_data_store.cache_data, this->_data_info.cache_len));
        break;
    case DT_INT:
        ret = (double)(this->_data_store.intmax_v);
        break;
    case DT_DOUBLE:
        ret = this->_data_store.double_v;
        break;
    case DT_STD_STRING:
        ret = CR_Class_NS::str2double(*(this->_data_info.str_p));
        break;
    default:
        ret = default_val;
        break;
    }

    return ret;
}

const char *
CR_AutoMsg::as_c_str(size_t *len_p) const
{
    const char *ret;

    switch (this->_type) {
    case DT_LOCAL_CACHED:
        ret = this->_data_store.cache_data;
        if (len_p)
            *len_p = this->_data_info.cache_len;
        break;
    case DT_STD_STRING:
        ret = this->_data_info.str_p->c_str();
        if (len_p)
            *len_p = this->_data_info.str_p->length();
        break;
    default:
        ret = NULL;
        break;
    }

    return ret;
}

std::string
CR_AutoMsg::as_string() const
{
    std::string ret;

    switch (this->_type) {
    case DT_LOCAL_CACHED:
        ret.assign(this->_data_store.cache_data, this->_data_info.cache_len);
        break;
    case DT_INT:
        ret = CR_Class_NS::i642str(this->_data_store.intmax_v);
        break;
    case DT_DOUBLE:
        ret = CR_Class_NS::double2str(this->_data_store.double_v);
        break;
    case DT_STD_STRING:
        ret = *(this->_data_info.str_p);
        break;
    default:
        ret.clear();
        break;
    }

    return ret;
}

void*
CR_AutoMsg::as_memblock(size_t &size) const
{
    void *ret;

    switch (this->_type) {
    case DT_MEMBLOCK_INFO:
        ret = this->_data_info.mb_p;
        size = this->_data_store.mb_size;
        break;
    default:
        ret = NULL;
        size = 0;
        break;
    }

    return ret;
}

CR_AutoMsg::function_t
CR_AutoMsg::as_function() const
{
    if ((this->_type == DT_STD_FUNCTION) && (*(this->_data_info.func_p))) {
        return *(this->_data_info.func_p);
    } else {
        return std::bind(_cr_automsg_nop, std::placeholders::_1);
    }
}

size_t
CR_AutoMsg::size() const
{
    size_t ret = 0;

    switch (this->_type) {
    case DT_INT:
        ret = sizeof(this->_data_store.intmax_v);
        break;
    case DT_DOUBLE:
        ret = sizeof(this->_data_store.double_v);
        break;
    case DT_LOCAL_CACHED:
        ret = this->_data_info.cache_len;
        break;
    case DT_STD_STRING:
        ret = this->_data_info.str_p->size();
        break;
    case DT_STD_FUNCTION:
        ret = sizeof(function_t);
        break;
    default:
        break;
    }

    return ret;
}
