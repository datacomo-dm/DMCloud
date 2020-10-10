#include <time.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_addon.h>
#include <cfm_drv/dm_ib_rowdata.h>

int64_t
DM_IB_RowData_NS::mkibdate(int16_t year, uint8_t month, uint8_t day,
    uint8_t hour, uint8_t minute, uint8_t second)
{
    DM_IB_RowData_NS::ib_datetime_t ib_datetime_tmp;

    ib_datetime_tmp.union_v.int_v = 0;

    ib_datetime_tmp.fields.year = abs(CR_Class_NS::limited_range<int16_t>(year, -8192, 8191));
    ib_datetime_tmp.fields.month = CR_Class_NS::limited_range<uint8_t>(month, 1, 12);
    ib_datetime_tmp.fields.day = CR_Class_NS::limited_range<uint8_t>(day, 1, 31);
    ib_datetime_tmp.fields.hour = CR_Class_NS::limited_range<uint8_t>(hour, 0, 23);
    ib_datetime_tmp.fields.minute = CR_Class_NS::limited_range<uint8_t>(minute, 0, 59);
    ib_datetime_tmp.fields.second = CR_Class_NS::limited_range<uint8_t>(second, 0, 59);

    if(year < 0) {
        ib_datetime_tmp.union_v.int_v = ib_datetime_tmp.union_v.int_v * -1;
    }

    return ib_datetime_tmp.union_v.int_v;
}

void
DM_IB_RowData_NS::tm2ibdate(const struct tm &t, int64_t &ibdate)
{
    ibdate = DM_IB_RowData_NS::mkibdate(t.tm_year + 1900, t.tm_mon + 1, t.tm_mday,
                                        t.tm_hour, t.tm_min, t.tm_sec);
}

void
DM_IB_RowData_NS::ibdate2tm(const int64_t ibdate, struct tm &t)
{
    int sign = ibdate < 0 ? -1 : 1;
    DM_IB_RowData_NS::ib_datetime_t ib_datetime_tmp;
    ib_datetime_tmp.union_v.int_v = llabs(ibdate);
    bzero(&t, sizeof(t));
    t.tm_year = ib_datetime_tmp.fields.year * sign - 1900;
    t.tm_mon = ib_datetime_tmp.fields.month - 1;
    t.tm_mday = ib_datetime_tmp.fields.day;
    t.tm_hour = ib_datetime_tmp.fields.hour;
    t.tm_min = ib_datetime_tmp.fields.minute;
    t.tm_sec = ib_datetime_tmp.fields.second;
}

void
DM_IB_RowData_NS::tm2str(const struct tm &t, char *t_str)
{
    ::strftime(t_str, 15, DM_IB_ROWDATA_DATE_FORMAT_STRING, &t);
}

void
DM_IB_RowData_NS::str2tm(const char *t_str, struct tm &t)
{
    ::strptime(t_str, DM_IB_ROWDATA_DATE_FORMAT_STRING, &t);
}

void
DM_IB_RowData_NS::str2ibdate(const char *t_str, int64_t &ibdate)
{
    struct tm t;
    DM_IB_RowData_NS::str2tm(t_str, t);
    DM_IB_RowData_NS::tm2ibdate(t, ibdate);
}

void
DM_IB_RowData_NS::ibdate2str(const int64_t ibdate, char *t_str)
{
    struct tm t;
    DM_IB_RowData_NS::ibdate2tm(ibdate, t);
    DM_IB_RowData_NS::tm2str(t, t_str);
}

//////////////////////////////////////

DM_IB_RowData::~DM_IB_RowData()
{
    this->Reset();
}

void
DM_IB_RowData::Reset()
{
    if (!this->_is_readonly && this->_row_data_p)
        delete []this->_row_data_p;
    this->_row_data_p = NULL;
    this->_is_readonly = false;
}

void
DM_IB_RowData::Clear()
{
    if (!this->_is_readonly && this->_row_data_p) {
        ::bzero(this->_row_data_p, sizeof(uint64_t) * this->_next_str_slot);
#if defined(DIAGNOSTIC)
        ::memset(this->_row_data_p, 0xFF, sizeof(uint64_t) * this->_null_slot);
#endif // DIAGNOSTIC
        this->_next_str_slot = this->_null_slot + this->_table_info_p->size();
    }
}

void
DM_IB_RowData::BindTableInfo(const DM_IB_TableInfo &table_info)
{
    if (this->_table_info_p)
        return;
    this->_table_info_p = &table_info;
    size_t max_string_slot = 0;
    size_t col_count = table_info.size();
    size_t null_slot = (CR_Class_NS::BMP_BITNSLOTS(col_count) + sizeof(uint64_t) - 1) / sizeof(uint64_t);
    for (size_t i=0; i<col_count; i++) {
        const DM_IB_TableInfo::column_info_t &cur_col_info = table_info[i];
        switch (cur_col_info.column_type) {
        case DM_IB_TABLEINFO_CT_INT :
        case DM_IB_TABLEINFO_CT_BIGINT :
        case DM_IB_TABLEINFO_CT_FLOAT :
        case DM_IB_TABLEINFO_CT_DOUBLE :
        case DM_IB_TABLEINFO_CT_DATETIME :
            break;
        case DM_IB_TABLEINFO_CT_CHAR :
        case DM_IB_TABLEINFO_CT_VARCHAR :
            max_string_slot += (cur_col_info.column_scale + sizeof(uint64_t) - 1) / sizeof(uint64_t);
            break;
        default :
            break;
        }
    }
    this->_null_slot = null_slot;
    this->_max_slot = null_slot + col_count + max_string_slot;
    this->_row_data_p = new CR_Class_NS::union64_t[this->_max_slot];
    ::bzero(this->_row_data_p, sizeof(uint64_t) * this->_max_slot);
    this->Clear();
}

int
DM_IB_RowData::SetNull(const size_t col_id)
{
#if defined(DIAGNOSTIC)
    if (col_id >= this->_table_info_p->size())
        return ERANGE;
    if (this->_is_readonly)
        return EPERM;
    if (this->_max_slot == 0)
        return ENODATA;
#endif // DIAGNOSTIC
    CR_Class_NS::BMP_BITSET(this->_row_data_p, INT_MAX, col_id);
    return 0;
}

int
DM_IB_RowData::SetInt64(const size_t col_id, const int64_t v)
{
#if defined(DIAGNOSTIC)
    if (col_id >= this->_table_info_p->size())
        return ERANGE;
    if (this->_is_readonly)
        return EPERM;
    if (this->_max_slot == 0)
        return ENODATA;
    switch ((*(this->_table_info_p))[col_id].column_type) {
    case DM_IB_TABLEINFO_CT_INT :
    case DM_IB_TABLEINFO_CT_BIGINT :
        break;
    default :
        return ENOTSUP;
    }
    CR_Class_NS::BMP_BITCLEAR(this->_row_data_p, INT_MAX, col_id);
#endif // DIAGNOSTIC
    this->_row_data_p[this->_null_slot + col_id].int_v = v;
    return 0;
}

int
DM_IB_RowData::SetDouble(const size_t col_id, const double v)
{
#if defined(DIAGNOSTIC)
    if (col_id >= this->_table_info_p->size())
        return ERANGE;
    if (this->_is_readonly)
        return EPERM;
    if (this->_max_slot == 0)
        return ENODATA;
    switch ((*(this->_table_info_p))[col_id].column_type) {
    case DM_IB_TABLEINFO_CT_FLOAT :
    case DM_IB_TABLEINFO_CT_DOUBLE :
        break;
    default :
        return ENOTSUP;
    }
    CR_Class_NS::BMP_BITCLEAR(this->_row_data_p, INT_MAX, col_id);
#endif // DIAGNOSTIC
    this->_row_data_p[this->_null_slot + col_id].float_v = v;
    return 0;
}

int
DM_IB_RowData::SetDateTime(const size_t col_id, const int64_t ibdate)
{
#if defined(DIAGNOSTIC)
    if (col_id >= this->_table_info_p->size())
        return ERANGE;
    if (this->_is_readonly)
        return EPERM;
    if (this->_max_slot == 0)
        return ENODATA;
    switch ((*(this->_table_info_p))[col_id].column_type) {
    case DM_IB_TABLEINFO_CT_DATETIME :
        break;
    default :
        return ENOTSUP;
    }
    CR_Class_NS::BMP_BITCLEAR(this->_row_data_p, INT_MAX, col_id);
#endif // DIAGNOSTIC
    this->_row_data_p[this->_null_slot + col_id].int_v = ibdate;
    return 0;
}

int
DM_IB_RowData::SetDateTime(const size_t col_id, const struct tm &v)
{
    int64_t ibdate;
    DM_IB_RowData_NS::tm2ibdate(v, ibdate);
    return this->SetDateTime(col_id, ibdate);
}

int
DM_IB_RowData::SetDateTime(const size_t col_id, const char *v)
{
    int64_t ibdate;
    DM_IB_RowData_NS::str2ibdate(v, ibdate);
    return this->SetDateTime(col_id, ibdate);
}

int
DM_IB_RowData::SetString(const size_t col_id, const char *v, const size_t vlen)
{
#if defined(DIAGNOSTIC)
    if (col_id >= this->_table_info_p->size())
        return ERANGE;
    if (this->_is_readonly)
        return EPERM;
    if (this->_max_slot == 0)
        return ENODATA;
    switch ((*(this->_table_info_p))[col_id].column_type) {
    case DM_IB_TABLEINFO_CT_CHAR :
    case DM_IB_TABLEINFO_CT_VARCHAR :
        break;
    default :
        return ENOTSUP;
    }
#endif // DIAGNOSTIC
    CR_Class_NS::union64_t &cur_col_data = this->_row_data_p[this->_null_slot + col_id];
    const size_t vlen_local = CR_Class_NS::min(vlen, (size_t)(*(this->_table_info_p))[col_id].column_scale);
    const size_t use_slot_old = (cur_col_data.pos_v.len + sizeof(uint64_t) - 1) / sizeof(uint64_t);
    const size_t use_slot = (vlen_local + sizeof(uint64_t) - 1) / sizeof(uint64_t);
    if (cur_col_data.pos_v.offset && (use_slot <= use_slot_old)) {
        ::memcpy(&(this->_row_data_p[cur_col_data.pos_v.offset]), v, vlen_local);
        cur_col_data.pos_v.len = vlen_local;
    } else {
        if ((this->_next_str_slot + use_slot) > this->_max_slot)
            return ENOMEM;
        uint16_t save_slot = this->_next_str_slot;
        this->_next_str_slot += use_slot;
        cur_col_data.pos_v.offset = save_slot;
        ::memcpy(&(this->_row_data_p[save_slot]), v, vlen_local);
        cur_col_data.pos_v.len = vlen_local;
    }
#if defined(DIAGNOSTIC)
    CR_Class_NS::BMP_BITCLEAR(this->_row_data_p, INT_MAX, col_id);
#endif // DIAGNOSTIC
    return 0;
}

bool
DM_IB_RowData::IsNull(size_t col_id) const
{
#if defined(DIAGNOSTIC)
    if (col_id >= this->_table_info_p->size())
        return true;
#endif // DIAGNOSTIC
    return CR_Class_NS::BMP_BITTEST(this->_row_data_p, INT_MAX, col_id);
}

int
DM_IB_RowData::GetInt64(const size_t col_id, int64_t &v) const
{
#if defined(DIAGNOSTIC)
    if (col_id >= this->_table_info_p->size())
        return ERANGE;
    if (this->_max_slot == 0)
        return ENODATA;
    switch ((*(this->_table_info_p))[col_id].column_type) {
    case DM_IB_TABLEINFO_CT_INT :
    case DM_IB_TABLEINFO_CT_BIGINT :
        break;
    default :
        return ENOTSUP;
    }
#endif // DIAGNOSTIC
    v = this->_row_data_p[this->_null_slot + col_id].int_v;
    return 0;
}

int
DM_IB_RowData::GetDouble(const size_t col_id, double &v) const
{
#if defined(DIAGNOSTIC)
    if (col_id >= this->_table_info_p->size())
        return ERANGE;
    if (this->_max_slot == 0)
        return ENODATA;
    switch ((*(this->_table_info_p))[col_id].column_type) {
    case DM_IB_TABLEINFO_CT_FLOAT :
    case DM_IB_TABLEINFO_CT_DOUBLE :
        break;
    default :
        return ENOTSUP;
    }
#endif // DIAGNOSTIC
    v = this->_row_data_p[this->_null_slot + col_id].float_v;
    return 0;
}

int
DM_IB_RowData::GetDateTime(const size_t col_id, int64_t &v) const
{
#if defined(DIAGNOSTIC)
    if (col_id >= this->_table_info_p->size())
        return ERANGE;
    if (this->_max_slot == 0)
        return ENODATA;
    switch ((*(this->_table_info_p))[col_id].column_type) {
    case DM_IB_TABLEINFO_CT_DATETIME :
        break;
    default :
        return ENOTSUP;
    }
#endif // DIAGNOSTIC
    v = this->_row_data_p[this->_null_slot + col_id].int_v;
    return 0;
}

int
DM_IB_RowData::GetDateTime(const size_t col_id, struct tm &v) const
{
    int64_t ibdate;
    int fret = this->GetDateTime(col_id, ibdate);
    if (!fret)
        DM_IB_RowData_NS::ibdate2tm(ibdate, v);
    return fret;
}

int
DM_IB_RowData::GetDateTime(const size_t col_id, char *v) const
{
    int64_t ibdate;
    int fret = this->GetDateTime(col_id, ibdate);
    if (!fret)
        DM_IB_RowData_NS::ibdate2str(ibdate, v);
    return fret;
}

int
DM_IB_RowData::GetString(const size_t col_id, const char *&v, size_t &vlen) const
{
#if defined(DIAGNOSTIC)
    if (col_id >= this->_table_info_p->size())
        return ERANGE;
    if (this->_max_slot == 0)
        return ENODATA;
    switch ((*(this->_table_info_p))[col_id].column_type) {
    case DM_IB_TABLEINFO_CT_CHAR :
    case DM_IB_TABLEINFO_CT_VARCHAR :
        break;
    default :
        return ENOTSUP;
    }
#endif // DIAGNOSTIC
    const CR_Class_NS::union64_t &cur_row_data = this->_row_data_p[this->_null_slot + col_id];
    if (cur_row_data.pos_v.offset == 0) {
        v = NULL;
        vlen = 0;
    } else {
        v = (const char *)&(this->_row_data_p[cur_row_data.pos_v.offset]);
        vlen = cur_row_data.pos_v.len;
    }
    return 0;
}

int
DM_IB_RowData::Data(const void *&data, size_t &size) const
{
#if defined(DIAGNOSTIC)
    if (this->_row_data_p == NULL)
        return ENODATA;
#endif // DIAGNOSTIC
    data = this->_row_data_p;
    size = this->_next_str_slot * sizeof(uint64_t);
    return 0;
}

int
DM_IB_RowData::MapFromArray(const void * buf, const uint64_t buf_size)
{
#if defined(DIAGNOSTIC)
    if (buf == NULL)
        return EFAULT;
    if ((buf_size % sizeof(uint64_t)) != 0)
        return EINVAL;
#endif // DIAGNOSTIC
    this->Reset();
    this->_is_readonly = true;
    this->_row_data_p = (CR_Class_NS::union64_t*)buf;
    return 0;
}

std::string
DM_IB_RowData::GetSortAbleColKey(size_t col_id, const bool use_null_prefix) const
{
    std::string ret;
#if defined(DIAGNOSTIC)
    if (col_id >= this->_table_info_p->size())
        return ret;
#endif // DIAGNOSTIC
    if (this->IsNull(col_id)) {
        if (use_null_prefix) {
            ret = CR_Class_NS::bool_false_str;
        }
    } else {
        if (use_null_prefix) {
            ret = CR_Class_NS::bool_true_str;
        }
        switch ((*(this->_table_info_p))[col_id].column_type) {
        case DM_IB_TABLEINFO_CT_INT :
        case DM_IB_TABLEINFO_CT_BIGINT :
        case DM_IB_TABLEINFO_CT_DATETIME :
          {
            int64_t int_v;
            this->GetInt64(col_id, int_v);
            ret.append(CR_Class_NS::i642str(int_v));
            break;
          }
        case DM_IB_TABLEINFO_CT_CHAR :
        case DM_IB_TABLEINFO_CT_VARCHAR :
          {
            const char *str_v;
            size_t str_vlen;
            this->GetString(col_id, str_v, str_vlen);
            ret.append(str_v, str_vlen);
            break;
          }
        case DM_IB_TABLEINFO_CT_FLOAT :
        case DM_IB_TABLEINFO_CT_DOUBLE :
          {
            double double_v;
            this->GetDouble(col_id, double_v);
            ret.append(CR_Class_NS::double2str(double_v));
            break;
          }
        default :
            break;
        }
    }
    return ret;
}
