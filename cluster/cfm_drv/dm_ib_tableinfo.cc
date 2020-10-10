#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_addon.h>
#include <cfm_drv/dm_ib_tableinfo.h>

DM_IB_TableInfo::DM_IB_TableInfo()
{
}

DM_IB_TableInfo::~DM_IB_TableInfo()
{
}

int
DM_IB_TableInfo::SerializeToString(std::string &tbl_info_str) const
{
    std::string tmp_str;

    // serialize 16bit col_numbers
    {
        uint16_t col_numbers = this->table_info.size();
        col_numbers = htobe16(col_numbers);
        tmp_str.append((const char*)(&col_numbers), sizeof(col_numbers));
    }

    // serialize columns
    for (size_t i=0; i< this->table_info.size(); i++) {
        const column_info_t &column_info = this->table_info[i];
        bool new_version = false;

        // serialize 16bit col_scale
        {
            uint16_t col_scale = column_info.column_scale;
            col_scale = htobe16(col_scale);
            tmp_str.append((const char*)(&col_scale), sizeof(col_scale));
        }

        // serialize 8bit col_type
        {
            uint8_t col_type = column_info.column_type;
            if ((column_info.column_flags != 0) || (column_info.column_comment.size() != 0)) {
                col_type |= UINT8_C(0x80);
                new_version = true;
            }
            tmp_str.append((const char*)(&col_type), sizeof(col_type));
        }

        // serialize col_name
        {
            // serialize 8bit col_name_len
            uint8_t col_name_len = column_info.column_name.size();
            tmp_str.append((const char*)(&col_name_len), sizeof(col_name_len));
            // serialize col_name data
            tmp_str.append(column_info.column_name);
        }

        if (new_version) {
            // serialize 16bit col_version
            {
                uint16_t col_ver = 1;
                col_ver = htobe16(col_ver);
                tmp_str.append((const char*)(&col_ver), sizeof(col_ver));
            }

            // serialize 16bit col_flags
            {
                uint16_t col_flags = column_info.column_flags;
                col_flags = htobe16(col_flags);
                tmp_str.append((const char*)(&col_flags), sizeof(col_flags));
            }

            // serialize col_comment
            {
                // serialize 16bit col_comment_len
                uint16_t col_comment_len = column_info.column_comment.size();
                col_comment_len = htobe16(col_comment_len);
                tmp_str.append((const char*)(&col_comment_len), sizeof(col_comment_len));
                // serialize col_comment data
                tmp_str.append(column_info.column_comment);
            }
        }
    }

    tbl_info_str = tmp_str;

    return 0;
}

int
DM_IB_TableInfo::ParseFromString(const std::string &tbl_info_str)
{
    std::vector<column_info_t> table_info_local;
    const char *str_data = tbl_info_str.data();
    const size_t str_size = tbl_info_str.size();
    const char *str_cur_p = str_data;

    // parse 16bit col_numbers
    uint16_t col_numbers;
    if (!CR_BOUNDARY_CHECK(str_cur_p, sizeof(col_numbers), str_data, str_size))
        return EILSEQ;
    str_cur_p = (const char *)CR_Class_NS::memread(&col_numbers, str_cur_p, sizeof(col_numbers));

    size_t column_count = be16toh(col_numbers);

    // parse columns
    for (size_t i=0; i<column_count; i++) {
        column_info_t column_info_tmp;
        bool new_version = false;

        // parse 16bit col_scale
        {
            uint16_t col_scale;
            if (!CR_BOUNDARY_CHECK(str_cur_p, sizeof(col_scale), str_data, str_size))
                return EILSEQ;
            str_cur_p = (const char *)CR_Class_NS::memread(&col_scale, str_cur_p, sizeof(col_scale));
            col_scale = be16toh(col_scale);
            column_info_tmp.column_scale = col_scale;
        }

        // parse 8bit col_type
        {
            uint8_t col_type;
            if (!CR_BOUNDARY_CHECK(str_cur_p, sizeof(col_type), str_data, str_size))
                return EILSEQ;
            str_cur_p = (const char *)CR_Class_NS::memread(&col_type, str_cur_p, sizeof(col_type));
            if ((col_type & UINT8_C(0x80)) == UINT8_C(0x80)) {
                new_version = true;
                col_type &= ~(UINT8_C(0x80));
            }
            switch (col_type) {
            case DM_IB_TABLEINFO_CT_INT :
            case DM_IB_TABLEINFO_CT_BIGINT :
            case DM_IB_TABLEINFO_CT_CHAR :
            case DM_IB_TABLEINFO_CT_VARCHAR :
            case DM_IB_TABLEINFO_CT_FLOAT :
            case DM_IB_TABLEINFO_CT_DOUBLE :
            case DM_IB_TABLEINFO_CT_DATETIME :
                break;
            default :
                return EPROTOTYPE;
            }
            column_info_tmp.column_type = col_type;
        }

        // parse col_name
        {
            // parse 8bit col_name_len
            uint8_t col_name_len;
            if (!CR_BOUNDARY_CHECK(str_cur_p, sizeof(col_name_len), str_data, str_size))
                return EILSEQ;
            str_cur_p = (const char *)CR_Class_NS::memread(&col_name_len, str_cur_p, sizeof(col_name_len));

            // parse col_name data
            {
                if (!CR_BOUNDARY_CHECK(str_cur_p, col_name_len, str_data, str_size))
                    return EILSEQ;
                column_info_tmp.column_name.assign(str_cur_p, col_name_len);
                str_cur_p += col_name_len;
            }
        }

        if (new_version) {
            // parse 16bit col_version
            {
                uint16_t col_ver;
                if (!CR_BOUNDARY_CHECK(str_cur_p, sizeof(col_ver), str_data, str_size))
                    return EILSEQ;
                str_cur_p = (const char *)CR_Class_NS::memread(&col_ver, str_cur_p, sizeof(col_ver));
                col_ver = be16toh(col_ver);
                switch (col_ver) {
                case 0:
                    break;
                case 1:
                    // parse 16bit col_flags
                    {
                        uint16_t col_flags;
                        if (!CR_BOUNDARY_CHECK(str_cur_p, sizeof(col_flags), str_data, str_size))
                            return EILSEQ;
                        str_cur_p = (const char *)CR_Class_NS::memread(
                          &col_flags, str_cur_p, sizeof(col_flags));
                        col_flags = be16toh(col_flags);
                        column_info_tmp.column_flags = col_flags;
                    }

                    // parse col_comment
                    {
                        // parse 16bit col_comm_len
                        uint16_t col_comm_len;
                        if (!CR_BOUNDARY_CHECK(str_cur_p, sizeof(col_comm_len), str_data, str_size))
                            return EILSEQ;
                        str_cur_p = (const char *)CR_Class_NS::memread(
                          &col_comm_len, str_cur_p, sizeof(col_comm_len));
                        col_comm_len = be16toh(col_comm_len);
                        // parse col_comment data
                        {
                            if (!CR_BOUNDARY_CHECK(str_cur_p, col_comm_len, str_data, str_size))
                                return EILSEQ;
                            column_info_tmp.column_comment.assign(str_cur_p, col_comm_len);
                            str_cur_p += col_comm_len;
                        }
                    }
                    break;
                default :
                    return EILSEQ;
                }
            }
        }

        table_info_local.push_back(column_info_tmp);
    }

    if (str_cur_p != (str_data + str_size))
        return EILSEQ;

    this->table_info = table_info_local;

    return 0;
}

bool
DM_IB_TableInfo::push_back(const std::string &column_name,
    const uint8_t column_type, const uint16_t column_scale,
    const uint16_t column_flags, const std::string &column_comment)
{
    if (this->table_info.size() >= UINT16_MAX)
        return false;

    if (column_type > UINT8_C(0x7F))
        return false;

    switch (column_type) {
    case DM_IB_TABLEINFO_CT_INT :
    case DM_IB_TABLEINFO_CT_BIGINT :
    case DM_IB_TABLEINFO_CT_CHAR :
    case DM_IB_TABLEINFO_CT_VARCHAR :
    case DM_IB_TABLEINFO_CT_FLOAT :
    case DM_IB_TABLEINFO_CT_DOUBLE :
    case DM_IB_TABLEINFO_CT_DATETIME :
        break;
    default :
        return false;
    }

    column_info_t column_info;
    column_info.column_name = column_name.substr(0, UINT8_MAX);
    column_info.column_type = column_type;
    column_info.column_scale = column_scale;
    column_info.column_flags = column_flags;
    column_info.column_comment = column_comment;
    this->table_info.push_back(column_info);

    return true;
}

std::string
DM_IB_TableInfo::Print(ssize_t col_id) const
{
    std::string ret;

    if (col_id >= 0) {
        ret = this->print_one(col_id);
    } else {
        for (size_t i=0; i< this->table_info.size(); i++) {
            if (i == 0) {
                ret = this->print_one(i);
            } else {
                ret.append(", ");
                ret.append(this->print_one(i));
            }
        }
    }

    return ret;
}

std::string
DM_IB_TableInfo::print_one(size_t col_id) const
{
    std::string ret;

    if (col_id < this->table_info.size()) {
        const column_info_t &column_info = this->table_info[col_id];
        char type_buf[64];
        ret = "`";
        ret.append(column_info.column_name);
        ret.append("`");
        switch (column_info.column_type) {
        case DM_IB_TABLEINFO_CT_INT :
            ret.append(" INT");
            break;
        case DM_IB_TABLEINFO_CT_BIGINT :
            ret.append(" BIGINT");
            break;
        case DM_IB_TABLEINFO_CT_FLOAT :
            ret.append(" FLOAT");
            break;
        case DM_IB_TABLEINFO_CT_DOUBLE :
            ret.append(" DOUBLE");
            break;
        case DM_IB_TABLEINFO_CT_DATETIME :
            ret.append(" DATETIME");
            break;
        case DM_IB_TABLEINFO_CT_CHAR :
            snprintf(type_buf, sizeof(type_buf), " CHAR(%u)", column_info.column_scale);
            ret.append(type_buf);
            break;
        case DM_IB_TABLEINFO_CT_VARCHAR :
            snprintf(type_buf, sizeof(type_buf), " VARCHAR(%u)", column_info.column_scale);
            ret.append(type_buf);
            break;
        default :
            return CR_Class_NS::blank_string;
        }
        if (column_info.column_comment.size() > 0) {
            ret.append(" COMMENT '");
            ret.append(column_info.column_comment);
            ret.append("'");
        }
    }

    return ret;
}
