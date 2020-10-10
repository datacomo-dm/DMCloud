#include <errno.h>
#include <cr_class/cr_class.h>

#include "ibpack_n.h"

#include "IBCompress.h"

IBDataPack_N::IBDataPack_N(const std::string &packbuf)
{
    if(!msgPackN.ParseFromString(packbuf))
        throw std::string("Bad data for PackS");

    this->no_obj = msgPackN.no_obj() + 1;
    this->value_type = this->ChooseValueType(this->CalculateByteSize(msgPackN.max_val()));

    if (msgPackN.has_compressed_buf()) {
        uint32_t *nulls_tmp = (uint32_t *)malloc(8192);
		memset(nulls_tmp,0,8192);
        size_t data_tmp_size = this->value_type * this->no_obj;
        void *data_tmp = malloc(data_tmp_size);
        int fret;

        fret = ib_packn_decompress((const uint8_t *)msgPackN.compressed_buf().data(),
          msgPackN.compressed_buf().size(), msgPackN.optimal_mode(), nulls_tmp,
          msgPackN.no_nulls(), data_tmp, this->no_obj);

        if (!fret) {
            if (data_tmp_size > 0) {
                msgPackN.set_data((char *)data_tmp, data_tmp_size);
            }
            if (msgPackN.no_nulls() > 0) {
                msgPackN.set_nulls((char *)nulls_tmp, 8192);
            }
            msgPackN.clear_compressed_buf();
        }

        free(data_tmp);
        free(nulls_tmp);
		if(fret) 
           THROWF("Decode packN error");
    }

    if (msgPackN.has_data()) {
        this->data_ptr = msgPackN.data().data();
        this->data_size = msgPackN.data().size();
    } else {
        this->data_ptr = NULL;
        this->data_size = 0;
    }

    if (msgPackN.has_nulls()) {
        this->nulls_ptr = (const uint8_t *)msgPackN.nulls().data();
        this->nulls_size = msgPackN.nulls().size();
    } else {
        this->nulls_ptr = NULL;
        this->nulls_size = 0;
    }

    DPRINTFX(50, "msgPackN.data().size() == %ld\n", this->data_size);
    DPRINTFX(50, "this->no_obj == %d\n", this->no_obj);
    DPRINTFX(50, "msgPackN.no_nulls() == %u\n", msgPackN.no_nulls());
    DPRINTFX(50, "msgPackN.max_val() == %lu\n", msgPackN.max_val());
    DPRINTFX(50, "this->value_type == %d\n", this->value_type);
}

IBDataPack_N::~IBDataPack_N()
{
}

uint64_t
IBDataPack_N::get_value_unsigned(const uint16_t pos, const bool pack_is_float)
{
    if (pack_is_float)
        return this->get_value_double(pos, pack_is_float);

    uint64_t ret = 0;

    if ((pos * (unsigned)(this->value_type)) >= this->data_size)
        THROWF("ESPIPE");

    switch (this->value_type) {
    case UCHAR :
      {
        const uint8_t *u8_ptr = (const uint8_t *)(this->data_ptr);
        ret = u8_ptr[pos];
        break;
      }
    case USHORT :
      {
        const uint16_t *u16_ptr = (const uint16_t *)(this->data_ptr);
        ret = u16_ptr[pos];
        break;
      }
    case UINT :
      {
        const uint32_t *u32_ptr = (const uint32_t *)(this->data_ptr);
        ret = u32_ptr[pos];
        break;
      }
    case UINT64 :
      {
        const uint64_t *u64_ptr = (const uint64_t *)(this->data_ptr);
        ret = u64_ptr[pos];
        break;
      }
    }

    return ret;
}

int64_t
IBDataPack_N::get_value_signed(const uint16_t pos, const bool pack_is_float)
{
    if (pack_is_float)
        return this->get_value_double(pos, pack_is_float);

    int64_t ret = 0;

    if ((pos * (unsigned)(this->value_type)) >= this->data_size)
        THROWF("ESPIPE");

    switch (this->value_type) {
    case UCHAR :
      {
        const int8_t *s8_ptr = (const int8_t *)(this->data_ptr);
        ret = s8_ptr[pos];
        break;
      }
    case USHORT :
      {
        const int16_t *s16_ptr = (const int16_t *)(this->data_ptr);
        ret = s16_ptr[pos];
        break;
      }
    case UINT :
      {
        const int32_t *s32_ptr = (const int32_t *)(this->data_ptr);
        ret = s32_ptr[pos];
        break;
      }
    case UINT64 :
      {
        const int64_t *s64_ptr = (const int64_t *)(this->data_ptr);
        ret = s64_ptr[pos];
        break;
      }
    }

    return ret;
}

double
IBDataPack_N::get_value_double(const uint16_t pos, const bool pack_is_float)
{
    if (!pack_is_float)
        return this->get_value_signed(pos, pack_is_float);

    double ret = 0;

    if ((pos * sizeof(double)) >= this->data_size)
        THROWF("ESPIPE");

    const double *double_ptr = (const double *)(this->data_ptr);
    ret = double_ptr[pos];

    return ret;
}

inline int
IBDataPack_N::CalculateByteSize(uint64_t n) const
{
    int res = 0;
    while(n != 0) {
        n >>= 8;
        res++;
    }
    return res;
}

inline IBDataPack_N::ValueType
IBDataPack_N::ChooseValueType(const int bytesize) const
{
    if(bytesize <= 1)
         return UCHAR;
    else if(bytesize <= 2)
         return USHORT;
    else if(bytesize <= 4)
         return UINT;
    else
         return UINT64;
}

int
IBDataPack_N::Save(std::string &packbuf, IBRiakDataPackQueryParam &msgIBQP)
{
    int ret = 0;
    uint16_t row_begin = 0;
    uint16_t row_end = no_obj-1;
    bool msg_has_exp = false;

    if (CR_Class_NS::get_debug_level() >= 20) {
        IBRiakDataPackQueryParam msgIBQP_tmp = msgIBQP;
        msgIBQP_tmp.clear_cond_rowmap_in();
        DPRINTF("msgIBQP.ShortDebugString() = %s\n", msgIBQP_tmp.ShortDebugString().c_str());
    }

    msg_has_exp =msgIBQP.has_expression();

    if (msgIBQP.has_first_row() && msgIBQP.has_last_row()) {
        row_begin = msgIBQP.first_row();
      	row_end = msgIBQP.last_row();
        if (row_end < row_begin)
            THROWF("row_end == %u, row_begin == %u\n", row_end, row_begin);
    }

    // if not query expression, but both range query and cond_rowmap_in used, cond_rowmap_in first
    if (msg_has_exp || msgIBQP.has_cond_rowmap_in()) {
        int first_row = -1;
        int last_row = -1;
        uint8_t cond_rowmap[8192];
        const uint8_t *cond_rowmap_in = NULL;
        size_t cond_rowmap_in_size=0;
        bool cond_rowmap_or = false;
        uint32_t cond_value_count = 0;
        bool keep_full_data = false;
        bool keep_cond_data = true;

        msgPackN.clear_cond_ret_line_no();

        if (msgIBQP.has_cond_rowmap_or()) {
            cond_rowmap_or = msgIBQP.cond_rowmap_or();
                if(!msg_has_exp)
                    THROWF("cond_rowmap_or MUST used with query expression");
        }

        if (msgIBQP.has_cond_rowmap_in()) {
            cond_rowmap_in_size = msgIBQP.cond_rowmap_in().size();
            cond_rowmap_in = (const uint8_t *)msgIBQP.cond_rowmap_in().data();
        }

        memset(cond_rowmap, 0, sizeof(cond_rowmap));

        std::string cond_data;

        for (uint32_t i=row_begin;i<=row_end;i++) {
            if (CR_Class_NS::BMP_BITTEST(this->nulls_ptr, this->nulls_size, i)) {
                 continue;
            }

            bool row_ok = false;

            if (cond_rowmap_or) {
                if (CR_Class_NS::BMP_BITTEST(cond_rowmap_in, cond_rowmap_in_size, i, row_begin)
                  || (this->_cond_cmp(i, msgIBQP))) {
                    row_ok = true;
                }
            } else {
                if (cond_rowmap_in) {
                    if (CR_Class_NS::BMP_BITTEST(cond_rowmap_in, cond_rowmap_in_size, i, row_begin)
                      && (!msg_has_exp || this->_cond_cmp(i, msgIBQP))) {
                        row_ok = true;
                    }
                } else if (this->_cond_cmp(i, msgIBQP)) {
                    row_ok = true;
                }
            }

            if (row_ok) {
                if (first_row < 0) {
                    first_row = i;
                }
                last_row = i;

                CR_Class_NS::BMP_BITSET(cond_rowmap, sizeof(cond_rowmap), i);
                msgPackN.add_cond_ret_line_no(i);
                if (this->data_size > 0) {
                    cond_data.append(this->data_ptr + (i * this->value_type), this->value_type);
                }

                cond_value_count++;
            }
        }

        DPRINTF("\tCOND: cond_value_count == %u\n", cond_value_count);

        msgPackN.set_first_row(first_row);
        msgPackN.set_last_row(last_row);

        if(first_row >= 0) {
            msgPackN.set_cond_rowmap(&(cond_rowmap[CR_Class_NS::BMP_BITSLOT(first_row)]),
              CR_Class_NS::BMP_BITNSLOTS(last_row, first_row));

            if (msgPackN.cond_ret_line_no_size() > 0) {
                msgPackN.set_cond_data(cond_data);
            }
        }

        if (!msgIBQP.has_full_data_req_level()) {
            keep_full_data = false;
        } else {
            switch (msgIBQP.full_data_req_level()) {
            case IBRiakDataPackQueryParam::RL_AUTO :
                keep_full_data = this->_keep_full_data_pic(msgIBQP, cond_value_count);
                break;
            case IBRiakDataPackQueryParam::RL_NEED :
                keep_full_data = true;
                break;
            case IBRiakDataPackQueryParam::RL_NEVER :
                keep_full_data = false;
                break;
            default :
                break;
            }
        }

        if (keep_full_data) {
            msgPackN.clear_range_nulls();
            msgPackN.clear_range_data();
            msgPackN.clear_cond_data();
        } else {
            msgPackN.clear_nulls();
            msgPackN.clear_data();

            if (!msgIBQP.has_cond_data_req_level()) {
                keep_cond_data = true;
            } else {
                switch (msgIBQP.cond_data_req_level()) {
                case IBRiakDataPackQueryParam::RL_NEED :
                    keep_cond_data = true;
                    break;
                case IBRiakDataPackQueryParam::RL_NEVER :
                    keep_cond_data = false;
                    break;
                default :
                    break;
                }
            }

            if (keep_cond_data && first_row >= 0) {
                size_t cond_line_data_size = msgPackN.cond_ret_line_no_size() * 4;

                if (msgPackN.has_cond_data()) {
                    cond_line_data_size += msgPackN.cond_data().size();
                }

                if (cond_line_data_size > msgPackN.cond_rowmap().size()) {
                    msgPackN.clear_cond_ret_line_no();
                } else {
                    msgPackN.clear_cond_rowmap();
		}
            } else {
                msgPackN.clear_cond_ret_line_no();
                msgPackN.clear_cond_data();
            }
        }
    } else if (msgIBQP.has_first_row() && msgIBQP.first_row()>=0){ // just range query only
        DPRINTF("\tRANGE: row_begin == %u, row_end == %u\n", row_begin, row_end);

        msgPackN.set_first_row(row_begin);
        msgPackN.set_last_row(row_end);

        uint32_t byte_spos = row_begin * value_type;
        uint32_t byte_epos = row_end * value_type;
        uint32_t byte_size = byte_epos - byte_spos + value_type;

        if (this->data_size >= (byte_spos + byte_size)) {
            msgPackN.set_range_data(this->data_ptr + byte_spos, byte_size);
        } else if((long)msgPackN.max_val()!=(long)msgIBQP.pack_local_min_num() || !msgPackN.has_nulls())
            throw "Out of data of PackN";

        if (msgPackN.has_nulls() && (msgPackN.nulls().size() > 0)) {
            uint32_t new_spos = (row_begin >> 3);
            uint32_t new_epos = (row_end >> 3);
            uint32_t new_nsize = new_epos - new_spos + 1;
            if (msgPackN.nulls().size() >= (new_spos + new_nsize)) {
                msgPackN.set_range_nulls(msgPackN.nulls().data() + new_spos, new_nsize);
            } else {
                throw "Out of nulls of PackN";
            }
        }

        msgPackN.clear_nulls();
        msgPackN.clear_data();
    }

    if (msgPackN.has_first_row()) {
        DPRINTFX(20, "\tmsgPackN.first_row()==%d\n", msgPackN.first_row());
    }
    if (msgPackN.has_last_row()) {
        DPRINTFX(20, "\tmsgPackN.last_row()==%d\n", msgPackN.last_row());
    }
    if (msgPackN.has_nulls()) {
        DPRINTFX(20, "\tmsgPackN.nulls().size()==%lu\n", msgPackN.nulls().size());
    }
    if (msgPackN.has_data()) {
        DPRINTFX(20, "\tmsgPackN.data().size()==%lu\n", msgPackN.data().size());
    }

    msgPackN.SerializeToString(&packbuf);

    return ret;
}

bool
IBDataPack_N::_cond_cmp(const uint16_t pos, const IBRiakDataPackQueryParam &msgIBQP)
{
    bool ret = false;
    bool pack_is_float = false;
    int64_t value_s;
    double value_f;
    int64_t value_local_min = 0;
    size_t num_value_count = msgIBQP.expression().value_list_num_size();
    size_t float_value_count = msgIBQP.expression().value_list_flt_size();

    if (msgIBQP.pack_type() == IBRiakDataPackQueryParam::PACK_FLOAT)
        pack_is_float = true;

    if (msgIBQP.has_pack_local_min_num()) {
        value_local_min = msgIBQP.pack_local_min_num();
    }

    value_s = this->get_value_unsigned(pos, pack_is_float);
    DPRINTFX(120, "orig value_s[%u] = %ld\n", pos, value_s);
    value_s += value_local_min;
    DPRINTFX(120, "cur value_s[%u] = %ld\n", pos, value_s);
    value_f = this->get_value_double(pos, pack_is_float);

    switch (msgIBQP.expression().op_type()) {
    case IBRiakDataPackQueryParam::EOP_EQ :
        if (num_value_count == 1) {
            if (value_s == msgIBQP.expression().value_list_num(0)) {
                ret = true;
            }
        } else if (float_value_count == 1) {
            if (value_f == msgIBQP.expression().value_list_flt(0)) {
                ret = true;
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_NE :
        if (num_value_count == 1) {
            if (value_s != msgIBQP.expression().value_list_num(0)) {
                ret = true;
            }
        } else if (float_value_count == 1) {
            if (value_f != msgIBQP.expression().value_list_flt(0)) {
                ret = true;
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_GT :
        if (num_value_count == 1) {
            if (value_s > msgIBQP.expression().value_list_num(0)) {
                ret = true;
            }
        } else if (float_value_count == 1) {
            if (value_f > msgIBQP.expression().value_list_flt(0)) {
                ret = true;
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_LT :
        if (num_value_count == 1) {
            if (value_s < msgIBQP.expression().value_list_num(0)) {
                ret = true;
            }
        } else if (float_value_count == 1) {
            if (value_f < msgIBQP.expression().value_list_flt(0)) {
                ret = true;
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_GE :
        if (num_value_count == 1) {
            if (value_s >= msgIBQP.expression().value_list_num(0)) {
                ret = true;
            }
        } else if (float_value_count == 1) {
            if (value_f >= msgIBQP.expression().value_list_flt(0)) {
                ret = true;
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_LE :
        if (num_value_count == 1) {
            if (value_s <= msgIBQP.expression().value_list_num(0)) {
                ret = true;
            }
        } else if (float_value_count == 1) {
            if (value_f <= msgIBQP.expression().value_list_flt(0)) {
                ret = true;
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_BT :
        if (num_value_count == 2) {
            if ((value_s >= msgIBQP.expression().value_list_num(0))
            	&& (value_s <= msgIBQP.expression().value_list_num(1))) {
                ret = true;
            }
        } else if (float_value_count == 1) {
            if ((value_f >= msgIBQP.expression().value_list_flt(0))
            	&& (value_f <= msgIBQP.expression().value_list_flt(1))) {
                ret = true;
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_NBT :
        if (num_value_count == 2) {
            if ((value_s < msgIBQP.expression().value_list_num(0))
            	|| (value_s > msgIBQP.expression().value_list_num(1))) {
                ret = true;
            }
        } else if (float_value_count == 1) {
            if ((value_f < msgIBQP.expression().value_list_flt(0))
            	|| (value_f > msgIBQP.expression().value_list_flt(1))) {
                ret = true;
            }
        }
        break;
//    case IBRiakDataPackQueryParam::EOP_LK :
//        break;
//    case IBRiakDataPackQueryParam::EOP_NLK :
//        break;
    case IBRiakDataPackQueryParam::EOP_IN :
        if (num_value_count >= 1) {
            for (size_t i=0; i<num_value_count; i++) {
                if (value_s == msgIBQP.expression().value_list_num(i)) {
                    ret = true;
                    break;
                }
            }
        } else if (float_value_count >= 1) {
            for (size_t i=0; i<float_value_count; i++) {
                if (value_f == msgIBQP.expression().value_list_flt(i)) {
                    ret = true;
                    break;
                }
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_NIN :
        if (num_value_count >= 1) {
            for (size_t i=0; i<num_value_count; i++) {
                if (value_s == msgIBQP.expression().value_list_num(i)) {
                    ret = false;
                    break;
                }
            }
        } else if (float_value_count >= 1) {
            for (size_t i=0; i<float_value_count; i++) {
                if (value_f == msgIBQP.expression().value_list_flt(i)) {
                    ret = false;
                    break;
                }
            }
        }
        break;
    default :
        break;
    }

    if (ret) {
        if (num_value_count >= 1) {
            DPRINTFX(100, "\t\tpos==%u, value_s==%ld\n", (unsigned)pos, value_s);
        } else if (float_value_count >= 1) {
            DPRINTFX(100, "\t\tpos==%u, value_f==%f\n", (unsigned)pos, value_f);
        }
    }

    return ret;
}

bool IBDataPack_N::_keep_full_data_pic(const IBRiakDataPackQueryParam &msgIBQP, const uint32_t cond_value_count)
{
    bool ret = false;

    if (cond_value_count >= (this->no_obj * 3 / 10))
        ret = true;

    return ret;
}
