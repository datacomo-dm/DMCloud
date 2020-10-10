#include <cr_class/cr_class.h>

#include "riak-proxy.h"
#include "ibpack_s.h"

#include "IBCompress.h"

IBDataPack_S::IBDataPack_S(const std::string &packbuf)
{
    if(!msgPackS.ParseFromString(packbuf))
        throw std::string("Bad data for PackS");

    this->no_obj = msgPackS.no_obj() + 1;

    if (msgPackS.has_compressed_buf()) {
        uint32_t *nulls_tmp = (uint32_t *)malloc(8192);
		memset(nulls_tmp,0,8192);
        size_t data_tmp_size = msgPackS.data_full_byte_size();
        void *data_tmp = malloc(data_tmp_size);
        void *lens_tmp = malloc(65536 * 2);
        int fret;

        fret = ib_packs_decompress((const uint8_t *)msgPackS.compressed_buf().data(),
          msgPackS.compressed_buf().size(), this->no_obj, msgPackS.optimal_mode(), data_tmp,
          data_tmp_size, 2, lens_tmp, nulls_tmp, msgPackS.no_nulls());

        if (!fret) {
            msgPackS.set_nulls((char *)nulls_tmp, 8192);
            msgPackS.set_lens((char *)lens_tmp, (65536 * 2));
            msgPackS.set_data((char *)data_tmp, data_tmp_size);
            msgPackS.clear_compressed_buf();
        }
        free(data_tmp);
        free(nulls_tmp);
        free(lens_tmp);
		if(fret) 
           THROWF("Decode packS error");
     }

    const char *data_p = msgPackS.data().data();
    const uint8_t *null_map = (const uint8_t *)(msgPackS.nulls().data());
    size_t null_map_size = msgPackS.nulls().size();
    this->lens = (const uint16_t *)(msgPackS.lens().data());
    this->lens_count = msgPackS.lens().size()/sizeof(uint16_t);
    this->lines_p = new const char*[this->no_obj];

    for (uint32_t i=0; i< this->no_obj; i++) {
        if (null_map_size==0 || !CR_Class_NS::BMP_BITTEST(null_map, null_map_size, i)) {
            if (i >= lens_count) {
                this->lines_p[i] = NULL;
                if (cr_debug_level > 120) {
                    DPRINTF("str[%u] == NULL\n", i);
                }
            } else {
                this->lines_p[i] = data_p;
                data_p += lens[i];
                if (cr_debug_level > 120) {
                    std::string s_tmp;
                    s_tmp.assign(this->lines_p[i], lens[i]);
                    DPRINTF("str[%u] == \"%s\"\n", i, s_tmp.c_str());
                }
            }
        } else {
            this->lines_p[i] = NULL;
        }
    }
}

IBDataPack_S::~IBDataPack_S()
{
    delete [](this->lines_p);
}

int
IBDataPack_S::Save(std::string &packbuf, IBRiakDataPackQueryParam &msgIBQP)
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

    msg_has_exp = msgIBQP.has_expression();

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

        msgPackS.clear_cond_ret_line_no();

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

        std::string range_data;
        std::string range_lens;

        std::string cond_data;
        std::string cond_lens;

        for (uint32_t i=row_begin;i<=row_end;i++) {
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
                msgPackS.add_cond_ret_line_no(i);
                cond_data.append(this->lines_p[i], this->lens[i]);
                cond_lens.append((const char *)(&(this->lens[i])), sizeof(this->lens[i]));

                cond_value_count++;
            }
        }

        DPRINTF("\tCOND: cond_value_count == %u\n", cond_value_count);

        msgPackS.set_first_row(first_row);
        msgPackS.set_last_row(last_row);

        if(first_row >= 0) {
            msgPackS.set_cond_rowmap(&(cond_rowmap[CR_Class_NS::BMP_BITSLOT(first_row)]),
              CR_Class_NS::BMP_BITNSLOTS(last_row, first_row));

            if (msgPackS.cond_ret_line_no_size() > 0) {
                msgPackS.set_cond_data(cond_data);
                msgPackS.set_cond_lens(cond_lens);
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
            msgPackS.clear_range_nulls();
            msgPackS.clear_range_data();
            msgPackS.clear_range_lens();
            msgPackS.clear_cond_data();
            msgPackS.clear_cond_lens();
        } else {
            msgPackS.clear_nulls();
            msgPackS.clear_data();
            msgPackS.clear_lens();

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

            if (keep_cond_data) {
                size_t cond_line_data_size = msgPackS.cond_ret_line_no_size() * 4;

                if (msgPackS.has_cond_data()) {
                    cond_line_data_size += msgPackS.cond_data().size();
                    cond_line_data_size += msgPackS.cond_lens().size();
                }

                if (cond_line_data_size > msgPackS.cond_rowmap().size()) {
                    msgPackS.clear_cond_ret_line_no();
                } else {
                    msgPackS.clear_cond_rowmap();
                }
            } else {
                msgPackS.clear_cond_ret_line_no();
                msgPackS.clear_cond_data();
                msgPackS.clear_cond_lens();
            }
        }
    } else if(msgIBQP.has_first_row() && msgIBQP.first_row()>=0) { // just range query only
        DPRINTF("\tRANGE: row_begin == %u, row_end == %u\n", row_begin, row_end);

        size_t sum_cond_lens = 0;
        size_t start_cond_pos = 0;

        msgPackS.set_first_row(row_begin);
        msgPackS.set_last_row(row_end);

        for (uint32_t i=0; i<=row_end; i++) {
            //null row has a invalid lens[i]--not clear lens memory while loading?
            if(!lines_p[i])
                continue;
            if (i < row_begin) {
                start_cond_pos += this->lens[i];
            } else {
                sum_cond_lens += this->lens[i];
            }
        }

        if (msgPackS.data().size() >= (start_cond_pos + sum_cond_lens)) {
            msgPackS.set_range_data(msgPackS.data().data() + start_cond_pos, sum_cond_lens);
        } else if(msgPackS.data().size()!=0)
            throw "Out of data of PackS";

        if (msgPackS.lens().size() >= (sizeof(this->lens[0]) * (row_end + 1))) {
            msgPackS.set_range_lens((lens + row_begin), sizeof(this->lens[0]) * (row_end - row_begin + 1));
        } else {
            throw "Out of len of PackS";
        }

        if(msgPackS.has_nulls()) {
            uint32_t new_spos = (row_begin >> 3);
            uint32_t new_epos = (row_end >> 3);
            uint32_t new_nsize = new_epos - new_spos + 1;
            if (msgPackS.nulls().size() >= (new_spos + new_nsize)) {
                msgPackS.set_range_nulls(msgPackS.nulls().data() + new_spos, new_nsize);
            } else {
                throw "Out of nulls of PackS";
            }
        }

        msgPackS.clear_nulls();
        msgPackS.clear_data();
        msgPackS.clear_lens();
    }

    if (msgPackS.has_first_row()) {
        DPRINTFX(20, "\tmsgPackS.first_row()==%d\n", msgPackS.first_row());
    }
    if (msgPackS.has_last_row()) {
        DPRINTFX(20, "\tmsgPackS.last_row()==%d\n", msgPackS.last_row());
    }
    if (msgPackS.has_nulls()) {
        DPRINTFX(20, "\tmsgPackS.nulls().size()==%u\n", (unsigned)msgPackS.nulls().size());
    }
    if (msgPackS.has_data()) {
        DPRINTFX(20, "\tmsgPackS.data().size()==%u\n", (unsigned)msgPackS.data().size());
    }

    msgPackS.SerializeToString(&packbuf);

    return ret;
}

bool
IBDataPack_S::_cond_cmp(const uint16_t pos, const IBRiakDataPackQueryParam &msgIBQP)
{
    if (!(this->lines_p[pos]))
        return false;

    bool ret = false;
    size_t str_value_count = msgIBQP.expression().value_list_str_size();
    std::string value_str(this->lines_p[pos], this->lens[pos]);

    switch (msgIBQP.expression().op_type()) {
    case IBRiakDataPackQueryParam::EOP_EQ :
        if (str_value_count == 1) {
            if (value_str == msgIBQP.expression().value_list_str(0)) {
                ret = true;
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_NE :
        if (str_value_count == 1) {
            if (value_str != msgIBQP.expression().value_list_str(0)) {
                ret = true;
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_GT :
        if (str_value_count == 1) {
            if (value_str > msgIBQP.expression().value_list_str(0)) {
                ret = true;
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_LT :
        if (str_value_count == 1) {
            if (value_str < msgIBQP.expression().value_list_str(0)) {
                ret = true;
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_GE :
        if (str_value_count == 1) {
            if (value_str >= msgIBQP.expression().value_list_str(0)) {
                ret = true;
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_LE :
        if (str_value_count == 1) {
            if (value_str <= msgIBQP.expression().value_list_str(0)) {
                ret = true;
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_BT :
        if (str_value_count == 2) {
            if ((value_str >= msgIBQP.expression().value_list_str(0))
            	&& (value_str <= msgIBQP.expression().value_list_str(1))) {
                ret = true;
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_NBT :
        if (str_value_count == 2) {
            if ((value_str < msgIBQP.expression().value_list_str(0))
            	|| (value_str > msgIBQP.expression().value_list_str(1))) {
                ret = true;
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_LK :
        if (str_value_count == 1) {
            if (CR_Class_NS::str_like_cmp(value_str, msgIBQP.expression().value_list_str(0))) {
                ret = true;
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_NLK :
        if (str_value_count == 1) {
            if (!CR_Class_NS::str_like_cmp(value_str, msgIBQP.expression().value_list_str(0))) {
                ret = true;
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_IN :
        if (str_value_count >= 1) {
            for (size_t i=0; i<str_value_count; i++) {
                if (value_str == msgIBQP.expression().value_list_str(i)) {
                    ret = true;
                    break;
                }
            }
        }
        break;
    case IBRiakDataPackQueryParam::EOP_NIN :
        if (str_value_count >= 1) {
            for (size_t i=0; i<str_value_count; i++) {
                if (value_str == msgIBQP.expression().value_list_str(i)) {
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
        if (str_value_count >= 1) {
            DPRINTFX(100, "\t\tpos==%u, value_s==\"%s\"\n", (unsigned)pos, value_str.c_str());
        }
    }

    return ret;
}

bool IBDataPack_S::_keep_full_data_pic(const IBRiakDataPackQueryParam &msgIBQP, const uint32_t cond_value_count)
{
    bool ret = false;

    if (cond_value_count >= (this->no_obj * 3 / 10))
        ret = true;

    return ret;
}
