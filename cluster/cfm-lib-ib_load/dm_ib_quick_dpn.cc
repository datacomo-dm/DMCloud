#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <values.h>
#include <string>
#include <cr_class/cr_file.h>
#include <cfm_drv/dm_ib_sort_load.h>
#include <cfm_drv/dm_ib_tableinfo.h>
#include "dm_ib_quick_dpn.h"

#define _DMIB_BUF_SIZE		(1024 * 1024)

/////////////////////////////

DM_IB_QuickDPN::QDPN::QDPN()
{
    this->pack_file = QUICKDPN_PF_NO_OBJ;
    this->pack_addr = 0;
    this->no_objs = 0;
    this->no_nulls = 0;
}

DM_IB_QuickDPN::QDPN::~QDPN()
{
}

void
DM_IB_QuickDPN::QDPN::SetArgs(const uint8_t column_type)
{
    switch (column_type) {
    case DM_IB_TABLEINFO_CT_INT :
    case DM_IB_TABLEINFO_CT_BIGINT :
    case DM_IB_TABLEINFO_CT_DATETIME :
        this->min_v.int_v = INT64_MAX;
        this->max_v.int_v = INT64_MIN;
        this->sum_v.int_v = 0;
        break;
    case DM_IB_TABLEINFO_CT_FLOAT :
    case DM_IB_TABLEINFO_CT_DOUBLE :
      {
        this->min_v.float_v = DBL_MAX;
        this->max_v.float_v = DBL_MAX * -1;
        this->sum_v.float_v = 0.0;
        break;
      }
    case DM_IB_TABLEINFO_CT_CHAR :
    case DM_IB_TABLEINFO_CT_VARCHAR :
        this->min_v.uint_v = ~(UINT64_C(0));
        this->max_v.uint_v = 0;
        this->sum_v.uint_v = 0;
        break;
    default :
        throw EPROTOTYPE;
    }
}

void
DM_IB_QuickDPN::QDPN::PutNull()
{
    this->no_nulls++;
    this->no_objs++;
}

void
DM_IB_QuickDPN::QDPN::PutValueI(const int64_t v)
{
    if (this->pack_file == QUICKDPN_PF_NO_OBJ) {
        this->pack_file = 0;
        this->min_v.int_v = v;
        this->max_v.int_v = v;
    } else {
        if (this->min_v.int_v > v)
            this->min_v.int_v = v;
        if (this->max_v.int_v < v)
            this->max_v.int_v = v;
    }
    this->sum_v.int_v += v;
    this->no_objs++;
}

void
DM_IB_QuickDPN::QDPN::PutValueD(const double v)
{
    if (this->pack_file == QUICKDPN_PF_NO_OBJ) {
        this->pack_file = 0;
        this->min_v.float_v = v;
        this->max_v.float_v = v;
    } else {
        if (this->min_v.float_v > v)
            this->min_v.float_v = v;
        if (this->max_v.float_v < v)
            this->max_v.float_v = v;
    }
    this->sum_v.float_v += v;
    this->no_objs++;
}

void
DM_IB_QuickDPN::QDPN::PutValueS(const char *v, const size_t vlen)
{
    const size_t vlen_local = MIN(vlen, sizeof(CR_Class_NS::union64_t));
    if (this->pack_file == QUICKDPN_PF_NO_OBJ) {
        this->pack_file = 0;
        this->min_v.uint_v = 0;
        this->max_v.uint_v = ~(UINT64_C(0));
        strncpy(this->min_v.str_v, v, vlen_local);
        strncpy(this->max_v.str_v, v, vlen_local);
    } else {
        if (strncmp(this->min_v.str_v, v, vlen_local) > 0)
            strncpy(this->min_v.str_v, v, vlen_local);
        if (strncmp(this->max_v.str_v, v, vlen_local) < 0)
            strncpy(this->max_v.str_v, v, vlen_local);
    }
    if (this->sum_v.uint_v < vlen)
        this->sum_v.uint_v = vlen;
    this->no_objs++;
}

size_t
DM_IB_QuickDPN::QDPN::GetPrefixLen()
{
    if(this->no_objs == this->no_nulls)
        return 0;

    size_t dif_pos = 0;

    for(; ( dif_pos < sizeof(CR_Class_NS::union64_t) )
        && this->min_v.str_v[dif_pos]
        && this->max_v.str_v[dif_pos]
        && ( this->min_v.str_v[dif_pos] == this->max_v.str_v[dif_pos] ); ++ dif_pos )
    ;

    return dif_pos;
}

void
DM_IB_QuickDPN::QDPN::AppendToString(std::string &s) const
{
    char buf[37];
    int32_t pack_file_tmp;
    uint16_t no_nulls_tmp;
    uint16_t no_objs_tmp = (uint16_t)(this->no_objs - 1);
    if(this->no_nulls == this->no_objs) {
        pack_file_tmp = QUICKDPN_PF_NULLS_ONLY;
        no_nulls_tmp = 0;
    } else {
        pack_file_tmp = this->pack_file;
        no_nulls_tmp = this->no_nulls;
    }
    memcpy(&(buf[0]), &pack_file_tmp, 4);
    memcpy(&(buf[4]), &this->pack_addr, 4);
    memcpy(&(buf[8]), &this->min_v, 8);
    memcpy(&(buf[16]), &this->max_v, 8);
    memcpy(&(buf[24]), &this->sum_v, 8);
    memcpy(&(buf[32]), &no_objs_tmp, 2);
    memcpy(&(buf[34]), &no_nulls_tmp, 2);
    buf[36] = 0;

    s.append(buf, sizeof(buf));
}

const char *
DM_IB_QuickDPN::QDPN::ReadFromBuf(const char *buf)
{
    uint16_t no_objs_tmp;

    memcpy(&this->pack_file, &(buf[0]), 4);
    memcpy(&this->pack_addr, &(buf[4]), 4);
    memcpy(&this->min_v, &(buf[8]), 8);
    memcpy(&this->max_v, &(buf[16]), 8);
    memcpy(&this->sum_v, &(buf[24]), 8);
    memcpy(&no_objs_tmp, &(buf[32]), 2);
    this->no_objs = (uint32_t)no_objs_tmp + 1;
    if (QUICKDPN_PF_NULLS_ONLY == this->pack_file) {
        this->no_nulls = this->no_objs;
    } else {
        memcpy(&this->no_nulls, &(buf[34]), 2);
    }

    return buf + 37;
}

/////////////////////////////

DM_IB_QuickDPN::DM_IB_QuickDPN()
{
    this->_lineid_begin = INT64_MAX;
    this->_lineid_end = INT64_MIN;
}

DM_IB_QuickDPN::~DM_IB_QuickDPN()
{
}

void
DM_IB_QuickDPN::SetArgs(const uint8_t column_type, const int64_t lineid_begin, const int64_t lineid_end)
{
    if (lineid_begin > lineid_end)
        return;
    if (this->_dpn_array.size() == 0) {
        int64_t packid_begin = lineid_begin / DM_IB_MAX_BLOCKLINES;
        int64_t packid_end = lineid_end / DM_IB_MAX_BLOCKLINES;
        int64_t pack_count = packid_end - packid_begin + 1;

        this->_lineid_begin = lineid_begin;
        this->_lineid_end = lineid_end;
        this->_packid_begin = packid_begin;
        this->_dpn_array.resize(pack_count);

        for (int64_t i=0; i<pack_count; i++) {
            this->_dpn_array[i].SetArgs(column_type);
        }
    }
}

DM_IB_QuickDPN::QDPN *
DM_IB_QuickDPN::LineToDPN(const int64_t lineid)
{
    if ((lineid < this->_lineid_begin) || (lineid > this->_lineid_end))
        return NULL;
    int64_t packid = lineid / DM_IB_MAX_BLOCKLINES;
    int64_t packid_local = packid - this->_packid_begin;
    if ((size_t)packid_local >= this->_dpn_array.size())
        return NULL;
    return &(this->_dpn_array[packid_local]);
}

int
DM_IB_QuickDPN::Save(int fd)
{
    if (fd < 0)
        return EBADF;
    ssize_t fret;
    std::string s;
    s.reserve(_DMIB_BUF_SIZE);
    CR_File::lseek(fd, 0, SEEK_SET);
    for (size_t i=0; i< this->_dpn_array.size(); i++) {
        if (s.size() >= (_DMIB_BUF_SIZE - sizeof(QDPN))) {
            fret = CR_File::write(fd, s.data(), s.size());
            if (fret < 0)
                return errno;
            s.clear();
            s.reserve(_DMIB_BUF_SIZE);
        }
        this->_dpn_array[i].AppendToString(s);
    }
    if (s.size() > 0) {
        fret = CR_File::write(fd, s.data(), s.size());
        if (fret < 0)
            return errno;
    }
    return 0;
}
