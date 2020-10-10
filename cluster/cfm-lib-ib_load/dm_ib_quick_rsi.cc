#include <cr_class/cr_addon.h>
#include <cfm_drv/dm_ib_sort_load.h>
#include <cfm_drv/dm_ib_tableinfo.h>
#include "IBFile.h"
#include "RSI_interface.h"
#include "dm_ib_quick_dpn.h"
#include "dm_ib_quick_rsi.h"

DM_IB_QuickRSI::DM_IB_QuickRSI()
{
    this->_column_type = DM_IB_TABLEINFO_CT_VARCHAR;
    this->_rsi_handle = NULL;
    this->_lineid_begin = INT64_MAX;
    this->_lineid_end = INT64_MIN;
}

DM_IB_QuickRSI::~DM_IB_QuickRSI()
{
    if (this->_rsi_handle) {
        switch (this->_column_type) {
        case DM_IB_TABLEINFO_CT_INT :
        case DM_IB_TABLEINFO_CT_BIGINT :
        case DM_IB_TABLEINFO_CT_DATETIME :
        case DM_IB_TABLEINFO_CT_FLOAT :
        case DM_IB_TABLEINFO_CT_DOUBLE :
          {
            ib_rsi::IRSI_Hist *ris_hist_p = (ib_rsi::IRSI_Hist*)(this->_rsi_handle);
            delete ris_hist_p;
            break;
          }
        default :
          {
            ib_rsi::IRSI_CMap *ris_cmap_p = (ib_rsi::IRSI_CMap*)(this->_rsi_handle);
            delete ris_cmap_p;
            break;
          }
        }
    }
}

void
DM_IB_QuickRSI::SetArgs(const uint8_t column_type, const size_t column_scale,
    const int64_t lineid_begin, const int64_t lineid_end)
{
    if (!this->_rsi_handle) {
        int64_t packid_begin = lineid_begin / DM_IB_MAX_BLOCKLINES;
        int64_t packid_end = lineid_end / DM_IB_MAX_BLOCKLINES;
        int64_t pack_count = packid_end - packid_begin + 1;
        int64_t create_size = pack_count * DM_IB_MAX_BLOCKLINES;

        switch (column_type) {
        case DM_IB_TABLEINFO_CT_INT :
        case DM_IB_TABLEINFO_CT_BIGINT :
        case DM_IB_TABLEINFO_CT_DATETIME :
            this->_rsi_handle = ib_rsi::create_rsi_hist();
            ((ib_rsi::IRSI_Hist*)(this->_rsi_handle))->Create(create_size, true);
            break;
        case DM_IB_TABLEINFO_CT_FLOAT :
        case DM_IB_TABLEINFO_CT_DOUBLE :
            this->_rsi_handle = ib_rsi::create_rsi_hist();
            ((ib_rsi::IRSI_Hist*)(this->_rsi_handle))->Create(create_size, false);
            break;
        default :
            this->_rsi_handle = ib_rsi::create_rsi_cmap();
            ((ib_rsi::IRSI_CMap*)(this->_rsi_handle))->Create(create_size,
              (column_scale > 64)?(64):(column_scale));
            break;
        }

        this->_column_type = column_type;
        this->_lineid_begin = lineid_begin;
        this->_lineid_end = lineid_end;
        this->_packid_begin = packid_begin;
    }
}

int
DM_IB_QuickRSI::PutValueN(const int64_t lineid, const int64_t v, const int64_t minv, const int64_t maxv)
{
    if ((lineid < this->_lineid_begin) || (lineid > this->_lineid_end))
        return ERANGE;

    int64_t packid = lineid / DM_IB_MAX_BLOCKLINES;
    int64_t packid_local = packid - this->_packid_begin;

    try {
        ((ib_rsi::IRSI_Hist*)(this->_rsi_handle))->PutValue(v, packid_local, minv, maxv);
    } catch (...) {
        DPRINTF("line %lld failed\n", (long long)lineid);
        return EINVAL;
    }

    return 0;
}

int
DM_IB_QuickRSI::PutValueS(const int64_t lineid, const char *v, const size_t vlen, const size_t prefix_len)
{
    if ((lineid < this->_lineid_begin) || (lineid > this->_lineid_end))
        return ERANGE;

    int64_t packid = lineid / DM_IB_MAX_BLOCKLINES;
    int64_t packid_local = packid - this->_packid_begin;
    size_t vlen_rsi;

    if (vlen > prefix_len) {
        vlen_rsi = vlen - prefix_len;
    } else {
        vlen_rsi = 0;
    }

    try {
        ((ib_rsi::IRSI_CMap*)(this->_rsi_handle))->PutValue(v + prefix_len, vlen_rsi, packid_local);
    } catch (...) {
        DPRINTF("line %lld failed\n", (long long)lineid);
        return EINVAL;
    }

    return 0;
}

int
DM_IB_QuickRSI::Save(int fd)
{
    if (!this->_rsi_handle)
        return EFAULT;
    ib_rsi::IBFile ib_file(fd);
    return ((ib_rsi::IRSIndex*)(this->_rsi_handle))->Save(&ib_file,0);
}
