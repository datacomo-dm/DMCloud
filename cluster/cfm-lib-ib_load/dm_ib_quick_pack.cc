#include "dm_ib_quick_pack.h"
#include <IBCompress.h>
#include <stdlib.h>
#include <cr_class/md5.h>
#include <cr_class/cr_file.h>
#include <cr_class/cr_compress.h>
#include "msgIB.pb.h"

//////////////////////////

std::string _get_riak_key(const int64_t packno);

static int64_t _alloc_key(const std::string &job_id, CR_BlockQueue<int64_t> *_ks_pool_p,
    std::list<int64_t> *pack_id_list_p);

//////////////////////////

std::string
_get_riak_key(const int64_t packno)
{
    return CR_Class_NS::str_printf("P%llX-%X", (long long)(packno>>12),(int)(packno&0xfff));
}

//////////////////////////

DM_IB_QuickPack::DM_IB_QuickPack()
{
    this->_column_type = 0xFF;
    this->_compress_level = -1;
    this->_packs_data_full_byte_size = 0;
    this->_dpn_p = NULL;
    this->_riakp = NULL;
    this->_riak_handle = NULL;
    this->_ks_pool_p = NULL;
}

DM_IB_QuickPack::~DM_IB_QuickPack()
{
    this->clear();
}

void
DM_IB_QuickPack::clear()
{
    bzero(this->nulls, sizeof(this->nulls));
    this->_packn_data_array.clear();
    this->_packs_index_array.clear();
    this->_packs_lens_array.clear();
    this->_packs_data_full_byte_size = 0;
    this->_dpn_p = NULL;
    this->_rsis = NULL;
}

int
DM_IB_QuickPack::SetArgs(const std::string &job_id, const size_t column_id, const uint8_t column_type,
    const int compress_level, const int64_t lineid_begin, const int64_t lineid_end,
    DM_IB_QuickDPN::QDPN *dpn_p, DM_IB_QuickRSI &rsis, RiakCluster *riakp,
    const std::string &riak_bucket, void *riak_handle, CR_BlockQueue<int64_t> *ks_pool_p)
{
    if (lineid_begin > lineid_end)
        return EINVAL;

    int no_objs = lineid_end - lineid_begin + 1;
    if (no_objs > DM_IB_MAX_BLOCKLINES)
        return EMSGSIZE;

    switch (column_type) {
    case DM_IB_TABLEINFO_CT_INT :
    case DM_IB_TABLEINFO_CT_BIGINT :
    case DM_IB_TABLEINFO_CT_DATETIME :
    case DM_IB_TABLEINFO_CT_FLOAT :
    case DM_IB_TABLEINFO_CT_DOUBLE :
      {
        this->clear();
        this->_packn_data_array.resize(no_objs);
        ::bzero(this->_packn_data_array.data(), sizeof(CR_Class_NS::union64_t) * no_objs);
        break;
      }
    case DM_IB_TABLEINFO_CT_CHAR :
    case DM_IB_TABLEINFO_CT_VARCHAR :
      {
        this->clear();
        this->_packs_index_array.resize(no_objs);
        this->_packs_lens_array.resize(no_objs);
        break;
      }
    default :
        return EPROTOTYPE;
    }

    this->_job_id = job_id;
    this->_column_id = column_id;
    this->_column_type = column_type;
    this->_compress_level = compress_level;
    this->_lineid_begin = lineid_begin;
    this->_lineid_end = lineid_end;
    this->_dpn_p = dpn_p;
    this->_rsis = &rsis;
    this->_riakp = riakp;
    this->_riak_bucket = riak_bucket;
    this->_riak_handle = riak_handle;
    this->_ks_pool_p = ks_pool_p;

    return 0;
}

int
DM_IB_QuickPack::PutNull(const int64_t lineid)
{
#if defined(DIAGNOSTIC)
    if ((lineid < this->_lineid_begin) || (lineid > this->_lineid_end))
        return ERANGE;
#endif // DIAGNOSTIC
    CR_Class_NS::BMP_BITSET(this->nulls, sizeof(this->nulls), lineid - this->_lineid_begin);
    this->_dpn_p->PutNull();
    return 0;
}

int
DM_IB_QuickPack::PutValueI(const int64_t lineid, const int64_t v)
{
#if defined(DIAGNOSTIC)
    if ((lineid < this->_lineid_begin) || (lineid > this->_lineid_end))
        return ERANGE;
    switch (this->_column_type) {
    case DM_IB_TABLEINFO_CT_INT :
    case DM_IB_TABLEINFO_CT_BIGINT :
    case DM_IB_TABLEINFO_CT_DATETIME :
        break;
    default :
        return EPROTOTYPE;
    }
#endif // DIAGNOSTIC
    this->_packn_data_array[lineid - this->_lineid_begin].int_v = v;
    this->_dpn_p->PutValueI(v);
    return 0;
}

int
DM_IB_QuickPack::PutValueD(const int64_t lineid, const double v)
{
#if defined(DIAGNOSTIC)
    if ((lineid < this->_lineid_begin) || (lineid > this->_lineid_end))
        return ERANGE;
    switch (this->_column_type) {
    case DM_IB_TABLEINFO_CT_FLOAT :
    case DM_IB_TABLEINFO_CT_DOUBLE :
        break;
    default :
        return EPROTOTYPE;
    }
#endif // DIAGNOSTIC
    this->_packn_data_array[lineid - this->_lineid_begin].float_v = v;
    this->_dpn_p->PutValueD(v);
    return 0;
}

int
DM_IB_QuickPack::PutValueS(const int64_t lineid, const char *v, const size_t vlen)
{
#if defined(DIAGNOSTIC)
    if ((lineid < this->_lineid_begin) || (lineid > this->_lineid_end))
        return ERANGE;
    switch (this->_column_type) {
    case DM_IB_TABLEINFO_CT_CHAR :
    case DM_IB_TABLEINFO_CT_VARCHAR :
        break;
    default :
        return EPROTOTYPE;
    }
#endif // DIAGNOSTIC
    int64_t lineid_local = lineid - this->_lineid_begin;
    this->_packs_data_full_byte_size += vlen;
    this->_packs_index_array[lineid_local] = v;
    this->_packs_lens_array[lineid_local] = vlen;
    this->_dpn_p->PutValueS(v, vlen);
    return 0;
}

int
DM_IB_QuickPack::SavePack(std::list<int64_t> *pack_id_list_p, int fd,
    int file_id, CR_DataControl::Sem *file_sem_p)
{
    if ((!this->_riakp) && (fd < 0))
        return EINVAL;

    int32_t no_objs = this->_dpn_p->no_objs;
    int32_t no_nulls = this->_dpn_p->no_nulls;

    if ((no_objs == 0)
      || (no_objs == no_nulls)
      || ((this->_column_type != DM_IB_TABLEINFO_CT_CHAR)
       && (this->_column_type != DM_IB_TABLEINFO_CT_VARCHAR)
       && (no_nulls == 0)
       && (this->_dpn_p->min_v.uint_v == this->_dpn_p->max_v.uint_v)))
    {
        return 0;
    }

    int ret = 0;
    std::string pack_str;
    uint8_t optimal_mode = 0;
    uint8_t head_buf[32];
    int head_size = 0;
    off_t pack_addr = 0;

    if (!this->_riakp) {
        pack_addr = CR_File::lseek(fd, 0, SEEK_CUR);
        if (pack_addr < 0)
            return errno;
    }

    switch (this->_column_type) {
    case DM_IB_TABLEINFO_CT_INT :
    case DM_IB_TABLEINFO_CT_BIGINT :
    case DM_IB_TABLEINFO_CT_DATETIME :
      {
        int64_t local_min = this->_dpn_p->min_v.int_v;
        int64_t local_max = this->_dpn_p->max_v.int_v;
        uint64_t max_save_v = 0;
        void *data_full = NULL;
        size_t data_full_size = 0;

        for (int i=0; i<no_objs; i++) {
            int64_t &cur_val = this->_packn_data_array[i].int_v;
            if (!CR_Class_NS::BMP_BITTEST(this->nulls, sizeof(this->nulls), i))
                this->_rsis->PutValueN(this->_lineid_begin + i, cur_val, local_min, local_max);
            cur_val -= local_min;
            if (max_save_v < (uint64_t)cur_val)
                max_save_v = (uint64_t)cur_val;
        }

        if (max_save_v <= UINT8_MAX) {
            data_full_size = sizeof(uint8_t) * no_objs;
            data_full = ::malloc(data_full_size);
            for (int i=0; i<no_objs; i++)
                ((uint8_t*)data_full)[i] = this->_packn_data_array[i].uint_v;
        } else if (max_save_v <= UINT16_MAX) {
            data_full_size = sizeof(uint16_t) * no_objs;
            data_full = ::malloc(data_full_size);
            for (int i=0; i<no_objs; i++)
                ((uint16_t*)data_full)[i] = this->_packn_data_array[i].uint_v;
        } else if (max_save_v <= UINT32_MAX) {
            data_full_size = sizeof(uint32_t) * no_objs;
            data_full = ::malloc(data_full_size);
            for (int i=0; i<no_objs; i++)
                ((uint32_t*)data_full)[i] = this->_packn_data_array[i].uint_v;
        } else {
            data_full_size = sizeof(uint64_t) * no_objs;
            data_full = this->_packn_data_array.data();
        }

        if (!this->_riakp) {
            switch (this->_compress_level) {
            case CR_Compress::CT_ZLIB :
                ret = ib_packn_compress_zlib(this->nulls, no_nulls, data_full, no_objs, max_save_v,
                  pack_str, optimal_mode);
                break;
            case CR_Compress::CT_SNAPPY :
                ret = ib_packn_compress_snappy(this->nulls, no_nulls, data_full, no_objs, max_save_v,
                  pack_str, optimal_mode);
                break;
            case CR_Compress::CT_LZ4 :
                ret = ib_packn_compress_lz4(this->nulls, no_nulls, data_full, no_objs, max_save_v,
                  pack_str, optimal_mode);
                break;
            default :
                ret = ib_packn_compress(this->nulls, no_nulls, data_full, no_objs, max_save_v,
                  pack_str, optimal_mode);
                break;
            }
            if (ret) {
                DPRINTF("ib_packn_compress(%p, %d, %p, %d, %llu, ...) == %d\n",
                  this->nulls, no_nulls, data_full, no_objs,
                  (long long unsigned)max_save_v, ret);
            }
        } else {
            msgRCAttr_packN msg_pack;

            msg_pack.set_no_obj(no_objs - 1);
            msg_pack.set_no_nulls(no_nulls);
            msg_pack.set_optimal_mode(optimal_mode);
            msg_pack.set_max_val(max_save_v);
            msg_pack.set_total_size(data_full_size);
            msg_pack.set_nulls(this->nulls, sizeof(this->nulls));
            msg_pack.set_data(data_full, data_full_size);

            if (!msg_pack.SerializeToString(&pack_str))
                ret = EFAULT;
        }

        if (data_full != this->_packn_data_array.data()) {
            ::free(data_full);
        }

        if (ret)
            return ret;

        if (!this->_riakp) {
            head_size = 17;

            uint32_t total_size = pack_str.size() + head_size;
            uint16_t no_objs_head = (uint16_t)(no_objs - 1);
            uint16_t no_nulls_head = (uint16_t)no_nulls;

            memcpy(&(head_buf[0]), &total_size, 4);
            head_buf[4] = optimal_mode;
            memcpy(&(head_buf[5]), &no_objs_head, 2);
            memcpy(&(head_buf[7]), &no_nulls_head, 2);
            memcpy(&(head_buf[9]), &max_save_v, 8);
        }

        break;
      }
    case DM_IB_TABLEINFO_CT_FLOAT :
    case DM_IB_TABLEINFO_CT_DOUBLE :
      {
        int64_t local_min = this->_dpn_p->min_v.int_v;
        int64_t local_max = this->_dpn_p->max_v.int_v;
        uint64_t max_save_v = UINT64_MAX;

        for (int i=0; i<no_objs; i++) {
            if (!CR_Class_NS::BMP_BITTEST(this->nulls, sizeof(this->nulls), i)) {
                this->_rsis->PutValueN(
                  this->_lineid_begin + i, this->_packn_data_array[i].int_v, local_min, local_max);
            }
        }

        if (!this->_riakp) {
            switch (this->_compress_level) {
            case CR_Compress::CT_ZLIB :
                ret = ib_packn_compress_zlib(this->nulls, no_nulls, this->_packn_data_array.data(),
                  no_objs, UINT64_MAX, pack_str, optimal_mode);
                break;
            case CR_Compress::CT_SNAPPY :
                ret = ib_packn_compress_snappy(this->nulls, no_nulls, this->_packn_data_array.data(),
                  no_objs, UINT64_MAX, pack_str, optimal_mode);
                break;
            case CR_Compress::CT_LZ4 :
                ret = ib_packn_compress_lz4(this->nulls, no_nulls, this->_packn_data_array.data(),
                  no_objs, UINT64_MAX, pack_str, optimal_mode);
                break;
            default :
                ret = ib_packn_compress(this->nulls, no_nulls, this->_packn_data_array.data(),
                  no_objs, UINT64_MAX, pack_str, optimal_mode);
                break;
            }
            if (ret) {
                DPRINTF("ib_packn_compress(%p, %d, %p, %d, ...) == %d\n", this->nulls, no_nulls,
                  this->_packn_data_array.data(), no_objs, ret);
            }
        } else {
            msgRCAttr_packN msg_pack;

            msg_pack.set_no_obj(no_objs - 1);
            msg_pack.set_no_nulls(no_nulls);
            msg_pack.set_optimal_mode(optimal_mode);
            msg_pack.set_max_val(max_save_v);
            msg_pack.set_total_size(sizeof(double) * this->_packn_data_array.size());
            msg_pack.set_nulls(this->nulls, sizeof(this->nulls));
            msg_pack.set_data(this->_packn_data_array.data(),
              sizeof(double) * this->_packn_data_array.size());

            if (!msg_pack.SerializeToString(&pack_str))
                ret = EFAULT;
        }

        if (ret)
            return ret;

        if (!this->_riakp) {
            head_size = 17;

            uint32_t total_size = pack_str.size() + head_size;
            uint16_t no_objs_head = (uint16_t)(no_objs - 1);
            uint16_t no_nulls_head = (uint16_t)no_nulls;

            memcpy(&(head_buf[0]), &total_size, 4);
            head_buf[4] = optimal_mode;
            memcpy(&(head_buf[5]), &no_objs_head, 2);
            memcpy(&(head_buf[7]), &no_nulls_head, 2);
            memcpy(&(head_buf[9]), &max_save_v, 8);
        }

        break;
      }
    case DM_IB_TABLEINFO_CT_CHAR :
    case DM_IB_TABLEINFO_CT_VARCHAR :
      {
        size_t prefix_len = this->_dpn_p->GetPrefixLen();
        char *packs_data_p = new char[this->_packs_data_full_byte_size];
        void *data_p_tmp = packs_data_p;

        for (int i=0; i<no_objs; i++) {
            const char *cur_line_p = this->_packs_index_array[i];
            const size_t cur_line_size = this->_packs_lens_array[i];
            data_p_tmp = CR_Class_NS::memwrite(data_p_tmp, cur_line_p, cur_line_size);
            if (!CR_Class_NS::BMP_BITTEST(this->nulls, sizeof(this->nulls), i))
                this->_rsis->PutValueS(this->_lineid_begin + i, cur_line_p, cur_line_size, prefix_len);
        }

        if (!this->_riakp) {
            switch (this->_compress_level) {
            case CR_Compress::CT_ZLIB :
                ret = ib_packs_compress_zlib(this->nulls, no_nulls, packs_data_p,
                  this->_packs_data_full_byte_size, no_objs, this->_packs_lens_array.data(),
                  sizeof(uint16_t), pack_str, optimal_mode);
                break;
            case CR_Compress::CT_SNAPPY :
                ret = ib_packs_compress_snappy(this->nulls, no_nulls, packs_data_p,
                  this->_packs_data_full_byte_size, no_objs, this->_packs_lens_array.data(),
                  sizeof(uint16_t), pack_str, optimal_mode);
                break;
            case CR_Compress::CT_LZ4 :
                ret = ib_packs_compress_lz4(this->nulls, no_nulls, packs_data_p,
                  this->_packs_data_full_byte_size, no_objs, this->_packs_lens_array.data(),
                  sizeof(uint16_t), pack_str, optimal_mode);
                break;
            default :
                ret = ib_packs_compress(this->nulls, no_nulls, packs_data_p,
                  this->_packs_data_full_byte_size, no_objs,
                  this->_packs_lens_array.data(), sizeof(uint16_t), pack_str, optimal_mode);
                break;
            }
            if (ret) {
                DPRINTF("ib_packs_compress(%p, %d, %p, %lu, %d, %p, ...) == %d\n",
                  this->nulls, no_nulls, packs_data_p,
                  (long unsigned)this->_packs_data_full_byte_size, no_objs,
                  this->_packs_lens_array.data(), ret);
            }
        } else {
            msgRCAttr_packS msg_pack;

            msg_pack.set_no_obj(no_objs - 1);
            msg_pack.set_no_nulls(no_nulls);
            msg_pack.set_optimal_mode(optimal_mode);
            msg_pack.set_data_full_byte_size(this->_packs_data_full_byte_size);
            msg_pack.set_nulls(this->nulls, sizeof(this->nulls));
            msg_pack.set_data(packs_data_p, this->_packs_data_full_byte_size);
            msg_pack.set_lens(this->_packs_lens_array.data(),
              sizeof(uint16_t) * this->_packs_lens_array.size());

            msg_pack.set_decomposer_id(0);
            msg_pack.set_no_groups(0);
            msg_pack.set_ver(0);

            if (!msg_pack.SerializeToString(&pack_str))
                ret = EFAULT;
        }

        delete []packs_data_p;

        if (ret)
            return ret;

        if (!this->_riakp) {
            head_size = 13;

            uint32_t total_size = pack_str.size() + head_size;
            uint16_t no_objs_head = (uint16_t)(no_objs - 1);
            uint16_t no_nulls_head = (uint16_t)no_nulls;

            memcpy(&(head_buf[0]), &total_size, 4);
            head_buf[4] = optimal_mode;
            memcpy(&(head_buf[5]), &no_objs_head, 2);
            memcpy(&(head_buf[7]), &no_nulls_head, 2);
            memcpy(&(head_buf[9]), &(this->_packs_data_full_byte_size), 4);
        }

        break;
      }
    default :
        ret = EPROTOTYPE;
        break;
    }

    if (!this->_riakp) {
        if (file_sem_p)
            file_sem_p->wait();

        if (CR_File::write(fd, head_buf, head_size) != head_size) {
            ret = errno;
        } else {
            if (CR_File::write(fd, pack_str.data(), pack_str.size())
              != (ssize_t)pack_str.size()) {
                ret = errno;
            }
        }

        if (file_sem_p)
            file_sem_p->post();

        if (!ret) {
            this->_dpn_p->pack_file = file_id;
            this->_dpn_p->pack_addr = pack_addr;
        }
    } else {
        do {
            int64_t pack_no = _alloc_key(this->_job_id, this->_ks_pool_p, pack_id_list_p);
            if (pack_no < 0) {
                DPRINTF("Get pack no failed[%s]\n", CR_Class_NS::strerrno(errno));
                break;
            }
            std::string riak_key = _get_riak_key(pack_no);
            ret = this->_riakp->AsyncPut(this->_riak_handle, this->_riak_bucket, riak_key, pack_str);

            if (!ret) {
                int64_t pack_ksid = pack_no & (~((INT64_C(1) << 12) - 1));
                DPRINTFX(15, "Column %u Pack %lld using key space 0x%016llX(%lld)\n",
                  (unsigned)this->_column_id, (long long)(this->_lineid_begin >> 16),
                  (long long)pack_ksid, (long long)(pack_ksid >> 12));
                this->_dpn_p->pack_file = (int32_t)(pack_no >> 32);
                this->_dpn_p->pack_addr = (uint32_t)(pack_no & 0xFFFFFFFF);
            } else {
                DPRINTF("AsyncPut(\"%s\", \"%s\", ...) failed[%s]\n",
                  this->_riak_bucket.c_str(), riak_key.c_str(), CR_Class_NS::strerrno(ret));
                break;
            }
        } while (0);
    }

    return ret;
}

static int64_t
_alloc_key(const std::string &job_id, CR_BlockQueue<int64_t> *_ks_pool_p, std::list<int64_t> *pack_id_list_p)
{
    int fret = 0;
    int64_t ks_no = -1;

    if (pack_id_list_p->empty()) {
        fret = _ks_pool_p->pop_front(ks_no, 3600);
        if (fret) {
            DPRINTF("Get key_space from queue failed[%s]\n", CR_Class_NS::strerrno(fret));
            errno = fret;
            return -1;
        }

        for (int i=0; i<4096; i++) {
            int64_t tmp_packno = ks_no + i;
            pack_id_list_p->push_back(tmp_packno);
        }
    }

    ks_no = pack_id_list_p->front();
    pack_id_list_p->pop_front();

    return ks_no;
}
