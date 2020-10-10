#include <cr_class/cr_class.h>
#include <cr_class/cr_compress.h>
#include <cr_class/cr_quickhash.h>

#include <errno.h>
#include <string.h>
#include <stdlib.h>

#include <lz4.h>
#include <snappy-c.h>
#include <zlib.h>
#include <lzma.h>

#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif /* _BSD_SOURCE */
#include <endian.h>

#define _CC_SAMPLE_SIZE			(512)
#define _CC_SAMPLE_COUNT		(16)

#define _CC_OSIZE_SIZE			(sizeof(uint64_t))
#define _CC_ADDON_SIZE			(_CC_OSIZE_SIZE + sizeof(uint64_t))

/////////////////////

static int _compress_none(const void *, const uint64_t, void *, uint64_t &, const int);
static int _decompress_none(const void *, const uint64_t, void *, const uint64_t);

static int _compress_lz4(const void *, const uint64_t, void *, uint64_t &, const int);
static int _decompress_lz4(const void *, const uint64_t, void *, const uint64_t);

static int _compress_snappy(const void *, const uint64_t, void *, uint64_t &, const int);
static int _decompress_snappy(const void *, const uint64_t, void *, const uint64_t);

static int _compress_zlib(const void *, const uint64_t, void *, uint64_t &, const int);
static int _decompress_zlib(const void *, const uint64_t, void *, const uint64_t);

static int _compress_lzma(const void *, const uint64_t, void *, uint64_t &, const int);
static int _decompress_lzma(const void *, const uint64_t, void *, const uint64_t);

/////////////////////

uint64_t
CR_Compress::compress_bound(const uint64_t src_size, const int compress_type)
{
    uint64_t bound_size = 0;

    switch (compress_type) {
    case CR_Compress::CT_NONE :
        bound_size = src_size;
        break;
    case CR_Compress::CT_LZ4 :
        bound_size = ::LZ4_compressBound(src_size);
        break;
    case CR_Compress::CT_SNAPPY :
        bound_size = ::snappy_max_compressed_length(src_size);
        break;
    case CR_Compress::CT_ZLIB :
        bound_size = ::compressBound(src_size);
        break;
    case CR_Compress::CT_LZMA :
        bound_size = CR_Class_NS::max(bound_size, (src_size + 4096));
        break;
    default :
        bound_size = 0;
        bound_size = CR_Class_NS::max(bound_size, (uint64_t)::LZ4_compressBound(src_size));
        bound_size = CR_Class_NS::max(bound_size, ::snappy_max_compressed_length(src_size));
        bound_size = CR_Class_NS::max(bound_size, ::compressBound(src_size));
        bound_size = CR_Class_NS::max(bound_size, (src_size + 4096));
        break;
    }

    if (bound_size > 0)
        bound_size += _CC_ADDON_SIZE;

    return bound_size;
}

uint64_t
CR_Compress::decompress_size(const void *src_p)
{
    if (!src_p)
        return 0;
    uint64_t orig_size_nl = 0;
    ::memcpy((void*)(((uintptr_t)(&orig_size_nl))+2), (void*)((uintptr_t)src_p+2), sizeof(orig_size_nl)-2);
    return be64toh(orig_size_nl);
}

/////////////////////

void *
CR_Compress::compress(const void *src_p, const uint64_t src_size, void *dst_p, uint64_t *dst_size_p,
    const int compress_type, const int preset)
{
    if (!src_p) {
        errno = EFAULT;
        return NULL;
    }

    if (src_size > UINT64_C(0x0000FFFFFFFFFFF0)) {
        errno = EMSGSIZE;
        return NULL;
    }

    uint64_t bound_size = CR_Compress::compress_bound(src_size, compress_type);

    if (bound_size == 0) {
        errno = EINVAL;
        return NULL;
    }

    int8_t *c_p;

    if (dst_p && dst_size_p) {
        if (*dst_size_p >= bound_size) {
            c_p = (int8_t*)dst_p;
        } else {
            errno = ENOBUFS;
            return NULL;
        }
    } else {
        c_p = (int8_t*)::malloc(bound_size);
    }

    if (!c_p) {
        errno = ENOMEM;
        return NULL;
    }

    int8_t c_type_local = compress_type;
    int (*do_compress)(const void *, const uint64_t, void *, uint64_t &, const int);
    do_compress = NULL;

    switch (compress_type) {
    case CT_NONE :
        do_compress = _compress_none;
        break;
    case CT_LZ4 :
        do_compress = _compress_lz4;
        break;
    case CT_SNAPPY :
        do_compress = _compress_snappy;
        break;
    case CT_ZLIB :
        do_compress = _compress_zlib;
        break;
    case CT_LZMA :
        do_compress = _compress_lzma;
        break;
    default :
      {
        do_compress = _compress_lz4;
        c_type_local = CT_LZ4;
        if (src_size >= (_CC_SAMPLE_SIZE * _CC_SAMPLE_COUNT)) {
            uint8_t sample_buf_buf[_CC_SAMPLE_SIZE * _CC_SAMPLE_COUNT];
            uint8_t test_buf_buf[_CC_SAMPLE_SIZE * _CC_SAMPLE_COUNT * 2 + 4096];
            uint8_t *sample_buf = sample_buf_buf;
            uint8_t *test_buf = test_buf_buf;
            uint64_t test_size = sizeof(test_buf);
            uint8_t *tmp_src_p = (uint8_t*)src_p;
            uint64_t pos_step = src_size / _CC_SAMPLE_COUNT;
            for (int i=0; i<_CC_SAMPLE_COUNT; i++) {
                ::memcpy(sample_buf, tmp_src_p, _CC_SAMPLE_SIZE);
                tmp_src_p += pos_step;
                sample_buf += _CC_SAMPLE_SIZE;
            }
            _compress_lz4(sample_buf, sizeof(sample_buf), test_buf, test_size, 1);
            if (test_size <= (_CC_SAMPLE_SIZE * _CC_SAMPLE_COUNT / 32 * 24)) {
                do_compress = _compress_lzma;
                c_type_local = CT_LZMA;
            } else if (test_size <= (_CC_SAMPLE_SIZE * _CC_SAMPLE_COUNT / 32 * 29)) {
                do_compress = _compress_zlib;
                c_type_local = CT_ZLIB;
            } else if (test_size > (_CC_SAMPLE_SIZE * _CC_SAMPLE_COUNT / 32 * 31)) {
                do_compress = _compress_none;
                c_type_local = CT_NONE;
            }
        }
        break;
      }
    }

    uint64_t c_size_local = bound_size;
    int c_fret = do_compress(src_p, src_size, &(c_p[_CC_OSIZE_SIZE]), c_size_local, preset);
    uint64_t src_size_nl;
    uint64_t total_crc;
    uint64_t total_crc_nl;

    if (c_fret) {
        errno = c_fret;
        goto errout;
    }

    src_size_nl = htobe64(src_size);

    ::memcpy(c_p, &src_size_nl, sizeof(src_size_nl));
    c_p[0] = c_type_local;

    total_crc = CR_QuickHash::CRC64Hash(c_p, c_size_local + _CC_OSIZE_SIZE);
    total_crc_nl = htobe64(total_crc);

    ::memcpy(&(c_p[_CC_OSIZE_SIZE + c_size_local]), &total_crc_nl, sizeof(total_crc_nl));

    if (dst_size_p)
        *dst_size_p = c_size_local + _CC_ADDON_SIZE;

    return c_p;

errout:

    if (c_p && (c_p != dst_p))
        ::free(c_p);

    return NULL;
}

int64_t
CR_Compress::compress(const void *src_p, const uint64_t src_size, void *dst_p, uint64_t dst_max_size,
    const int compress_type, const int preset)
{
    if (!dst_p) {
        errno = EFAULT;
        return -1;
    }

    void *fret = CR_Compress::compress(src_p, src_size, dst_p, &dst_max_size, compress_type, preset);
    if (!fret)
        return -1;

    return dst_max_size;
}

int
CR_Compress::compress(const void *src_p, const uint64_t src_size,
    std::string &dst, const int compress_type, const int preset)
{
    uint64_t dst_size = 0;
    char *dst_p = (char *)CR_Compress::compress(src_p, src_size, NULL, &dst_size,
      compress_type, preset);
    if (dst_p) {
        dst.assign(dst_p, dst_size);
        ::free(dst_p);
        return 0;
    }
    return errno;
}

int
CR_Compress::compress(const std::string &src, std::string &dst, const int compress_type, const int preset)
{
    return CR_Compress::compress(src.data(), src.size(), dst, compress_type, preset);
}

std::string
CR_Compress::compress(const void *src_p, const uint64_t src_size, const int compress_type, const int preset)
{
    std::string ret;
    errno = CR_Compress::compress(src_p, src_size, ret, compress_type, preset);
    return ret;
}

std::string
CR_Compress::compress(const std::string &src, const int compress_type, const int preset)
{
    std::string ret;
    errno = CR_Compress::compress(src, ret, compress_type, preset);
    return ret;
}

/////////////////////

void *
CR_Compress::decompress(const void *src_p, const uint64_t src_size, void *dst_p, uint64_t *dst_size_p)
{
    if (!src_p) {
        errno = EFAULT;
        return NULL;
    }

    if (src_size < _CC_ADDON_SIZE) {
        errno = ENOMSG;
        return NULL;
    }

    int fret;
    int8_t compress_type = ((int8_t*)src_p)[0];
    int (*do_decompress)(const void *, const uint64_t, void *, const uint64_t);

    do_decompress = NULL;

    switch (compress_type) {
    case CT_NONE :
    case CT_NONE_VER_0 :
        do_decompress = _decompress_none;
        break;
    case CT_LZ4 :
    case CT_LZ4_VER_0 :
        do_decompress = _decompress_lz4;
        break;
    case CT_SNAPPY :
    case CT_SNAPPY_VER_0 :
        do_decompress = _decompress_snappy;
        break;
    case CT_ZLIB :
    case CT_ZLIB_VER_0 :
        do_decompress = _decompress_zlib;
        break;
    case CT_LZMA :
    case CT_LZMA_VER_0 :
        do_decompress = _decompress_lzma;
        break;
    default :
        errno = ENOTSUP;
        return NULL;
    }

    uint64_t orig_size = decompress_size(src_p);
    void *orig_msg = NULL;

    if (dst_p && dst_size_p) {
        if (*dst_size_p >= orig_size) {
            orig_msg = dst_p;
        } else {
            errno = ENOBUFS;
            return NULL;
        }
    } else {
        orig_msg = ::malloc(orig_size);
        if (!orig_msg) {
            errno = ENOMEM;
            return NULL;
        }
    }

    uint64_t total_crc_nl = htobe64(CR_QuickHash::CRC64Hash(src_p, src_size - sizeof(total_crc_nl)));

    if (::memcmp(&(((int8_t*)src_p)[src_size - sizeof(total_crc_nl)]), &total_crc_nl, sizeof(total_crc_nl))) {
        errno = EBADMSG;
        goto errout;
    }

    fret = do_decompress(&(((int8_t*)src_p)[_CC_OSIZE_SIZE]), src_size - _CC_ADDON_SIZE, orig_msg, orig_size);

    if (fret) {
        errno = fret;
        goto errout;
    }

    if (dst_size_p)
        *dst_size_p = orig_size;

    return orig_msg;

errout:

    if (orig_msg && (orig_msg != dst_p))
        ::free(orig_msg);

    return NULL;
}

int64_t
CR_Compress::decompress(const void *src_p, const uint64_t src_size, void *dst_p, uint64_t dst_max_size)
{
    if (!dst_p) {
        errno = EFAULT;
        return -1;
    }

    void *fret = CR_Compress::decompress(src_p, src_size, dst_p, &dst_max_size);
    if (!fret)
        return -1;

    return dst_max_size;
}

int
CR_Compress::decompress(const void *src_p, const uint64_t src_size, std::string &dst)
{
    uint64_t dst_size = 0;
    char *dst_p = (char *)CR_Compress::decompress(src_p, src_size, NULL, &dst_size);
    if (dst_p) {
        dst.assign(dst_p, dst_size);
        ::free(dst_p);
        return 0;
    }
    return errno;
}

int
CR_Compress::decompress(const std::string &src, std::string &dst)
{
    return CR_Compress::decompress(src.data(), src.size(), dst);
}

std::string
CR_Compress::decompress(const void *src_p, const uint64_t src_size)
{
    std::string ret;
    errno = CR_Compress::decompress(src_p, src_size, ret);
    return ret;
}

std::string
CR_Compress::decompress(const std::string &src)
{
    std::string ret;
    errno = CR_Compress::decompress(src, ret);
    return ret;
}

/////////////////////

static int
_compress_none(const void *o_p, const uint64_t o_size, void *c_p, uint64_t &c_size, const int preset)
{
    ::memcpy(c_p, o_p, o_size);
    c_size = o_size;

    return 0;
}

static int
_decompress_none(const void *c_p, const uint64_t c_size, void *o_p, const uint64_t o_size)
{
    if (o_size != c_size)
        return ENOMSG;

    ::memcpy(o_p, c_p, o_size);

    return 0;
}

/////////////////////

static int
_compress_lz4(const void *o_p, const uint64_t o_size, void *c_p, uint64_t &c_size, const int preset)
{
    int c_size_real = ::LZ4_compress((const char*)o_p, (char *)c_p, o_size);

    if (c_size_real > 0) {
        c_size = c_size_real;
        return 0;
    } else {
        return EINVAL;
    }
}

static int
_decompress_lz4(const void *c_p, const uint64_t c_size, void *o_p, const uint64_t o_size)
{
    int dst_size_real = ::LZ4_decompress_safe((const char*)c_p, (char*)o_p, c_size, o_size);

    if (dst_size_real != (int)o_size)
        return EINVAL;

    return 0;
}

/////////////////////

static int
_compress_snappy(const void *o_p, const uint64_t o_size, void *c_p, uint64_t &c_size, const int preset)
{
    size_t c_size_local = c_size;
    snappy_status fret = ::snappy_compress((const char *)o_p, o_size, (char*)c_p, &c_size_local);

    switch (fret) {
    case SNAPPY_OK :
        c_size = c_size_local;
        return 0;
    case SNAPPY_BUFFER_TOO_SMALL :
        return ENOBUFS;
    default :
        return EINVAL;
    }
}

static int
_decompress_snappy(const void *c_p, const uint64_t c_size, void *o_p, const uint64_t o_size)
{
    uint64_t dec_size = o_size;

    if (::snappy_uncompress((const char*)c_p, c_size, (char*)o_p, &dec_size) != SNAPPY_OK)
        return EINVAL;

    return 0;
}

/////////////////////

static int
_compress_zlib(const void *o_p, const uint64_t o_size, void *c_p, uint64_t &c_size, const int preset)
{
    uLongf c_size_local = c_size;
    int fret = ::compress2((Bytef*)c_p, &c_size_local, (const Bytef*)o_p, o_size, preset);

    switch (fret) {
    case Z_OK :
        c_size = c_size_local;
        return 0;
    case Z_BUF_ERROR :
        return ENOBUFS;
    default :
        return EINVAL;
    }
}

static int
_decompress_zlib(const void *c_p, const uint64_t c_size, void *o_p, const uint64_t o_size)
{
    uint64_t dst_size_real = o_size;

    if (::uncompress((Bytef*)o_p, &dst_size_real, (const Bytef*)c_p, c_size) != Z_OK)
        return EINVAL;

    return 0;
}

/////////////////////

static int
_compress_lzma(const void *o_p, const uint64_t o_size, void *c_p, uint64_t &c_size, const int preset)
{
    uint64_t c_size_local = c_size;
    lzma_stream strm = LZMA_STREAM_INIT;

    int fret = ::lzma_easy_encoder(&strm, preset, LZMA_CHECK_NONE);
    if (fret == LZMA_OK) {
        strm.next_in = (const uint8_t*)o_p;
        strm.avail_in = o_size;
        strm.next_out = (uint8_t*)c_p;
        strm.avail_out = c_size_local;
        fret = ::lzma_code(&strm, LZMA_FINISH);
        if (fret == LZMA_STREAM_END) {
            c_size_local -= strm.avail_out;
            fret = 0;
        } else
            fret = EINVAL;
    } else
        fret = EINVAL;

    lzma_end(&strm);

    if (!fret)
        c_size = c_size_local;

    return fret;
}

static int
_decompress_lzma(const void *c_p, const uint64_t c_size, void *o_p, const uint64_t o_size)
{
    uint64_t dst_size_real = o_size;

    lzma_stream strm = LZMA_STREAM_INIT;

    strm.next_in = (uint8_t *)c_p;
    strm.avail_in = c_size;

    int fret = lzma_stream_decoder(&strm, UINT32_C(0x8000000), LZMA_CONCATENATED);
    if (fret == LZMA_OK) {
        strm.next_out = (uint8_t*)o_p;
        strm.avail_out = dst_size_real;
        fret = lzma_code(&strm, LZMA_FINISH);
        if (fret == LZMA_STREAM_END) {
            dst_size_real -= strm.avail_out;
            fret = 0;
        } else
            fret = EINVAL;
    } else
        fret = EINVAL;

    lzma_end(&strm);

    return fret;
}

/////////////////////
