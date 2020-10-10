#include <assert.h>
#include <stdlib.h>
#include <zlib.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_quickhash.h>
#include <cr_class/md5.h>
#include <cr_class/sha1.h>
#include <cr_class/sha2.h>

//////////////////

static const uint64_t _crc64_table[] = {
    0X0000000000000000, 0XB32E4CBE03A75F6F, 0XF4843657A840A05B, 0X47AA7AE9ABE7FF34,
    0X7BD0C384FF8F5E33, 0XC8FE8F3AFC28015C, 0X8F54F5D357CFFE68, 0X3C7AB96D5468A107,
    0XF7A18709FF1EBC66, 0X448FCBB7FCB9E309, 0X0325B15E575E1C3D, 0XB00BFDE054F94352,
    0X8C71448D0091E255, 0X3F5F08330336BD3A, 0X78F572DAA8D1420E, 0XCBDB3E64AB761D61,
    0X7D9BA13851336649, 0XCEB5ED8652943926, 0X891F976FF973C612, 0X3A31DBD1FAD4997D,
    0X064B62BCAEBC387A, 0XB5652E02AD1B6715, 0XF2CF54EB06FC9821, 0X41E11855055BC74E,
    0X8A3A2631AE2DDA2F, 0X39146A8FAD8A8540, 0X7EBE1066066D7A74, 0XCD905CD805CA251B,
    0XF1EAE5B551A2841C, 0X42C4A90B5205DB73, 0X056ED3E2F9E22447, 0XB6409F5CFA457B28,
    0XFB374270A266CC92, 0X48190ECEA1C193FD, 0X0FB374270A266CC9, 0XBC9D3899098133A6,
    0X80E781F45DE992A1, 0X33C9CD4A5E4ECDCE, 0X7463B7A3F5A932FA, 0XC74DFB1DF60E6D95,
    0X0C96C5795D7870F4, 0XBFB889C75EDF2F9B, 0XF812F32EF538D0AF, 0X4B3CBF90F69F8FC0,
    0X774606FDA2F72EC7, 0XC4684A43A15071A8, 0X83C230AA0AB78E9C, 0X30EC7C140910D1F3,
    0X86ACE348F355AADB, 0X3582AFF6F0F2F5B4, 0X7228D51F5B150A80, 0XC10699A158B255EF,
    0XFD7C20CC0CDAF4E8, 0X4E526C720F7DAB87, 0X09F8169BA49A54B3, 0XBAD65A25A73D0BDC,
    0X710D64410C4B16BD, 0XC22328FF0FEC49D2, 0X85895216A40BB6E6, 0X36A71EA8A7ACE989,
    0X0ADDA7C5F3C4488E, 0XB9F3EB7BF06317E1, 0XFE5991925B84E8D5, 0X4D77DD2C5823B7BA,
    0X64B62BCAEBC387A1, 0XD7986774E864D8CE, 0X90321D9D438327FA, 0X231C512340247895,
    0X1F66E84E144CD992, 0XAC48A4F017EB86FD, 0XEBE2DE19BC0C79C9, 0X58CC92A7BFAB26A6,
    0X9317ACC314DD3BC7, 0X2039E07D177A64A8, 0X67939A94BC9D9B9C, 0XD4BDD62ABF3AC4F3,
    0XE8C76F47EB5265F4, 0X5BE923F9E8F53A9B, 0X1C4359104312C5AF, 0XAF6D15AE40B59AC0,
    0X192D8AF2BAF0E1E8, 0XAA03C64CB957BE87, 0XEDA9BCA512B041B3, 0X5E87F01B11171EDC,
    0X62FD4976457FBFDB, 0XD1D305C846D8E0B4, 0X96797F21ED3F1F80, 0X2557339FEE9840EF,
    0XEE8C0DFB45EE5D8E, 0X5DA24145464902E1, 0X1A083BACEDAEFDD5, 0XA9267712EE09A2BA,
    0X955CCE7FBA6103BD, 0X267282C1B9C65CD2, 0X61D8F8281221A3E6, 0XD2F6B4961186FC89,
    0X9F8169BA49A54B33, 0X2CAF25044A02145C, 0X6B055FEDE1E5EB68, 0XD82B1353E242B407,
    0XE451AA3EB62A1500, 0X577FE680B58D4A6F, 0X10D59C691E6AB55B, 0XA3FBD0D71DCDEA34,
    0X6820EEB3B6BBF755, 0XDB0EA20DB51CA83A, 0X9CA4D8E41EFB570E, 0X2F8A945A1D5C0861,
    0X13F02D374934A966, 0XA0DE61894A93F609, 0XE7741B60E174093D, 0X545A57DEE2D35652,
    0XE21AC88218962D7A, 0X5134843C1B317215, 0X169EFED5B0D68D21, 0XA5B0B26BB371D24E,
    0X99CA0B06E7197349, 0X2AE447B8E4BE2C26, 0X6D4E3D514F59D312, 0XDE6071EF4CFE8C7D,
    0X15BB4F8BE788911C, 0XA6950335E42FCE73, 0XE13F79DC4FC83147, 0X521135624C6F6E28,
    0X6E6B8C0F1807CF2F, 0XDD45C0B11BA09040, 0X9AEFBA58B0476F74, 0X29C1F6E6B3E0301B,
    0XC96C5795D7870F42, 0X7A421B2BD420502D, 0X3DE861C27FC7AF19, 0X8EC62D7C7C60F076,
    0XB2BC941128085171, 0X0192D8AF2BAF0E1E, 0X4638A2468048F12A, 0XF516EEF883EFAE45,
    0X3ECDD09C2899B324, 0X8DE39C222B3EEC4B, 0XCA49E6CB80D9137F, 0X7967AA75837E4C10,
    0X451D1318D716ED17, 0XF6335FA6D4B1B278, 0XB199254F7F564D4C, 0X02B769F17CF11223,
    0XB4F7F6AD86B4690B, 0X07D9BA1385133664, 0X4073C0FA2EF4C950, 0XF35D8C442D53963F,
    0XCF273529793B3738, 0X7C0979977A9C6857, 0X3BA3037ED17B9763, 0X888D4FC0D2DCC80C,
    0X435671A479AAD56D, 0XF0783D1A7A0D8A02, 0XB7D247F3D1EA7536, 0X04FC0B4DD24D2A59,
    0X3886B22086258B5E, 0X8BA8FE9E8582D431, 0XCC0284772E652B05, 0X7F2CC8C92DC2746A,
    0X325B15E575E1C3D0, 0X8175595B76469CBF, 0XC6DF23B2DDA1638B, 0X75F16F0CDE063CE4,
    0X498BD6618A6E9DE3, 0XFAA59ADF89C9C28C, 0XBD0FE036222E3DB8, 0X0E21AC88218962D7,
    0XC5FA92EC8AFF7FB6, 0X76D4DE52895820D9, 0X317EA4BB22BFDFED, 0X8250E80521188082,
    0XBE2A516875702185, 0X0D041DD676D77EEA, 0X4AAE673FDD3081DE, 0XF9802B81DE97DEB1,
    0X4FC0B4DD24D2A599, 0XFCEEF8632775FAF6, 0XBB44828A8C9205C2, 0X086ACE348F355AAD,
    0X34107759DB5DFBAA, 0X873E3BE7D8FAA4C5, 0XC094410E731D5BF1, 0X73BA0DB070BA049E,
    0XB86133D4DBCC19FF, 0X0B4F7F6AD86B4690, 0X4CE50583738CB9A4, 0XFFCB493D702BE6CB,
    0XC3B1F050244347CC, 0X709FBCEE27E418A3, 0X3735C6078C03E797, 0X841B8AB98FA4B8F8,
    0XADDA7C5F3C4488E3, 0X1EF430E13FE3D78C, 0X595E4A08940428B8, 0XEA7006B697A377D7,
    0XD60ABFDBC3CBD6D0, 0X6524F365C06C89BF, 0X228E898C6B8B768B, 0X91A0C532682C29E4,
    0X5A7BFB56C35A3485, 0XE955B7E8C0FD6BEA, 0XAEFFCD016B1A94DE, 0X1DD181BF68BDCBB1,
    0X21AB38D23CD56AB6, 0X9285746C3F7235D9, 0XD52F0E859495CAED, 0X6601423B97329582,
    0XD041DD676D77EEAA, 0X636F91D96ED0B1C5, 0X24C5EB30C5374EF1, 0X97EBA78EC690119E,
    0XAB911EE392F8B099, 0X18BF525D915FEFF6, 0X5F1528B43AB810C2, 0XEC3B640A391F4FAD,
    0X27E05A6E926952CC, 0X94CE16D091CE0DA3, 0XD3646C393A29F297, 0X604A2087398EADF8,
    0X5C3099EA6DE60CFF, 0XEF1ED5546E415390, 0XA8B4AFBDC5A6ACA4, 0X1B9AE303C601F3CB,
    0X56ED3E2F9E224471, 0XE5C372919D851B1E, 0XA26908783662E42A, 0X114744C635C5BB45,
    0X2D3DFDAB61AD1A42, 0X9E13B115620A452D, 0XD9B9CBFCC9EDBA19, 0X6A978742CA4AE576,
    0XA14CB926613CF817, 0X1262F598629BA778, 0X55C88F71C97C584C, 0XE6E6C3CFCADB0723,
    0XDA9C7AA29EB3A624, 0X69B2361C9D14F94B, 0X2E184CF536F3067F, 0X9D36004B35545910,
    0X2B769F17CF112238, 0X9858D3A9CCB67D57, 0XDFF2A94067518263, 0X6CDCE5FE64F6DD0C,
    0X50A65C93309E7C0B, 0XE388102D33392364, 0XA4226AC498DEDC50, 0X170C267A9B79833F,
    0XDCD7181E300F9E5E, 0X6FF954A033A8C131, 0X28532E49984F3E05, 0X9B7D62F79BE8616A,
    0XA707DB9ACF80C06D, 0X14299724CC279F02, 0X5383EDCD67C06036, 0XE0ADA17364673F59
};

//////////////////

uint32_t
CR_QuickHash::CRC32Hash(const void *p, size_t size, uint32_t hash)
{
    if (!p || !size)
        return hash;

    return ::crc32(hash, (const Bytef*)p, size);
}

uint32_t
CR_QuickHash::SDBMHash(const void *p, size_t size, uint32_t hash)
{
    if (!p || !size)
        return hash;

    const uint8_t *s = (const uint8_t *)p;

    if (!p) {
        return hash;
    }

    while (size) {
        // equivalent to: hash = 65599*hash + (*s++);
        hash = (*s++) + (hash << 6) + (hash << 16) - hash;
        size--;
    }

    return (hash & UINT32_C(0x7FFFFFFF));
}

// RS Hash Function
uint32_t
CR_QuickHash::RSHash(const void *p, size_t size, uint32_t hash)
{
    if (!p || !size)
        return hash;

    const uint8_t *s = (const uint8_t *)p;
    uint32_t b = 378551;
    uint32_t a = 63689;

    if (!p) {
        return hash;
    }

    while (size) {
        hash = hash * a + (*s++);
        a *= b;
        size--;
    }

    return (hash & UINT32_C(0x7FFFFFFF));
}

// P. J. Weinberger Hash Function
uint32_t
CR_QuickHash::PJWHash(const void *p, size_t size, uint32_t hash)
{
    if (!p || !size)
        return hash;

    const uint8_t *s = (const uint8_t *)p;
    uint32_t BitsInUnignedInt = (uint32_t)(sizeof(uint32_t) * 8);
    uint32_t ThreeQuarters    = (uint32_t)((BitsInUnignedInt  * 3) / 4);
    uint32_t OneEighth        = (uint32_t)(BitsInUnignedInt / 8);
    uint32_t HighBits         = UINT32_C(0xFFFFFFFF) << (BitsInUnignedInt - OneEighth);
    uint32_t test             = 0;

    if (!p) {
        return hash;
    }

    while (size) {
        hash = (hash << OneEighth) + (*s++);
        if ((test = hash & HighBits) != 0) {
            hash = ((hash ^ (test >> ThreeQuarters)) & (~HighBits));
        }
        size--;
    }

    return (hash & UINT32_C(0x7FFFFFFF));
}

// ELF Hash Function
uint32_t
CR_QuickHash::ELFHash(const void *p, size_t size, uint32_t hash)
{
    if (!p || !size)
        return hash;

    const uint8_t *s = (const uint8_t *)p;
    uint32_t x    = 0;

    if (!p) {
        return hash;
    }

    while (size) {
        hash = (hash << 4) + (*s++);
        if ((x = hash & UINT32_C(0xF0000000)) != 0) {
            hash ^= (x >> 24);
            hash &= ~x;
        }
        size--;
    }

    return (hash & UINT32_C(0x7FFFFFFF));
}

// BKDR Hash Function
uint32_t
CR_QuickHash::BKDRHash(const void *p, size_t size, uint32_t hash)
{
    if (!p || !size)
        return hash;

    const uint8_t *s = (const uint8_t *)p;
    uint32_t seed = 131; // 31 131 1313 13131 131313 etc..

    if (!p) {
        return hash;
    }

    while (size) {
        hash = hash * seed + (*s++);
        size--;
    }

    return (hash & UINT32_C(0x7FFFFFFF));
}

// AP Hash Function
uint32_t
CR_QuickHash::APHash(const void *p, size_t size, uint32_t hash)
{
    if (!p || !size)
        return hash;

    const uint8_t *s = (const uint8_t *)p;

    if (!p) {
        return hash;
    }

    for (size_t i=0; i<size; i++) {
        if ((i & 1) == 0) {
            hash ^= ((hash << 7) ^ (*s++) ^ (hash >> 3));
        } else {
            hash ^= (~((hash << 11) ^ (*s++) ^ (hash >> 5)));
        }
    }

    return (hash & UINT32_C(0x7FFFFFFF));
}

//static void
//_crc64_init()
//{
//    const uint64_t poly = 0xC96C5795D7870F42;
//    for (uint32_t i = 0; i < 256; ++i) {
//        uint64_t r = i;
//        for (uint32_t j = 0; j < 8; ++j) {
//            r = (r >> 1) ^ (poly & ~((r & 1) - 1));
//        }
//        _crc64_table[i] = r;
//    }
//}

uint64_t
CR_QuickHash::CRC64Hash(const void *p, size_t size, uint64_t hash)
{
    if (!p || !size)
        return hash;

    const uint8_t *buf = (const uint8_t *)p;
    hash = ~hash;
    while (size > 0) {
        hash = _crc64_table[*buf++ ^ (hash & 0xFF)] ^ (hash >> 8);
        --size;
    }
    return ~hash;
}

/////////////////////

std::string
CR_QuickHash::md5raw(const void *buf, const size_t length)
{
    MD5_CTX context;
    uint8_t digest[MD5_DIGEST_LENGTH];
    MD5Init(&context);
    MD5Update(&context, (const uint8_t*)buf, length);
    MD5Final(digest, &context);
    return std::string((const char*)digest, sizeof(digest));
}

std::string
CR_QuickHash::md5raw(const std::string &str)
{
    return md5raw(str.data(), str.size());
}

std::string
CR_QuickHash::md5(const void *buf, const size_t length)
{
    return CR_Class_NS::str2hex(md5raw(buf, length));
}

std::string
CR_QuickHash::md5(const std::string &str)
{
    return md5(str.data(), str.size());
}

/////////////////////

std::string
CR_QuickHash::sha1raw(const void *buf, const size_t length)
{
    SHA1_CTX context;
    uint8_t digest[SHA1_DIGEST_LENGTH];
    SHA1Init(&context);
    SHA1Update(&context, (const uint8_t*)buf, length);
    SHA1Final(digest, &context);
    return std::string((const char*)digest, sizeof(digest));
}

std::string
CR_QuickHash::sha1raw(const std::string &str)
{
    return sha1raw(str.data(), str.size());
}

std::string
CR_QuickHash::sha1(const void *buf, const size_t length)
{
    return CR_Class_NS::str2hex(sha1raw(buf, length));
}

std::string
CR_QuickHash::sha1(const std::string &str)
{
    return sha1(str.data(), str.size());
}

/////////////////////

std::string
CR_QuickHash::sha256raw(const void *buf, const size_t length)
{
    SHA2_CTX context;
    uint8_t digest[SHA256_DIGEST_LENGTH];
    SHA256Init(&context);
    SHA256Update(&context, (const uint8_t*)buf, length);
    SHA256Final(digest, &context);
    return std::string((const char*)digest, sizeof(digest));
}

std::string
CR_QuickHash::sha256raw(const std::string &str)
{
    return sha256raw(str.data(), str.size());
}

std::string
CR_QuickHash::sha256(const void *buf, const size_t length)
{
    return CR_Class_NS::str2hex(sha256raw(buf, length));
}

std::string
CR_QuickHash::sha256(const std::string &str)
{
    return sha256(str.data(), str.size());
}

/////////////////////

std::string
CR_QuickHash::sha384raw(const void *buf, const size_t length)
{
    SHA2_CTX context;
    uint8_t digest[SHA384_DIGEST_LENGTH];
    SHA384Init(&context);
    SHA384Update(&context, (const uint8_t*)buf, length);
    SHA384Final(digest, &context);
    return std::string((const char*)digest, sizeof(digest));
}

std::string
CR_QuickHash::sha384raw(const std::string &str)
{
    return sha384raw(str.data(), str.size());
}

std::string
CR_QuickHash::sha384(const void *buf, const size_t length)
{
    return CR_Class_NS::str2hex(sha384raw(buf, length));
}

std::string
CR_QuickHash::sha384(const std::string &str)
{
    return sha384(str.data(), str.size());
}

/////////////////////

std::string
CR_QuickHash::sha512raw(const void *buf, const size_t length)
{
    SHA2_CTX context;
    uint8_t digest[SHA512_DIGEST_LENGTH];
    SHA512Init(&context);
    SHA512Update(&context, (const uint8_t*)buf, length);
    SHA512Final(digest, &context);
    return std::string((const char*)digest, sizeof(digest));
}

std::string
CR_QuickHash::sha512raw(const std::string &str)
{
    return sha512raw(str.data(), str.size());
}

std::string
CR_QuickHash::sha512(const void *buf, const size_t length)
{
    return CR_Class_NS::str2hex(sha512raw(buf, length));
}

std::string
CR_QuickHash::sha512(const std::string &str)
{
    return sha512(str.data(), str.size());
}
