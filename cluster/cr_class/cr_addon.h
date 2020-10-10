#ifndef __H_CR_ADDON_H__
#define __H_CR_ADDON_H__

#ifndef __STDC_CONSTANT_MACROS
#define __STDC_CONSTANT_MACROS
#endif

#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif

#include <stdint.h>
#include <stddef.h>

#ifdef MIN
#undef MIN
#endif
#define MIN(a, b)	(((a) < (b)) ? (a) : (b))

#ifdef MAX
#undef MAX
#endif
#define MAX(a, b)	(((a) > (b)) ? (a) : (b))

#ifdef __cplusplus
extern "C"
{
#endif

inline bool
CR_BOUNDARY_CHECK(const void *obj_p, const uintptr_t obj_size,
    const void *range_p, const uintptr_t range_size)
{
    if ((uintptr_t)obj_p < ((uintptr_t)range_p))
        return false;
    if (((uintptr_t)obj_p + obj_size) < (uintptr_t)obj_p)
        return false;
    if (((uintptr_t)range_p + range_size) < (uintptr_t)range_p)
        return false;
    if (((uintptr_t)obj_p + obj_size) > ((uintptr_t)range_p + range_size))
        return false;
    return true;
}

inline uint64_t
CR_POW_OF_2(const int s)
{
    if (s < 0) {
        return 0;
    }
    return ((uint64_t)1 << s);
}

inline uintptr_t
CR_ALIGN(const uintptr_t p)
{
	return ( (p + (sizeof(uintptr_t) - 1) ) & ( ~(sizeof(uintptr_t) - 1) ) );
}

inline uintmax_t
CR_BOUNDTO(const uintmax_t a, const uintmax_t b)
{
	if (a == 0) {
		return 0;
	}
	return ((((a - 1) / b) + 1) * b);
}

inline uint16_t
CR_SWAP16(const uint16_t v)
{
        return (
	  ((v & UINT16_C(0xff)) << 8)
	  | ((v & UINT16_C(0xff00)) >> 8)
	);
}

inline uint32_t
CR_SWAP32(const uint32_t v)
{
	return (
	  ((v & UINT32_C(0xff)) << 24)
	  | ((v & UINT32_C(0xff00)) << 8)
	  | ((v & UINT32_C(0xff0000)) >> 8)
	  | ((v & UINT32_C(0xff000000)) >> 24)
	);
}

inline uint64_t
CR_SWAP64(const uint64_t v)
{
        return (
	  ((v & UINT64_C(0xff)) << 56)
	  | ((v & UINT64_C(0xff00)) << 40)
	  | ((v & UINT64_C(0xff0000)) << 24)
	  | ((v & UINT64_C(0xff000000)) << 8)
	  | ((v & UINT64_C(0xff00000000)) >> 8)
	  | ((v & UINT64_C(0xff0000000000)) >> 24)
	  | ((v & UINT64_C(0xff000000000000)) >> 40)
	  | ((v & UINT64_C(0xff00000000000000)) >> 56)
	);
}

inline bool
CR_BIT_TEST_64(const uint64_t v, const int bit_pos)
{
	return ((v & ((uint64_t)1 << (bit_pos & 0x3F))) != 0);
}

inline uint64_t
CR_BIT_SET_64(const uint64_t v, const int bit_pos)
{
	return (v | ((uint64_t)1 << (bit_pos & 0x3F)));
}

inline uint64_t
CR_BIT_CLEAR_64(const uint64_t v, const int bit_pos)
{
	return (v & (~((uint64_t)1 << (bit_pos & 0x3F))));
}

inline uint64_t
CR_BIT_TURN_64(const uint64_t v, const int bit_pos)
{
	return (v ^ ((uint64_t)1 << (bit_pos & 0x3F)));
}

inline bool
CR_BIT_TEST_32(const uint32_t v, const int bit_pos)
{
	return ((v & ((uint32_t)1 << (bit_pos & 0x1F))) != 0);
}

inline uint32_t
CR_BIT_SET_32(const uint32_t v, const int bit_pos)
{
	return (v | ((uint32_t)1 << (bit_pos & 0x1F)));
}

inline uint32_t
CR_BIT_CLEAR_32(const uint32_t v, const int bit_pos)
{
	return (v & (~((uint32_t)1 << (bit_pos & 0x1F))));
}

inline uint32_t
CR_BIT_TURN_32(const uint32_t v, const int bit_pos)
{
	return (v ^ ((uint32_t)1 << (bit_pos & 0x1F)));
}

inline bool
CR_BIT_TEST_16(const uint16_t v, const int bit_pos)
{
	return ((v & ((uint16_t)1 << (bit_pos & 0x0F))) != 0);
}

inline uint16_t
CR_BIT_SET_16(const uint16_t v, const int bit_pos)
{
	return (v | ((uint16_t)1 << (bit_pos & 0x0F)));
}

inline uint16_t
CR_BIT_CLEAR_16(const uint16_t v, const int bit_pos)
{
	return (v & (~((uint16_t)1 << (bit_pos & 0x0F))));
}

inline uint16_t
CR_BIT_TURN_16(const uint16_t v, const int bit_pos)
{
	return (v ^ ((uint16_t)1 << (bit_pos & 0x0F)));
}

inline bool
CR_BIT_TEST_8(const uint8_t v, const int bit_pos)
{
	return ((v & ((uint8_t)1 << (bit_pos & 0x07))) != 0);
}

inline uint8_t
CR_BIT_SET_8(const uint8_t v, const int bit_pos)
{
	return (v | ((uint8_t)1 << (bit_pos & 0x07)));
}

inline uint8_t
CR_BIT_CLEAR_8(const uint8_t v, const int bit_pos)
{
	return (v & (~((uint8_t)1 << (bit_pos & 0x07))));
}

inline uint8_t
CR_BIT_TURN_8(const uint8_t v, const int bit_pos)
{
	return (v ^ ((uint8_t)1 << (bit_pos & 0x07)));
}

inline bool
CR_IS_POW_OF_2(const uint64_t v)
{
	return ((v & (v-1)) == 0);
}

inline uint64_t
CR_ROUND_TO_POW_OF_2(uint64_t v)
{
	if (!(v & (v-1)))
		return v;

	v |= v >> 32;
	v |= v >> 16;
	v |= v >> 8;
	v |= v >> 4;
	v |= v >> 2;
	v |= v >> 1;

	return (v + 1);
}

inline int
CR_FFC64(const uint64_t v)
{
	register uint64_t v1;
	register uint64_t v2;
	register int lcb_pos;

	if (v == UINT64_C(0xFFFFFFFFFFFFFFFF))
		return 0;

	lcb_pos = 64;
	v2 = v;
	v1 = v2 | UINT64_C(0xFFFFFFFF00000000);
	if (v1 != UINT64_C(0xFFFFFFFFFFFFFFFF)) {
		lcb_pos -= 32;
		v2 = v1;
	} else {
		v1 = v2;
	}
	v1 = v2 | UINT64_C(0xFFFF0000FFFF0000);
	if (v1 != UINT64_C(0xFFFFFFFFFFFFFFFF)) {
		lcb_pos -= 16;
		v2 = v1;
	} else {
		v1 = v2;
	}
	v1 = v2 | UINT64_C(0xFF00FF00FF00FF00);
	if (v1 != UINT64_C(0xFFFFFFFFFFFFFFFF)) {
		lcb_pos -= 8;
		v2 = v1;
	} else {
		v1 = v2;
	}
	v1 = v2 | UINT64_C(0xF0F0F0F0F0F0F0F0);
	if (v1 != UINT64_C(0xFFFFFFFFFFFFFFFF)) {
		lcb_pos -= 4;
		v2 = v1;
	} else {
		v1 = v2;
	}
	v1 = v2 | UINT64_C(0xCCCCCCCCCCCCCCCC);
	if (v1 != UINT64_C(0xFFFFFFFFFFFFFFFF)) {
		lcb_pos -= 2;
		v2 = v1;
	} else {
		v1 = v2;
	}
	v1 = v2 | UINT64_C(0xAAAAAAAAAAAAAAAA);
	if (v1 != UINT64_C(0xFFFFFFFFFFFFFFFF)) {
		lcb_pos -= 1;
	}

	return lcb_pos;
}

inline int
CR_FLC64(const uint64_t v)
{
	register uint64_t v1;
	register uint64_t v2;
	register int mcb_pos;

	if (v == UINT64_C(0xFFFFFFFFFFFFFFFF))
		return 0;

	v2 = v;
	v1 = v2 | UINT64_C(0x00000000FFFFFFFF);
	if (v1 != UINT64_C(0xFFFFFFFFFFFFFFFF)) {
		mcb_pos = 33;
		v2 = v1;
	} else {
		mcb_pos = 1;
		v1 = v2;
	}
	v1 = v2 | UINT64_C(0x0000FFFF0000FFFF);
	if (v1 != UINT64_C(0xFFFFFFFFFFFFFFFF)) {
		mcb_pos += 16;
		v2 = v1;
	} else {
		v1 = v2;
	}
	v1 = v2 | UINT64_C(0x00FF00FF00FF00FF);
	if (v1 != UINT64_C(0xFFFFFFFFFFFFFFFF)) {
		mcb_pos += 8;
		v2 = v1;
	} else {
		v1 = v2;
	}
	v1 = v2 | UINT64_C(0x0F0F0F0F0F0F0F0F);
	if (v1 != UINT64_C(0xFFFFFFFFFFFFFFFF)) {
		mcb_pos += 4;
		v2 = v1;
	} else {
		v1 = v2;
	}
	v1 = v2 | UINT64_C(0x3333333333333333);
	if (v1 != UINT64_C(0xFFFFFFFFFFFFFFFF)) {
		mcb_pos += 2;
		v2 = v1;
	} else {
		v1 = v2;
	}
	v1 = v2 | UINT64_C(0x5555555555555555);
	if (v1 != UINT64_C(0xFFFFFFFFFFFFFFFF)) {
		mcb_pos += 1;
	}

	return mcb_pos;
}

inline int
CR_FFS64(const uint64_t v)
{
	register uint64_t v1;
	register uint64_t v2;
	register int lsb_pos;

	if (v == 0)
		return 0;

	lsb_pos = 64;
	v2 = v;
	v1 = v2 & UINT64_C(0x00000000FFFFFFFF);
	if (v1) {
		lsb_pos -= 32;
		v2 = v1;
	} else {
		v1 = v2;
	}
	v1 = v2 & UINT64_C(0x0000FFFF0000FFFF);
	if (v1) {
		lsb_pos -= 16;
		v2 = v1;
	} else {
		v1 = v2;
	}
	v1 = v2 & UINT64_C(0x00FF00FF00FF00FF);
	if (v1) {
		lsb_pos -= 8;
		v2 = v1;
	} else {
		v1 = v2;
	}
	v1 = v2 & UINT64_C(0x0F0F0F0F0F0F0F0F);
	if (v1) {
		lsb_pos -= 4;
		v2 = v1;
	} else {
		v1 = v2;
	}
	v1 = v2 & UINT64_C(0x3333333333333333);
	if (v1) {
		lsb_pos -= 2;
		v2 = v1;
	} else {
		v1 = v2;
	}
	v1 = v2 & UINT64_C(0x5555555555555555);
	if (v1) {
		lsb_pos -= 1;
	}

	return lsb_pos;
}

inline int
CR_FLS64(const uint64_t v)
{
	register uint64_t v1;
	register uint64_t v2;
	register int msb_pos;

	if (v == 0)
		return 0;

	v2 = v;
	v1 = v2 & UINT64_C(0xFFFFFFFF00000000);
	if (v1) {
		msb_pos = 33;
		v2 = v1;
	} else {
		msb_pos = 1;
		v1 = v2;
	}
	v1 = v2 & UINT64_C(0xFFFF0000FFFF0000);
	if (v1) {
		msb_pos += 16;
		v2 = v1;
	} else {
		v1 = v2;
	}
	v1 = v2 & UINT64_C(0xFF00FF00FF00FF00);
	if (v1) {
		msb_pos += 8;
		v2 = v1;
	} else {
		v1 = v2;
	}
	v1 = v2 & UINT64_C(0xF0F0F0F0F0F0F0F0);
	if (v1) {
		msb_pos += 4;
		v2 = v1;
	} else {
		v1 = v2;
	}
	v1 = v2 & UINT64_C(0xCCCCCCCCCCCCCCCC);
	if (v1) {
		msb_pos += 2;
		v2 = v1;
	} else {
		v1 = v2;
	}
	v1 = v2 & UINT64_C(0xAAAAAAAAAAAAAAAA);
	if (v1) {
		msb_pos += 1;
	}

	return msb_pos;
}

inline unsigned int
CR_LOG_2_CEIL(const uint64_t v)
{
	register int msb_pos;

	if (v == 0)
		return 0;

	msb_pos = CR_FLS64(v) - 1;

	if ((((uint64_t)1) << msb_pos) == v)
		return msb_pos;

	return (msb_pos + 1);
}

inline unsigned int
CR_BIT_COUNT(uint64_t v)
{
	register unsigned int bit_count = 0;

	while (v) {
		v &= (v - 1);
		bit_count++;
	}

	return bit_count;
}

inline size_t
BINTREE_LEFT_LEAF(size_t index)
{
    return ((index) * 2 + 1);
}

inline size_t
BINTREE_RIGHT_LEAF(size_t index)
{
    return ((index) * 2 + 2);
}

inline size_t
BINTREE_PARENT(size_t index)
{
    return (((index) + 1) / 2 - 1);
}

#ifdef __cplusplus
}
#endif

#endif /* __H_CR_ADDON_H__  */
