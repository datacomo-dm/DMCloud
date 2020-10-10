#include "IBCompress.h"
#include <assert.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_memguard.h>

#include "common/CommonDefinitions.h"	// uint ,uchar, ...
#include "common/bhassert.h"	// BHASSERT
#include "system/fet.h"		// MEASURE_FET

// packs 压缩类型设置
bool ib_packs_compressmode_assist::IsModeNullsCompressed(const uchar
							 optimal_mode)
{
	return optimal_mode & 0x1;
}

bool ib_packs_compressmode_assist::IsModeDataCompressed(const uchar
							optimal_mode)
{
	return optimal_mode & 0x2;
}

bool ib_packs_compressmode_assist::IsModeCompressionApplied(const uchar
							    optimal_mode)
{
	return IsModeDataCompressed(optimal_mode)
	    || IsModeNullsCompressed(optimal_mode);
}

bool ib_packs_compressmode_assist::IsModeNoCompression(const uchar optimal_mode)
{
	return optimal_mode & 0x4;
}

bool ib_packs_compressmode_assist::IsModeValid(const uchar optimal_mode)
{
	return ((optimal_mode & 0xFC) == 0)
	    || IsModeNoCompression(optimal_mode);
}

void ib_packs_compressmode_assist::SetModeDataCompressed(uchar & optimal_mode)
{
	ResetModeNoCompression(optimal_mode);
	optimal_mode |= 0x2;
}

void ib_packs_compressmode_assist::SetModeNullsCompressed(uchar & optimal_mode)
{
	ResetModeNoCompression(optimal_mode);
	optimal_mode |= 0x1;
}

void ib_packs_compressmode_assist::
ResetModeNullsCompressed(uchar & optimal_mode)
{
	optimal_mode &= 0xFE;
}

void ib_packs_compressmode_assist::ResetModeDataCompressed(uchar & optimal_mode)
{
	optimal_mode &= 0xFD;
}

void ib_packs_compressmode_assist::ResetModeNoCompression(uchar & optimal_mode)
{
	optimal_mode &= 0xB;
}

// packn 压缩类型设置
bool ib_packn_compressmode_assist::IsModeNullsCompressed(const uchar
							 optimal_mode)
{
	return optimal_mode & 0x10;
}

bool ib_packn_compressmode_assist::IsModeDataCompressed(const uchar
							optimal_mode)
{
	return optimal_mode & 0x20;
}

bool ib_packn_compressmode_assist::IsModeCompressionApplied(const uchar
							    optimal_mode)
{
	return IsModeDataCompressed(optimal_mode)
	    || IsModeNullsCompressed(optimal_mode);
}

bool ib_packn_compressmode_assist::IsModeNoCompression(const uchar optimal_mode)
{
	return optimal_mode & 0x40;
}

bool ib_packn_compressmode_assist::IsModeValid(const uchar optimal_mode)
{
	return ((optimal_mode & 0xCF) == 0)
	    || IsModeNoCompression(optimal_mode);
}

void ib_packn_compressmode_assist::ResetModeNoCompression(uchar & optimal_mode)
{
	optimal_mode &= 0xBF;
}

void ib_packn_compressmode_assist::SetModeDataCompressed(uchar & optimal_mode)
{
	ResetModeNoCompression(optimal_mode);
	optimal_mode |= 0x20;
}

void ib_packn_compressmode_assist::SetModeNullsCompressed(uchar & optimal_mode)
{
	ResetModeNoCompression(optimal_mode);
	optimal_mode |= 0x10;
}

void ib_packn_compressmode_assist::
ResetModeNullsCompressed(uchar & optimal_mode)
{
	optimal_mode &= 0xEF;
}

void ib_packn_compressmode_assist::ResetModeDataCompressed(uchar & optimal_mode)
{
	optimal_mode &= 0xDF;
}

// 判断某一个元素是否为空，对应:AttrPack::IsNull
bool ib_cprs_assist::IsNull(const uint * nulls, const int no_nulls,
			    const int no_obj, const int i)
{
	assert(no_nulls <= no_obj);
	assert(i <= no_obj);
	if (no_nulls == 0)
		return false;
	if (no_nulls == no_obj)
		return true;
	if (!nulls)
		return false;
	return ((nulls[i >> 5] & ((uint) (1) << (i % 32))) != 0);	// the i-th bit of the table
}

// 设置packn的null值，对应AttrPackN::SetNull(int i),nulls大小为8k字节
void ib_cprs_assist::SetPacknNull(uint * nulls, int &no_nulls, const int i)
{
	assert(nulls != NULL);
	int max_no_obj = 65535;
	assert(no_nulls <= max_no_obj);
	assert(i <= max_no_obj);

	int mask = (uint) (1) << (i % 32);
	// maybe set by last failed load?
	//BHASSERT_WITH_NO_PERFORMANCE_IMPACT((nulls[i>>5] & mask)==0);
	nulls[i >> 5] |= mask;	// set the i-th bit of the table
	no_nulls++;

}

// 设置packs的null值，对应AttrPackS::SetNull(int i),nulls大小为8k字节
void ib_cprs_assist::SetPacksNull(uint * nulls, int &no_nulls, const int i)
{
	int max_no_obj = 65535;
	assert(nulls != NULL);
	assert(no_nulls <= max_no_obj);
	assert(i <= max_no_obj);
	int ono = i;

	int mask = (uint) (1) << (ono & 31);
	if ((nulls[ono >> 5] & mask) == 0) {
		nulls[ono >> 5] |= mask;	// set the i-th bit of the table
		no_nulls++;
	} else {
		// maybe set by last failed load
		//BHASSERT(0, "Expression evaluation failed!");
	}
}

// 判断是否需要压缩，对象数小于65536不压缩,对应:AttrPack::ShouldNotCompress()
bool ib_pack_assist::ShouldNotCompress(const int no_obj)
{
	return (no_obj < 0x10000);
}

const int ib_pack_assist::bin_sums[256] = { 0, 1, 1, 2, 1, 2, 2, 3,	// 00000***
	1, 2, 2, 3, 2, 3, 3, 4,	// 00001***
	1, 2, 2, 3, 2, 3, 3, 4,	// 00010***
	2, 3, 3, 4, 3, 4, 4, 5,	// 00011***
	1, 2, 2, 3, 2, 3, 3, 4,	// 00100***
	2, 3, 3, 4, 3, 4, 4, 5,	// 00101***
	2, 3, 3, 4, 3, 4, 4, 5,	// 00110***
	3, 4, 4, 5, 4, 5, 5, 6,	// 00111***
	1, 2, 2, 3, 2, 3, 3, 4,	// 01000***
	2, 3, 3, 4, 3, 4, 4, 5,	// 01001***
	2, 3, 3, 4, 3, 4, 4, 5,	// 01010***
	3, 4, 4, 5, 4, 5, 5, 6,	// 01011***
	2, 3, 3, 4, 3, 4, 4, 5,	// 01100***
	3, 4, 4, 5, 4, 5, 5, 6,	// 01101***
	3, 4, 4, 5, 4, 5, 5, 6,	// 01110***
	4, 5, 5, 6, 5, 6, 6, 7,	// 01111***
	1, 2, 2, 3, 2, 3, 3, 4,	// 10000***
	2, 3, 3, 4, 3, 4, 4, 5,	// 10001***
	2, 3, 3, 4, 3, 4, 4, 5,	// 10010***
	3, 4, 4, 5, 4, 5, 5, 6,	// 10011***
	2, 3, 3, 4, 3, 4, 4, 5,	// 10100***
	3, 4, 4, 5, 4, 5, 5, 6,	// 10101***
	3, 4, 4, 5, 4, 5, 5, 6,	// 10110***
	4, 5, 5, 6, 5, 6, 6, 7,	// 10111***
	2, 3, 3, 4, 3, 4, 4, 5,	// 11000***
	3, 4, 4, 5, 4, 5, 5, 6,	// 11001***
	3, 4, 4, 5, 4, 5, 5, 6,	// 11010***
	4, 5, 5, 6, 5, 6, 6, 7,	// 11011***
	3, 4, 4, 5, 4, 5, 5, 6,	// 11100***
	4, 5, 5, 6, 5, 6, 6, 7,	// 11101***
	4, 5, 5, 6, 5, 6, 6, 7,	// 11110***
	5, 6, 6, 7, 6, 7, 7, 8
};				// 11111***

// 和Bintoos.cpp 中的函数一样的
int ib_pack_assist::CalculateBinSum(unsigned int n)
{
	int s = 0;
	s += bin_sums[n % 256];
	n = n >> 8;
	s += bin_sums[n % 256];
	n = n >> 8;
	s += bin_sums[n % 256];
	n = n >> 8;
	s += bin_sums[n % 256];
	return s;
}

// 获取数据类型
ValueType ib_packn_assist::GetValueType(const _uint64 maxv)
{
	if (maxv <= 0xff)
		return UCHAR;
	else if (maxv <= 0xffff)
		return USHORT;
	else if (maxv <= 0xffffffff)
		return UINT;
	else
		return UINT64;
}

// 获取某一个元素的值,对应AttrPackN::GetVal64(int n)
_int64
    ib_packn_assist::GetVal64(const void *data_full,
			      const ValueType value_type, const int n)
{
	if (data_full == NULL)
		return 0;
	if (value_type == UINT)
		return (_int64) ((uint *) data_full)[n];
	else if (value_type == UINT64)
		return ((_int64 *) data_full)[n];
	else if (value_type == USHORT)
		return (_int64) ((ushort *) data_full)[n];
	return (_int64) ((uchar *) data_full)[n];
}

// 设置某一个元素的值,对应AttrPackN::SetVal64
void
 ib_packn_assist::SetVal64(void *data_full, const ValueType value_type,
			   const uint no_obj, const uint n,
			   const _uint64 & val_code)
{
	assert(n < no_obj);
	if (data_full) {
		if (value_type == UINT64)
			((_uint64 *) data_full)[n] = _uint64(val_code);
		else if (value_type == UCHAR)
			((uchar *) data_full)[n] = uchar(val_code);
		else if (value_type == USHORT)
			((ushort *) data_full)[n] = ushort(val_code);
		else if (value_type == UINT)
			((uint *) data_full)[n] = uint(val_code);
	}
}

// 对应:AttrPackS::GetSize
int ib_packs_assist::GetSize(const void *lens, const short len_mode, int ono)
{
	if (len_mode == sizeof(ushort)) {
		return ((ushort *) lens)[ono];
	} else {
		return ((uint *) lens)[ono];
	}
}

//对应:AttrPackS::SetSize
void ib_packs_assist::SetSize(void *lens, const short len_mode, const int ono,
			      uint size)
{
	if (len_mode == sizeof(ushort)) {
		((ushort *) lens)[ono] = (ushort) size;
	} else {
		((uint *) lens)[ono] = (uint) size;
	}
}

/*
CPRS_SUCCESS = 0,
CPRS_ERR_BUF = 1,       // buffer overflow error
CPRS_ERR_PAR = 2,       // bad parameters
CPRS_ERR_SUM = 3,       // wrong cumulative-sum table (for arithmetic coding)
CPRS_ERR_VER = 4,       // incorrect version of compressed data
CPRS_ERR_COR = 5,       // compressed data are corrupted
CPRS_ERR_MEM = 6,       // memory allocation error
CPRS_ERR_OTH = 100      // other error (unrecognized)
*/
void ib_cprs_message(const int retcode, char *retmsg, int msglen)
{
	switch (retcode) {
	case CPRS_SUCCESS:
		snprintf(retmsg, msglen, "cprs success.");
		break;
	case CPRS_ERR_BUF:
		snprintf(retmsg, msglen, "buffer overflow error.");
		break;
	case CPRS_ERR_PAR:
		snprintf(retmsg, msglen, "bad parameters.");
		break;
	case CPRS_ERR_SUM:
		snprintf(retmsg, msglen,
			 "wrong cumulative-sum table (for arithmetic coding).");
		break;
	case CPRS_ERR_VER:
		snprintf(retmsg, msglen,
			 "incorrect version of compressed data.");
		break;
	case CPRS_ERR_COR:
		snprintf(retmsg, msglen, "compressed data are corrupted.");
		break;
	case CPRS_ERR_MEM:
		snprintf(retmsg, msglen, "memory allocation error.");
		break;
	case CPRS_ERR_OTH:
		snprintf(retmsg, msglen, "other error (unrecognized).");
		break;
	default:
		snprintf(retmsg, msglen, "retcode input error.");
		break;
	}
}
