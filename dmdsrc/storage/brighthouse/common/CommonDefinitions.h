/* Copyright (C)  2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free  Software Foundation.

This program is distributed in the hope that  it will be useful, but
WITHOUT ANY WARRANTY; without even  the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License version 2.0 for more details.

You should have received a  copy of the GNU General Public License
version 2.0  along with this  program; if not, write to the Free
Software Foundation,  Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA  */

#ifndef _COMMONDEFINITIONS_H_
#define _COMMONDEFINITIONS_H_

#include <climits>
#include <cfloat>
#include <string>
#include <vector>
#include <boost/logic/tribool.hpp>
#include <stdarg.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>

//#pragma comment(linker, "/DEFAULTLIB:rcbaselib.lib")
#include "fwd.h"
#include "compress/defs.h"
#include "common/ProcessType.h"
//#include "bin64.h"

typedef bool mysql_bool;
typedef unsigned char boolean;

#ifndef FALSE
#define FALSE  0
#endif

#ifndef TRUE
#define TRUE  1
#endif

#ifndef _int64_
#ifdef __GNUC__
typedef long long int _int64;
#endif
#define _int64_
#endif

#ifndef _uint64_
#ifdef __GNUC__
typedef unsigned long long _uint64;
#else
typedef unsigned __int64 _uint64;
#endif
#define _uint64_
#endif

#ifndef int64_
#ifdef __GNUC__
typedef long long int int64;
#else
typedef __int64       int64;
#endif
#define int64_
#define HAVE_INT64   1
#endif

#ifndef uint64_
#ifdef __GNUC__
typedef unsigned long long int uint64;
#else
typedef unsigned __int64 uint64;
#endif
#define uint64_
#define HAVE_UINT64   1
#endif

#ifndef _int32_
#ifdef __GNUC__
typedef int _int32;
#define _int32_
#endif
#endif

#ifndef int32_
#ifdef __GNUC__
typedef int int32;
#define int32_
#endif
#endif

#ifndef _uint32_
typedef unsigned int _uint32;
#define _uint32_
#endif

#ifndef uint32_
#ifdef __GNUC__
typedef unsigned int uint32;
#define uint32_
#endif
#endif

#ifndef uint_
typedef unsigned int uint;
#define uint_
#endif

#ifndef uchar_
typedef unsigned char uchar;
#define uchar_
#endif

#ifndef __GNUC__
typedef unsigned short ushort;
typedef unsigned char uchar;
typedef unsigned int  uint;
#endif

#ifdef __WIN__

	#ifndef UINT64_MAX
	#define UINT64_MAX _UI64_MAX
	#endif

	#undef strcasecmp
	#define strcasecmp _stricmp
	//#define strncasecmp _strnicmp
	#define snprintf _snprintf
	#define strdup _strdup
	#define strtoll _strtoi64
	#define __PRETTY_FUNCTION__   __FUNCSIG__
#ifdef PURE_LIBRARY
	#define gmtime_r(x, y) gmtime_s((y), (x))
#endif
#else

	#ifndef UINT64_MAX
	#define UINT64_MAX ULONG_LONG_MAX
	#endif

#endif

#include "mysql_gate.h"

#define SPTR(TYPE) boost::shared_ptr<TYPE>
#define APTR(TYPE) std::auto_ptr<TYPE>

/////////////////////// NOTE: do not change the order of implemented data types! Stored as int(...) on disk.

enum AttributeType	{	RC_STRING,				// string treated either as dictionary value or "free" text
						RC_VARCHAR,				// as above (discerned for compatibility with SQL)
						RC_INT,					// integer 32-bit
						RC_NUM,					// numerical: decimal, up to DEC(18,18)
						RC_DATE,				// numerical (treated as integer in YYYYMMDD format)
						RC_TIME,				// numerical (treated as integer in HHMMSS format)
						RC_BYTEINT,				// integer 8-bit
						RC_SMALLINT,			// integer 16-bit
						RC_BIN,					// free binary (BLOB), no encoding
						RC_BYTE,				// free binary, fixed size, no encoding
						RC_VARBYTE,				// free binary, variable size, no encoding
						RC_REAL,				// double (stored as non-interpreted _int64, null value is NULL_VALUE_64)
						RC_DATETIME,
						RC_TIMESTAMP,
						RC_DATETIME_N,
						RC_TIMESTAMP_N,
						RC_TIME_N,
						RC_FLOAT,
						RC_YEAR,
						RC_MEDIUMINT,
						RC_BIGINT,
						RC_UNKNOWN = 255
					};

#define MAX_PACK_ROW_SIZE 0x10000LL

#define PLUS_INF_64		_int64(0x7FFFFFFFFFFFFFFFULL)
#define MINUS_INF_64	_int64(0x8000000000000000ULL)
#define NULL_VALUE_64	_int64(0x8000000000000001ULL)
#define NULL_VALUE_32	int(0x80000000)
#define NULL_VALUE_D	(*(double*)("\x01\x00\x00\x00\x00\x00\x00\x80"))
#define NULL_VALUE_F	(*(float*)&NULL_VALUE)
#define NULL_VALUE_SH	short(-32768)
#define NULL_VALUE_C	char(-128)
#define NULL_VALUE_M	-(1 << 23) -1
#define NULL_VALUE_U				0xFFFFFFFC
#define MBYTE						1048576LL
#define GBYTE						1073741824LL
#define MAX_ROW_NUMBER	_int64(0x00007FFFFFFFFFFFULL)			// 2^47 - 1
#define DEFAULT_PIPE_TIMEOUT		600
#define DEFAULT_DELIMITER			';'
//#define DEFAULT_DELIMITER			'\t'
#define DEFAULT_STRING_QUALIFIER	'"'
//#define DEFAULT_STRING_QUALIFIER	0
#define DEFAULT_PIPE_MODE			0

const static double PLUS_INF_DBL = DBL_MAX;
const static double MINUS_INF_DBL = DBL_MAX * -1;

#define BH_BIGINT_MAX		_int64(PLUS_INF_64 - 1)
#define BH_BIGINT_MIN		_int64(NULL_VALUE_64 + 1)
#define BH_INT_MAX			  2147483647
#define BH_INT_MIN			(-2147483647)
#define BH_MEDIUMINT_MAX	  ((1 << 23) - 1)
#define BH_MEDIUMINT_MIN	(-((1 << 23) ))
#define BH_TINYINT_MAX		  127
#define BH_TINYINT_MIN		(-127)
#define BH_SMALLINT_MAX		  ((1 << 15) -1)
#define BH_SMALLINT_MIN		(-((1 << 15) -1))


#define MAX_DEC_PRECISION 18

#define ZERO_LENGTH_STRING ""

#define RESULT_SENDER_CACHE_SIZE 65536

// enum type to char will be controlled by -fshort-enums parameter in the MakeFile
// g++ -fshort-enums  ...
#ifdef __GNUC__
enum RSValue {		RS_NONE = 0,				// the pack is empty
					RS_SOME = 1,				// the pack is suspected (but may be empty or full) (i.e. RS_SOME & RS_ALL = RS_SOME)
					RS_ALL	= 2,				// the pack is full
					RS_UNKNOWN = 3				// the pack was not checked yet (i.e. RS_UNKNOWN & RS_ALL = RS_ALL)
};
#else	//_MSC_VER
// enum type can be user defined
enum RSValue :char{	RS_NONE = 0,				// the pack is empty
					RS_SOME = 1,				// the pack is suspected (but may be empty or full) (i.e. RS_SOME & RS_ALL = RS_SOME)
					RS_ALL	= 2,				// the pack is full
					RS_UNKNOWN = 3				// the pack was not checked yet (i.e. RS_UNKNOWN & RS_ALL = RS_ALL)
};
#endif

/**
	The types of support SQL query operators within Brighthouse.

	The order of these enumerated values is important and
	relevent to the Descriptor class for the time being.
	Any changes made here must also be reflected in the
	Descriptor class' interim createQueryOperator() member.
 */
enum Operator
{
	O_EQ = 0,
	O_EQ_ALL,
	O_EQ_ANY,
	O_NOT_EQ,
	O_NOT_EQ_ALL,
	O_NOT_EQ_ANY,
	O_LESS,
	O_LESS_ALL,
	O_LESS_ANY,
	O_MORE,
	O_MORE_ALL,
	O_MORE_ANY,
	O_LESS_EQ,
	O_LESS_EQ_ALL,
	O_LESS_EQ_ANY,
	O_MORE_EQ,
	O_MORE_EQ_ALL,
	O_MORE_EQ_ANY,

	O_IS_NULL,
	O_NOT_NULL,
	O_BETWEEN,
	O_NOT_BETWEEN,
	O_LIKE,
	O_NOT_LIKE,
	O_IN,
	O_NOT_IN,
	O_EXISTS,
	O_NOT_EXISTS,

	O_FALSE,
	O_TRUE,				// constants
	O_ESCAPE,			// O_ESCAPE is special terminating value, do not interpret
	O_OR_TREE,			// fake operator indicating complex descriptor

	// below operators correspond to MySQL special operators used in MySQL expression tree
	O_MULT_EQUAL_FUNC,	// a=b=c
	O_NOT_FUNC,			// NOT
	O_NOT_ALL_FUNC,		//
	O_UNKNOWN_FUNC,		//
	O_ERROR,			// undefined operator

	/**
	Enumeration member count.
	This should always be the last member.  It's a count of
	the elements in this enumeration and can be used for both
	compiled-time and run-time bounds checking.
	*/
	OPERATOR_ENUM_COUNT
};

enum ArithOperator
{
	O_PLUS, O_MINUS, O_MULT, O_DIV, O_MOD
};

enum LogicalOperator {O_AND, O_OR};

enum ColOperation { DELAYED,
					LISTING, COUNT, SUM, MIN, MAX, AVG, GROUP_BY,
					STD_POP, STD_SAMP, VAR_POP, VAR_SAMP,
					BIT_AND, BIT_OR, BIT_XOR,
					GROUP_CONCAT};
enum QueryType {SELECT, CREATE_TABLE, INSERT_OP, INSERT_FILE, COMMIT_OP, ROLLBACK_OP, DROP_TABLE, ROLLBACK_LASTcommitted};
enum NullMode {NO_NULLS, AS_VALUE, AS_MISSED, AS_NOT_APPLICABLE};
enum Parameter {PIPEMODE, DELIMITER, STRING_QUALIFIER, TIMEOUT, CHARSET_INFO_NUMBER, CHARSETS_INDEX_FILE, LINE_STARTER, LINE_TERMINATOR, SKIP_LINES,
				LOCAL_LOAD, VALUE_LIST_ELEMENTS, LOCK_OPTION, OPTIONALLY_ENCLOSED };

enum RSCType { RS_short, RS_ushort, RS_int, RS_uint, /*RS_long, RS_ulong, */
	RS_char_ptr, RS_uchar_ptr, RS_float, RS_double, RS_int64, RS_uint64, RS_byte, RS_ubyte, RS_wchar_ptr,
	RS_internal_int, RS_internal_int64, RS_binary};

enum DescriptorType
{
	Attr,
	Attr_Attr,
	Attr_Val,
	Attr_Set,
	Attr_Attr_Attr,
	Attr_Attr_Val,
	Attr_Val_Attr,
	Attr_Val_Val,
	Val,
	Val_Val,
	Val_Attr,
	Val_Set,
	Val_Attr_Attr,
	Val_Attr_Val,
	Val_Val_Attr,
	Val_Val_Val,
} ;


class BHTribool {
	// NOTE: in comparisons and assignments use the following three values:
	//
	//  v = true;
	//  v = false;
	//  v = BHTRIBOOL_UNKNOWN;
	//
	// The last one is an alias (constant) of BHTribool() default constructor,
	// which initializes BHTribool as the "unknown" value.
	// Do not use the enumerator defined below, it is internal only.

	enum tribool {BH_FALSE, BH_TRUE, BH_UNKNOWN};
public:

	BHTribool()									{ v = BH_UNKNOWN; }
	BHTribool(bool b)							{ v = (b ? BH_TRUE : BH_FALSE); }
	BHTribool(int) { v = BH_UNKNOWN; assert(!"bad initializer"); }
	bool operator==(BHTribool sec)				{ return v == sec.v; }
	bool operator!=(BHTribool sec)				{ return v != sec.v; }
	const BHTribool operator!()					{ return BHTribool(v == BH_TRUE ? BH_FALSE : (v == BH_FALSE ? BH_TRUE : BH_UNKNOWN));  }
	static BHTribool And(BHTribool a, BHTribool b);
	static BHTribool Or(BHTribool a, BHTribool b);
private:
	BHTribool(BHTribool::tribool b)				{ v = b; }
	BHTribool::tribool v;
};

const BHTribool BHTRIBOOL_UNKNOWN	= BHTribool();


typedef int DataPackId;

template<typename base_t, typename T>
std::auto_ptr<base_t> Clone( T const& orig ) {
	return std::auto_ptr<base_t>(new T((T&)*orig));
}

namespace bh
{
namespace common
{
inline int NoObj2NoPacks(_int64 no_obj)
{
	return (int) ((no_obj + 0xffff) >> 16);
}
}
}

extern std::string infobright_home_dir;
extern std::string infobright_data_dir;

//>> Begin: data compress / decompress type , add by liujs
typedef enum DataCompressionType
{
   Compress_DEFAULT = 0,       // create AttrPack object : AttrPackS,AttrPackN
   Compress_Soft_Zip = 1,     // create AttrPack object : AttrPackS_Zlib,AttrPackN_Zip
   Compress_Hard_Zip = 2,     // create AttrPack object : AttrPackS_Hard_Zlib,AttrPackN_Hard_Zip
}DataCompressionType;
#define      ZIP_COMPRESSION_TYPE		0x80                   // Attr header [25],zlib compresstion		
//<< End: add by liujs


// typedef in 5.1 GA
/////////////////////////

#define RIAK_HOSTNAME_ENV_NAME	"RIAK_HOSTNAME"
#define RIAK_PORT_ENV_NAME	"RIAK_PORT"
#define	RIAK_CONNTCT_THREAD_TIMES_ENV_NAME "RIAK_CONNTCT_THREAD_TIMES"   // ��ѯ����: riak�ڵ㱶�����������ò�ѯ�߳���Ŀ
#define RIAK_CONNECT_THREAD_NUMBER_ENV_NAME "RIAK_CONNECT_NUMBER"        // װ��Ͳ�ѯ����: ����riak���߳���Ŀ�������˸�ѡ��:RIAK_CONNTCT_THREAD_TIMES ��Ч
#define	RIAK_MAX_CONNECT_PER_NODE_ENV_NAME "RIAK_MAX_CONNECT_PER_NODE"   // riak�����㣬ÿһ��riak node�ڵ����ӳص����������
#define RIAK_CLUSTER_DP_BUCKET_NAME	"IB_DATAPACKS"
#define	RIAK_CONNTCT_THREAD_TIMES_MIN	4
#define RIAK_ASYNC_WAIT_TIMEOUT_US	1000000*1800   	//  �첽save�����˳�wait��ʱʱ��30����
#define RIAK_ASYNC_PUT_TIMEOUT_US	1000000*300	 	//  �첽save���������ȴ���ʱʱ��5����


#ifndef TIMEOFDAY
#define TIMEOFDAY CLOCK_REALTIME
#endif
#ifndef MYTIMER_CLASS
#define MYTIMER_CLASS

#ifndef F2MS
// fraction 4 digits,multiply 1000 (seccond -> microsecond)
#define F4MS(tm) (((long)(tm*10000000))/10000.0)
// fraction 2 digits,multiply 1000 (seccond -> microsecond)
#define F2MS(tm) (((long)(tm*100000))/100.0)
// fraction 4 digits,seccond 
#define F4S(tm) (((long)(tm*10000))/10000.0)
// fraction 3 digits,seccond 
#define F3S(tm) (((long)(tm*1000))/1000.0)
// fraction 2 digits,seccond 
#define F2S(tm) (((long)(tm*100))/100.0)
#endif

class mytimer {
	struct timespec st,ct;
public:
	mytimer() {
	 memset(&st,0,sizeof(timespec));
	 memset(&ct,0,sizeof(timespec));
	}
	void Clear() {
		memset(&ct,0,sizeof(timespec));
	}
	void Start() {
	 clock_gettime(TIMEOFDAY,&st);
	}
	void Stop() {
	timespec ed;
	clock_gettime(TIMEOFDAY,&ed);
	ct.tv_sec+=ed.tv_sec-st.tv_sec;
	ct.tv_nsec+=ed.tv_nsec-st.tv_nsec;
	st.tv_sec=ed.tv_sec;st.tv_nsec=ed.tv_nsec;
	}
	void Restart() {
	 Clear();Start();
	}
	double GetTime() {
		return (double)ct.tv_sec+ct.tv_nsec/1e9;
	}
};
#endif

// SASU-54 test
#ifndef PERFORMANCE_SASU_54
#define PERFORMANCE_SASU_54  0
#endif

// riak�Ĳ�����־��save��load��drop // ��������:"RIAK_OPER_LOG" �п���
extern int RIAK_OPER_LOG;

size_t dm_strlcat(char *dst, const char *src, size_t dstsize);
size_t dm_strlcpy(char *dst, const char *src, size_t dstsize);

#endif

