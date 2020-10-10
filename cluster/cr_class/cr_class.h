#ifndef __H_CR_CLASS_H__
#define __H_CR_CLASS_H__

#include <cr_class/cr_addon.h>
#include <limits.h>
#include <stdio.h>
#include <string>
#include <netdb.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>
#include <set>
#include <map>
#include <stdexcept>
#include <cr_class/cr_class.pb.h>
#include <cr_class/cr_locked.h>

#define VERSION_STR_CR_CLASS_H		"1.00.0000"

#define CR_BLOCKOP_FLAG_ERRCHECK	(0x00000001)
#define CR_BLOCKOP_FLAG_ISSOCKFD	(0x00000002)
#define CR_BLOCKOP_FLAG_IGNORECRCERR	(0x00000004)

#ifndef DPRINTFX
#define DPRINTFX(__dprintfx_local_lvl_need, __dprintfx_local_fmt, ...)					\
	do { int __dprintfx_local_lvl_need_local = (int)__dprintfx_local_lvl_need;			\
	     if (CR_Class_NS::get_debug_level() >= __dprintfx_local_lvl_need_local) {			\
		intptr_t __dprintfx_local_tid;								\
		char __dprintfx_local_str_time[32];							\
		struct timeval __dprintfx_local_s_tv;							\
		time_t __dprintfx_local_i_time;								\
		struct tm __dprintfx_local_s_time;							\
		int __dprintfx_local_orig_cancel_state__;						\
		__dprintfx_local_tid = (intptr_t)pthread_self();					\
		gettimeofday(&__dprintfx_local_s_tv, NULL);						\
		__dprintfx_local_i_time = __dprintfx_local_s_tv.tv_sec;					\
		localtime_r(&__dprintfx_local_i_time, &__dprintfx_local_s_time);			\
		strftime(__dprintfx_local_str_time, sizeof(__dprintfx_local_str_time),			\
		  "%F %T", &__dprintfx_local_s_time);							\
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &__dprintfx_local_orig_cancel_state__);	\
		FILE *__dprintfx_local_errfp = stderr;							\
		std::string __dprintfx_filename_local = CR_Class_NS::dprintfx_filename.value();		\
		std::string __dprintfx_line_prefix_local = CR_Class_NS::dprintfx_line_prefix.value();	\
		if (__dprintfx_filename_local.size() > 0)						\
		  __dprintfx_local_errfp = fopen(__dprintfx_filename_local.c_str(), "a+");		\
		fprintf(__dprintfx_local_errfp, "%s.%06ld,%d,%d,%08X,%s,%d,%s: %s"			\
		  __dprintfx_local_fmt, __dprintfx_local_str_time, __dprintfx_local_s_tv.tv_usec,	\
		  __dprintfx_local_lvl_need_local, getpid(), (unsigned)__dprintfx_local_tid,		\
		  __FILE__,__LINE__,__FUNCTION__, __dprintfx_line_prefix_local.c_str(), ##__VA_ARGS__);	\
		if (__dprintfx_local_errfp != stderr) fclose(__dprintfx_local_errfp);			\
		pthread_setcancelstate(__dprintfx_local_orig_cancel_state__, NULL);			\
	} } while (0)
#endif /* DPRINTFX "*/

#ifndef DPRINTF
#define DPRINTF(__dprintf_local_fmt, ...)								\
	do { DPRINTFX(10, __dprintf_local_fmt, ##__VA_ARGS__); } while (0)
#endif

#ifndef THROWF
#define THROWF(__fmt_logged_throw_local_fmt, ...)							\
	do {												\
		char __fmt_logged_throw_local_buf[4096];						\
		snprintf(__fmt_logged_throw_local_buf, sizeof(__fmt_logged_throw_local_buf),		\
			__fmt_logged_throw_local_fmt, ##__VA_ARGS__);					\
		DPRINTFX(0, "%s", __fmt_logged_throw_local_buf);					\
		throw (std::runtime_error(std::string(__fmt_logged_throw_local_buf)));			\
	} while (0)
#endif /* THROWF "*/

#ifdef HAS_INT128
typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;
#endif /* HAS_INT128 */

#ifdef HAS_FP16
typedef __fp16 float16_t;
#endif /* HAS_FP16 */
typedef float float32_t;
typedef double float64_t;
#ifdef HAS_INT128
typedef long double float128_t;
#endif /* HAS_INT128 */

namespace CR_Class_NS {
	typedef enum {
		NN_INPROC,
		NN_IPC,
		NN_TCP
	} nn_transport_t;

	typedef enum {
		CPU_TYPE_USER,
		CPU_TYPE_NICE,
		CPU_TYPE_SYSTEM,
		CPU_TYPE_IDLE,
		CPU_TYPE_IOWAIT,
		CPU_TYPE_IRQ,
		CPU_TYPE_SOFTIRQ
	} cpu_type_t;

	typedef union {
		int16_t int_v;
		uint16_t uint_v;
#ifdef HAS_FP16
		float16_t float_v;
#endif /* HAS_FP16 */
		char str_v[sizeof(uint16_t)];
		struct {
			uint8_t offset;
			uint8_t len;
		} pos_v;
	} union16_t;

	typedef union {
		int32_t int_v;
		uint32_t uint_v;
		float32_t float_v;
		char str_v[sizeof(uint32_t)];
		struct {
			uint16_t offset;
			uint16_t len;
		} pos_v;
		union16_t union16_arr[sizeof(uint32_t) / sizeof(union16_t)];
	} union32_t;

	typedef union {
		int64_t int_v;
		uint64_t uint_v;
		float64_t float_v;
		char str_v[sizeof(uint64_t)];
		struct {
			uint32_t offset;
			uint32_t len;
		} pos_v;
		union16_t union16_arr[sizeof(uint64_t) / sizeof(union16_t)];
		union32_t union32_arr[sizeof(uint64_t) / sizeof(union32_t)];
	} union64_t;

#ifdef HAS_INT128
	typedef union {
		int128_t int_v;
		uint128_t uint_v;
		float128_t float_v;
		char str_v[sizeof(uint128_t)];
		struct {
			uint64_t offset;
			uint64_t len;
		} pos_v;
		union16_t union16_arr[sizeof(uint128_t) / sizeof(union16_t)];
		union32_t union32_arr[sizeof(uint128_t) / sizeof(union32_t)];
		union64_t union64_arr[sizeof(uint128_t) / sizeof(union64_t)];
	} union128_t;
#endif /* HAS_INT128 */

	typedef int (*sort_cmp_func_t)(const void *membl, const void *membr, void *arg);

	extern CR_Locked<std::string> dprintfx_filename;
	extern CR_Locked<std::string> dprintfx_line_prefix;

	// A blank std::string
	extern const std::string blank_string;
	// std::string "1"
	extern const std::string bool_true_str;
	// std::string "0"
	extern const std::string bool_false_str;

	// Get and Set CR_DEBUG_LEVEL
	int get_debug_level();
	int set_debug_level(const int new_value);

	// Get local uuid
	std::string get_local_uuid_bin();
	uint64_t get_local_uuid_int();

	// Return a 64-bit random number
	uint64_t rand();
	// Return a random string
	std::string randstr(const size_t loop_count=1, const bool raw_data=false);

	// Get and time, can use CLOCK_REALTIME or CLOCK_MONOTONIC
	double clock_gettime(const clockid_t clk_id=CLOCK_REALTIME, struct timespec *ts_p=NULL);
	int clock_settime(const double time_in, const clockid_t clk_id=CLOCK_REALTIME);

	// Get a string's CRC32
	uint32_t crc32(const std::string &data, const uint32_t prev_crc=0);

	// Get a string's CRC64
	uint64_t crc64(const std::string &data, uint64_t crc=0);

	// Dump output a block of memory
	void hexdump(FILE *f, const void *mem, unsigned int len, unsigned int cols,
		const char *line_prefix=NULL);

	// Convert value to comparable uint
	uint8_t i82u8(const int8_t v);
	uint16_t i162u16(const int16_t v);
	uint32_t i322u32(const int32_t v);
	uint64_t i642u64(const int64_t v);
	uint32_t float2u32(const float v);
	uint64_t double2u64(const double v);

	// De-convert it
	int8_t u82i8(const uint8_t v);
	int16_t u162i16(const uint16_t v);
	int32_t u322i32(const uint32_t v);
	int64_t u642i64(const uint64_t v);
	float u322float(const uint32_t v);
	double u642double(const uint64_t v);

	std::string u82binstr(const uint8_t v);
	std::string u162binstr(const uint16_t v);
	std::string u322binstr(const uint32_t v);
	std::string u642binstr(const uint64_t v);

	// Convert other type of data to comparable string
	std::string u82str(const uint8_t v);
	std::string u162str(const uint16_t v);
	std::string u322str(const uint32_t v);
	std::string u642str(const uint64_t v);

	std::string i82str(const int8_t v);
	std::string i162str(const int16_t v);
	std::string i322str(const int32_t v);
	std::string i642str(const int64_t v);

	std::string float2str(const float f);
	std::string double2str(const double d);

	std::string ptr2str(const void *p);
	std::string bool2str(const bool b);
	std::string hex2str(const std::string &h);

	std::string u8arr2str(const std::vector<uint8_t> &v_arr);
	std::string u16arr2str(const std::vector<uint16_t> &v_arr);
	std::string u32arr2str(const std::vector<uint32_t> &v_arr);
	std::string u64arr2str(const std::vector<uint64_t> &v_arr);

	std::string i8arr2str(const std::vector<int8_t> &v_arr);
	std::string i16arr2str(const std::vector<int16_t> &v_arr);
	std::string i32arr2str(const std::vector<int32_t> &v_arr);
	std::string i64arr2str(const std::vector<int64_t> &v_arr);

	std::string floatarr2str(const std::vector<float> &v_arr);
	std::string doublearr2str(const std::vector<double> &v_arr);

	std::string strarr2str(const std::vector<std::string> &v_arr);

	// De-convert it
	uint8_t str2u8(const std::string &s, const uint8_t defaule_value=0);
	uint16_t str2u16(const std::string &s, const uint16_t defaule_value=0);
	uint32_t str2u32(const std::string &s, const uint32_t defaule_value=0);
	uint64_t str2u64(const std::string &s, const uint64_t defaule_value=0);

	int8_t str2i8(const std::string &s, const int8_t defaule_value=0);
	int16_t str2i16(const std::string &s, const int16_t defaule_value=0);
	int32_t str2i32(const std::string &s, const int32_t defaule_value=0);
	int64_t str2i64(const std::string &s, const int64_t defaule_value=0);

	float str2float(const std::string &s, const float defaule_value=0.0);
	double str2double(const std::string &s, const double defaule_value=0.0);

	void *str2ptr(const std::string &s, const void *default_value=NULL);
	bool str2bool(const std::string &s, const bool default_value=false);
	std::string str2hex(const std::string &s);
	std::string str2hex(const std::string &s, const bool upper_case);

	std::vector<uint8_t> str2u8arr(const std::string &s, const uint8_t defaule_value=0);
	std::vector<uint16_t> str2u16arr(const std::string &s, const uint16_t defaule_value=0);
	std::vector<uint32_t> str2u32arr(const std::string &s, const uint32_t defaule_value=0);
	std::vector<uint64_t> str2u64arr(const std::string &s, const uint64_t defaule_value=0);

	std::vector<int8_t> str2i8arr(const std::string &s, const int8_t defaule_value=0);
	std::vector<int16_t> str2i16arr(const std::string &s, const int16_t defaule_value=0);
	std::vector<int32_t> str2i32arr(const std::string &s, const int32_t defaule_value=0);
	std::vector<int64_t> str2i64arr(const std::string &s, const int64_t defaule_value=0);

	std::vector<float> str2floatarr(const std::string &s, const float defaule_value=0.0);
	std::vector<double> str2doublearr(const std::string &s, const double defaule_value=0.0);

	std::vector<std::string> str2strarr(const std::string &s);

	// Get dirname from pathname
	std::string dirname(const char *path);
	// Get basename from pathname
	std::string basename(const char *path);
	// mkdir 'path/to/name' at once
	int mkdir_p(const std::string &dirname, mode_t mode);

	// Save a string to file
	int save_string(const char *pathname, const std::string &content, const mode_t mode=0640);
	// Load a file to string
	int load_string(const char *pathname, std::string &content);
	std::string load_string(const char *pathname);
	int fload_string(int fd, std::string &content);

	// Compress data use specific algorithm
	int compressx_auto(const std::string &src, std::string &dst);
	int compressx_none(const std::string &src, std::string &dst);
	int compressx_snappy(const std::string &src, std::string &dst);
	int compressx_zlib(const std::string &src, std::string &dst);
	int compressx_lzma(const std::string &src, std::string &dst);
	// Compress data use auto algorithm
	int compressx(const std::string &src, std::string &dst, const int compress_level=-1);

	// Decompress data from any algorithm
	int decompressx(const std::string &src, std::string &dst);

	// Just SQL's LIKE compare
	bool str_like_cmp(const std::string &s, const std::string &pattern,
		char escape_character='\\', char wildcard_sc='_', char wildcard_mc='%');
	bool str_like_cmp(const char *s, size_t s_len, const char *pattern, size_t pattern_len,
		char escape_character='\\', char wildcard_sc='_', char wildcard_mc='%');

	// Fork a child, and watch it,
	//   if child's return value in retry_set_ret, or child's stop signal in retry_set_sig
	//   re-fork it
	pid_t protection_fork(int *err_no,
		const std::set<int> *retry_set_ret, const std::set<int> *retry_set_sig);

	// Parse a param string (eg. "K1=v1&K2=v2&K3=v3")
	//  to std::map<std::string,std::string> (eg. map["K1"]=="v1", map["K2"]=="v2", map["K3"]==v3)
	int str_param_parse(const std::string &s, std::map<std::string,std::string> *param_map_p=NULL);
	// Merge a std::map<std::string,std::string> to param string
	std::string str_param_merge(const std::map<std::string,std::string> &param_map, bool safe_mode=true);

	// Add/Set a param to an param string s, return new param string
	std::string str_setparam(const std::string &s, const std::string &param_name,
		const std::string &param_value);

	std::string str_setparam_u8(const std::string &s, const std::string &param_name,
		const uint8_t param_value);
	std::string str_setparam_u16(const std::string &s, const std::string &param_name,
		const uint16_t param_value);
	std::string str_setparam_u32(const std::string &s, const std::string &param_name,
		const uint32_t param_value);
	std::string str_setparam_u64(const std::string &s, const std::string &param_name,
		const uint64_t param_value);

	std::string str_setparam_i8(const std::string &s, const std::string &param_name,
		const int8_t param_value);
	std::string str_setparam_i16(const std::string &s, const std::string &param_name,
		const int16_t param_value);
	std::string str_setparam_i32(const std::string &s, const std::string &param_name,
		const int32_t param_value);
	std::string str_setparam_i64(const std::string &s, const std::string &param_name,
		const int64_t param_value);

	std::string str_setparam_float(const std::string &s, const std::string &param_name,
		const float param_value);
	std::string str_setparam_double(const std::string &s, const std::string &param_name,
		const double param_value);

	std::string str_setparam_ptr(const std::string &s, const std::string &param_name,
		const void * param_value);

	std::string str_setparam_bool(const std::string &s, const std::string &param_name,
		const bool param_value);

	std::string str_setparam_u8arr(const std::string &s, const std::string &param_name,
		const std::vector<uint8_t> &v_arr);
	std::string str_setparam_u16arr(const std::string &s, const std::string &param_name,
		const std::vector<uint16_t> &v_arr);
	std::string str_setparam_u32arr(const std::string &s, const std::string &param_name,
		const std::vector<uint32_t> &v_arr);
	std::string str_setparam_u64arr(const std::string &s, const std::string &param_name,
		const std::vector<uint64_t> &v_arr);

	std::string str_setparam_i8arr(const std::string &s, const std::string &param_name,
		const std::vector<int8_t> &v_arr);
	std::string str_setparam_i16arr(const std::string &s, const std::string &param_name,
		const std::vector<int16_t> &v_arr);
	std::string str_setparam_i32arr(const std::string &s, const std::string &param_name,
		const std::vector<int32_t> &v_arr);
	std::string str_setparam_i64arr(const std::string &s, const std::string &param_name,
		const std::vector<int64_t> &v_arr);

	std::string str_setparam_floatarr(const std::string &s, const std::string &param_name,
		const std::vector<float> &v_arr);
	std::string str_setparam_doublearr(const std::string &s, const std::string &param_name,
		const std::vector<double> &v_arr);

	std::string str_setparam_strarr(const std::string &s, const std::string &param_name,
		const std::vector<std::string> &v_arr);

	// Get a param from param string s, if not exist, return blank string
	std::string str_getparam(const std::string &s, const std::string &param_name);

	uint8_t str_getparam_u8(const std::string &s, const std::string &param_name,
		const uint8_t default_value=0);
	uint16_t str_getparam_u16(const std::string &s, const std::string &param_name,
		const uint16_t default_value=0);
	uint32_t str_getparam_u32(const std::string &s, const std::string &param_name,
		const uint32_t default_value=0);
	uint64_t str_getparam_u64(const std::string &s, const std::string &param_name,
		const uint64_t default_value=0);

	int8_t str_getparam_i8(const std::string &s, const std::string &param_name,
		const int8_t default_value=0);
	int16_t str_getparam_i16(const std::string &s, const std::string &param_name,
		const int16_t default_value=0);
	int32_t str_getparam_i32(const std::string &s, const std::string &param_name,
		const int32_t default_value=0);
	int64_t str_getparam_i64(const std::string &s, const std::string &param_name,
		const int64_t default_value=0);

	float str_getparam_float(const std::string &s, const std::string &param_name,
		const float default_value=0.0);
	double str_getparam_double(const std::string &s, const std::string &param_name,
		const double default_value=0.0);

	void * str_getparam_ptr(const std::string &s, const std::string &param_name,
		void * default_value=NULL);

	bool str_getparam_bool(const std::string &s, const std::string &param_name,
		const bool default_value=false);

	std::vector<uint8_t> str_getparam_u8arr(const std::string &s, const std::string &param_name,
		const uint8_t default_value=0);
	std::vector<uint16_t> str_getparam_u16arr(const std::string &s, const std::string &param_name,
		const uint16_t default_value=0);
	std::vector<uint32_t> str_getparam_u32arr(const std::string &s, const std::string &param_name,
		const uint32_t default_value=0);
	std::vector<uint64_t> str_getparam_u64arr(const std::string &s, const std::string &param_name,
		const uint64_t default_value=0);

	std::vector<int8_t> str_getparam_i8arr(const std::string &s, const std::string &param_name,
		const int8_t default_value=0);
	std::vector<int16_t> str_getparam_i16arr(const std::string &s, const std::string &param_name,
		const int16_t default_value=0);
	std::vector<int32_t> str_getparam_i32arr(const std::string &s, const std::string &param_name,
		const int32_t default_value=0);
	std::vector<int64_t> str_getparam_i64arr(const std::string &s, const std::string &param_name,
		const int64_t default_value=0);

	std::vector<float> str_getparam_floatarr(const std::string &s, const std::string &param_name,
		const float default_value=0);
	std::vector<double> str_getparam_doublearr(const std::string &s, const std::string &param_name,
		const double default_value=0);

	std::vector<std::string> str_getparam_strarr(const std::string &s, const std::string &param_name);

	// Return an error number's ERRNO string (eg. strerrno(EPERM) == "EPERM"), thread-safe
	const char *strerrno(int errnum);
	// Return an error number's DESCRIPTION string
	//   (eg. strerrno(EPERM) == "Operation not permitted"), thread-safe
	const char *strerror(int errnum);

	// Return an signal number's SIGNO string (eg. strsigno(SIGHUP) == "SIGHUP"), thread-safe
	const char *strsigno(int signum);
	// Return an signal number's SIGNO string (eg. strsignal(SIGHUP) == "Hangup"), thread-safe
	const char *strsignal(int signum);

	// Just strlcat and strlcpy
	size_t strlcat(char *dst, const char *src, size_t dstsize);
	size_t strlcpy(char *dst, const char *src, size_t dstsize);

	int memlcmp(const void *s1, size_t n1, const void *s2, size_t n2);

	std::string strrev(const std::string &s);

	// Use kill() to force exit
	void exit(int is_force=0);

	// Get format time string,
	//   fmt is format string, tm is timestamp (0 is now time), is_gmtime==true will use GMT timezone
	std::string time2str(const char *fmt=NULL, time_t tm=0, const bool is_gmtime=false);

	// Encode error message (with errcode, errmsg, errtime) to a single string
	std::string error_encode(const uint32_t errcode, const std::string &errmsg, const double errtime=0.0);
	// Decode error message from a encoded single string
	uint32_t error_decode(const std::string &errdata, std::string *errmsg_p=NULL, double *errtime_p=NULL);
	// Just parse errmsg from a encoded single string
	std::string error_parse_msg(const std::string &errdata);

	ssize_t timed_send(int sockfd, const void *buf, const size_t want_len, double timeout_sec=0.0);
	ssize_t timed_recv(int sockfd, void *buf, const size_t want_len, double timeout_sec=0.0);

	// Put a memory block (with msg_code, size and crc) to fd's current pos
	int fput_block(int fd, const int8_t msg_code, const void *p, const uint32_t size,
		const int flags=0, const double timeout_sec=0.0, const std::string *chkstr_p=NULL);
	// Get a memory block from fd's current pos,
	//   if buf_p==NULL or size < real_need_size,
	//   it will alloc a new memory block use malloc()
	//   size will be message real size after return
	void *fget_block(int fd, int8_t &msg_code, void *buf_p, uint32_t &size,
		const int flags=0, const double timeout_sec=0.0, const std::string *chkstr_p=NULL);

	// Put a protobuf message to fd's current pos
	int fput_protobuf_msg(int fd, const int8_t msg_code, const ::google::protobuf::Message *msg_p,
		const int flags=0, const double timeout_sec=0.0, const std::string *chkstr_p=NULL);

	// Get a protobuf message from fd's current pos
	//   if return's msg_code != param's msg_code, and err_str_p != NULL,
	//   raw data will be save as *err_str_p, and do nothing to msg_p
	int fget_protobuf_msg(int fd, int8_t &msg_code, ::google::protobuf::Message *msg_p,
		std::string *err_str_p=NULL, const int flags=0, const double timeout_sec=0.0,
		const std::string *chkstr_p=NULL);

	// Put a std::string (with msg_code, size and crc) to fd' s current pos
	int fput_str(int fd, const int8_t msg_code, const std::string &s,
		const int flags=0, const double timeout_sec=0.0, const std::string *chkstr_p=NULL);
	// Get a std::string from fd's current pos
	int fget_str(int fd, int8_t &msg_code, std::string &s,
		const int flags=0, const double timeout_sec=0.0, const std::string *chkstr_p=NULL);

	// Set a alarm, at (cur_time + timeout_sec), alarm_func_p(alarm_arg) will be call
	//  if alarm_func_p(alarm_arg) != 0.0, push new alarm at (cur_time + return)
	int set_alarm(double (*alarm_func_p)(void *), void *alarm_arg,
		const double timeout_sec, const bool allow_dup_arg=false);

	std::string nn_addr_merge(const nn_transport_t transport,
		const std::string &hostname, const std::string &srvname);

	int nn_addr_split(const char *addr, nn_transport_t &transport,
		std::string &hostname, std::string &srvname);

	int introsort_r(void *base, size_t nmemb, size_t size, sort_cmp_func_t cmp, void *cmp_arg);

	int qsort_r(void *base, size_t nmemb, size_t size, sort_cmp_func_t cmp, void *cmp_arg);

	int heapsort_r(void *base, size_t nmemb, size_t size, sort_cmp_func_t cmp, void *cmp_arg);

	int mergesort_r(void *base, size_t nmemb, size_t size, sort_cmp_func_t cmp, void *cmp_arg);

	// return src + n
	const void *memread(void *dest, const void *src, size_t n);

	// return dst + n
	void *memwrite(void *dest, const void *src, size_t n);

	int system(const char *command);

	std::vector<std::string> str_split(const std::string &s, const char c, const bool keep_empty=false);

	std::vector<std::string> str_split(const std::string &s, int (*splitter)(int c),
		const bool keep_empty=false);

	std::vector<std::string> str_split(const std::string &s, int (*splitter)(int c, void *arg),
		void *splitter_arg, const bool keep_empty=false);

	double get_cpu_count(const cpu_type_t cpu_type=CPU_TYPE_IDLE, size_t *ncpu=NULL);

	std::string str_trim(const std::string &s);

	int32_t getsockport(int sock, std::string *sock_addr=NULL, const bool get_peer_info=false);

	void delay_exit(const double exit_timeout_sec, const int exit_code=0, const std::string &exit_msg="");

	std::string str_printf(const char *fmt, ...);

	int32_t dice(uint16_t count, uint16_t side, int32_t plus=0);

	inline intmax_t sint_min(const size_t value_size)
	{
		switch (value_size) {
		case sizeof(int8_t):
			return INT8_MIN;
		case sizeof(int16_t):
			return INT16_MIN;
		case sizeof(int32_t):
			return INT32_MIN;
		case sizeof(int64_t):
			return INT64_MIN;
		default:
			return INTMAX_MIN;
		}
	}

	inline intmax_t sint_max(const size_t value_size)
	{
		switch (value_size) {
		case sizeof(int8_t):
			return INT8_MAX;
		case sizeof(int16_t):
			return INT16_MAX;
		case sizeof(int32_t):
			return INT32_MAX;
		case sizeof(int64_t):
			return INT64_MAX;
		default:
			return INTMAX_MAX;
		}
	}

	inline uintmax_t uint_max(const size_t value_size)
	{
		switch (value_size) {
		case sizeof(uint8_t):
			return UINT8_MAX;
		case sizeof(uint16_t):
			return UINT16_MAX;
		case sizeof(uint32_t):
			return UINT32_MAX;
		case sizeof(uint64_t):
			return UINT64_MAX;
		default:
			return UINTMAX_MAX;
		}
	}

	template<typename T>
	bool align_uintarr(T *varr, const size_t vcount, const size_t align_size)
	{
		if (!varr || !vcount)
			return true;
		if (align_size <= 1)
			return true;
		if (align_size > uint_max(sizeof(T)))
			return false;
		uintmax_t max_v = uint_max(sizeof(T)) - align_size;
		for (size_t i=0; i<vcount; i++) {
			if (varr[i] >= max_v)
				return false;
		}
		for (size_t i=0; i<vcount; i++) {
			const size_t val_bias = CR_BOUNDTO(varr[i], align_size) - varr[i];
			size_t val_bias_cur = val_bias;
			if (val_bias_cur) {
				for (size_t j=(i+1); j<vcount; j++) {
					if (varr[j] >= val_bias_cur) {
						varr[j] -= val_bias_cur;
						val_bias_cur = 0;
						break;
					} else {
						val_bias_cur -= varr[j];
						varr[j] = 0;
					}
				}
				varr[i] += val_bias - val_bias_cur;
				if (val_bias_cur)
					break;
			}
		}
		return true;
	}

	template<typename T>
	bool select_uintarr(T *varr, const size_t vcount, uintmax_t skip_size, uintmax_t use_size)
	{
		if (!varr || !vcount)
			return true;
		size_t i=0;
		for (; i<vcount; i++) {
			if (varr[i] <= skip_size) {
				skip_size -= varr[i];
				varr[i] = 0;
			} else {
				varr[i] -= skip_size;
				break;
			}
		}
		for (; i<vcount; i++) {
			if (use_size >= varr[i]) {
				use_size -= varr[i];
			} else {
				varr[i] = use_size;
				use_size = 0;
			}
		}
		return true;
	}

	template<typename T>
	void vector_assign(std::vector<T> &dest, const void * buf, const size_t len)
	{
		const T * src_array = (const T *)buf;
		size_t src_count = len / sizeof(T);
		dest.clear();
		dest.reserve(src_count);
		for (size_t i=0; i<src_count; i++) {
			dest.push_back(src_array[i]);
		}
	}

	template<typename T>
	void vector_append(std::vector<T> &dest, const void * buf, const size_t len)
	{
		const T * src_array = (const T *)buf;
		size_t src_count = len / sizeof(T);
		dest.reserve(dest.size() + src_count);
		for (size_t i=0; i<src_count; i++) {
			dest.push_back(src_array[i]);
		}
	}

	template<typename T>
	inline const T & min(const T & v1, const T & v2)
	{
		if (v1 <= v2) {
			return v1;
		} else {
			return v2;
		}
	}

	template<typename T>
	inline const T & max(const T & v1, const T & v2)
	{
		if (v1 > v2) {
			return v1;
		} else {
			return v2;
		}
	}

	template<typename T>
	inline T limited_range(const T & v, const T & limit_min, const T & limit_max)
	{
		T ret = v;
		if (ret < limit_min)
			ret = limit_min;
		if (ret > limit_max)
			ret = limit_max;
		return ret;
	}

	// Bit-map funcs:

	// Find bit-pos's byte slot pos in bit-map
	inline size_t BMP_BITSLOT(const size_t bit_pos, const size_t min_bit_pos=0,
		const size_t map_size=INT_MAX)
	{
#if defined(DIAGNOSTIC)
		if (bit_pos < min_bit_pos) {
			THROWF("bit_pos == %lu, min_bit_pos == %lu\n",
				(long)bit_pos, (long)min_bit_pos);
		}
#endif // DIAGNOSTIC
		size_t ret = ((bit_pos >> 3) - (min_bit_pos >> 3));
#if defined(DIAGNOSTIC)
		if (ret >= map_size) {
			THROWF("bit_pos == %lu, min_bit_pos == %lu, slot == %ld, map_size == %lu\n",
				(long)bit_pos, (long)min_bit_pos, (long)ret, (long)map_size);
		}
#endif // DIAGNOSTIC
		return ret;
	}

	// Get bit-pos's mask bit in bit-map
	inline uint8_t BMP_BITMASK(const int bit_pos)
	{
		return (1 << (bit_pos & 0x7));
	}

	// Test a bit in bit-map
	inline bool BMP_BITTEST(const void *map_p, const size_t map_size,
		const size_t bit_pos, const size_t min_bit_pos=0)
	{
		if (map_p) {
			return (((const uint8_t*)map_p)
			  [BMP_BITSLOT(bit_pos, min_bit_pos, map_size)] & BMP_BITMASK(bit_pos));
		} else {
			return false;
		}
	}

	// Set a bit in bit-map
	inline void BMP_BITSET(void *map_p, const size_t map_size,
		const size_t bit_pos, const size_t min_bit_pos=0)
	{
		if (map_p) {
			((uint8_t*)map_p)
			  [BMP_BITSLOT(bit_pos, min_bit_pos, map_size)] |= BMP_BITMASK(bit_pos);
		}
	}

	// Clear a bit in bit-map
	inline void BMP_BITCLEAR(void *map_p, const size_t map_size,
		const size_t bit_pos, const size_t min_bit_pos=0)
	{
		if (map_p) {
			((uint8_t*)map_p)[BMP_BITSLOT(bit_pos, min_bit_pos, map_size)] &= ~BMP_BITMASK(bit_pos);
		}
	}

	// Turn a bit in bit-map
	inline void BMP_BITTURN(void *map_p, const size_t map_size,
		const size_t bit_pos, const size_t min_bit_pos=0)
	{
		if (map_p) {
			((uint8_t*)map_p)
			  [BMP_BITSLOT(bit_pos, min_bit_pos, map_size)] ^= BMP_BITMASK(bit_pos);
		}
	}

	// Get bit-pos's mask bit in bit-map
	inline uint8_t BMP_BITMASK_N(const int bit_pos)
	{
		return (0x80 >> (bit_pos & 0x7));
	}

	// Test a bit in bit-map
	inline bool BMP_BITTEST_N(const void *map_p, const size_t map_size,
		const size_t bit_pos, const size_t min_bit_pos=0)
	{
		if (map_p) {
			return (((const uint8_t*)map_p)
			  [BMP_BITSLOT(bit_pos, min_bit_pos, map_size)] & BMP_BITMASK_N(bit_pos));
		} else {
			return false;
		}
	}

	// Set a bit in bit-map
	inline void BMP_BITSET_N(void *map_p, const size_t map_size,
		const size_t bit_pos, const size_t min_bit_pos=0)
	{
		if (map_p) {
			((uint8_t*)map_p)
			  [BMP_BITSLOT(bit_pos, min_bit_pos, map_size)] |= BMP_BITMASK_N(bit_pos);
		}
	}

	// Clear a bit in bit-map
	inline void BMP_BITCLEAR_N(void *map_p, const size_t map_size,
		const size_t bit_pos, const size_t min_bit_pos=0)
	{
		if (map_p) {
			((uint8_t*)map_p)[BMP_BITSLOT(bit_pos, min_bit_pos, map_size)] &=
                          ~BMP_BITMASK_N(bit_pos);
		}
	}

	// Turn a bit in bit-map
	inline void BMP_BITTURN_N(void *map_p, const size_t map_size,
		const size_t bit_pos, const size_t min_bit_pos=0)
	{
		if (map_p) {
			((uint8_t*)map_p)
			  [BMP_BITSLOT(bit_pos, min_bit_pos, map_size)] ^= BMP_BITMASK_N(bit_pos);
		}
	}

	// How many bit-slots needs for bit-map
	inline size_t BMP_BITNSLOTS(const size_t max_bit_pos, const size_t min_bit_pos=0)
	{
		if (max_bit_pos < min_bit_pos) {
#if defined(DIAGNOSTIC)
			THROWF("max_bit_pos == %lu, min_bit_pos == %lu\n",
				(long)max_bit_pos, (long)min_bit_pos);
#endif // DIAGNOSTIC
			return 0;
                }

		return (((max_bit_pos + 8) >> 3) - ((min_bit_pos + 8) >> 3) + 1);
	}

	inline bool test_bit_in_map(const void *map_p, const size_t map_size, const size_t bit_pos)
	{
		return BMP_BITTEST(map_p, map_size, bit_pos);
	}

	inline void set_bit_in_map(void *map_p, const size_t map_size, const size_t bit_pos)
	{
		BMP_BITSET(map_p, map_size, bit_pos);
	}

	inline void clear_bit_in_map(void *map_p, const size_t map_size, const size_t bit_pos)
	{
		BMP_BITCLEAR(map_p, map_size, bit_pos);
	}
};

#endif /* __H_CR_CLASS_H__  */
