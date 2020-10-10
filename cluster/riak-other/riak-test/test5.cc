#include <lzma.h>
#include <string>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <limits.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/user.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_addon.h>
#include <cr_class/cr_datacontrol.h>
#include <cr_class/cr_quickhash.h>

#if defined (__linux__)
#include <endian.h>
#endif

typedef union {
    uint64_t msec   : 20;
    uint64_t second : 6;
    uint64_t minute : 6;
    uint64_t hour   : 5;
    uint64_t day    : 5;
    uint64_t month  : 4;
    uint64_t year   : 17;
    uint64_t unused : 1;
} cr_datetime_t;

static int _do_splitter(int c, void *arg);

int main(int argc, char *argv[])
{
	if (argc < 4) {
		fprintf(stderr, "Usage: %s <q | i | u | f | d | s | b | e> <val1> <val2>\n", argv[0]);
		return 1;
	}

	union {
		uint32_t u32;
		uint8_t arr[4];
	} x;

	x.arr[0] = 0x11;     // Lowest-address byte
	x.arr[1] = 0x22;
	x.arr[2] = 0x33;
	x.arr[3] = 0x44;     // Highest-address byte

	printf("x.u32 = 0x%X\n", x.u32);
	printf("htole32(x.u32) = 0x%X, htobe32(x.u32) = 0x%X, htonl(x.u32) = 0x%X\n",
		htole32(x.u32), htobe32(x.u32), htonl(x.u32));

	sockaddr_un server_address;
	printf("sizeof(cr_datetime_t) == %lu, sizeof(__int128) == %lu\n",
		sizeof(cr_datetime_t), sizeof(__int128));
	printf("sizeof(bool) == %lu, sizeof(char) == %lu, sizeof(short) == %lu\n",
		sizeof(bool), sizeof(char), sizeof(short));
	printf("sizeof(int) == %lu, sizeof(long) == %lu, sizeof(long long) == %lu\n",
		sizeof(int), sizeof(long), sizeof(long long));
	printf("sizeof(off_t) == %lu, sizeof(float) == %lu, sizeof(double) == %lu\n",
		sizeof(off_t), sizeof(float), sizeof(double));
	printf("sizeof(long double) == %lu, sizeof(server_address.sun_path) == %lu\n",
		sizeof(long double), sizeof(server_address.sun_path));
	printf("sizeof(union32_t) == %lu, sizeof(union64_t) == %lu\n",
		sizeof(CR_Class_NS::union32_t), sizeof(CR_Class_NS::union64_t));
	printf("INTMAX_MAX==%lld, INTMAX_MIN==%lld\ni642str(INTMAX_MAX)==%s, "
		"i642str(0)==%s, i642str(INTMAX_MIN)==%s\n",
		(long long)INTMAX_MAX, (long long)INTMAX_MIN, CR_Class_NS::i642str(INTMAX_MAX).c_str(),
		CR_Class_NS::i642str(0).c_str(), CR_Class_NS::i642str(INTMAX_MIN).c_str());
	printf("UINTMAX_MAX==%llu, u642str(UINTMAX_MAX)==%s, u642str(0)==%s\n",
		(long long unsigned)UINT64_MAX, CR_Class_NS::u642str(UINT64_MAX).c_str(),
		CR_Class_NS::u642str(0).c_str());

	printf("randstr() = %s\n", CR_Class_NS::randstr().c_str());

	std::vector<int8_t> i8_arr;
	std::vector<int16_t> i16_arr;
	std::vector<int32_t> i32_arr;
	std::vector<int64_t> i64_arr;
	int64_t i1, i2, i3;
	int e1;
	std::vector<uint8_t> u8_arr;
	std::vector<uint16_t> u16_arr;
	std::vector<uint32_t> u32_arr;
	std::vector<uint64_t> u64_arr;
	uint64_t u1, u2;
	std::vector<float> f_arr;
	float f1, f2;
	std::vector<double> d_arr;
	double d1, d2, d_time;
	bool b1, b2;
	std::vector<std::string> s_arr;
	std::string s1, s2, s3;
	CR_Class_NS::union64_t union64_v;
	volatile register int cmp_res;

	switch (argv[1][0]) {
	case 'q' :
		i1 = atoll(argv[2]);
		i2 = atoll(argv[3]);
		printf("%s\n", CR_Class_NS::u642binstr(i1).c_str());
		printf("ffs(0x%016llX)\t== %d\n", (long long)i1, ffs(i1));
		printf("CR_FFS64(0x%016llX)\t== %d\n", (long long)i1, CR_FFS64(i1));
		printf("CR_FLS64(0x%016llX)\t== %d\n", (long long)i1, CR_FLS64(i1));
		printf("CR_FFC64(0x%016llX)\t== %d\n", (long long)i1, CR_FFC64(i1));
		printf("CR_FLC64(0x%016llX)\t== %d\n", (long long)i1, CR_FLC64(i1));
		printf("ROUND2(%lld)\t== %lld\n", (long long)i1, (long long)1 << CR_LOG_2_CEIL(i1));
		i3 = i1;
		d_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
		for (int64_t i=i2; i>0; i--) {
			cmp_res = ffs(i3);
			i3++;
		}
		printf("ffsll usage: %f\n", CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - d_time);
		i3 = i1;
		d_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
		for (int64_t i=i2; i>0; i--) {
			cmp_res = CR_FFS64(i3);
			i3++;
		}
		printf("CR_FFS64 usage: %f\n", CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - d_time);
		i3 = i1;
		d_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
		for (int64_t i=i2; i>0; i--) {
			cmp_res = CR_FLS64(i3);
			i3++;
		}
		printf("CR_FLS64 usage: %f\n", CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - d_time);
		i3 = i1;
		d_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
		for (int64_t i=i2; i>0; i--) {
			cmp_res = CR_FFC64(i3);
			i3++;
		}
		printf("CR_FFC64 usage: %f\n", CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - d_time);
		i3 = i1;
		d_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
		for (int64_t i=i2; i>0; i--) {
			cmp_res = CR_FLC64(i3);
			i3++;
		}
		printf("CR_FLC64 usage: %f\n", CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - d_time);
		break;
	case 'i' :
		i1 = atoll(argv[2]);
		i2 = atoll(argv[3]);
		printf("int val1==%lld, val2==%lld\n", (long long int)i1, (long long int)i2);
		union64_v.int_v = i1;
		printf("(int64_t)(%lld) put to double is %f\n", (long long)i1, union64_v.float_v);
		s1 = CR_Class_NS::i82str(i1);
		s2 = CR_Class_NS::i82str(i2);
		printf("i82str(i1)==%s, i82str(i2)==%s\n", s1.c_str(), s2.c_str());
		printf("str2i8(i1)==%d, str2i8(i2)==%d\n",
		  CR_Class_NS::str2i8(s1), CR_Class_NS::str2i8(s2));
		s1 = CR_Class_NS::i162str(i1);
		s2 = CR_Class_NS::i162str(i2);
		printf("i162str(i1)==%s, i162str(i2)==%s\n", s1.c_str(), s2.c_str());
		printf("str2i16(i1)==%d, str2i16(i2)==%d\n",
		  CR_Class_NS::str2i16(s1), CR_Class_NS::str2i16(s2));
		s1 = CR_Class_NS::i322str(i1);
		s2 = CR_Class_NS::i322str(i2);
		printf("i322str(i1)==%s, i322str(i2)==%s\n", s1.c_str(), s2.c_str());
		printf("str2i32(i1)==%d, str2i32(i2)==%d\n",
		  CR_Class_NS::str2i32(s1), CR_Class_NS::str2i32(s2));
		s1 = CR_Class_NS::i642str(i1);
		s2 = CR_Class_NS::i642str(i2);
		printf("i642str(i1)==%s, i642str(i2)==%s\n", s1.c_str(), s2.c_str());
		printf("str2i64(i1)==%lld, str2i64(i2)==%lld\n",
		  (long long)CR_Class_NS::str2i64(s1), (long long)CR_Class_NS::str2i64(s2));
		s3 = CR_Class_NS::str_setparam_i64(s3, "i1", i1);
		s3 = CR_Class_NS::str_setparam_i64(s3, "i2", i2);
		i8_arr.push_back(i1);
		i8_arr.push_back(i2);
		printf("i8arr2str([i1,i2]==%s)\n", CR_Class_NS::i8arr2str(i8_arr).c_str());
		i16_arr.push_back(i1);
		i16_arr.push_back(i2);
		printf("i16arr2str([i1,i2]==%s)\n", CR_Class_NS::i16arr2str(i16_arr).c_str());
		i32_arr.push_back(i1);
		i32_arr.push_back(i2);
		printf("i32arr2str([i1,i2]==%s)\n", CR_Class_NS::i32arr2str(i32_arr).c_str());
		i64_arr.push_back(i1);
		i64_arr.push_back(i2);
		printf("i64arr2str([i1,i2]==%s)\n", CR_Class_NS::i64arr2str(i64_arr).c_str());
		s3 = CR_Class_NS::str_setparam_i8arr(s3, "i8arr", i8_arr);
		s3 = CR_Class_NS::str_setparam_i16arr(s3, "i16arr", i16_arr);
		s3 = CR_Class_NS::str_setparam_i32arr(s3, "i32arr", i32_arr);
		s3 = CR_Class_NS::str_setparam_i64arr(s3, "i64arr", i64_arr);
		printf("int val1==%lld, val2==%lld, val?==%lld\n",
			(long long int)CR_Class_NS::str_getparam_i64(s3, "i1"),
                        (long long int)CR_Class_NS::str_getparam_i64(s3, "i2"),
			(long long int)CR_Class_NS::str_getparam_i64(s3, "??", 86400)
                );
		break;
	case 'u' :
		u1 = atoll(argv[2]);
		u2 = atoll(argv[3]);
		printf("uint val1==%llu, val2==%llu\n", (long long unsigned int)u1, (long long unsigned int)u2);
		union64_v.uint_v = u1;
		printf("(uint64_t)(%llu) put to double is %f\n", (long long unsigned)u1, union64_v.float_v);
		s1 = CR_Class_NS::u642str(u1);
		s2 = CR_Class_NS::u642str(u2);
		s3 = CR_Class_NS::str_setparam_u64(s3, "u1", u1);
		s3 = CR_Class_NS::str_setparam_u64(s3, "u2", u2);
		u8_arr.push_back(u1);
		u8_arr.push_back(u2);
		printf("i8arr2str([u1,u2]==%s)\n", CR_Class_NS::u8arr2str(u8_arr).c_str());
		u16_arr.push_back(u1);
		u16_arr.push_back(u2);
		printf("i16arr2str([u1,u2]==%s)\n", CR_Class_NS::u16arr2str(u16_arr).c_str());
		u32_arr.push_back(u1);
		u32_arr.push_back(u2);
		printf("i32arr2str([u1,u2]==%s)\n", CR_Class_NS::u32arr2str(u32_arr).c_str());
		u64_arr.push_back(u1);
		u64_arr.push_back(u2);
		printf("i64arr2str([u1,u2]==%s)\n", CR_Class_NS::u64arr2str(u64_arr).c_str());
		s3 = CR_Class_NS::str_setparam_u8arr(s3, "u8arr", u8_arr);
		s3 = CR_Class_NS::str_setparam_u16arr(s3, "u16arr", u16_arr);
		s3 = CR_Class_NS::str_setparam_u32arr(s3, "u32arr", u32_arr);
		s3 = CR_Class_NS::str_setparam_u64arr(s3, "u64arr", u64_arr);
		printf("uint val1==%llu, val2==%llu\n",
			(long long unsigned)CR_Class_NS::str2u64(s1), (long long unsigned)CR_Class_NS::str2u64(s2));
		printf("uint val1==%llu, val2==%llu, val?==%llu\n",
			(long long unsigned)CR_Class_NS::str_getparam_u64(s3, "u1"),
                        (long long unsigned)CR_Class_NS::str_getparam_u64(s3, "u2"),
			(long long unsigned)CR_Class_NS::str_getparam_u64(s3, "??", 86400)
                );
		break;
	case 'f' :
		f1 = atof(argv[2]);
		f2 = atof(argv[3]);
		printf("float val1==%f, val2==%f\n", f1, f2);
		f_arr.push_back(f1);
		f_arr.push_back(f2);
		printf("floatarr2str([f1,f2]==%s)\n", CR_Class_NS::floatarr2str(f_arr).c_str());
		s1 = CR_Class_NS::float2str(f1);
		s2 = CR_Class_NS::float2str(f2);
		s3 = CR_Class_NS::str_setparam_float(s3, "f1", f1);
		s3 = CR_Class_NS::str_setparam_float(s3, "f2", f2);
		s3 = CR_Class_NS::str_setparam_floatarr(s3, "farr", f_arr);
		printf("float val1==%f, val2==%f\n",
			CR_Class_NS::str2float(s1), CR_Class_NS::str2float(s2));
		printf("float val1==%f, val2==%f\n",
			CR_Class_NS::str_getparam_float(s3, "f1"),
                        CR_Class_NS::str_getparam_float(s3, "f2")
                );
		break;
	case 'd' :
		d1 = atof(argv[2]);
		d2 = atof(argv[3]);
		printf("double val1==%f, val2==%f\n", d1, d2);
		union64_v.float_v = f1;
		printf("(double)(%f) put to int64_t is %lld\n", d1, (long long)union64_v.int_v);
		d_arr.push_back(d1);
		d_arr.push_back(d2);
		printf("doublearr2str([d1,d2]==%s)\n", CR_Class_NS::doublearr2str(d_arr).c_str());
		s1 = CR_Class_NS::double2str(d1);
		s2 = CR_Class_NS::double2str(d2);
		s3 = CR_Class_NS::str_setparam_double(s3, "d1", d1);
		s3 = CR_Class_NS::str_setparam_double(s3, "d2", d2);
		s3 = CR_Class_NS::str_setparam_doublearr(s3, "darr", d_arr);
		printf("double val1==%f, val2==%f\n",
			CR_Class_NS::str2double(s1), CR_Class_NS::str2double(s2));
		printf("double val1==%f, val2==%f\n",
			CR_Class_NS::str_getparam_double(s3, "d1"),
                        CR_Class_NS::str_getparam_double(s3, "d2")
                );
		break;
	case 's' :
		s1 = argv[2];
		s2 = argv[3];
		s3 = CR_Class_NS::str_getparam(s1, s2);
		printf("s==%s, s.data()==0x%016llX, p==%s, v=%s, like_cmp=%c\n",
                  s1.c_str(), (long long)s1.data(), s2.c_str(), s3.c_str(),
                  (CR_Class_NS::str_like_cmp(s1, s2)?'Y':'N'));
		printf("str_trim(\"%s\") == \"%s\"(%ld)\n",
                  s1.c_str(), CR_Class_NS::str_trim(s1).c_str(), (long)CR_Class_NS::str_trim(s1).size());
		s_arr = CR_Class_NS::str_split(s1, _do_splitter, &(s2[0]));
		for (unsigned i=0; i<s_arr.size(); i++) {
			printf("s_arr[%u] == \"%s\"\n", i, s_arr[i].c_str());
		}
		printf("md5(\"%s\") == \"%s\"\n", s1.c_str(), CR_QuickHash::md5(s1).c_str());
		printf("md5raw(\"%s\") == \"%s\"\n", s1.c_str(), CR_Class_NS::str2hex(CR_QuickHash::md5raw(s1), true).c_str());
		printf("sha1(\"%s\") == \"%s\"\n", s1.c_str(), CR_QuickHash::sha1(s1).c_str());
		printf("sha1raw(\"%s\") == \"%s\"\n", s1.c_str(), CR_Class_NS::str2hex(CR_QuickHash::sha1raw(s1), true).c_str());
		printf("sha256(\"%s\") == \"%s\"\n", s1.c_str(), CR_QuickHash::sha256(s1).c_str());
		printf("sha256raw(\"%s\") == \"%s\"\n", s1.c_str(), CR_Class_NS::str2hex(CR_QuickHash::sha256raw(s1), true).c_str());
		printf("sha384(\"%s\") == \"%s\"\n", s1.c_str(), CR_QuickHash::sha384(s1).c_str());
		printf("sha384raw(\"%s\") == \"%s\"\n", s1.c_str(), CR_Class_NS::str2hex(CR_QuickHash::sha384raw(s1), true).c_str());
		printf("sha512(\"%s\") == \"%s\"\n", s1.c_str(), CR_QuickHash::sha512(s1).c_str());
		printf("sha512raw(\"%s\") == \"%s\"\n", s1.c_str(), CR_Class_NS::str2hex(CR_QuickHash::sha512raw(s1), true).c_str());
		break;
	case 'b' :
		b1 = atof(argv[2]);
		b2 = atof(argv[3]);
		printf("bool val1==%d, val2==%d\n", b1, b2);
		s1 = CR_Class_NS::bool2str(b1);
		s2 = CR_Class_NS::bool2str(b2);
		s3 = CR_Class_NS::str_setparam_bool(s3, "b1", b1);
		s3 = CR_Class_NS::str_setparam_bool(s3, "b2", b2);
		printf("bool val1==%d, val2==%d\n",
			CR_Class_NS::str2bool(s1), CR_Class_NS::str2bool(s2));
		printf("bool val1==%d, val2==%d\n",
			CR_Class_NS::str_getparam_bool(s3, "b1"),
                        CR_Class_NS::str_getparam_bool(s3, "b2")
                );
		break;
	case 'e' :
		e1 = atoi(argv[2]);
		if (WIFEXITED(e1)) {
			int child_ret = WEXITSTATUS(e1);
			DPRINTF("Child return %d(%s)\n", child_ret, CR_Class_NS::strerrno(child_ret));
		} else if (WIFSIGNALED(e1)) {
			int child_sig = WTERMSIG(e1);
			DPRINTF("Child got signal %d(%s)\n", child_sig, CR_Class_NS::strsignal(child_sig));
		}
		break;
	default :
		printf("Param error\n");
		return -1;
	}

	cmp_res = s1.compare(s2);

	printf("\ns1[%s]\ns2[%s]\ns3[%s]\ncmp_res[%d]\n",
          s1.c_str(), s2.c_str(), s3.c_str(), cmp_res);

	return cmp_res;
}

static int
_do_splitter(int c, void *arg)
{
	if (c == *((char*)arg))
		return 1;
	return 0;
}
