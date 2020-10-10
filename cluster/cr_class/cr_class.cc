#include <cr_class/cr_addon.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <limits.h>
#include <ctype.h>
#include <signal.h>
#include <math.h>
#include <stdarg.h>

#include <snappy-c.h>
#include <zlib.h>
#include <lzma.h>

#include <pthread.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/utsname.h>
#include <sys/sysctl.h>
#include <sys/un.h>

#include <list>
#include <thread>

#include <cr_class/md5.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_file.h>
#include <cr_class/cr_quickhash.h>
#include <cr_class/cr_datacontrol.h>
#include <cr_class/cr_compress.h>

#define _COMPRESS_DATA_PREFIX_NONE  'N'
#define _COMPRESS_DATA_PREFIX_SNAPPY    'S'
#define _COMPRESS_DATA_PREFIX_ZLIB  'Z'
#define _COMPRESS_DATA_PREFIX_LZMA  'L'

#define _ARR2STR_SPLIT_CHAR_UINT    'U'
#define _ARR2STR_SPLIT_CHAR_INT     'I'
#define _ARR2STR_SPLIT_CHAR_REAL    'R'
#define _ARR2STR_SPLIT_CHAR_STRING  'S'

#define _CR_BLOCK_FLAG_CRC      UINT32_C(0x80000000)
#define _CR_BLOCK_FLAG_CHKSTR       UINT32_C(0x40000000)

#define _CR_FD_LOCK_COUNT       (8192)
#define _CR_FD_LOCK_TIMEOUT     (10)

///////////////////////////////

extern std::string __cr_class_ns_str_errno(int errnum);
extern std::string __cr_class_ns_str_signo(int signum);

///////////////////////////////

namespace CR_Class_NS
{

    typedef struct {
        char key[32];
        char value[256];
    } Pair;

    typedef struct ADB {
        double timeout;
        void *alarm_arg;
        double (*alarm_func_p)(void *);
	ADB(){ alarm_arg=NULL;alarm_func_p=NULL;timeout=0;};
	virtual ~ADB(){};
    } ADB_t;

///////////////////////////////

    static void _protection_fork_child_handler(int);
    static void _cr_class_init();
    static void _cr_alarm_loader();
    static void _cr_delay_exit();

///////////////////////////////

    static const char _cr_class_hex_digit_upper[] = "0123456789ABCDEF";
    static const char _cr_class_hex_digit_lower[] = "0123456789abcdef";
    static pthread_once_t _cr_class_init_once = PTHREAD_ONCE_INIT;
    CR_Locked<std::string> dprintfx_filename;
    CR_Locked<std::string> dprintfx_line_prefix;
    CR_DataControl::CondMutex _cr_delay_exit_cmtx;
    double _cr_exit_time = 0.0;
    int _cr_exit_code = 0;
    std::string _cr_exit_msg;
    const std::string blank_string = "";
    static CR_Class_NS::Pair err_array[256];
    static CR_Class_NS::Pair sig_array[256];
    static volatile int _cr_class_debug_value = 10;
    static int _protection_fork_child_status = 0;
    static pid_t _protection_fork_waiting_pid = 0;
    const std::string bool_true_str = "1";
    const std::string bool_false_str = "0";
    CR_DataControl::CondMutex _cr_class_alarm_cmtx;
    //static std::multimap<double,ADB_t> _cr_class_alarm_map;
    static std::vector<ADB_t> _cr_class_alarm_map;
    const static char _nn_transport_inproc_str[] = "inproc://";
    const static char _nn_transport_ipc_str[] = "ipc://";
    const static char _nn_transport_tcp_str[] = "tcp://";
    const static size_t _nn_transport_inproc_len = strlen(_nn_transport_inproc_str);
    const static size_t _nn_transport_ipc_len = strlen(_nn_transport_ipc_str);
    const static size_t _nn_transport_tcp_len = strlen(_nn_transport_tcp_str);
    static std::string _local_uuid_bin;
    static uint64_t _local_uuid_int;
    static CR_DataControl::Mutex _posix_system_mtx;
    static double _cr_timer_clock_gettime_bias = 0.0;
    static CR_DataControl::Mutex _cr_fd_lock_arr[_CR_FD_LOCK_COUNT];

///////////////////////////////

    template<typename T,typename rT>
    rT
    int2uint(const T v)
    {
        switch (sizeof(T)) {
            case sizeof(int8_t) :
                return ((uint8_t)v + INT8_MAX + 1);
            case sizeof(int16_t) :
                return ((uint16_t)v + INT16_MAX + 1);
            case sizeof(int32_t) :
                return ((uint32_t)v + INT32_MAX + 1);
            case sizeof(int64_t) :
                return ((uint64_t)v + INT64_MAX + 1);
            default :
                throw ENOTSUP;
        }
    }

    template<typename T, typename rT>
    rT
    fp2uint(const T v)
    {
        switch (sizeof(T)) {
            case sizeof(uint32_t) : {
                uint32_t *tmp_v_p = (uint32_t *)(&v);
                uint32_t tmp_v = *tmp_v_p;
                if (tmp_v & UINT32_C(0x80000000))
                    tmp_v = ~tmp_v;             // Negative
                else
                    tmp_v |= UINT32_C(0x80000000);      // Positive
                return tmp_v;
            }
            case sizeof(uint64_t) : {
                uint64_t *tmp_v_p = (uint64_t *)(&v);
                uint64_t tmp_v = *tmp_v_p;
                if (tmp_v & UINT64_C(0x8000000000000000))
                    tmp_v = ~tmp_v;             // Negative
                else
                    tmp_v |= UINT64_C(0x8000000000000000);  // Positive
                return tmp_v;
            }
            default :
                throw ENOTSUP;
        }
    }

    template<typename T, typename rT>
    rT
    uint2int(const T v)
    {
        switch (sizeof(T)) {
            case sizeof(uint8_t) :
                return ((int8_t)v - INT8_MAX - 1);
            case sizeof(uint16_t) :
                return ((int16_t)v - INT16_MAX - 1);
            case sizeof(uint32_t) :
                return ((int32_t)v - INT32_MAX - 1);
            case sizeof(uint64_t) :
                return ((int64_t)v - INT64_MAX - 1);
            default :
                throw ENOTSUP;
        }
    }

    template<typename T, typename rT>
    rT
    uint2fp(const T v)
    {
        switch (sizeof(T)) {
            case sizeof(uint32_t) : {
                uint32_t tmp_v;
                uint32_t *tmp_v_p = &tmp_v;
                if (v & UINT32_C(0x80000000))
                    tmp_v = v & (~(UINT32_C(0x80000000)));      // Positive
                else
                    tmp_v = ~v;                     // Negative
                return *((float*)tmp_v_p);
            }
            case sizeof(uint64_t) : {
                uint64_t tmp_v;
                uint64_t *tmp_v_p = &tmp_v;
                if (v & UINT64_C(0x8000000000000000))
                    tmp_v = v & (~(UINT64_C(0x8000000000000000)));  // Positive
                else
                    tmp_v = ~v;                     // Negative
                return *((double*)tmp_v_p);
            }
            default :
                throw ENOTSUP;
        }
    }

    template<typename T>
    std::string
    uint2str(const T v)
    {
        std::string ret;
        char _buf[sizeof(T) * 2];
        register T v_tmp = v;
        register uint8_t c_tmp;

        for (int i=(sizeof(_buf)-1); i>=0; i--) {
            c_tmp = v_tmp & 0xF;
            _buf[i] = _cr_class_hex_digit_upper[c_tmp];
            v_tmp >>= 4;
        }
        ret.assign(_buf, sizeof(_buf));
        return ret;
    }

    template<typename T>
    std::string
    int2str(const T v)
    {
        switch (sizeof(T)) {
            case sizeof(uint8_t) :
                return uint2str<uint8_t>(int2uint<int8_t,uint8_t>(v));
            case sizeof(uint16_t) :
                return uint2str<uint16_t>(int2uint<int16_t,uint16_t>(v));
            case sizeof(uint32_t) :
                return uint2str<uint32_t>(int2uint<int32_t,uint32_t>(v));
            case sizeof(uint64_t) :
                return uint2str<uint64_t>(int2uint<int64_t,uint64_t>(v));
            default :
                throw ENOTSUP;
        }
    }

    template<typename T>
    T
    str2uint(const std::string &s, const T defaule_value)
    {
        T ret;
        char *bad_ptr;
        const char *s_ptr = s.c_str();
        ret = strtoull(s_ptr, &bad_ptr, 16);
        if ((ret == 0) && (bad_ptr == s_ptr))
            return defaule_value;
        return ret;
    }

    template<typename T>
    T
    str2int(const std::string &s, const T defaule_value)
    {
        switch (sizeof(T)) {
            case sizeof(uint8_t) :
                return uint2int<uint8_t,int8_t>(str2uint<uint8_t>(s, int2uint<int8_t,uint8_t>(defaule_value)));
            case sizeof(uint16_t) :
                return uint2int<uint16_t,int16_t>(str2uint<uint16_t>(s, int2uint<int16_t,uint16_t>(defaule_value)));
            case sizeof(uint32_t) :
                return uint2int<uint32_t,int32_t>(str2uint<uint32_t>(s, int2uint<int32_t,uint32_t>(defaule_value)));
            case sizeof(uint64_t) :
                return uint2int<uint64_t,int64_t>(str2uint<uint64_t>(s, int2uint<int64_t,uint64_t>(defaule_value)));
            default :
                throw ENOTSUP;
        }
    }

    template<typename T>
    std::string
    uint2binstr(const T v)
    {
        std::string ret;
        char _buf[sizeof(v) * 8];
        int j = 0;

        for (int i=(sizeof(_buf)-1); i>=0; i--) {
            if (v & ((T)1 << i))
                _buf[j] = '1';
            else
                _buf[j] = '0';
            j++;
        }
        ret.assign(_buf, sizeof(_buf));
        return ret;
    }

    template<typename T>
    std::string
    uintarr2str(const std::vector<T> &v_arr)
    {
        std::string ret;
        size_t arr_size = v_arr.size();

        ret.reserve((sizeof(T) * 2 + 1) * arr_size);
        for (size_t i=0; i<arr_size; i++) {
            ret += uint2str<T>(v_arr[i]);
            ret += _ARR2STR_SPLIT_CHAR_UINT;
        }

        return ret;
    }

    template<typename T>
    std::string
    intarr2str(const std::vector<T> &v_arr)
    {
        std::string ret;
        size_t arr_size = v_arr.size();

        ret.reserve((sizeof(T) * 2 + 1) * arr_size);
        for (size_t i=0; i<arr_size; i++) {
            ret += int2str<T>(v_arr[i]);
            ret += _ARR2STR_SPLIT_CHAR_INT;
        }

        return ret;
    }

    template<typename T>
    std::vector<T>
    str2uintarr(const std::string &s, const T defaule_value)
    {
        std::vector<T> ret;
        size_t begin_pos = 0;
        size_t end_pos = 0;
        std::string s_tmp;

        while (1) {
            end_pos = s.find(_ARR2STR_SPLIT_CHAR_UINT, begin_pos);
            if (end_pos == std::string::npos)
                break;
            s_tmp = s.substr(begin_pos, (end_pos - begin_pos));
            ret.push_back(str2uint<T>(s_tmp, defaule_value));
            begin_pos = end_pos + 1;
        }

        return ret;
    }

    template<typename T>
    std::vector<T>
    str2intarr(const std::string &s, const T defaule_value)
    {
        std::vector<T> ret;
        size_t begin_pos = 0;
        size_t end_pos = 0;
        std::string s_tmp;

        while (1) {
            end_pos = s.find(_ARR2STR_SPLIT_CHAR_INT, begin_pos);
            if (end_pos == std::string::npos)
                break;
            s_tmp = s.substr(begin_pos, (end_pos - begin_pos));
            ret.push_back(str2int<T>(s_tmp, defaule_value));
            begin_pos = end_pos + 1;
        }

        return ret;
    }

///////////////////////////////

    static void
    _cr_class_init()
    {
        int fret;
        const char *_str_tmp;
        struct utsname uname_buf;
        double tmp_time;
        pid_t tmp_pid;
        uint32_t rand_seed = 0;
        char _uuid_buf[16];
        size_t _uuid_buf_size = sizeof(_uuid_buf);
        int _mib[3];

        signal(SIGPIPE, SIG_IGN);
        signal(SIGCHLD, _protection_fork_child_handler);

        _cr_class_debug_value = getenv("CR_DEBUG_LEVEL") ? atoi(getenv("CR_DEBUG_LEVEL")) : 10;

        for (int i=0; i<256; i++) {
            // do errno
            _str_tmp = ::strerror(i);
            CR_Class_NS::strlcpy(err_array[i].key, ::__cr_class_ns_str_errno(i).c_str(), sizeof(err_array[i].key));
            CR_Class_NS::strlcpy(err_array[i].value, _str_tmp, sizeof(err_array[i].value));

            // do signo
            _str_tmp = ::strsignal(i);
            CR_Class_NS::strlcpy(sig_array[i].key, ::__cr_class_ns_str_signo(i).c_str(), sizeof(sig_array[i].key));
            CR_Class_NS::strlcpy(sig_array[i].value, _str_tmp, sizeof(sig_array[i].value));
        }

#if defined (__linux__)
        _mib[0] = CTL_KERN;
        _mib[1] = KERN_RANDOM;
        _mib[2] = RANDOM_BOOT_ID;
        fret = sysctl(_mib, 3, _uuid_buf, &_uuid_buf_size, NULL, 0);
#elif defined (__OpenBSD__)
        _mib[0] = CTL_HW;
        _mib[1] = HW_UUID;
        fret = sysctl(_mib, 2, _uuid_buf, &_uuid_buf_size, NULL, 0);
#else
        _mib[0] = CTL_KERN;
        _mib[1] = KERN_HOSTUUID;
        fret = sysctl(_mib, 2, _uuid_buf, &_uuid_buf_size, NULL, 0);
#endif

        if (fret) {
            FILE *fp = popen("uuid -F bin", "r");
            if (fp) {
                size_t read_count = fread(_uuid_buf, _uuid_buf_size, 1, fp);
                pclose(fp);
                if (read_count > 0) {
                    fret = 0;
                }
            }
        }

        if (!fret) {
            _local_uuid_bin.assign(_uuid_buf, _uuid_buf_size);
            memcpy(&_local_uuid_int, _uuid_buf, sizeof(_local_uuid_int));
            rand_seed = CR_QuickHash::CRC32Hash(_uuid_buf, _uuid_buf_size, rand_seed);
        }

        uname(&uname_buf);

        rand_seed = CR_QuickHash::CRC32Hash(uname_buf.sysname, strlen(uname_buf.sysname), rand_seed);
        rand_seed = CR_QuickHash::CRC32Hash(uname_buf.nodename, strlen(uname_buf.nodename), rand_seed);
        rand_seed = CR_QuickHash::CRC32Hash(uname_buf.release, strlen(uname_buf.release), rand_seed);
        rand_seed = CR_QuickHash::CRC32Hash(uname_buf.version, strlen(uname_buf.version), rand_seed);
        rand_seed = CR_QuickHash::CRC32Hash(uname_buf.machine, strlen(uname_buf.machine), rand_seed);

        tmp_time = CR_Class_NS::clock_gettime();
        rand_seed = CR_QuickHash::CRC32Hash(&tmp_time, sizeof(tmp_time), rand_seed);
        tmp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
        rand_seed = CR_QuickHash::CRC32Hash(&tmp_time, sizeof(tmp_time), rand_seed);
        tmp_pid = ::getpid();
        rand_seed = CR_QuickHash::CRC32Hash(&tmp_pid, sizeof(tmp_pid), rand_seed);

        srand(rand_seed);

        std::thread (_cr_delay_exit).detach();
        std::thread (_cr_alarm_loader).detach();

        double gettime_test_enter = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
        for (size_t i=0; i<100000; i++)
            CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

        _cr_timer_clock_gettime_bias =
            (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - gettime_test_enter) / 100000;
    }

    static void
    _cr_delay_exit()
    {
        double _exit_time;
        int _exit_code;
        std::string _exit_msg;

        while (1) {
            CR_Class_NS::_cr_delay_exit_cmtx.lock();
            _exit_time = CR_Class_NS::_cr_exit_time;
            _exit_code = CR_Class_NS::_cr_exit_code;
            _exit_msg = CR_Class_NS::_cr_exit_msg;
            CR_Class_NS::_cr_delay_exit_cmtx.unlock();
            if ((_exit_time > 0.0) && (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) >= _exit_time)) {
                if (_exit_msg.size() > 0) {
                    DPRINTFX(5, "%s\n", _exit_msg.c_str());
                } else {
                    DPRINTFX(5, "%s\n", CR_Class_NS::strerrno(_exit_code));
                }
                CR_Class_NS::exit(_exit_code);
                return;
            }
            usleep(10000);
        }
    }

    static void
    _cr_alarm_loader()
    {
        //std::multimap<double,ADB_t>::iterator map_it;
        std::vector<ADB_t>::iterator map_it;
        std::list<ADB_t> adb_list;
        double cur_time;
        double fret;
        int _cr_class_alarm_map_size;
        int _cr_class_alarm_map_idx;

        pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);

        while (1) {
            _cr_class_alarm_cmtx.lock();
            _cr_class_alarm_cmtx.wait(0.1);

            cur_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

#if 0     // delete by liujs 
            for (map_it=_cr_class_alarm_map.begin(); map_it!=_cr_class_alarm_map.end(); map_it++) {
                if ((*map_it).first > cur_time)
                    break;
                adb_list.push_back((*map_it).second);
                _cr_class_alarm_map.erase(map_it);
            }
#else
            bool _break_loop = false;
            _cr_class_alarm_map_size = _cr_class_alarm_map.size();
            _cr_class_alarm_map_idx =0;
            while(1){
                _break_loop = false;
                map_it = _cr_class_alarm_map.begin();
                
                if(map_it != _cr_class_alarm_map.end()){
                    _cr_class_alarm_map_idx++;
                    //if ((*map_it).first > cur_time){
                    if ((*map_it).timeout > cur_time){
                        _break_loop= true;
                    }else{
                        //adb_list.push_back((*map_it).second);              
                        adb_list.push_back((*map_it));
                        _cr_class_alarm_map.erase(map_it);  
                        continue;
                    }
                }
                
                if(_cr_class_alarm_map.empty() || _break_loop){
                     break;
                }
            };
#endif

            _cr_class_alarm_cmtx.unlock();

            while (!adb_list.empty()) {
                ADB_t &adb = adb_list.front();
                adb_list.pop_front();
                if (adb.alarm_func_p && adb.alarm_arg) {
                    fret = adb.alarm_func_p(adb.alarm_arg);
                    if (fret != 0.0)
                        CR_Class_NS::set_alarm(adb.alarm_func_p, adb.alarm_arg, fret);
                } else {
                    DPRINTF("adb.alarm_func_p == NULL || adb.alarm_arg == NULL\n");
                }
            }
        }
    }

///////////////////////////////

    int
    get_debug_level()
    {
        pthread_once(&_cr_class_init_once, _cr_class_init);

        volatile int v1 = 1;
        volatile int v2 = 2;

        do {
            v1 = _cr_class_debug_value;
            v2 = _cr_class_debug_value;
        } while (v1 != v2);

        return v1;
    }

    int
    set_debug_level(const int new_value)
    {
        int ret = get_debug_level();
        _cr_class_debug_value = new_value;
        return ret;
    }

    std::string
    get_local_uuid_bin()
    {
        pthread_once(&_cr_class_init_once, _cr_class_init);

        return _local_uuid_bin;
    }

    uint64_t
    get_local_uuid_int()
    {
        pthread_once(&_cr_class_init_once, _cr_class_init);

        return _local_uuid_int;
    }

    uint64_t
    rand()
    {
        uint64_t ret = 0;

        ret |= (((uint64_t)::rand()) & 0xFFFF);
        ret <<= 16;
        ret |= (((uint64_t)::rand()) & 0xFFFF);
        ret <<= 16;
        ret |= (((uint64_t)::rand()) & 0xFFFF);
        ret <<= 16;
        ret |= (((uint64_t)::rand()) & 0xFFFF);

        return ret;
    }

    std::string
    randstr(const size_t loop_count, const bool raw_data)
    {
        pthread_once(&_cr_class_init_once, _cr_class_init);

        std::string rand_str;
        uint64_t rand_val;
        uint32_t pid = htonl(getpid());
        uint32_t tid = htonl((uint32_t)pthread_self());
        uint64_t timev = CR_Class_NS::double2u64(CR_Class_NS::clock_gettime(CLOCK_MONOTONIC));

        rand_str = _local_uuid_bin;
        rand_str.append((const char*)(&pid), sizeof(pid));
        rand_str.append((const char*)(&tid), sizeof(tid));
        rand_str.append((const char*)(&timev), sizeof(timev));

        for (size_t i=0; i<loop_count; i++) {
            rand_val = CR_Class_NS::rand();
            rand_str.append((const char*)(&rand_val), sizeof(rand_val));
        }

        if (raw_data)
            return rand_str;
        else
            return str2hex(rand_str);
    }

    double
    clock_gettime(const clockid_t clk_id, struct timespec *ts_p)
    {
        if ((clk_id != CLOCK_REALTIME) && (clk_id != CLOCK_MONOTONIC)) {
            errno = ENOTSUP;
            return NAN;
        }
        double ret = 0.0;
        int fret;
        struct timespec ts_i;
        struct timespec *ts_p_i;
        if (ts_p) {
            ts_p_i = ts_p;
        } else {
            ts_p_i = &ts_i;
        }
        fret = ::clock_gettime(clk_id, ts_p_i);
        if (!fret) {
            ret = ts_p_i->tv_sec;
            if (ts_p_i->tv_sec >=0)
                ret += ((double)ts_p_i->tv_nsec) / 1000000000;
            else
                ret -= ((double)ts_p_i->tv_nsec) / 1000000000;
        }
        return ret - _cr_timer_clock_gettime_bias;
    }

    int
    clock_settime(const double time_in, const clockid_t clk_id)
    {
        if ((clk_id != CLOCK_REALTIME) && (clk_id != CLOCK_MONOTONIC)) {
            return ENOTSUP;
        }
        int ret = 0;
        struct timespec ts;
        ts.tv_sec = time_in;
        ts.tv_nsec = (time_in - (double)(ts.tv_sec)) * 1000000000;
        if (ts.tv_nsec < 0)
            ts.tv_nsec = 0 - ts.tv_nsec;
        if (::clock_settime(clk_id, &ts) != 0)
            ret = errno;
        return ret;
    }

    uint8_t
    i82u8(const int8_t v)
    {
        return int2uint<int8_t,uint8_t>(v);
    }

    uint16_t
    i162u16(const int16_t v)
    {
        return int2uint<int16_t,uint16_t>(v);
    }

    uint32_t
    i322u32(const int32_t v)
    {
        return int2uint<int32_t,uint32_t>(v);
    }

    uint64_t
    i642u64(const int64_t v)
    {
        return int2uint<int64_t,uint64_t>(v);
    }

    uint32_t
    float2u32(const float v)
    {
        return fp2uint<float,uint32_t>(v);
    }

    uint64_t
    double2u64(const double v)
    {
        return fp2uint<double,uint64_t>(v);
    }

    int8_t
    u82i8(const uint8_t v)
    {
        return uint2int<uint8_t,int8_t>(v);
    }

    int16_t
    u162i16(const uint16_t v)
    {
        return uint2int<uint16_t,int16_t>(v);
    }

    int32_t
    u322i32(const uint32_t v)
    {
        return uint2int<uint32_t,int32_t>(v);
    }

    int64_t
    u642i64(const uint64_t v)
    {
        return uint2int<uint64_t,int64_t>(v);
    }

    float
    u322float(const uint32_t v)
    {
        return uint2fp<uint32_t,float>(v);
    }

    double
    u642double(const uint64_t v)
    {
        return uint2fp<uint64_t,double>(v);
    }

    std::string
    u82binstr(const uint8_t v)
    {
        return uint2binstr<uint8_t>(v);
    }

    std::string
    u162binstr(const uint16_t v)
    {
        return uint2binstr<uint16_t>(v);
    }

    std::string
    u322binstr(const uint32_t v)
    {
        return uint2binstr<uint32_t>(v);
    }

    std::string
    u642binstr(const uint64_t v)
    {
        return uint2binstr<uint64_t>(v);
    }

    std::string
    u82str(const uint8_t v)
    {
        return uint2str<uint8_t>(v);
    }

    std::string
    u162str(const uint16_t v)
    {
        return uint2str<uint16_t>(v);
    }

    std::string
    u322str(const uint32_t v)
    {
        return uint2str<uint32_t>(v);
    }

    std::string
    u642str(const uint64_t v)
    {
        return uint2str<uint64_t>(v);
    }

    std::string
    i82str(const int8_t v)
    {
        return int2str<int8_t>(v);
    }

    std::string
    i162str(const int16_t v)
    {
        return int2str<int16_t>(v);
    }

    std::string
    i322str(const int32_t v)
    {
        return int2str<int32_t>(v);
    }

    std::string
    i642str(const int64_t v)
    {
        return int2str<int64_t>(v);
    }

    std::string
    float2str(const float f)
    {
        return u322str(float2u32(f));
    }

    std::string
    double2str(const double d)
    {
        return u642str(double2u64(d));
    }

    std::string
    ptr2str(const void *p)
    {
        return u642str((uint64_t)p);
    }

    std::string
    bool2str(const bool b)
    {
        if (b)
            return bool_true_str;
        else
            return bool_false_str;
    }

    std::string
    hex2str(const std::string &h)
    {
        std::string ret;
        size_t h_size = h.size();
        const char *h_data = h.data();
        char _h, _i;
        char _c = 0;

        ret.reserve(h_size / 2 + 1);

        for (size_t pos=0; pos<h_size; pos++) {
            _h = h_data[pos];

            if ((_h >= 'a') && (_h <= 'z')) {
                _i = _h - 'a' + 10;
            } else if ((_h >= 'A') && (_h <= 'Z')) {
                _i = _h - 'A' + 10;
            } else if ((_h >= '0') && (_h <= '9')) {
                _i = _h - '0';
            } else {
                return ret;
            }

            if (pos & 0x1) {
                _c |= _i;
                ret += _c;
            } else {
                _c = _i << 4;
            }
        }

        return ret;
    }

    std::string
    u8arr2str(const std::vector<uint8_t> &v_arr)
    {
        return uintarr2str<uint8_t>(v_arr);
    }

    std::string
    u16arr2str(const std::vector<uint16_t> &v_arr)
    {
        return uintarr2str<uint16_t>(v_arr);
    }

    std::string
    u32arr2str(const std::vector<uint32_t> &v_arr)
    {
        return uintarr2str<uint32_t>(v_arr);
    }

    std::string
    u64arr2str(const std::vector<uint64_t> &v_arr)
    {
        return uintarr2str<uint64_t>(v_arr);
    }

    std::string
    i8arr2str(const std::vector<int8_t> &v_arr)
    {
        return intarr2str<int8_t>(v_arr);
    }

    std::string
    i16arr2str(const std::vector<int16_t> &v_arr)
    {
        return intarr2str<int16_t>(v_arr);
    }

    std::string
    i32arr2str(const std::vector<int32_t> &v_arr)
    {
        return intarr2str<int32_t>(v_arr);
    }

    std::string
    i64arr2str(const std::vector<int64_t> &v_arr)
    {
        return intarr2str<int64_t>(v_arr);
    }

    std::string
    floatarr2str(const std::vector<float> &v_arr)
    {
        std::string ret;
        size_t arr_size = v_arr.size();

        ret.reserve((sizeof(float) * 2 + 1) * arr_size);
        for (size_t i=0; i<arr_size; i++) {
            ret += float2str(v_arr[i]);
            ret += _ARR2STR_SPLIT_CHAR_REAL;
        }

        return ret;
    }

    std::string
    doublearr2str(const std::vector<double> &v_arr)
    {
        std::string ret;
        size_t arr_size = v_arr.size();

        ret.reserve((sizeof(double) * 2 + 1) * arr_size);
        for (size_t i=0; i<arr_size; i++) {
            ret += double2str(v_arr[i]);
            ret += _ARR2STR_SPLIT_CHAR_REAL;
        }

        return ret;
    }

    std::string
    strarr2str(const std::vector<std::string> &v_arr)
    {
        std::string ret;
        size_t arr_size = v_arr.size();

        for (size_t i=0; i<arr_size; i++) {
            ret += str2hex(v_arr[i]);
            ret += _ARR2STR_SPLIT_CHAR_STRING;
        }

        return ret;
    }

    uint8_t
    str2u8(const std::string &s, const uint8_t defaule_value)
    {
        return str2uint<uint8_t>(s, defaule_value);
    }

    uint16_t
    str2u16(const std::string &s, const uint16_t defaule_value)
    {
        return str2uint<uint16_t>(s, defaule_value);
    }

    uint32_t
    str2u32(const std::string &s, const uint32_t defaule_value)
    {
        return str2uint<uint32_t>(s, defaule_value);
    }

    uint64_t
    str2u64(const std::string &s, const uint64_t defaule_value)
    {
        return str2uint<uint64_t>(s, defaule_value);
    }

    int8_t
    str2i8(const std::string &s, const int8_t defaule_value)
    {
        return str2int<int8_t>(s, defaule_value);
    }

    int16_t
    str2i16(const std::string &s, const int16_t defaule_value)
    {
        return str2int<int16_t>(s, defaule_value);
    }

    int32_t
    str2i32(const std::string &s, const int32_t defaule_value)
    {
        return str2int<int32_t>(s, defaule_value);
    }

    int64_t
    str2i64(const std::string &s, const int64_t defaule_value)
    {
        return str2int<int64_t>(s, defaule_value);
    }

    float
    str2float(const std::string &s, const float defaule_value)
    {
        return u322float(str2u32(s, float2u32(defaule_value)));
    }

    double
    str2double(const std::string &s, const double defaule_value)
    {
        return u642double(str2u64(s, double2u64(defaule_value)));
    }

    void *
    str2ptr(const std::string &s, const void *default_value)
    {
        return (void *)str2u64(s, (uintptr_t)default_value);
    }

    bool
    str2bool(const std::string &s, const bool default_value)
    {
        if (s == bool_true_str)
            return true;
        else if (s == bool_false_str)
            return false;
        else
            return default_value;
    }

    std::string
    str2hex(const std::string &s)
    {
        return str2hex(s, false);
    }

    std::string
    str2hex(const std::string &s, const bool upper_case)
    {
        std::string ret;
        size_t s_size = s.size();
        const char *s_data = s.data();
        int _c;
        char _byte_buf[2];
        const char *hex_digit_lower;

        if (upper_case)
            hex_digit_lower = _cr_class_hex_digit_upper;
        else
            hex_digit_lower = _cr_class_hex_digit_lower;

        ret.reserve(s.size() * 2);
        for (size_t pos=0; pos<s_size; pos++) {
            _c = s_data[pos];
            _byte_buf[0] = hex_digit_lower[(_c >> 4) & 15];
            _byte_buf[1] = hex_digit_lower[_c & 15];
            ret.append(_byte_buf, 2);
        }

        return ret;
    }

    std::vector<uint8_t>
    str2u8arr(const std::string &s, const uint8_t defaule_value)
    {
        return str2uintarr<uint8_t>(s, defaule_value);
    }

    std::vector<uint16_t>
    str2u16arr(const std::string &s, const uint16_t defaule_value)
    {
        return str2uintarr<uint16_t>(s, defaule_value);
    }

    std::vector<uint32_t>
    str2u32arr(const std::string &s, const uint32_t defaule_value)
    {
        return str2uintarr<uint32_t>(s, defaule_value);
    }

    std::vector<uint64_t>
    str2u64arr(const std::string &s, const uint64_t defaule_value)
    {
        return str2uintarr<uint64_t>(s, defaule_value);
    }

    std::vector<int8_t>
    str2i8arr(const std::string &s, const int8_t defaule_value)
    {
        return str2intarr<int8_t>(s, defaule_value);
    }

    std::vector<int16_t>
    str2i16arr(const std::string &s, const int16_t defaule_value)
    {
        return str2intarr<int16_t>(s, defaule_value);
    }

    std::vector<int32_t>
    str2i32arr(const std::string &s, const int32_t defaule_value)
    {
        return str2intarr<int32_t>(s, defaule_value);
    }

    std::vector<int64_t>
    str2i64arr(const std::string &s, const int64_t defaule_value)
    {
        return str2intarr<int64_t>(s, defaule_value);
    }

    std::vector<float>
    str2floatarr(const std::string &s, const float defaule_value)
    {
        std::vector<float> ret;
        size_t begin_pos = 0;
        size_t end_pos = 0;
        std::string s_tmp;

        while (1) {
            end_pos = s.find(_ARR2STR_SPLIT_CHAR_REAL, begin_pos);
            if (end_pos == std::string::npos)
                break;
            s_tmp = s.substr(begin_pos, (end_pos - begin_pos));
            ret.push_back(str2float(s_tmp, defaule_value));
            begin_pos = end_pos + 1;
        }

        return ret;
    }

    std::vector<double>
    str2doublearr(const std::string &s, const double defaule_value)
    {
        std::vector<double> ret;
        size_t begin_pos = 0;
        size_t end_pos = 0;
        std::string s_tmp;

        while (1) {
            end_pos = s.find(_ARR2STR_SPLIT_CHAR_REAL, begin_pos);
            if (end_pos == std::string::npos)
                break;
            s_tmp = s.substr(begin_pos, (end_pos - begin_pos));
            ret.push_back(str2double(s_tmp, defaule_value));
            begin_pos = end_pos + 1;
        }

        return ret;
    }

    std::vector<std::string>
    str2strarr(const std::string &s)
    {
        std::vector<std::string> ret;
        size_t begin_pos = 0;
        size_t end_pos = 0;
        std::string s_tmp;

        while (1) {
            end_pos = s.find(_ARR2STR_SPLIT_CHAR_STRING, begin_pos);
            if (end_pos == std::string::npos)
                break;
            s_tmp = s.substr(begin_pos, (end_pos - begin_pos));
            ret.push_back(hex2str(s_tmp));
            begin_pos = end_pos + 1;
        }

        return ret;
    }

    std::string
    dirname(const char *path)
    {
        size_t len;
        const char *endp;

        if (path == NULL || *path == '\0')
            return std::string(".");

        endp = path + strlen(path) - 1;
        while (endp > path && *endp == '/')
            endp--;

        while (endp > path && *endp != '/')
            endp--;

        if (endp == path) {
            if (*endp == '/')
                return std::string("/");
            else
                return std::string(".");
        } else {
            do {
                endp--;
            } while (endp > path && *endp == '/');
        }

        len = endp - path + 1;

        return std::string(path, len);
    }

    std::string
    basename(const char *path)
    {
        size_t len;
        const char *endp, *startp;

        if (path == NULL || *path == '\0')
            return std::string(".");

        endp = path + strlen(path) - 1;
        while (endp > path && *endp == '/')
            endp--;

        if (endp == path && *endp == '/')
            return std::string("/");

        startp = endp;
        while (startp > path && *(startp - 1) != '/')
            startp--;

        len = endp - startp + 1;

        return std::string(startp, len);
    }

    int
    mkdir_p(const std::string &dirname, mode_t mode)
    {
        int ret = 0;
        size_t length = dirname.length();
        size_t pos = 0;

        do {
            pos = dirname.find_first_of("/\\", pos);
            if (pos > 0) {
                if (CR_File::mkdir(dirname.substr(0, pos).c_str(), mode) != 0) {
                    if (errno != EEXIST) {
                        ret = errno;
                        break;
                    }
                }
            }
            if (pos == std::string::npos)
                break;
            pos++;
        } while (pos < length);

        return ret;
    }

#if 0
    int
    save_string(const char *pathname, const std::string &content, const mode_t mode)
    {
        if (!pathname)
            return EFAULT;

        int ret = 0;
        int fd;
        ssize_t fret;

        ret = mkdir_p(dirname(pathname), 0750);
        if (ret != 0)
            return ret;

        fd = CR_File::open(pathname, O_WRONLY|O_CREAT|O_TRUNC, mode);
        if (fd < 0)
            return errno;

        fret = CR_File::write(fd, content.data(), content.size());

        CR_File::close(fd);

        if (fret < 0) {
            ret = errno;
            CR_File::unlink(pathname);
        }

        return ret;
    }
#else
    // syncode from huangwei's git
   
int
save_string(const char *pathname, const std::string &content, const mode_t 
mode)
{
    if (!pathname)
        return EFAULT;

    int ret = 0;
    int fd;
    ssize_t fret;

    ret = mkdir_p(dirname(pathname), 0750);
    if (ret != 0)
        return ret;

    std::string pathname_new = pathname;
    std::string pathname_old = pathname_new;

    pathname_new.append(".new-");
    pathname_new.append(CR_Class_NS::randstr());

    pathname_old.append(".old-");
    pathname_old.append(CR_Class_NS::randstr());

    fd = CR_File::open(pathname_new.c_str(), O_WRONLY|O_CREAT|O_TRUNC, mode);
    if (fd < 0)
        return errno;

    fret = CR_File::write(fd, content.data(), content.size());

    CR_File::close(fd);

    if (fret != (ssize_t)content.size()) {
        if (fret < 0)
            ret = errno;
        else
            ret = ENOSPC;
        CR_File::unlink(pathname_new.c_str());
        return ret;
    }

    if (CR_File::rename(pathname, pathname_old.c_str()) < 0) {
        ret = errno;
        if (ret != ENOENT) {
            CR_File::unlink(pathname_new.c_str());
            return ret;
        }
    }

    if (CR_File::rename(pathname_new.c_str(), pathname) < 0) {
        ret = errno;
        CR_File::rename(pathname_old.c_str(), pathname);
        CR_File::unlink(pathname_new.c_str());
        return ret;
    }

    CR_File::unlink(pathname_old.c_str());

    return 0;
}

#endif
    int
    fload_string(int fd, std::string &content)
    {
        if (fd < 0)
            return EINVAL;

        int ret = 0;
        off_t cur_offset;
        ssize_t fsize=0;
        off_t fseeksize;
        char *buf=NULL;

        cur_offset = CR_File::lseek(fd, 0, SEEK_CUR);
        if (cur_offset < 0) {
            ret = errno;
        } else {
            fseeksize = CR_File::lseek(fd, 0, SEEK_END);
            if (fseeksize > 0) {
                buf = new char[fseeksize];
                CR_File::lseek(fd, 0, SEEK_SET);
                fsize = CR_File::read(fd, buf, fseeksize);
                if (fsize < 0) {
                    ret = errno;
                }
            }
            if (fsize > 0) {
                content.assign(buf, fsize);
            } else if (fsize == 0) {
                content.clear();
            }
            if (buf)
                delete []buf;
            CR_File::lseek(fd, cur_offset, SEEK_SET);
        }

        return ret;
    }

    int
    load_string(const char *pathname, std::string &content)
    {
        if (!pathname)
            return EFAULT;

        int fd = CR_File::open(pathname, O_RDONLY, 0);
        if (fd < 0)
            return errno;

        int ret = CR_Class_NS::fload_string(fd, content);

        CR_File::close(fd);

        return ret;
    }

    std::string
    load_string(const char *pathname)
    {
        std::string ret;
        int fret = CR_Class_NS::load_string(pathname, ret);
        errno = fret;
        return ret;
    }

    void
    hexdump(FILE *f, const void *mem, unsigned int len, unsigned int cols, const char *line_prefix)
    {
        if (f == NULL)
            f = stdout;

        unsigned int i, j;

        for(i = 0; i < len + ((len % cols) ? (cols - len % cols) : 0); i++) {
            /* print offset */
            if(i % cols == 0) {
                if (line_prefix) {
                    fprintf(f, "%s0x%08X: ", line_prefix, i);
                } else {
                    fprintf(f, "0x%08X: ", i);
                }
            }

            /* print hex data */
            if(i < len)
                fprintf(f, "%02X ", 0xFF & ((char*)mem)[i]);
            else /* end of block, just aligning for ASCII dump */
                fprintf(f, "   ");

            /* print ASCII dump */
            if(i % cols == (cols - 1)) {
                for(j = i - (cols - 1); j <= i; j++) {
                    if(j >= len) /* end of block, not really printing */
                        putc(' ', f);
                    else if(isprint(((char*)mem)[j])) /* printable char */
                        putc(0xFF & ((char*)mem)[j], f);
                    else /* other char */
                        putc('.', f);
                }
                putc('\n', f);
            }
        }
    }

    bool
    str_like_cmp(const std::string &s, const std::string &pattern,
                 char escape_character, char wildcard_sc, char wildcard_mc)
    {
        return str_like_cmp(s.data(), s.size(), pattern.data(), pattern.size(),
                            escape_character, wildcard_sc, wildcard_mc);
    }

    bool
    str_like_cmp(const char *s, size_t s_len, const char *pattern, size_t pattern_len,
                 char escape_character, char wildcard_sc, char wildcard_mc)
    {
        if(pattern_len == 0)
            return (s_len == 0);

        char processed_pattern[pattern_len];
        char processed_wildcards[pattern_len];
        const char *p = pattern;
        const char *w = pattern;
        const char *v = s;
        bool escaped = false;

        for (size_t i=0; i<pattern_len-1; i++) {
            if ( p[i] == escape_character
                 && ( p[i + 1] == escape_character
                      || p[i + 1] == wildcard_sc
                      || p[i + 1] == wildcard_mc )
               ) {
                escaped = true;
                break;
            }
        }

        // redefine the pattern by processing escape characters
        if(escaped) {
            memset(processed_pattern, 0, sizeof(processed_pattern));
            memset(processed_wildcards, 0, sizeof(processed_wildcards));

            // position of the processed pattern
            size_t i = 0;

            // position of the original pattern
            size_t j = 0;

            while (j < pattern_len) {
                if ( (j < (pattern_len - 1))
                     && p[j] == escape_character
                     && ( p[j + 1] == escape_character
                          || p[j + 1] == wildcard_sc
                          || p[j + 1] == wildcard_mc )
                   ) {
                    j++;
                    processed_pattern[i] = p[j];
                    processed_wildcards[i] = ' ';
                    j++;
                    i++;
                } else {
                    processed_pattern[i] = p[j];
                    if(p[j] == wildcard_sc || p[j] == wildcard_mc) {
                        // copy only wildcards
                        processed_wildcards[i] = p[j];
                    } else {
                        processed_wildcards[i] = ' ';
                    }
                    j++;
                    i++;
                }
            }

            // the rest of pattern buffers are just ignored
            pattern_len = i;

            p = processed_pattern;
            w = processed_wildcards;
        }

        // Pattern processing

        // are we in "after %" mode?
        bool was_wild = false;

        // pattern positions
        size_t cur_p = 0;
        size_t cur_p_beg = 0;

        // source positions
        size_t cur_s = 0;
        size_t cur_s_beg = 0;

        do {
            // first omit all %
            while (cur_p < pattern_len && w[cur_p] == wildcard_mc) {
                was_wild = true;
                cur_p++;
            }

            cur_s_beg = cur_s;
            cur_p_beg = cur_p;

            // internal loop: try to match a part between %...%
            do {
                // find the first match...
                while ((cur_p < pattern_len)
                       && (cur_s < s_len)
                       && ((v[cur_s] == p[cur_p]) || (w[cur_p] == wildcard_sc))
                       && (w[cur_p] != wildcard_mc)) {
                    cur_s++;
                    cur_p++;
                }

                // not matching (loop finished prematurely) - try the next source position
                if ( (cur_s < s_len)
                     && ((cur_p < pattern_len && w[cur_p] != wildcard_mc) || cur_p >= pattern_len) ) {
                    if(!was_wild) {
                        // no % up to now => the first non-matching is critical
                        return false;
                    }

                    // step forward in the source, rewind the matching pointers
                    cur_p = cur_p_beg;
                    cur_s = ++cur_s_beg;
                }

                // end of the source
                if (cur_s == s_len) {
                    while (cur_p < pattern_len) {
                        // Pattern nontrivial yet? No more chances for matching.
                        if (w[cur_p] != wildcard_mc) {
                            return false;
                        }
                        cur_p++;
                    }
                    return true;
                }
                // try the next match position
            } while (cur_p < pattern_len && w[cur_p] != wildcard_mc);
        } while (cur_p < pattern_len && cur_s < s_len);

        return true;
    }

    pid_t
    protection_fork(int *err_no, const std::set<int> *retry_set_ret, const std::set<int> *retry_set_sig)
    {
        pid_t ret = 0;
        bool retry;

        signal(SIGCHLD, _protection_fork_child_handler);

        do {
            retry = false;

            pid_t child_pid = fork();

            ret = child_pid;

            if (child_pid > 0) {        // parent
                DPRINTF("New child pid == %d\n", child_pid);

                int wait_status = 0;

                _protection_fork_child_status = 0;
                _protection_fork_waiting_pid = child_pid;

                while (wait_status == 0) {
                    usleep(1000);
                    wait_status = _protection_fork_child_status;
                }

                DPRINTFX(5, "Child %d wait_status = %d.\n", child_pid, wait_status);

                if (WIFEXITED(wait_status)) {
                    int child_ret = WEXITSTATUS(wait_status);

                    DPRINTFX(5, "Child %d return %d(%s).\n", child_pid, child_ret, CR_Class_NS::strerror(child_ret));

                    if (retry_set_ret) {
                        if (retry_set_ret->find(child_ret) != retry_set_ret->end()) {
                            retry = true;
                        }
                    }
                } else if (WIFSIGNALED(wait_status)) {
                    int child_sig = WTERMSIG(wait_status);

                    DPRINTFX(5, "Child %d got signal %d(%s).\n", child_pid, child_sig, CR_Class_NS::strsignal(child_sig));

                    if (retry_set_sig) {
                        if (retry_set_sig->find(child_sig) != retry_set_sig->end()) {
                            retry = true;
                        }
                    }
                }
            }

            if (retry)
                DPRINTFX(5, "Respawn child\n");
        } while (retry);

        if (err_no)
            *err_no = errno;

        return ret;
    }

    static void
    _protection_fork_child_handler(int sig_type)
    {
        pid_t   pid;
        int     stat;

        while((pid = waitpid(-1, &stat, WNOHANG)) > 0) {
            if (pid == _protection_fork_waiting_pid) {
                _protection_fork_child_status = stat;
            }
        }

        return;
    }

    std::string
    str_setparam(const std::string &s, const std::string &param_name, const std::string &param_value)
    {
        std::map<std::string,std::string> param_map;

        if (str_param_parse(s, &param_map) >= 0) {
            param_map[param_name] = param_value;
            return str_param_merge(param_map);
        }

        return s;
    }

    std::string
    str_setparam_u8(const std::string &s, const std::string &param_name,
                    const uint8_t param_value)
    {
        return str_setparam(s, param_name, u82str(param_value));
    }

    std::string
    str_setparam_u16(const std::string &s, const std::string &param_name,
                     const uint16_t param_value)
    {
        return str_setparam(s, param_name, u162str(param_value));
    }

    std::string
    str_setparam_u32(const std::string &s, const std::string &param_name,
                     const uint32_t param_value)
    {
        return str_setparam(s, param_name, u322str(param_value));
    }

    std::string
    str_setparam_u64(const std::string &s, const std::string &param_name,
                     const uint64_t param_value)
    {
        return str_setparam(s, param_name, u642str(param_value));
    }

    std::string
    str_setparam_i8(const std::string &s, const std::string &param_name,
                    const int8_t param_value)
    {
        return str_setparam(s, param_name, i82str(param_value));
    }

    std::string
    str_setparam_i16(const std::string &s, const std::string &param_name,
                     const int16_t param_value)
    {
        return str_setparam(s, param_name, i162str(param_value));
    }

    std::string
    str_setparam_i32(const std::string &s, const std::string &param_name,
                     const int32_t param_value)
    {
        return str_setparam(s, param_name, i322str(param_value));
    }

    std::string
    str_setparam_i64(const std::string &s, const std::string &param_name,
                     const int64_t param_value)
    {
        return str_setparam(s, param_name, i642str(param_value));
    }

    std::string
    str_setparam_float(const std::string &s, const std::string &param_name,
                       const float param_value)
    {
        return str_setparam(s, param_name, float2str(param_value));
    }

    std::string
    str_setparam_double(const std::string &s, const std::string &param_name,
                        const double param_value)
    {
        return str_setparam(s, param_name, double2str(param_value));
    }

    std::string
    str_setparam_ptr(const std::string &s, const std::string &param_name,
                     const void * param_value)
    {
        return str_setparam(s, param_name, ptr2str(param_value));
    }

    std::string
    str_setparam_bool(const std::string &s, const std::string &param_name,
                      const bool param_value)
    {
        return str_setparam(s, param_name, bool2str(param_value));
    }

    std::string
    str_setparam_u8arr(const std::string &s, const std::string &param_name, const std::vector<uint8_t> &v_arr)
    {
        return str_setparam(s, param_name, u8arr2str(v_arr));
    }

    std::string
    str_setparam_u16arr(const std::string &s, const std::string &param_name, const std::vector<uint16_t> &v_arr)
    {
        return str_setparam(s, param_name, u16arr2str(v_arr));
    }

    std::string
    str_setparam_u32arr(const std::string &s, const std::string &param_name, const std::vector<uint32_t> &v_arr)
    {
        return str_setparam(s, param_name, u32arr2str(v_arr));
    }

    std::string
    str_setparam_u64arr(const std::string &s, const std::string &param_name, const std::vector<uint64_t> &v_arr)
    {
        return str_setparam(s, param_name, u64arr2str(v_arr));
    }

    std::string
    str_setparam_i8arr(const std::string &s, const std::string &param_name, const std::vector<int8_t> &v_arr)
    {
        return str_setparam(s, param_name, i8arr2str(v_arr));
    }

    std::string
    str_setparam_i16arr(const std::string &s, const std::string &param_name, const std::vector<int16_t> &v_arr)
    {
        return str_setparam(s, param_name, i16arr2str(v_arr));
    }

    std::string
    str_setparam_i32arr(const std::string &s, const std::string &param_name, const std::vector<int32_t> &v_arr)
    {
        return str_setparam(s, param_name, i32arr2str(v_arr));
    }

    std::string
    str_setparam_i64arr(const std::string &s, const std::string &param_name, const std::vector<int64_t> &v_arr)
    {
        return str_setparam(s, param_name, i64arr2str(v_arr));
    }

    std::string
    str_setparam_floatarr(const std::string &s, const std::string &param_name, const std::vector<float> &v_arr)
    {
        return str_setparam(s, param_name, floatarr2str(v_arr));
    }

    std::string
    str_setparam_doublearr(const std::string &s, const std::string &param_name, const std::vector<double> &v_arr)
    {
        return str_setparam(s, param_name, doublearr2str(v_arr));
    }

    std::string
    str_setparam_strarr(const std::string &s, const std::string &param_name, const std::vector<std::string> &v_arr)
    {
        return str_setparam(s, param_name, strarr2str(v_arr));
    }

    std::string
    str_getparam(const std::string &s, const std::string &param_name)
    {
        std::string ret;
        std::map<std::string,std::string> param_map;

        if (str_param_parse(s, &param_map) >= 0) {
            ret = param_map[param_name];
        }

        return ret;
    }

    uint8_t
    str_getparam_u8(const std::string &s, const std::string &param_name, const uint8_t default_value)
    {
        return str2u8(str_getparam(s, param_name), default_value);
    }

    uint16_t
    str_getparam_u16(const std::string &s, const std::string &param_name, const uint16_t default_value)
    {
        return str2u16(str_getparam(s, param_name), default_value);
    }

    uint32_t
    str_getparam_u32(const std::string &s, const std::string &param_name, const uint32_t default_value)
    {
        return str2u32(str_getparam(s, param_name), default_value);
    }

    uint64_t
    str_getparam_u64(const std::string &s, const std::string &param_name, const uint64_t default_value)
    {
        return str2u64(str_getparam(s, param_name), default_value);
    }

    int8_t
    str_getparam_i8(const std::string &s, const std::string &param_name, const int8_t default_value)
    {
        return str2i8(str_getparam(s, param_name), default_value);
    }

    int16_t
    str_getparam_i16(const std::string &s, const std::string &param_name, const int16_t default_value)
    {
        return str2i16(str_getparam(s, param_name), default_value);
    }

    int32_t
    str_getparam_i32(const std::string &s, const std::string &param_name, const int32_t default_value)
    {
        return str2i32(str_getparam(s, param_name), default_value);
    }

    int64_t
    str_getparam_i64(const std::string &s, const std::string &param_name, const int64_t default_value)
    {
        return str2i64(str_getparam(s, param_name), default_value);
    }

    float
    str_getparam_float(const std::string &s, const std::string &param_name, const float default_value)
    {
        return str2float(str_getparam(s, param_name), default_value);
    }

    double
    str_getparam_double(const std::string &s, const std::string &param_name, const double default_value)
    {
        return str2double(str_getparam(s, param_name), default_value);
    }

    void *
    str_getparam_ptr(const std::string &s, const std::string &param_name, void * default_value)
    {
        return str2ptr(str_getparam(s, param_name), default_value);
    }

    bool
    str_getparam_bool(const std::string &s, const std::string &param_name,
                      const bool default_value)
    {
        return str2bool(str_getparam(s, param_name), default_value);
    }

    std::vector<uint8_t>
    str_getparam_u8arr(const std::string &s, const std::string &param_name, const uint8_t default_value)
    {
        return str2u8arr(str_getparam(s, param_name), default_value);
    }

    std::vector<uint16_t>
    str_getparam_u16arr(const std::string &s, const std::string &param_name, const uint16_t default_value)
    {
        return str2u16arr(str_getparam(s, param_name), default_value);
    }

    std::vector<uint32_t>
    str_getparam_u32arr(const std::string &s, const std::string &param_name, const uint32_t default_value)
    {
        return str2u32arr(str_getparam(s, param_name), default_value);
    }

    std::vector<uint64_t>
    str_getparam_u64arr(const std::string &s, const std::string &param_name, const uint64_t default_value)
    {
        return str2u64arr(str_getparam(s, param_name), default_value);
    }

    std::vector<int8_t>
    str_getparam_i8arr(const std::string &s, const std::string &param_name, const int8_t default_value)
    {
        return str2i8arr(str_getparam(s, param_name), default_value);
    }

    std::vector<int16_t>
    str_getparam_i16arr(const std::string &s, const std::string &param_name, const int16_t default_value)
    {
        return str2i16arr(str_getparam(s, param_name), default_value);
    }

    std::vector<int32_t>
    str_getparam_i32arr(const std::string &s, const std::string &param_name, const int32_t default_value)
    {
        return str2i32arr(str_getparam(s, param_name), default_value);
    }

    std::vector<int64_t>
    str_getparam_i64arr(const std::string &s, const std::string &param_name, const int64_t default_value)
    {
        return str2i64arr(str_getparam(s, param_name), default_value);
    }

    std::vector<float>
    str_getparam_floatarr(const std::string &s, const std::string &param_name, const float default_value)
    {
        return str2floatarr(str_getparam(s, param_name), default_value);
    }

    std::vector<double>
    str_getparam_doublearr(const std::string &s, const std::string &param_name, const double default_value)
    {
        return str2doublearr(str_getparam(s, param_name), default_value);
    }

    std::vector<std::string>
    str_getparam_strarr(const std::string &s, const std::string &param_name)
    {
        return str2strarr(str_getparam(s, param_name));
    }

    uint32_t
    crc32(const std::string &data, const uint32_t prev_crc)
    {
        return ::crc32(prev_crc, (const Bytef*)(data.data()), data.size());
    }

    const char *
    strerrno(int errnum)
    {
        pthread_once(&_cr_class_init_once, _cr_class_init);

        if (errnum > 255)
            errnum = 255;
        if (errnum < 0)
            errnum = 255;

        return err_array[errnum].key;
    }

    const char *
    strerror(int errnum)
    {
        pthread_once(&_cr_class_init_once, _cr_class_init);

        if (errnum > 255)
            errnum = 255;
        if (errnum < 0)
            errnum = 255;

        return err_array[errnum].value;
    }

    const char *
    strsigno(int signum)
    {
        pthread_once(&_cr_class_init_once, _cr_class_init);

        if (signum > 255)
            signum = 255;
        if (signum < 0)
            signum = 255;

        return sig_array[signum].key;
    }

    const char *
    strsignal(int signum)
    {
        pthread_once(&_cr_class_init_once, _cr_class_init);

        if (signum > 255)
            signum = 255;
        if (signum < 0)
            signum = 255;

        return sig_array[signum].value;
    }

    int
    compressx_auto(const std::string &src, std::string &dst)
    {
        return CR_Compress::compress(src, dst, CR_Compress::CT_AUTO);
    }

    int
    compressx_none(const std::string &src, std::string &dst)
    {
        return CR_Compress::compress(src, dst, CR_Compress::CT_NONE);
    }

    int
    compressx_snappy(const std::string &src, std::string &dst)
    {
        return CR_Compress::compress(src, dst, CR_Compress::CT_SNAPPY);
    }

    int
    compressx_zlib(const std::string &src, std::string &dst)
    {
        return CR_Compress::compress(src, dst, CR_Compress::CT_ZLIB);
    }

    int
    compressx_lzma(const std::string &src, std::string &dst)
    {
        return CR_Compress::compress(src, dst, CR_Compress::CT_LZMA);
    }

    int compressx(const std::string &src, std::string &dst, const int compress_level)
    {
        int compress_level_local = CR_Compress::CT_AUTO;
        int preset = 1;
        switch (compress_level) {
            case 0 :
                compress_level_local = CR_Compress::CT_NONE;
                break;
            case 1 :
                compress_level_local = CR_Compress::CT_LZ4;
                break;
            case 2 :
                compress_level_local = CR_Compress::CT_SNAPPY;
                break;
            case 3 :
                compress_level_local = CR_Compress::CT_ZLIB;
                break;
            case 4 :
                compress_level_local = CR_Compress::CT_LZMA;
                break;
            case 5 :
                compress_level_local = CR_Compress::CT_LZMA;
                preset = 4;
                break;
        }
        return CR_Compress::compress(src, dst, compress_level_local, preset);
    }

    int
    decompressx(const std::string &src, std::string &dst)
    {
        return CR_Compress::decompress(src, dst);
    }

    size_t
    strlcpy(char *dst, const char *src, size_t siz)
    {
#if defined(DIAGNOSTIC)
        if ((!dst) || (!src) || (!siz)) {
            return 0;
        }
#endif // DIAGNOSTIC
        char *d = dst;
        const char *s = src;
        size_t n = siz;

        /* Copy as many bytes as will fit */
        if (n != 0) {
            while (--n != 0) {
                if ((*d++ = *s++) == '\0')
                    break;
            }
        }

        /* Not enough room in dst, add NUL and traverse rest of src */
        if (n == 0) {
            if (siz != 0)
                *d = '\0';              /* NUL-terminate dst */
            while (*s++)
                ;
        }

        return (s - src - 1);    /* count does not include NUL */
    }

    size_t
    strlcat(char *dst, const char *src, size_t siz)
    {
#if defined(DIAGNOSTIC)
        if ((!dst) || (!src) || (!siz)) {
            return 0;
        }
#endif // DIAGNOSTIC
        char *d = dst;
        const char *s = src;
        size_t n = siz;
        size_t dlen;

        /* Find the end of dst and adjust bytes left but don't go past end */
        while (n-- != 0 && *d != '\0')
            d++;
        dlen = d - dst;
        n = siz - dlen;

        if (n == 0)
            return(dlen + strlen(s));
        while (*s != '\0') {
            if (n != 1) {
                *d++ = *s;
                n--;
            }
            s++;
        }
        *d = '\0';

        return (dlen + (s - src));       /* count does not include NUL */
    }

    int
    memlcmp(const void *s1, size_t n1, const void *s2, size_t n2)
    {
        int fret;

        if (s1 == NULL) {
            if (s2 == NULL)
                return 0;
            else
                return -1;
        } else if (s2 == NULL)
            return 1;

        fret = ::memcmp(s1, s2, MIN(n1, n2));

        if (fret != 0)
            return fret;

        if (n1 < n2)
            return -1;
        else if (n1 > n2)
            return 1;
        else
            return 0;
    }

    std::string
    strrev(const std::string &s)
    {
        std::string ret;
        size_t s_size = s.size();
        char *buf = new char[s_size];

        for (size_t i=0; i<s_size; i++) {
            buf[i] = s[s_size - 1 - i];
        }

        ret.assign(buf, s_size);

        delete []buf;

        return ret;
    }

    int
    str_param_parse(const std::string &s, std::map<std::string,std::string> *param_map_p)
    {
        int ret = 0;
        std::map<std::string,std::string> cur_param_map;

        const char *_p = s.data();
        size_t _len = s.size();
        const char *cur_key_p = NULL;
        const char *cur_value_p = NULL;
        size_t cur_key_size = 0;
        size_t cur_value_size = 0;
        bool processing_key = true;
        bool enter_new = true;
        bool bin_key = false;
        bool bin_value = false;
        bool do_pair = false;
        char _c;
        std::string tmp_key;
        std::string tmp_value;

        while (_len) {
            _c = *_p;
            if (processing_key) {
                if (enter_new) {
                    cur_key_p = _p;
                    cur_key_size = 0;
                    cur_value_p = _p;
                    cur_value_size = 0;
                    if (_c == '#') {
                        bin_key = true;
                    }
                    enter_new = false;
                } else if (bin_key && (_c != '=') && (!isxdigit(_c))) {
                    return (0 -EILSEQ);
                }
                if (_c == '=') {
                    processing_key = false;
                    enter_new = true;
                } else {
                    cur_key_size++;
                }
            } else {
                if (enter_new) {
                    cur_value_p = _p;
                    cur_value_size = 0;
                    if (_c == '#') {
                        bin_value = true;
                    }
                    enter_new = false;
                } else if (bin_value && (_c != '&') && (!isxdigit(_c))) {
                    return (0 -EILSEQ);
                }
                if (_c == '&') {
                    processing_key = true;
                    do_pair = true;
                    enter_new = true;
                } else {
                    cur_value_size++;
                }
            }

            if (do_pair || (_len == 1)) {
                if (bin_key) {
                    cur_key_p++;
                    cur_key_size--;
                    tmp_key = hex2str(std::string(cur_key_p, cur_key_size));
                } else {
                    tmp_key.assign(cur_key_p, cur_key_size);
                }
                if (bin_value) {
                    cur_value_p++;
                    cur_value_size--;
                    tmp_value = hex2str(std::string(cur_value_p, cur_value_size));
                } else {
                    tmp_value.assign(cur_value_p, cur_value_size);
                }
                cur_param_map[tmp_key] = tmp_value;
                bin_key = false;
                bin_value = false;
                do_pair = false;
                ret++;
            }

            _p++;
            _len--;
        }

        if (param_map_p) {
            std::map<std::string,std::string>::const_iterator cur_param_map_it;

            for (cur_param_map_it=cur_param_map.begin(); cur_param_map_it!=cur_param_map.end(); cur_param_map_it++) {
                (*param_map_p)[cur_param_map_it->first] = cur_param_map_it->second;
            }
        }

        return ret;
    }

    std::string
    str_param_merge(const std::map<std::string,std::string> &param_map, bool safe_mode)
    {
        std::string ret;
        std::map<std::string,std::string>::const_iterator param_map_it;
        std::string bin_key;
        std::string bin_value;

        size_t _len;
        const char *_p;
        size_t i;
        char _c;

        ret.clear();

        for (param_map_it=param_map.begin(); param_map_it!=param_map.end(); param_map_it++) {
            const std::string &orig_key = param_map_it->first;
            const std::string &orig_value = param_map_it->second;
            const std::string *key_p = &orig_key;
            const std::string *value_p = &orig_value;

            if (safe_mode) {
                _len = key_p->size();
                _p = key_p->data();
                for (i=0; i<_len; i++) {
                    _c = _p[i];
                    if (!isalnum(_c) && (_c != '_') && (_c != '-')) {
                        bin_key = '#';
                        bin_key += str2hex(*key_p);
                        key_p = &bin_key;
                        break;
                    }
                }

                _len = value_p->size();
                _p = value_p->data();
                for (i=0; i<_len; i++) {
                    _c = _p[i];
                    if (!isalnum(_c) && (_c != '_') && (_c != '-')) {
                        bin_value = '#';
                        bin_value += str2hex(*value_p);
                        value_p = &bin_value;
                        break;
                    }
                }

                if (ret.size() > 0) {
                    ret += '&';
                }
                ret += *key_p;
                ret += '=';
                ret += *value_p;
            } else {
                if (ret.size() > 0) {
                    ret += '&';
                }
                ret += *key_p;
                ret += '=';
                ret += *value_p;
            }
        }

        return ret;
    }

    void
    exit(int is_force)
    {
        if (is_force) {
            kill(getpid(), SIGKILL);
        } else {
            signal(SIGINT, SIG_DFL);
            kill(getpid(), SIGINT);
        }

        while (1);
    }

    std::string
    time2str(const char *fmt, time_t tm, const bool is_gmtime)
    {
        std::string ret;
        char str_time[256];
        struct tm s_time;

        if (!fmt)
            fmt = "%Y%m%d%H%M%S";

        if (!tm)
            tm = time(NULL);

        if (is_gmtime)
            gmtime_r(&tm, &s_time);
        else
            localtime_r(&tm, &s_time);

        strftime(str_time, sizeof(str_time), fmt, &s_time);

        ret = str_time;

        return ret;
    }

    std::string
    error_encode(const uint32_t errcode, const std::string &errmsg, const double errtime)
    {
        msgCRError msg_error;
        std::string str_error;

        msg_error.set_errcode(errcode);
        msg_error.set_errmsg(errmsg);
        if (errtime != 0.0) {
            msg_error.set_errtime(errtime);
        }

        msg_error.SerializeToString(&str_error);

        return str_error;
    }

    uint32_t
    error_decode(const std::string &errdata, std::string *errmsg_p, double *errtime_p)
    {
        msgCRError msg_error;

        if (msg_error.ParseFromString(errdata)) {
            if (errmsg_p) {
                *errmsg_p = msg_error.errmsg();
            }

            if (errtime_p) {
                if (msg_error.has_errtime()) {
                    *errtime_p = msg_error.errtime();
                } else {
                    *errtime_p = 0.0;
                }
            }

            return msg_error.errcode();
        }

        return UINT32_MAX;
    }

    std::string
    error_parse_msg(const std::string &errdata)
    {
        msgCRError msg_error;

        if (msg_error.ParseFromString(errdata)) {
            return msg_error.errmsg();
        }

        return 0;
    }

    int
    fput_block(int fd, const int8_t msg_code, const void *p, const uint32_t size,
               const int flags, const double timeout_sec, const std::string *chkstr_p)
    {
        if (fd < 0)
            return EINVAL;

        if (size > UINT32_C(0xFFFFFF0))
            return EMSGSIZE;

        ssize_t fret;
        uint32_t err_chk_val;
        uint32_t err_chk_val_nl = 0;
        uint32_t size_local;
        uint32_t size_nl;
        int8_t headbuf[5];
        off_t old_offset = -1;
        bool with_err_check = (((flags & CR_BLOCKOP_FLAG_ERRCHECK) != 0) || (chkstr_p != NULL));
        bool is_sock_fd = ((flags & CR_BLOCKOP_FLAG_ISSOCKFD) != 0);
        uint32_t flags_in_size = 0;

        if (!is_sock_fd) {
            DPRINTFX(30, "(%d, %d, 0x%016llX, %u, 0x%08X, %f, ...)\n",
                     fd, (int)msg_code, (long long)p, (unsigned)size, flags, timeout_sec);
        }

        if (!is_sock_fd && (fd < _CR_FD_LOCK_COUNT)) {
            int lock_fret = _cr_fd_lock_arr[fd].lock(_CR_FD_LOCK_TIMEOUT);
            if (lock_fret) {
#ifdef DIAGNOSTIC
                THROWF("fd %d lock failed, %s\n", fd, CR_Class_NS::strerrno(lock_fret));
#else
                DPRINTF("fd %d lock failed, %s\n", fd, CR_Class_NS::strerrno(lock_fret));
#endif /* DIAGNOSTIC */
                errno = lock_fret;
                return lock_fret;
            }
        }

        if (!is_sock_fd) {
            old_offset = CR_File::lseek(fd, 0, SEEK_CUR);
            if (old_offset < 0)
                goto errout;
        }

        if (!p) {
            size_local = 0;
        } else {
            size_local = size;
        }

        if (with_err_check) {
            flags_in_size |= _CR_BLOCK_FLAG_CRC;
            err_chk_val = CR_QuickHash::CRC32Hash(&msg_code, sizeof(msg_code));
            err_chk_val = CR_QuickHash::CRC32Hash(p, size, err_chk_val);
            if (chkstr_p) {
                err_chk_val = CR_QuickHash::CRC32Hash(chkstr_p->data(), chkstr_p->size(), err_chk_val);
                flags_in_size |= _CR_BLOCK_FLAG_CHKSTR;
            }
            err_chk_val_nl = htonl(err_chk_val);
            size_local += sizeof(err_chk_val_nl);
        }

        size_local += sizeof(msg_code); // cmd_code
        size_local |= flags_in_size;
        size_nl = htonl(size_local);
        memcpy(headbuf, &size_nl, 4);
        headbuf[4] = msg_code;

        if (is_sock_fd)
            fret = timed_send(fd, &headbuf, sizeof(headbuf), timeout_sec);
        else
            fret = CR_File::write(fd, &headbuf, sizeof(headbuf));

        if (fret < 0) {
            goto errout;
        } else if (fret != sizeof(headbuf)) {
            errno = ENOMSG;
            goto errout;
        }

        if (p) {
            if (is_sock_fd)
                fret = timed_send(fd, p, size, timeout_sec);
            else
                fret = CR_File::write(fd, p, size);

            if (fret < 0) {
                goto errout;
            } else if (fret != size) {
                errno = ENOMSG;
                goto errout;
            }
        }

        if (with_err_check) {
            if (is_sock_fd)
                fret = timed_send(fd, &err_chk_val_nl, sizeof(err_chk_val_nl), timeout_sec);
            else
                fret = CR_File::write(fd, &err_chk_val_nl, sizeof(err_chk_val_nl));

            if (fret < 0) {
                goto errout;
            } else if (fret != sizeof(err_chk_val_nl)) {
                errno = ENOMSG;
                goto errout;
            }
        }

        if (!is_sock_fd && (fd < _CR_FD_LOCK_COUNT))
            _cr_fd_lock_arr[fd].unlock();

        if (!is_sock_fd) {
            DPRINTFX(30, "fd == %d, old_offset == %lld, crc32 == 0x%08X\n",
                     fd, (long long)old_offset, (unsigned)err_chk_val_nl);
        }

        return 0;

    errout:
        if (old_offset >= 0)
            CR_File::lseek(fd, old_offset, SEEK_SET);

        if (!is_sock_fd && (fd < _CR_FD_LOCK_COUNT))
            _cr_fd_lock_arr[fd].unlock();

        if (!is_sock_fd) {
            DPRINTFX(20, "(%d, %d, 0x%016llX, %u, 0x%08X, %f, ...) == %s, old_offset == %lld\n",
                     fd, (int)msg_code, (long long)p, (unsigned)size, flags, timeout_sec,
                     CR_Class_NS::strerrno(errno), (long long)old_offset);
        }

        return errno;
    }

    void *
    fget_block(int fd, int8_t &msg_code, void *buf_p, uint32_t &size,
               const int flags, const double timeout_sec, const std::string *chkstr_p)
    {
        if (fd < 0) {
            errno = EINVAL;
            return NULL;
        }

        ssize_t fret;
        uint32_t err_chk_val = 0;
        uint32_t err_chk_val_nl = 0;
        uint32_t size_nl, size_local = 0;
        bool with_err_check = false;
        int8_t msg_code_local;
        int8_t headbuf[5];
        off_t old_offset = -1;
        void *p = buf_p;
        bool is_sock_fd = ((flags & CR_BLOCKOP_FLAG_ISSOCKFD) != 0);
        bool is_ignore_crc = ((flags & CR_BLOCKOP_FLAG_IGNORECRCERR) != 0);
        uint32_t flags_in_size = 0;

        if (!is_sock_fd) {
            DPRINTFX(30, "(%d, ..., 0x%08X, %f, ...)\n", fd, flags, timeout_sec);
        }

        if (!is_sock_fd && (fd < _CR_FD_LOCK_COUNT)) {
            int lock_fret = _cr_fd_lock_arr[fd].lock(_CR_FD_LOCK_TIMEOUT);
            if (lock_fret) {
#ifdef DIAGNOSTIC
                THROWF("fd %d lock failed, %s\n", fd, CR_Class_NS::strerrno(lock_fret));
#else
                DPRINTF("fd %d lock failed, %s\n", fd, CR_Class_NS::strerrno(lock_fret));
#endif /* DIAGNOSTIC */
                errno = lock_fret;
                return NULL;
            }
        }

        if (!is_sock_fd) {
            old_offset = CR_File::lseek(fd, 0, SEEK_CUR);
            if (old_offset < 0)
                goto errout;
        }

        if (is_sock_fd)
            fret = timed_recv(fd, &headbuf, sizeof(headbuf), timeout_sec);
        else
            fret = CR_File::read(fd, &headbuf, sizeof(headbuf));

        if (fret < 0) {
            goto errout;
        } else if (fret != sizeof(headbuf)) {
            errno = ENOMSG;
            goto errout;
        }

        memcpy(&size_nl, headbuf, sizeof(size_nl));
        msg_code_local = headbuf[4];

        size_local = ntohl(size_nl);
        flags_in_size = size_local & UINT32_C(0xF0000000);
        size_local &= UINT32_C(0xFFFFFFF);

        if ((flags_in_size & _CR_BLOCK_FLAG_CRC) == 0) {
            if (chkstr_p) {
                errno = EPERM;
                goto errout;
            }
            if (size_local < sizeof(msg_code)) {
                errno = EILSEQ;
                goto errout;
            }
            size_local -= sizeof(msg_code);
        } else {
            if (size_local < (sizeof(err_chk_val_nl) + sizeof(msg_code))) {
                errno = EILSEQ;
                goto errout;
            }
            size_local -= (sizeof(err_chk_val_nl) + sizeof(msg_code));
            with_err_check = true;
            if ((flags_in_size & _CR_BLOCK_FLAG_CHKSTR) != 0) {
                if (!chkstr_p) {
                    errno = EPERM;
                    goto errout;
                }
            }
        }

        if ((!p) || (size_local > size)) {
            p = ::malloc(size_local);
            if (!p)
                goto errout;
        }

        if (is_sock_fd)
            fret = timed_recv(fd, p, size_local, timeout_sec);
        else
            fret = CR_File::read(fd, p, size_local);

        if (fret < 0) {
            goto errout;
        } else if (fret != size_local) {
            errno = ENOMSG;
            goto errout;
        }

        if (with_err_check) {
            if (is_sock_fd)
                fret = timed_recv(fd, &err_chk_val_nl, sizeof(err_chk_val_nl), timeout_sec);
            else
                fret = CR_File::read(fd, &err_chk_val_nl, sizeof(err_chk_val_nl));

            if (fret < 0) {
                goto errout;
            } else if (fret != sizeof(err_chk_val_nl)) {
                errno = ENOMSG;
                goto errout;
            }

            err_chk_val = CR_QuickHash::CRC32Hash(&msg_code_local, sizeof(msg_code_local));
            err_chk_val = CR_QuickHash::CRC32Hash(p, size_local, err_chk_val);
            if (chkstr_p) {
                err_chk_val = CR_QuickHash::CRC32Hash(chkstr_p->data(), chkstr_p->size(), err_chk_val);
            }

            if ((htonl(err_chk_val) != err_chk_val_nl) && (!is_ignore_crc)) {
                errno = EFAULT;
                goto errout;
            }
        }

        msg_code = msg_code_local;
        size = size_local;

        if (!is_sock_fd && (fd < _CR_FD_LOCK_COUNT))
            _cr_fd_lock_arr[fd].unlock();

        if (!is_sock_fd) {
            DPRINTFX(30, "fd == %d, old_offset == %lld, crc32 == 0x%08X\n",
                     fd, (long long)old_offset, (unsigned)err_chk_val_nl);
        }

        return p;

    errout:
        if (old_offset >= 0)
            CR_File::lseek(fd, old_offset, SEEK_SET);

        if (!is_sock_fd && (fd < _CR_FD_LOCK_COUNT))
            _cr_fd_lock_arr[fd].unlock();

        if (p && (p != buf_p))
            ::free(p);

        if (!is_sock_fd) {
            DPRINTFX(20, "(%d, ..., 0x%08X, %f, ...) == %s, old_offset == %lld, size_local == %u,"
                     " flags == 0x%08X, crc32_read == 0x%08X, crc32_calc == 0x%08X\n",
                     fd, flags, timeout_sec, CR_Class_NS::strerrno(errno), (long long)old_offset,
                     size_local, flags_in_size, err_chk_val_nl, htonl(err_chk_val));
        }

        return NULL;
    }

    int
    fput_str(int fd, const int8_t msg_code, const std::string &s, const int flags,
             const double timeout_sec, const std::string *chkstr_p)
    {
        return fput_block(fd, msg_code, s.data(), s.size(), flags, timeout_sec, chkstr_p);
    }

    int
    fget_str(int fd, int8_t &msg_code, std::string &s, const int flags,
             const double timeout_sec, const std::string *chkstr_p)
    {
        int ret = 0;
        char *str_p;
        char _str_buf_local[2048];
        uint32_t str_size = sizeof(_str_buf_local);

        str_p = (char*)fget_block(fd, msg_code, _str_buf_local, str_size, flags, timeout_sec, chkstr_p);

        if (str_p) {
            s.assign(str_p, str_size);
            if (str_p != _str_buf_local) {
                ::free(str_p);
            }
        } else
            ret = errno;

        return ret;
    }

    int
    fput_protobuf_msg(int fd, const int8_t msg_code, const ::google::protobuf::Message *msg_p,
                      const int flags, const double timeout_sec, const std::string *chkstr_p)
    {
        std::string s_tmp;

        if (msg_p) {
            if (!msg_p->SerializeToString(&s_tmp))
                return EBADMSG;
        }

        return fput_str(fd, msg_code, s_tmp, flags, timeout_sec, chkstr_p);
    }

    int
    fget_protobuf_msg(int fd, int8_t &msg_code, ::google::protobuf::Message *msg_p,
                      std::string *err_str_p, const int flags, const double timeout_sec, const std::string *chkstr_p)
    {
        int ret = 0;
        void *msg_data = NULL;
        char _msg_buf_local[2048];
        uint32_t msg_size = sizeof(_msg_buf_local);
        int8_t msg_code_local;

        do {
            msg_data = fget_block(fd, msg_code_local, _msg_buf_local, msg_size, flags, timeout_sec, chkstr_p);
            if (!msg_data) {
                ret = errno;
                break;
            }
            if ((msg_code != msg_code_local) && (err_str_p)) {
                err_str_p->assign((const char*)msg_data, msg_size);
            } else {
                if (msg_p) {
                    if (!msg_p->ParseFromArray(msg_data, msg_size))
                        ret = EFAULT;
                }
            }
            msg_code = msg_code_local;
        } while (0);

        if (msg_data && (msg_data != _msg_buf_local))
            ::free(msg_data);

        return ret;
    }

    ssize_t
    timed_send(int sockfd, const void *buf, const size_t want_len, double timeout_sec)
    {
        static const struct timeval new_tv = {0, 500000};
        ssize_t ret_send, send_count;
        double exp_time;
        const uint8_t *p_tmp = (const uint8_t *)buf;
        size_t len = want_len;

        if (sockfd < 0) {
            errno = EINVAL;
            goto errout;
        }

        ::setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO,  &new_tv, sizeof(new_tv));

        exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;

        send_count = 0;
        while (len > 0) {
            if (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) > exp_time) {
                //errno = ETIMEDOUT;
                //goto errout;
                break;
            }
            ret_send = send(sockfd, p_tmp, len, MSG_NOSIGNAL);
            if (ret_send < 0) {
                if ((errno == EINTR) || (errno == EAGAIN)) {
                    continue;
                } else {
                    send_count = -1;
                    goto errout;
                }
            } else if (ret_send > 0) {
                p_tmp += ret_send;
                len -= ret_send;
                send_count += ret_send;
            } else {
                break;
            }
        }

        return send_count;

    errout:

        DPRINTFX(20, "timed_send(%d, %p, %llu, %f), %s\n",
                 sockfd, buf, (long long unsigned)want_len, timeout_sec,
                 CR_Class_NS::strerrno(errno));

        return -1;
    }

    ssize_t
    timed_recv(int sockfd, void *buf, const size_t want_len, double timeout_sec)
    {
        static const struct timeval new_tv = {0, 500000};
        ssize_t recv_count = -1;
        ssize_t recv_ret;
        int8_t *p_tmp = (int8_t *)buf;
        double exp_time;
        size_t len = want_len;

        if (sockfd < 0) {
            errno = EINVAL;
            goto errout;
        }

        ::setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO,  &new_tv, sizeof(new_tv));

        exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;

        recv_count = 0;
        while (len > 0) {
            if (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) > exp_time) {
                //errno = ETIMEDOUT;
                //goto errout;
                break;
            }
            recv_ret = recv(sockfd, p_tmp, len, MSG_WAITALL);
            if (recv_ret < 0) {
                if ((errno == EINTR) || (errno == EAGAIN)) {
                    continue;
                } else {
                    goto errout;
                }
            } else if (recv_ret > 0) {
                p_tmp += recv_ret;
                len -= recv_ret;
                recv_count += recv_ret;
            } else {
                break;
            }
        }

        return recv_count;

    errout:

        DPRINTFX(20, "timed_recv(%d, %p, %llu, %f), %s\n",
                 sockfd, buf, (long long unsigned)want_len, timeout_sec,
                 CR_Class_NS::strerrno(errno));

        return -1;
    }

    int
    set_alarm(double (*alarm_func_p)(void *), void *alarm_arg, double timeout_sec, const bool allow_dup_arg)
    {
        pthread_once(&_cr_class_init_once, _cr_class_init);

        if (!alarm_func_p || !alarm_arg)
            return EINVAL;

        int ret = 0;
        ADB_t adb;
        //std::multimap<double,ADB_t>::iterator map_it;
        std::vector<ADB_t>::iterator map_it;
        adb.alarm_func_p = alarm_func_p;
        adb.alarm_arg = alarm_arg;

        CANCEL_DISABLE_ENTER();

        _cr_class_alarm_cmtx.lock();

        if (!allow_dup_arg) {
            for (map_it=_cr_class_alarm_map.begin(); map_it!=_cr_class_alarm_map.end(); map_it++) {
                //if ((*map_it).second.alarm_arg == alarm_arg) {
                if ((*map_it).alarm_arg == alarm_arg) {
                    ret = EALREADY;
                    break;
                }
            }
        }

        if (!ret) {
            //_cr_class_alarm_map.insert(std::pair<double,ADB_t>(CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec, adb));
            adb.timeout = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;
            _cr_class_alarm_map.push_back(adb);
            _cr_class_alarm_cmtx.broadcast();
        }

        _cr_class_alarm_cmtx.unlock();

        CANCEL_DISABLE_LEAVE();

        return ret;
    }

    std::string
    nn_addr_merge(const nn_transport_t transport, const std::string &hostname, const std::string &srvname)
    {
        std::string ret;

        switch (transport) {
            case NN_INPROC:
                ret.assign(_nn_transport_inproc_str, _nn_transport_inproc_len);
                ret += hostname;
                break;
            case NN_IPC:
                ret.assign(_nn_transport_ipc_str, _nn_transport_ipc_len);
                ret += hostname;
                break;
            case NN_TCP:
                ret.assign(_nn_transport_tcp_str, _nn_transport_tcp_len);
                ret += hostname;
                ret += ":";
                ret += srvname;
                break;
            default:
                break;
        }

        return ret;
    }

    int
    nn_addr_split(const char *addr, nn_transport_t &transport, std::string &hostname, std::string &srvname)
    {
        if (!addr)
            return EFAULT;

        size_t addr_len = strlen(addr);

        if ((addr_len >= _nn_transport_inproc_len)
            && (strncmp(addr, _nn_transport_inproc_str, _nn_transport_inproc_len) == 0)) {
            transport = NN_INPROC;
            hostname.assign(addr + _nn_transport_inproc_len, addr_len - _nn_transport_inproc_len);
        } else if ((addr_len >= _nn_transport_ipc_len)
                   && (strncmp(addr, _nn_transport_ipc_str, _nn_transport_ipc_len) == 0)) {
            transport = NN_IPC;
            hostname.assign(addr + _nn_transport_ipc_len, addr_len - _nn_transport_ipc_len);
        } else if ((addr_len >= _nn_transport_tcp_len)
                   && (strncmp(addr, _nn_transport_tcp_str, _nn_transport_tcp_len) == 0)) {
            transport = NN_TCP;
            const char *addr2 = addr + _nn_transport_tcp_len;
            size_t addr2_len = addr_len - _nn_transport_tcp_len;
            const char *_find_pos = strchr(addr2, ':');
            if (_find_pos) {
                size_t hostname_len = (uintptr_t)_find_pos - (uintptr_t)addr2;
                size_t srvname_len = addr2_len - 1 - hostname_len;
                hostname.assign(addr2, hostname_len);
                if (srvname_len > 0) {
                    srvname.assign(_find_pos + 1, srvname_len);
                } else {
                    srvname.clear();
                }
            } else {
                hostname.assign(addr2, addr2_len);
                srvname.clear();
            }
        } else {
            return EINVAL;
        }

        return 0;
    }

    const void *
    memread(void *dest, const void *src, size_t n)
    {
        ::memcpy(dest, src, n);
        return (const void *)((uintptr_t)src + n);
    }

    void *
    memwrite(void *dest, const void *src, size_t n)
    {
        ::memcpy(dest, src, n);
        return (void *)((uintptr_t)dest + n);
    }

    int
    system(const char *command)
    {
        typedef void (*sighandler_t)(int);

        int ret = 0;
        sighandler_t old_handler;

        _posix_system_mtx.lock();

        old_handler = signal(SIGCHLD, SIG_DFL);
        ret = ::system(command);
        signal(SIGCHLD, old_handler);

        _posix_system_mtx.unlock();

        return ret;
    }

    std::vector<std::string>
    str_split(const std::string &s, const char c, const bool keep_empty)
    {
        size_t cur_pos = 0;
        size_t next_pos;
        std::string s_tmp;
        std::vector<std::string> s_out;

        while (1) {
            next_pos = s.find(c, cur_pos);
            if (next_pos == std::string::npos) {
                s_tmp = s.substr(cur_pos);
            } else {
                s_tmp = s.substr(cur_pos, (next_pos - cur_pos));
            }
            if (keep_empty || (s_tmp.size() > 0))
                s_out.push_back(s_tmp);
            if (next_pos == std::string::npos)
                break;
            cur_pos = next_pos + 1;
        }

        return s_out;
    }

    std::vector<std::string>
    str_split(const std::string &s, int (*splitter)(int c), const bool keep_empty)
    {
        size_t cur_pos = 0;
        size_t next_pos;
        std::string s_tmp;
        std::vector<std::string> s_out;
        const char *sdata = s.data();
        size_t ssize = s.size();

        while (1) {
            next_pos = std::string::npos;
            if (splitter) {
                for (size_t pos_tmp=cur_pos; pos_tmp<ssize; pos_tmp++) {
                    if (splitter(sdata[pos_tmp])) {
                        next_pos = pos_tmp;
                        break;
                    }
                }
            }
            if (next_pos == std::string::npos) {
                s_tmp = s.substr(cur_pos);
            } else {
                s_tmp = s.substr(cur_pos, (next_pos - cur_pos));
            }
            if (keep_empty || (s_tmp.size() > 0))
                s_out.push_back(s_tmp);
            if (next_pos == std::string::npos)
                break;
            cur_pos = next_pos + 1;
        }

        return s_out;
    }

    std::vector<std::string>
    str_split(const std::string &s, int (*splitter)(int c, void *arg), void *splitter_arg, const bool keep_empty)
    {
        size_t cur_pos = 0;
        size_t next_pos;
        std::string s_tmp;
        std::vector<std::string> s_out;
        const char *sdata = s.data();
        size_t ssize = s.size();

        while (1) {
            next_pos = std::string::npos;
            if (splitter) {
                for (size_t pos_tmp=cur_pos; pos_tmp<ssize; pos_tmp++) {
                    if (splitter(sdata[pos_tmp], splitter_arg)) {
                        next_pos = pos_tmp;
                        break;
                    }
                }
            }
            if (next_pos == std::string::npos) {
                s_tmp = s.substr(cur_pos);
            } else {
                s_tmp = s.substr(cur_pos, (next_pos - cur_pos));
            }
            if (keep_empty || (s_tmp.size() > 0))
                s_out.push_back(s_tmp);
            if (next_pos == std::string::npos)
                break;
            cur_pos = next_pos + 1;
        }

        return s_out;
    }

    double
    get_cpu_count(const cpu_type_t cpu_type, size_t *ncpu)
    {
        double ret = 0.0;
        FILE *fp = ::fopen("/proc/stat","rt");
        char *line = NULL;
        size_t len = 0;
        long long user = 1;
        long long nice = 0;
        long long system = 0;
        long long idle = 0;
        long long iowait = 0;
        long long irq = 0;
        long long softirq = 0;
        size_t cpuct = 0;
        int sys_cpuct = -1;
        double cpu_all = 0.0;

        if (fp) {
            while (::getline(&line, &len, fp) != -1) {
                if (::strncmp(line, "cpu ", 4) == 0) {
                    sscanf(line + 5, "%lld %lld %lld %lld %lld %lld %lld",
                           &user, &nice, &system, &idle, &iowait, &irq, &softirq);
                    if(sys_cpuct != -1)
                        break;
                } else if (strncmp(line, "cpu", 3) == 0) {
                    sys_cpuct++;
                } else {
                    break;
                }
            }
            cpuct = sys_cpuct;
            if (cpuct == 0)
                cpuct = 1;
            if (line)
                free(line);
            if (ncpu)
                *ncpu = cpuct;
            fclose(fp);
        } else {
            return -1;
        }

        cpu_all = (double)user + nice + system + idle + iowait + irq + softirq;

        switch (cpu_type) {
            case CPU_TYPE_USER :
                ret = (double)cpuct * user / cpu_all;
                break;
            case CPU_TYPE_NICE :
                ret = (double)cpuct * nice / cpu_all;
                break;
            case CPU_TYPE_SYSTEM :
                ret = (double)cpuct * system / cpu_all;
                break;
            case CPU_TYPE_IDLE :
                ret = (double)cpuct * idle / cpu_all;
                break;
            case CPU_TYPE_IOWAIT :
                ret = (double)cpuct * iowait / cpu_all;
                break;
            case CPU_TYPE_IRQ :
                ret = (double)cpuct * irq / cpu_all;
                break;
            case CPU_TYPE_SOFTIRQ :
                ret = (double)cpuct * softirq / cpu_all;
                break;
            default :
                ret = -1;
                errno = EINVAL;
                break;
        }

        return ret;
    }

    std::string
    str_trim(const std::string &s)
    {
        std::string ret;
        const char *cp_l = s.data();
        const char *cp_r = cp_l + s.size();

        for (; cp_l <= cp_r; cp_l++) {
            if (!isspace(*cp_l))
                break;
        }

        for (; cp_r >= cp_l; cp_r--) {
            if (*cp_r && !isspace(*cp_r))
                break;
        }

        ret.assign(cp_l, cp_r - cp_l + 1);

        return ret;
    }

    int32_t
    getsockport(int sock, std::string *sock_addr, const bool get_peer_info)
    {
        int32_t ret = -1;
        struct sockaddr_storage ss;
        socklen_t ss_len = sizeof(ss);
        char addr_buf[64];
        int fret;

        if (get_peer_info) {
            fret = ::getpeername(sock, (struct sockaddr *)(&ss), &ss_len);
        } else {
            fret = ::getsockname(sock, (struct sockaddr *)(&ss), &ss_len);
        }

        if (fret == 0) {
            switch (ss.ss_family) {
                case AF_UNIX :
                    ret = 0;
                    if (sock_addr) {
                        struct sockaddr_un *sun_p = (struct sockaddr_un *)(&ss);
                        sock_addr->assign(sun_p->sun_path, strnlen(sun_p->sun_path, sizeof(sun_p->sun_path)));
                    }
                    break;
                case AF_INET : {
                    struct sockaddr_in *sin_p = (struct sockaddr_in *)(&ss);
                    ret = ntohs(sin_p->sin_port);
                    if (sock_addr) {
                        if (inet_ntop(ss.ss_family, &(sin_p->sin_addr), addr_buf, sizeof(addr_buf)) != NULL) {
                            sock_addr->assign(addr_buf);
                        }
                    }
                }
                break;
                case AF_INET6 : {
                    struct sockaddr_in6 *sin6_p = (struct sockaddr_in6 *)(&ss);
                    ret = ntohs(sin6_p->sin6_port);
                    if (sock_addr) {
                        if (inet_ntop(ss.ss_family, &(sin6_p->sin6_addr), addr_buf, sizeof(addr_buf)) != NULL) {
                            sock_addr->assign(addr_buf);
                        }
                    }
                }
                break;
                default :
                    errno = EAFNOSUPPORT;
                    break;
            }
        }

        return ret;
    }

    void
    delay_exit(const double exit_timeout_sec, const int exit_code, const std::string &exit_msg)
    {
        pthread_once(&_cr_class_init_once, _cr_class_init);

        double _exit_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + exit_timeout_sec;
        CR_Class_NS::_cr_delay_exit_cmtx.lock();
        if ((CR_Class_NS::_cr_exit_code == 0) || (CR_Class_NS::_cr_exit_time <= 0.0)) {
            CR_Class_NS::_cr_exit_code = exit_code;
            CR_Class_NS::_cr_exit_time = _exit_time;
            CR_Class_NS::_cr_exit_msg = exit_msg;
        }
        CR_Class_NS::_cr_delay_exit_cmtx.unlock();
    }

    std::string
    str_printf(const char *fmt, ...)
    {
        va_list ap;
        std::string ret;
        int sz;
        char *buf = NULL;

        va_start(ap, fmt);
        sz = ::vasprintf(&buf, fmt, ap);
        va_end(ap);
        if (buf && (sz >= 0)) {
            ret.assign(buf, sz);
            free(buf);
        }

        return ret;
    }

    uint64_t
    crc64(const std::string &data, uint64_t crc)
    {
        return CR_QuickHash::CRC64Hash(data.data(), data.size(), crc);
    }

    int32_t
    dice(uint16_t count, uint16_t side, int32_t plus)
    {
        int32_t ret = 0;
        for (uint32_t i=0; i<count; i++) {
            ret += rand() % side;
        }
        ret += plus;
        return ret;
    }

} /* namespace CR_Class_NS */
