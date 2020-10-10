#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <cr_class/cr_timer.h>
#include <cr_class/cr_class.h>

CR_Timer::CR_Timer()
{
    assert(sizeof(void *) >= sizeof(double));

    pthread_key_create(&(this->_time_tmp_store), NULL);

    this->_name.clear();
    this->_p = NULL;

    this->_create_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
    this->_usage_time = 0;
    this->_stop_count = 0;

    DPRINTFX(30, "[%s(%p)]enter\n", this->_name.c_str(), this->_p);
}

CR_Timer::CR_Timer(const std::string &name, const void *p)
{
    assert(sizeof(void *) >= sizeof(double));

    pthread_key_create(&(this->_time_tmp_store), NULL);

    this->Init(name, p);
}

CR_Timer::~CR_Timer()
{
    double exit_time = CR_Class_NS::clock_gettime();

    DPRINTFX(20, "[%s(%p)]leave:%f, %s\n",
      this->_name.c_str(), this->_p, exit_time, this->DebugString().c_str());

    pthread_key_delete(this->_time_tmp_store);
}

void
CR_Timer::Init(const std::string &name, const void *p)
{
    this->_name = name;
    this->_p = p;

    this->_create_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
    this->_usage_time = 0;
    this->_stop_count = 0;

    pthread_key_delete(this->_time_tmp_store);
    pthread_key_create(&(this->_time_tmp_store), NULL);

    DPRINTFX(20, "[%s(%p)]enter:%f\n",
      this->_name.c_str(), this->_p, CR_Class_NS::clock_gettime());
}

std::string
CR_Timer::DebugString()
{
    std::string ret;
    char buf[256];
    double usage_time;
    size_t stop_count;
    double cur_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

    this->_spin.lock();

    usage_time = this->_usage_time;
    stop_count = this->_stop_count;

    this->_spin.unlock();

    int strlen = snprintf(buf, sizeof(buf), "past:%f, usage:%f, count:%llu, avg_usage:%f",
      cur_time - this->_create_time, usage_time, (long long)stop_count, (usage_time / stop_count));

    ret.assign(buf, strlen);

    return ret;
}

void
CR_Timer::Start(double &time_tmp) const
{
    time_tmp = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
}

double
CR_Timer::Stop(const double &time_tmp)
{
    double ret = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - time_tmp;

    this->_spin.lock();

    this->_usage_time += ret;
    this->_stop_count++;

    this->_spin.unlock();

    return ret;
}

void
CR_Timer::Start()
{
    CR_Class_NS::union64_t save_tmp;

    save_tmp.float_v = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

    pthread_setspecific(this->_time_tmp_store, (void*)(save_tmp.uint_v));
}

double
CR_Timer::Stop()
{
    CR_Class_NS::union64_t save_tmp;

    save_tmp.uint_v = (uintptr_t)(pthread_getspecific(this->_time_tmp_store));

    if (!save_tmp.uint_v) {
        DPRINTFX(0, "CR_Timer::Stop() without CR_Timer::Start()\n");
    }

    double ret = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - save_tmp.float_v;

    this->_spin.lock();

    this->_usage_time += ret;
    this->_stop_count++;

    this->_spin.unlock();

    return ret;
}

const std::string &
CR_Timer::GetName() const
{
    return this->_name;
}

const void *
CR_Timer::GetP() const
{
    return this->_p;
}

double
CR_Timer::GetCreateTime() const
{
    return this->_create_time;
}

void
CR_Timer::GetUsage(double &usage_time, size_t &stop_count)
{

    this->_spin.lock();

    usage_time = this->_usage_time;
    stop_count = this->_stop_count;

    this->_spin.unlock();
}
