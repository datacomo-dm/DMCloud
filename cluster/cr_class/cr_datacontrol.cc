#include <cr_class/cr_class.h>
#include <cr_class/cr_datacontrol.h>

////////////////////////////////

pthread_mutexattr_t *cr_class_mutex_attr_p = NULL;
pthread_condattr_t *cr_class_cond_attr_p = NULL;

static pthread_mutexattr_t cr_class_mutex_attr;
static pthread_condattr_t cr_class_cond_attr;

static pthread_once_t cr_class_attr_init_once = PTHREAD_ONCE_INIT;

static void cr_class_attr_init(void);

////////////////////////////////

static void
cr_class_attr_init(void)
{
    pthread_mutexattr_init(&cr_class_mutex_attr);
    pthread_mutexattr_settype(&cr_class_mutex_attr, PTHREAD_MUTEX_ERRORCHECK);

    pthread_condattr_init(&cr_class_cond_attr);
    pthread_condattr_setclock(&cr_class_cond_attr, CLOCK_MONOTONIC);

    cr_class_mutex_attr_p = &cr_class_mutex_attr;
    cr_class_cond_attr_p = &cr_class_cond_attr;
}

////////////////////////////////

struct timespec &
CR_DataControl::timespec_add(struct timespec &ts, const int64_t time_usec)
{
    ts.tv_sec += time_usec / (1000000);
    if (time_usec >= 0) {
        ts.tv_nsec += (time_usec % 1000000) * 1000;
        if (ts.tv_nsec > 1000000000) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000;
        }
    } else {
        ts.tv_nsec -= (time_usec % 1000000) * 1000;
        if (ts.tv_nsec < 0) {
            ts.tv_sec--;
            ts.tv_nsec += 1000000000;
        }
    }

    return ts;
}

int
CR_DataControl::timespec_cmp(const struct timespec &ts1, const struct timespec &ts2)
{
    if (ts1.tv_sec < ts2.tv_sec)
        return -1;

    if (ts1.tv_sec > ts2.tv_sec)
        return 1;

    if (ts1.tv_nsec < ts2.tv_nsec)
        return -1;

    if (ts1.tv_nsec > ts2.tv_nsec)
        return 1;

    return 0;
}

struct timeval &
CR_DataControl::timeval_add(struct timeval &tv, const int64_t time_usec)
{
    tv.tv_sec += time_usec / (1000000);
    if (time_usec >= 0) {
        tv.tv_usec += time_usec % 1000000;
        if (tv.tv_usec > 1000000) {
            tv.tv_sec++;
            tv.tv_usec -= 1000000;
        }
    } else {
        tv.tv_usec -= time_usec % 1000000;
        if (tv.tv_usec < 0) {
            tv.tv_sec--;
            tv.tv_usec += 1000000;
        }
    }

    return tv;
}

int
CR_DataControl::timeval_cmp(const struct timeval &tv1, const struct timeval &tv2)
{
    if (tv1.tv_sec < tv2.tv_sec)
        return -1;

    if (tv1.tv_sec > tv2.tv_sec)
        return 1;

    if (tv1.tv_usec < tv2.tv_usec)
        return -1;

    if (tv1.tv_usec > tv2.tv_usec)
        return 1;

    return 0;
}

////////////////////////////////

CR_DataControl::Spin::Spin()
{
    pthread_spin_init(&(this->_spin), 0);
}

CR_DataControl::Spin::~Spin()
{
    pthread_spin_destroy(&(this->_spin));
}

int
CR_DataControl::Spin::lock()
{
    return pthread_spin_lock(&(this->_spin));
}

int
CR_DataControl::Spin::trylock()
{
    return pthread_spin_trylock(&(this->_spin));
}

int
CR_DataControl::Spin::unlock()
{
    return pthread_spin_unlock(&(this->_spin));
}

////////////////////////////////

CR_DataControl::Mutex::Mutex()
{
    pthread_once(&cr_class_attr_init_once, cr_class_attr_init);
    pthread_mutex_init(&(this->_mutex), cr_class_mutex_attr_p);
}

CR_DataControl::Mutex::~Mutex()
{
    pthread_mutex_destroy(&(this->_mutex));
}

int
CR_DataControl::Mutex::lock(double timeout_sec)
{
    int ret;
    struct timespec ts;

    if (timeout_sec <= 0.0) {
        ret = pthread_mutex_lock(&(this->_mutex));
    } else {
        ::clock_gettime(CLOCK_REALTIME, &ts);
        CR_DataControl::timespec_add(ts, (int64_t)(timeout_sec * 1000000));
        ret = pthread_mutex_timedlock(&(this->_mutex), &ts);
    }

    return ret;
}

int
CR_DataControl::Mutex::trylock()
{
    return pthread_mutex_trylock(&(this->_mutex));
}

int
CR_DataControl::Mutex::unlock()
{
    return pthread_mutex_unlock(&(this->_mutex));
}

////////////////////////////////

CR_DataControl::RWLock::RWLock()
{
    pthread_once(&cr_class_attr_init_once, cr_class_attr_init);
    pthread_rwlock_init(&(this->_rwlock), NULL);
}

CR_DataControl::RWLock::~RWLock()
{
    pthread_rwlock_destroy(&(this->_rwlock));
}

int
CR_DataControl::RWLock::rdlock(double timeout_sec)
{
    int ret;
    struct timespec ts;

    if (timeout_sec <= 0.0) {
        ret = pthread_rwlock_rdlock(&(this->_rwlock));
    } else {
        ::clock_gettime(CLOCK_REALTIME, &ts);
        CR_DataControl::timespec_add(ts, (int64_t)(timeout_sec * 1000000));
        ret = pthread_rwlock_timedrdlock(&(this->_rwlock), &ts);
    }

    return ret;
}

int
CR_DataControl::RWLock::tryrdlock()
{
    return pthread_rwlock_tryrdlock(&(this->_rwlock));
}

int
CR_DataControl::RWLock::wrlock(double timeout_sec)
{
    int ret;
    struct timespec ts;

    if (timeout_sec <= 0.0) {
        ret = pthread_rwlock_wrlock(&(this->_rwlock));
    } else {
        ::clock_gettime(CLOCK_REALTIME, &ts);
        CR_DataControl::timespec_add(ts, (int64_t)(timeout_sec * 1000000));
        ret = pthread_rwlock_timedwrlock(&(this->_rwlock), &ts);
    }

    return ret;
}

int
CR_DataControl::RWLock::trywrlock()
{
    return pthread_rwlock_trywrlock(&(this->_rwlock));
}

int
CR_DataControl::RWLock::unlock()
{
    return pthread_rwlock_unlock(&(this->_rwlock));
}

////////////////////////////////

CR_DataControl::CondMutex::CondMutex()
{
    pthread_once(&cr_class_attr_init_once, cr_class_attr_init);
    pthread_mutex_init(&(this->_mutex), cr_class_mutex_attr_p);
    pthread_cond_init(&(this->_cond), cr_class_cond_attr_p);
}

CR_DataControl::CondMutex::~CondMutex()
{
    pthread_cond_destroy(&(this->_cond));
    pthread_mutex_destroy(&(this->_mutex));
}

int
CR_DataControl::CondMutex::lock(double timeout_sec)
{
    int ret;
    struct timespec ts;

    if (timeout_sec <= 0.0) {
        ret = pthread_mutex_lock(&(this->_mutex));
    } else {
        ::clock_gettime(CLOCK_REALTIME, &ts);
        CR_DataControl::timespec_add(ts, (int64_t)(timeout_sec * 1000000));
        ret = pthread_mutex_timedlock(&(this->_mutex), &ts);
    }

    return ret;
}

int
CR_DataControl::CondMutex::trylock()
{
    return pthread_mutex_trylock(&(this->_mutex));
}

int
CR_DataControl::CondMutex::unlock()
{
    return pthread_mutex_unlock(&(this->_mutex));
}

int
CR_DataControl::CondMutex::wait(double timeout_sec)
{
    int ret;
    struct timespec ts;
    double exp_time;

    if (timeout_sec <= 0.0) {
        ret = pthread_cond_wait(&(this->_cond), &(this->_mutex));
    } else {
        exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC, &ts) + timeout_sec;
        do {
            CR_DataControl::timespec_add(ts, (int64_t)(timeout_sec * 1000000));
            ret = pthread_cond_timedwait(&(this->_cond), &(this->_mutex), &ts);
            if (ret == EINTR)
                continue;
            break;
        } while (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) < exp_time);
    }

    return ret;
}

int
CR_DataControl::CondMutex::signal()
{
    return pthread_cond_signal(&(this->_cond));
}

int
CR_DataControl::CondMutex::broadcast()
{
    return pthread_cond_broadcast(&(this->_cond));
}

////////////////////////////////

CR_DataControl::Sem::Sem(unsigned int sval)
{
    pthread_once(&cr_class_attr_init_once, cr_class_attr_init);
    pthread_mutex_init(&(this->_mutex), cr_class_mutex_attr_p);
    sem_init(&(this->_sem), 0, sval);
    this->_max_value = sval;
}

CR_DataControl::Sem::~Sem()
{
    sem_destroy(&(this->_sem));
    pthread_mutex_destroy(&(this->_mutex));
}

int
CR_DataControl::Sem::wait(double timeout_sec)
{
    int ret;
    struct timespec ts;
    double exp_time;

    if (timeout_sec <= 0.0) {
        ret = sem_wait(&(this->_sem));
    } else {
        exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC, &ts) + timeout_sec;
        do {
            CR_DataControl::timespec_add(ts, (int64_t)(timeout_sec * 1000000));
            ret = sem_timedwait(&(this->_sem), &ts);
            if (ret == EINTR)
                continue;
            break;
        } while (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) < exp_time);
    }

    return ret;
}

int
CR_DataControl::Sem::trywait()
{
    return sem_trywait(&(this->_sem));
}

int
CR_DataControl::Sem::post()
{
    return sem_post(&(this->_sem));
}

int
CR_DataControl::Sem::getvalue(int *sval_p)
{
    return sem_getvalue(&(this->_sem), sval_p);
}

int
CR_DataControl::Sem::setmaxvalue(unsigned int val, double timeout_sec)
{
    int ret = 0;

    ret = pthread_mutex_lock(&(this->_mutex));
    if (!ret) {
        if (this->_max_value < val) {
            for (; this->_max_value < val; this->_max_value++) {
                ret = this->post();
                if (ret)
                    break;
            }
        } else if (this->_max_value > val) {
            if (timeout_sec > 0.0) {
                double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;
                double cur_timeout = 0.0;
                for (; this->_max_value > val; this->_max_value--) {
                    cur_timeout = exp_time - CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
                    if (cur_timeout <= 0.0)
                        cur_timeout = 0.01;
                    ret = this->wait(cur_timeout);
                    if (ret)
                        break;
                }
            } else {
                for (; this->_max_value > val; this->_max_value--) {
                    ret = this->wait();
                    if (ret)
                        break;
                }
            }
        }
        pthread_mutex_unlock(&(this->_mutex));
    }

    return ret;
}

unsigned int
CR_DataControl::Sem::getmaxvalue()
{
    unsigned int ret = 0;

    pthread_mutex_lock(&(this->_mutex));

    ret = this->_max_value;

    pthread_mutex_unlock(&(this->_mutex));

    return ret;
}

////////////////////////////////

CR_DataControl::Descriptor::Descriptor()
{
    CANCEL_DISABLE_ENTER();

    pthread_once(&cr_class_attr_init_once, cr_class_attr_init);
    pthread_mutex_init(&(this->_mutex), cr_class_mutex_attr_p);
    pthread_cond_init(&(this->_cond), cr_class_cond_attr_p);

    this->_ref_count = 0;
    this->_closed = false;

    CANCEL_DISABLE_LEAVE();
}

CR_DataControl::Descriptor::~Descriptor()
{
    CANCEL_DISABLE_ENTER();

    this->close();
    this->wait_close();

    pthread_cond_destroy(&(this->_cond));
    pthread_mutex_destroy(&(this->_mutex));

    CANCEL_DISABLE_LEAVE();
}

int
CR_DataControl::Descriptor::lock(double timeout_sec)
{
    int ret;
    struct timespec ts;

    if (timeout_sec <= 0.0) {
        ret = pthread_mutex_lock(&(this->_mutex));
    } else {
        ::clock_gettime(CLOCK_REALTIME, &ts);
        CR_DataControl::timespec_add(ts, (int64_t)(timeout_sec * 1000000));
        ret = pthread_mutex_timedlock(&(this->_mutex), &ts);
    }

    return ret;
}

int
CR_DataControl::Descriptor::trylock()
{
    int ret;

    ret = pthread_mutex_trylock(&(this->_mutex));

    return ret;
}

int
CR_DataControl::Descriptor::unlock()
{
    int ret;

    ret = pthread_mutex_unlock(&(this->_mutex));

    return ret;
}

int
CR_DataControl::Descriptor::wait(double timeout_sec)
{
    int ret;
    struct timespec ts;
    double exp_time;

    if (timeout_sec <= 0.0) {
        ret = pthread_cond_wait(&(this->_cond), &(this->_mutex));
    } else {
        exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC, &ts) + timeout_sec;
        do {
            CR_DataControl::timespec_add(ts, (int64_t)(timeout_sec * 1000000));
            ret = pthread_cond_timedwait(&(this->_cond), &(this->_mutex), &ts);
            if (ret == EINTR)
                continue;
            break;
        } while (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) < exp_time);
    }

    return ret;
}

int
CR_DataControl::Descriptor::signal()
{
    return pthread_cond_signal(&(this->_cond));
}

int
CR_DataControl::Descriptor::broadcast()
{
    return pthread_cond_broadcast(&(this->_cond));
}

int
CR_DataControl::Descriptor::get(double timeout_sec)
{
    int ret = 0;

    CANCEL_DISABLE_ENTER();

    ret = this->lock();
    if (ret == 0) {
        if (!this->_closed) {
            this->_ref_count++;
        } else
            ret = EPIPE;

        this->unlock();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_DataControl::Descriptor::put()
{
    int ret = 0;

    CANCEL_DISABLE_ENTER();

    ret = this->lock();
    if (ret == 0) {
        if (this->_ref_count > 0) {
            this->_ref_count--;
            this->broadcast();
        } else
            ret = EPERM;

        this->unlock();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_DataControl::Descriptor::close()
{
    int ret = 0;

    CANCEL_DISABLE_ENTER();

    ret = this->lock();
    if (ret == 0) {
        if (!this->_closed) {
            this->_closed = true;
            this->broadcast();
        }

        this->unlock();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_DataControl::Descriptor::reopen()
{
    int ret = 0;

    CANCEL_DISABLE_ENTER();

    ret = this->lock();
    if (ret == 0) {
        if (this->_closed) {
            if (this->_ref_count == 0) {
                this->_closed = false;
                this->broadcast();
            } else {
                DPRINTFX(15, "this->_ref_count == %ld\n", this->_ref_count);
                ret = EINVAL;
            }
        } else
            ret = EBUSY;

        this->unlock();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

bool
CR_DataControl::Descriptor::is_closed()
{
    bool ret = false;

    CANCEL_DISABLE_ENTER();

    this->lock();

    ret = this->_closed;

    this->unlock();

    CANCEL_DISABLE_LEAVE();

    return ret;
}

bool
CR_DataControl::Descriptor::is_real_closed()
{
    bool ret = false;

    CANCEL_DISABLE_ENTER();

    this->lock();

    if ((this->_closed) && (this->_ref_count == 0)) {
        ret = true;
    }

    this->unlock();

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_DataControl::Descriptor::wait_close(double timeout_sec)
{
    int ret = 0;

    if (!this->_closed) {
        return EBUSY;
    }

    double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;

    CANCEL_DISABLE_ENTER();

    while (1) {
        ret = this->lock();
        if (ret == 0) {
            if (this->_ref_count == 0) {
                this->unlock();
                break;
            }
            this->unlock();
            if (timeout_sec > 0) {
                if (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) > exp_time) {
                    ret = ETIMEDOUT;
                    break;
                }
            }
            pthread_yield();
        } else {
            break;
        }
    }

    CANCEL_DISABLE_LEAVE();

    return ret;


}

////////////////////////////////
