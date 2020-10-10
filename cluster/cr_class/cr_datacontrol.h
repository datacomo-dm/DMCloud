#ifndef __H_RC_DATACONTROL_H_
#define __H_RC_DATACONTROL_H_

#include <cr_class/cr_addon.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <errno.h>

extern pthread_mutexattr_t *cr_class_mutex_attr_p;
extern pthread_condattr_t *cr_class_cond_attr_p;

#ifndef _HAS_CR_CANCELABLE_FUNCS_
#define _HAS_CR_CANCELABLE_FUNCS_

#define CANCEL_DISABLE_ENTER(...)								\
	do {											\
		int __orig_cancel_state_d__;							\
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &__orig_cancel_state_d__);	\
		do {} while (0)

#define CANCEL_DISABLE_LEAVE(...)								\
		pthread_setcancelstate(__orig_cancel_state_d__, NULL);				\
	} while (0)

#define CANCEL_ENABLE_ENTER(...)								\
	do {											\
		int __orig_cancel_state_e__;							\
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &__orig_cancel_state_e__);	\
		do {} while (0)

#define CANCEL_ENABLE_LEAVE(...)								\
		pthread_setcancelstate(__orig_cancel_state_e__, NULL);				\
	} while (0)

#define CANCELABLE_MUTEX_LOCK_RESULT (__cancelable_mutex_lock_result)

#define CANCELABLE_MUTEX_LOCK(mutex_p)								\
	do {											\
		pthread_mutex_t *__ca_mtx_p = (mutex_p);					\
		int __ca_orig_ct__;								\
		int CANCELABLE_MUTEX_LOCK_RESULT;						\
		pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, &__ca_orig_ct__);		\
		pthread_cleanup_push((void(*)(void *))pthread_mutex_unlock, __ca_mtx_p);	\
		CANCELABLE_MUTEX_LOCK_RESULT = pthread_mutex_trylock(__ca_mtx_p);		\
		if (CANCELABLE_MUTEX_LOCK_RESULT == EBUSY)					\
			CANCELABLE_MUTEX_LOCK_RESULT = pthread_mutex_lock(__ca_mtx_p);		\
		do {} while (0)

#define CANCELABLE_MUTEX_UNLOCK(...)								\
		if (CANCELABLE_MUTEX_LOCK_RESULT == 0)						\
			pthread_mutex_unlock(__ca_mtx_p);					\
		pthread_cleanup_pop(0);								\
		pthread_setcanceltype(__ca_orig_ct__, NULL);					\
	} while (0)

#endif /* _HAS_CR_CANCELABLE_FUNCS_ */


namespace CR_DataControl {
	struct timespec & timespec_add(struct timespec &ts, const int64_t time_usec);
	int timespec_cmp(const struct timespec &ts1, const struct timespec &ts2);

	struct timeval & timeval_add(struct timeval &tv, const int64_t time_usec);
	int timeval_cmp(const struct timeval &tv1, const struct timeval &tv2);

	class Spin {
	public:
		Spin();
		~Spin();

		int lock();
		int trylock();
		int unlock();
	private:
		pthread_spinlock_t _spin;

		Spin(const Spin&);				/* declarations only */
		Spin& operator=(const Spin&);			/* declarations only */
	};

	class Mutex {
	public:
		Mutex();
		~Mutex();

		int lock(double timeout_sec=0.0);
		int trylock();
		int unlock();

	private:
		pthread_mutex_t _mutex;

		Mutex(const Mutex&);				/* declarations only */
		Mutex& operator=(const Mutex&);			/* declarations only */
	};

	class RWLock {
	public:
		RWLock();
		~RWLock();

		int rdlock(double timeout_sec=0.0);
		int tryrdlock();
		int wrlock(double timeout_sec=0.0);
		int trywrlock();
		int unlock();

	private:
		pthread_rwlock_t _rwlock;

		RWLock(const RWLock&);				/* declarations only */
		RWLock& operator=(const RWLock&);		/* declarations only */
	};

	class CondMutex {
	public:
		CondMutex();
		~CondMutex();

		int lock(double timeout_sec=0.0);
		int trylock();
		int unlock();

		int wait(double timeout_usec=0.0);
		int signal();
		int broadcast();

	private:
		pthread_mutex_t _mutex;
		pthread_cond_t _cond;

		CondMutex(const CondMutex&);			/* declarations only */
		CondMutex& operator=(const CondMutex&);		/* declarations only */
	};

	class Sem {
	public:
		Sem(unsigned int sval=0);
		~Sem();

		int wait(double timeout_sec=0.0);
		int trywait();
		int post();
		int getvalue(int *sval_p);
		int setmaxvalue(unsigned int val, double timeout_sec=0.0);
		unsigned int getmaxvalue();

	private:
		pthread_mutex_t _mutex;
		sem_t _sem;
		unsigned int _max_value;

		Sem(const Sem&);				/* declarations only */
		Sem& operator=(const Sem&);			/* declarations only */
	};

	class Descriptor {
	public:
		Descriptor();
		~Descriptor();

		int lock(double timeout_sec=0.0);
		int trylock();
		int unlock();

		int wait(double timeout_sec=0.0);
		int signal();
		int broadcast();

		int get(double timeout_sec=0.0);
		int put();

		int close();
		int wait_close(double timeout_sec=0.0);
		int reopen();

		bool is_closed();
		bool is_real_closed();
	private:
		pthread_mutex_t _mutex;
		pthread_cond_t _cond;

		size_t _ref_count;
		bool _closed;

		Descriptor(const Descriptor&);			/* declarations only */
		Descriptor& operator=(const Descriptor&);	/* declarations only */
	};
}

#endif /* __H_RC_DATACONTROL_H_ */
