#include <assert.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <map>
#include <list>
#include <stack>
#include <string>
#include <exception>

#include <cr_class/cr_thread.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_pool.h>
#include <cr_class/cr_multithread.h>
#include <cr_class/queue.h>

#define CR_THREAD_KILL_TIMEOUT		(10.0)
#define CR_THREAD_CLEAR_TCB_TIMEOUT	(3600)

typedef struct TCB {
    CR_DataControl::Descriptor desc;
    pthread_t thread;
    intptr_t tid;
    struct {
        bool inuse;
        bool stop;
    } status;
    std::map<std::string,std::string> tags;
    CR_Thread::TDB_t tdb;
    std::string result;
} TCB_t;

static void *_thread_loader(void *);
static void _cr_thread_term_handle(int);

static double _cr_thread_tcb_dealloc(void *);

static void _cr_thread_tcb_get_cleanup(void *);
static void _cr_thread_multithread_get_cleanup(void *);

static void _cr_thread_init();

size_t cr_thread_count = 0;
size_t cr_thread_kill_count = 0;
size_t cr_thread_recycling_count = 0;
size_t cr_thread_recycled_count = 0;
size_t cr_thread_garbage_count = 0;

static pthread_once_t _cr_thread_init_once = PTHREAD_ONCE_INIT;

static CR_Pool<TCB_t> * _cr_thread_tcb_pool_p = NULL;

static void
_cr_thread_term_handle(int sig)
{
    pthread_exit(NULL);
}

static double
_cr_thread_tcb_dealloc(void *_arg)
{
    TCB_t *tcb = (TCB_t *)_arg;

    tcb->desc.close();
    tcb->desc.wait_close();

    _cr_thread_tcb_pool_p->Put(tcb);

    return 0;
}

static void
_cr_thread_init()
{
    _cr_thread_tcb_pool_p = new CR_Pool<TCB_t>(1024, true, true);
}

static void *
_thread_loader(void *_arg)
{
    TCB_t *tcb = (TCB_t *)_arg;
    std::string thread_result;
    bool can_join = false;
    CR_Thread::thread_func_t thread_func;
    CR_Thread::on_thread_exit_t on_thread_exit;
    CR_Thread::on_thread_exception_t on_thread_exception;

    CR_MultiThread *_cr_mt = NULL;

    if (!tcb)
        return NULL;

    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

    signal(SIGTERM, _cr_thread_term_handle);

    thread_func = tcb->tdb.thread_func;
    on_thread_exit = tcb->tdb.on_thread_exit;
    on_thread_exception = tcb->tdb.on_thread_exception;
    if (tcb->tdb.thread_flags & CR_THREAD_JOINABLE)
        can_join = true;

    if (!can_join)
        pthread_detach(pthread_self());

    tcb->desc.get();

    _cr_mt = (CR_MultiThread *)CR_Class_NS::str2ptr(CR_Thread::gettag(tcb, CR_MULTITHREAD_TAG_MULTITHREAD_PTR));

    if (_cr_mt) {
        _cr_mt->Get();
    }

    tcb->tid = (intptr_t)pthread_self();

    pthread_cleanup_push(_cr_thread_multithread_get_cleanup, _cr_mt);
    pthread_cleanup_push(_cr_thread_tcb_get_cleanup, tcb);

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

    if (thread_func) {
        try {
            thread_result = thread_func(tcb);
        } catch (int e) {
            if (on_thread_exception) {
                on_thread_exit = NULL;
                try {
                    on_thread_exception(tcb, CR_Thread::E_INT, &e);
                } catch (...) {
                    DPRINTF("Dual exception NOT support.\n");
                }
            } else {
                DPRINTF("Unhandled exception captured, type:int, e:%d, strerror(e):%s\n", e, CR_Class_NS::strerror(e));
            }
        } catch (std::string &e) {
            if (on_thread_exception) {
                on_thread_exit = NULL;
                try {
                    on_thread_exception(tcb, CR_Thread::E_STRING, &e);
                } catch (...) {
                    DPRINTF("Dual exception NOT support.\n");
                }
            } else {
                DPRINTF("Unhandled exception captured, type:str, e.c_str():%s\n", e.c_str());
            }
        } catch (void *e) {
            if (on_thread_exception) {
                on_thread_exit = NULL;
                try {
                    on_thread_exception(tcb, CR_Thread::E_VOIDPTR, e);
                } catch (...) {
                    DPRINTF("Dual exception NOT support.\n");
                }
            } else {
                DPRINTF("Unhandled exception captured, type:void-ptr\n");
            }
        } catch (std::exception &e) {
            if (on_thread_exception) {
                on_thread_exit = NULL;
                try {
                    on_thread_exception(tcb, CR_Thread::E_STDEXP, &e);
                } catch (...) {
                    DPRINTF("Dual exception NOT support.\n");
                }
            } else {
                DPRINTF("Unhandled exception captured, type:std_exception, e.what():%s\n", e.what());
            }
#ifdef __linux__
        } catch (__cxxabiv1::__forced_unwind&) {
            throw;
#endif /* __linux__ */
        } catch (...) {
            if (on_thread_exception) {
                on_thread_exit = NULL;
                try {
                    on_thread_exception(tcb, CR_Thread::E_UNKNOWN, NULL);
                } catch (...) {
                    DPRINTF("Dual exception NOT support.\n");
                }
            } else {
                DPRINTF("Unhandled exception captured, type:unknown\n");
            }
        }
    }

    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);

    if (on_thread_exit)
        on_thread_exit(tcb, thread_result);

    if (_cr_mt) {
        _cr_mt->Put();
    }

    tcb->desc.lock();

    tcb->result = thread_result;

    tcb->status.inuse = false;
    if (!can_join)
        CR_Thread::push_delayed_job(tcb, _cr_thread_tcb_dealloc, CR_THREAD_CLEAR_TCB_TIMEOUT);

    tcb->desc.broadcast();

    tcb->desc.unlock();

    tcb->desc.put();

    pthread_cleanup_pop(0);
    pthread_cleanup_pop(0);

    return &(tcb->result);
}

CR_Thread::TDB_t::TDB_t()
{
    this->thread_func = NULL;
    this->on_thread_exit = NULL;
    this->on_thread_cancel = NULL;
    this->on_thread_exception = NULL;
    this->thread_flags = 0;
    this->default_tags.clear();
}

CR_Thread::TDB_t::~TDB_t()
{
}

int
CR_Thread::push_delayed_job(void *p, double (*job_func_p)(void *), double timeout_sec)
{
    return CR_Class_NS::set_alarm(job_func_p, p, timeout_sec);
}

static void
_cr_thread_tcb_get_cleanup(void *_arg)
{
    TCB_t *tcb = (TCB_t *)_arg;

    if (tcb)
        tcb->desc.put();
}

static void
_cr_thread_multithread_get_cleanup(void *_arg)
{
    CR_MultiThread *_cr_mt = (CR_MultiThread *)_arg;

    if (_cr_mt)
        _cr_mt->Put();
}

CR_Thread::handle_t
CR_Thread::talloc(const TDB_t &tdb)
{
    pthread_once(&_cr_thread_init_once, _cr_thread_init);

    TCB_t *tcb = _cr_thread_tcb_pool_p->Get();

    if (tcb) {
        tcb->tid = 0;
        tcb->status.inuse = false;
        tcb->status.stop = false;
        tcb->tdb.thread_flags = tdb.thread_flags;
        tcb->tdb.thread_func = tdb.thread_func;
        tcb->tdb.on_thread_exit = tdb.on_thread_exit;
        tcb->tdb.on_thread_cancel = tdb.on_thread_cancel;
        tcb->tdb.on_thread_exception = tdb.on_thread_exception;
        if (!tdb.default_tags.empty()) {
            tcb->tags = tdb.default_tags;
        }
    }

    return tcb;
}

void
CR_Thread::tfree(handle_t handle)
{
    TCB_t *tcb = (TCB_t *)handle;

    if (tcb)
        _cr_thread_tcb_pool_p->Put(tcb);
}

intptr_t
CR_Thread::gettid(handle_t handle)
{
    intptr_t ret = 0;

    TCB_t *tcb = (TCB_t *)handle;

    if (tcb)
        ret = tcb->tid;

    return ret;
}

int
CR_Thread::start(handle_t handle)
{
    int ret = 0;
    TCB_t *tcb = (TCB_t *)handle;

    if (!tcb)
        return EFAULT;

    CANCEL_DISABLE_ENTER();

    ret = tcb->desc.get();
    if (!ret) {
        tcb->desc.lock();

        if (!tcb->status.inuse) {
            ret = pthread_create(&(tcb->thread), NULL, _thread_loader, tcb);
            if (!ret) {
                tcb->status.inuse = true;
            } else {
                _cr_thread_tcb_pool_p->Put(tcb);
                tcb = NULL;
            }
        } else
            ret = EBUSY;

        tcb->desc.unlock();
        tcb->desc.put();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_Thread::stop(handle_t handle)
{
    int ret = 0;
    TCB_t *tcb = (TCB_t *)handle;

    if (!tcb)
        return EFAULT;

    CANCEL_DISABLE_ENTER();

    ret = tcb->desc.get();
    if (!ret) {
        tcb->desc.lock();

        if (tcb->status.inuse) {
            tcb->status.stop = true;
        } else
            ret = EBUSY;

        tcb->desc.unlock();
        tcb->desc.put();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_Thread::cancel(handle_t handle)
{
    int ret = 0;
    TCB_t *tcb = (TCB_t *)handle;
    CR_Thread::on_thread_cancel_t on_thread_cancel = NULL;

    if (!tcb)
        return EFAULT;

    CANCEL_DISABLE_ENTER();

    ret = tcb->desc.get();
    if (!ret) {
        if (tcb->tdb.thread_flags & CR_THREAD_JOINABLE) {
            tcb->tdb.thread_flags &= ~(CR_THREAD_JOINABLE);
            pthread_detach(tcb->thread);
        }

        on_thread_cancel = tcb->tdb.on_thread_cancel;
        ret = pthread_cancel(tcb->thread);

        if (!ret && on_thread_cancel)
            ret = on_thread_cancel(tcb);

        CR_Thread::push_delayed_job(tcb, _cr_thread_tcb_dealloc, CR_THREAD_CLEAR_TCB_TIMEOUT);

        tcb->desc.put();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

bool
CR_Thread::teststop(handle_t handle)
{
    bool ret = true;
    TCB_t *tcb = (TCB_t *)handle;

    if (!tcb)
        return true;

    CANCEL_DISABLE_ENTER();

    if (tcb->desc.get() == 0) {
        tcb->desc.lock();

        ret = tcb->status.stop;

        tcb->desc.unlock();
        tcb->desc.put();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_Thread::join(handle_t handle, std::string *thread_result)
{
    int ret = 0;
    TCB_t *tcb = (TCB_t *)handle;
    void *thread_result_join = NULL;

    if (!tcb)
        return EFAULT;

    CANCEL_DISABLE_ENTER();

    ret = tcb->desc.get();
    if (!ret) {
        if (tcb->tdb.thread_flags & CR_THREAD_JOINABLE) {
            ret = pthread_join(tcb->thread, &thread_result_join);
            if (!ret && thread_result_join) {
                std::string *thread_result_p = (std::string *)thread_result_join;
                if (thread_result)
                    *thread_result = *thread_result_p;
            }
            CR_Thread::push_delayed_job(tcb, _cr_thread_tcb_dealloc, CR_THREAD_CLEAR_TCB_TIMEOUT);
        } else {
            DPRINTFX(15, "!(tcb->tdb.thread_flags & CR_THREAD_JOINABLE)\n");
            ret = EINVAL;
        }

        tcb->desc.put();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_Thread::settag(handle_t handle, const std::string &tag_key, const std::string &tag_value)
{
    int ret = 0;
    TCB_t *tcb = (TCB_t *)handle;

    if (!tcb)
        return EFAULT;

    CANCEL_DISABLE_ENTER();

    ret = tcb->desc.get();
    if (!ret) {
        tcb->desc.lock();

        tcb->tags[tag_key] = tag_value;

        tcb->desc.unlock();
        tcb->desc.put();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

std::string
CR_Thread::gettag(handle_t handle, const std::string &tag_key)
{
    std::string ret;
    TCB_t *tcb = (TCB_t *)handle;

    if (!tcb)
        return ret;

    CANCEL_DISABLE_ENTER();

    if (tcb->desc.get() == 0) {
        tcb->desc.lock();

        ret = tcb->tags[tag_key];

        tcb->desc.unlock();
        tcb->desc.put();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_Thread::desc_get(handle_t handle, double timeout_sec)
{
    TCB_t *tcb = (TCB_t *)handle;

    if (!tcb)
        return EFAULT;

    return tcb->desc.get(timeout_sec);
}

int
CR_Thread::desc_put(handle_t handle)
{
    TCB_t *tcb = (TCB_t *)handle;

    if (!tcb)
        return EFAULT;

    return tcb->desc.put();
}

int
CR_Thread::desc_lock(handle_t handle, double timeout_sec)
{
    TCB_t *tcb = (TCB_t *)handle;

    if (!tcb)
        return EFAULT;

    return tcb->desc.lock(timeout_sec);
}

int
CR_Thread::desc_unlock(handle_t handle)
{
    TCB_t *tcb = (TCB_t *)handle;

    if (!tcb)
        return EFAULT;

    return tcb->desc.unlock();
}

int
CR_Thread::desc_wait(handle_t handle, double timeout_sec)
{
    TCB_t *tcb = (TCB_t *)handle;

    if (!tcb)
        return EFAULT;

    return tcb->desc.wait(timeout_sec);
}

int
CR_Thread::desc_signal(handle_t handle)
{
    TCB_t *tcb = (TCB_t *)handle;

    if (!tcb)
        return EFAULT;

    return tcb->desc.signal();
}

int
CR_Thread::desc_boardcast(handle_t handle)
{
    TCB_t *tcb = (TCB_t *)handle;

    if (!tcb)
        return EFAULT;

    return tcb->desc.broadcast();
}
