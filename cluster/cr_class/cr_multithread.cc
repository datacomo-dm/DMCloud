#include <stdlib.h>
#include <list>
#include <cr_class/cr_class.h>
#include <cr_class/cr_multithread.h>
#include <cr_class/cr_quickhash.h>

typedef struct TAB {
    std::list<CR_Thread::handle_t> tlist;
    CR_BlockQueue<std::string> req_queue;
    size_t use_count;
} TAB_t;

static CR_DataControl::CondMutex *_cr_multithread_condmutex = NULL;
static pthread_once_t _cr_multithread_init_once = PTHREAD_ONCE_INIT;

static void _cr_multithread_init();
static double _cr_multithread_tab_dealloc(void *);
static double _cr_multithread_tree_dealloc(void *);

static void
_cr_multithread_init()
{
    _cr_multithread_condmutex = new CR_DataControl::CondMutex;
}

static double
_cr_multithread_tab_dealloc(void *_arg)
{
    double ret = 1;
    TAB_t *tab = (TAB_t *)_arg;

    _cr_multithread_condmutex->lock();

    if (!tab->use_count == 0) {
        delete tab;
        ret = 0;
    }

    _cr_multithread_condmutex->unlock();

    return ret;
}

static double
_cr_multithread_tree_dealloc(void *_arg)
{
    double ret = 1;
    CR_TreeStorage *tree = (CR_TreeStorage *)_arg;

    _cr_multithread_condmutex->lock();

    if (tree->close() == 0) {
        delete tree;
        ret = 0;
    }

    _cr_multithread_condmutex->unlock();

    return ret;
}

CR_MultiThread::CR_MultiThread()
{
    pthread_once(&_cr_multithread_init_once, _cr_multithread_init);
}

CR_MultiThread::~CR_MultiThread()
{
    std::set<void*>::iterator _it;

    CANCEL_DISABLE_ENTER();

    _cr_multithread_condmutex->lock();

    for (_it=this->_handle_set.begin(); _it!=this->_handle_set.end(); _it++) {
        CR_Thread::handle_t th = (CR_Thread::handle_t)(*_it);
        CR_Thread::stop(th);
    }

    for (_it=this->_tab_set.begin(); _it!=this->_tab_set.end(); _it++) {
        TAB_t *p = (TAB_t *)(*_it);
        CR_Thread::push_delayed_job(p, _cr_multithread_tab_dealloc, 10);
    }

    for (_it=this->_tree_set.begin(); _it!=this->_tree_set.end(); _it++) {
        CR_TreeStorage *p = (CR_TreeStorage *)(*_it);
        CR_Thread::push_delayed_job(p, _cr_multithread_tree_dealloc, 10);
    }

    _cr_multithread_condmutex->unlock();

    CANCEL_DISABLE_LEAVE();
}

inline std::string
CR_MultiThread::name2id(const std::string &name)
{
    return CR_QuickHash::md5(name);
}

int
CR_MultiThread::Get(double timeout_sec)
{
    return this->_cr_desc.get(timeout_sec);
}

int
CR_MultiThread::Put()
{
    return this->_cr_desc.put();
}

int
CR_MultiThread::Spawn(const std::string &thread_class, const std::string &thread_name, const CR_Thread::TDB_t &tdb)
{
    int ret = 0;
    std::string thread_hash;
    std::string tab_p_str;
    TAB_t *tab_p = NULL;
    CR_TreeStorage *_name2tab_tree;

    CANCEL_DISABLE_ENTER();

    CR_Thread::handle_t th = CR_Thread::talloc(tdb);

    thread_hash = this->name2id(thread_name);
    CR_Thread::settag(th, CR_MULTITHREAD_TAG_MULTITHREAD_PTR, CR_Class_NS::ptr2str(this));

    CR_Thread::settag(th, CR_MULTITHREAD_TAG_THREAD_CLASS, thread_class);
    CR_Thread::settag(th, CR_MULTITHREAD_TAG_THREAD_NAME, thread_name);

    _cr_multithread_condmutex->lock();

    _name2tab_tree = this->_name2tab_tree_map[thread_class];
    if (!_name2tab_tree) {
        _name2tab_tree = new CR_TreeStorage;
        this->_tree_set.insert(_name2tab_tree);
        this->_name2tab_tree_map[thread_class] = _name2tab_tree;
    }

    ret = _name2tab_tree->Get(thread_hash, tab_p_str);
    if (ret == 0) {
        tab_p = (TAB_t *)CR_Class_NS::str2ptr(tab_p_str);
    }

    if (!tab_p) {
        tab_p = new TAB_t;
        tab_p->use_count = 0;
        this->_tab_set.insert(tab_p);
        _name2tab_tree->Set(thread_hash, CR_Class_NS::ptr2str(tab_p));
    }

    tab_p->tlist.push_back(th);
    this->_handle_set.insert(th);
    ret = CR_Thread::start(th);

    _cr_multithread_condmutex->broadcast();

    _cr_multithread_condmutex->unlock();

    if (ret != 0) {
        CR_Thread::tfree(th);
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

void *
CR_MultiThread::get_tab(const std::string &thread_class, const std::string &key, const bool hash)
{
    TAB_t *tab_p = NULL;
    std::string tab_p_str;
    std::string _foundkey;
    CR_TreeStorage *_name2tab_tree = NULL;
    int fret;

    _cr_multithread_condmutex->lock();

    _name2tab_tree = this->_name2tab_tree_map[thread_class];
    if (!_name2tab_tree) {
        _name2tab_tree = new CR_TreeStorage;
        this->_tree_set.insert(_name2tab_tree);
        this->_name2tab_tree_map[thread_class] = _name2tab_tree;
    }

    if (hash) {
        fret = _name2tab_tree->FindNearByTree(key, _foundkey, tab_p_str);
        if (fret == 0) {
            tab_p = (TAB_t *)CR_Class_NS::str2ptr(tab_p_str);
        }
    } else {
        fret = _name2tab_tree->Get(this->name2id(key), tab_p_str);
        if (fret == 0) {
            tab_p = (TAB_t *)CR_Class_NS::str2ptr(tab_p_str);
        }
        if (!tab_p) {
            std::string thread_hash = this->name2id(key);
            tab_p = new TAB_t;
            tab_p->use_count = 0;
            this->_tab_set.insert(tab_p);
            _name2tab_tree->Set(thread_hash, CR_Class_NS::ptr2str(tab_p));
        }
    }

    if (tab_p) {
        tab_p->use_count++;
    }

    _cr_multithread_condmutex->unlock();

    return tab_p;
}

void
CR_MultiThread::put_tab(void *_tab_p)
{
    TAB_t *tab_p = (TAB_t *)_tab_p;

    if (tab_p) {
        _cr_multithread_condmutex->lock();
        tab_p->use_count--;
        _cr_multithread_condmutex->unlock();
    }
}

CR_TreeStorage *
CR_MultiThread::_get_tree_p(const std::string &tree_name)
{
    CR_TreeStorage *tree_p = NULL;

    _cr_multithread_condmutex->lock();

    tree_p = this->_data_tree_map[tree_name];
    if (!tree_p) {
        tree_p = new CR_TreeStorage;
        this->_data_tree_map[tree_name] = tree_p;
        this->_tree_set.insert(tree_p);
    }

    _cr_multithread_condmutex->unlock();

    return tree_p;
}

int
CR_MultiThread::Stop(const std::string &thread_class, const std::string &thread_name,
	const intptr_t thread_id)
{
    int ret = ENOENT;
    TAB_t *tab_p;
    CR_Thread::handle_t th;
    std::list<CR_Thread::handle_t> th_list;
    std::list<CR_Thread::handle_t>::iterator tlist_it;

    CANCEL_DISABLE_ENTER();

    tab_p = (TAB_t *)this->get_tab(thread_class, thread_name);
    if (tab_p) {
        _cr_multithread_condmutex->lock();

        if (thread_id >= 0) {
            for (tlist_it=tab_p->tlist.begin(); tlist_it!=tab_p->tlist.end(); tlist_it++) {
                th = *tlist_it;
                if (CR_Thread::gettid(th) == thread_id) {
                    tab_p->tlist.erase(tlist_it);
                    th_list.push_back(th);
                    this->_handle_set.erase(th);
                    break;
                }
            }
        } else {
            for (tlist_it=tab_p->tlist.begin(); tlist_it!=tab_p->tlist.end(); tlist_it++) {
                th = *tlist_it;
                th_list.push_back(th);
                this->_handle_set.erase(th);
            }
            tab_p->tlist.clear();
        }

        _cr_multithread_condmutex->unlock();

        this->put_tab(tab_p);
    }

    while (!th_list.empty()) {
        th = th_list.front();
        th_list.pop_front();
        CR_Thread::stop(th);
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_MultiThread::ThreadGroupSetTag(const std::string &thread_class, const std::string &thread_name,
	const std::string &tag_key, const std::string &tag_value)
{
    int ret = 0;
    TAB_t *tab_p;
    CR_Thread::handle_t th;
    std::list<CR_Thread::handle_t>::iterator tlist_it;

    if (tag_key.compare(CR_MULTITHREAD_TAG_MULTITHREAD_PTR) == 0) {
        DPRINTFX(15, "tag_key == \"%s\"\n", CR_MULTITHREAD_TAG_MULTITHREAD_PTR);
        return EINVAL;
    }

    if (tag_key.compare(CR_MULTITHREAD_TAG_THREAD_CLASS) == 0) {
        DPRINTFX(15, "tag_key == \"%s\"\n", CR_MULTITHREAD_TAG_THREAD_CLASS);
        return EINVAL;
    }

    if (tag_key.compare(CR_MULTITHREAD_TAG_THREAD_NAME) == 0) {
        DPRINTFX(15, "tag_key == \"%s\"\n", CR_MULTITHREAD_TAG_THREAD_NAME);
        return EINVAL;
    }

    CANCEL_DISABLE_ENTER();

    tab_p = (TAB_t *)this->get_tab(thread_class, thread_name);
    if (tab_p) {
        _cr_multithread_condmutex->lock();

        for (tlist_it=tab_p->tlist.begin(); tlist_it!=tab_p->tlist.end(); tlist_it++) {
            th = *tlist_it;
            ret = CR_Thread::settag(th, tag_key, tag_value);
        }

        _cr_multithread_condmutex->unlock();

        this->put_tab(tab_p);
    } else
        ret = ENOENT;

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_MultiThread::ThreadGroupGetTag(const std::string &thread_class, const std::string &thread_name,
	const std::string &tag_key, std::map<intptr_t,std::string> &thread_group_tag_value_map)
{
    int ret = 0;
    TAB_t *tab_p;
    CR_Thread::handle_t th;
    std::list<CR_Thread::handle_t>::iterator tlist_it;

    CANCEL_DISABLE_ENTER();

    tab_p = (TAB_t *)this->get_tab(thread_class, thread_name);
    if (tab_p) {
        thread_group_tag_value_map.clear();

        _cr_multithread_condmutex->lock();

        for (tlist_it=tab_p->tlist.begin(); tlist_it!=tab_p->tlist.end(); tlist_it++) {
            th = *tlist_it;
            thread_group_tag_value_map[CR_Thread::gettid(th)] = CR_Thread::gettag(th, tag_key);
        }

        _cr_multithread_condmutex->unlock();

        this->put_tab(tab_p);
    } else
        ret = ENOENT;

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_MultiThread::QueueSetMaxSize(const std::string &thread_class, const std::string &thread_name,
	const size_t max_size)
{
    int ret = 0;
    TAB_t *tab_p;

    CANCEL_DISABLE_ENTER();

    tab_p = (TAB_t *)this->get_tab(thread_class, thread_name);
    if (tab_p) {
        tab_p->req_queue.set_max_size(max_size);

        this->put_tab(tab_p);
    } else
        ret = ENOENT;

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_MultiThread::QueueGetSize(const std::string &thread_class, const std::string &thread_name,
                size_t &size)
{
    int ret = 0;
    TAB_t *tab_p;

    CANCEL_DISABLE_ENTER();

    tab_p = (TAB_t *)this->get_tab(thread_class, thread_name);
    if (tab_p) {
        size = tab_p->req_queue.size();

        this->put_tab(tab_p);
    } else
        ret = ENOENT;

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_MultiThread::do_queue_push(const bool push_front, const bool hash_push,
	const std::string &thread_class, const std::string &key, const std::string &s, double timeout_sec)
{
    int ret = 0;
    TAB_t *tab_p;

    CANCEL_DISABLE_ENTER();

    tab_p = (TAB_t *)this->get_tab(thread_class, key, hash_push);
    if (tab_p) {
        if (push_front)
            tab_p->req_queue.push_front(s);
        else
            ret = tab_p->req_queue.push_back(s, timeout_sec);

        this->put_tab(tab_p);
    } else
        ret = ENOENT;

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_MultiThread::QueuePushByName(const std::string &thread_class, const std::string &thread_name,
	const std::string &s, double timeout_sec)
{
    return this->do_queue_push(false, false, thread_class, thread_name, s, timeout_sec);
}

int
CR_MultiThread::QueuePushByHash(const std::string &thread_class, const std::string &key,
	const std::string &s, double timeout_sec)
{
    return this->do_queue_push(false, true, thread_class, key, s, timeout_sec);
}

int
CR_MultiThread::QueuePushFrontByName(const std::string &thread_class, const std::string &thread_name,
	const std::string &s)
{
    return this->do_queue_push(true, false, thread_class, thread_name, s);
}

int
CR_MultiThread::QueuePushFrontByHash(const std::string &thread_class, const std::string &key,
	const std::string &s)
{
    return this->do_queue_push(true, true, thread_class, key, s);
}

int
CR_MultiThread::QueuePull(const std::string &thread_class, const std::string &thread_name,
	std::string &s, double timeout_sec)
{
    int ret = 0;
    TAB_t *tab_p;

    CANCEL_DISABLE_ENTER();

    tab_p = (TAB_t *)this->get_tab(thread_class, thread_name);
    if (tab_p) {
        ret = tab_p->req_queue.pop_front(s, timeout_sec);

        this->put_tab(tab_p);
    } else
        ret = ENOENT;

    CANCEL_DISABLE_LEAVE();

    return ret;
}

CR_TreeStorage *
CR_MultiThread::GetTreePtr(const std::string &thread_class)
{
    CR_TreeStorage *tree_p = NULL;

    CANCEL_DISABLE_ENTER();

    tree_p = this->_get_tree_p(thread_class);

    CANCEL_DISABLE_LEAVE();

    return tree_p;
}
