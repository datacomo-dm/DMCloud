#include <stdlib.h>
#include <list>

#include <cr_class/cr_class.h>
#include <cr_class/cr_treestorage.h>
#include <cr_class/cr_quickhash.h>
#include <cr_class/tree.h>

#define THIS_HEAD_P ((cr_treestorage_head_type *)(this->_head_p))

typedef struct cr_treestorage_node {
    RB_ENTRY(cr_treestorage_node) entry;
    std::string key;
    std::string value;
    std::string lock_pass;
    double lock_expire;
} cr_treestorage_node_t;

typedef RB_HEAD(cr_treestorage_tree, cr_treestorage_node) cr_treestorage_head_type;

static int cr_treestorage_compare(const cr_treestorage_node_t *, const cr_treestorage_node_t *);

////////////////////////////////////

RB_GENERATE(cr_treestorage_tree, cr_treestorage_node, entry, cr_treestorage_compare)

static int
cr_treestorage_compare(const cr_treestorage_node_t *e1, const cr_treestorage_node_t *e2)
{
    return e1->key.compare(e2->key);
}

static bool _chk_node_lock(const cr_treestorage_node_t *np, const std::string &lock);
static bool _set_node_lock(cr_treestorage_node_t *np, const std::string &lock, double timeout_sec);
static bool _clr_node_lock(cr_treestorage_node_t *np, const std::string &lock);

////////////////////////////////////

CR_TreeStorage::CR_TreeStorage()
{
    CANCEL_DISABLE_ENTER();

    this->_count = 0;
    this->_is_static = false;
    this->_head_p = malloc(sizeof(cr_treestorage_head_type));
    RB_INIT(THIS_HEAD_P);

    CANCEL_DISABLE_LEAVE();
}

CR_TreeStorage::~CR_TreeStorage()
{
    CANCEL_DISABLE_ENTER();

    this->close();
    free(this->_head_p);

    CANCEL_DISABLE_LEAVE();
}

int
CR_TreeStorage::close(std::map<std::string,std::string> *overstocks)
{
    int ret = 0;
    cr_treestorage_node_t *np, *nxtp;

    CANCEL_DISABLE_ENTER();

    if (overstocks)
        overstocks->clear();

    ret = this->_cr_desc.close();
    if (ret == 0) {
        ret = this->_cr_desc.lock();
        if (ret == 0) {
            for (np = RB_MIN(cr_treestorage_tree, THIS_HEAD_P); np != NULL; np = nxtp) {
                nxtp = RB_NEXT(cr_treestorage_tree, THIS_HEAD_P, np);
                RB_REMOVE(cr_treestorage_tree, THIS_HEAD_P, np);
                if (overstocks)
                    (*overstocks)[np->key] = np->value;
                delete np;
            }
            this->_count = 0;
            this->_is_static = false;
            this->_cr_desc.unlock();
        }
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_TreeStorage::reopen()
{
    int ret = 0;

    CANCEL_DISABLE_ENTER();

    ret = this->_cr_desc.reopen();

    CANCEL_DISABLE_LEAVE();

    return ret;
}

size_t
CR_TreeStorage::count()
{
    size_t ret = 0;

    CANCEL_DISABLE_ENTER();

    if (this->_cr_desc.lock() == 0) {
        ret = this->_count;
        this->_cr_desc.unlock();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

static bool
_chk_node_lock(const cr_treestorage_node_t *np, const std::string &lock_pass)
{
    if (np->lock_pass.size() > 0)
        if (np->lock_pass != lock_pass)
            if (np->lock_expire > CR_Class_NS::clock_gettime(CLOCK_MONOTONIC))
                return false;

    return true;
}

static bool
_set_node_lock(cr_treestorage_node_t *np, const std::string &lock_pass, double timeout)
{
    if (_chk_node_lock(np, lock_pass)) {
        np->lock_pass = lock_pass;
        np->lock_expire = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout;
        return true;
    }

    return false;
}

static bool
_clr_node_lock(cr_treestorage_node_t *np, const std::string &lock_pass)
{
    if (_chk_node_lock(np, lock_pass)) {
        np->lock_pass.clear();
        np->lock_expire = 0;
        return true;
    }

    return false;
}

void
CR_TreeStorage::Touch()
{
    CANCEL_DISABLE_ENTER();

    this->_cr_desc.broadcast();

    CANCEL_DISABLE_LEAVE();
}

int
CR_TreeStorage::Solidify()
{
    int ret = 0;

    CANCEL_DISABLE_ENTER();

    ret = this->_cr_desc.get();
    if (ret == 0) {
        ret = this->_cr_desc.lock();
        if (ret == 0) {
            this->_is_static = true;

            this->_cr_desc.unlock();
        }
        this->_cr_desc.put();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_TreeStorage::Add(const std::string &key, const std::string &value, const std::string &lock_pass,
    double locktimeout_sec, double waittimeout_sec)
{
    if (this->_is_static)
        return EROFS;

    int ret = 0;
    double exp_time = 0.0;
    double wait_gap = (waittimeout_sec <= 0.1) ? waittimeout_sec : 0.1;

    if (waittimeout_sec > 0.0)
        exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + waittimeout_sec;;

    CANCEL_DISABLE_ENTER();

    cr_treestorage_node_t *np = new cr_treestorage_node_t;

    np->key = key;
    np->value = value;
    np->lock_expire = 0;

    if (locktimeout_sec > 0)
        _set_node_lock(np, lock_pass, locktimeout_sec);

    ret = this->_cr_desc.get();
    if (ret == 0) {
        ret = this->_cr_desc.lock();
        if (ret == 0) {
            do {
                if (RB_INSERT(cr_treestorage_tree, THIS_HEAD_P, np) == NULL) {
                    this->_count++;
                    this->_cr_desc.broadcast();
                    ret = 0;
                    break;
                } else {
                    if (waittimeout_sec <= 0.0) {
                        ret = EEXIST;
                        break;
                    } else {
                        ret = ETIMEDOUT;
                        this->_cr_desc.wait(wait_gap);
                    }
                }
            } while (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) < exp_time);
            this->_cr_desc.unlock();
        }
        this->_cr_desc.put();
    }

    if ((ret != 0) && np)
        delete np;

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_TreeStorage::Get(const std::string &key, std::string &value, double timeout_sec)
{
    int ret = 0;
    cr_treestorage_node_t nkey;
    cr_treestorage_node_t *np;
    double exp_time = 0.0;
    double wait_gap = (timeout_sec <= 0.1) ? timeout_sec : 0.1;

    if (timeout_sec > 0.0)
        exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;

    nkey.key = key;

    if (this->_is_static) {
        do {
            np = RB_FIND(cr_treestorage_tree, THIS_HEAD_P, &nkey);
            if (np) {
                value = np->value;
                ret = 0;
                break;
            } else {
                if (timeout_sec <= 0.0) {
                    ret = ENOENT;
                    break;
                } else {
                    ret = ETIMEDOUT;
                    this->_cr_desc.wait(wait_gap);
                }
            }
        } while (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) < exp_time);
    } else {
        CANCEL_DISABLE_ENTER();

        ret = this->_cr_desc.get();
        if (ret == 0) {
            ret = this->_cr_desc.lock();
            if (ret == 0) {
                do {
                    np = RB_FIND(cr_treestorage_tree, THIS_HEAD_P, &nkey);
                    if (np) {
                        value = np->value;
                        ret = 0;
                        break;
                    } else {
                        if (timeout_sec <= 0.0) {
                            ret = ENOENT;
                            break;
                        } else {
                            ret = ETIMEDOUT;
                            this->_cr_desc.wait(wait_gap);
                        }
                    }
                } while (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) < exp_time);
                this->_cr_desc.unlock();
            }
            this->_cr_desc.put();
        }

        CANCEL_DISABLE_LEAVE();
    }

    return ret;
}

int
CR_TreeStorage::Set(const std::string &key, const std::string &newvalue, std::string *oldvalue,
    const std::string &lock_pass, double locktimeout_sec, double waittimeout_sec)
{
    if (this->_is_static)
        return EROFS;

    int ret = 0;
    cr_treestorage_node_t nkey;
    cr_treestorage_node_t *np = NULL;
    double exp_time = 0.0;
    double wait_gap = (waittimeout_sec <= 0.1) ? waittimeout_sec : 0.1;

    if (waittimeout_sec > 0.0)
        exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + waittimeout_sec;

    nkey.key = key;

    CANCEL_DISABLE_ENTER();

    ret = this->_cr_desc.get();
    if (ret == 0) {
        ret = this->_cr_desc.lock();
        if (ret == 0) {
            do {
                np = RB_FIND(cr_treestorage_tree, THIS_HEAD_P, &nkey);
                if (np != NULL) {
                    if (oldvalue != NULL)
                        *oldvalue = np->value;
                    if (_set_node_lock(np, lock_pass, locktimeout_sec)) {
                        np->value = newvalue;
                        this->_cr_desc.broadcast();
                        ret = 0;
                        break;
                    } else {
                        if (waittimeout_sec <= 0.0) {
                            ret = EACCES;
                            break;
                        } else {
                            ret = ETIMEDOUT;
                            this->_cr_desc.wait(wait_gap);
                        }
                    }
                } else {
                    np = new cr_treestorage_node_t;

                    if (np) {
                        np->key = key;
                        np->value = newvalue;
                        np->lock_expire = 0;

                        if (locktimeout_sec > 0)
                            _set_node_lock(np, lock_pass, locktimeout_sec);

                        if (RB_INSERT(cr_treestorage_tree, THIS_HEAD_P, np) == NULL) {
                            this->_count++;
                            this->_cr_desc.broadcast();
                            ret = 0;
                        } else {
                            delete np;
                            ret = EFAULT;
                        }
                    } else
                        ret = ENOMEM;

                    break;
                }
            } while (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) < exp_time);
            this->_cr_desc.unlock();
        }
        this->_cr_desc.put();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_TreeStorage::Del(const std::string &key, const std::string &lock_pass)
{
    if (this->_is_static)
        return EROFS;

    int ret = 0;
    cr_treestorage_node_t nkey;
    cr_treestorage_node_t *np;

    nkey.key = key;

    CANCEL_DISABLE_ENTER();

    ret = this->_cr_desc.get();
    if (ret == 0) {
        ret = this->_cr_desc.lock();
        if (ret == 0) {
            np = RB_FIND(cr_treestorage_tree, THIS_HEAD_P, &nkey);
            if (np) {
                if (_chk_node_lock(np, lock_pass)) {
                    RB_REMOVE(cr_treestorage_tree, THIS_HEAD_P, np);
                    delete np;
                    this->_count--;
                    this->_cr_desc.broadcast();
                } else
                    ret = EACCES;
            } else
                ret = ENOENT;
            this->_cr_desc.unlock();
        }
        this->_cr_desc.put();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_TreeStorage::GetAll(std::map<std::string,std::string> &goods,
    const std::string *key_like_pattern, char escape_character)
{
    int ret = 0;
    cr_treestorage_node_t *np, *nxtp;

    if (this->_is_static) {
        goods.clear();

        for (np = RB_MIN(cr_treestorage_tree, THIS_HEAD_P);
          np != NULL; np = nxtp) {
            nxtp = RB_NEXT(cr_treestorage_tree, THIS_HEAD_P, np);
            if (key_like_pattern) {
                if (!CR_Class_NS::str_like_cmp(np->key, *key_like_pattern, escape_character)) {
                    continue;
                }
            }
            goods[np->key] = np->value;
        }

        if (goods.size() < 1)
            ret = ENOENT;
    } else {
        CANCEL_DISABLE_ENTER();

        goods.clear();

        ret = this->_cr_desc.get();
        if (ret == 0) {
            ret = this->_cr_desc.lock();
            if (ret == 0) {
                for (np = RB_MIN(cr_treestorage_tree, THIS_HEAD_P);
                  np != NULL; np = nxtp) {
                    nxtp = RB_NEXT(cr_treestorage_tree, THIS_HEAD_P, np);
                    if (key_like_pattern) {
                        if (!CR_Class_NS::str_like_cmp(np->key, *key_like_pattern, escape_character)) {
                            continue;
                        }
                    }
                    goods[np->key] = np->value;
                }
                this->_cr_desc.unlock();
            }
            this->_cr_desc.put();
        }

        if (goods.size() < 1)
            ret = ENOENT;

        CANCEL_DISABLE_LEAVE();
    }

    return ret;
}

int
CR_TreeStorage::Clear(const std::string &lock_pass)
{
    if (this->_is_static)
        return EROFS;

    int ret = 0;
    cr_treestorage_node_t *np, *nxtp;

    CANCEL_DISABLE_ENTER();

    ret = this->_cr_desc.get();
    if (ret == 0) {
        ret = this->_cr_desc.lock();
        if (ret == 0) {
            for (np = RB_MIN(cr_treestorage_tree, THIS_HEAD_P); np != NULL; np = nxtp) {
                nxtp = RB_NEXT(cr_treestorage_tree, THIS_HEAD_P, np);
                if (_chk_node_lock(np, lock_pass)) {
                    RB_REMOVE(cr_treestorage_tree, THIS_HEAD_P, np);
                    delete np;
                    this->_count--;
                }
            }
            this->_cr_desc.broadcast();
            this->_cr_desc.unlock();
        }
        this->_cr_desc.put();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_TreeStorage::Min(std::string &minkey, std::string *value)
{
    int ret = 0;
    cr_treestorage_node_t *np;

    if (this->_is_static) {
        np = RB_MIN(cr_treestorage_tree, THIS_HEAD_P);
        if (np) {
            minkey = np->key;
            if (value != NULL)
                *value = np->value;
        } else
            ret = ENOENT;
    } else {
        CANCEL_DISABLE_ENTER();

        ret = this->_cr_desc.get();
        if (ret == 0) {
            ret = this->_cr_desc.lock();
            if (ret == 0) {
                np = RB_MIN(cr_treestorage_tree, THIS_HEAD_P);
                if (np) {
                    minkey = np->key;
                    if (value != NULL)
                        *value = np->value;
                } else
                    ret = ENOENT;
                this->_cr_desc.unlock();
            }
            this->_cr_desc.put();
        }

        CANCEL_DISABLE_LEAVE();
    }

    return ret;
}

int
CR_TreeStorage::Max(std::string &maxkey, std::string *value)
{
    int ret = 0;
    cr_treestorage_node_t *np;

    if (this->_is_static) {
        np = RB_MAX(cr_treestorage_tree, THIS_HEAD_P);
        if (np) {
            maxkey = np->key;
            if (value != NULL)
                *value = np->value;
        } else
            ret = ENOENT;
    } else {
        CANCEL_DISABLE_ENTER();

        ret = this->_cr_desc.get();
        if (ret == 0) {
            ret = this->_cr_desc.lock();
            if (ret == 0) {
                np = RB_MAX(cr_treestorage_tree, THIS_HEAD_P);
                if (np) {
                    maxkey = np->key;
                    if (value != NULL)
                        *value = np->value;
                } else
                    ret = ENOENT;
                this->_cr_desc.unlock();
            }
            this->_cr_desc.put();
        }

        CANCEL_DISABLE_LEAVE();
    }

    return ret;
}

int
CR_TreeStorage::Prev(const std::string &key, std::string &prevkey, std::string *value)
{
    int ret = 0;
    cr_treestorage_node_t nkey;
    cr_treestorage_node_t *np, *prevp;

    nkey.key = key;

    if (this->_is_static) {
        np = RB_NFIND(cr_treestorage_tree, THIS_HEAD_P, &nkey);
        if (np) {
            prevp = RB_PREV(cr_treestorage_tree, THIS_HEAD_P, np);
            if (prevp) {
                prevkey = prevp->key;
                if (value != NULL)
                    *value = prevp->value;
            } else
                ret = ERANGE;
        } else
           ret = ENOENT;
    } else {
        CANCEL_DISABLE_ENTER();

        ret = this->_cr_desc.get();
        if (ret == 0) {
            ret = this->_cr_desc.lock();
            if (ret == 0) {
                np = RB_NFIND(cr_treestorage_tree, THIS_HEAD_P, &nkey);
                if (np) {
                    prevp = RB_PREV(cr_treestorage_tree, THIS_HEAD_P, np);
                    if (prevp) {
                        prevkey = prevp->key;
                        if (value != NULL)
                            *value = prevp->value;
                    } else
                        ret = ERANGE;
                } else
                   ret = ENOENT;
                this->_cr_desc.unlock();
            }
            this->_cr_desc.put();
        }

        CANCEL_DISABLE_LEAVE();
    }

    return ret;
}

int
CR_TreeStorage::Next(const std::string &key, std::string &nextkey, std::string *value)
{
    int ret = 0;
    cr_treestorage_node_t nkey;
    cr_treestorage_node_t *np, *nextp;

    nkey.key = key;

    if (this->_is_static) {
        np = RB_NFIND(cr_treestorage_tree, THIS_HEAD_P, &nkey);
        if (np) {
            nextp = RB_NEXT(cr_treestorage_tree, THIS_HEAD_P, np);
            if (nextp) {
                nextkey = nextp->key;
                if (value != NULL)
                    *value = nextp->value;
            } else
                ret = ERANGE;
        } else
            ret = ENOENT;
    } else {
        CANCEL_DISABLE_ENTER();

        ret = this->_cr_desc.get();
        if (ret == 0) {
            ret = this->_cr_desc.lock();
            if (ret == 0) {
                np = RB_NFIND(cr_treestorage_tree, THIS_HEAD_P, &nkey);
                if (np) {
                    nextp = RB_NEXT(cr_treestorage_tree, THIS_HEAD_P, np);
                    if (nextp) {
                        nextkey = nextp->key;
                        if (value != NULL)
                            *value = nextp->value;
                    } else
                        ret = ERANGE;
                } else
                    ret = ENOENT;
                this->_cr_desc.unlock();
            }
            this->_cr_desc.put();
        }

        CANCEL_DISABLE_LEAVE();
    }

    return ret;
}

int
CR_TreeStorage::FindNear(const std::string &key, std::string *foundkey,
    std::string *value, const stay_mode_t stay_mode)
{
    int ret = 0;
    cr_treestorage_node_t nkey;
    cr_treestorage_node_t *np;

    nkey.key = key;

    if (this->_is_static) {
        np = RB_NFIND(cr_treestorage_tree, THIS_HEAD_P, &nkey);
        if (!np) {
            if (stay_mode == STAY_MIN) {
                np = RB_MIN(cr_treestorage_tree, THIS_HEAD_P);
            } else if (stay_mode == STAY_MAX) {
                np = RB_MAX(cr_treestorage_tree, THIS_HEAD_P);
            } else
                ret = ENOENT;
        }

        if (ret == 0) {
            if (np) {
                if (foundkey)
                    *foundkey = np->key;
                if (value)
                    *value = np->value;
            } else
                ret = ENOENT;
        }
    } else {
        CANCEL_DISABLE_ENTER();

        ret = this->_cr_desc.get();
        if (ret == 0) {
            ret = this->_cr_desc.lock();
            if (ret == 0) {
                np = RB_NFIND(cr_treestorage_tree, THIS_HEAD_P, &nkey);
                if (!np) {
                    if (stay_mode == STAY_MIN) {
                        np = RB_MIN(cr_treestorage_tree, THIS_HEAD_P);
                    } else if (stay_mode == STAY_MAX) {
                        np = RB_MAX(cr_treestorage_tree, THIS_HEAD_P);
                    } else
                        ret = ENOENT;
                }

                if (ret == 0) {
                    if (np) {
                        if (foundkey)
                            *foundkey = np->key;
                        if (value)
                            *value = np->value;
                    } else
                        ret = ENOENT;
                }
                this->_cr_desc.unlock();
            }
            this->_cr_desc.put();
        }

        CANCEL_DISABLE_LEAVE();
    }

    return ret;
}

int
CR_TreeStorage::FindNear(const std::string &key, std::string &foundkey,
    std::string &value, const stay_mode_t stay_mode)
{
    return this->FindNear(key, &foundkey, &value, stay_mode);
}

int
CR_TreeStorage::FindNearByTree(const std::string &key, std::string &foundkey, std::string &value)
{
    int ret = 0;
    cr_treestorage_node_t *np, *np_tmp;
    std::string key_hash = CR_QuickHash::md5raw(key);
    uint8_t *key_hash_data = (uint8_t *)key_hash.data();
    size_t key_hash_size = key_hash.size();
    size_t bit_size = key_hash_size << 3;

    if (this->_is_static) {
        np = RB_ROOT(THIS_HEAD_P);
        if (np) {
            for (size_t i=0; i<bit_size; i++) {
                if (CR_Class_NS::BMP_BITTEST(key_hash_data, key_hash_size, i))
                    np_tmp = RB_RIGHT(np, entry);
                else
                    np_tmp = RB_LEFT(np, entry);

                if (np_tmp)
                    np = np_tmp;
                else
                    break;
            }

            if (np) {
                foundkey = np->key;
                value = np->value;
            } else
                ret = ENOENT;
        } else
            ret = ENOENT;
    } else {
        CANCEL_DISABLE_ENTER();

        ret = this->_cr_desc.get();
        if (ret == 0) {
            ret = this->_cr_desc.lock();
            if (ret == 0) {
                np = RB_ROOT(THIS_HEAD_P);
                if (np) {
                    for (size_t i=0; i<bit_size; i++) {
                        if (CR_Class_NS::BMP_BITTEST(key_hash_data, key_hash_size, i))
                            np_tmp = RB_RIGHT(np, entry);
                        else
                            np_tmp = RB_LEFT(np, entry);

                        if (np_tmp)
                            np = np_tmp;
                        else
                            break;
                    }

                    if (np) {
                        foundkey = np->key;
                        value = np->value;
                    } else
                        ret = ENOENT;
                } else
                    ret = ENOENT;

                this->_cr_desc.unlock();
            }
            this->_cr_desc.put();
        }

        CANCEL_DISABLE_LEAVE();
    }

    return ret;
}

int
CR_TreeStorage::ListKeys(std::vector<std::string> &keys)
{
    int ret = 0;
    cr_treestorage_node_t *np;

    if (this->_is_static) {
        RB_FOREACH(np, cr_treestorage_tree, THIS_HEAD_P) {
            keys.push_back(np->key);
        }
    } else {
        CANCEL_DISABLE_ENTER();

        keys.clear();

        ret = this->_cr_desc.get();
        if (ret == 0) {
            ret = this->_cr_desc.lock();
            if (ret == 0) {
                RB_FOREACH(np, cr_treestorage_tree, THIS_HEAD_P) {
                    keys.push_back(np->key);
                }
                this->_cr_desc.unlock();
            }
            this->_cr_desc.put();
        }

        CANCEL_DISABLE_LEAVE();
    }

    return ret;
}

int
CR_TreeStorage::ListKeysRange(const std::string &lkey, const std::string &rkey,
    std::vector<std::string> &keys, const std::string &lock_pass, double locktimeout_sec)
{
    int ret = 0;
    cr_treestorage_node_t firstkey;
    cr_treestorage_node_t *np, *nextp;

    if (lkey.compare(rkey) > 0) {
        DPRINTFX(15, "lkey[\"%s\"] > rkey[\"%s\"]\n", lkey.c_str(), rkey.c_str());
        return EINVAL;
    }

    firstkey.key = lkey;

    if (this->_is_static) {
        np = RB_NFIND(cr_treestorage_tree, THIS_HEAD_P, &firstkey);
        if (np) {
            while (np) {
                if (np->key.compare(rkey) > 0)
                    break;
                nextp = RB_NEXT(cr_treestorage_tree, THIS_HEAD_P, np);
                if (locktimeout_sec > 0) {
                    if (_set_node_lock(np, lock_pass, locktimeout_sec))
                        keys.push_back(np->key);
                } else
                    keys.push_back(np->key);
                np = nextp;
            }
        } else
            ret = ENOENT;
    } else {
        CANCEL_DISABLE_ENTER();

        keys.clear();

        ret = this->_cr_desc.get();
        if (ret == 0) {
            ret = this->_cr_desc.lock();
            if (ret == 0) {
                np = RB_NFIND(cr_treestorage_tree, THIS_HEAD_P, &firstkey);
                if (np) {
                    while (np) {
                        if (np->key.compare(rkey) > 0)
                            break;
                        nextp = RB_NEXT(cr_treestorage_tree, THIS_HEAD_P, np);
                        if (locktimeout_sec > 0) {
                            if (_set_node_lock(np, lock_pass, locktimeout_sec))
                                keys.push_back(np->key);
                        } else
                            keys.push_back(np->key);
                        np = nextp;
                    }
                } else
                    ret = ENOENT;
                this->_cr_desc.unlock();
            }
            this->_cr_desc.put();
        }

        if (keys.size() < 1)
            ret = ENOENT;

        CANCEL_DISABLE_LEAVE();
    }

    return ret;
}

int
CR_TreeStorage::PopOne(const std::string &key, std::string &value,
    const std::string &lock_pass, double timeout_sec)
{
    if (this->_is_static)
        return EROFS;

    int ret = 0;
    cr_treestorage_node_t nkey;
    cr_treestorage_node_t *np;
    double exp_time = 0.0;
    double wait_gap = (timeout_sec <= 0.1) ? timeout_sec : 0.1;

    if (timeout_sec > 0.0)
        exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;

    nkey.key = key;

    CANCEL_DISABLE_ENTER();

    ret = this->_cr_desc.get();
    if (ret == 0) {
        ret = this->_cr_desc.lock();
        if (ret == 0) {
            while (ret == 0) {
                np = RB_FIND(cr_treestorage_tree, THIS_HEAD_P, &nkey);
                if (np) {
                    value = np->value;
                    if (_chk_node_lock(np, lock_pass)) {
                        RB_REMOVE(cr_treestorage_tree, THIS_HEAD_P, np);
                        delete np;
                        this->_count--;
                        this->_cr_desc.broadcast();
                        ret = 0;
                    } else {
                        ret = EACCES;
                    }
                    break;
                }

                if (timeout_sec <= 0.0) {
                    ret = ENOENT;
                    break;
                } else if (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) >= exp_time) {
                    ret = ETIMEDOUT;
                    break;
                } else {
                    this->_cr_desc.wait(wait_gap);
                }
            }
            this->_cr_desc.unlock();
        }
        this->_cr_desc.put();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_TreeStorage::PopOneScoped(const std::string &lkey, const std::string &rkey,
    std::string &foundkey, std::string &value, const std::string &lock_pass, double timeout_sec)
{
    if (this->_is_static)
        return EROFS;

    int ret = 0;
    cr_treestorage_node_t nkey;
    cr_treestorage_node_t *np, *nextp;
    double exp_time = 0.0;
    double wait_gap = (timeout_sec <= 0.1) ? timeout_sec : 0.1;

    if (timeout_sec > 0.0)
        exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;

    nkey.key = lkey;

    CANCEL_DISABLE_ENTER();

    ret = this->_cr_desc.get();
    if (ret == 0) {
        ret = this->_cr_desc.lock();
        if (ret == 0) {
            while (ret == 0) {
                np = RB_NFIND(cr_treestorage_tree, THIS_HEAD_P, &nkey);
                while (np) {
                    if (np->key.compare(rkey) >= 0) {
                        np = NULL;
                        break;
                    }
                    if (_chk_node_lock(np, lock_pass))
                        break;
                    nextp = RB_NEXT(cr_treestorage_tree, THIS_HEAD_P, np);
                    np = nextp;
                }
                if (np) {
                    foundkey = np->key;
                    value = np->value;
                    RB_REMOVE(cr_treestorage_tree, THIS_HEAD_P, np);
                    delete np;
                    this->_count--;
                    this->_cr_desc.broadcast();
                    ret = 0;
                    break;
                }

                if (timeout_sec <= 0.0) {
                    ret = ENOENT;
                    break;
                } else if (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) >= exp_time) {
                    ret = ETIMEDOUT;
                    break;
                } else {
                    this->_cr_desc.wait(wait_gap);
                }
            }
            this->_cr_desc.unlock();
        }
        this->_cr_desc.put();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_TreeStorage::PopOneScopedReverse(const std::string &lkey, const std::string &rkey,
    std::string &foundkey, std::string &value, const std::string &lock_pass, double timeout_sec)
{
    if (this->_is_static)
        return EROFS;

    int ret = 0;
    cr_treestorage_node_t nkey;
    cr_treestorage_node_t *np, *prevp, *prevp0;
    double exp_time = 0.0;
    double wait_gap = (timeout_sec <= 0.1) ? timeout_sec : 0.1;

    if (timeout_sec > 0.0)
        exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;

    nkey.key = rkey;

    CANCEL_DISABLE_ENTER();

    ret = this->_cr_desc.get();
    if (ret == 0) {
        ret = this->_cr_desc.lock();
        if (ret == 0) {
            while (ret == 0) {
                np = RB_NFIND(cr_treestorage_tree, THIS_HEAD_P, &nkey);
                if (np) {
                    if (np->key.compare(rkey) > 0) {
                        prevp = RB_PREV(cr_treestorage_tree, THIS_HEAD_P, np);
                        np = NULL;
                        while (prevp) {
                            if (prevp->key.compare(lkey) > 0) {
                                if (_chk_node_lock(prevp, lock_pass)) {
                                    np = prevp;
                                    break;
                                }
                                prevp0 = RB_PREV(cr_treestorage_tree, THIS_HEAD_P, prevp);
                                prevp = prevp0;
                            } else
                                break;
                        }
                    }
                } else {
                    prevp = RB_MAX(cr_treestorage_tree, THIS_HEAD_P);
                    while (prevp) {
                        if (prevp->key.compare(lkey) > 0) {
                            if (_chk_node_lock(prevp, lock_pass)) {
                                np = prevp;
                                break;
                            }
                            prevp0 = RB_PREV(cr_treestorage_tree, THIS_HEAD_P, prevp);
                            prevp = prevp0;
                        } else
                            break;
                    }
                }
                if (np) {
                    foundkey = np->key;
                    value = np->value;
                    RB_REMOVE(cr_treestorage_tree, THIS_HEAD_P, np);
                    delete np;
                    this->_count--;
                    this->_cr_desc.broadcast();
                    ret = 0;
                    break;
                }

                if (timeout_sec <= 0.0) {
                    ret = ENOENT;
                    break;
                } else if (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) >= exp_time) {
                    ret = ETIMEDOUT;
                    break;
                } else {
                    this->_cr_desc.wait(wait_gap);
                }
            }
            this->_cr_desc.unlock();
        }
        this->_cr_desc.put();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_TreeStorage::LockKey(const std::string &key, const std::string &lock_pass, double locktimeout_sec)
{
    if (this->_is_static)
        return EROFS;

    int ret = 0;
    cr_treestorage_node_t nkey;
    cr_treestorage_node_t *np;

    nkey.key = key;

    CANCEL_DISABLE_ENTER();

    ret = this->_cr_desc.get();
    if (ret == 0) {
        ret = this->_cr_desc.lock();
        if (ret == 0) {
            np = RB_FIND(cr_treestorage_tree, THIS_HEAD_P, &nkey);
            if (np) {
                if (!_set_node_lock(np, lock_pass, locktimeout_sec))
                    ret = EPERM;
            } else
                ret = ENOENT;
            this->_cr_desc.unlock();
        }
        this->_cr_desc.put();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

ssize_t
CR_TreeStorage::LockRange(const std::string &lkey, const std::string &rkey, const std::string &lock_pass,
    double locktimeout_sec)
{
    if (this->_is_static)
        return EROFS;

    ssize_t count = 0;
    cr_treestorage_node_t nkey;
    cr_treestorage_node_t *np, *nextp;

    nkey.key = lkey;

    CANCEL_DISABLE_ENTER();

    if (this->_cr_desc.get() == 0) {
        if (this->_cr_desc.lock() == 0) {
            np = RB_NFIND(cr_treestorage_tree, THIS_HEAD_P, &nkey);
            while (np) {
                if (_set_node_lock(np, lock_pass, locktimeout_sec))
                    count++;

                nextp = RB_NEXT(cr_treestorage_tree, THIS_HEAD_P, np);
                if (nextp)
                    if (nextp->key.compare(rkey) > 0)
                        break;

                np = nextp;
            }
            this->_cr_desc.unlock();
        }
        this->_cr_desc.put();
    }

    CANCEL_DISABLE_LEAVE();

    return count;
}

int
CR_TreeStorage::UnlockKey(const std::string &key, const std::string &lock_pass)
{
    if (this->_is_static)
        return EROFS;

    int ret = 0;
    cr_treestorage_node_t nkey;
    cr_treestorage_node_t *np;

    nkey.key = key;

    CANCEL_DISABLE_ENTER();

    ret = this->_cr_desc.get();
    if (ret == 0) {
        ret = this->_cr_desc.lock();
        if (ret == 0) {
            np = RB_FIND(cr_treestorage_tree, THIS_HEAD_P, &nkey);
            if (np) {
                if (!_clr_node_lock(np, lock_pass))
                    ret = EPERM;
            } else
                ret = ENOENT;
            this->_cr_desc.unlock();
        }
        this->_cr_desc.put();
    }

    CANCEL_DISABLE_LEAVE();

    return ret;
}

ssize_t
CR_TreeStorage::UnlockRange(const std::string &lkey, const std::string &rkey, const std::string &lock_pass)
{
    if (this->_is_static)
        return EROFS;

    ssize_t count = 0;
    cr_treestorage_node_t nkey;
    cr_treestorage_node_t *np, *nextp;

    nkey.key = lkey;

    CANCEL_DISABLE_ENTER();

    if (this->_cr_desc.get() == 0) {
        if (this->_cr_desc.lock() == 0) {
            np = RB_NFIND(cr_treestorage_tree, THIS_HEAD_P, &nkey);
            while (np) {
                if (_clr_node_lock(np, lock_pass))
                    count++;

                nextp = RB_NEXT(cr_treestorage_tree, THIS_HEAD_P, np);
                if (nextp)
                    if (nextp->key.compare(rkey) > 0)
                        break;

                np = nextp;
            }
            this->_cr_desc.unlock();
        }
        this->_cr_desc.put();
    }

    CANCEL_DISABLE_LEAVE();

    return count;
}
