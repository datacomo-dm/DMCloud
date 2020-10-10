#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <string>
#include <list>

#include <cr_class/cr_class.h>
#include <cr_class/cr_thread.h>
#include <cr_class/cr_treestorage.h>

#define MIN_THREAD_COUNT	(2)

#define DEFAULT_TREE_NAME	"share_tree"

std::string tree_add(CR_Thread::handle_t th);
std::string tree_pop(CR_Thread::handle_t th);

int on_thread_exit(CR_Thread::handle_t th, const std::string &result);
int on_thread_cancel(CR_Thread::handle_t th);

int main(int argc, char *argv[])
{
	std::list<CR_Thread::handle_t> thread_list;
	CR_Thread::handle_t th;
	CR_TreeStorage *tree_p;
	CR_Thread::TDB_t tdb;
	int rate, i = 0;
	int err = 0;
	std::string thread_result;
	size_t thread_count = 0;

	if (argc > 2)
		thread_count = atoi(argv[1]);
	if (thread_count < MIN_THREAD_COUNT)
		thread_count = MIN_THREAD_COUNT;

	tdb.thread_flags = CR_THREAD_JOINABLE;

	err = err;
	srand(time(NULL));
	tree_p = new CR_TreeStorage;

	while (1) {
		rate = thread_list.size() * 100 / thread_count;

		if (rand() % 100 >= rate) {
			if (rand() % 100 < 90) {
				tdb.thread_func = tree_pop;
				tdb.on_thread_exit = on_thread_exit;
				tdb.on_thread_cancel = on_thread_cancel;
				th = CR_Thread::talloc(tdb);
				CR_Thread::settag(th, "tree", CR_Class_NS::ptr2str(tree_p));
				CR_Thread::settag(th, "class", "tree_pop");
				CR_Thread::start(th);
				DPRINTFX(7, "Start thread[%016lX] OK\n", CR_Thread::gettid(th));
			} else {
				tdb.thread_func = tree_add;
				tdb.on_thread_exit = on_thread_exit;
				tdb.on_thread_cancel = on_thread_cancel;
				th = CR_Thread::talloc(tdb);
				CR_Thread::settag(th, "tree", CR_Class_NS::ptr2str(tree_p));
				CR_Thread::settag(th, "class", "tree_add");
				CR_Thread::start(th);
				DPRINTFX(7, "Start thread[%016lX] OK\n", CR_Thread::gettid(th));
			}
			thread_list.push_back(th);
		} else {
			th = thread_list.front();
			if (!th)
				continue;
			thread_list.pop_front();
			if (rand() % 100 < 30) {
				err = CR_Thread::cancel(th);
				if (err == 0)
					DPRINTFX(5, "Cancel thread[%016lX] OK\n", CR_Thread::gettid(th));
				else
					DPRINTFX(5, "Cancel thread[%016lX] Failed, err:%s\n", CR_Thread::gettid(th), strerror(err));
				err = CR_Thread::join(th, &thread_result);
				if (err == 0)
					DPRINTFX(0, "Thread[%016lX] join OK, result=%s\n", CR_Thread::gettid(th), thread_result.c_str());
				else
					DPRINTFX(0, "Thread[%016lX] join FAILED!, ret=%d, msg=%s\n", CR_Thread::gettid(th), err, strerror(err));
			} else {
				err = CR_Thread::stop(th);
				if (err == 0)
					DPRINTFX(5, "Stop thread[%016lX] OK\n", CR_Thread::gettid(th));
				else
					DPRINTFX(5, "Stop thread[%016lX] Failed, err:%s\n", CR_Thread::gettid(th), strerror(err));
				err = CR_Thread::join(th, &thread_result);
				if (err == 0)
					DPRINTFX(0, "Thread[%016lX] join OK, result=%s\n", CR_Thread::gettid(th), thread_result.c_str());
				else
					DPRINTFX(0, "Thread[%016lX] join FAILED!, ret=%d, msg=%s\n", CR_Thread::gettid(th), err, strerror(err));
			}
		}

		i++;

		usleep(1000);
	}

	sleep(1);
	DPRINTFX(0, "\n=============\n==CLEAR ALL==\n=============\n");

	while (!thread_list.empty()) {
		th = thread_list.front();
		if (!th)
			continue;
		thread_list.pop_front();
		err = CR_Thread::cancel(th);
		if (err == 0)
			DPRINTFX(5, "Stop thread[%016lX] OK\n", CR_Thread::gettid(th));
		else
			DPRINTFX(5, "Stop thread[%016lX] Failed, err:%s\n", CR_Thread::gettid(th), strerror(err));
		err = CR_Thread::join(th, &thread_result);
		if (err == 0)
			DPRINTFX(0, "Thread[%016lX] join OK, result=%s\n", CR_Thread::gettid(th), thread_result.c_str());
		else
			DPRINTFX(0, "Thread[%016lX] join FAILED!, ret=%d, msg=%s\n", CR_Thread::gettid(th), err, strerror(err));
	}

//	delete tree_p;

	printf("OK\n");

	return 0;
}

std::string
tree_add(CR_Thread::handle_t th)
{
	CR_TreeStorage *tree_p = NULL;
	std::string tree_p_buf = "";
	std::string k, v;
	char k_buf[32];
	char v_buf[64];
	int err;

	tree_p = (CR_TreeStorage *)CR_Class_NS::str2ptr(CR_Thread::gettag(th, "tree"));
	if (!tree_p) {
		DPRINTF("NO TREE\n");
		return "NOTREE";
	}

	while (1) {
		if (CR_Thread::teststop(th)) {
			DPRINTF("Got STOP\n");
			return "Stopped";
		}

		snprintf(k_buf, sizeof(k_buf), "K-%08X", rand());
		snprintf(v_buf, sizeof(v_buf), "V-%08X:%08X:%08X", rand(), rand(), rand());
		k = k_buf;
		v = v_buf;

		if ((err = tree_p->Add(k, v)) == 0) {
			DPRINTF("Add K=%s, V=%s OK\n", k.c_str(), v.c_str());
		} else {
			DPRINTF("Add K=%s, V=%s Failed, err:%s\n", k.c_str(), v.c_str(), strerror(err));
		}

		usleep(rand() % 100000 + 100000);
	}
}

std::string
tree_pop(CR_Thread::handle_t th)
{
	CR_TreeStorage *tree_p = NULL;
	std::string tree_p_buf = "";
	std::string k, v;
	int err;

	tree_p = (CR_TreeStorage *)CR_Class_NS::str2ptr((CR_Thread::gettag(th, "tree")));
	if (!tree_p) {
		DPRINTF("NO TREE\n");
		return "NOTREE";
	}

	while (1) {
		if (CR_Thread::teststop(th)) {
			DPRINTF("Got STOP\n");
			return "Stopped";
		}

		if ((err = tree_p->PopOneScoped("K-", "L", k, v, NULL, 0.1)) == 0) {
			DPRINTF("Pop K=%s, V=%s OK\n", k.c_str(), v.c_str());
		} else if (err != ETIMEDOUT) {
			DPRINTF("Pop Failed, err:%s\n", strerror(err));
		}
	}
}

int
on_thread_exit(CR_Thread::handle_t th, const std::string &result)
{
	std::string t_class;
	intptr_t tid = 0;

	t_class.clear();

	t_class = CR_Thread::gettag(th, "class");
	tid = CR_Thread::gettid(th);

	DPRINTFX(7, "thread %s[%016lX] sad \"%s\" and exit\n",
		t_class.c_str(), tid, result.c_str());

	return 0;
}

int
on_thread_cancel(CR_Thread::handle_t th)
{
	std::string t_class;
	intptr_t tid = 0;

	t_class.clear();

	t_class = CR_Thread::gettag(th, "class");
	tid = CR_Thread::gettid(th);

	DPRINTFX(7, "thread %s[%016lX] canceled\n",
		t_class.c_str(), tid);

	return 0;
}
