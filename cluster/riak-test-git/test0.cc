#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <string>
#include <list>

#include <cr_class/cr_class.h>
#include <cr_class/cr_thread.h>
#include <cr_class/cr_treestorage.h>

#define THREAD_COUNT		(200)

#define DEFAULT_TREE_NAME	"share_tree"

std::string tree_add(CR_Thread::handle_t thread_self);
std::string tree_pop(CR_Thread::handle_t thread_self);

int on_thread_exit(CR_Thread::handle_t thread_self, const std::string &result);
int on_thread_cancel(CR_Thread::handle_t thread_self);

int main(int argc, char *argv[])
{
	std::list<CR_Thread::handle_t> thread_list;
	CR_Thread::handle_t th;
	CR_TreeStorage *tree_p;
	CR_Thread::TDB_t tdb;
	char name_buf[32];
	int rate, i = 0;
	int err = 0;

	err = err;
	srand(time(NULL));
	tree_p = new CR_TreeStorage;

	while (1) {
		if (rand() % 100000 < 10)
			break;

		rate = thread_list.size() * 100 / THREAD_COUNT;

		if (rand() % 100 >= rate) {
			if (rand() % 100 < 90) {
				tdb.thread_func = tree_pop;
				tdb.on_thread_exit = on_thread_exit;
				tdb.on_thread_cancel = on_thread_cancel;
				th = CR_Thread::talloc(tdb);
				CR_Thread::settag_ptr(th, "tree", tree_p);
				CR_Thread::settag_str(th, "class", "tree_pop");
				snprintf(name_buf, sizeof(name_buf), "%08d", i);
				CR_Thread::settag_str(th, "id", std::string(name_buf));
				CR_Thread::start(th);
				DPRINTF("Start thread[%016llX] OK\n", (long long unsigned int)th);
			} else {
				tdb.thread_func = tree_add;
				tdb.on_thread_exit = on_thread_exit;
				tdb.on_thread_cancel = on_thread_cancel;
				th = CR_Thread::talloc(tdb);
				CR_Thread::settag_ptr(th, "tree", tree_p);
				CR_Thread::settag_str(th, "class", "tree_add");
				snprintf(name_buf, sizeof(name_buf), "%08d", i);
				CR_Thread::settag_str(th, "id", std::string(name_buf));
				CR_Thread::start(th);
				DPRINTF("Start thread[%016llX] OK\n", (long long unsigned int)th);
			}
			thread_list.push_back(th);
		} else {
			th = thread_list.front();
			if (!th)
				continue;
			thread_list.pop_front();
			err = CR_Thread::stop(th, 5);
			if (err == 0)
				DPRINTF("Stop thread[%016llX] OK\n", (long long unsigned int)th);
			else
				DPRINTF("Stop thread[%016llX] Failed, err:%s\n", (long long unsigned int)th, strerror(err));
		}

		i++;

		usleep(rand() % 1000 + 1000);
	}

	sleep(5);
	DPRINTF("\n=============\n==CLEAR ALL==\n=============\n");

	while (!thread_list.empty()) {
		th = thread_list.front();
		if (!th)
			continue;
		thread_list.pop_front();
		err = CR_Thread::stop(th, 5);
		if (err == 0)
			DPRINTF("Stop thread[%016llX] OK\n", (long long unsigned int)th);
		else
			DPRINTF("Stop thread[%016llX] Failed, err:%s\n", (long long unsigned int)th, strerror(err));
	}

	sleep(20);

	DPRINTF("OK\n");

	return 0;
}

std::string
tree_add(CR_Thread::handle_t thread_self)
{
	CR_TreeStorage *tree_p = NULL;
	std::string tree_p_buf = "";
	std::string k, v;
	char k_buf[32];
	char v_buf[64];
	int err;

	tree_p = (CR_TreeStorage *)CR_Thread::gettag_ptr(thread_self, "tree");
	if (!tree_p) {
		DPRINTF("NO TREE\n");
		return "NOTREE";
	}

	while (1) {
		if (CR_Thread::teststop(thread_self)) {
			DPRINTF("Got STOP\n");
			sleep(rand() % 15);
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
tree_pop(CR_Thread::handle_t thread_self)
{
	CR_TreeStorage *tree_p = NULL;
	std::string tree_p_buf = "";
	std::string k, v;
	int err;

	tree_p = (CR_TreeStorage *)CR_Thread::gettag_ptr(thread_self, "tree");
	if (!tree_p) {
		DPRINTF("NO TREE\n");
		return "NOTREE";
	}

	while (1) {
		if (CR_Thread::teststop(thread_self)) {
			DPRINTF("Got STOP\n");
			sleep(rand() % 10);
			return "Stopped";
		}

		if ((err = tree_p->PopOneScoped("K-", "L", k, v, NULL, 1)) == 0) {
			DPRINTF("Pop K=%s, V=%s OK\n", k.c_str(), v.c_str());
		} else {
			DPRINTF("Pop Failed, err:%s\n", strerror(err));
		}
	}
}

int
on_thread_exit(CR_Thread::handle_t thread_self, const std::string &result)
{
	std::string t_class, t_id;

	t_class.clear();
	t_id.clear();

	t_class = CR_Thread::gettag_str(thread_self, "class");
	t_id = CR_Thread::gettag_str(thread_self, "id");

	DPRINTF("thread %s[%s] sad \"%s\" and exit\n",
		t_class.c_str(), t_id.c_str(), result.c_str());

	return 0;
}

int
on_thread_cancel(CR_Thread::handle_t thread_self)
{
	std::string t_class, t_id;

	t_class.clear();
	t_id.clear();

	t_class = CR_Thread::gettag_str(thread_self, "class");
	t_id = CR_Thread::gettag_str(thread_self, "id");

	DPRINTF("thread %s[%s] CANCELED\n",
		t_class.c_str(), t_id.c_str());

	return 0;
}
