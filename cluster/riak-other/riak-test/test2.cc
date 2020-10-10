#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <string>

#include <cr_class/cr_class.h>
#include <cr_class/cr_multithread.h>

#define TREE_ADD_COUNT		(5)
#define TREE_POP_COUNT		(5)
#define QUEUE_PUSH_COUNT	(10)
#define QUEUE_PULL_COUNT	(10)

std::string tree_add(CR_Thread::handle_t thread_self);
std::string tree_pop(CR_Thread::handle_t thread_self);

std::string queue_push(CR_Thread::handle_t thread_self);
std::string queue_pull(CR_Thread::handle_t thread_self);

int on_thread_exit(CR_Thread::handle_t thread_self, const std::string &result);

int main(int argc, char *argv[])
{
	char name_buf[32];
	std::string thread_name;
	CR_MultiThread *cr_mt = new CR_MultiThread;
	std::list<std::string> senders;
	std::list<std::string> treepop;
	std::list<std::string> queuepull;
	CR_Thread::TDB_t tdb;
	int ret;

	for (int i=0; i<TREE_ADD_COUNT; i++) {
		snprintf(name_buf, sizeof(name_buf), "TREE_ADD-%08X", i);
		thread_name = name_buf;
		tdb.thread_func = tree_add;
		tdb.on_thread_exit = on_thread_exit;
		tdb.default_tags["tree"] = "global_tree";
		for (int j=0;j<TREE_ADD_COUNT;j++) {
			ret = cr_mt->Spawn("SEND", thread_name, tdb);
		}
		senders.push_back(thread_name);
	}

	for (int i=0; i<TREE_POP_COUNT; i++) {
		snprintf(name_buf, sizeof(name_buf), "TREE_POP-%08X", i);
		thread_name = name_buf;
		tdb.thread_func = tree_pop;
		tdb.on_thread_exit = on_thread_exit;
		tdb.default_tags["tree"] = "global_tree";
		for (int j=0;j<TREE_POP_COUNT;j++) {
			ret = cr_mt->Spawn("TREE_POP", thread_name, tdb);
		}
		treepop.push_back(thread_name);
	}

	for (int i=0; i<QUEUE_PUSH_COUNT; i++) {
		snprintf(name_buf, sizeof(name_buf), "QUEUE_PUSH-%08X", i);
		thread_name = name_buf;
		tdb.thread_func = queue_push;
		tdb.on_thread_exit = on_thread_exit;
		for (int j=0;j<QUEUE_PUSH_COUNT;j++) {
			ret = cr_mt->Spawn("SEND", thread_name, tdb);
		}
		senders.push_back(thread_name);
	}

	for (int i=0; i<QUEUE_PULL_COUNT; i++) {
		snprintf(name_buf, sizeof(name_buf), "QUEUE_PULL-%08X", i);
		thread_name = name_buf;
		tdb.thread_func = queue_pull;
		tdb.on_thread_exit = on_thread_exit;
		for (int j=0;j<QUEUE_PULL_COUNT;j++) {
			ret = cr_mt->Spawn("QUEUE_PULL", thread_name, tdb);
		}
		queuepull.push_back(thread_name);
	}

	DPRINTF("SIZE = %llu\n", (long long unsigned int)senders.size());

	sleep(10);

	while (!senders.empty()) {
		thread_name = senders.front();
		cr_mt->Stop("SEND", thread_name, -1);
		DPRINTF("Kill %s\n", thread_name.c_str());
		senders.pop_front();
	}

	sleep(2);

	while (!treepop.empty()) {
		thread_name = treepop.front();
		cr_mt->Stop("TREE_POP", thread_name, -1);
		DPRINTF("Kill %s\n", thread_name.c_str());
		treepop.pop_front();
	}

	while (!queuepull.empty()) {
		thread_name = queuepull.front();
		cr_mt->Stop("QUEUE_PULL", thread_name, -1);
		DPRINTF("Kill %s\n", thread_name.c_str());
		queuepull.pop_front();
	}

	sleep(10);

	delete cr_mt;

	DPRINTF("cr_mt deleted\n");

	sleep(10);

	DPRINTF("OK\n");

	return ret;
}

std::string
tree_add(CR_Thread::handle_t thread_self)
{
	CR_MultiThread *cmt_p;
	CR_TreeStorage *tree_p;
	std::string k, v;
	char k_buf[32];
	char v_buf[64];
	int err;

	DPRINTF("ENTER\n");

	cmt_p = (CR_MultiThread *)CR_Class_NS::str2ptr(
        	CR_Thread::gettag(thread_self, CR_MULTITHREAD_TAG_MULTITHREAD_PTR));
	tree_p = cmt_p->GetTreePtr(CR_Thread::gettag(thread_self, "tree"));

	while (1) {
		if (CR_Thread::teststop(thread_self)) {
			sleep(rand() % 20);
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

		usleep(rand() % 200000 + 200000);
	}
}

std::string
tree_pop(CR_Thread::handle_t thread_self)
{
	CR_MultiThread *cmt_p;
	CR_TreeStorage *tree_p;
	std::string k, v, extra_arg;
	int err;

	DPRINTF("ENTER\n");

	cmt_p = (CR_MultiThread *)CR_Class_NS::str2ptr(
        	CR_Thread::gettag(thread_self, CR_MULTITHREAD_TAG_MULTITHREAD_PTR));
	tree_p = cmt_p->GetTreePtr(CR_Thread::gettag(thread_self, "tree"));

	while (1) {
		if (CR_Thread::teststop(thread_self)) {
			sleep(rand() % 20);
			return "Stopped";
		}

                err = tree_p->PopOneScoped("K-", "L", k, v, NULL, 1);
		if (err == 0) {
			DPRINTF("Pop K=%s, V=%s OK\n", k.c_str(), v.c_str());
		} else if (err != ETIMEDOUT) {
			DPRINTF("Pop Failed, err:%s\n", strerror(err));
		}
	}
}

std::string
queue_push(CR_Thread::handle_t thread_self)
{
	CR_MultiThread *cmt_p;
	std::string v;
	char v_buf[64];
	int err;
	std::string thread_class;
	std::string thread_name;
	int64_t thread_id;

	cmt_p = (CR_MultiThread *)CR_Class_NS::str2ptr(
        	CR_Thread::gettag(thread_self, CR_MULTITHREAD_TAG_MULTITHREAD_PTR));
	thread_class = CR_Thread::gettag(thread_self, CR_MULTITHREAD_TAG_THREAD_CLASS);
	thread_name = CR_Thread::gettag(thread_self, CR_MULTITHREAD_TAG_THREAD_NAME);
	thread_id = CR_Thread::gettid(thread_self);

	printf("%s->%s[%d]:Enter\n",
		thread_class.c_str(), thread_name.c_str(), (int)thread_id);

	while (1) {
		if (CR_Thread::teststop(thread_self)) {
			sleep(rand() % 20);
			return "Stopped";
		}

		snprintf(v_buf, sizeof(v_buf), "V-%08X:%08X:%08X", rand(), rand(), rand());
		v = v_buf;

                err = cmt_p->QueuePushByHash("QUEUE_PULL", v, v);
		if (err == 0) {
			printf("%s->%s[%d]:QueuePushByHash V=%s OK\n",
				thread_class.c_str(), thread_name.c_str(), (int)thread_id, v.c_str());
		} else {
			fprintf(stderr, "%s->%s[%d]:QueuePushByHash V=%s ERROR, ret = %d, msg = %s\n",
				thread_class.c_str(), thread_name.c_str(), (int)thread_id, v.c_str(),
				err, strerror(err));
		}

		usleep(rand() % 200000 + 200000);
	}
}

std::string
queue_pull(CR_Thread::handle_t thread_self)
{
	CR_MultiThread *cmt_p;
	std::string v;
	int err;
	std::string thread_class;
	std::string thread_name;
	int64_t thread_id;

	cmt_p = (CR_MultiThread *)CR_Class_NS::str2ptr(
        	CR_Thread::gettag(thread_self, CR_MULTITHREAD_TAG_MULTITHREAD_PTR));
	thread_class = CR_Thread::gettag(thread_self, CR_MULTITHREAD_TAG_THREAD_CLASS);
	thread_name = CR_Thread::gettag(thread_self, CR_MULTITHREAD_TAG_THREAD_NAME);
	thread_id = CR_Thread::gettid(thread_self);

	printf("%s->%s[%d]:Enter\n",
		thread_class.c_str(), thread_name.c_str(), (int)thread_id);

	while (1) {
		if (CR_Thread::teststop(thread_self)) {
			sleep(rand() % 20);
			return "Stopped";
		}

                err = cmt_p->QueuePull(thread_class, thread_name, v, 1);
		if (err == 0) {
			printf("%s->%s[%d]:QueuePull OK, got %s\n",
				thread_class.c_str(), thread_name.c_str(), (int)thread_id, v.c_str());
		} else if (err != ETIMEDOUT) {
			fprintf(stderr, "%s->%s[%d]:QueuePull ERR, ret = %d, msg = %s\n",
				thread_class.c_str(), thread_name.c_str(), (int)thread_id,
				err, strerror(err));
		}
	}
}

int
on_thread_exit(CR_Thread::handle_t thread_self, const std::string &result)
{
	std::string t_class;
	int64_t t_id;

	t_class = CR_Thread::gettag(thread_self, CR_MULTITHREAD_TAG_THREAD_CLASS);
	t_id = CR_Thread::gettid(thread_self);

	DPRINTF("thread %s[%d] sad \"%s\" and exit\n",
		t_class.c_str(), (int)t_id, result.c_str());

	return 0;
}
