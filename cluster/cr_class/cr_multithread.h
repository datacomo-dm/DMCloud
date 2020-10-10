
#ifndef __H_CR_MULTITHREAD_H__
#define __H_CR_MULTITHREAD_H__

#include <unistd.h>

#include <set>
#include <map>

#include <cr_class/cr_datacontrol.h>
#include <cr_class/cr_blockqueue.h>
#include <cr_class/cr_treestorage.h>
#include <cr_class/cr_thread.h>

#define CR_MULTITHREAD_TAG_MULTITHREAD_PTR	("_cr_multithread_tag_multithread_ptr")
#define CR_MULTITHREAD_TAG_THREAD_CLASS		("_cr_multithread_tag_thread_class")
#define CR_MULTITHREAD_TAG_THREAD_NAME		("_cr_multithread_tag_thread_name")

class CR_MultiThread {
public:
	CR_MultiThread();
	~CR_MultiThread();

	int Get(double timeout_sec=0.0);
	int Put();

	int Spawn(const std::string &thread_class, const std::string &thread_name, const CR_Thread::TDB_t &tdb);

	int Stop(const std::string &thread_class, const std::string &thread_name,
		const intptr_t thread_id);

	int ThreadGroupSetTag(const std::string &thread_class, const std::string &thread_name,
		const std::string &tag_key, const std::string &tag_value);

	int ThreadGroupGetTag(const std::string &thread_class, const std::string &thread_name,
		const std::string &tag_key, std::map<intptr_t,std::string> &thread_group_tag_value_map);

	int QueueSetMaxSize(const std::string &thread_class, const std::string &thread_name,
		const size_t max_size);

	int QueueGetSize(const std::string &thread_class, const std::string &thread_name,
		size_t &size);

	int QueuePushByName(const std::string &thread_class, const std::string &thread_name,
		const std::string &s, double timeout_sec=0.0);
	int QueuePushByHash(const std::string &thread_class, const std::string &key,
		const std::string &s, double timeout_sec=0.0);

	int QueuePushFrontByName(const std::string &thread_class,
		const std::string &thread_name, const std::string &s);
	int QueuePushFrontByHash(const std::string &thread_class,
		const std::string &key, const std::string &s);

	int QueuePull(const std::string &thread_class, const std::string &thread_name,
		std::string &s, double timeout_sec=0.0);

	CR_TreeStorage *GetTreePtr(const std::string &tree_name);
private:
	CR_DataControl::Descriptor _cr_desc;

	std::map<std::string,CR_TreeStorage*> _data_tree_map;
	std::map<std::string,CR_TreeStorage*> _name2tab_tree_map;

	std::set<void*> _handle_set;
	std::set<void*> _tree_set;
	std::set<void*> _tab_set;

	inline std::string name2id(const std::string &name);
	int do_queue_push(const bool push_front, const bool hash_pushconst,
		const std::string &thread_class, const std::string &key,
		const std::string &s, double timeout_sec=0.0);

	void *get_tab(const std::string &thread_class, const std::string &key,
		const bool hash=false);
	void put_tab(void *_tab_p);

	CR_TreeStorage *_get_tree_p(const std::string &tree_name);
};

#endif /* __H_CR_MULTITHREAD_H__  */
