#ifndef __H_CR_THREAD_H__
#define __H_CR_THREAD_H__

#ifndef __CR_THREAD_FLAGS__
#define __CR_THREAD_FLAGS__

#define CR_THREAD_JOINABLE (0x00000001U)

#endif /* __CR_THREAD_FLAGS__ */

#include <unistd.h>
#include <map>
#include <string>
#include <cr_class/cr_datacontrol.h>
#include <cr_class/cr_automsg.h>

extern size_t cr_thread_count;
extern size_t cr_thread_kill_count;
extern size_t cr_thread_recycling_count;
extern size_t cr_thread_recyced_count;
extern size_t cr_thread_garbage_count;

namespace CR_Thread {
	typedef void *handle_t;

	typedef enum {E_UNKNOWN=0, E_INT=10, E_STRING=20, E_VOIDPTR=30, E_STDEXP=40} exp_type_t;

	typedef std::string (*thread_func_t)(handle_t handle);
	typedef int (*on_thread_exit_t)(handle_t handle, const std::string &result);
	typedef int (*on_thread_cancel_t)(handle_t handle);
	typedef int (*on_thread_exception_t)(handle_t handle, const exp_type_t exp_type, void *exp_ptr);

	class TDB_t {
	public:
		TDB_t();
		~TDB_t();

		thread_func_t thread_func;
		on_thread_exit_t on_thread_exit;
		on_thread_cancel_t on_thread_cancel;
		on_thread_exception_t on_thread_exception;
		uint64_t thread_flags;
		std::map<std::string,std::string> default_tags;
	};

	///////////////

        int push_delayed_job(void *p, double (*job_func_p)(void *), double timeout_sec=1.0);

	handle_t talloc(const TDB_t &tdb);
        void tfree(handle_t handle);

	intptr_t gettid(handle_t handle);

	int start(handle_t handle);
	int stop(handle_t handle);
	int cancel(handle_t handle);
	bool teststop(handle_t handle);
	int join(handle_t handle, std::string *result=NULL);

	int desc_get(handle_t handle, double timeout_sec=0.0);
	int desc_put(handle_t handle);
	int desc_lock(handle_t handle, double timeout_sec=0.0);
	int desc_unlock(handle_t handle);
	int desc_wait(handle_t handle, double timeout_sec=0.0);
	int desc_signal(handle_t handle);
	int desc_boardcast(handle_t handle);

	int settag(handle_t handle, const std::string &tag_key, const std::string &tag_value);
	std::string gettag(handle_t handle, const std::string &tag_key);
};

#endif /* __H_CR_THREAD_H__ */
