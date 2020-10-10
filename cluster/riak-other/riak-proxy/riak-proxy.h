#ifndef __H_RIAK_PROXY_H__
#define __H_RIAK_PROXY_H__

#include <riakdrv/riakconnect.h>
#include <cr_class/cr_thread.h>

int doaccept(const int sock, const std::string &, const std::string &, void *);

std::string refresh_thread(CR_Thread::handle_t);

std::string mkriakerr(const int);

int doaccept (int, const std::string&);

std::string get_convert(const std::string &, const std::string &, const std::string &, const std::string &);

extern int cr_debug_level;

#endif // __H_RIAK_PROXY_H__
