#ifndef __H_RIAK_TOOLBOX_H__
#define __H_RIAK_TOOLBOX_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <vector>
#include <algorithm>

#include <cr_class/cr_class.h>
#include <riakdrv/riakcluster.h>
#include <riakdrv/riakdrv++.pb.h>

#include "msgIB.pb.h"

#define RIAK_HOSTNAME_ENV_NAME  "RIAK_HOSTNAME"
#define RIAK_PORT_ENV_NAME	"RIAK_PORT"

typedef int(*func_t)(int argc, char *argv[]);

extern const char *riak_hostname;
extern const char *riak_port_str;

int do_addnode(int, char **);
int do_listkeys(int, char **);
int do_kvtest(int, char **);
int do_kvtest_sc(int, char **);
int do_asyncput(int, char **);
int do_asyncget(int, char **);
int do_asyncget_with_callback(int, char **);
int do_batch_check(int, char **);
int do_mvbucket(int, char **);
int do_getbucket(int, char **);
int do_setbucket(int, char **);
int do_kstest(int, char **);
int do_index(int, char **);
int do_gettime(int, char **);

int async_get_callback(const std::string *, const int, const std::string &);
int batch_check_callback(const std::string *, const int, const std::string &);

#endif /* __H_RIAK_TOOLBOX_H__ */
