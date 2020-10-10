#ifndef __H_CFM_TOOLBOX_H__
#define __H_CFM_TOOLBOX_H__

#include <cr_class/cr_class.h>

#define CFM_TB_EN_LOAD_SORTED_DATA_DIR		"LOAD_SORTED_DATA_DIR"

typedef int(*func_t)(int argc, char **argv);

int do_sltest(int, char **);
int do_slmerge(int, char **);
int do_slcancel(int, char **);
int do_slpkichk(int, char **);
int do_perftest(int, char **);
int do_flstest(int, char **);
int do_slpkichk2(int, char **);
int do_rowtest(int, char **);
int do_keysampletest(int, char **);
int do_lock(int, char **);
int do_unlock(int, char **);
int do_id(int, char **);

extern const char *string_arr[];

#endif /* __H_CFM_TOOLBOX_H__ */
