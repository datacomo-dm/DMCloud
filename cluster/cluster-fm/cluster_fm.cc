#include "cluster_fm.h"
#include <stdio.h>
#include <map>

int app_argc;
char **app_argv;
std::string app_job_id;
int64_t app_job_slave_level;
std::string app_cwd;
std::string app_lib_dir_path;
std::vector<std::string> app_work_path_arr;
CR_TreeStorage app_treestorage;
CR_SimpleServer *app_listen_server_p = NULL;
void *app_job_handle = NULL;
std::string app_cluster_str;
CFM_Basic_NS::cluster_info_t app_cluster_info;
std::pair<int,int> app_current_node;
CR_Class_NS::nn_transport_t app_cur_transport;
std::string app_cur_hostname;
std::string app_cur_srvname;
CR_BigSet<int64_t> app_data_rowid_set;
bool app_lib_static_work;
std::mutex app_lib_fm_mtx;
CFMLibInfo *app_lib_info_p = NULL;

int
main(int argc, char *argv[])
{
  int fret = 0;

  app_argc = argc;
  app_argv = argv;
  char *app_cwd_p = ::get_current_dir_name();
  app_cwd = app_cwd_p;
  ::free(app_cwd_p);

  srand(time(NULL));

  std::map<std::string,func_t> function_map;
  std::map<std::string,func_t>::iterator func_map_it;

  function_map["cfm"]		= do_cfm;
  function_map["lib"]		= do_lib;
  function_map["slib"]		= do_lib;
  function_map["replica"]	= do_replica;

  if ((argc < 2) || (function_map.find(argv[1]) == function_map.end())) {
    fprintf(stderr, "Usage:\n");
    for (func_map_it=function_map.begin(); func_map_it!=function_map.end(); func_map_it++) {
      fprintf(stderr, "\t%s %s [...]\n", argv[0], func_map_it->first.c_str());
    }
  } else {
    func_t one_func = function_map[std::string(argv[1])];
    fret = one_func(argc, argv);
    printf("%s\n", CR_Class_NS::strerrno(fret));
  }

  return fret;
}
