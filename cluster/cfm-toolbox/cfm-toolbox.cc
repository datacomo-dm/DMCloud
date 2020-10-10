#include "cfm-toolbox.h"
#include <stdio.h>
#include <map>

int
main(int argc, char *argv[])
{
  int fret = EINVAL;
  double enter_time, exit_time;

  enter_time = CR_Class_NS::clock_gettime();

  DPRINTFX(15, "Enter timestamp:%f\n", enter_time);

  std::map<std::string,func_t> function_map;
  std::map<std::string,func_t>::iterator func_map_it;

  function_map["sltest"]		= do_sltest;
  function_map["slmerge"]		= do_slmerge;
  function_map["slcancel"]		= do_slcancel;
  function_map["slpkichk"]		= do_slpkichk;
  function_map["perftest"]		= do_perftest;
  function_map["flstest"]		= do_flstest;
  function_map["slpkichk2"]		= do_slpkichk2;
  function_map["rowtest"]		= do_rowtest;
  function_map["keysampletest"]		= do_keysampletest;
  function_map["lock"]			= do_lock;
  function_map["unlock"]		= do_unlock;
  function_map["id"]			= do_id;

  if (argc >= 2) {
    if (function_map.find(argv[1]) != function_map.end()) {
      func_t one_func = function_map[std::string(argv[1])];
      fret = one_func(argc, argv);
      printf("%s\n", CR_Class_NS::strerrno(fret));
    } else {
      fret = ENOTSUP;
    }
  } else {
    fprintf(stderr, "Usage:\n");
    for (func_map_it=function_map.begin(); func_map_it!=function_map.end(); func_map_it++) {
      fprintf(stderr, "\t%s %s [...]\n", argv[0], func_map_it->first.c_str());
    }
  }

  exit_time = CR_Class_NS::clock_gettime();

  DPRINTFX(15, "Exit timestamp:%f, Usage seconds:%f\n", exit_time, (exit_time - enter_time));

  return fret;
}
