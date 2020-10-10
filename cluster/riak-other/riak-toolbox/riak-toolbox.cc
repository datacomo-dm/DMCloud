#include "riak-toolbox.h"

const char *riak_hostname = NULL;
const char *riak_port_str = NULL;

int
main(int argc, char *argv[])
{
    int fret = EINVAL;
    double enter_time, exit_time;

    enter_time = CR_Class_NS::clock_gettime();

    DPRINTF("Enter timestamp:%f\n", enter_time);

    std::map<std::string,func_t> function_map;
    std::map<std::string,func_t>::iterator func_map_it;

    function_map["listkeys"]  = do_listkeys;
    function_map["addnode"]   = do_addnode;
    function_map["kvtest"]    = do_kvtest;
    function_map["kvtest-sc"] = do_kvtest_sc;
    function_map["asyncput"]  = do_asyncput;
    function_map["asyncget"]  = do_asyncget;
    function_map["asyncgetc"] = do_asyncget_with_callback;
    function_map["batch-check"]   = do_batch_check;
    function_map["mvbucket"]  = do_mvbucket;
    function_map["getbucket"] = do_getbucket;
    function_map["setbucket"] = do_setbucket;
    function_map["keyspace"]  = do_kstest;
    function_map["index"]     = do_index;
    function_map["gettime"]   = do_gettime;

    std::string default_riak_node = "127.0.0.1:8087";

    riak_hostname = getenv(RIAK_HOSTNAME_ENV_NAME);
    riak_port_str = getenv(RIAK_PORT_ENV_NAME);

    if (!riak_hostname)
        riak_hostname = default_riak_node.c_str();

    if (argc > 1) {
        if (function_map.find(argv[1]) != function_map.end()) {
            func_t one_func = function_map[std::string(argv[1])];
            fret = one_func(argc, argv);
        }
    } else {
        fprintf(stderr, "Usage:");

        for (func_map_it=function_map.begin(); func_map_it!=function_map.end(); func_map_it++) {
            fprintf(stderr, "\t%s %s [...]\n", argv[0], func_map_it->first.c_str());
        }
    }

    exit_time = CR_Class_NS::clock_gettime();

    DPRINTF("Exit timestamp:%f, Usage seconds:%f\n", exit_time, (exit_time - enter_time));

    return fret;
}
