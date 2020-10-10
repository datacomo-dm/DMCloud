#include <cr_class/cr_class.h>
#include <evpaxos.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <netdb.h>
#include "cluster_fm.h"

//////////////////////

struct client_value
{
    struct timeval t;
    size_t size;
    char value[];
};

//////////////////////

static void help_out(int err_code=EINVAL);
static void handle_sigint(int sig, short ev, void* arg);
static void deliver(unsigned iid, char* value, size_t size, void* arg);
static void start_replica(int id, const char* config);

//////////////////////

static void
help_out(int err_code)
{
    fprintf(stderr, "Usage: %s %s <cluster_str> <current_node_addr> <work_path>\n" , app_argv[0], app_argv[1]);
    exit(err_code);
}

static void
handle_sigint(int sig, short ev, void* arg)
{
    struct event_base* base = (struct event_base*)arg;
    printf("Caught signal %d\n", sig);
    event_base_loopexit(base, NULL);
}

static void
deliver(unsigned iid, char* value, size_t size, void* arg)
{
//    struct client_value* val = (struct client_value*)value;
//    printf("%ld.%06d [%.16s] %ld bytes\n", (long)val->t.tv_sec, (int)val->t.tv_usec,
//        val->value, (long)val->size);
}

static void
start_replica(int id, const char* config)
{
    struct event* sig;
    struct event_base* base;
    struct evpaxos_replica* replica = NULL;

    base = event_base_new();
    replica = evpaxos_replica_init(id, config, deliver, NULL, base);

    unlink(config);

    if (replica == NULL) {
        printf("Could not start the replica!\n");
        exit(1);
    }

    sig = evsignal_new(base, SIGINT, handle_sigint, base);
    evsignal_add(sig, NULL);
    event_base_dispatch(base);

    event_free(sig);
    evpaxos_replica_free(replica);
    event_base_free(base);
}

static size_t
make_config(const char *config)
{
    std::string config_text;
    size_t cur_id = 0;
    unsigned int node_id = 0;

    config_text = "## LibPaxos configuration file\n";

    for (size_t i=0; i<app_cluster_info.size(); i++) {
        const CFM_Basic_NS::striping_info_t &cur_striping = app_cluster_info[i];
        for (size_t j=0; j<cur_striping.size(); j++) {
            if ((app_current_node.first == (int)i) && (app_current_node.second == (int)j)) {
                cur_id = node_id;
            }
            CR_Class_NS::nn_transport_t tmp_transport;
            std::string tmp_hostname;
            std::string tmp_srvname;
            CR_Class_NS::nn_addr_split(cur_striping[j].c_str(), tmp_transport, tmp_hostname, tmp_srvname);
            int tmp_port = atoi(tmp_srvname.c_str());
            if (tmp_port <= 0) {
                struct servent *sp = getservbyname(tmp_srvname.c_str(), "tcp");
                tmp_port = sp->s_port;
            }
            config_text.append(CR_Class_NS::str_printf("replica %u %s %d\n", node_id,
              tmp_hostname.c_str(), tmp_port));
            node_id++;
        }
    }

    config_text.append("verbosity error\n");
    config_text.append("proposer-timeout 10\n");
    config_text.append("proposer-preexec-window 1024\n");
    config_text.append("storage-backend lmdb\n");
    config_text.append("lmdb-mapsize 128mb\n");
    config_text.append(CR_Class_NS::str_printf("lmdb-env-path %s/replica\n", app_work_path_arr[0].c_str()));

    config_text.append(CR_Class_NS::str_printf("#current node id is %u\n", (unsigned)cur_id));

    CR_Class_NS::save_string(config, config_text);

    DPRINTF("\n%s\n", config_text.c_str());

    return cur_id;
}

int
do_replica(int argc, char *argv[])
{
    if (argc < 5)
        help_out();

    app_cluster_str = argv[2];

    std::string current_node_addr = argv[3];

    app_work_path_arr.push_back(std::string(argv[4]));

    int fret = CR_Class_NS::nn_addr_split(current_node_addr.c_str(),
      app_cur_transport, app_cur_hostname, app_cur_srvname);

    if (fret || (app_cur_transport != CR_Class_NS::NN_TCP) || (app_cur_srvname.size() == 0)) {
        DPRINTF("Current node \"%s\" is invalid\n", current_node_addr.c_str());
        return EINVAL;
    }

    app_current_node = CFM_Basic_NS::parse_cluster_info(
      app_cluster_str, current_node_addr, app_cluster_info);

    if (app_current_node.first < 0) {
        DPRINTF("Cluster info parse failed[%s]\n", CR_Class_NS::strerrno(app_current_node.second));
        return app_current_node.second;
    }

    std::string config_file = "/tmp/CFM-";

    config_file += CR_Class_NS::randstr();

    config_file += ".conf";

    int id = make_config(config_file.c_str());

    start_replica(id, config_file.c_str());

    return 0;
}
