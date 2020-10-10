#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>

#include <cr_class/cr_class.h>
#include <riakdrv/riakcluster.h>
#include <riakdrv/msgRiakCluster.pb.h>
#include "msgIBRiak.pb.h"
#define RIAK_HOSTNAME_ENV_NAME	"RIAK_HOSTNAME"
#define RIAK_PORT_ENV_NAME	"RIAK_PORT"

int
main(int argc, char *argv[])
{
  if (argc == 1) {
    fprintf(stderr, "Usage: %s <addnode | ringtest | kvtest> [...]\n", argv[0]);
    exit(1);
  }

  int fret;

  char *riak_hostname;
  char *riak_port_str;
  int riak_port;

  riak_hostname = getenv(RIAK_HOSTNAME_ENV_NAME);
  riak_port_str = getenv(RIAK_PORT_ENV_NAME);

  if (!riak_hostname || !riak_port_str) {
    fprintf(stderr, "Must set $%s and $%s\n", RIAK_HOSTNAME_ENV_NAME, RIAK_PORT_ENV_NAME);
    exit(1);
  }

  riak_port = atoi(riak_port_str);

  RiakConnect _riak_conn;
  if (_riak_conn.Connect(riak_hostname, riak_port) != 0){
    fprintf(stderr, "Connect to node %s:%d failed\n", riak_hostname, riak_port);
    exit(1);
  }

  if (strcmp(argv[1], "addnode") == 0) {
    if (argc < 5) {
      fprintf(stderr, "Usage: %s addnode <node_name> <hostname> <pb_port>\n", argv[0]);
      exit(1);
    }

    msgRiakNodeInfo riak_node_info;
    std::string riak_node_info_str;

    riak_node_info.set_node_name(argv[2]);
    riak_node_info.set_inet_addr(argv[3]);
    riak_node_info.set_inet_port_pb(atoi(argv[4]));

    riak_node_info.SerializeToString(&riak_node_info_str);
    if (_riak_conn.PutSimple(RIAKCLUSTER_RING_NODES_STORAGE_BUCKET, argv[2], "", riak_node_info_str) != 0) {
      fprintf(stderr, "Put node info failed\n");
      exit(1);
    }

    sleep(1);

    if (_riak_conn.GetOne(RIAKCLUSTER_RING_NODES_STORAGE_BUCKET, argv[2], "", riak_node_info_str) != 0) {
      fprintf(stderr, "Get node info failed\n");
      exit(1);
    }

    riak_node_info.Clear();

    if (!riak_node_info.ParseFromString(riak_node_info_str)) {
      fprintf(stderr, "Parse node info failed\n");
      exit(1);
    }

    printf("Node info :\n\tnode_name=%s\n\thostname=%s\n\tport=%d\n",
    	riak_node_info.node_name().c_str(), riak_node_info.inet_addr().c_str(),
    	riak_node_info.inet_port_pb());
  }

  if (strcmp(argv[1], "ringtest") == 0) {
    if (argc < 6) {
      fprintf(stderr, "Usage: %s ringtest <0:dont_keep_result | other_number:keep_result> <bucket> <key> <value>\n", argv[0]);
      exit(1);
    }

    std::string riak_value;
    RiakCluster _cluster_client(30);
    int nodes;
    bool keep_result;

    keep_result = atoi(argv[2]);
    nodes = _cluster_client.Rebuild(riak_hostname, riak_port);

    if (nodes > 0)
      printf("Success connected to %d nodes\n", nodes);
    else {
      fprintf(stderr, "Connect to riak ring failed, err = %d, msg = %s\n", (0-nodes), strerror(0-nodes));
      exit(0-nodes);
    }

    for (int i=5; i<argc; i++) {
      printf("Put value = %s\n", argv[i]);
      _cluster_client.Put(argv[3], argv[4], argv[i]);
      riak_value.clear();
      printf("Get value ... ");
      fret = _cluster_client.Get(argv[3], argv[4], riak_value, NULL, keep_result, 100000000);
      if (fret == 0)
        printf("Value = %s\n", riak_value.c_str());
      else
        printf("ERROR, return code = %d\n", fret);
    }
  }

  if (strcmp(argv[1], "kvtest") == 0) {
    if (argc < 5) {
      fprintf(stderr, "Usage: %s kvtest <PUT | GET | DEL> <bucket> <key> [value]\n", argv[0]);
      exit(1);
    }

    std::string riak_value;
    RiakCluster _cluster_client(30);
    int nodes;
    bool keep_result;

    keep_result = atoi(argv[2]);
    nodes = _cluster_client.Rebuild(riak_hostname, riak_port);

    if (nodes > 0)
      printf("Success connected to %d nodes\n", nodes);
    else {
      fprintf(stderr, "Connect to riak ring failed, err = %d, msg = %s\n", (0-nodes), strerror(0-nodes));
      exit(0-nodes);
    }

    if (strcmp(argv[2], "PUT") == 0) {
      if (argc < 6) {
        fprintf(stderr, "PUT command NEED value\n");
        exit(1);
      }
      _cluster_client.Put(argv[3], argv[4], argv[5]);
      fret = _cluster_client.Get(argv[3], argv[4], riak_value, NULL, false, 100000000);
      if ((fret == 0) && (riak_value == argv[5])) {
        printf("PUT B = \"%s\", K = \"%s\", V= \"%s\" successed.\n", argv[3], argv[4], argv[5]);
      } else {
        fprintf(stderr, "PUT B = \"%s\", K = \"%s\", V= \"%s\" failed.\n", argv[3], argv[4], argv[5]);
        exit(1);
      }
    } else if (strcmp(argv[2], "GET") == 0) {
      fret = _cluster_client.Get(argv[3], argv[4], riak_value, NULL, false, 100000000);

      if (fret == 0) {
      	msgRCAttr_packS msg_pack;
      	msg_pack.ParseFromString(riak_value);
        printf("GET B = \"%s\", K = \"%s\" successed, V is %d bytes,no_obj:%d,no_nulls:%d\n", argv[3], argv[4], (int)riak_value.size(),msg_pack.no_obj(),msg_pack.no_nulls());
        //printf("GET B = \"%s\", K = \"%s\" successed, V is \"%s\"\n", argv[3], argv[4], riak_value.c_str());
      } else {
        printf("GET B = \"%s\", K = \"%s\" failed\n", argv[3], argv[4]);
        exit(1);
      }
    } else if (strcmp(argv[2], "DEL") == 0) {
      _cluster_client.Del(argv[3], argv[4]);
      fret = _cluster_client.Get(argv[3], argv[4], riak_value, NULL, false, 100000000);
      if (riak_value == "") {
        printf("DEL B = \"%s\", K = \"%s\" OK\n", argv[3], argv[4]);
      } else {
        fprintf(stderr, "DEL B = \"%s\", K = \"%s\" Failed\n", argv[3], argv[4]);
      }
    } else {
      fprintf(stderr, "Unknown command %s\n", argv[2]);
      exit(1);
    }
  }

  return 0;
}
