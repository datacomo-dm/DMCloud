#include "riak-toolbox.h"
#include <riakdrv/riak_common.h>

int
do_gettime(int argc, char *argv[])
{
    double self_time;
    double node_time;

    RiakConnect conn("");

    if (conn.Connect(riak_hostname, riak_port_str) != 0) {
      fprintf(stderr, "Connect to node %s:%s failed\n", riak_hostname, riak_port_str);
      return 1;
    }

    for (int i=0; i<10; i++) {
      self_time = CR_Class_NS::clock_gettime();
      node_time = conn.GetTimeOfDay();
      printf("SELF => %f, NODE => %f\n", self_time, node_time);
      sleep(1);
    }

    return 0;
}

int
do_addnode(int argc, char *argv[])
{
    int fret;
    std::string riak_err_msg;

    if (argc < 5) {
      fprintf(stderr, "Usage: %s %s <node_name> <hostname> <pb_port>\n", argv[0], argv[1]);
      return 1;
    }

    RiakConnect _riak_conn("");
    RiakCluster _cluster_client(120, 1);

    if (riak_hostname) {
      if (riak_port_str) {
        if (_riak_conn.Connect(riak_hostname, riak_port_str) != 0){
          fprintf(stderr, "Connect to node %s:%s failed\n", riak_hostname, riak_port_str);
          return 1;
        }
      } else {
        char buf[MAXPATHLEN];
        char *cur_arg, *next_arg;

        bzero(buf, MAXPATHLEN);
        strncpy(buf, riak_hostname, MAXPATHLEN-1);

        next_arg = buf;
        while (next_arg) {
          cur_arg=strsep(&next_arg, ",");

          char *arg_srvname = cur_arg;
          char *arg_hostname = strsep(&arg_srvname, ":");

          if (arg_hostname && arg_srvname) {
            fret = _riak_conn.Connect(arg_hostname, arg_srvname);

            if (fret == 0) {
              break;
            }
          }
        }
      }
    }

    RpbBucketProps bucket_props;

    bucket_props.set_n_val(3);
    bucket_props.set_allow_mult(false);
    bucket_props.set_last_write_wins(true);
    bucket_props.set_backend("eleveldb_default");

    printf("Set bucket's backend=eleveldb_default, allow_mult=0, last_write_wins=1, n_val=3...");
    fret = _riak_conn.SetBucket(_cluster_client.get_clusterinfo_bucket(), bucket_props, &riak_err_msg);
    if (fret) {
      printf("failed, err=%d, msg=%s, riak_msg=%s\n", fret, CR_Class_NS::strerror(fret), riak_err_msg.c_str());
    } else {
      printf("OK\n");
    }

    std::string bucket;
    const char *_clusterinfo_tmp_p = getenv(RIAK_CLUSTERINFO_STORAGE_BUCKET_ENVNAME);
    if (_clusterinfo_tmp_p)
        bucket = _clusterinfo_tmp_p;
    else
        bucket = "__DEFAULT_CLUSTERINFO_BUCKET__";
    std::string key = argv[2];

    RpbContent node_content;

    node_content.set_value(CR_Class_NS::blank_string);
    RiakMisc::SetUserMeta(node_content, RIAKC_META_NAME_HOST_NAME, argv[3]);
    RiakMisc::SetUserMeta(node_content, RIAKC_META_NAME_SRV_NAME, argv[4]);
    RiakMisc::SetUserMeta(node_content, RIAK_META_NAME_SAVE_COMPRESS, RIAK_META_VALUE_FALSE);
    RiakMisc::SetUserMeta(node_content, RIAK_META_NAME_TRANSFER_COMPRESS, RIAK_META_VALUE_FALSE);

    RiakMisc::SetIndexes(node_content, RIAKC_INDEX_NAME_CLUSTER_NODES, key);

    fret = _riak_conn.PutEx(bucket, &key, NULL,
      RIAK_CONNECT_FLAG_PUT_DIRECT | RIAK_CONNECT_FLAG_RETURN_HEAD, &node_content,
      NULL, NULL, NULL);

    if (!fret) {
      printf("Node %s(%s:%s) add ok\n", argv[2], argv[3], argv[4]);
    } else {
      printf("Node %s(%s:%s) add failed, ret=%d, msg=%s\n", argv[2], argv[3], argv[4],
        fret, CR_Class_NS::strerror(fret));
    }

    return fret;
}
