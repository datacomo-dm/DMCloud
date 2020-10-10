#include <cr_class/cr_quickhash.h>
#include "riak-toolbox.h"

#define __CLUSTER_PTR__		"CLUSTER_P"
#define __BUCKET__		"BUCKET"
#define __SRC_NODE_HOST__	"SRC_HOST"
#define __SRC_NODE_PORT__	"SRC_PORT"
#define __KEY_LIST_PTR__	"KEYLIST_P"
#define __KEY_LIST_LPOS__	"KEYLIST_LPOS"
#define __KEY_LIST_POS_STEP__	"KEYLIST_POS_STEP"
#define __CUR_PROC_NO__		"CUR_PROC_NO"
#define __TOTAL_PROC_COUNT__	"TOTAL_PROC_COUNT"

std::string _split_move(CR_Thread::handle_t);

int
do_getbucket(int argc, char *argv[])
{
    int fret;

    if (argc < 5) {
      fprintf(stderr, "Usage: %s getbucket <node_hostname> <node_port> <bucket>\n", argv[0]);
      return 1;
    }

    RiakConnect _riak_conn("", 86400, 10);

    fret = _riak_conn.Connect(std::string(argv[2]), std::string(argv[3]));

    if (fret) {
      fprintf(stderr, "Connect failed, err=%d, msg=%s\n", fret, CR_Class_NS::strerror(fret));
      return fret;
    }

    std::string bucket = argv[4];
    RpbBucketProps bucket_props;

    fret = _riak_conn.GetBucket(bucket, bucket_props);
    if (fret) {
      fprintf(stderr, "GetBucket, err=%d, msg=%s\n", fret, CR_Class_NS::strerror(fret));
      return fret;
    }

    printf("B[%s].props => %s\n", bucket.c_str(), bucket_props.DebugString().c_str());

    return 0;
}

int
do_setbucket(int argc, char *argv[])
{
    int fret;

    if (argc < 8) {
      fprintf(stderr, "Usage: %s setbucket <node_hostname> <node_port> <bucket> <n_val> <allow_mult> <last_write_wins> [backend]\n", argv[0]);
      return 1;
    }

    RiakConnect _riak_conn("", 86400, 10);

    fret = _riak_conn.Connect(std::string(argv[2]), std::string(argv[3]));

    if (fret) {
      fprintf(stderr, "Connect failed, err=%d, msg=%s\n", fret, CR_Class_NS::strerror(fret));
      return fret;
    }

    std::string bucket = argv[4];
    int n_val = atoi(argv[5]);
    bool allow_mult = false;
    bool last_write_wins = false;

    if (n_val < 1)
        n_val = 1;

    if (atoi(argv[6]) > 0)
        allow_mult = true;

    if (atoi(argv[7]) > 0)
        last_write_wins = true;

    if (allow_mult)
      last_write_wins = false;

    RpbBucketProps bucket_props;

    bucket_props.set_n_val(n_val);
    bucket_props.set_allow_mult(allow_mult);
    bucket_props.set_last_write_wins(last_write_wins);
    if (argc > 8)
      bucket_props.set_backend(argv[8]);

    fret = _riak_conn.SetBucket(bucket, bucket_props);
    if (fret) {
      fprintf(stderr, "SetBucket, err=%d, msg=%s\n", fret, CR_Class_NS::strerror(fret));
      return fret;
    }

    return 0;
}

int
do_listkeys(int argc, char *argv[])
{
    int fret;

    if (argc < 6) {
      fprintf(stderr, "Usage: %s listkeys <node_hostname> <node_port> <bucket> <output_filename>\n", argv[0]);
      return 1;
    }

    RiakConnect _riak_conn("", 86400, 10);

    fret = _riak_conn.Connect(std::string(argv[2]), std::string(argv[3]));

    if (fret) {
      fprintf(stderr, "Connect failed, err=%d, msg=%s\n", fret, CR_Class_NS::strerror(fret));
      return fret;
    }

    std::vector<std::string> key_list;

    fret = _riak_conn.ListKeys(std::string(argv[4]), key_list);

    if (fret) {
      fprintf(stderr, "ListKeys failed, err=%d, msg=%s\n", fret, CR_Class_NS::strerror(fret));
    }

    size_t list_size = key_list.size();
    if (list_size > 0) {
      FILE *fp = fopen(argv[5], "w");

      if (!fp) {
        fprintf(stderr, "Open file failed, err=%d, msg=%s\n", errno, CR_Class_NS::strerror(errno));
        return errno;
      }

      for (size_t i=0; i<list_size; i++) {
        fprintf(fp, "%s\n", key_list[i].c_str());
      }
    }

    return 0;
}

int
do_index(int argc, char *argv[])
{
    int fret;
    int is_key_only = 0;

    if (argc < 6) {
      fprintf(stderr, "Usage: %s %s <bucket> <index> <range_min> <range_max> [is_key_only]\n", argv[0], argv[1]);
      return 1;
    }

    RiakConnect _riak_conn("", 86400, 10);

    fret = _riak_conn.Connect(riak_hostname, riak_port_str);

    if (fret) {
      fprintf(stderr, "Connect failed, err=%d, msg=%s\n", fret, CR_Class_NS::strerror(fret));
      return fret;
    }

    if (argc > 6) {
      is_key_only = atoi(argv[6]);
    }

    std::vector<std::string> key_list;
    std::map<std::string,std::string> kv_map;
    std::map<std::string,std::string>::iterator it;

    if (is_key_only)
        fret = _riak_conn.Index(argv[2], argv[3], argv[4], argv[5], &key_list, NULL);
    else
        fret = _riak_conn.Index(argv[2], argv[3], argv[4], argv[5], &key_list, &kv_map);

    if (fret) {
      fprintf(stderr, "Index failed, err=%d, msg=%s\n", fret, CR_Class_NS::strerror(fret));
    }

    printf("\nKey Array:\n");
    size_t list_size = key_list.size();
    if (list_size > 0) {
      for (size_t i=0; i<list_size; i++) {
        printf("%s\n", key_list[i].c_str());
      }
    }

    printf("\nKey-Value Map:\n");
    for (it=kv_map.begin(); it!=kv_map.end(); it++) {
      printf("Key => %s,\tIndexValue => %s\n", it->second.c_str(), it->first.c_str());
    }

    return 0;
}

int
do_mvbucket(int argc, char *argv[])
{
  int fret = 0;

  if (argc < 8) {
    fprintf(stderr, "Usage: %s mvbucket <bucket> <src_node_host> <src_node_port> <dst_cluster_host:dst_cluster_port[,dst_cluster_host:dst_cluster_port,...]> <cur_proc_no> <total_proc_count> [conn_num] [max_connects_per_node]\n", argv[0]);
    return EINVAL;
  }

  const std::string riak_bucket = argv[2];
  const std::string src_node_host = argv[3];
  const std::string src_node_port = argv[4];
  const char *dst_cluster_info = argv[5];

  int conn_num = 4;
  int max_connects_per_node = 1;
  int cluster_nodes;
  unsigned cur_proc_no;
  unsigned total_proc_count;

  cur_proc_no = atoi(argv[6]);
  if (cur_proc_no > 1023)
    cur_proc_no = 1023;

  total_proc_count = atoi(argv[6]);
  if (total_proc_count > 1024)
    total_proc_count = 1024;

  if (argc > 8) {
    conn_num = atoi(argv[8]);
    if (conn_num < 1)
      conn_num = 1;
    if (conn_num > 1024)
      conn_num = 1024;
  }

  if (argc > 9) {
    max_connects_per_node = atoi(argv[9]);
    if (max_connects_per_node < 1)
      max_connects_per_node = 1;
    if (max_connects_per_node > 20)
      max_connects_per_node = 20;
  }

  RiakCluster riak_cluster(120, max_connects_per_node);

  cluster_nodes = riak_cluster.Rebuild(dst_cluster_info);

  if (cluster_nodes <= 0) {
    DPRINTF("Rebuild failed\n");
    return (0-cluster_nodes);
  }

  RiakConnect src_conn(src_node_host, 3600);

  fret = src_conn.Connect(src_node_host, src_node_port);

  if (fret) {
    DPRINTF("Connect to src node failed, ret=%d, msg=%s\n", fret, CR_Class_NS::strerror(fret));
    return fret;
  }

  std::vector<std::string> key_list;

  DPRINTF("Start ListKeys from src node\n");

  int loop_count = 0;
  while (1) {
    fret = src_conn.ListKeys(riak_bucket, key_list);
    if (!fret && key_list.size()) {
      break;
    }
    DPRINTFX(20, "ListKeys failed, ret=%d, msg=%s\n", fret, CR_Class_NS::strerror(fret));
    loop_count++;
    if (loop_count > max_connects_per_node) {
      DPRINTF("ListKeys failed final. ret=%d, msg=%s\n", fret, CR_Class_NS::strerror(fret));
      return fret;
    }
  }

  size_t key_list_size = key_list.size();

  printf("ListKeys OK, total %ld keys in list.\n", key_list_size);

  CR_Thread::TDB_t tdb;
  std::list<CR_Thread::handle_t> t_list;
  size_t _step = key_list_size / conn_num;

  tdb.thread_func = _split_move;
  tdb.thread_flags = CR_THREAD_JOINABLE;
  tdb.default_tags[__CLUSTER_PTR__] = CR_Class_NS::ptr2str(&riak_cluster);
  tdb.default_tags[__BUCKET__] = riak_bucket;
  tdb.default_tags[__SRC_NODE_HOST__] = src_node_host;
  tdb.default_tags[__SRC_NODE_PORT__] = src_node_port;
  tdb.default_tags[__KEY_LIST_PTR__] = CR_Class_NS::ptr2str(&key_list);
  tdb.default_tags[__KEY_LIST_POS_STEP__] = CR_Class_NS::u642str(_step);
  tdb.default_tags[__CUR_PROC_NO__] = CR_Class_NS::u642str(cur_proc_no);
  tdb.default_tags[__TOTAL_PROC_COUNT__] = CR_Class_NS::u642str(total_proc_count);

  for (size_t i=0; i<key_list_size; i+=_step) {
    tdb.default_tags[__KEY_LIST_LPOS__] = CR_Class_NS::u642str(i);
    CR_Thread::handle_t t_handle = CR_Thread::talloc(tdb);
    CR_Thread::start(t_handle);
    t_list.push_back(t_handle);
  }

  while (!t_list.empty()) {
    std::string t_result = "";
    CR_Thread::handle_t t_handle = t_list.front();
    fret = CR_Thread::join(t_handle, &t_result);
    if (fret)
      break;
    if (t_result != "OK") {
      DPRINTFX(0, "Thread [%08X] failed with result:[%s]\n", (int)CR_Thread::gettid(t_handle), t_result.c_str());
    }
  }

  return fret;
}

std::string
_split_move(CR_Thread::handle_t handle)
{
  int fret;

  RiakCluster *dst_cluster_p = (RiakCluster *)CR_Class_NS::str2ptr(CR_Thread::gettag(handle, __CLUSTER_PTR__));
  const std::string riak_bucket = CR_Thread::gettag(handle, __BUCKET__);
  const std::string src_node_host = CR_Thread::gettag(handle, __SRC_NODE_HOST__);
  const std::string src_node_port = CR_Thread::gettag(handle, __SRC_NODE_PORT__);
  const std::vector<std::string> *key_list_p = (const std::vector<std::string> *)
  	CR_Class_NS::str2ptr(CR_Thread::gettag(handle, __KEY_LIST_PTR__));
  const size_t key_lpos = CR_Class_NS::str2u64(CR_Thread::gettag(handle, __KEY_LIST_LPOS__));
  const size_t key_pos_step = CR_Class_NS::str2u64(CR_Thread::gettag(handle, __KEY_LIST_POS_STEP__));
  const unsigned cur_proc_no = CR_Class_NS::str2u64(CR_Thread::gettag(handle, __CUR_PROC_NO__));
  const unsigned total_proc_count = CR_Class_NS::str2u64(CR_Thread::gettag(handle, __TOTAL_PROC_COUNT__));
  size_t key_rpos = key_lpos + key_pos_step - 1;
  RiakConnect src_connect(src_node_host, 60);

  std::string riak_key, riak_param, riak_param_verify;
  std::string src_value, src_md5, dst_md5;
  size_t riak_stored_size;

  if (key_rpos >= key_list_p->size()) {
    key_rpos = key_list_p->size() - 1;
  }

  DPRINTF("Enter, key_list_lpos=%lu, key_list_rpos=%lu\n", key_lpos, key_rpos);

  if (src_connect.Connect(src_node_host, src_node_port) != 0) {
    return std::string("Connect to" + src_node_host + ":" + src_node_port + " failed");
  }

  for (size_t pos=key_lpos; pos<=key_rpos; pos++) {
    riak_key = (*key_list_p)[pos];

    if ((CR_Class_NS::crc32(riak_key) % total_proc_count) != cur_proc_no) {
      DPRINTFX(30, "Key[%s] is not my task,skip.\n", riak_key.c_str());
      continue;
    }

    riak_param = "DIRECT_BUCKET=" + riak_bucket + "&DIRECT_KEY=" + riak_key;
    riak_param_verify = riak_param + "&VERIFY_TYPE=MD5";

    // read key from src
    for (int retry=0; retry<3; retry++) {
      fret = src_connect.Get(riak_bucket, riak_key, src_value, &riak_param);
      if (!fret)
        break;
    }
    if (fret) {
      DPRINTF("Read B[%s],K[%s] from %s:%s failed. err=%d, msg=%s\n",
      	riak_bucket.c_str(), riak_key.c_str(), src_node_host.c_str(), src_node_port.c_str(),
      	fret, CR_Class_NS::strerror(fret));
    } else {
      DPRINTFX(20, "Read B[%s],K[%s] from %s:%s OK, V.size()=%lu, V[0]='%c'\n",
      	riak_bucket.c_str(), riak_key.c_str(), src_node_host.c_str(), src_node_port.c_str(),
      	src_value.size(), src_value.c_str()[0]);

      src_md5 = CR_QuickHash::md5(src_value);

      // read md5(key) from dst without backup cluster
      fret = dst_cluster_p->Get(riak_bucket, riak_key, dst_md5, &riak_param_verify);
      if (!fret) {
        if (src_md5 != dst_md5) {
          DPRINTF("B[%s],K[%s] exist and diff, src_md5==%s, dst_md5==%s, skip.\n",
          	riak_bucket.c_str(), riak_key.c_str(), src_md5.c_str(), dst_md5.c_str());
          continue;
        } else {
          fret = src_connect.Del(riak_bucket, riak_key, NULL);
          if (!fret) {
            DPRINTFX(20, "B[%s],K[%s] exist and same, delete in src OK.\n",
            	riak_bucket.c_str(), riak_key.c_str());
            continue;
          } else {
            DPRINTF("B[%s],K[%s] exist and same, but delete in src failed. err=%d, msg=%s.\n",
            	riak_bucket.c_str(), riak_key.c_str(), fret, CR_Class_NS::strerror(fret));
            continue;
          }
        }
      }

      // save key to dst
      fret = dst_cluster_p->Put(riak_bucket, riak_key, src_value, &riak_param, &riak_stored_size);
      if (!fret) {
        DPRINTFX(20, "Save B[%s],K[%s] to cluster first OK. V.stored_size=%lu\n",
        	riak_bucket.c_str(), riak_key.c_str(), riak_stored_size);
      } else {
        DPRINTF("Save B[%s],K[%s] to cluster failed. err=%d, msg=%s\n",
        	riak_bucket.c_str(), riak_key.c_str(), fret, CR_Class_NS::strerror(fret));
        continue;
      }

      size_t src_value_size = src_value.size();

      // read key from dst
      fret = dst_cluster_p->Get(riak_bucket, riak_key, dst_md5, &riak_param_verify);
      if (!fret) {
        DPRINTFX(20, "Read B[%s],K[%s] from cluster OK.\n",
        	riak_bucket.c_str(), riak_key.c_str());
      } else {
        DPRINTF("Read B[%s],K[%s] from cluster failed. err=%d, msg=%s\n",
        	riak_bucket.c_str(), riak_key.c_str(), fret, CR_Class_NS::strerror(fret));
        continue;
      }

      // verify it
      if ((dst_md5.size() != 32) || (dst_md5 != src_md5)) {
        DPRINTF("Verify B[%s],K[%s] from cluster failed. src_size=%lu, src_md5=%s, dst_size=%lu\n",
        	riak_bucket.c_str(), riak_key.c_str(), src_value_size, src_md5.c_str(), dst_md5.size());
        continue;
      } else {
        DPRINTFX(20, "Verify B[%s],K[%s] OK\n", riak_bucket.c_str(), riak_key.c_str());
      }

      // del key in src
      fret = src_connect.Del(riak_bucket, riak_key, NULL);
      if (fret) {
        DPRINTF("Delete B[%s],K[%s] at %s:%s failed. err=%d, msg=%s\n",
        	riak_bucket.c_str(), riak_key.c_str(), src_node_host.c_str(), src_node_port.c_str(),
        	fret, CR_Class_NS::strerror(fret));
        continue;
      }
      DPRINTFX(5, "B[%s],K[%s] move ok\n", riak_bucket.c_str(), riak_key.c_str());
    }
  }

  DPRINTF("Exit\n");

  return std::string("OK");
}
