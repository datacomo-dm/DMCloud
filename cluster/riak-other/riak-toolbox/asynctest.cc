#include "riak-toolbox.h"

int
do_asyncput(int argc, char *argv[])
{
    int fret;

    if (argc < 5) {
      fprintf(stderr, "Usage: %s asyncput <bucket> <value_filename> <value_loop> [conn_num] [max_connects_per_node]\n", argv[0]);
      return (1);
    }

    std::string riak_bucket;
    std::string riak_value, riak_value_get;
    std::string riak_key;
    std::string errmsg;
    char key_buf[32];
    int nodes;
    int v_loop = 1;
    int conn_num = 4;
    void *async_handle;
    std::string value_filename;
    ssize_t total_orig_size = 0;
    int max_connects_per_node = 1;

    if (argc >= 7) {
      max_connects_per_node = atoi(argv[6]);
      if (max_connects_per_node < 1)
        max_connects_per_node = 1;
      if (max_connects_per_node > 16)
        max_connects_per_node = 16;
    }

    RiakCluster _cluster_client(120, max_connects_per_node);

    riak_bucket = argv[2];
    value_filename = argv[3];
    v_loop = atoi(argv[4]);
    if (argc > 5)
      conn_num = atoi(argv[5]);

    if (conn_num < 1)
      conn_num = 1;

    if (conn_num > 1024)
      conn_num = 1024;

    fret = CR_Class_NS::load_string(value_filename.c_str(), riak_value);
    if (fret != 0) {
      fprintf(stderr, "open file %s failed, err=%d,msg=%s\n", value_filename.c_str(), fret, CR_Class_NS::strerror(fret));
      return (fret);
    }

    nodes = _cluster_client.Rebuild(riak_hostname, riak_port_str);

    if (nodes > 0)
      printf("Success connected to %d nodes\n", nodes);
    else {
      fprintf(stderr, "Connect to riak ring failed, err = %d, msg = %s\n", (0-nodes), CR_Class_NS::strerror(0-nodes));
      return (0-nodes);
    }

    async_handle = _cluster_client.AsyncInit(conn_num, 512);
    if (!async_handle) {
      fprintf(stderr, "Async init failed\n");
      return (-1);
    }

    for (int i=0; i<v_loop; i++) {
      snprintf(key_buf, sizeof(key_buf), "%08d", i);
      riak_key = key_buf;
      fret = _cluster_client.AsyncPut(async_handle, riak_bucket, riak_key, riak_value);
      if (fret == 0) {
        total_orig_size += riak_value.size();
        printf("AsyncPut(%s) OK\n", key_buf);
      } else {
        fprintf(stderr, "AsyncPut(%s) FAILED,ret=%d,msg=%s\n", key_buf, fret, CR_Class_NS::strerror(fret));
      }
    }

    printf("AsyncWait Start\n");
    ssize_t total_save_size = _cluster_client.AsyncWait(async_handle, errmsg, 900);
    if (total_save_size >= 0) {
      printf("Async write total stored size is %ld, compress rate is %f%%\n", total_save_size,
        ((double)total_save_size / (double)total_orig_size * 100.0));
    } else {
      fret = total_save_size;
      fprintf(stderr, "Async wait failed at [%s], ret = %d, msg = %s\n", errmsg.c_str(), 0-fret, CR_Class_NS::strerror(0-fret));
      return (fret);
    }
    return 0;
}

int
do_asyncget(int argc, char *argv[])
{
    int fret;

    if (argc < 5) {
      fprintf(stderr, "Usage: %s asyncget <bucket> <value_list_file> <param> [max_saved_packs] [conn_num] [max_connects_per_node]\n", argv[0]);
      fprintf(stderr, "\tparam format: <S:<like_pattern> | N:<between_left>,<between_right>[,<local_min>] | <SR: | NR:><range_left>,<range_right>>\n");
      return (1);
    }

    std::string riak_bucket;
    std::string riak_value;
    std::string riak_param;
    std::string errmsg;

    std::string param_filename;
    char key_buf[128];
    int nodes;
    int max_saved_packs = 100;
    int conn_num = 4;
    void *async_handle;
    int max_connects_per_node = 1;

    std::vector<std::string> riak_key_list;

    if (strlen(argv[4]) > 2) {
      char like_pattern[256];
      int bet_l=0, bet_r=0, local_min=0;
      int range_l=0, range_r=65535;
      bool param_ok = false;

      bzero(like_pattern, sizeof(like_pattern));

      IBRiakDataPackQueryParam ibqp_tmp;
      IBRiakDataPackQueryParam::RowRange *one_rowrange = ibqp_tmp.add_row_range();
      one_rowrange->set_pos_begin(0);
      one_rowrange->set_pos_end(65535);

      if (sscanf(argv[4], "S:%s", like_pattern) > 0) {
        ibqp_tmp.set_pack_type(IBRiakDataPackQueryParam::PACK_STRING);
        ibqp_tmp.mutable_expression()->set_op_type(IBRiakDataPackQueryParam::EOP_LK);
        ibqp_tmp.mutable_expression()->add_value_list_str(std::string(like_pattern));
        param_ok = true;
      } else if (sscanf(argv[4], "N:%d,%d,%d", &bet_l, &bet_r, &local_min) > 0) {
        ibqp_tmp.set_pack_local_min_num(local_min);
        ibqp_tmp.set_pack_type(IBRiakDataPackQueryParam::PACK_NUM_SIGNED);
        ibqp_tmp.mutable_expression()->set_op_type(IBRiakDataPackQueryParam::EOP_BT);
        ibqp_tmp.mutable_expression()->add_value_list_num(bet_l);
        ibqp_tmp.mutable_expression()->add_value_list_num(bet_r);
        param_ok = true;
      } else if (sscanf(argv[4], "SR:%d,%d", &range_l, &range_r) > 0) {
        ibqp_tmp.set_pack_type(IBRiakDataPackQueryParam::PACK_STRING);
        one_rowrange->set_pos_begin(range_l);
        one_rowrange->set_pos_end(range_r);
        param_ok = true;
      } else if (sscanf(argv[4], "NR:%d,%d", &range_l, &range_r) > 0) {
        ibqp_tmp.set_pack_type(IBRiakDataPackQueryParam::PACK_NUM_SIGNED);
        one_rowrange->set_pos_begin(range_l);
        one_rowrange->set_pos_end(range_r);
        param_ok = true;
      }

      if (param_ok)
        ibqp_tmp.SerializeToString(&riak_param);
      else
        riak_param = argv[4];
    }

    if (argc > 5)
      max_saved_packs = atoi(argv[5]);

    if (max_saved_packs < 1)
      max_saved_packs = 1;

    if (max_saved_packs > 100000)
      max_saved_packs = 100000;

    if (argc >= 8) {
      max_connects_per_node = atoi(argv[7]);
      if (max_connects_per_node < 1)
        max_connects_per_node = 1;
      if (max_connects_per_node > 16)
        max_connects_per_node = 16;
    }

    RiakCluster _cluster_client(120, max_connects_per_node);

    riak_bucket = argv[2];
    param_filename = argv[4];
    if (argc > 6)
      conn_num = atoi(argv[6]);

    if (conn_num < 1)
      conn_num = 1;

    if (conn_num > 1024)
      conn_num = 1024;

    FILE *klist = fopen(argv[3], "r");
    if (!klist) {
      fprintf(stderr, "open klist file %s failed, err=%d,msg=%s\n", argv[3], errno, CR_Class_NS::strerror(errno));
      return (errno);
    }

    nodes = _cluster_client.Rebuild(riak_hostname, riak_port_str);

    if (nodes > 0)
      printf("Success connected to %d nodes\n", nodes);
    else {
      fprintf(stderr, "Connect to riak ring failed, err = %d, msg = %s\n", (0-nodes), CR_Class_NS::strerror(0-nodes));
      return (0-nodes);
    }

    async_handle = _cluster_client.AsyncInit(conn_num, 1024);
    if (!async_handle) {
      fprintf(stderr, "Async init failed\n");
      return (-1);
    }

    int total_proc_lines = 0;
    size_t total_value_size = 0;

    DPRINTF("AsyncGet start.\n");

    int cur_read_lines = 0;

    while (fgets(key_buf, sizeof(key_buf), klist)) {
      riak_key_list.push_back(std::string(key_buf, strlen(key_buf) - 1));

      cur_read_lines++;

      if (cur_read_lines == max_saved_packs) {
        for (int i=0; i<cur_read_lines; i++) {
          total_proc_lines++;

          const std::string &riak_key = riak_key_list[i];
          fret = _cluster_client.AsyncGet(async_handle, riak_bucket, riak_key, 0, &riak_param);
          if (!fret) {
            printf("Push AsyncGet(%s, %s) request...\n",
            	riak_bucket.c_str(), riak_key.c_str());
          } else {
            printf("AsyncGet(%s, %s) FAILED, ret=%d, msg=%s\n",
            	riak_bucket.c_str(), riak_key.c_str(), fret, CR_Class_NS::strerror(fret));
            return (fret);
          }
        }

        for (int i=0; i<cur_read_lines; i++) {
          const std::string &riak_key = riak_key_list[i];
          fret = _cluster_client.AsyncPopValue(async_handle, riak_bucket, riak_key, 0, riak_value);
          if (fret) {
            printf("AsyncPopValue(%s, %s) FAILED, ret=%d, msg=%s\n",
            	riak_bucket.c_str(), riak_key.c_str(), fret, CR_Class_NS::strerror(fret));
            if (fret != ENOENT)
              return (fret);
          } else {
            printf("B[%s],K[%s],VSIZE=%lu\n", riak_bucket.c_str(), riak_key.c_str(), riak_value.size());
            total_value_size += riak_value.size();
          }
        }

        cur_read_lines = 0;
        riak_key_list.clear();
      }
    }

    DPRINTF("AsyncGet stop, total_lines == %d, total_value_size == %lu.\n", total_proc_lines, total_value_size);

    std::string _err;
    _cluster_client.AsyncWait(async_handle, _err, 900);

    return 0;
}

int
do_asyncget_with_callback(int argc, char *argv[])
{
    int fret;

    if (argc < 5) {
      fprintf(stderr, "Usage: %s asyncgetc <bucket> <value_list_file> <param> [conn_num] [max_connects_per_node]\n", argv[0]);
      fprintf(stderr, "\tparam format: <S:<like_pattern> | N:<between_left>,<between_right>[,<local_min>]>\n");
      return (1);
    }

    std::string riak_bucket;
    std::string riak_key;
    std::string riak_param;
    std::string errmsg;

    std::string param_filename;
    char key_buf[128];
    int nodes;
    int conn_num = 4;
    void *async_handle;
    int max_connects_per_node = 1;

    if (strlen(argv[4]) > 2) {
      char like_pattern[256];
      int bet_l=0, bet_r=0, local_min=0;
      bool param_ok = false;

      bzero(like_pattern, sizeof(like_pattern));

      IBRiakDataPackQueryParam ibqp_tmp;
      IBRiakDataPackQueryParam::RowRange *range_p = ibqp_tmp.add_row_range();
      range_p->set_pos_begin(1);
      range_p->set_pos_end(65535);

      if (sscanf(argv[4], "S:%s", like_pattern) > 0) {
        ibqp_tmp.set_pack_type(IBRiakDataPackQueryParam::PACK_STRING);
        ibqp_tmp.mutable_expression()->set_op_type(IBRiakDataPackQueryParam::EOP_LK);
        ibqp_tmp.mutable_expression()->add_value_list_str(std::string(like_pattern));
        param_ok = true;
      } else if (sscanf(argv[4], "N:%d,%d,%d", &bet_l, &bet_r, &local_min) > 0) {
        ibqp_tmp.set_pack_local_min_num(local_min);
        ibqp_tmp.set_pack_type(IBRiakDataPackQueryParam::PACK_NUM_SIGNED);
        ibqp_tmp.mutable_expression()->set_op_type(IBRiakDataPackQueryParam::EOP_BT);
        ibqp_tmp.mutable_expression()->add_value_list_num(bet_l);
        ibqp_tmp.mutable_expression()->add_value_list_num(bet_r);
        param_ok = true;
      }

      if (param_ok)
        ibqp_tmp.SerializeToString(&riak_param);
      else
        riak_param = argv[4];
    }

    if (argc > 6) {
      max_connects_per_node = atoi(argv[6]);
      if (max_connects_per_node < 1)
        max_connects_per_node = 1;
      if (max_connects_per_node > 100)
        max_connects_per_node = 100;
    }

    RiakCluster _cluster_client(120, max_connects_per_node);

    riak_bucket = argv[2];
    param_filename = argv[4];
    if (argc > 5)
      conn_num = atoi(argv[5]);

    if (conn_num < 1)
      conn_num = 1;

    if (conn_num > 1000)
      conn_num = 1000;

    FILE *klist = fopen(argv[3], "r");
    if (!klist) {
      fprintf(stderr, "open klist file %s failed, err=%d,msg=%s\n", argv[3], errno, CR_Class_NS::strerror(errno));
      return (errno);
    }

    nodes = _cluster_client.Rebuild(riak_hostname, riak_port_str);

    if (nodes > 0)
      printf("Success connected to %d nodes\n", nodes);
    else {
      fprintf(stderr, "Connect to riak ring failed, err = %d, msg = %s\n", (0-nodes), CR_Class_NS::strerror(0-nodes));
      return (0-nodes);
    }

    async_handle = _cluster_client.AsyncInit(conn_num, 1024);
    if (!async_handle) {
      fprintf(stderr, "Async init failed\n");
      return (-1);
    }

    intptr_t total_lines = 0;
    std::string tag_data;

    while (fgets(key_buf, sizeof(key_buf), klist)) {
      riak_key = std::string(key_buf, strlen(key_buf) - 1);

      total_lines++;

      tag_data = CR_Class_NS::ptr2str((void *)total_lines);

      fret = _cluster_client.AsyncGetWithCallback(async_handle, riak_bucket, riak_key, 0, &riak_param,
      		async_get_callback, &tag_data);
      if (!fret) {
        printf("Push AsyncGetWithCallback(%s, %s) request...\n",
        	riak_bucket.c_str(), riak_key.c_str());
      } else {
        printf("Push AsyncGetWithCallback(%s, %s) request FAILED, ret=%d, msg=%s\n",
        	riak_bucket.c_str(), riak_key.c_str(), fret, CR_Class_NS::strerror(fret));
        return (fret);
      }
    }

    DPRINTF("AsyncGetWithCallback request pushed ok, total_lines == %d\n", (int)total_lines);

    std::string _err;
    _cluster_client.AsyncWait(async_handle, _err, 900);

    return 0;
}

int
do_batch_check(int argc, char *argv[])
{
    int fret;

    if (argc < 4) {
      fprintf(stderr, "Usage: %s batch-check <bucket> <key_list_file> [conn_num] [max_connects_per_node]\n", argv[0]);
      return (1);
    }

    std::string riak_bucket;
    std::string riak_key;
    std::string riak_param;
    std::string errmsg;

    char param_buf[2048];

    char key_buf[128];
    int nodes;
    int conn_num = 4;
    void *async_handle;
    int max_connects_per_node = 1;

    if (argc > 4) {
      conn_num = atoi(argv[4]);
      if (conn_num < 1)
        conn_num = 1;
      if (conn_num > 1000)
        conn_num = 1000;
    }

    if (argc > 5) {
      max_connects_per_node = atoi(argv[5]);
      if (max_connects_per_node < 1)
        max_connects_per_node = 1;
      if (max_connects_per_node > 100)
        max_connects_per_node = 100;
    }

    RiakCluster _cluster_client(120, max_connects_per_node);

    riak_bucket = argv[2];

    FILE *klist = fopen(argv[3], "r");
    if (!klist) {
      fprintf(stderr, "open key list file %s failed, err=%d,msg=%s\n", argv[3], errno, CR_Class_NS::strerror(errno));
      return (errno);
    }

    nodes = _cluster_client.Rebuild(riak_hostname, riak_port_str);

    if (nodes <= 0) {
      fprintf(stderr, "Connect to riak ring failed, err = %d, msg = %s\n", (0-nodes), CR_Class_NS::strerror(0-nodes));
      return (0-nodes);
    }

    async_handle = _cluster_client.AsyncInit(conn_num, 1024);
    if (!async_handle) {
      fprintf(stderr, "Async init failed\n");
      return (-1);
    }

    int total_lines = 0;
    std::string tag_data;

    while (fgets(key_buf, sizeof(key_buf), klist)) {
      riak_key = std::string(key_buf, strlen(key_buf) - 1);

      total_lines++;

      tag_data = CR_Class_NS::i642str(total_lines);

      snprintf(param_buf, sizeof(param_buf), "DIRECT_BUCKET=%s&DIRECT_KEY=%s&VERIFY_TYPE=MD5",
      	riak_bucket.c_str(), riak_key.c_str());

      riak_param = param_buf;

      fret = _cluster_client.AsyncGetWithCallback(async_handle, riak_bucket, riak_key, 0, &riak_param,
      		batch_check_callback, &tag_data);
      if (fret) {
        printf("Push AsyncGetWithCallback(%s, %s) request FAILED, ret=%d, msg=%s\n",
        	riak_bucket.c_str(), riak_key.c_str(), fret, CR_Class_NS::strerror(fret));
        return (fret);
      }
    }

    std::string _err;
    _cluster_client.AsyncWait(async_handle, _err, 900);

    return 0;
}

int
async_get_callback(const std::string *tag_data, const int op_ret, const std::string &op_data)
{
  msgRiakAsyncResp msgResp;

  if (op_ret)
    DPRINTF("op_ret==%d, msg==%s\n", op_ret, CR_Class_NS::strerror(op_ret));

  if (msgResp.ParseFromString(op_data)) {
    printf("tag_data==%s, cmdcode==%d, bucket==%s, key==%s, intret==%ld, queryid==%ld, value.size()==%lu\n",
    	(tag_data)?(tag_data->c_str()):"NULL", msgResp.cmdcode(), msgResp.bucket().c_str(), msgResp.key().c_str(),
        (msgResp.has_intret())?(msgResp.intret()):0, (msgResp.has_queryid())?(msgResp.queryid()):0,
        (msgResp.has_value())?(msgResp.value().size()):0);
  } else {
    DPRINTF("ParseFromString(op_data) failed\n");
  }

  return 0;
}

int
batch_check_callback(const std::string *tag_data, const int op_ret, const std::string &op_data)
{
  msgRiakAsyncResp msgResp;
  int64_t lines = CR_Class_NS::str2i64(*tag_data);

  if (op_ret)
    fprintf(stderr, "lines==%08ld, op_ret==%d, msg==%s\n", lines, op_ret, CR_Class_NS::strerror(op_ret));

  if (msgResp.ParseFromString(op_data)) {
    const std::string *value_p = NULL;
    int64_t intret = -1;

    if (msgResp.has_value()) {
      value_p = &(msgResp.value());

      if (msgResp.has_intret()) {
        intret = msgResp.intret();

        if ((intret != 0) || (value_p->size() != 32)) {
          fprintf(stderr, "tag_data==%08ld, cmdcode==%d, bucket==%s, key==%s, intret==%ld, V.size()==%ld\n",
        	lines, msgResp.cmdcode(), msgResp.bucket().c_str(),
        	msgResp.key().c_str(), intret, value_p->size());
        }
      } else {
        fprintf(stderr, "lines==%08ld, bucket==%s, key==%s, intret NOT found!",
        	lines, msgResp.bucket().c_str(), msgResp.key().c_str());
      }
    } else {
      fprintf(stderr, "lines==%08ld, bucket==%s, key==%s, value NOT found!",
      	lines, msgResp.bucket().c_str(), msgResp.key().c_str());
    }
  } else {
    fprintf(stderr, "lines==%08ld, ParseFromString(op_data) failed\n", lines);
  }

  return 0;
}
