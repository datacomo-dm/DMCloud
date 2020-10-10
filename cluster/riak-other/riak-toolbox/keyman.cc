#include "riak-toolbox.h"
struct DT{
	uint64_t second	: 6;
	uint64_t minute	: 6;
	uint64_t hour	: 16;
	uint64_t day 	: 5;
	uint64_t month	: 4;
	uint64_t year	: 14;
	uint64_t unused	: 13;
} ;

long get_date(const char *str) {
	DT  dt;
	int year,month,day,hour,minute,second;
	sscanf(str,"%04d-%02d-%02d %02d:%02d:%02d",&year,&month,&day,&hour,&minute,&second);
	dt.year=year;dt.month=month;dt.day=day;
	dt.hour=hour;dt.minute=minute;dt.second=second;
	return *((long *)&dt);
}

int
do_kvtest(int argc, char *argv[])
{
    int fret;

    if (argc < 5) {
      fprintf(stderr, "Usage: %s kvtest <PUT | GET | DEL> <bucket> <key> [param_str | param_filename] [value | value_filename]\n"
        "\tparam_str format: <S:<like_pattern> | N:<between_left>,<between_right>[,<local_min>] | <SR: | NR:><range_left>,<range_right>>\n"
        , argv[0]);
      return 1;
    }

    std::string riak_value;
    std::string riak_param;
    int nodes;

    RiakCluster _cluster_client(120, 1);

    nodes = _cluster_client.Rebuild(riak_hostname, riak_port_str);

    if (nodes > 0)
      printf("Success connected to %d nodes\n", nodes);
    else {
      fprintf(stderr, "Connect to riak ring failed, err = %d, msg = %s\n", (0-nodes), CR_Class_NS::strerror(0-nodes));
      return 0-nodes;
    }

    if (argc > 5) {
      char pattern[256];
      long bet_l=0, bet_r=0, local_min=0;
      int range_l=0, range_r=65535;
      bool param_ok = false;

      IBRiakDataPackQueryParam ibqp_tmp;
      IBRiakDataPackQueryParam::RowRange *one_rowrange = ibqp_tmp.add_row_range();
      one_rowrange->set_pos_begin(0);
      one_rowrange->set_pos_end(65535);

      bzero(pattern, sizeof(pattern));
	  //string between
      if (sscanf(argv[5], "S:%s", pattern) > 0) {
        ibqp_tmp.set_pack_type(IBRiakDataPackQueryParam::PACK_STRING);
        ibqp_tmp.mutable_expression()->set_op_type(IBRiakDataPackQueryParam::EOP_BT);
        ibqp_tmp.mutable_expression()->add_value_list_str(pattern);
        ibqp_tmp.mutable_expression()->add_value_list_str(pattern);
        param_ok = true;
	  } 
	  //string like
	  else if (sscanf(argv[5], "SL:%s", pattern) > 0) {
		  ibqp_tmp.set_pack_type(IBRiakDataPackQueryParam::PACK_STRING);
		  ibqp_tmp.mutable_expression()->set_op_type(IBRiakDataPackQueryParam::EOP_LK);
		  ibqp_tmp.mutable_expression()->add_value_list_str(std::string(pattern));
		  param_ok = true;
      } 
	  //datetime between
	  else if (sscanf(argv[5], "D:%s,%ld", pattern,&local_min) > 0) {
      	long v=get_date(pattern);
        ibqp_tmp.set_pack_type(IBRiakDataPackQueryParam::PACK_NUM_SIGNED);
        ibqp_tmp.set_pack_local_min_num(local_min);
        ibqp_tmp.mutable_expression()->set_op_type(IBRiakDataPackQueryParam::EOP_BT);
        ibqp_tmp.mutable_expression()->add_value_list_num(v);
        ibqp_tmp.mutable_expression()->add_value_list_num(v);
        param_ok = true;
	  } 
	  //double between
	  else if (sscanf(argv[5], "F:%s", pattern) > 0) {
      	double v=atof(pattern);
        ibqp_tmp.set_pack_type(IBRiakDataPackQueryParam::PACK_FLOAT);
        ibqp_tmp.mutable_expression()->set_op_type(IBRiakDataPackQueryParam::EOP_BT);
        ibqp_tmp.mutable_expression()->add_value_list_flt(v);
        ibqp_tmp.mutable_expression()->add_value_list_flt(v);
        param_ok = true;
      }
	  //long betwen
	  else if (sscanf(argv[5], "N:%ld,%ld,%ld", &bet_l, &bet_r, &local_min) > 0) {
        ibqp_tmp.set_pack_local_min_num(local_min);
        ibqp_tmp.set_pack_type(IBRiakDataPackQueryParam::PACK_NUM_SIGNED);
        ibqp_tmp.mutable_expression()->set_op_type(IBRiakDataPackQueryParam::EOP_BT);
        ibqp_tmp.mutable_expression()->add_value_list_num(bet_l);
        ibqp_tmp.mutable_expression()->add_value_list_num(bet_r);
        param_ok = true;
      } 
	  //string RANGE
	  else if (sscanf(argv[5], "SR:%d,%d", &range_l, &range_r) > 0) {
        ibqp_tmp.set_pack_type(IBRiakDataPackQueryParam::PACK_STRING);
        one_rowrange->set_pos_begin(range_l);
        one_rowrange->set_pos_end(range_r);
        param_ok = true;
      }
	  //long between
	  else if (sscanf(argv[5], "NR:%d,%d", &range_l, &range_r) > 0) {
        ibqp_tmp.set_pack_type(IBRiakDataPackQueryParam::PACK_NUM_SIGNED);
        one_rowrange->set_pos_begin(range_l);
        one_rowrange->set_pos_end(range_r);
        param_ok = true;
      }

      if (param_ok)
        ibqp_tmp.SerializeToString(&riak_param);
      else
        riak_param = argv[5];
    }
    else
      riak_param = "";

    if (strcmp(argv[2], "PUT") == 0) {
      if (argc < 7) {
        fprintf(stderr, "PUT command NEED value\n");
        return 1;
      }
      if (CR_Class_NS::load_string(argv[6], riak_value) != 0)
        riak_value = argv[6];

      size_t stored_size;
      fret = _cluster_client.Put(argv[3], argv[4], riak_value, &riak_param, &stored_size);
      if (fret == 0) {
        printf("PUT B = \"%s\", K = \"%s\", stored_size()= %lu successed.\n", argv[3], argv[4], stored_size);
      } else {
        fprintf(stderr, "PUT B = \"%s\", K = \"%s\", V= \"%s\" failed, fret=%d, msg=%s.\n",
        	argv[3], argv[4], argv[6], fret, CR_Class_NS::strerror(fret));
        return 1;
      }
    } else if (strcmp(argv[2], "GET") == 0) {
      const char *riak_node_id = getenv("RIAK_SPEC_NODE_ID");
      const char *riak_node_id_never = getenv("RIAK_NEVER_NODE_ID");
      std::string str_node_id;
      std::string *str_node_id_p = NULL;
      std::string str_node_id_never;
      std::string *str_node_id_never_p = NULL;
      if (riak_node_id) {
        str_node_id = riak_node_id;
        str_node_id_p = &str_node_id;
      }
      if (riak_node_id_never) {
        str_node_id_never = riak_node_id_never;
        str_node_id_never_p = &str_node_id_never;
      }
      fret = _cluster_client.Get(argv[3], argv[4], riak_value, &riak_param, str_node_id_p, str_node_id_never_p);

      if (fret == 0) {
        if (riak_value.size() <= 32) {
          printf("GET B = \"%s\", K = \"%s\" successed, Vsize = %lu, V=\"%s\"\n",
          	argv[3], argv[4], riak_value.size(), riak_value.c_str());
        } else {
          printf("GET B = \"%s\", K = \"%s\" successed, Vsize = %lu, v(0) is \"%c\"\n",
          	argv[3], argv[4], riak_value.size(), riak_value.c_str()[0]);
        }
        if (argc > 6) {
          if (CR_Class_NS::save_string(argv[6], riak_value) == 0) {
            printf("\tValue was saved at file: %s\n", argv[6]);
          }
        }
        // try to decode :
        msgRCAttr_packS msg_packS;
        msgRCAttr_packN msg_packN;
        if(msg_packS.ParseFromString(riak_value)) {
          printf("Get PackS data:\n%s\n--------------\n",msg_packS.DebugString().c_str());
        } else if(msg_packN.ParseFromString(riak_value)) {
          printf("Get PackN data:\n%s\n--------------\n",msg_packN.DebugString().c_str());
        }
      } else {
        fprintf(stderr, "GET B = \"%s\", K = \"%s\" failed, fret=%d, msg=%s.\n",
        	argv[3], argv[4], fret, CR_Class_NS::strerror(fret));
        return 1;
      }
    } else if (strcmp(argv[2], "DEL") == 0) {
      _cluster_client.Del(argv[3], argv[4]);
      fret = _cluster_client.Get(argv[3], argv[4], riak_value, &riak_param);
      if (riak_value == "") {
        printf("DEL B = \"%s\", K = \"%s\" OK\n", argv[3], argv[4]);
      } else {
        fprintf(stderr, "DEL B = \"%s\", K = \"%s\" failed, fret=%d, msg=%s.\n",
        	argv[3], argv[4], fret, CR_Class_NS::strerror(fret));
      }
    } else {
      fprintf(stderr, "Unknown command %s\n", argv[2]);
      return 1;
    }

    return 0;
}

int
do_kvtest_sc(int argc, char *argv[])
{
    int fret;

    if (argc < 5) {
      fprintf(stderr, "Usage: %s kvtest_sc <PUT | GET | DEL> <bucket> <key> [param] [value | value_filename]\n", argv[0]);
      return 1;
    }

    std::string riak_value;
    std::string riak_param;
    RiakConnect _riak("", 60);

    fret = _riak.Connect(riak_hostname, riak_port_str);

    if (fret) {
      fprintf(stderr, "Connect to riak node failed, err = %d, msg = %s\n", fret, CR_Class_NS::strerror(fret));
      return fret;
    }

    double riak_time = _riak.GetTimeOfDay();
    printf("Node time: %.06f\n", riak_time);

    if (argc > 5)
      riak_param = argv[5];
    else
      riak_param = "";

    if (strcmp(argv[2], "PUT") == 0) {
      if (argc < 7) {
        fprintf(stderr, "PUT command NEED value\n");
        return 1;
      }

      if (CR_Class_NS::load_string(argv[6], riak_value) != 0)
        riak_value = argv[6];

      size_t stored_size;
      fret = _riak.Put(argv[3], argv[4], riak_value, &riak_param, &stored_size);
      if (fret == 0) {
        printf("PUT B = \"%s\", K = \"%s\", stored_size()= %lu successed.\n", argv[3], argv[4], stored_size);
      } else {
        fprintf(stderr, "PUT B = \"%s\", K = \"%s\", V=\"%s\" failed, fret=%d, msg=%s.\n",
        	argv[3], argv[4], argv[5], fret, CR_Class_NS::strerror(fret));
        return 1;
      }
    }

    if (strcmp(argv[2], "GET") == 0) {
      fret = _riak.Get(argv[3], argv[4], riak_value, &riak_param);
      if (fret == 0) {
        if (riak_value.size() <= 32) {
          printf("GET B = \"%s\", K = \"%s\" successed, Vsize = %lu, V=\"%s\"\n",
          	argv[3], argv[4], riak_value.size(), riak_value.c_str());
        } else {
          printf("GET B = \"%s\", K = \"%s\" successed, Vsize = %lu, v(0) is \"%c\"\n",
          	argv[3], argv[4], riak_value.size(), riak_value.c_str()[0]);
        }
        if (argc > 6) {
          if (CR_Class_NS::save_string(argv[6], riak_value) == 0) {
            printf("\tValue was saved at file: %s\n", argv[6]);
          }
        }
      } else {
        fprintf(stderr, "GET B = \"%s\", K = \"%s\" failed, fret=%d, msg=%s.\n",
        	argv[3], argv[4], fret, CR_Class_NS::strerror(fret));
        return 1;
      }
    }

    if (strcmp(argv[2], "DEL") == 0) {
      _riak.Del(argv[3], argv[4], NULL);
      fret = _riak.Get(argv[3], argv[4], riak_value, &riak_param);
      if (fret == ENOENT) {
        printf("DEL B = \"%s\", K = \"%s\" OK\n", argv[3], argv[4]);
      } else {
        fprintf(stderr, "DEL B = \"%s\", K = \"%s\" failed, fret=%d, msg=%s.\n",
        	argv[3], argv[4], fret, CR_Class_NS::strerror(fret));
      }
    }

    return 0;
}
