#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include <riakdrv/riakconnect.h>

#define BIG_BLOCK_SIZE (1024 * 1024 * 8)
#define PUT_LOOP_COUNT 512

int main(int argc, char *argv[])
{
        int ret;
	char *hostname;
	int port;
	std::string s_in_1, s_in_2, s_in_3, s_in_4;
	std::string s_out_1, s_out_2, s_out_3, errmsg;
	char *tmp_buf;
	char name_buf[16];
	uint32_t u_in;
	uint32_t u_out;
	bool b_in;
	bool b_out;
	int fail_count;
	std::vector<std::string> vector_tmp;
	std::map<std::string, std::string> map_tmp;

	if (argc < 3) {
		std::cerr<<"Usage : "<<argv[0]<<" [hostname] [port]\n";
		return 1;
	}

	srand(time(NULL));

	tmp_buf = (char *)malloc(BIG_BLOCK_SIZE);
	memset(tmp_buf, 'K', BIG_BLOCK_SIZE);

	hostname = argv[1];
	port = atoi(argv[2]);

	RiakConnect *_riak_conn = new RiakConnect();

	do {
		if (_riak_conn->Connect(hostname, port) == 0) {
			std::cout<<"[Ping]\t\t";
			if ((ret = _riak_conn->Ping()) == 0) {
				std::cout<<"Success!\n";
			} else {
				std::cout<<"failed!\n";
			}

			std::cout<<"[GetServerInfo]\t";
			if ((ret = _riak_conn->GetServerInfo(s_out_1, s_out_2, &errmsg)) == 0) {
				std::cout<<"node = "<<s_out_1<<"\tserver_version = "<<s_out_2<<"\n";
			} else {
				std::cout<<"errmsg = "<<errmsg<<"\n";
			}

			std::cout<<"[SetClientId]\t";
			s_in_1 = "Client-test-0000";
			if ((ret = _riak_conn->SetClientId(s_in_1, &errmsg)) == 0) {
				std::cout<<"Success!\n";
			} else {
				std::cout<<"errmsg = "<<errmsg<<"\n";
			}

			std::cout<<"[GetClientId]\t";
			if ((ret = _riak_conn->GetClientId(s_out_1, &errmsg)) == 0) {
				std::cout<<"client_id = "<<s_out_1<<"\n";
			} else {
				std::cout<<"errmsg = "<<errmsg<<"\n";
			}

			std::cout<<"[GetBucket]\t";
			s_in_1 = "B1";
			u_out = 0;
			b_out = false;
			if ((ret = _riak_conn->GetBucket(s_in_1, u_out, b_out, &errmsg)) == 0) {
				std::cout<<"B = "<<s_in_1<<" -> n_val = "<<u_out<<", allow_mult = "<<b_out<<"\n";
			} else {
				std::cout<<"errmsg = "<<errmsg<<"\n";
			}

			std::cout<<"[SetBucket]\t";
			s_in_1 = "B1";
			u_in = 1;
			b_in = false;
			if ((ret = _riak_conn->SetBucket(s_in_1, u_in, b_in, &errmsg)) == 0) {
				std::cout<<"B = "<<s_in_1<<" -> n_val = "<<u_in<<", allow_mult = "<<b_in<<". Success!\n";
			} else {
				std::cout<<"errmsg = "<<errmsg<<"\n";
			}

			std::cout<<"[GetBucket]\t";
			s_in_1 = "B1";
			u_out = 0;
			b_out = false;
			if ((ret = _riak_conn->GetBucket(s_in_1, u_out, b_out, &errmsg)) == 0) {
				std::cout<<"B = "<<s_in_1<<" -> n_val = "<<u_out<<", allow_mult = "<<b_out<<"\n";
			} else {
				std::cout<<"errmsg = "<<errmsg<<"\n";
			}

			std::cout<<"[Put]\t\t";
			s_in_1 = "B1";
			s_in_2 = "K1";
                        s_in_3 = "QP1";
			s_in_4 = "V1";
			if ((ret = _riak_conn->PutSimple(s_in_1, s_in_2, s_in_3, s_in_4, &errmsg)) == 0) {
				std::cout<<"B = "<<s_in_1<<", K = "<<s_in_2<<", QP = "<<s_in_3<<", V = "<<s_in_4<<". Success!\n";
			} else {
				std::cout<<"errmsg = "<<errmsg<<"\n";
			}

			std::cout<<"[GetOne]\t";
			s_in_1 = "B1";
			s_in_2 = "K1";
                        s_in_3 = "QP1";
			s_out_1.clear();
			if ((ret = _riak_conn->GetOne(s_in_1, s_in_2, s_in_3, s_out_1, &errmsg)) == 0) {
				std::cout<<"B = "<<s_in_1<<", K = "<<s_in_2<<", QP = "<<s_in_3<<", V = "<<s_out_1<<"\n";
			} else {
				std::cout<<"errmsg = "<<errmsg<<"\n";
			}

			std::cout<<"[Del]\t\t";
			s_in_1 = "B1";
			s_in_2 = "K1";
                        s_in_3.clear();
			if ((ret = _riak_conn->Del(s_in_1, s_in_2, s_in_3, &errmsg)) == 0) {
				std::cout<<"B = "<<s_in_1<<", K = "<<s_in_2<<", QP = "<<s_in_3<<". Success!\n";
			} else {
				std::cout<<"errmsg = "<<errmsg<<"\n";
			}

			std::cout<<"[GetOne]\t";
			s_in_1 = "B1";
			s_in_2 = "K1";
                        s_in_3 = "QP1";
			s_out_1.clear();
			if ((ret = _riak_conn->GetOne(s_in_1, s_in_2, s_in_3, s_out_1, &errmsg)) == 0) {
				std::cout<<"B = "<<s_in_1<<", K = "<<s_in_2<<", QP = "<<s_in_3<<", V = "<<s_out_1<<"\n";
			} else {
				std::cout<<"errmsg = "<<errmsg<<"\n";
			}

			std::cout<<"[GetOne]\t";
			s_in_1 = "B1";
			s_in_2 = "K1";
                        s_in_3 = "QP1";
			s_out_1.clear();
			if ((ret = _riak_conn->GetOne(s_in_1, s_in_2, s_in_3, s_out_1, &errmsg)) == 0) {
				std::cout<<"B = "<<s_in_1<<", K = "<<s_in_2<<", QP = "<<s_in_3<<", V = "<<s_out_1<<"\n";
			} else {
				std::cout<<"errmsg = "<<errmsg<<"\n";
			}

			std::cout<<"[Put-big]\t";
			s_in_1 = "B1";
			s_in_2 = "K2";
                        s_in_3 = "QP1";
			s_in_4.assign(tmp_buf, BIG_BLOCK_SIZE);
			if ((ret = _riak_conn->PutSimple(s_in_1, s_in_2, s_in_3, s_in_4, &errmsg)) == 0) {
				std::cout<<"B = "<<s_in_1<<", K = "<<s_in_2<<", QP = "<<s_in_3<<", V.size() = "<<s_in_4.size()<<". Success!\n";
			} else {
				std::cout<<"errmsg = "<<errmsg<<"\n";
			}

			std::cout<<"[GetOne-big]\t";
			s_in_1 = "B1";
			s_in_2 = "K2";
                        s_in_3 = "QP1";
			s_out_1.clear();
			if ((ret = _riak_conn->GetOne(s_in_1, s_in_2, s_in_3, s_out_1, &errmsg)) == 0) {
				std::cout<<"B = "<<s_in_1<<", K = "<<s_in_2<<", QP = "<<s_in_3<<", V.size() = "<<s_out_1.size()<<"\n";
			} else {
				std::cout<<"errmsg = "<<errmsg<<"\n";
			}

			std::cout<<"[ListBuckets]\t";
			if ((ret = _riak_conn->ListBuckets(vector_tmp, &errmsg)) == 0) {
				std::cout<<"Buckets["<<vector_tmp.size()<<"] = {";
				for (size_t i=0; i<vector_tmp.size(); i++) {
					std::cout<<vector_tmp[i];
					if (i != vector_tmp.size()-1)
						std::cout<<", ";
				}
				std::cout<<"}\n";
			} else {
				std::cout<<"errmsg = "<<errmsg<<"\n";
			}

			fail_count = 0;
			std::cout<<"[Put-LOOP]\t";
			for (int i=0; i<PUT_LOOP_COUNT; i++) {
				s_in_1 = "B1";
				s_in_2 = "K-";
				bzero(name_buf, sizeof(name_buf));
				snprintf(name_buf, sizeof(name_buf), "%08X", rand());
				s_in_2 += name_buf;
                        	s_in_3 = "QP1";
				s_in_4 = "V-";
				bzero(name_buf, sizeof(name_buf));
				snprintf(name_buf, sizeof(name_buf), "%08X", rand());
				s_in_4 += name_buf;
				if ((ret = _riak_conn->PutSimple(s_in_1, s_in_2, s_in_3, s_in_4, &errmsg)) != 0)
					fail_count++;
			}
			std::cout<<"Total = "<<PUT_LOOP_COUNT<<", failed = "<<fail_count<<"\n";

			std::cout<<"[ListKeys]\t";
			s_in_1 = "B1";
			if ((ret = _riak_conn->ListKeys(s_in_1, vector_tmp, &errmsg)) == 0) {
				std::cout<<"keys_count = "<<vector_tmp.size()<<"\n";
			} else {
				std::cout<<"errmsg = "<<errmsg<<"\n";
			}

			std::cout<<"[IndexBucket]\t";
			s_in_1 = "B1";
			if ((ret = _riak_conn->IndexBucket(s_in_1, vector_tmp, &errmsg)) == 0) {
				std::cout<<"keys_count = "<<vector_tmp.size()<<"\n";
			} else {
				std::cout<<"errmsg = "<<errmsg<<"\n";
			}

			std::cout<<"[IndexKeyRange]\t";
			s_in_1 = "B1";
			s_in_2 = "K-12";
			s_in_3 = "K-13";
			if ((ret = _riak_conn->IndexKeyRange(s_in_1, s_in_2, s_in_3, vector_tmp, &errmsg)) == 0) {
				std::cout<<"Keys["<<vector_tmp.size()<<"] = {";
				for (size_t i=0; i<vector_tmp.size(); i++) {
					std::cout<<vector_tmp[i];
					if (i != vector_tmp.size()-1)
						std::cout<<", ";
				}
				std::cout<<"}\n";
			} else {
				std::cout<<"errmsg = "<<errmsg<<"\n";
			}

			std::cout<<"[Put-FULL]\t";
			s_in_1 = "B1";
			s_in_2 = "K3";
                        s_in_3 = "QP1";
			s_in_4 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
			map_tmp["IK_bin"] = s_in_4;
			if ((ret = _riak_conn->Put(s_in_1, s_in_2, s_in_3, s_in_4,
					NULL, NULL, NULL, NULL, &map_tmp, &errmsg)) == 0) {
				std::cout<<"B = "<<s_in_1<<", K = "<<s_in_2<<", QP = "<<s_in_3<<", V = "<<s_in_4<<". Success!\n";
			} else {
				std::cout<<"errmsg = "<<errmsg<<"\n";
			}

		} else
			std::cout<<"Connect failed!\n";
	} while(0);

	return 0;
}
