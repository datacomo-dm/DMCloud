#include "cluster_fm.h"
#include "cfm_jobexecutor.h"
#include <thread>

typedef struct comm_resp {
    comm_resp() {
        this->comm_ret = ENOTCONN;
        this->same_count = 0;
    };

    std::thread *tp;
    int comm_ret;
    int8_t cmdcode_resp;
    std::string s_resp;
    size_t same_count;
} comm_resp_t;

static int do_req_self(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);

int
do_req_distributed(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg)
{
    int ret = 0;
    const CFM_Basic_NS::striping_info_t &striping_info = app_cluster_info[app_current_node.first];
    size_t striping_size = striping_info.size();

    if ((striping_size > 1) && CFM_Basic_NS::exchange_msg_dtst(data_from_client)) {
        std::string s_req = data_from_client;
        std::vector<CR_SimpleComm*> &striping_conn_array = *((std::vector<CR_SimpleComm*>*)connect_private_p);
        if (striping_conn_array.empty()) {  // if striping connect array is empty, make it
            CR_Class_NS::nn_transport_t tmp_transport;
            std::string tmp_hostname;
            std::string tmp_srvname;
            for (size_t i=0; i<striping_size; i++) {
                CR_SimpleComm *conn_p = NULL;
                int fret = CR_Class_NS::nn_addr_split(striping_info[i].c_str(),
                  tmp_transport, tmp_hostname, tmp_srvname);
                if ((!fret) && (tmp_transport == CR_Class_NS::NN_TCP)) {
                    conn_p = new CR_SimpleComm;
                    conn_p->Connect(tmp_hostname, tmp_srvname);
                }
                striping_conn_array.push_back(conn_p);
            }
        }

        CFM_Basic_NS::exchange_msg_dcli(s_req); // clear 'distributed' bit in req string

        std::vector<comm_resp_t> comm_resp_array;
        comm_resp_array.resize(striping_size);

        for (size_t i=0; i<striping_size; i++) { // distribut req to all striping nodes
            comm_resp_t &cur_comm_resp = comm_resp_array[i];
            cur_comm_resp.tp = new std::thread(CR_SimpleComm_TF::SendAndRecv,
              striping_conn_array[i], &(cur_comm_resp.comm_ret), cmd_code_from_client, &s_req,
              &(cur_comm_resp.cmdcode_resp), &(cur_comm_resp.s_resp), 0);
        }

        for (size_t i=0; i<striping_size; i++) { // collect striping node resps
            comm_resp_t &cur_comm_resp = comm_resp_array[i];
            cur_comm_resp.tp->join();
            delete cur_comm_resp.tp;
            cur_comm_resp.tp = NULL;
        }

        for (size_t i=0; i<striping_size; i++) { // check resp
            comm_resp_t &cur_comm_resp = comm_resp_array[i];
            for (size_t j=0; j<i; j++) { // find same resp
                comm_resp_t &cmp_comm_resp = comm_resp_array[j];
                if ((cmp_comm_resp.comm_ret == cur_comm_resp.comm_ret)
                  && (cmp_comm_resp.cmdcode_resp == cur_comm_resp.cmdcode_resp)
                  && (cmp_comm_resp.s_resp == cur_comm_resp.s_resp)) {
                    cmp_comm_resp.same_count++;
                    break;
                }
            }
/*
            if (cur_comm_resp.comm_ret != 0) {
                DPRINTF("Node %s:%s comm failed[%s]\n",
                  striping_conn_array[i]->get_hostname().c_str(),
                  striping_conn_array[i]->get_srvname().c_str(),
                  CR_Class_NS::strerrno(cur_comm_resp.comm_ret));
                striping_conn_array[i]->DisConnect();
            } else if (cur_comm_resp.cmdcode_resp != (cmd_code_from_client + 1)) {
                DPRINTF("Node %s:%s resp failed\n",
                  striping_conn_array[i]->get_hostname().c_str(),
                  striping_conn_array[i]->get_srvname().c_str());
                striping_conn_array[i]->DisConnect();
            }
*/
        }
    } else {
        ret = do_req_self(server_private_p, connect_private_p, client_host, client_srv,
          cmd_code_from_client, data_from_client, cmd_code_to_client, data_to_client, errmsg);
    }

    return ret;
}

static int
do_req_self(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg)
{
    int ret = 0;

    switch (cmd_code_from_client) {
    case CFM_PING_REQ :
        ret = do_nd_ping_req(server_private_p, connect_private_p, client_host, client_srv,
          cmd_code_from_client, data_from_client, cmd_code_to_client, data_to_client, errmsg);
        break;
    case CFM_JOB_START_REQ :
        ret = do_nd_job_start_req(server_private_p, connect_private_p, client_host, client_srv,
          cmd_code_from_client, data_from_client, cmd_code_to_client, data_to_client, errmsg);
        break;
    case CFM_JOB_STOP_REQ :
        ret = do_nd_job_stop_req(server_private_p, connect_private_p, client_host, client_srv,
          cmd_code_from_client, data_from_client, cmd_code_to_client, data_to_client, errmsg);
        break;
    case CFM_JOB_CANCEL_REQ :
        ret = do_nd_job_cancel_req(server_private_p, connect_private_p, client_host, client_srv,
          cmd_code_from_client, data_from_client, cmd_code_to_client, data_to_client, errmsg);
        break;
    case CFM_ID_REQ :
        ret = do_nd_id_req(server_private_p, connect_private_p, client_host, client_srv,
          cmd_code_from_client, data_from_client, cmd_code_to_client, data_to_client, errmsg);
        break;
    case LOCK_REQ :
        ret = do_nd_lock_req(server_private_p, connect_private_p, client_host, client_srv,
          cmd_code_from_client, data_from_client, cmd_code_to_client, data_to_client, errmsg);
        break;
    case UNLOCK_REQ :
        ret = do_nd_unlock_req(server_private_p, connect_private_p, client_host, client_srv,
          cmd_code_from_client, data_from_client, cmd_code_to_client, data_to_client, errmsg);
        break;
    case CFM_JOB_QUERY_REQ :
    case CFM_JOB_DATA_REQ :
    case CFM_JOB_CLEAR_REQ :
    case CFM_JOB_KVPUT_REQ :
    case CFM_JOB_KVGET_REQ :
        ret = do_nd_job_other_req(server_private_p, connect_private_p, client_host, client_srv,
          cmd_code_from_client, data_from_client, cmd_code_to_client, data_to_client, errmsg);
        break;
    default :
        ret = ENOTSUP;
        break;
    }

    return ret;
}
