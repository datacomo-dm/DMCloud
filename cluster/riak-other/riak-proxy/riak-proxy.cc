#include <cr_class/cr_class.h>

#include <ctype.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/param.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/stat.h>

#include <vector>

#include <cr_class/cr_quickhash.h>
#include <cr_class/cr_thread.h>
#include <cr_class/cr_simplecomm.h>
#include <cr_class/cr_simpleserver.h>
#include <riakdrv/riak_common.h>
#include <riakdrv/riakconnect.h>
#include <riakdrv/riakdrv++.pb.h>
#include <riakdrv/riak.pb.h>
#include <riakdrv/riak_kv.pb.h>
#include <riakdrv/riak_search.pb.h>

#include "riak-proxy.h"
#include "msgIB.pb.h"
#include "ibpack_n.h"
#include "ibpack_s.h"

#define _THREAD_PARM_ACCEPT_SOCK_NAME		"THREAD_PARM_ACCEPT_SOCK"
#define _THREAD_PARM_SERVER_PORT_NAME		"THREAD_PARM_SERVER_PORT"
#define _THREAD_PARM_PEER_ADDR_NAME		"THREAD_PARM_PEER_ADDR"
#define _THREAD_PARM_PEER_SERV_NAME		"THREAD_PARM_PEER_SERV"

#define _THREAD_PARM_REFRESH_TIMEOUT_NAME	"_THREAD_PARM_REFRESH_TIMEOUT"

int cr_debug_level = 10;

int
main( int argc, char *argv[] )
{
    int fret;
    char *listen_port_str;
    char *server_port_str;
    int refresh_timeout = 86400;
    std::string client_host;
    std::string client_port;

    if (argc < 3) {
        fprintf(stderr, "Usage : %s <listen port> <server port> [refresh_timeout_sec]\n", argv[0]);
        exit(1);
    }

    if (argc > 3) {
        refresh_timeout = atoi(argv[3]);

        if (refresh_timeout < 0)
            refresh_timeout = 0;
    }

    cr_debug_level = CR_Class_NS::get_debug_level();

    std::set<int> sig_ret;
    std::set<int> sig_set;

    sig_ret.insert(0);
    sig_ret.insert(ETIMEDOUT);

    sig_set.insert(SIGSEGV);
    sig_set.insert(SIGTERM);
    sig_set.insert(SIGKILL);
    sig_set.insert(SIGABRT);

    if (refresh_timeout > 0) {
        fret = CR_Class_NS::protection_fork(NULL, &sig_ret, &sig_set);
        if (fret != 0)
            return 0;
    }

    listen_port_str = argv[1];
    server_port_str = argv[2];

    if ((atoi(listen_port_str) < 1024) || (atoi(listen_port_str) >= 49152)) {
        fprintf(stderr, "Listen port must between [1024, 49151]");
        exit(1);
    }

    CR_SimpleServer listen_server;
    CR_Thread::TDB_t tdb;

    fret = listen_server.Listen(CR_Class_NS::blank_string, listen_port_str, 100, 120);

    if (fret) {
        DPRINTF("Listen on port %s failed, err=%d, msg=\"%s\"\n", listen_port_str, fret, strerror(fret));
        return fret;
    }

    if (refresh_timeout > 0) {
        tdb.thread_func = refresh_thread;
        tdb.default_tags[_THREAD_PARM_REFRESH_TIMEOUT_NAME] = CR_Class_NS::i642str(refresh_timeout);
        CR_Thread::handle_t thread_handle = CR_Thread::talloc(tdb);
        CR_Thread::start(thread_handle);
    }

    fret = listen_server.AsyncAcceptStart(doaccept, server_port_str);

    if (fret) {
        return fret;
    }

    while (1) {
        sleep(1);
    }
}

std::string
refresh_thread(CR_Thread::handle_t handle)
{
    std::string ret = CR_Class_NS::blank_string;
    int refresh_timeout = CR_Class_NS::str2i64(CR_Thread::gettag(handle, _THREAD_PARM_REFRESH_TIMEOUT_NAME));
    int sleep_time = rand() % refresh_timeout + rand() % refresh_timeout + 1;

    sleep(sleep_time);
    exit(ETIMEDOUT);

    return ret;
}

std::string
mkriakerr(const int err_no)
{
    std::string ret;
    RpbErrorResp msg_error_resp;

    msg_error_resp.set_errcode(err_no);
    msg_error_resp.set_errmsg(CR_Class_NS::strerror(err_no));
    msg_error_resp.SerializeToString(&ret);

    return ret;
}

int
doaccept(const int sock, const std::string &client_host,
	const std::string &client_srv, void *arg)
{
    int fret = 0;
    CR_SimpleComm client_conn(sock);
    CR_SimpleComm server_conn;
    std::string server_host_port = (const char*)arg;
    size_t find_pos = server_host_port.find(':');

    DPRINTF("Client %s:%s connected.\n", client_host.c_str(), client_srv.c_str());

    if (find_pos == std::string::npos) {
        fret = server_conn.Connect("127.0.0.1", server_host_port);
    } else {
        fret = server_conn.Connect(server_host_port.substr(0, find_pos), server_host_port.substr(find_pos+1));
    }

    if (fret)
        return fret;

    int8_t client_req_code=0, client_resp_code=0;
    std::string client_req_str, client_resp_str;
    int8_t server_req_code=0, server_resp_code=0;
    std::string server_req_str, server_resp_str;
    bool push_error = false;

    enum {
        OP_NONE,
        OP_RECV_CLIENT,
        OP_SEND_SERVER,
        OP_RECV_SERVER,
        OP_SEND_CLIENT,
        OP_EXIT
    } op_mode;

    RpbGetReq msg_get_req;
    RpbGetResp msg_get_resp;

    RpbPutReq msg_put_req;
    RpbPutResp msg_put_resp;

    RpbListKeysResp msg_listkeys_resp;

    RpbMapRedResp msg_mapred_resp;

    RpbIndexResp msg_index_resp;

    std::string riak_bucket;
    std::string riak_key;
    const std::string *riak_value_p;
    std::string riak_params_get;
    std::string riak_params_put;

    size_t crude_size = 0;
    size_t stored_size = 0;

    client_conn.set_timeout_send(5);
    client_conn.set_timeout_recv(3600);

    server_conn.set_timeout_send(5);
    server_conn.set_timeout_recv(3600);

    op_mode = OP_RECV_CLIENT;

    while (!fret) {
        push_error = false;
        switch (op_mode) {
        case OP_RECV_CLIENT :
            fret = client_conn.ProtoRecv(client_req_code, &client_req_str);
            if (!fret) {
                switch (client_req_code) {
                case RPB_GET_REQ :
                    if (msg_get_req.ParseFromString(client_req_str)) {
                        riak_bucket = msg_get_req.bucket();
                        riak_key = msg_get_req.key();
                        DPRINTF("RPB_GET_REQ:B[%s]K[%s]\n", riak_bucket.c_str(), riak_key.c_str());

                        stored_size = riak_bucket.size() + riak_key.size() + 320;

                        if (msg_get_req.has_queryparams()) {
                            riak_params_get = msg_get_req.queryparams();
                        } else {
                            riak_params_get = CR_Class_NS::blank_string;
                        }
                        server_req_str = client_req_str;
                    } else {
                        DPRINTF("RPB_GET_REQ: ParseFromString FAILED!\n");
                        fret = EBADMSG;
                        push_error = true;
                    }
                    break;
                case RPB_PUT_REQ :
                    if (msg_put_req.ParseFromString(client_req_str)) {
                        const RpbContent &const_put_content = msg_put_req.content();
                        RpbContent &put_content = *(msg_put_req.mutable_content());

                        riak_bucket = msg_put_req.bucket();

                        if (msg_put_req.has_key()) {
                            riak_key = msg_put_req.key();
                        } else {
                            riak_key = CR_Class_NS::blank_string;
                        }

                        if (msg_put_req.has_queryparams()) {
                            riak_params_put = msg_put_req.queryparams();
                        } else {
                            riak_params_put = CR_Class_NS::blank_string;
                        }

                        riak_value_p = &(const_put_content.value());

                        DPRINTF("RPB_PUT_REQ:B[%s]K[%s]PS[%ld]VS[%ld]\n",
                          riak_bucket.c_str(), riak_key.c_str(), riak_params_put.size(),
                          riak_value_p->size());

                        crude_size = riak_value_p->size();

                        stored_size = riak_bucket.size() + riak_key.size() + 400;

                        int riakf_op_mode = 0;
                        if (msg_put_req.has_riakf_op_mode()) {
                            riakf_op_mode = msg_put_req.riakf_op_mode();
                        }

                        if (riakf_op_mode == 1) {
                            server_req_str = client_req_str;
                        } else {
                            std::string riak_value_trans_decomp;
                            std::string riak_value_save_comp;
                            std::string transfer_compress_str;
                            std::string save_compress_str;

                            if (RiakMisc::GetUserMeta(const_put_content,
                              RIAK_META_NAME_TRANSFER_COMPRESS) == RIAK_META_VALUE_TRUE) {
                                RiakMisc::DelUserMeta(put_content, RIAK_META_NAME_TRANSFER_COMPRESS);
                                if (CR_Class_NS::decompressx(*riak_value_p, riak_value_trans_decomp) == 0) {
                                    riak_value_p = &riak_value_trans_decomp;
                                    crude_size = riak_value_trans_decomp.size();
                                } else {
                                    DPRINTF("WARNING: User-Meta %s is %s, but decompress failed.\n",
                                      RIAK_META_NAME_TRANSFER_COMPRESS, RIAK_META_VALUE_TRUE);
                                }
                            }

                            RiakMisc::SetUserMeta(put_content, RIAK_META_NAME_VALUE_HASH,
                              CR_QuickHash::md5(*riak_value_p));

                            if (RiakMisc::GetUserMeta(const_put_content,
                              RIAK_META_NAME_SAVE_COMPRESS) != RIAK_META_VALUE_FALSE) {
                                if (CR_Class_NS::compressx(*riak_value_p, riak_value_save_comp) == 0) {
                                    RiakMisc::SetUserMeta(put_content,
                                      RIAK_META_NAME_SAVE_COMPRESS, RIAK_META_VALUE_TRUE);
                                    riak_value_p = &riak_value_save_comp;
                                }
                            }

                            put_content.set_value(*riak_value_p);

                            stored_size += put_content.ByteSize();

                            RiakMisc::SetUserMeta(put_content, RIAK_META_NAME_STORED_SIZE,
                              CR_Class_NS::u642str(stored_size));
                            RiakMisc::SetUserMeta(put_content, RIAK_META_NAME_CRUDE_SIZE,
                              CR_Class_NS::u642str(crude_size));

                            msg_put_req.SerializeToString(&server_req_str);
                        }
                    } else {
                        DPRINTF("RPB_PUT_REQ: ParseFromString FAILED!\n");
                        fret = EBADMSG;
                        push_error = true;
                    }
                    break;
                default :
                    server_req_str = client_req_str;
                    break;
                }
                server_req_code = client_req_code;
                op_mode = OP_SEND_SERVER;
            }
            break;
        case OP_SEND_SERVER :
            fret = server_conn.ProtoSend(server_req_code, &server_req_str);
            if (!fret) {
                op_mode = OP_RECV_SERVER;
            } else
                push_error = true;
            break;
        case OP_RECV_SERVER :
            fret = server_conn.ProtoRecv(server_resp_code, &server_resp_str);
            if (!fret) {
                client_resp_code = server_resp_code;
                switch (server_resp_code) {
                case RPB_GET_RESP :
                    if (msg_get_resp.ParseFromString(server_resp_str)) {
                        int riakf_op_mode = 0;
                        if (msg_put_req.has_riakf_op_mode()) {
                            riakf_op_mode = msg_put_req.riakf_op_mode();
                        }

                        if (riakf_op_mode == 1) {
                            client_resp_str = server_resp_str;
                        } else {
                            for (int i=0; i<msg_get_resp.content_size(); i++) {
                                RpbContent* content_p = msg_get_resp.mutable_content(i);
                                const std::string &_value = content_p->value();
                                size_t _value_size = _value.size();
                                if (_value_size == 0)
                                    continue;
                                riak_value_p = &_value;
                                std::string _riak_value_decompress;
                                if (RiakMisc::GetUserMeta(*content_p, RIAK_META_NAME_SAVE_COMPRESS)
                                  == RIAK_META_VALUE_TRUE) {
                                    RiakMisc::DelUserMeta(*content_p, RIAK_META_NAME_SAVE_COMPRESS);
                                    if (CR_Class_NS::decompressx(*riak_value_p, _riak_value_decompress) == 0) {
                                        riak_value_p = &_riak_value_decompress;
                                    } else {
                                        DPRINTF("WARNING: User-Meta %s is %s, but decompress failed.\n",
                                          RIAK_META_NAME_SAVE_COMPRESS, RIAK_META_VALUE_TRUE);
                                    }
                                } else if (!CR_Class_NS::decompressx(*riak_value_p, _riak_value_decompress)) {
                                    riak_value_p = &_riak_value_decompress;
                                }

                                std::string _riak_value_paramed = get_convert(riak_bucket, riak_key,
                                  riak_params_get, *riak_value_p);
                                riak_value_p = &_riak_value_paramed;

                                std::string _riak_value_snappy;
                                if (riak_value_p->size() >= RIAK_CONNECT_TRANSFER_COMPRESS_MIN_SIZE) {
                                    if (CR_Class_NS::compressx_snappy(*riak_value_p, _riak_value_snappy) == 0) {
                                        RiakMisc::SetUserMeta(*content_p, RIAK_META_NAME_TRANSFER_COMPRESS,
                                          RIAK_META_VALUE_TRUE);
                                        riak_value_p = &_riak_value_snappy;
                                    }
                                }

                                content_p->set_value(*riak_value_p);
                            }
                            msg_get_resp.SerializeToString(&client_resp_str);
                        }
                    } else {
                        fret = EBADMSG;
                        push_error = true;
                    }
                    break;
                default :
                    client_resp_str = server_resp_str;
                    break;
                }
                op_mode = OP_SEND_CLIENT;
            } else
                push_error = true;
            break;
        case OP_SEND_CLIENT :
            op_mode = OP_RECV_SERVER;
            switch (client_resp_code) {
            case RPB_LIST_KEYS_RESP :
                if (msg_listkeys_resp.ParseFromString(client_resp_str)) {
                    if (msg_listkeys_resp.has_done())
                        if (msg_listkeys_resp.done())
                            op_mode = OP_RECV_CLIENT;
                } else {
                    fret = EBADMSG;
                    push_error = true;
                }
                break;
            case RPB_MAPRED_RESP :
                if (msg_mapred_resp.ParseFromString(client_resp_str)) {
                    if (msg_mapred_resp.has_done())
                        if (msg_mapred_resp.done())
                            op_mode = OP_RECV_CLIENT;
                } else {
                    fret = EBADMSG;
                    push_error = true;
                }
                break;
            case RPB_INDEX_RESP :
                if (msg_index_resp.ParseFromString(client_resp_str)) {
                    if (msg_index_resp.has_done())
                        if (msg_index_resp.done())
                            op_mode = OP_RECV_CLIENT;
                } else {
                    fret = EBADMSG;
                    push_error = true;
                }
                break;
            default :
                op_mode = OP_RECV_CLIENT;
                break;
            }
            if (!fret)
                fret = client_conn.ProtoSend(client_resp_code, &client_resp_str);
            break;
        default :
            fret = EFAULT;
            break;
        }

        if (fret && push_error) {
            client_resp_str = mkriakerr(fret);
            client_conn.ProtoSend(RPB_ERROR_RESP, &client_resp_str);
        }
    }

    DPRINTF("Client %s:%s dis-connected.\n", client_host.c_str(), client_srv.c_str());

    return fret;
}

std::string
get_convert(const std::string &bucket, const std::string &key, const std::string &queryparams,
	const std::string &value_in)
{
    std::string value_param;
    const std::string *value_ret_p = &value_in;
    bool value_param_ok = false;
    bool show_detail = false;
    double enter_time = 0.0;

    if (CR_Class_NS::get_debug_level() >= 35) {
        show_detail = true;
        enter_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
    }

    if (queryparams.size() > 1) {
        IBRiakDataPackQueryParam msg_ibqp;
        try {
            if (msg_ibqp.ParseFromString(queryparams)) {
                switch (msg_ibqp.pack_type()) {
                case IBRiakDataPackQueryParam::PACK_NUM_UNSIGNED :
                case IBRiakDataPackQueryParam::PACK_NUM_SIGNED :
                case IBRiakDataPackQueryParam::PACK_FLOAT :
                    {
                        DPRINTF("pack_type == PackN\n");
                        IBDataPack_N pack(value_in);
                        pack.Save(value_param, msg_ibqp);
                        value_param_ok = true;
                    }
                    break;
                case IBRiakDataPackQueryParam::PACK_STRING :
                    {
                        DPRINTF("pack_type == PackS\n");
                        IBDataPack_S pack(value_in);
                        pack.Save(value_param, msg_ibqp);
                        value_param_ok = true;
                    }
                    break;
                case IBRiakDataPackQueryParam::PACK_BINARY :
                    {
                        //todo : pack_type binary
                        DPRINTF("pack_type == PackB\n");
                    }
                    break;
                default :
                    break;
                }
            }
        } catch (int e) {
            DPRINTF("GET do-queryparams failed, code=%d, msg=%s\n", e, CR_Class_NS::strerror(e));
        } catch (const char *msg) {
            DPRINTF("GET do-queryparams failed:'%s', params:\n---\n%s\n---\n,riak_key:%s\n",
              msg,msg_ibqp.ShortDebugString().c_str(),key.c_str());
        } catch (std::string e) {
            DPRINTF("GET do-queryparams failed, msg=%s\n", e.c_str());
        } catch (...) {
            DPRINTF("GET do-queryparams failed, unknown error.\n");
        }
    }

    if (value_param_ok)
        value_ret_p = &value_param;

    if (show_detail) {
        double proc_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - enter_time;
        DPRINTF("value_in.size()==%lu, value_out.size()==%lu, processing_time==%f\n",
          value_in.size(), value_ret_p->size(), proc_time);
    }

    return *value_ret_p;
}
