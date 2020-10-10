#include <cr_class/cr_addon.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/param.h>
#include <sys/user.h>
#include <sys/time.h>
#include <sys/types.h>
#include <signal.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_simpleserver.h>
#include <cr_class/cr_simplecomm.h>
#include <cr_class/cr_quickhash.h>

#define _CR_SIMPLESERVER_PTR_NAME		"SIMPLESERVER_PTR"
#define _CR_SIMPLESERVER_CLIENT_SOCKFD_NAME	"CLIENT_SOCKFD"
#define _CR_SIMPLESERVER_CLIENT_HOST_NAME	"CLIENT_HOST"
#define _CR_SIMPLESERVER_CLIENT_PORT_NAME	"CLIENT_PORT"

#define _CR_SIMPLESERVER_ON_ACCEPT_PTR_NAME	"ON_ACCEPT_PTR"
#define _CR_SIMPLESERVER_ON_ACCEPT_PARAMS_NAME	"ON_ACCEPT_PARAMS"

#define _ASYNC_RECV_PARAMS_FUNC_MAP_P		"ON_ASYNC_RECV_FUNC_MAP_PTR"
#define _ASYNC_RECV_PARAMS_INIT_PTR_NAME	"ON_ASYNC_RECV_INIT_PTR"
#define _ASYNC_RECV_PARAMS_CLEAR_PTR_NAME	"ON_ASYNC_RECV_CLEAR_PTR"
#define _ASYNC_RECV_PARAMS_ERROR_PTR_NAME	"ON_ASYNC_RECV_ERROR_PTR"
#define _ASYNC_RECV_PARAMS_SERVER_PRIVATE_P	"ON_ASYNC_RECV_SERVER_PRIVATE_PTR"
#define _ASYNC_RECV_PARAMS_DEFAULT_ERRNO	"ON_ASYNC_RECV_DEFAULT_ERRNO"

///////////////////////////////

typedef struct {
    int default_errno;
    CR_SimpleServer::on_async_recv_init_func_t on_async_recv_init;
    CR_SimpleServer::on_async_recv_clear_func_t on_async_recv_clear;
    CR_SimpleServer::on_async_recv_error_func_t on_async_recv_error;
    void *server_private_p;
    std::map<int8_t,CR_SimpleServer::on_async_recv_func_t> recv_func_map;
    CR_SimpleServer::on_async_recv_func_t default_recv_func;
} async_recv_param_t;

///////////////////////////////

static void _on_accept_caller(CR_SimpleServer::on_accept_func_t on_accept,
    void *on_accept_param_p, int client_fd,
    const std::string client_host, const std::string client_srv);
static void _do_async_accept(CR_SimpleServer *server_p, const std::string accept_key,
    CR_SimpleServer::on_accept_func_t on_accept_func,
    void *on_accept_param_p, volatile bool *stop_p);
static int async_recv_on_accept(const int sockfd, const std::string &client_host,
    const std::string &client_srv, void *on_accept_param_p);
static double _params_dealloc(void *p);

///////////////////////////////

static double
_params_dealloc(void *p)
{
    async_recv_param_t *params_p = (async_recv_param_t*)p;

    if (params_p)
        delete params_p;

    return 0;
}

static int
async_recv_on_accept(const int sockfd, const std::string &client_host,
    const std::string &client_srv, void *on_accept_param_p)
{
    if (!on_accept_param_p)
        return EFAULT;

    int ret = 0;
    int8_t cmd_recv, cmd_send;
    std::string s_recv, s_send, s_errmsg;
    async_recv_param_t params = *((async_recv_param_t*)on_accept_param_p);
    CR_SimpleServer::on_async_recv_func_t on_recv_func = NULL;
    void *connect_private_p = NULL;
    std::map<int8_t,CR_SimpleServer::on_async_recv_func_t>::iterator recv_func_map_it;

    CR_SimpleComm client_connect(sockfd);
    std::string chkstr;
    std::string *chkstr_p = &chkstr;

    chkstr.clear();
    client_connect.set_timeout_recv(INT_MAX);

    if (params.on_async_recv_init) {
        connect_private_p = params.on_async_recv_init(params.server_private_p,
          client_host, client_srv, chkstr_p);
        if (chkstr_p) {
            client_connect.set_chkstr(chkstr);
        }
    }

    while (1) {
        DPRINTFX(40, "[fd:%d]ProtoRecv(...)\n", sockfd);
        ret = client_connect.ProtoRecv(cmd_recv, &s_recv);
        DPRINTFX(40, "[fd:%d]ProtoRecv(%d), %s\n", sockfd, cmd_recv, CR_Class_NS::strerrno(ret));
        if (ret)
            break;

        recv_func_map_it = params.recv_func_map.find(cmd_recv);
        if (recv_func_map_it != params.recv_func_map.end()) {
            on_recv_func = recv_func_map_it->second;
        } else {
            on_recv_func = params.default_recv_func;
        }

        s_errmsg.clear();

        if (on_recv_func) {
            cmd_send = cmd_recv + 1;
            ret = on_recv_func(params.server_private_p, connect_private_p, client_host, client_srv,
              cmd_recv, s_recv, cmd_send, s_send, s_errmsg);
        } else {
            cmd_send = 0;
            s_send.clear();
            ret = params.default_errno;
        }

        if (!ret) {
            DPRINTFX(40, "[fd:%d]ProtoSend(%d, ...)\n", sockfd, cmd_send);
            ret = client_connect.ProtoSend(cmd_send, &s_send);
            DPRINTFX(40, "[fd:%d]ProtoSend(%d, ...), %s\n", sockfd, cmd_send, CR_Class_NS::strerrno(ret));
            if (ret)
                break;
        } else {
            if (params.on_async_recv_error) {
                ret = params.on_async_recv_error(client_host, client_srv, ret, s_errmsg, cmd_send, s_send);
                if (ret == EAGAIN) {
                    DPRINTFX(40, "[fd:%d]ProtoSend(%d, ...)\n", sockfd, cmd_send);
                    ret = client_connect.ProtoSend(cmd_send, &s_send);
                    DPRINTFX(40, "[fd:%d]ProtoSend(%d, ...), %s\n",
                      sockfd, cmd_send, CR_Class_NS::strerrno(ret));
                    if (ret)
                        break;
                } else {
                    DPRINTFX(40, "[fd:%d]ProtoSend(%d, ...)\n", sockfd, cmd_send);
                    client_connect.ProtoSend(cmd_send, &s_send);
                    DPRINTFX(40, "[fd:%d]ProtoSend(%d, ...), %s\n",
                      sockfd, cmd_send, CR_Class_NS::strerrno(ret));
                    break;
                }
            } else
                break;
        }
    }

    if (params.on_async_recv_clear) {
        params.on_async_recv_clear(params.server_private_p, client_host, client_srv, connect_private_p, ret);
    }

    return ret;
}

static void
_on_accept_caller(CR_SimpleServer::on_accept_func_t on_accept, void *on_accept_param_p,
    int client_fd, const std::string client_host, const std::string client_srv)
{
    int fret;

    DPRINTFX(20, "Connect (fd:%d) from %s:%s accepted\n",
    	client_fd, client_host.c_str(), client_srv.c_str());

    if (on_accept) {
        fret = on_accept(client_fd, client_host, client_srv, on_accept_param_p);
    } else {
        fret = EFAULT;
    }

    close(client_fd);

    DPRINTFX(20, "Connect (fd:%d) from %s:%s closed, %s\n",
    	client_fd, client_host.c_str(), client_srv.c_str(), CR_Class_NS::strerrno(fret));
}

static void
_do_async_accept(CR_SimpleServer *server_p, const std::string accept_key,
    CR_SimpleServer::on_accept_func_t on_accept_func,
    void *on_accept_param_p, volatile bool *stop_p)
{
    int fret;
    int client_fd;
    std::string client_host;
    std::string client_srv;

    while (server_p && stop_p) {
        if (*stop_p) {
            DPRINTFX(20, "Stop received, thread stop.\n");
            break;
        }

        fret = server_p->Accept(client_fd, &client_host, &client_srv, accept_key);
        if (fret) {
            if ((fret == EAGAIN) || (fret == EWOULDBLOCK)) {
                usleep(5000);
            } else {
                DPRINTFX(15, "AsyncAccept, %s\n", CR_Class_NS::strerrno(fret));
                break;
            }
        } else {
            std::thread t(_on_accept_caller, on_accept_func, on_accept_param_p,
              client_fd, client_host, client_srv);
            t.detach();
        }
    }
}

///////////////////////////////

CR_SimpleServer::CR_SimpleServer()
{
    this->s = -1;
    this->_sock_type = AF_UNSPEC;
    this->_accept_tp = NULL;
}

CR_SimpleServer::~CR_SimpleServer()
{
    this->Close();
}

void
CR_SimpleServer::Close()
{
    CANCEL_DISABLE_ENTER();
    this->_mutex.lock();

    if (this->s >= 0) {
        close(this->s);
    }

    if (this->_sock_type == AF_UNIX) {
        unlink(this->_addr.c_str());
    }

    if (this->_accept_tp) {
        this->_accept_stop = true;
        this->_accept_tp->join();
        delete this->_accept_tp;
        this->_accept_tp = NULL;
        this->_accept_key = "";
    }

    this->_mutex.unlock();
    CANCEL_DISABLE_LEAVE();
}

int
CR_SimpleServer::Listen(const std::string &sockfilename, int backlog, double timeout_sec, bool force_listen)
{
    int ret = 0;
    int fret;
    int sockfd;
    sockaddr_un server_addr;
    size_t _cplen = MIN(sizeof(server_addr.sun_path), (sockfilename.size()+1));
    double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;

    if (backlog < 1)
        backlog = 1;

    CANCEL_DISABLE_ENTER();
    this->_mutex.lock();

    do {
        if (this->s >= 0) {
            ret = EALREADY;
            break;
        }

        struct stat stat_buf;
        if (stat(sockfilename.c_str(), &stat_buf) == 0) {
            if (!S_ISSOCK(stat_buf.st_mode)) {
                ret = EEXIST;
                break;
            } else if (force_listen) {
                unlink(sockfilename.c_str());
            }
        } else if (errno != ENOENT) {
            ret = errno;
            break;
        }

        sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (sockfd < 0) {
            ret = errno;
            break;
        }

        server_addr.sun_family = AF_UNIX;
        memcpy(server_addr.sun_path, sockfilename.c_str(), _cplen);

        while ((fret = bind(sockfd, (sockaddr*)(&server_addr), sizeof(server_addr))) != 0) {
            ret = errno;
            if (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) > exp_time)
                break;
            usleep(rand() % 50000 + 50000);
        }

        if (fret == 0)
            ret = 0;

        if (ret) {
            close(sockfd);
            break;
        }

        if (listen(sockfd, backlog) != 0) {
            close(sockfd);
            ret = errno;
            break;
        }

        this->s = sockfd;
        this->_sock_type = server_addr.sun_family;
        this->_addr.assign(server_addr.sun_path, _cplen);
    } while (0);

    this->_mutex.unlock();
    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_SimpleServer::Listen(const std::string &addr, const std::string &srv,
	int backlog, double timeout_sec, bool force_listen)
{
    int ret = 0;
    int sockfd;
    int fret;
    double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;

    struct addrinfo hints;
    struct addrinfo *result, *rp;

    if (backlog < 1)
        backlog = 1;

    bzero(&hints, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    CANCEL_DISABLE_ENTER();
    this->_mutex.lock();

    do {
        if (this->s >= 0) {
            ret = EALREADY;
            break;
        }

        fret = getaddrinfo((addr.size()>0)?(addr.c_str()):(NULL), srv.c_str(), &hints, &result);
        if (fret) {
            DPRINTFX(20, "getaddrinfo(\"%s\", \"%s\") => %s\n", addr.c_str(), srv.c_str(), gai_strerror(fret));
            ret = fret;
            break;
        }

        for (rp = result; rp != NULL; rp = rp->ai_next) {

            sockfd = socket(rp->ai_family, rp->ai_socktype,
            	rp->ai_protocol);
            if (sockfd < 0)
                continue;

            while ((fret = bind(sockfd, rp->ai_addr, rp->ai_addrlen)) != 0) {
                ret = errno;
                if (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) > exp_time)
                    break;
                if (force_listen) {
                    char cmd_buf[256];
                    char result_buf[32];
                    FILE *fp = NULL;
                    int port_tmp = atoi(srv.c_str());
                    if (port_tmp > 0) {
                        snprintf(cmd_buf, sizeof(cmd_buf),
#if defined (__OpenBSD__)
                          "fstat 2>/dev/null | "
                          "grep internet | grep ':%d' | "
                          "grep -v '<--' | grep -v '\-->' | "
                          "head -1 | awk '{printf $3}'"
#else
                          "netstat -anp 2>/dev/null | "
                          "grep ':%d' | grep 'LISTEN' | "
                          "awk '{printf $7}' | cut -d '/' -f1"
#endif
                          , port_tmp);
                        fp = popen(cmd_buf, "r");
                    } else {
                        snprintf(cmd_buf, sizeof(cmd_buf),
#if defined (__OpenBSD__)
                          "fstat 2>/dev/null | "
                          "grep internet | grep ':%s' | "
                          "grep -v '<--' | grep -v '\-->' | "
                          "head -1 | awk '{printf $3}'"
#else
                          "netstat -anp 2>/dev/null | "
                          "grep ':%s' | grep 'LISTEN' | "
                          "awk '{printf $7}' | cut -d '/' -f1"
#endif
                          , srv.c_str());
                        fp = popen(cmd_buf, "r");
                    }
                    if (fp) {
                        bzero(result_buf, sizeof(result_buf));
                        size_t read_ret = fread(result_buf, 1, sizeof(result_buf) - 1, fp);
                        if (read_ret > 0) {
                            pid_t target_pid = atoll(result_buf);
                            if (target_pid > 0) {
                                DPRINTF("Address already in use by pid %ld, try kill it\n", (long)target_pid);
                                kill(target_pid, SIGKILL);
                            }
                        }
                        pclose(fp);
                    }
                } else {
                    usleep(10000);
                }
            }

            if (fret == 0)
                ret = 0;

            if (!ret)		// Success
                break;

            close(sockfd);
        }

        freeaddrinfo(result);

        if (!ret && !rp)
            ret = EAGAIN;

        if (ret)
            break;

        if (listen(sockfd, backlog) != 0) {
            close(sockfd);
            ret = errno;
            break;
        }

        this->s = sockfd;
        this->_sock_type = rp->ai_family;
        this->_addr = addr;
        this->_srv = srv;
    } while (0);

    this->_mutex.unlock();
    CANCEL_DISABLE_LEAVE();

    return ret;
}

int32_t
CR_SimpleServer::GetSockPort(std::string *sock_addr)
{
    int32_t ret = 0;

    CANCEL_DISABLE_ENTER();
    this->_mutex.lock();

    ret = CR_Class_NS::getsockport(this->s, sock_addr);

    this->_mutex.unlock();
    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_SimpleServer::Accept(int &client_fd, std::string *client_host, std::string *client_srv,
    const std::string &key_str)
{
    int ret = 0;

    CANCEL_DISABLE_ENTER();
    this->_mutex.lock();

    if (key_str == this->_accept_key) {
        if (this->s >= 0) {
            if (this->_sock_type == AF_UNIX) {
                struct sockaddr_un client_addr;
                socklen_t client_addr_len = sizeof(struct sockaddr_un);

                client_fd = accept(this->s, (sockaddr*)&client_addr, &client_addr_len);
                if (client_fd < 0)
                    ret = errno;
            } else {
                struct sockaddr client_addr;
                socklen_t client_addrlen = sizeof(struct sockaddr);
                char client_host_buf[128];
                char client_serv_buf[8];

                client_fd = accept(this->s, &client_addr, &client_addrlen);
                if (client_fd >= 0) {
                    if (client_host || client_srv) {
                        getnameinfo(&client_addr, client_addrlen,
                          client_host_buf, sizeof(client_host_buf)-1,
                          client_serv_buf, sizeof(client_serv_buf)-1,
                          NI_NUMERICHOST | NI_NUMERICSERV);
                        if (client_host)
                            *client_host = client_host_buf;
                        if (client_srv)
                            *client_srv = client_serv_buf;
                    }
                } else
                    ret = errno;
            }
        } else
            ret = ENOTCONN;
    } else
        ret = EPERM;

    this->_mutex.unlock();
    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_SimpleServer::AsyncAcceptStart(CR_SimpleServer::on_accept_func_t on_accept_func,
    void *on_accept_param_p)
{
    int ret = 0;

    CANCEL_DISABLE_ENTER();
    this->_mutex.lock();

    if (!this->_accept_tp) {
        this->_accept_key = CR_Class_NS::randstr(4);
        this->_accept_param_p = on_accept_param_p;
        this->_accept_stop = false;
        this->_accept_tp = new std::thread(_do_async_accept,
          this, this->_accept_key, on_accept_func, on_accept_param_p, &(this->_accept_stop));
    } else
        ret = EINPROGRESS;

    this->_mutex.unlock();
    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_SimpleServer::AsyncAcceptStop(void **on_accept_param_pp)
{
    int ret = 0;

    CANCEL_DISABLE_ENTER();
    this->_mutex.lock();

    if (this->_accept_tp) {
        this->_accept_stop = true;
        this->_accept_tp->join();
        delete this->_accept_tp;
        this->_accept_tp = NULL;
        if (on_accept_param_pp) {
            *on_accept_param_pp = this->_accept_param_p;
        }
        this->_accept_key = "";
    } else
        ret = EFAULT;

    this->_mutex.unlock();
    CANCEL_DISABLE_LEAVE();

    return ret;
}

int
CR_SimpleServer::AsyncRecvStart(std::map<int8_t,on_async_recv_func_t> &recv_func_map,
    on_async_recv_func_t default_recv_func, on_async_recv_init_func_t on_async_recv_init,
    on_async_recv_clear_func_t on_async_recv_clear, on_async_recv_error_func_t on_async_recv_error,
    void *server_private_p, const int default_errno)
{
    int ret;

    async_recv_param_t *params_p = new async_recv_param_t;

    if (default_errno == 0) {
        params_p->default_errno = EPROTONOSUPPORT;
    } else {
        params_p->default_errno = default_errno;
    }

    params_p->recv_func_map = recv_func_map;
    params_p->default_recv_func = default_recv_func;
    params_p->on_async_recv_init = on_async_recv_init;
    params_p->on_async_recv_clear = on_async_recv_clear;
    params_p->on_async_recv_error = on_async_recv_error;
    params_p->server_private_p = server_private_p;

    ret = this->AsyncAcceptStart(async_recv_on_accept, params_p);

    return ret;
}

int
CR_SimpleServer::AsyncRecvStop()
{
    int ret = 0;
    void *on_accept_param_p;

    ret = this->AsyncAcceptStop(&on_accept_param_p);

    CR_Class_NS::set_alarm(_params_dealloc, on_accept_param_p, 30);

    return ret;
}
