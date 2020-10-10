#include <cr_class/cr_addon.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <netdb.h>
#include <errno.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/param.h>
#include <netinet/in.h>

#include <cr_class/cr_simplecomm.h>

bool
CR_SimpleComm::_do_init()
{
    this->_connect_timeout = 0.0;

    this->s = -1;
    this->_sock_type = AF_UNSPEC;

    this->_timeout_send = 60;
    this->_timeout_recv = 60;

    this->_chkstr_p = NULL;
    this->_tag = NULL;

    return true;
}

CR_SimpleComm::CR_SimpleComm()
{
    this->_do_init();
}

CR_SimpleComm::CR_SimpleComm(const int _sock)
{
    this->_do_init();

    if (_sock >= 0)
        this->s = _sock;
}

CR_SimpleComm::~CR_SimpleComm()
{
    this->DisConnect();
    if (this->_chkstr_p)
        delete this->_chkstr_p;
}

int
CR_SimpleComm::get_timeout_send()
{
    return this->_timeout_send;
}

int
CR_SimpleComm::get_timeout_recv()
{
    return this->_timeout_recv;
}

void
CR_SimpleComm::set_timeout_send(int timeout)
{
    this->_timeout_send = timeout;
}

void
CR_SimpleComm::set_timeout_recv(int timeout)
{
    this->_timeout_recv = timeout;
}

int
CR_SimpleComm::get_sock_type()
{
    return this->_sock_type;
}

std::string
CR_SimpleComm::get_hostname()
{
    return this->_hostname;
}

std::string
CR_SimpleComm::get_srvname()
{
    return this->_srvname;
}

int
CR_SimpleComm::set_chkstr(const std::string &chkstr)
{
    int ret = 0;

    this->_lock.lock();

    if (this->_chkstr_p == NULL) {
        this->_chkstr_p = new std::string(chkstr);
    } else {
        ret = EALREADY;
    }

    this->_lock.unlock();

    return ret;
}

void
CR_SimpleComm::set_tag(void *p)
{
    this->_lock.lock();

    if (!this->_tag)
        this->_tag = p;

    this->_lock.unlock();
}

void *
CR_SimpleComm::get_tag()
{
    return this->_tag;
}

bool
CR_SimpleComm::IsConnected()
{
    bool ret = false;

    this->_lock.lock();

    if (this->s >= 0)
        ret = true;

    this->_lock.unlock();

    return ret;
}

void
CR_SimpleComm::DisConnect()
{
    this->_lock.lock();

    if (this->s >= 0)
        close(this->s);

    this->s = -1;

    this->_lock.unlock();
}

int
CR_SimpleComm::_do_reconnect()
{
    int sock = -1;
    double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + this->_connect_timeout;

    switch (this->_sock_type) {
    case AF_UNIX :
        sock = this->_do_connect(this->_hostname);
        break;
    case AF_INET :
        while (1) {
            sock = this->_do_connect(this->_hostname, this->_srvname);
            if ((sock >= 0) || (CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) > exp_time))
                break;
            sleep(1);
        }
        break;
    default :
        break;
    }

    if (sock >= 0) {
        if (this->s >= 0)
            close(this->s);
        this->s = sock;
    }

    if (sock >= 0)
        return 0;

    return (0 - sock);
}

int
CR_SimpleComm::ReConnect()
{
    int ret;

    this->_lock.lock();

    ret = this->_do_reconnect();

    this->_lock.unlock();

    return ret;
}

int
CR_SimpleComm::_do_connect(const std::string &sockfilename)
{
    static const struct timeval tv = {0, 500000};
    int save_errno = 0;
    int sock;
    struct sockaddr_un stConverter;

    if (sockfilename.size() == 0) {
        DPRINTFX(15, "sockfilename.size() == 0\n");
        return (0 - EINVAL);
    }

    sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock >= 0) {
        stConverter.sun_family = AF_UNIX;
        CR_Class_NS::strlcpy(stConverter.sun_path, sockfilename.c_str(), sizeof(stConverter.sun_path));
        if (connect(sock, (struct sockaddr *)(&stConverter), sizeof(stConverter)) != 0) {
            save_errno = errno;
            close(sock);
            sock = -1;
        }
    } else {
        save_errno = errno;
    }

    if ((sock >= 0) && (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO,  &tv, sizeof(tv)) < 0)) {
        save_errno = errno;
        close(sock);
        sock = -1;
    }

    if ((sock >= 0) && (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO,  &tv, sizeof(tv)) < 0)) {
        save_errno = errno;
        close(sock);
        sock = -1;
    }

    if (sock < 0) {
        if (save_errno > 0) {
            sock = (0 - save_errno);
            errno = save_errno;
        } else {
            sock = (0 - EFAULT);
            errno = EFAULT;
        }
    }

    return sock;
}

int
CR_SimpleComm::_do_connect(const std::string &hostname, const std::string &srvname)
{
    static const struct timeval tv = {0, 500000};
    struct addrinfo hints, *res, *res0;
    int error;
    int save_errno = 0;
    int sock = -1;

    if ((hostname.size() == 0) || (srvname.size() == 0)) {
        DPRINTFX(15, "hostname.size() == %ld, srvname.size() == %ld\n", hostname.size(), srvname.size());
        return (0 - EINVAL);
    }

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    error = getaddrinfo(hostname.c_str(), srvname.c_str(), &hints, &res0);
    if (error) {
        if (error > 0) {
            error = 0 - error;
        }
        return error;
    }

    for (res = res0; res; res = res->ai_next) {
        sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
        if (sock < 0)
            continue;
        if (connect(sock, res->ai_addr, res->ai_addrlen) < 0) {
            save_errno = errno;
            close(sock);
            sock = -1;
            continue;
        }

        break;  /* okay we got one */
    }

    freeaddrinfo(res0);

    if ((sock >= 0) && (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO,  &tv, sizeof(tv)) < 0)) {
        save_errno = errno;
        DPRINTFX(15, "setsockopt(%d, SOL_SOCKET, SO_SNDTIMEO) failed[%s], fd closed\n",
          sock, CR_Class_NS::strerrno(save_errno));
        close(sock);
        sock = -1;
    }

    if ((sock >= 0) && (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO,  &tv, sizeof(tv)) < 0)) {
        save_errno = errno;
        DPRINTFX(15, "setsockopt(%d, SOL_SOCKET, SO_RCVTIMEO) failed[%s], fd closed\n",
          sock, CR_Class_NS::strerrno(save_errno));
        close(sock);
        sock = -1;
    }

    if (sock < 0) {
        if (save_errno > 0) {
            sock = (0 - save_errno);
            errno = save_errno;
        } else {
            sock = (0 - EFAULT);
            errno = EFAULT;
        }
    }

    return sock;
}

int
CR_SimpleComm::Connect(const std::string &sockfilename)
{
    int sock;

    this->_lock.lock();

    sock = this->_do_connect(sockfilename);

    if (sock >= 0) {
        this->DisConnect();
        this->s = sock;
        this->_sock_type = AF_UNIX;
        this->_hostname = sockfilename;
    }

    this->_lock.unlock();

    if (sock >= 0)
        return 0;

    return (0 - sock);
}

int
CR_SimpleComm::Connect(const std::string &hostname, const std::string &srvname, const double timeout_sec)
{
    int sock;
    double exp_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) + timeout_sec;

    this->_lock.lock();

    while (1) {
        double cur_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
        sock = this->_do_connect(hostname, srvname);
        if (sock < 0) {
            DPRINTFX(15, "Connect(\"%s\", \"%s\", %f) == %d[%s], cur_time == %f, exp_time == %f\n",
              hostname.c_str(), srvname.c_str(), timeout_sec, sock, CR_Class_NS::strerrno(0 - sock),
              cur_time, exp_time);
        } else {
            DPRINTFX(20, "Connect(\"%s\", \"%s\", %f) == %d\n", hostname.c_str(), srvname.c_str(),
              timeout_sec, sock);
        }
        if ((sock >= 0) || (cur_time > exp_time))
            break;
        sleep(1);
    }

    if (sock >= 0) {
        this->DisConnect();
        this->_connect_timeout = timeout_sec;
        this->s = sock;
        this->_sock_type = AF_INET;
        this->_hostname = hostname;
        this->_srvname = srvname;
    }

    this->_lock.unlock();

    if (sock >= 0)
        return 0;

    return (0 - sock);
}

int32_t
CR_SimpleComm::GetSockPort(std::string *sock_addr)
{
    int32_t ret = 0;

    this->_lock.lock();

    ret = CR_Class_NS::getsockport(this->s, sock_addr);

    this->_lock.unlock();

    return ret;
}

int
CR_SimpleComm::_do_proto_send(const int8_t CmdCode, const std::string *sToOpposite)
{
    int ret = 0;

    if (this->s >= 0) {
        if (sToOpposite) {
            ret = CR_Class_NS::fput_block(this->s, CmdCode,
              sToOpposite->data(), sToOpposite->size(), CR_BLOCKOP_FLAG_ISSOCKFD,
              this->_timeout_send, this->_chkstr_p);
        } else {
            ret = CR_Class_NS::fput_block(this->s, CmdCode,
              NULL, 0, CR_BLOCKOP_FLAG_ISSOCKFD, this->_timeout_send, this->_chkstr_p);
        }
    } else {
        ret = ENOTCONN;
    }

    return ret;
}

int
CR_SimpleComm::ProtoSend(const int8_t CmdCode, const std::string *sToOpposite)
{
    int ret = 0;

    this->_lock.lock();

    ret = this->_do_proto_send(CmdCode, sToOpposite);

    this->_lock.unlock();

    return ret;
}

int
CR_SimpleComm::_do_proto_recv(int8_t& CmdCode, std::string *sFromOpposite)
{
    int ret = 0;
    void *new_buf = NULL;
    uint32_t new_size = 0;
    int8_t msg_code_local = 0;

    if (this->s >= 0) {
        new_buf = CR_Class_NS::fget_block(this->s, msg_code_local, NULL, new_size,
          CR_BLOCKOP_FLAG_ISSOCKFD, this->_timeout_recv, this->_chkstr_p);
    } else {
        errno = ENOTCONN;
    }

    if (new_buf) {
        if (sFromOpposite) {
            sFromOpposite->assign((char*)new_buf, new_size);
        }
        CmdCode = msg_code_local;
        ::free(new_buf);
    } else
        ret = errno;

    return ret;
}

int
CR_SimpleComm::ProtoRecv(int8_t& CmdCode, std::string *sFromOpposite)
{
    int ret = 0;

    this->_lock.lock();

    ret = this->_do_proto_recv(CmdCode, sFromOpposite);

    this->_lock.unlock();

    return ret;
}

int
CR_SimpleComm::SendAndRecv(const int8_t CmdCodeToOpposite, const std::string *sToOpposite,
    int8_t &CmdCodeFromOpposite, std::string *sFromOpposite, const size_t retry_count)
{
    int ret;

    this->_lock.lock();

    if (this->s >= 0) {
        for (size_t i=0; i<=retry_count; i++) {
            ret = this->_do_proto_send(CmdCodeToOpposite, sToOpposite);
            if (!ret) {
                ret = this->_do_proto_recv(CmdCodeFromOpposite, sFromOpposite);
                if (!ret)
                    break;
            }
            this->_do_reconnect();
        }
    } else
        ret = ENOTCONN;

    this->_lock.unlock();

    return ret;
}

int
CR_SimpleComm::_do_proto_send_FLS(const int8_t CmdCode, CR_FixedLinearStorage *fls_to)
{
    int ret = 0;

    if (this->s >= 0) {
        ret = fls_to->SaveToFileDescriptor(this->s, 0, NULL, true, true, this->_timeout_send);
    } else {
        ret = ENOTCONN;
    }

    return ret;
}

int
CR_SimpleComm::ProtoSendFLS(const int8_t CmdCode, CR_FixedLinearStorage *fls_to)
{
    if (!fls_to)
        return EFAULT;

    int ret = 0;

    this->_lock.lock();

    ret = this->_do_proto_send_FLS(CmdCode, fls_to);

    this->_lock.unlock();

    return ret;
}

int
CR_SimpleComm::_do_proto_recv_FLS(int8_t& CmdCode, CR_FixedLinearStorage *fls_from)
{
    int ret = 0;

    if (this->s >= 0) {
        ret = fls_from->LoadFromFileDescriptor(this->s, this->_timeout_send);
    } else {
        ret = ENOTCONN;
    }

    return ret;
}

int
CR_SimpleComm::ProtoRecvFLS(int8_t& CmdCode, CR_FixedLinearStorage *fls_from)
{
    if (!fls_from)
        return EFAULT;

    int ret = 0;

    this->_lock.lock();

    ret = this->_do_proto_recv_FLS(CmdCode, fls_from);

    this->_lock.unlock();

    return ret;
}

int
CR_SimpleComm::SendAndRecvFLS(const int8_t CmdCodeToOpposite, CR_FixedLinearStorage *fls_to,
    int8_t &CmdCodeFromOpposite, CR_FixedLinearStorage *fls_from, const size_t retry_count)
{
    int ret;

    this->_lock.lock();

    if (this->s >= 0) {
        for (size_t i=0; i<=retry_count; i++) {
            ret = this->_do_proto_send_FLS(CmdCodeToOpposite, fls_to);
            if (!ret) {
                ret = this->_do_proto_recv_FLS(CmdCodeFromOpposite, fls_from);
                if (!ret)
                    break;
            }
            this->_do_reconnect();
        }
    } else
        ret = ENOTCONN;

    this->_lock.unlock();

    return ret;
}

int
CR_SimpleComm::_do_proto_send_msg(const int8_t CmdCode, const ::google::protobuf::Message *msgToOpposite)
{
    int ret = 0;

    if (this->s >= 0) {
        ret = CR_Class_NS::fput_protobuf_msg(this->s, CmdCode,
          msgToOpposite, CR_BLOCKOP_FLAG_ISSOCKFD, this->_timeout_send, this->_chkstr_p);
    } else {
        ret = ENOTCONN;
    }

    return ret;
}

int
CR_SimpleComm::ProtoSendMsg(const int8_t CmdCode, const ::google::protobuf::Message *msgToOpposite)
{
    int ret = 0;

    this->_lock.lock();

    ret = this->_do_proto_send_msg(CmdCode, msgToOpposite);

    this->_lock.unlock();

    return ret;
}

int
CR_SimpleComm::_do_proto_recv_msg(int8_t& CmdCode, ::google::protobuf::Message *msgFromOpposite,
    std::string *err_str_p)
{
    int ret = 0;

    if (this->s >= 0) {
        ret = CR_Class_NS::fget_protobuf_msg(this->s, CmdCode, msgFromOpposite, err_str_p,
          CR_BLOCKOP_FLAG_ISSOCKFD, this->_timeout_recv, this->_chkstr_p);
    } else {
        ret = ENOTCONN;
    }

    return ret;
}

int
CR_SimpleComm::ProtoRecvMsg(int8_t& CmdCode, ::google::protobuf::Message *msgFromOpposite,
    std::string *err_str_p)
{
    int ret = 0;

    this->_lock.lock();

    ret = this->_do_proto_recv_msg(CmdCode, msgFromOpposite, err_str_p);

    this->_lock.unlock();

    return ret;
}

int
CR_SimpleComm::SendAndRecvMsg(const int8_t CmdCodeToOpposite, const ::google::protobuf::Message *msgToOpposite,
    int8_t &CmdCodeFromOpposite, ::google::protobuf::Message *msgFromOpposite,
    std::string *err_str_p, const size_t retry_count)
{
    int ret;

    this->_lock.lock();

    if (this->s >= 0) {
        for (size_t i=0; i<=retry_count; i++) {
            ret = this->_do_proto_send_msg(CmdCodeToOpposite, msgToOpposite);
            if (!ret) {
                ret = this->_do_proto_recv_msg(CmdCodeFromOpposite, msgFromOpposite, err_str_p);
                if (!ret)
                    break;
            }
            this->_do_reconnect();
        }
    } else
        ret = ENOTCONN;

    this->_lock.unlock();

    return ret;
}

void
CR_SimpleComm_TF::SendAndRecv(CR_SimpleComm *comm_p, int *comm_ret,
    const int8_t CmdCodeToOpposite, const std::string *sToOpposite,
    int8_t *CmdCodeFromOpposite, std::string *sFromOpposite, const size_t retry_count)
{
    if (!comm_p)
        return;
    *comm_ret = comm_p->SendAndRecv(CmdCodeToOpposite,
      sToOpposite, *CmdCodeFromOpposite, sFromOpposite, retry_count);
}

void
CR_SimpleComm_TF::SendAndRecvMsg(CR_SimpleComm *comm_p, int *comm_ret,
    const int8_t CmdCodeToOpposite, const ::google::protobuf::Message *msgToOpposite,
    int8_t *CmdCodeFromOpposite, ::google::protobuf::Message *msgFromOpposite,
    std::string *err_str_p, const size_t retry_count)
{
    if (!comm_p)
        return;
    *comm_ret = comm_p->SendAndRecvMsg(CmdCodeToOpposite,
      msgToOpposite, *CmdCodeFromOpposite, msgFromOpposite, err_str_p, retry_count);
}
