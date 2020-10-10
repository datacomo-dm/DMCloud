#ifndef __H_CR_SIMPLECOMM_H__
#define __H_CR_SIMPLECOMM_H__

#include <cr_class/cr_addon.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/param.h>
#include <string>
#include <cr_class/cr_class.h>
#include <cr_class/cr_datacontrol.h>
#include <cr_class/cr_fixedlinearstorage.h>
#include <cr_class/cr_class.pb.h>

class CR_SimpleComm {
public :
	CR_SimpleComm();
	CR_SimpleComm(const int _sock);

	~CR_SimpleComm();

	int Connect(const std::string &sockfilename);
	int Connect(const std::string &hostname, const std::string &srvname, const double timeout_sec=120.0);

	void DisConnect();
	int ReConnect();
	bool IsConnected();

	int32_t GetSockPort(std::string *sock_addr=NULL);

	int ProtoSend(const int8_t CmdCode, const std::string *sToOpposite);
	int ProtoRecv(int8_t& CmdCode, std::string *sFromOpposite);
	int SendAndRecv(const int8_t CmdCodeToOpposite, const std::string *sToOpposite,
		int8_t &CmdCodeFromOpposite, std::string *sFromOpposite, const size_t retry_count=0);

	int ProtoSendFLS(const int8_t CmdCode, CR_FixedLinearStorage *fls_to);
	int ProtoRecvFLS(int8_t& CmdCode, CR_FixedLinearStorage *fls_from);
	int SendAndRecvFLS(const int8_t CmdCodeToOpposite, CR_FixedLinearStorage *fls_to,
		int8_t &CmdCodeFromOpposite, CR_FixedLinearStorage *fls_from, const size_t retry_count=0);

	int ProtoSendMsg(const int8_t CmdCode, const ::google::protobuf::Message *msgToOpposite);
	int ProtoRecvMsg(int8_t& CmdCode, ::google::protobuf::Message *msgFromOpposite,
		std::string *err_str_p=NULL);
	int SendAndRecvMsg(const int8_t CmdCodeToOpposite, const ::google::protobuf::Message *msgToOpposite,
		int8_t &CmdCodeFromOpposite, ::google::protobuf::Message *msgFromOpposite,
		std::string *err_str_p=NULL, const size_t retry_count=0);

	int set_chkstr(const std::string &chkstr);

	void set_tag(void *p);
	void *get_tag();

	int get_timeout_send();
	int get_timeout_recv();

	void set_timeout_send(int timeout);
	void set_timeout_recv(int timeout);

	int get_sock_type();
	std::string get_hostname();
	std::string get_srvname();

private :
	CR_DataControl::Mutex _lock;

	double _connect_timeout;

	std::string *_chkstr_p;

	int _sock_type;
	std::string _hostname;
	std::string _srvname;

	int s;

	void *_tag;

	int _timeout_send;
	int _timeout_recv;

	bool _do_init();

	int _do_connect(const std::string &sockfilename);
	int _do_connect(const std::string &hostname, const std::string &srvname);

	int _do_reconnect();

	int _do_proto_send(const int8_t CmdCode, const std::string *sToOpposite);
	int _do_proto_recv(int8_t& CmdCode, std::string *sFromOpposite);

	int _do_proto_send_FLS(const int8_t CmdCode, CR_FixedLinearStorage *fls_to);
	int _do_proto_recv_FLS(int8_t& CmdCode, CR_FixedLinearStorage *fls_from);

	int _do_proto_send_msg(const int8_t CmdCode, const ::google::protobuf::Message *msgToOpposite);
	int _do_proto_recv_msg(int8_t& CmdCode, ::google::protobuf::Message *msgFromOpposite,
		std::string *err_str_p);
};

namespace CR_SimpleComm_TF {
	void SendAndRecv(CR_SimpleComm *comm_p, int *comm_ret,
		const int8_t CmdCodeToOpposite, const std::string *sToOpposite,
		int8_t *CmdCodeFromOpposite, std::string *sFromOpposite, const size_t retry_count);

	void SendAndRecvMsg(CR_SimpleComm *comm_p, int *comm_ret,
		const int8_t CmdCodeToOpposite, const ::google::protobuf::Message *msgToOpposite,
		int8_t *CmdCodeFromOpposite, ::google::protobuf::Message *msgFromOpposite,
		std::string *err_str_p, const size_t retry_count);
};

#endif /* __H_CR_SIMPLECOMM_H__ */
