#ifndef __CR_SIMPLESERVER_H__
#define __CR_SIMPLESERVER_H__

#include <unistd.h>
#include <string>
#include <map>
#include <thread>
#include <cr_class/cr_datacontrol.h>

class CR_SimpleServer {
public:
	typedef int(*on_accept_func_t)(const int sockfd, const std::string &client_host,
		const std::string &client_srv, void *on_accept_param_p);

	typedef int(*on_async_recv_func_t)(void *server_private_p, void *connect_private_p,
		const std::string &client_host, const std::string &client_srv,
		const int8_t cmd_code_from_client, const std::string &data_from_client,
		int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg);

	typedef void*(*on_async_recv_init_func_t)(void *server_private_p,
		const std::string &client_host, const std::string &client_srv, std::string *&chkstr_p);

	typedef void(*on_async_recv_clear_func_t)(void *server_private_p,
		const std::string &client_host, const std::string &client_srv,
		void *p, const int last_errno);

	typedef int(*on_async_recv_error_func_t)(const std::string &client_host,
		const std::string &client_srv, const int outer_errcode, const std::string &outer_errmsg,
		int8_t &cmd_code_to_client, std::string &data_to_client);

	CR_SimpleServer();
	~CR_SimpleServer();

	void Close();

	int Listen(const std::string &sockfilename, int backlog=10, double timeout_sec=1.0,
		bool force_listen=false);
	int Listen(const std::string &addr, const std::string &srv, int backlog=10, double timeout_sec=1.0,
		bool force_listen=false);

	int32_t GetSockPort(std::string *sock_addr=NULL);

	int Accept(int &client_fd, std::string *client_host=NULL, std::string *client_srv=NULL,
		const std::string &key_str="");
	int AsyncAcceptStart(on_accept_func_t on_accept_func, void *on_accept_param_p=NULL);
	int AsyncAcceptStop(void **on_accept_param_pp=NULL);

	int AsyncRecvStart(std::map<int8_t,on_async_recv_func_t> &recv_func_map,
		on_async_recv_func_t default_recv_func,
		on_async_recv_init_func_t on_async_recv_init,
		on_async_recv_clear_func_t on_async_recv_clear,
		on_async_recv_error_func_t on_async_recv_error,
		void *server_private_p=NULL, const int default_errno=EPROTONOSUPPORT);
	int AsyncRecvStop();

private:
	CR_DataControl::Mutex _mutex;

	int _sock_type;
        std::string _addr;
        std::string _srv;

	int s;

	std::thread *_accept_tp;
        bool _accept_stop;
	std::string _accept_key;
	void *_accept_param_p;

	std::map<int8_t,on_async_recv_func_t> _recv_func_map;
};

#endif /* __CR_SIMPLESERVER_H__ */
