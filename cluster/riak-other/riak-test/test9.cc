#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <thread>
#include <cr_class/cr_class.h>
#include <cr_class/cr_simpleserver.h>
#include <cr_class/cr_simplecomm.h>

void help_out(int argc, char *argv[]);

void
help_out(int argc, char *argv[])
{
    fprintf(stderr,
      "Usage: %s put <file name> <Y | N> <data string>\n"
      "Usage: %s get <file name>\n"
      "Usage: %s protobuf <filename> <loop>\n"
      "Usage: %s protobuf-net <filename> <loop>\n"
      , argv[0], argv[0], argv[0], argv[0]
    );

    exit(1);
}

static void
_sender(std::string sock_filename, const int loop)
{
    int fret;
    CR_SimpleComm comm;
    msgCRError msg_out;

    fret = comm.Connect(sock_filename);
    if (fret) {
        fprintf(stderr, "Conn to server failed[%s]\n", CR_Class_NS::strerrno(fret));
        return;
    }

    for (int i=0; i<loop; i++) {
        msg_out.set_errmsg(CR_Class_NS::randstr());
        msg_out.set_errcode(rand());
        comm.ProtoSendMsg(i, &msg_out);
    }
}

int
main(int argc, char *argv[])
{
    int fd = -1;
    int fret = 0;
    std::string data_str;
    bool do_crc = false;
    std::string file_name;
    int8_t msg_code;

    if (argc < 2) {
        help_out(argc, argv);
    }

    double enter_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

    if (strcmp(argv[1], "put") == 0) {
        if (argc < 5) {
            help_out(argc, argv);
        }

        file_name = argv[2];
        data_str = argv[4];

        if (argv[3][0] == 'Y')
            do_crc = true;

        fd = creat(file_name.c_str(), 0644);

        if (fd < 0) {
            fprintf(stderr, "open failed[%s]\n", CR_Class_NS::strerrno(errno));
            return errno;
        }

        fret = CR_Class_NS::fput_str(fd, 0x55, data_str, do_crc);
    } else if (strcmp(argv[1], "get") == 0) {
        if (argc < 3) {
            help_out(argc, argv);
        }

        file_name = argv[2];
        fd = open(file_name.c_str(), O_RDONLY);

        fret = CR_Class_NS::fget_str(fd, msg_code, data_str);
        if (!fret) {
            printf("size=%llu\nmsg_code=%X\n%s\n",
              (long long unsigned)data_str.size(), msg_code, data_str.c_str());
        }
    } else if (strcmp(argv[1], "protobuf") == 0) {
        if (argc < 4) {
            help_out(argc, argv);
        }

        file_name = argv[2];
        fd = open(file_name.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);

        int loop = atoi(argv[3]);
        msgCRError msg_out;
        msgCRError msg_in;

        for (int i=0; i<loop; i++) {
            msg_out.set_errmsg(CR_Class_NS::randstr());
            msg_out.set_errcode(rand());
            CR_Class_NS::fput_protobuf_msg(fd, i, &msg_out);
        }

        close(fd);
        fd = open(file_name.c_str(), O_RDONLY);

        for (int i=0; i<loop; i++) {
            fret = CR_Class_NS::fget_protobuf_msg(fd, msg_code, &msg_in);
            if (fret) {
                fprintf(stderr, "fget_protobuf_msg failed[%s]\n", CR_Class_NS::strerrno(fret));
                break;
            }
            fprintf(stderr, "[%08X], msg[%08X]=\"%s\"\n", msg_code, msg_in.errcode(), msg_in.errmsg().c_str());
        }
    } else if (strcmp(argv[1], "protobuf-net") == 0) {
        if (argc < 4) {
            help_out(argc, argv);
        }

        file_name = argv[2];
        int loop = atoi(argv[3]);
        CR_SimpleServer server;

        fret = server.Listen(file_name);
        if (fret) {
            fprintf(stderr, "listen failed[%s]\n", CR_Class_NS::strerrno(fret));
            return fret;
        }

        std::thread th(_sender, file_name, loop);

        int client_fd;

        fret = server.Accept(client_fd);
        if (fret) {
            fprintf(stderr, "accept failed[%s]\n", CR_Class_NS::strerrno(fret));
            return fret;
        }

        CR_SimpleComm comm(client_fd);

        msgCRError msg_in;

        do {
            fret = comm.ProtoRecvMsg(msg_code, &msg_in);
            if (fret) {
                fprintf(stderr, "ProtoRecv failed[%s]\n", CR_Class_NS::strerrno(fret));
                break;
            }
            DPRINTF("[%08X]:msg[%08X]=\"%s\"\n", msg_code, msg_in.errcode(), msg_in.errmsg().c_str());
        } while (!fret);

        th.join();
    }

    if (fd >= 0)
        close(fd);

    printf("%s, time usage: %f seconds\n", CR_Class_NS::strerrno(fret),
      CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - enter_time);

    return fret;
}
