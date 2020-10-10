#include <errno.h>
#include <signal.h>
#include <string>
#include <cr_class/cr_class.h>

std::string
__cr_class_ns_str_errno(int errnum)
{
    std::string __str_tmp;

    switch (errnum) {
        case 0:
            __str_tmp = "SUCCESS";
            break;
#       ifdef E2BIG
        case E2BIG:
            __str_tmp = "E2BIG";
            break;
#       endif /* E2BIG */
#       ifdef EACCES
        case EACCES:
            __str_tmp = "EACCES";
            break;
#       endif /* EACCES */
#       ifdef EADDRINUSE
        case EADDRINUSE:
            __str_tmp = "EADDRINUSE";
            break;
#       endif /* EADDRINUSE */
#       ifdef EADDRNOTAVAIL
        case EADDRNOTAVAIL:
            __str_tmp = "EADDRNOTAVAIL";
            break;
#       endif /* EADDRNOTAVAIL */
#       ifdef EAFNOSUPPORT
        case EAFNOSUPPORT:
            __str_tmp = "EAFNOSUPPORT";
            break;
#       endif /* EAFNOSUPPORT */
#       ifdef EAGAIN
        case EAGAIN:
            __str_tmp = "EAGAIN";
            break;
#       endif /* EAGAIN */
#       ifdef EALREADY
        case EALREADY:
            __str_tmp = "EALREADY";
            break;
#       endif /* EALREADY */
#       ifdef EBADE
        case EBADE:
            __str_tmp = "EBADE";
            break;
#       endif /* EBADE */
#       ifdef EBADF
        case EBADF:
            __str_tmp = "EBADF";
            break;
#       endif /* EBADF */
#       ifdef EBADFD
        case EBADFD:
            __str_tmp = "EBADFD";
            break;
#       endif /* EBADFD */
#       ifdef EBADMSG
        case EBADMSG:
            __str_tmp = "EBADMSG";
            break;
#       endif /* EBADMSG */
#       ifdef EBADR
        case EBADR:
            __str_tmp = "EBADR";
            break;
#       endif /* EBADR */
#       ifdef EBADRQC
        case EBADRQC:
            __str_tmp = "EBADRQC";
            break;
#       endif /* EBADRQC */
#       ifdef EBADSLT
        case EBADSLT:
            __str_tmp = "EBADSLT";
            break;
#       endif /* EBADSLT */
#       ifdef EBUSY
        case EBUSY:
            __str_tmp = "EBUSY";
            break;
#       endif /* EBUSY */
#       ifdef ECANCELED
        case ECANCELED:
            __str_tmp = "ECANCELED";
            break;
#       endif /* ECANCELED */
#       ifdef ECHILD
        case ECHILD:
            __str_tmp = "ECHILD";
            break;
#       endif /* ECHILD */
#       ifdef ECHRNG
        case ECHRNG:
            __str_tmp = "ECHRNG";
            break;
#       endif /* ECHRNG */
#       ifdef ECOMM
        case ECOMM:
            __str_tmp = "ECOMM";
            break;
#       endif /* ECOMM */
#       ifdef ECONNABORTED
        case ECONNABORTED:
            __str_tmp = "ECONNABORTED";
            break;
#       endif /* ECONNABORTED */
#       ifdef ECONNREFUSED
        case ECONNREFUSED:
            __str_tmp = "ECONNREFUSED";
            break;
#       endif /* ECONNREFUSED */
#       ifdef ECONNRESET
        case ECONNRESET:
            __str_tmp = "ECONNRESET";
            break;
#       endif /* ECONNRESET */
#       ifdef EDEADLOCK
        case EDEADLOCK:
            __str_tmp = "EDEADLOCK";
            break;
#       endif /* EDEADLOCK */
#       ifdef EDESTADDRREQ
        case EDESTADDRREQ:
            __str_tmp = "EDESTADDRREQ";
            break;
#       endif /* EDESTADDRREQ */
#       ifdef EDOM
        case EDOM:
            __str_tmp = "EDOM";
            break;
#       endif /* EDOM */
#       ifdef EDQUOT
        case EDQUOT:
            __str_tmp = "EDQUOT";
            break;
#       endif /* EDQUOT */
#       ifdef EEXIST
        case EEXIST:
            __str_tmp = "EEXIST";
            break;
#       endif /* EEXIST */
#       ifdef EFAULT
        case EFAULT:
            __str_tmp = "EFAULT";
            break;
#       endif /* EFAULT */
#       ifdef EFBIG
        case EFBIG:
            __str_tmp = "EFBIG";
            break;
#       endif /* EFBIG */
#       ifdef EHOSTDOWN
        case EHOSTDOWN:
            __str_tmp = "EHOSTDOWN";
            break;
#       endif /* EHOSTDOWN */
#       ifdef EHOSTUNREACH
        case EHOSTUNREACH:
            __str_tmp = "EHOSTUNREACH";
            break;
#       endif /* EHOSTUNREACH */
#       ifdef EIDRM
        case EIDRM:
            __str_tmp = "EIDRM";
            break;
#       endif /* EIDRM */
#       ifdef EILSEQ
        case EILSEQ:
            __str_tmp = "EILSEQ";
            break;
#       endif /* EILSEQ */
#       ifdef EINPROGRESS
        case EINPROGRESS:
            __str_tmp = "EINPROGRESS";
            break;
#       endif /* EINPROGRESS */
#       ifdef EINTR
        case EINTR:
            __str_tmp = "EINTR";
            break;
#       endif /* EINTR */
#       ifdef EINVAL
        case EINVAL:
            __str_tmp = "EINVAL";
            break;
#       endif /* EINVAL */
#       ifdef EIO
        case EIO:
            __str_tmp = "EIO";
            break;
#       endif /* EIO */
#       ifdef EISCONN
        case EISCONN:
            __str_tmp = "EISCONN";
            break;
#       endif /* EISCONN */
#       ifdef EISDIR
        case EISDIR:
            __str_tmp = "EISDIR";
            break;
#       endif /* EISDIR */
#       ifdef EISNAM
        case EISNAM:
            __str_tmp = "EISNAM";
            break;
#       endif /* EISNAM */
#       ifdef EKEYEXPIRED
        case EKEYEXPIRED:
            __str_tmp = "EKEYEXPIRED";
            break;
#       endif /* EKEYEXPIRED */
#       ifdef EKEYREJECTED
        case EKEYREJECTED:
            __str_tmp = "EKEYREJECTED";
            break;
#       endif /* EKEYREJECTED */
#       ifdef EKEYREVOKED
        case EKEYREVOKED:
            __str_tmp = "EKEYREVOKED";
            break;
#       endif /* EKEYREVOKED */
#       ifdef EL2HLT
        case EL2HLT:
            __str_tmp = "EL2HLT";
            break;
#       endif /* EL2HLT */
#       ifdef EL2NSYNC
        case EL2NSYNC:
            __str_tmp = "EL2NSYNC";
            break;
#       endif /* EL2NSYNC */
#       ifdef EL3HLT
        case EL3HLT:
            __str_tmp = "EL3HLT";
            break;
#       endif /* EL3HLT */
#       ifdef EL3RST
        case EL3RST:
            __str_tmp = "EL3RST";
            break;
#       endif /* EL3RST */
#       ifdef ELIBACC
        case ELIBACC:
            __str_tmp = "ELIBACC";
            break;
#       endif /* ELIBACC */
#       ifdef ELIBBAD
        case ELIBBAD:
            __str_tmp = "ELIBBAD";
            break;
#       endif /* ELIBBAD */
#       ifdef ELIBMAX
        case ELIBMAX:
            __str_tmp = "ELIBMAX";
            break;
#       endif /* ELIBMAX */
#       ifdef ELIBSCN
        case ELIBSCN:
            __str_tmp = "ELIBSCN";
            break;
#       endif /* ELIBSCN */
#       ifdef ELIBEXEC
        case ELIBEXEC:
            __str_tmp = "ELIBEXEC";
            break;
#       endif /* ELIBEXEC */
#       ifdef ELOOP
        case ELOOP:
            __str_tmp = "ELOOP";
            break;
#       endif /* ELOOP */
#       ifdef EMEDIUMTYPE
        case EMEDIUMTYPE:
            __str_tmp = "EMEDIUMTYPE";
            break;
#       endif /* EMEDIUMTYPE */
#       ifdef EMFILE
        case EMFILE:
            __str_tmp = "EMFILE";
            break;
#       endif /* EMFILE */
#       ifdef EMLINK
        case EMLINK:
            __str_tmp = "EMLINK";
            break;
#       endif /* EMLINK */
#       ifdef EMSGSIZE
        case EMSGSIZE:
            __str_tmp = "EMSGSIZE";
            break;
#       endif /* EMSGSIZE */
#       ifdef EMULTIHOP
        case EMULTIHOP:
            __str_tmp = "EMULTIHOP";
            break;
#       endif /* EMULTIHOP */
#       ifdef ENAMETOOLONG
        case ENAMETOOLONG:
            __str_tmp = "ENAMETOOLONG";
            break;
#       endif /* ENAMETOOLONG */
#       ifdef ENETDOWN
        case ENETDOWN:
            __str_tmp = "ENETDOWN";
            break;
#       endif /* ENETDOWN */
#       ifdef ENETRESET
        case ENETRESET:
            __str_tmp = "ENETRESET";
            break;
#       endif /* ENETRESET */
#       ifdef ENETUNREACH
        case ENETUNREACH:
            __str_tmp = "ENETUNREACH";
            break;
#       endif /* ENETUNREACH */
#       ifdef ENFILE
        case ENFILE:
            __str_tmp = "ENFILE";
            break;
#       endif /* ENFILE */
#       ifdef ENOBUFS
        case ENOBUFS:
            __str_tmp = "ENOBUFS";
            break;
#       endif /* ENOBUFS */
#       ifdef ENODATA
        case ENODATA:
            __str_tmp = "ENODATA";
            break;
#       endif /* ENODATA */
#       ifdef ENODEV
        case ENODEV:
            __str_tmp = "ENODEV";
            break;
#       endif /* ENODEV */
#       ifdef ENOENT
        case ENOENT:
            __str_tmp = "ENOENT";
            break;
#       endif /* ENOENT */
#       ifdef ENOEXEC
        case ENOEXEC:
            __str_tmp = "ENOEXEC";
            break;
#       endif /* ENOEXEC */
#       ifdef ENOKEY
        case ENOKEY:
            __str_tmp = "ENOKEY";
            break;
#       endif /* ENOKEY */
#       ifdef ENOLCK
        case ENOLCK:
            __str_tmp = "ENOLCK";
            break;
#       endif /* ENOLCK */
#       ifdef ENOLINK
        case ENOLINK:
            __str_tmp = "ENOLINK";
            break;
#       endif /* ENOLINK */
#       ifdef ENOMEDIUM
        case ENOMEDIUM:
            __str_tmp = "ENOMEDIUM";
            break;
#       endif /* ENOMEDIUM */
#       ifdef ENOMEM
        case ENOMEM:
            __str_tmp = "ENOMEM";
            break;
#       endif /* ENOMEM */
#       ifdef ENOMSG
        case ENOMSG:
            __str_tmp = "ENOMSG";
            break;
#       endif /* ENOMSG */
#       ifdef ENONET
        case ENONET:
            __str_tmp = "ENONET";
            break;
#       endif /* ENONET */
#       ifdef ENOPKG
        case ENOPKG:
            __str_tmp = "ENOPKG";
            break;
#       endif /* ENOPKG */
#       ifdef ENOPROTOOPT
        case ENOPROTOOPT:
            __str_tmp = "ENOPROTOOPT";
            break;
#       endif /* ENOPROTOOPT */
#       ifdef ENOSPC
        case ENOSPC:
            __str_tmp = "ENOSPC";
            break;
#       endif /* ENOSPC */
#       ifdef ENOSR
        case ENOSR:
            __str_tmp = "ENOSR";
            break;
#       endif /* ENOSR */
#       ifdef ENOSTR
        case ENOSTR:
            __str_tmp = "ENOSTR";
            break;
#       endif /* ENOSTR */
#       ifdef ENOSYS
        case ENOSYS:
            __str_tmp = "ENOSYS";
            break;
#       endif /* ENOSYS */
#       ifdef ENOTBLK
        case ENOTBLK:
            __str_tmp = "ENOTBLK";
            break;
#       endif /* ENOTBLK */
#       ifdef ENOTCONN
        case ENOTCONN:
            __str_tmp = "ENOTCONN";
            break;
#       endif /* ENOTCONN */
#       ifdef ENOTDIR
        case ENOTDIR:
            __str_tmp = "ENOTDIR";
            break;
#       endif /* ENOTDIR */
#       ifdef ENOTEMPTY
        case ENOTEMPTY:
            __str_tmp = "ENOTEMPTY";
            break;
#       endif /* ENOTEMPTY */
#       ifdef ENOTSOCK
        case ENOTSOCK:
            __str_tmp = "ENOTSOCK";
            break;
#       endif /* ENOTSOCK */
#       ifdef ENOTSUP
        case ENOTSUP:
            __str_tmp = "ENOTSUP";
            break;
#       endif /* ENOTSUP */
#       ifdef ENOTTY
        case ENOTTY:
            __str_tmp = "ENOTTY";
            break;
#       endif /* ENOTTY */
#       ifdef ENOTUNIQ
        case ENOTUNIQ:
            __str_tmp = "ENOTUNIQ";
            break;
#       endif /* ENOTUNIQ */
#       ifdef ENXIO
        case ENXIO:
            __str_tmp = "ENXIO";
            break;
#       endif /* ENXIO */
#       ifdef EOVERFLOW
        case EOVERFLOW:
            __str_tmp = "EOVERFLOW";
            break;
#       endif /* EOVERFLOW */
#       ifdef EPERM
        case EPERM:
            __str_tmp = "EPERM";
            break;
#       endif /* EPERM */
#       ifdef EPFNOSUPPORT
        case EPFNOSUPPORT:
            __str_tmp = "EPFNOSUPPORT";
            break;
#       endif /* EPFNOSUPPORT */
#       ifdef EPIPE
        case EPIPE:
            __str_tmp = "EPIPE";
            break;
#       endif /* EPIPE */
#       ifdef EPROTO
        case EPROTO:
            __str_tmp = "EPROTO";
            break;
#       endif /* EPROTO */
#       ifdef EPROTONOSUPPORT
        case EPROTONOSUPPORT:
            __str_tmp = "EPROTONOSUPPORT";
            break;
#       endif /* EPROTONOSUPPORT */
#       ifdef EPROTOTYPE
        case EPROTOTYPE:
            __str_tmp = "EPROTOTYPE";
            break;
#       endif /* EPROTOTYPE */
#       ifdef ERANGE
        case ERANGE:
            __str_tmp = "ERANGE";
            break;
#       endif /* ERANGE */
#       ifdef EREMCHG
        case EREMCHG:
            __str_tmp = "EREMCHG";
            break;
#       endif /* EREMCHG */
#       ifdef EREMOTE
        case EREMOTE:
            __str_tmp = "EREMOTE";
            break;
#       endif /* EREMOTE */
#       ifdef EREMOTEIO
        case EREMOTEIO:
            __str_tmp = "EREMOTEIO";
            break;
#       endif /* EREMOTEIO */
#       ifdef ERESTART
        case ERESTART:
            __str_tmp = "ERESTART";
            break;
#       endif /* ERESTART */
#       ifdef EROFS
        case EROFS:
            __str_tmp = "EROFS";
            break;
#       endif /* EROFS */
#       ifdef ESHUTDOWN
        case ESHUTDOWN:
            __str_tmp = "ESHUTDOWN";
            break;
#       endif /* ESHUTDOWN */
#       ifdef ESPIPE
        case ESPIPE:
            __str_tmp = "ESPIPE";
            break;
#       endif /* ESPIPE */
#       ifdef ESOCKTNOSUPPORT
        case ESOCKTNOSUPPORT:
            __str_tmp = "ESOCKTNOSUPPORT";
            break;
#       endif /* ESOCKTNOSUPPORT */
#       ifdef ESRCH
        case ESRCH:
            __str_tmp = "ESRCH";
            break;
#       endif /* ESRCH */
#       ifdef ESTALE
        case ESTALE:
            __str_tmp = "ESTALE";
            break;
#       endif /* ESTALE */
#       ifdef ESTRPIPE
        case ESTRPIPE:
            __str_tmp = "ESTRPIPE";
            break;
#       endif /* ESTRPIPE */
#       ifdef ETIME
        case ETIME:
            __str_tmp = "ETIME";
            break;
#       endif /* ETIME */
#       ifdef ETIMEDOUT
        case ETIMEDOUT:
            __str_tmp = "ETIMEDOUT";
            break;
#       endif /* ETIMEDOUT */
#       ifdef ETXTBSY
        case ETXTBSY:
            __str_tmp = "ETXTBSY";
            break;
#       endif /* ETXTBSY */
#       ifdef EUCLEAN
        case EUCLEAN:
            __str_tmp = "EUCLEAN";
            break;
#       endif /* EUCLEAN */
#       ifdef EUNATCH
        case EUNATCH:
            __str_tmp = "EUNATCH";
            break;
#       endif /* EUNATCH */
#       ifdef EUSERS
        case EUSERS:
            __str_tmp = "EUSERS";
            break;
#       endif /* EUSERS */
#       ifdef EXDEV
        case EXDEV:
            __str_tmp = "EXDEV";
            break;
#       endif /* EXDEV */
#       ifdef EXFULL
        case EXFULL:
            __str_tmp = "EXFULL";
            break;
#       endif /* EXFULL */
        case 255:
        default:
            __str_tmp = "EUNKNOWN";
            break;
    }

    return __str_tmp;
}

std::string
__cr_class_ns_str_signo(int signum)
{
    std::string __str_tmp;

    switch (signum) {
#       ifdef SIGHUP
        case SIGHUP:
            __str_tmp = "SIGHUP";
            break;
#       endif /* SIGHUP */
#       ifdef SIGINT
        case SIGINT:
            __str_tmp = "SIGINT";
            break;
#       endif /* SIGINT */
#       ifdef SIGQUIT
        case SIGQUIT:
            __str_tmp = "SIGQUIT";
            break;
#       endif /* SIGQUIT */
#       ifdef SIGILL
        case SIGILL:
            __str_tmp = "SIGILL";
            break;
#       endif /* SIGILL */
#       ifdef SIGTRAP
        case SIGTRAP:
            __str_tmp = "SIGTRAP";
            break;
#       endif /* SIGTRAP */
#       ifdef SIGABRT
        case SIGABRT:
            __str_tmp = "SIGABRT";
            break;
#       endif /* SIGABRT */
#       ifdef SIGEMT
        case SIGEMT:
            __str_tmp = "SIGEMT";
            break;
#       endif /* SIGEMT */
#       ifdef SIGFPE
        case SIGFPE:
            __str_tmp = "SIGFPE";
            break;
#       endif /* SIGFPE */
#       ifdef SIGKILL
        case SIGKILL:
            __str_tmp = "SIGKILL";
            break;
#       endif /* SIGKILL */
#       ifdef SIGBUS
        case SIGBUS:
            __str_tmp = "SIGBUS";
            break;
#       endif /* SIGBUS */
#       ifdef SIGSEGV
        case SIGSEGV:
            __str_tmp = "SIGSEGV";
            break;
#       endif /* SIGSEGV */
#       ifdef SIGSYS
        case SIGSYS:
            __str_tmp = "SIGSYS";
            break;
#       endif /* SIGSYS */
#       ifdef SIGPIPE
        case SIGPIPE:
            __str_tmp = "SIGPIPE";
            break;
#       endif /* SIGPIPE */
#       ifdef SIGALRM
        case SIGALRM:
            __str_tmp = "SIGALRM";
            break;
#       endif /* SIGALRM */
#       ifdef SIGTERM
        case SIGTERM:
            __str_tmp = "SIGTERM";
            break;
#       endif /* SIGTERM */
#       ifdef SIGURG
        case SIGURG:
            __str_tmp = "SIGURG";
            break;
#       endif /* SIGURG */
#       ifdef SIGSTOP
        case SIGSTOP:
            __str_tmp = "SIGSTOP";
            break;
#       endif /* SIGSTOP */
#       ifdef SIGTSTP
        case SIGTSTP:
            __str_tmp = "SIGTSTP";
            break;
#       endif /* SIGTSTP */
#       ifdef SIGCONT
        case SIGCONT:
            __str_tmp = "SIGCONT";
            break;
#       endif /* SIGCONT */
#       ifdef SIGCHLD
        case SIGCHLD:
            __str_tmp = "SIGCHLD";
            break;
#       endif /* SIGCHLD */
#       ifdef SIGTTIN
        case SIGTTIN:
            __str_tmp = "SIGTTIN";
            break;
#       endif /* SIGTTIN */
#       ifdef SIGTTOU
        case SIGTTOU:
            __str_tmp = "SIGTTOU";
            break;
#       endif /* SIGTTOU */
#       ifdef SIGIO
        case SIGIO:
            __str_tmp = "SIGIO";
            break;
#       endif /* SIGIO */
#       ifdef SIGXCPU
        case SIGXCPU:
            __str_tmp = "SIGXCPU";
            break;
#       endif /* SIGXCPU */
#       ifdef SIGXFSZ
        case SIGXFSZ:
            __str_tmp = "SIGXFSZ";
            break;
#       endif /* SIGXFSZ */
#       ifdef SIGVTALRM
        case SIGVTALRM:
            __str_tmp = "SIGVTALRM";
            break;
#       endif /* SIGVTALRM */
#       ifdef SIGPROF
        case SIGPROF:
            __str_tmp = "SIGPROF";
            break;
#       endif /* SIGPROF */
#       ifdef SIGWINCH
        case SIGWINCH:
            __str_tmp = "SIGWINCH";
            break;
#       endif /* SIGWINCH */
#       ifdef SIGINFO
        case SIGINFO:
            __str_tmp = "SIGINFO";
            break;
#       endif /* SIGINFO */
#       ifdef SIGUSR1
        case SIGUSR1:
            __str_tmp = "SIGUSR1";
            break;
#       endif /* SIGUSR1 */
#       ifdef SIGUSR2
        case SIGUSR2:
            __str_tmp = "SIGUSR2";
            break;
#       endif /* SIGUSR2 */
#       ifdef SIGTHR
        case SIGTHR:
            __str_tmp = "SIGTHR";
            break;
#       endif /* SIGTHR */
        case 255:
        default:
            __str_tmp = "SIGUNKNOWN";
            break;
    }

    return __str_tmp;
}
