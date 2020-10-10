#if 0 /*

#Usage: display=`_readability_number <number>`
#Example: display=`_readability_number 1048576`
#         display ==> "1M"
_readability_number()
{
    local x
    local h
    local div_break

    x="${1}"
    h=""
    div_break=0

    [ "0${x}" -ne "0" ] > /dev/null 2>&1
    if [ "A${?}" != "A0" ]; then
        x=0
    fi

    while expr ${x} '>=' 1024 > /dev/null ; do
        x=`expr ${x} '/' 1024`
        case "${h}" in
            "")
                h="K"
                ;;
            K)
                h="M"
                ;;
            M)
                h="G"
                ;;
            G)
                h="T"
                ;;
            T)
                h="P"
                ;;
            P)
                h="E"
                ;;
            E)
                h="Z"
                ;;
            Z)
                h="Y"
                div_break=1
                ;;
            *)
                div_break=1
                ;;
        esac
        if [ "A${div_break}" = "A1" ]; then
            break
        fi
    done

    echo "${x}${h}"
}

#Usage: newparmlist=`_param_conn <curparm> [curparmlist]`
_param_conn()
{
    local curparam
    local paramlist
    local i
    local do_add

    curparam="${1}"
    paramlist="${2}"
    do_add=1

    for i in ${paramlist} ; do
        if [ "A${curparam}" = "A${i}" ]; then
            do_add=0
            break
        fi
    done

    if [ "A${do_add}" = "A1" ]; then
        if [ "A${paramlist}" = "A" ]; then
            echo "${curparam}"
        else
            echo "${paramlist} ${curparam}"
        fi
    else
        echo "${paramlist}"
    fi
}

#Usage: id=`_standalone_name_to_id <name> [base id]`
_standalone_name_to_id()
{
    local id
    local minid

    id=`echo "${1}" | cksum | cut -c 1-7`
    minid="${2}"

    expr "0${minid}" + "0${id}" + "1"
}

#Usage: x=`_ranged_string "${x}" <min_val> <max_val>`
_ranged_string()
{
    local x
    local min_val
    local max_val

    x="${1}"
    min_val="${2}"
    max_val="${3}"

    if [ A`expr "${min_val}" ">" "${max_val}"` != "A0" ]; then
        min_val="${max_val}"
    fi

    if [ A`expr "${x}" "<" "${min_val}"` != "A0" ]; then
        x="${min_val}"
    fi

    if [ A`expr "${x}" ">" "${max_val}"` != "A0" ]; then
        x="${max_val}"
    fi

    echo "${x}"
}

#Usage: x=`_ranged_number "${x}" <min_val> <max_val>`
_ranged_number()
{
    local x
    local min_val
    local max_val

    x="${1}"
    min_val="${2}"
    max_val="${3}"

    [ "0${min_val}" -ne "0" ] > /dev/null 2>&1
    if [ "A${?}" != "A0" ]; then
        min_val=0
    fi

    [ "0${max_val}" -ne "0" ] > /dev/null 2>&1
    if [ "A${?}" != "A0" ]; then
        max_val=0
    fi

    if [ A`expr "${min_val}" ">" "${max_val}"` != "A0" ]; then
        min_val="${max_val}"
    fi

    [ "0${x}" -ne "0" ] > /dev/null 2>&1
    if [ "A${?}" != "A0" ]; then
        x="${min_val}"
    fi

    if [ A`expr "${x}" "<" "${min_val}"` != "A0" ]; then
        x="${min_val}"
    fi

    if [ A`expr "${x}" ">" "${max_val}"` != "A0" ]; then
        x="${max_val}"
    fi

    echo ${x}
}

#Usage: username=`_IDN_get_username <IDN_string>`
#Example: username=`_IDN_get_username 'riak12@my.com'`
#         username ==> "riak12"
_IDN_get_username()
{
    local username

    username=`echo "${1}" | cut -d '@' -f 1`

    echo "${username}"
}

#Usage: userprefix=`_IDN_get_userprefix <IDN_string>`
#Example: userprefix=`_IDN_get_userprefix 'riak12@my.com'`
#         userprefix ==> "riak"
_IDN_get_userprefix()
{
    local username
    local userprefix

    username=`echo "${1}" | cut -d '@' -f 1`
    userprefix=`echo "${username}" | tr -d [:digit:]`

    echo "${userprefix}"
}

#Usage: instance=`_IDN_get_instance <IDN_string>`
#Example: instance=`_IDN_get_instance 'riak12@my.com'`
#         instance ==> "12"
_IDN_get_instance()
{
    local username
    local instance

    username=`echo "${1}" | cut -d '@' -f 1`
    instance=`echo "${username}" | tr -d [:alpha:]`
    instance=`_ranged_number "${instance}" 1 99`

    echo "${instance}"
}

#Usage: hostname=`_IDN_get_hostname <IDN_string>`
#Example: hostname=`_IDN_get_hostname 'riak12@my.com'`
#         hostname ==> "my.com"
_IDN_get_hostname()
{
    local domainname

    domainname=`echo "${1}" | cut -d '@' -f 2- | cut -d ':' -f 1`

    echo "${domainname}"
}

#Usage: rpath=`_IDN_get_rpath <IDN_string>`
#Example: rpath=`_IDN_get_rpath 'riak12@my.com:/absolute/path/to/name'`
#         rpath ==> "/absolute/path/to/name"
_IDN_get_rpath()
{
    local rpath

    rpath=`echo "${1}" | cut -d '@' -f 2- | cut -d ':' -f 2`

    echo "${rpath}"
}

#Usage: __remote_user=<user_name>
#       __remote_host=<host_name>
#       __remote_port=<port>
#       _execr <command> <args>
_execr()
{
    local command_line
    local status

    command_line="${@}"

    if [ "A${command_line}" != "A" ]; then
        __remote_port=`_ranged_number "${__remote_port}" 0 65535`

       	if [ "A${__remote_host}" != "A" -a "A${__remote_host}" != "Alocalhost" ]; then
            if [ "A${__remote_user}" != "A" ]; then
                ssh -p "${__remote_port}" "${__remote_host}" "su -l '${__remote_user}' -c '${command_line}'"
                status="${?}"
            else
                ssh -p "${__remote_port}" "${__remote_host}" "${command_line}"
                status="${?}"
            fi
        else
       	    if [ "A${__remote_user}" != "A" ]; then
                su -l "${__remote_user}" -c "${command_line}"
                status="${?}"
            else
                sh -c "${command_line}"
                status="${?}"
            fi
        fi
    fi

    if [ "A${status}" != "A0" ]; then
        echo "ERROR EXECR: ${__remote_user}@${__remote_host}:${__remote_port} => ${command_line}"
        exit ${status}
    fi
}

#Usage: __remote_user=<user_name>
#       __remote_host=<host_name>
#       __remote_port=<port>
#       _copyr <src> <dst>
_copyr()
{
    local src
    local dst
    local fname
    local status
    local _tmp
    local __remote_user_save

    src="${1}"
    dst="${2}"
    fname=`basename "${src}"`
    status=0
    _tmp=""

    while true ; do
        if [ "A${src}" != "A" -a "A${dst}" != "A" -a -f "${src}" ]; then
            __remote_port=`_ranged_number "${__remote_port}" 0 65535`

       	    if [ "A${__remote_host}" != "A" -a "A${__remote_host}" != "Alocalhost" ]; then
                if [ "A${__remote_user}" != "A" ]; then
                    __remote_user_save="${__remote_user}"
                    __remote_user=""
                    _tmp=`_execr mktemp -d`
                    scp -P "${__remote_port}" "${src}" "${__remote_host}:${_tmp}/" > /dev/null
                    status="${?}"
                    if [ "A${status}" != "A0" ]; then
                        _execr rm -rf "${_tmp}"
                        break
                    fi
                    _execr chmod -R 755 "${_tmp}"
                    __remote_user="${__remote_user_save}"
                    _execr cp "${_tmp}/${fname}" "${dst}"
                    __remote_user=""
                    _execr rm -rf "${_tmp}"
                    __remote_user="${__remote_user_save}"
                else
                    scp -P "${__remote_port}" "${src}" "${__remote_host}:${dst}" > /dev/null
                    status="${?}"
                fi
            else
                if [ "A${__remote_user}" != "A" ]; then
                    _tmp=`mktemp -d`
                    status="${?}"
                    cp "${src}" "${_tmp}/"
                    status="${?}"
                    chmod -R 755 "${_tmp}"
                    status="${?}"
                    su -l "${__remote_user}" -c "cp ${_tmp}/${fname} ${dst}"
                    status="${?}"
                    rm -f "${_tmp}"
                else
                    cp "${src}" "${dst}"
                    status="${?}"
                fi
            fi
        fi

        break
    done

    if [ "A${status}" != "A0" ]; then
        echo "ERROR COPYR: ${__remote_user}@${__remote_host}:${__remote_port} => ${src} ${dst}"
        exit ${status}
    fi
}

#else */
#endif
