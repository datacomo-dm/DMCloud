#include "cluster_fm.h"
#include "cfm_jobexecutor.h"
#include <cr_class/cr_persistenceid.h>

static std::mutex _id_lock_local;

int
do_nd_id_req(void *server_private_p, void *connect_private_p,
    const std::string &client_host, const std::string &client_srv,
    const int8_t cmd_code_from_client, const std::string &data_from_client,
    int8_t &cmd_code_to_client, std::string &data_to_client, std::string &errmsg)
{
    std::string job_id;
    std::string data_req;

    int ret = CFM_Basic_NS::exchange_msg_parse(data_from_client, job_id, &data_req);
    if (ret)
        return ret;

    CFMIDReq id_req;

    if (!id_req.ParseFromString(data_req))
        return ENOMSG;

    std::lock_guard<std::mutex> _lck(_id_lock_local);

    std::string perid_pathname;
    double perid_timeout = 86400 * 30;

    perid_pathname = app_work_path_arr[0];
    perid_pathname.append("/PERSIST_ID-N");
    perid_pathname.append(CR_Class_NS::i162str(app_current_node.first));
    perid_pathname.append("-");
    perid_pathname.append(id_req.id_name());

    if (id_req.has_autofree_timeout())
        perid_timeout = id_req.autofree_timeout();

    CR_PersistenceID perid;

    ret = perid.SetArgs(perid_pathname, 0, ((INT64_C(1) << CFM_ID_EFFECT_BIT_COUNT) - 1), perid_timeout);
    if (ret) {
        DPRINTF("(%s:%s)perid.SetArgs(\"%s\", ...) == %s\n", client_host.c_str(), client_srv.c_str(),
          perid_pathname.c_str(), CR_Class_NS::strerrno(ret));
        return ret;
    }

    CFMIDResp id_resp;
    std::string data_resp;

    switch (id_req.cmd_code()) {
    case CFM_ID_ALLOC :
      {
        std::vector<int64_t> id_arr;

        ret = perid.Alloc(id_req.alloc_count(), id_arr);

        if (!ret) {
            for (size_t i=0; i<id_arr.size(); i++) {
                id_resp.add_id_list(id_arr[i]);
            }
            id_resp.SerializeToString(&data_resp);
        }

        DPRINTFX(20, "(%s:%s)Alloc(\"%s\", %lu, %f) == %s, [%ld ~ %ld]\n",
          client_host.c_str(), client_srv.c_str(), id_req.id_name().c_str(),
          (long unsigned)id_req.alloc_count(), perid_timeout,
          CR_Class_NS::strerrno(ret), (long)((id_arr.size() > 0)?(id_arr[0]):(-1)),
          (long)((id_arr.size() > 0)?(id_arr[id_arr.size()-1]):(-1)));
      }
        break;
    case CFM_ID_FREE :
      {
        std::vector<int64_t> id_arr;

        for (int i=0; i<id_req.id_list_size(); i++) {
            id_arr.push_back(id_req.id_list(i));
        }

        ret = perid.Free(id_arr);

        DPRINTFX(20, "(%s:%s)Free(\"%s\", [%ld ~ %ld]) == %s\n",
          client_host.c_str(), client_srv.c_str(), id_req.id_name().c_str(),
          (long)((id_arr.size() > 0)?(id_arr[0]):(-1)),
          (long)((id_arr.size() > 0)?(id_arr[id_arr.size()-1]):(-1)),
          CR_Class_NS::strerrno(ret));
      }
        break;
    case CFM_ID_SOLIDIFY :
      {
        std::vector<int64_t> id_arr;

        for (int i=0; i<id_req.id_list_size(); i++) {
            id_arr.push_back(id_req.id_list(i));
        }

        ret = perid.Solidify(id_arr);

        DPRINTFX(20, "(%s:%s)Solidify(\"%s\", [%ld ~ %ld]) == %s\n",
          client_host.c_str(), client_srv.c_str(), id_req.id_name().c_str(),
          (long)((id_arr.size() > 0)?(id_arr[0]):(-1)),
          (long)((id_arr.size() > 0)?(id_arr[id_arr.size()-1]):(-1)),
          CR_Class_NS::strerrno(ret));
      }
        break;
    default :
        ret = EINVAL;
        break;
    }

    if (!ret)
        CFM_Basic_NS::exchange_msg_merge(job_id, &data_resp, data_to_client);

    return ret;
}
