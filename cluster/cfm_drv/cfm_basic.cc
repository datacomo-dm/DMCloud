#include <errno.h>
#include <cr_class/cr_quickhash.h>
#include <cfm_drv/cfm_basic.h>

#define CFM_DEFAULT_CHKSTR		"UMPWx5yhgfFd5A54MK6h7le4dMPlK6Mb"

CFMLibInfo::CFMLibInfo()
{
    this->on_start = NULL;
    this->on_stop = NULL;
    this->on_query = NULL;
    this->on_data = NULL;
    this->on_clear = NULL;
    this->on_async_recv_error = NULL;
}

int
CFM_Basic_NS::exchange_msg_merge(const std::string &job_id, const std::string *data_p, std::string &s_out)
{
    if (job_id.size() > 0x7F)
        return EMSGSIZE;

    uint8_t job_id_len = job_id.size();
    s_out.assign((const char*)(&job_id_len), sizeof(job_id_len));
    s_out.append(job_id);
    if (data_p) {
        s_out.append(*data_p);
    }

    return 0;
}

int
CFM_Basic_NS::exchange_msg_parse(const std::string &s_in, std::string &job_id, std::string *data_p)
{
    const char *str_data = s_in.data();
    const size_t str_size = s_in.size();
    const char *str_cur_p = str_data;

    // parse 1 byte job_id_len
    uint8_t job_id_len;
    if (!CR_BOUNDARY_CHECK(str_cur_p, sizeof(job_id_len), str_data, str_size))
        return EILSEQ;
    str_cur_p = (const char *)CR_Class_NS::memread(&job_id_len, str_cur_p, sizeof(job_id_len));
    if (job_id_len & 0x80) {
        job_id_len &= 0x7F;
    }

    // parse job_id
    if (!CR_BOUNDARY_CHECK(str_cur_p, job_id_len, str_data, str_size))
        return EILSEQ;
    job_id.assign(str_cur_p, job_id_len);
    str_cur_p += job_id_len;

    // parse data
    if (data_p)
        data_p->assign(str_cur_p, str_size - job_id_len - 1);

    return 0;
}

void
CFM_Basic_NS::exchange_msg_dsbi(std::string &s)
{
    if (s.size() == 0)
        return;
    s[0] |= 0x80;
}

void
CFM_Basic_NS::exchange_msg_dcli(std::string &s)
{
    if (s.size() == 0)
        return;
    s[0] &= 0x7F;
}

bool
CFM_Basic_NS::exchange_msg_dtst(const std::string &s)
{
    if (s.size() == 0)
        return false;
    return ((s[0] & 0x80) == 0x80);
}

int
CFM_Basic_NS::parse_cluster_info(const std::string &cluster_str, cluster_info_t &cluster_info)
{
    int ret = 0;
    std::pair<int,int> fret;

    fret = CFM_Basic_NS::parse_cluster_info(cluster_str, CR_Class_NS::blank_string, cluster_info);
    if (fret.first == INT_MIN)
        ret = fret.second;

    return ret;
}

std::pair<int,int>
CFM_Basic_NS::parse_cluster_info(const std::string &cluster_str,
    const std::string &cur_node_addr, CFM_Basic_NS::cluster_info_t &cluster_info)
{
    std::pair<int,int> ret;
    int fret;
    CFM_Basic_NS::striping_info_t tmp_striping_info;
    CFM_Basic_NS::cluster_info_t tmp_cluster_info;
    const char *cp = cluster_str.c_str();
    const char *last_cp = cp;
    std::string tmp_node_addr;
    std::set<std::string> node_addr_set;
    CR_Class_NS::nn_transport_t tmp_transport;
    std::string tmp_hostname;
    std::string tmp_srvname;
    int striping_id = 0;
    int node_id = 0;

    ret.first = -1;
    ret.second = ENOENT;

    while (1) {
        if ((*cp == ',') || (*cp == ';') || (*cp == '\0')) {
            tmp_node_addr.assign(last_cp, ((uintptr_t)cp - (uintptr_t)last_cp));
            if (tmp_node_addr.size() > 0) {
                if (node_addr_set.find(tmp_node_addr) == node_addr_set.end()) {
                    node_addr_set.insert(tmp_node_addr);
                    fret = CR_Class_NS::nn_addr_split(tmp_node_addr.c_str(),
                      tmp_transport, tmp_hostname, tmp_srvname);
                    if (fret) {
                        ret.first = INT_MIN;
                        ret.second = fret;
                        return ret;
                    }
                    if (tmp_transport != CR_Class_NS::NN_TCP) {
                        ret.first = INT_MIN;
                        ret.second = ENOTSUP;
                        return ret;
                    }
                    if ((tmp_hostname.size() == 0) || (tmp_srvname.size() == 0)) {
                        ret.first = INT_MIN;
                        ret.second = EINVAL;
                        return ret;
                    }
                    if (cur_node_addr == tmp_node_addr) {
                        ret.first = striping_id;
                        ret.second = node_id;
                    }
                    tmp_striping_info.push_back(tmp_node_addr);
                    node_id++;
                } else {
                    ret.first = INT_MIN;
                    ret.second = EEXIST;
                    return ret;
                }
            }
            if (*cp != ',') {
                if (tmp_striping_info.size() > 0) {
                    std::sort(tmp_striping_info.begin(), tmp_striping_info.end());
                    tmp_cluster_info.push_back(tmp_striping_info);
                    tmp_striping_info.clear();
                    striping_id++;
                    node_id = 0;
                }
                if (*cp != ';') {
                    break;
                }
            }
            cp++;
            last_cp = cp;
        } else {
            cp++;
        }
    }

    if (ret.first != INT_MIN) {
        cluster_info = tmp_cluster_info;
    }

    return ret;
}

std::string
CFM_Basic_NS::make_node_name_prefix(std::pair<int,int> current_node_pos)
{
    std::string ret;

    ret = std::string("CFM-S");
    ret += CR_Class_NS::u162str(current_node_pos.first);
    ret += std::string("P");
    ret += CR_Class_NS::u82str(current_node_pos.second);
    ret += std::string("-");

    return ret;
}

std::string
CFM_Basic_NS::get_default_chkstr()
{
    return std::string(CFM_DEFAULT_CHKSTR);
}
