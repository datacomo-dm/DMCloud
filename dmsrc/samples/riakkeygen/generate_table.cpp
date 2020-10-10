#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<sys/types.h>
#include<sys/stat.h>
#include<unistd.h>
#include<vector>
#include<algorithm>
#include<string>
#include "riakcluster.h"
#include "msgIBRiak.pb.h"
#include "IBCompress.h"
#include <pthread.h>
#include <glob.h>
#include "generate_part.h"
#include "generate_table.h"

using namespace std;

RiakCluster *_riak_cluster = NULL;
pthread_once_t _riak_cluster_init_once = PTHREAD_ONCE_INIT;
void*    pAsyncRiakHandle = NULL;
int _riak_nodes = 0;
int _riak_connect_number = 0;
int _riak_max_thread = 0;

/*
    ����: ��ĳһ�� ��pack����Ϣ��ת������뵽riak��
    ����: 
            pold_basepth[in]:Դ���ݿ�$DATAMERGER_HOME/var Ŀ¼
            pold_dbname[in]:Դ���ݿ�����
            pold_tbname[in]:Դ���ݿ������
            col_num[in]:��Ҫת�����б�ţ���0��ʼ
            col_type[in]:������
            pack_num[in]:���������� ��pack����Ŀ
            pnew_basepth[in]:�����ݿ�$DATAMERGER_HOME/var Ŀ¼
            pnew_dbname[in]:�����ݿ�����
            pnew_tbname[in]:�����ݿ������
            st_partition_info[in/out]:������Ϣ,�ڲ��洢���д���riak�е�keyspace,���������е�ͳһ�ύ�ͻع�
    ˵��:
        1> ��ԴSWATCH_AB�ļ�,�ж�dpn�����һ��������ζ�ȡ��
        2> ��Դdpn�ļ��������ȡ������Ϣ
        3> ����Դdpn�ж�ȡ�������ݰ����ж��Ƿ����pack�ļ���
        4> ���Ѿ�����Դpack�ļ������ݰ����ж�ȡ���Զ�ȡ�����ݰ����ݱ��浽riak��
        5> ������Ϣ��dpn�ļ����ݣ�����ȡ��dpn��д�뵽�µ��ļ���
        6> ������Ϣ�ķ����ļ�
        7> ��ͬ�ķ�����Ҫ���и����첽�������С���ύ
*/
int generate_riak_column(const char* pold_basepth,const char* pold_dbname,const char* pold_tbname,
                        const int col_num,const AttributeType col_type,const int pack_num,
                        const char* pnew_basepth,const char* pnew_dbname,const char* pnew_tbname,
                        stru_partinfo& st_partition_info);

ValueType GetValueType(const _uint64 maxv)
{
    if(maxv<= 0xff)
        return UCHAR;
    else if(maxv<=0xffff)
        return USHORT;
    else if(maxv<=0xffffffff)
        return UINT;
    else 
        return UINT64;
}

void _riak_cluster_init()
{
    char *riak_hostname;
    char *riak_port_str;
    int fret;

    riak_hostname = getenv(RIAK_HOSTNAME_ENV_NAME);
    riak_port_str = getenv(RIAK_PORT_ENV_NAME);

    if (!riak_hostname) {
        ThrowWith("Must set $%s and $%s\n", RIAK_HOSTNAME_ENV_NAME);
    }

    // ÿһ��riak�ڵ�������̳߳���Ŀ(riak �������ֵ����ӳ���)
    const char* priak_max_connect_per_node=getenv(RIAK_MAX_CONNECT_PER_NODE_ENV_NAME);

    if(NULL == priak_max_connect_per_node)
    {
        _riak_cluster = new RiakCluster(180);
    }
    else
    {
        int riak_max_connect_per_node = atoi(priak_max_connect_per_node);
        if(riak_max_connect_per_node >0 ){
            _riak_cluster = new RiakCluster(180,riak_max_connect_per_node);
        }
        else{
            _riak_cluster = new RiakCluster(180);
        }
    }
    fret = _riak_cluster->Rebuild(riak_hostname, riak_port_str);

        if (fret > 0) {
        DPRINTF("Riak cluster: found and success connect to %d nodes.\n", fret);
        } else if (fret == 0) {
        DPRINTF("Riak cluster: connect to first node, but cannot connect to any cluster node, maybe try again later.\n");
        } else {
            int ferr = (0 - fret);
            DPRINTF("Riak cluster: connect to first node failed, ferr=%d, msg=%s\n", ferr, strerror(ferr));
            throw ferr;
        }

    // ����riak��Ŀ
    const char* priak_connect_number = getenv(RIAK_CONNECT_THREAD_NUMBER_ENV_NAME);
    if(NULL != priak_connect_number)
        _riak_connect_number = atoi(priak_connect_number);

    // �������߳���������
    int riak_connect_thread_times = 0;
    const char *priak_connect_thread_times = getenv(RIAK_CONNTCT_THREAD_TIMES_ENV_NAME);
    if(NULL == priak_connect_thread_times){
        riak_connect_thread_times = RIAK_CONNTCT_THREAD_TIMES_MIN*2;
    }
    else{
        riak_connect_thread_times = (atoi(priak_connect_thread_times)>RIAK_CONNTCT_THREAD_TIMES_MIN) ?
            atoi(priak_connect_thread_times) : RIAK_CONNTCT_THREAD_TIMES_MIN;
    }
    _riak_max_thread = fret * riak_connect_thread_times +1;

    _riak_nodes = fret;
}

// ��ȡ��id������Ŀ
int generate_get_table_info(const char* pbasepth,const char* pdbname,const char* ptbname,int& tabid,int& colnum)
{
    char pth[300];
    // ---> /data/dt2/app/dma/var/zkb/test_continu_dump.bht/Table.ctb
    sprintf(pth,"%s/%s/%s.bht/Table.ctb",pbasepth,pdbname,ptbname);
    
    FILE  *pFile  = fopen(pth,"rb");
    if(!pFile){
        ThrowWith("�ļ�%s��ʧ��!\n",pth);    
    }
    // get colno
    char buf[500] = {0};
    int fsize = fread(buf,1,33,pFile);
    if (fsize < 33 || strncmp(buf, "RScT", 4) != 0 || buf[4] != '2'){
        ThrowWith("Bad format of table definition in %s\n",pth);
    }
    colnum=*((int*)(buf+25));
    
    // get tabno
    fseek(pFile,-4,SEEK_END);
    int _tabid = 0;
    fread(&_tabid,4,1,pFile);
    fclose(pFile);

    tabid = _tabid;
    
    return 0;
}


// ��ȡ����Ŀ��������
int generate_get_col_info(const char* pbasepth,const char* pdbname,const char* ptbname,const int col_number,int& pack_no, AttributeType& col_type)
{
    char path[256];
    sprintf(path,"%s/%s/%s.bht",pbasepth,pdbname,ptbname);
    std::string attr_file = AttrFileName(path,col_number);
    
    FILE  *pFile  = fopen(attr_file.c_str(),"rb");
    if(!pFile){
        lgprintf("�ļ�%s��ʧ��!\n",attr_file.c_str());    
        return -__LINE__;
    }

    const int CLMD_HEADER_LEN = 46;
    char read_buf[CLMD_HEADER_LEN+1];
    int ret = fread(read_buf,CLMD_HEADER_LEN,1,pFile);
    if(ret != 1){
        lgprintf("�ļ�%s��ȡʧ��!\n",attr_file.c_str());    
        return -__LINE__;
    }

    if ((uchar)read_buf[8] < 128) {
        col_type = (AttributeType)read_buf[8];
    } else {
        col_type = (AttributeType)((uchar)read_buf[8]-128);
    }

    pack_no = *((int*)(read_buf + 30));

    fclose(pFile);
    pFile = NULL;
    
    return 0;
}


// �޸����ݿ���id,��brighthouse.seq�л�ȡid,������id
int  _getSeqId(const char* pfile,int & tabid)
{
    // ���ļ��б����ļ��б��Ƿ��tar���е�һ��
    FILE* pFile = fopen(pfile,"rt");
    if(NULL == pFile){
        lgprintf("Open file %s error \n",pfile);
        return -__LINE__;
    }
    char lines[300];
    fgets(lines,300,pFile);
    tabid = atoi(lines);
    fclose(pFile);
    return 0;
}

int _updateSeqId(const char* pfile,const int tabid)
{
    FILE* pFile = fopen(pfile,"wt");
    if(NULL == pFile){
        lgprintf("Open file %s error \n",pfile);
        return -__LINE__;
    }
    fprintf(pFile,"%d",tabid);
    fclose(pFile);    
    return 0;
}

int _updateTableId(const char* pth,const int tabid)
{
    FILE  *pFile  = fopen(pth,"r+b");
    if(!pFile){
        lgprintf("�ļ�%s��ʧ��!",pth);
        return -__LINE__;
    }
    fseek(pFile,-4,SEEK_END);
    fwrite(&tabid,4,1,pFile);;
    fclose(pFile); 
    pFile = NULL;
    return 0;
}

int _getTableId(const char* pth,int & tabid)
{
    FILE  *pFile  = fopen(pth,"rb");
    if(!pFile){
        lgprintf("�ļ�%s��ʧ��!",pth);
        return -__LINE__;
    }
    fseek(pFile,-4,SEEK_END);
    int _tabid = 0;
    fread(&_tabid,4,1,pFile);
    fclose(pFile);

    tabid = _tabid;
    return tabid;
}

// ����ָ��Ŀ¼�µ�*.rsi�ļ��ı�id
// ????.%d.*.rsi �ļ��滻��ͨ�� %d �ı�id
#define RSI_FILE_HEAD_LEN  4   // "CMAP." or  "HIST."
#define RSI_POS            '.'  
bool _updateRsiID(const char* pth,const int tabid,const char* pattern)
{
    char rsi_parttern[300] = {0};
    // tabid�Ѿ����ˣ���������µ�id
    //sprintf(rsi_parttern,"%s/????.%d.*.rsi",pth,tabid);
    if(pattern == NULL){
        sprintf(rsi_parttern,"%s/*",pth);
    }
    else{
        sprintf(rsi_parttern,"%s/%s",pth,pattern);
    }
    
    char path_head[300];
    sprintf(path_head,"%s",pth);
    int path_head_len = strlen(path_head)+1;  // ������/���ĳ���

    glob_t globbuf;
    memset(&globbuf,0,sizeof(globbuf));
    globbuf.gl_offs = 0;
    //GLOB_NOSORT  Don��t sort the returned pathnames.  The only reason to do this is to save processing time.  By default, the returned pathnames are sorted.
    if(!glob(rsi_parttern, GLOB_DOOFFS, NULL, &globbuf)) {
        for(int i=0;i<globbuf.gl_pathc;i++) {
            char *poldname = globbuf.gl_pathv[i];

            std::string strname(poldname);
            int str_len = strname.size();
            // ��ȡCMAP ����HIST
            std::string strhead = strname.substr(path_head_len,RSI_FILE_HEAD_LEN);
            
            // %d.*.rsi �Ľ�ȡ    
            int  _tmp1 = path_head_len+RSI_FILE_HEAD_LEN + 1;  // '.' һ���ַ�
            int  _tmp2 = str_len-_tmp1;    //     %d.*.rsi �ĳ���
            
            std::string strtmp = strname.substr(_tmp1,_tmp2);
                
            // *.rsi�Ľ�ȡ
            int pos = strtmp.find(RSI_POS);
            str_len = strtmp.size();
            strtmp = strtmp.substr(pos+1);  // get 'colid.rsi'

            // get new filename by new tabid
            char pnewname[300];
            // %s/BH_RSI_Repository/????.%d.*.rsi
            sprintf(pnewname,"%s/%s.%d.%s",pth,strhead.c_str(),tabid,strtmp.c_str());

            // �ļ�����������  poldname ---> pnewname
            char cmd[300];
            sprintf(cmd,"mv %s %s",poldname,pnewname);
            int ret = system(cmd);
            assert(ret != -1);            
        }    
    }
    globfree(&globbuf);
    return true;
}

// ����tabid,return tabid
int  generate_change_table_id(const char* pbasepth,const char* dbname,const char* tbname)
{
    int tabid=0;
    int ret = -1;
    lgprintf("��ʼ�޸ı�[%s/%s/%s]��ID.",pbasepth,dbname,tbname);

    int seqid = 0;
    char seq_file[300];
    sprintf(seq_file,"%s/brighthouse.seq",pbasepth);
    ret = _getSeqId(seq_file,seqid);
    if(ret != 0){
        lgprintf("��ȡbrighthouse.seq��idʧ��.");
        return -__LINE__;
    }

    //  ֱ��ʹ��һ���µ�id��seqid + 1
    seqid = seqid +1;
    tabid= seqid;
    ret = _updateSeqId(seq_file,seqid);
    if(ret != 0){
        lgprintf("����brighthouse.seq��idʧ��.");
        return -__LINE__;
    }

    // ����tabid
    char _table_tcb_pth[300];
    sprintf(_table_tcb_pth,"%s/%s/%s.bht/Table.ctb",pbasepth,dbname,tbname);
    
    ret = _updateTableId(_table_tcb_pth,tabid);
    if(ret != 0){
        lgprintf("���±�[%s/%s/%s]��IDʧ��.",pbasepth,dbname,tbname);
        return -__LINE__;
    }
    
    lgprintf("�޸ı�[%s/%s/%s]��ID�ɹ�.",pbasepth,dbname,tbname);
    return tabid;
}


// solid ������Ӧ��riak keyspace
int generate_riak_solid_keyspace(const stru_partinfo st_partinfo)
{
    if(_riak_cluster != NULL){
        int tab_number = st_partinfo.tabid;
        int attr_number = st_partinfo.attrid;
        for(int part_num=0;part_num<st_partinfo.st_part_list.size();part_num++){
            for(int riak_pnode=0;riak_pnode<st_partinfo.st_part_list[part_num].riak_pnode_list.size();riak_pnode++){
                _riak_cluster->KeySpaceSolidify(RIAK_PAT_NAME,tab_number,attr_number,st_partinfo.st_part_list[part_num].riak_pnode_list[riak_pnode]);     
            }
        }
    }
    return 0;
}

// solid ������Ӧ��riak keyspace
int generate_riak_free_keyspace(const stru_partinfo st_partinfo)
{
    if(_riak_cluster != NULL){
        int tab_number = st_partinfo.tabid;
        int attr_number = st_partinfo.attrid;
        for(int part_num=0;part_num<st_partinfo.st_part_list.size();part_num++){
            for(int riak_pnode=0;riak_pnode<st_partinfo.st_part_list[part_num].riak_pnode_list.size();riak_pnode++){
                _riak_cluster->KeySpaceFree(RIAK_PAT_NAME,tab_number,attr_number,st_partinfo.st_part_list[part_num].riak_pnode_list[riak_pnode]);   
            }
        }
    }
    return 0;
}

// ����rsi�����ļ�
int generate_copy_rsi(const char* pold_basepth,const char* pnew_basepth,const int old_tabid,const int new_tabid)
{
    char old_table_rsi_pattern[300];
    char new_table_rsi_pattern[300];

    char cmd[500];
    sprintf(old_table_rsi_pattern,"%s/BH_RSI_Repository/????.%d.*.rsi",pold_basepth,old_tabid);
    

    sprintf(cmd,"cp %s/BH_RSI_Repository/????.%d.*.rsi %s/BH_RSI_Repository",pold_basepth,old_tabid,pnew_basepth);
    system(cmd);

    // ����rsi��tabid
    sprintf(new_table_rsi_pattern,"????.%d.*.rsi",old_tabid);
    char BH_RSI_dir[300];
    sprintf(BH_RSI_dir,"%s/BH_RSI_Repository",pnew_basepth);
    
    bool ret = _updateRsiID(BH_RSI_dir,new_tabid,new_table_rsi_pattern);
    if(!ret){
        lgprintf("_updateRsiID error");
        return -__LINE__;
    }
    return 0;
}

// ���ת��ʧ�ܽ��лع�
int generate_table_rallback(const char* pnew_basepth,const char* pnew_dbname,const char* pnew_tbname,const int pnew_tbid)    
{
    char cmd[1000];
    char path[256];
    sprintf(path,"%s/%s/%s.*",pnew_basepth,pnew_dbname,pnew_tbname);
    sprintf(cmd,"rm %s -rf",path);
    lgprintf("��������:%s",cmd);
    system(cmd);    

    if(pnew_tbid>0){
        sprintf(path,"%s/BH_RSI_Repository/????.%d.*.rsi -rf",pnew_basepth,pnew_tbid);
        lgprintf("��������:%s",cmd);
        system(cmd);
    }
    return 0;
}

// �ж����ݿ���Ƿ����
bool generate_exist_table(const char* pbasepth,const char* pdbname,const char* ptbname)
{
    char path_frm[256];
    sprintf(path_frm,"%s/%s/%s.frm",pbasepth,pdbname,ptbname);

    struct stat stat_info;
    return (0 == stat(path_frm, &stat_info)) ;
}

// ���ϵ����ݿ��,ת�����µ����ݿ��
int  generate_riak_table(const char* pold_basepth,const char* pold_dbname,const char* pold_tbname,
                        const char* pnew_basepth,const char* pnew_dbname,const char* pnew_tbname)
{
    // ����ֵ
    int ret = 0;
    bool generate_success = true; // �����Ƿ�ɹ�
    int column_num = 0; 
    int old_tabid = 0;
    int new_tabid = 0;
    
    // �洢����������riak��Ϣ
    std::vector<stru_partinfo> stru_partinfo_vector;

    AttributeType  col_type;
    int pack_no = 0;
    
    // 1> �ж�Ŀ����Ƿ����
    if(generate_exist_table(pnew_basepth,pnew_dbname,pnew_tbname)){
        lgprintf("Ŀ�����ݿ��[%s.%s]�Ѿ�����,���ܽ���ת����.",pnew_dbname,pnew_tbname);
        ret = -__LINE__;
        return ret;
    }

    // 2> ��ȡԴ��id�ʹ��ڶ�����
    ret = generate_get_table_info(pold_basepth,pold_dbname,pold_tbname,old_tabid,column_num);
    if(0 != ret){
        lgprintf("��ȡԴ��[%s.%s]�ṹ��Ϣʧ��.",pold_dbname,pold_tbname);
        ret = -__LINE__;
        generate_success = false;
        goto finish;
    }

    // 3> ������ṹ
    ret = generate_copy_frm_bht(pold_basepth,pold_dbname,pold_tbname,pnew_basepth, pnew_dbname, pnew_tbname,column_num);
    if(ret != 0){
        lgprintf("������[%s.%s]�ṹ-->[%s.%s]ʧ��.",pold_dbname,pold_tbname,pnew_dbname, pnew_tbname);
        ret = -__LINE__;
        generate_success = false;
        goto finish;
    }

    // 4> �޸ı�id�ͱ�����
    ret = generate_change_table_id(pnew_basepth,pnew_dbname, pnew_tbname);
    if(ret < 0){
        lgprintf("���±�[%s.%s]IDʧ��.",pold_dbname,pold_tbname,pnew_dbname, pnew_tbname);
        ret = -__LINE__;
        generate_success = false;
        goto finish;
    }
    new_tabid = ret;

    // 4> ����RSI��Ϣ
    lgprintf("��ʼ������[%s/%s/%s]��RSI�����ļ���Ϣ.",pold_basepth,pold_dbname,pold_tbname);
    ret = generate_copy_rsi(pold_basepth,pnew_basepth,old_tabid,new_tabid);
    if(ret != 0){   
        lgprintf("������[%s/%s/%s]��RSI�����ļ���Ϣʧ��.",pold_basepth,pold_dbname,pold_tbname);
        ret = -__LINE__;
        generate_success = false;
        goto finish;       
    }
    lgprintf("������[%s/%s/%s]��RSI�����ļ���Ϣ�ɹ�.",pold_basepth,pold_dbname,pold_tbname);

    // 5> �������ݽ���װ��
    lgprintf("��ʼ����[%s/%s/%s]������н���ת������[%s/%s/%s].",pold_basepth,pold_dbname,pold_tbname,pnew_basepth, pnew_dbname, pnew_tbname);

    // 5.1>�洢����������riak��Ϣ,���е�ת������
    for(int i=0;i<column_num;i++){
        int _column_num_index = i;
        lgprintf("��ʼ����[%s/%s/%s]�ĵ�[%d]�н���ת������[%s/%s/%s]�ĵ�[%d]��.",pold_basepth,pold_dbname,pold_tbname,_column_num_index,pnew_basepth, pnew_dbname, pnew_tbname,_column_num_index);
        stru_partinfo st_partition_info;
        st_partition_info.tabid = new_tabid;
        st_partition_info.attrid = _column_num_index;
        stru_partinfo_vector.push_back(st_partition_info);
        try{
            if(0 != generate_get_col_info(pold_basepth,pold_dbname,pold_tbname,_column_num_index, pack_no , col_type)){
                generate_success = false;
                ThrowWith("����[%s/%s/%s]�ĵ�[%d]�н���ת������[%s/%s/%s]�ĵ�[%d]��,��ȡ����Ϣʧ��.",pold_basepth,pold_dbname,pold_tbname,_column_num_index,pnew_basepth, pnew_dbname, pnew_tbname,_column_num_index);
            }
            
            
            ret = generate_riak_column(pold_basepth,pold_dbname,pold_tbname,_column_num_index, col_type,pack_no, pnew_basepth, pnew_dbname, pnew_tbname,stru_partinfo_vector[_column_num_index]);
            if(ret != 0){
                lgprintf("����[%s/%s/%s]�ĵ�[%d]�н���ת������[%s/%s/%s]�ĵ�[%d]��,װ��ʧ��.",pold_basepth,pold_dbname,pold_tbname,_column_num_index,pnew_basepth, pnew_dbname, pnew_tbname,_column_num_index);   
                generate_success = false;
                break;
            }else{
                lgprintf("����[%s/%s/%s]�ĵ�[%d]�н���ת������[%s/%s/%s]�ĵ�[%d]��,װ���ɹ�.",pold_basepth,pold_dbname,pold_tbname,_column_num_index,pnew_basepth, pnew_dbname, pnew_tbname,_column_num_index);   
            }
            
        }catch(int){
            generate_success = false;
            break;
        }catch(...){
            generate_success = false;
            break;
        }            
    }

    if(generate_success){// �ύkeyspace
        lgprintf("�Ѿ�����[%s/%s/%s]������н���ת������[%s/%s/%s]���,׼���ύRIAK��KeySpace.",pold_basepth,pold_dbname,pold_tbname,pnew_basepth, pnew_dbname, pnew_tbname);
        for(int j=0;j<stru_partinfo_vector.size();j++){
            ret = generate_riak_solid_keyspace(stru_partinfo_vector[j]);
            if(ret != 0){
                generate_success = false;                
                break;
            }
        }
    }

    if(generate_success){
        lgprintf("��[%s/%s/%s]��ת���Ѿ����,�ύRIAK��KeySpace�ɹ�.",pold_basepth,pold_dbname,pold_tbname,pnew_basepth, pnew_dbname, pnew_tbname);
    }
 
    if(!generate_success){// ʧ�ܾͻع�
        lgprintf("��[%s/%s/%s]��ת��ʧ��,�ع�RIAK��KeySpace.",pold_basepth,pold_dbname,pold_tbname,pnew_basepth, pnew_dbname, pnew_tbname);       
        generate_table_rallback(pnew_basepth, pnew_dbname, pnew_tbname,new_tabid);
        for(int j=0;j<stru_partinfo_vector.size();j++){
            generate_riak_free_keyspace(stru_partinfo_vector[j]);
        }
    }

finish:
    if(!generate_success){// ���Ŀ¼
        lgprintf("��[%s/%s/%s]��ת��ʧ��,�ع��������.",pnew_basepth, pnew_dbname, pnew_tbname);
        generate_table_rallback(pnew_basepth, pnew_dbname, pnew_tbname,new_tabid);
        lgprintf("��[%s/%s/%s]��ת��ʧ��,�ع�����������.",pnew_basepth, pnew_dbname, pnew_tbname);
        
        return ret;
    }
    
    lgprintf("��[%s/%s/%s]������н���ת������[%s/%s/%s]�ɹ�.",pold_basepth,pold_dbname,pold_tbname,pnew_basepth, pnew_dbname, pnew_tbname);
    
    return ret;
}

/*
    ����: ��ĳһ�� ��pack����Ϣ��ת������뵽riak��
    ����: 
            pold_basepth[in]:Դ���ݿ�$DATAMERGER_HOME/var Ŀ¼
            pold_dbname[in]:Դ���ݿ�����
            pold_tbname[in]:Դ���ݿ������
            col_num[in]:��Ҫת�����б�ţ���0��ʼ
            col_type[in]:������
            pack_num[in]:���������� ��pack����Ŀ
            pnew_basepth[in]:�����ݿ�$DATAMERGER_HOME/var Ŀ¼
            pnew_dbname[in]:�����ݿ�����
            pnew_tbname[in]:�����ݿ������
            st_partition_info[in/out]:������Ϣ,�ڲ��洢���д���riak�е�keyspace,���������е�ͳһ�ύ�ͻع�
    ˵��:
        1> ��ԴSWATCH_AB�ļ�,�ж�dpn�����һ��������ζ�ȡ��
        2> ��Դdpn�ļ��������ȡ������Ϣ
        3> ����Դdpn�ж�ȡ�������ݰ����ж��Ƿ����pack�ļ���
        4> ���Ѿ�����Դpack�ļ������ݰ����ж�ȡ���Զ�ȡ�����ݰ����ݱ��浽riak��
        5> ������Ϣ��dpn�ļ����ݣ�����ȡ��dpn��д�뵽�µ��ļ���
        6> ������Ϣ�ķ����ļ�
        7> ��ͬ�ķ�����Ҫ���и����첽�������С���ύ
*/
int generate_riak_column(const char* pold_basepth,const char* pold_dbname,const char* pold_tbname,
                        const int col_num,const AttributeType col_type,const int pack_num,
                        const char* pnew_basepth,const char* pnew_dbname,const char* pnew_tbname,
                        stru_partinfo& st_partition_info)
{
    // ------------------------------------------------------------------------
    // 1>  Դ��:dpn,part,pack �ļ�����
    stru_src_table_assist st_src_table_assist;    
    stru_dst_table_assist st_dst_table_assist;    
    bool has_catch_throw = false;
    std::string src_tmp = "";
    short len_mode = sizeof(short);

    try{
        // 1>  Դ��:dpn,part,pack �ļ�����
        sprintf(st_src_table_assist.table_path,"%s/%s/%s.bht",pold_basepth,pold_dbname,pold_tbname);
        
        src_tmp = DpnFileName(st_src_table_assist.table_path,col_num);
        strcpy(st_src_table_assist._s_d_p_item.fdpn_name,src_tmp.c_str());
        
        src_tmp = PartFileName(st_src_table_assist.table_path,col_num);
        strcpy(st_src_table_assist._s_d_p_item.fpart_name,src_tmp.c_str());

        
        // 1.1>  ��Դ��dpn,part
        st_src_table_assist._s_d_p_item.fdpn_handle = fopen(st_src_table_assist._s_d_p_item.fdpn_name,"rb");
        if(!st_src_table_assist._s_d_p_item.fdpn_handle){
            ThrowWith("�ļ�%s��ʧ��!\n",st_src_table_assist._s_d_p_item.fdpn_name);    
        }
        st_src_table_assist._s_d_p_item.fpart_handle= fopen(st_src_table_assist._s_d_p_item.fpart_name,"rb");
        if(!st_src_table_assist._s_d_p_item.fpart_handle){
            ThrowWith("�ļ�%s��ʧ��!\n",st_src_table_assist._s_d_p_item.fpart_name);    
        }

        // 1.3> ����Դ��part���ڴ�
        st_partition_info.LoadApPartInfo(st_src_table_assist._s_d_p_item.fpart_name,st_src_table_assist._s_d_p_item.fpart_handle);
        st_dst_table_assist.table_number = st_partition_info.tabid;
        st_dst_table_assist.attr_number = st_partition_info.attrid;
        
        // ------------------------------------------------------------------------
        // 2>  �±�:dpn,part,pack �ļ�����
        sprintf(st_dst_table_assist.table_path,"%s/%s/%s.bht",pnew_basepth,pnew_dbname,pnew_tbname);
        
        src_tmp = DpnFileName(st_dst_table_assist.table_path,col_num);
        strcpy(st_dst_table_assist._s_d_p_item.fdpn_name,src_tmp.c_str());
        
        src_tmp = PartFileName(st_dst_table_assist.table_path,col_num);
        strcpy(st_dst_table_assist._s_d_p_item.fpart_name,src_tmp.c_str());
            
        // 2.1> ��Ŀ����dpn,part
        st_dst_table_assist._s_d_p_item.fdpn_handle = fopen(st_dst_table_assist._s_d_p_item.fdpn_name,"wb");
        if(!st_dst_table_assist._s_d_p_item.fdpn_handle){
            ThrowWith("�ļ�%s��ʧ��!\n",st_dst_table_assist._s_d_p_item.fdpn_name);    
        }
        st_dst_table_assist._s_d_p_item.fpart_handle= fopen(st_dst_table_assist._s_d_p_item.fpart_name,"wb");
        if(!st_dst_table_assist._s_d_p_item.fpart_handle){
            ThrowWith("�ļ�%s��ʧ��!\n",st_dst_table_assist._s_d_p_item.fpart_name);    
        }

        // ------------------------------------------------------------------------
        // 3> ���������ͻ�ȡ���ݰ�����
        AttrPackType packtype;
        if (IsNumericType(col_type) ||IsDateTimeType(col_type)){
            packtype = PackN;
            len_mode = sizeof(short);
        }else{
            packtype = PackS;
            if(col_type == RC_BIN){
                len_mode = sizeof(uint);
            }
        }
        
        const int conDPNSize = 37;
        // ------------------------------------------------------------------------
        // 4> ����AB_SWITCH�ļ���ȡABS_STATE,����ȡ�ļ���Ϣ
        
        // ��dpn�ļ�����dpn�ж�ȡ����
        // ���һ������ȡ
        // 0:37 bytes :  ABS_A_STATE
        // 37:37 bytes :  ABS_B_STATE
        
        // ABS_A_STATE ---> A
        // ABS_B_STATE ---> B    
        ABSwitcher absw;
        ABSwitch cur_ab_switch_state = absw.GetState(st_src_table_assist.table_path);    
        int _pack_no = 0; // ��ǰ��ȡ����pack_no
        char dpn_buf[38];
        int ret = 0;

        // 4.1> ����������������£��Ƚ�ͷ2������λ��Ԥ�� ����
        if(pack_num >= 2)    {
            fseek(st_src_table_assist._s_d_p_item.fdpn_handle,2*conDPNSize,SEEK_SET);
            memset(dpn_buf,0,38);
            fwrite(dpn_buf,1,conDPNSize,st_dst_table_assist._s_d_p_item.fdpn_handle);
            fwrite(dpn_buf,1,conDPNSize,st_dst_table_assist._s_d_p_item.fdpn_handle);
        }

        // 4.2> ��dpn�������ȡÿһ�����ݰ�
        while(_pack_no < pack_num){
            if(_pack_no == pack_num - 1){// �Ƿ������һ����
                // û��switch_ab�ļ���ʱ�����һ������ȡ17-34�ֽ�
                if(cur_ab_switch_state == ABS_A_STATE){
                    fseek(st_src_table_assist._s_d_p_item.fdpn_handle,conDPNSize,SEEK_SET);
                    fseek(st_dst_table_assist._s_d_p_item.fdpn_handle,conDPNSize,SEEK_SET);
                }else{
                    fseek(st_src_table_assist._s_d_p_item.fdpn_handle,0,SEEK_SET);
                    fseek(st_dst_table_assist._s_d_p_item.fdpn_handle,0,SEEK_SET);
                }
            }

            // ��ȡ37���ֽ�(һ��������dpn��Ϣ)
            ret =fread(dpn_buf,conDPNSize,1,st_src_table_assist._s_d_p_item.fdpn_handle);
            bool is_stored = true;
            
            if(ret == 1){// ��ȡ�ɹ����ж��Ƿ�����Ч��
                char* buf=dpn_buf;
                int pack_file = *((int*) buf);
                int pack_addr = *((uint*) (buf + 4));
                _uint64 local_min = *((_uint64*) (buf + 8));
                _uint64 local_max = *((_uint64*) (buf + 16));
                _uint64 sum_size = 0;
                if(packtype == PackN){
                    sum_size = *((_uint64*) (buf + 24));
                }else{
                    sum_size = ushort(*((_uint64*) (buf + 24)));
                }
                ushort no_objs = *((ushort*) (buf + 32));
                ushort no_nulls = *((ushort*) (buf + 34));
                if(pack_file == PF_NULLS_ONLY){
                    no_nulls = no_objs + 1;
                }
                if(pack_file == PF_NULLS_ONLY || (packtype == PackN && local_min == local_max && no_nulls == 0)) {
                    is_stored = false;
                } else if(pack_file == PF_NO_OBJ) {
                    is_stored = false;
                } else {
                    is_stored = true;
                }

                // 4.3  ��Ҫ��ȡpack ���ݰ����ݣ������ݰ����ݴ���riak��
                if(is_stored){
                    src_tmp = AttrPackFileNameDirect(col_num,pack_file,st_src_table_assist.table_path);
                    // ���˵������Ҫ���µ��ļ�
                    if(strlen(st_src_table_assist.fpack_name) == 0 || strcmp(st_src_table_assist.fpack_name,src_tmp.c_str()) != 0){ 
                        strcpy(st_src_table_assist.fpack_name,src_tmp.c_str());
                        

                        // ��pack�ļ�
                        if(st_src_table_assist.fpack_handle != NULL){
                            fclose(st_src_table_assist.fpack_handle);
                            st_src_table_assist.fpack_handle = NULL;
                        }
                            
                        st_src_table_assist.fpack_handle = fopen(st_src_table_assist.fpack_name,"rb");
                        if(st_src_table_assist.fpack_handle == NULL){
                            ThrowWith("���ļ�%sʧ��!\n",st_src_table_assist.fpack_name);
                        }                            
                    }

                    
                    // 4.3.1> ���õ�ǰ��������Ӧ�ķ�������
                    bool new_partition_pack = false; // �ð��Ŷ�Ӧһ���µķ���
                    char* partition_name = st_partition_info.GetPartName(_pack_no);
                    if(strlen(st_dst_table_assist._s_d_p_item.fcurrent_part_name) == 0 ){ // ��һ�������Ŀ�ʼת��
                        st_src_table_assist._s_d_p_item.SetCurrentPartName(partition_name);
                        st_dst_table_assist._s_d_p_item.SetCurrentPartName(partition_name);
                      
                        pAsyncRiakHandle = _riak_cluster->AsyncInit(_riak_max_thread,1024,10);
                        
                        new_partition_pack = true;
                    }else if(strcmp(partition_name,st_dst_table_assist._s_d_p_item.fcurrent_part_name) != 0 ){ // ��һ������ת�����

                        // �ύ��һ��������װ���ļ�
                        {
                            std::string err_msg;
                            long ret = 0;
                            ret = _riak_cluster->AsyncWait(pAsyncRiakHandle,err_msg,(RIAK_ASYNC_WAIT_TIMEOUT_US / 1000000.0));
                            if(ret < 0){
                                ThrowWith(" Save pack error:%s ,AsyncWait return :%d",err_msg.c_str(),ret);
                            }
                            
                            // �ύriak�б����ļ���С
                            SaveRiakCommitSize(pnew_basepth,pnew_dbname,pnew_tbname,st_partition_info.tabid,st_dst_table_assist._s_d_p_item.GetCurrentPartName(),ret);
                        }
                        st_src_table_assist._s_d_p_item.SetCurrentPartName(partition_name);
                        st_dst_table_assist._s_d_p_item.SetCurrentPartName(partition_name);                        

                        pAsyncRiakHandle = _riak_cluster->AsyncInit(_riak_max_thread,1024,10);
                        new_partition_pack = true;
                    }

                    
                    // 4.3.2> ����pack_file �� pack_addr
                    if( new_partition_pack || st_dst_table_assist.ShouldAllocPNode()) { // ͬһ������,�Ѿ���������4096����
                        st_dst_table_assist.RiakAllocPNode();
                        // ���÷�����Ϣ,��riak��Ϣд�������
                        st_partition_info.AddRaikPnode(_pack_no,st_dst_table_assist.GetRiakPNode());
                    }else{
                    	// �Ѿ�������keyspace�ģ�ֱ��ʹ��
                    	st_dst_table_assist.AddRiakPNode();
                    }

                    //  4.3.3 > д��dpn ��Ϣ
                    {                        
                        *((int*) buf) = st_dst_table_assist.pack_file;
                        *((uint*) (buf + 4)) = st_dst_table_assist.pack_addr;
                        
                        ret = fwrite(dpn_buf,conDPNSize,1,st_dst_table_assist._s_d_p_item.fdpn_handle);
                        if(ret != 1){
                            ThrowWith("д���ļ�%sʧ��!\n",st_dst_table_assist._s_d_p_item.fdpn_name);    
                        }
                    }

                    // 4.4> ��ȡpack������,����riak����,��riak��д�����
                    {    
                        // �ƶ������ݵ�׼ȷλ��
                        fseek(st_src_table_assist.fpack_handle,pack_addr,SEEK_SET);

                        // ���� packn/packs
                        {
                            uint comp_buf_size,comp_null_buf_size,comp_len_buf_size;
                            comp_buf_size =comp_null_buf_size =comp_len_buf_size = 0;                        
                            unsigned char *compressed_buf = NULL;
                            uint buf_used;
                            uint* nulls = NULL;
                            uint data_full_byte_size = 0;
                            uint total_size = 0;
                            uchar optimal_mode = 0;
                            uint no_obj_in_pack=0;
                            uint no_nulls = 0;
                            
                            // ����riak��ʹ�õ�keyspace
                            std::string str_riak_key_value = st_dst_table_assist.Get_riak_key();
                            
                            if(packtype == PackN) {// read PackN and send to riak
                                void* data_full = NULL;
                                uchar head[17];
                                buf_used = fread(head,1,17,st_src_table_assist.fpack_handle);
                                if(buf_used < 17) {
                                    ThrowWith("Error: Cannot load or wrong header of data pack (AttrPackN).\n");
                                }
                                total_size = *((uint*) head);
                                optimal_mode = head[4];
                                no_obj_in_pack = *((ushort*) (head + 5));
                                ++no_obj_in_pack;
                                no_nulls = *((ushort*) (head + 7));
                                _uint64 max_val = *((_uint64*) (head + 9));
                                ValueType value_type = GetValueType(max_val);

                                if(ib_packn_compressmode_assist::IsModeNoCompression(optimal_mode)) { // ����Ҫ����ѹ�������ݰ�
                                    if(no_nulls) {
                                        nulls = (uint*)malloc(2048 * sizeof(uint));
                                        if(no_obj_in_pack < 65536){
                                            memset(nulls, 0, 8192);
                                        }
                                        fread((char*)nulls,1,((no_obj_in_pack+7)/8),st_src_table_assist.fpack_handle);
                                    }
                                    if(value_type * no_obj_in_pack) {
                                        data_full = malloc(value_type * no_obj_in_pack);
                                        fread((char*)data_full,1,value_type * no_obj_in_pack,st_src_table_assist.fpack_handle);
                                    }
                                    
                                    // ���ó�riak�ӿڽӿ���riak�з���
                                    AttrPackN_Save(no_nulls,nulls,data_full,no_obj_in_pack,optimal_mode,local_min,max_val,str_riak_key_value);
                                    
                                } else {
                                    if(total_size < 17) {
                                        ThrowWith("Wrong header of data pack in file %s \n",st_src_table_assist.fpack_name);
                                    }

                                    comp_buf_size = total_size - 17;
                                    compressed_buf = (uchar*) malloc((comp_buf_size + 1) * sizeof(uchar));
  
                                    if(!compressed_buf) {
                                        ThrowWith("Error: out of memory (%d bytes failed). (28)\n",comp_buf_size);
                                    }
                                    buf_used = fread((char*) compressed_buf,1,comp_buf_size,st_src_table_assist.fpack_handle);
                                    if(buf_used > comp_buf_size) {
                                        ThrowWith("Error: read compressed buf error");
                                    }

                                    // ��Ҫ��ѹ�����ݰ����н�ѹ��
                                    data_full_byte_size = value_type * no_obj_in_pack;
                                    data_full = malloc(data_full_byte_size);
                                    int _no_nulls = no_nulls;
                                    
                                   	if(nulls == NULL && _no_nulls>0){
                                    	nulls = (uint*)malloc(2048 * sizeof(uint));
	                                    if(no_obj_in_pack < 65536){
	                                        memset(nulls, 0, 8192);
	                                    }
                                    }	
                                    
                                    ret = ib_packn_decompress(compressed_buf,comp_buf_size,optimal_mode,nulls,_no_nulls,data_full,(int&)no_obj_in_pack);
                                    if(ret != 0){
                                        ThrowWith("ib_packn_decompress error return %d. ",ret);
                                    }                

                                    // ���ó�riak�ӿڽӿ���riak�з���
                                    AttrPackN_Save(_no_nulls,nulls,data_full,no_obj_in_pack,optimal_mode,local_min,max_val,str_riak_key_value);
                                    
                                }
                                
                                if(data_full != NULL){
                                    free(data_full);
                                    data_full = NULL;
                                }
                            }else { // PackS
                                uchar *data = NULL;    
                                void *lens = NULL;
                                uint decomposer_id = 0;
                                uchar no_groups = 0;

                                no_nulls = 0;
                                optimal_mode = 0;
                                uchar prehead[9];
                                buf_used = fread(prehead,1,9,st_src_table_assist.fpack_handle);                            
                                uint total_size = *((uint *)prehead);
                                uchar tmp_mode = prehead[4];
                                no_obj_in_pack= *((ushort*)(prehead + 5));
                                ++no_obj_in_pack;
                                no_nulls = *((ushort*)(prehead + 7));
                                int header_size;
                                uchar ver;
                                
                                if(tmp_mode >= 8 && tmp_mode <=253) {
                                    ver = tmp_mode;
                                    uchar head[10];
                                    buf_used = fread(head,1,10,st_src_table_assist.fpack_handle);
                                    optimal_mode = head[0];
                                    
                                    decomposer_id = *((uint*)(head + 1));
                                    no_groups = head[5];
                                    data_full_byte_size = *((uint*)(head + 6));
                                    header_size = 19;
                                } else {
                                    ver = 0;
                                    optimal_mode = tmp_mode;
                                    uchar head[4];
                                    buf_used = fread(head,1,4,st_src_table_assist.fpack_handle);
                                    data_full_byte_size = *((uint*)(head));
                                    header_size = 13;
                                }
                                if(!ib_packs_compressmode_assist::IsModeValid(optimal_mode)) {
                                    ThrowWith("Error: Unexpected byte in data pack (AttrPackS).\n");
                                } else {
                                    int _no_nulls = no_nulls;
                                    if(data == NULL) {
	                                   if(data_full_byte_size) {
                                          data = (uchar*)malloc(data_full_byte_size);
                                          if(NULL == data){
                                             ThrowWith("Error: out of memory ( %d bytes failed). (40).\n",data_full_byte_size);
                                          }
                                       }
                                    }
                                    lens = malloc(len_mode * no_obj_in_pack * sizeof(char));
                                    if(no_nulls > 0) {
                                        if(!nulls){
                                            nulls = (uint*) malloc(2048 * sizeof(uint));
                                        }
                                        if(no_obj_in_pack < 65536){
                                            memset(nulls, 0, 8192);
                                        }
                                    }

                                    if(ib_packs_compressmode_assist::IsModeNoCompression(optimal_mode)){ // û��ѹ����,ֱ�Ӷ�ȡ����
                                        if(data){
                                            fread((char*) *data,1,data_full_byte_size,st_src_table_assist.fpack_handle);
                                        }
                                        if(nulls){
                                            fread((char*) nulls,1,(no_obj_in_pack + 7) / 8,st_src_table_assist.fpack_handle);
                                        }
                                        if(lens){
                                            fread((char*) lens,1,len_mode * no_obj_in_pack,st_src_table_assist.fpack_handle);
                                        }
                                        
                                        // ���ó�riak�ӿڽӿ���riak�з���
                                        AttrPackS_Save(no_nulls,nulls,data,data_full_byte_size,len_mode,lens,decomposer_id,no_groups,no_obj_in_pack,optimal_mode,ver,str_riak_key_value);
                                    }else{
                                        // normal compression
                                        if(ib_packs_compressmode_assist::IsModeCompressionApplied(optimal_mode)) // 0,1    = Huffmann 16-bit compression of remainings with tree (0 - flat null encoding, 1 - compressed nulls)
                                        {
                                            comp_buf_size = total_size - header_size;
                                            compressed_buf = (uchar*)malloc((comp_buf_size + 1) * sizeof(uchar));

                                            if(!compressed_buf) {
                                                ThrowWith("Error: out of memory (%d bytes failed). (32)\n",comp_buf_size+1);
                                            }
                                            
                                            buf_used = fread((char*)compressed_buf,1,comp_buf_size,st_src_table_assist.fpack_handle);

                                            // �����ݰ����н�ѹ����                                     
                                            ret = ib_packs_decompress(compressed_buf,comp_buf_size,no_obj_in_pack,optimal_mode,data,data_full_byte_size,len_mode,lens,nulls,_no_nulls);
                                            if(ret != 0){
                                                ThrowWith("Error:ib_packs_decompress error.");
                                            }

                                            // ���ó�riak�ӿڽӿ���riak�з���
                                            AttrPackS_Save(_no_nulls,nulls,data,data_full_byte_size,len_mode,lens,decomposer_id,no_groups,no_obj_in_pack,optimal_mode,ver,str_riak_key_value);
            
                                        } 
                                        else {
                                            ThrowWith("The process reached to an invalid code path! (LoadData)\n");
                                        }
                                    }

                                    if(lens != NULL){
                                        free(lens);
                                        lens = NULL;
                                    }
                                    if(data != NULL){
                                        free(data);
                                        data = NULL;
                                    }                    
                                }
                            } // end if packs

                            if(compressed_buf != NULL){
                                free(compressed_buf);
                                compressed_buf = NULL;
                            }
                            if(nulls != NULL){
                                free(nulls);
                                nulls = NULL;
                            }
                        }
                    }
                    
                }
                else
                {
                    // ֱ��д��dpn�ļ��Ϳ���
                    ret = fwrite(dpn_buf,conDPNSize,1,st_dst_table_assist._s_d_p_item.fdpn_handle);
                    if(ret != 1){
                        ThrowWith("д���ļ�%sʧ��!\n",st_dst_table_assist._s_d_p_item.fdpn_name);
                    }
                }
            }    else    {
                ThrowWith("��ȡ�ļ�%sʧ��\n",st_src_table_assist._s_d_p_item.fdpn_name);
            }
            
            _pack_no ++;
        }

        // ���������Ϣ,����riak�汾��ʹ�õ�partition��Ϣ
        st_partition_info.SaveRiakPartInfo(st_dst_table_assist._s_d_p_item.fpart_name,st_dst_table_assist._s_d_p_item.fpart_handle);


        // �ύ��һ������ת����Ĵ���riak�еĴ�С
        long _current_save_size = 0;
        if((pack_num >0) && (strlen(st_dst_table_assist._s_d_p_item.GetCurrentPartName())>0))
        {
            std::string err_msg;
            _current_save_size = _riak_cluster->AsyncWait(pAsyncRiakHandle,err_msg,(RIAK_ASYNC_WAIT_TIMEOUT_US / 1000000.0));
            if(_current_save_size < 0){
                ThrowWith(" Save pack error:%s ,AsyncWait return :%d",err_msg.c_str(),_current_save_size);
            }    
            SaveRiakCommitSize(pnew_basepth,pnew_dbname,pnew_tbname,st_partition_info.tabid,st_dst_table_assist._s_d_p_item.GetCurrentPartName(),_current_save_size);
        }else if((pack_num >0) && (strlen(st_dst_table_assist._s_d_p_item.GetCurrentPartName()) == 0)){
            // û����riak�б�������ݰ���(ֻ��DPN�д�������),��¼riak�еİ���СΪ0
            _current_save_size = 0;
            char* partition_name = st_partition_info.GetPartName((_pack_no-1));
            SaveRiakCommitSize(pnew_basepth,pnew_dbname,pnew_tbname,st_partition_info.tabid,partition_name,_current_save_size);
        }     
              
    }catch(...){
        has_catch_throw = true;
        goto finish;   
    }

finish:
    if(st_src_table_assist.fpack_handle != NULL){
        fclose(st_src_table_assist.fpack_handle);
        st_src_table_assist.fpack_handle = NULL;
    }
    if(st_src_table_assist._s_d_p_item.fdpn_handle != NULL){
        fclose(st_src_table_assist._s_d_p_item.fdpn_handle);
        st_src_table_assist._s_d_p_item.fdpn_handle = NULL;
    }
    if(st_src_table_assist._s_d_p_item.fpart_handle!= NULL){
        fclose(st_src_table_assist._s_d_p_item.fpart_handle);
        st_src_table_assist._s_d_p_item.fpart_handle = NULL;
    }

    if(st_dst_table_assist._s_d_p_item.fdpn_handle != NULL){
        fclose(st_dst_table_assist._s_d_p_item.fdpn_handle);
        st_dst_table_assist._s_d_p_item.fdpn_handle = NULL;
    }
    if(st_dst_table_assist._s_d_p_item.fpart_handle!= NULL){
        fclose(st_dst_table_assist._s_d_p_item.fpart_handle);
        st_dst_table_assist._s_d_p_item.fpart_handle = NULL;
    }

    if(has_catch_throw){
        ThrowWith("generate_riak_column throw");
    }
    return 0;
}

void DecodePacksTest(const char *datafile,int seek_pos)
{ // PackS
    uchar *data = NULL;    
    void *lens = NULL;
    uint decomposer_id = 0;
    uchar no_groups = 0;

    int no_nulls = 0;
    uchar optimal_mode = 0;
    uchar prehead[9];
    FILE *fp=fopen(datafile,"rb");
    assert(fp);
    fseek(fp,seek_pos,SEEK_SET);
    int buf_used = fread(prehead,1,9,fp); 
    assert(buf_used==9);
    uint total_size = *((uint *)prehead);
    uchar tmp_mode = prehead[4];
    int no_obj_in_pack= *((ushort*)(prehead + 5));
    ++no_obj_in_pack;
    no_nulls = *((ushort*)(prehead + 7));
    int header_size;
    uchar ver;
    int data_full_byte_size=0;
    if(tmp_mode >= 8 && tmp_mode <=253) {
        ver = tmp_mode;
        uchar head[10];
        buf_used = fread(head,1,10,fp);
        optimal_mode = head[0];
        
        decomposer_id = *((uint*)(head + 1));
        no_groups = head[5];
        data_full_byte_size = *((uint*)(head + 6));
        header_size = 19;
    } else {
        ver = 0;
        optimal_mode = tmp_mode;
        uchar head[4];
        buf_used = fread(head,1,4,fp);
        data_full_byte_size = *((uint*)(head));
        header_size = 13;
    }
    int len_mode=2;
    uint *nulls=NULL;
    if(!ib_packs_compressmode_assist::IsModeValid(optimal_mode)) {
        ThrowWith("Error: Unexpected byte in data pack (AttrPackS).\n");
    } else {
        int _no_nulls = no_nulls;
        if(data == NULL) {
           if(data_full_byte_size) {
              data = (uchar*)malloc(data_full_byte_size);
              if(NULL == data){
                 ThrowWith("Error: out of memory ( %d bytes failed). (40).\n",data_full_byte_size);
              }
           }
        }
        lens = malloc(len_mode * no_obj_in_pack * sizeof(char));
        if(no_nulls > 0) {
            if(!nulls){
                nulls = (uint*) malloc(2048 * sizeof(uint));
            }
            if(no_obj_in_pack < 65536){
                memset(nulls, 0, 8192);
            }
        }
        int comp_buf_size=0;
        uchar *compressed_buf=NULL;
        if(ib_packs_compressmode_assist::IsModeNoCompression(optimal_mode)){ // û��ѹ����,ֱ�Ӷ�ȡ����
            if(data){
                fread((char*) *data,1,data_full_byte_size,fp);
            }
            if(nulls){
                fread((char*) nulls,1,(no_obj_in_pack + 7) / 8,fp);
            }
            if(lens){
                fread((char*) lens,1,len_mode * no_obj_in_pack,fp);
            }
        }else
        {
            // normal compression
            if(ib_packs_compressmode_assist::IsModeCompressionApplied(optimal_mode)) // 0,1    = Huffmann 16-bit compression of remainings with tree (0 - flat null encoding, 1 - compressed nulls)
            {
                comp_buf_size = total_size - header_size;
                compressed_buf = (uchar*)malloc((comp_buf_size + 1) * sizeof(uchar));

                if(!compressed_buf) {
                    ThrowWith("Error: out of memory (%d bytes failed). (32)\n",comp_buf_size+1);
                }
                
                buf_used = fread((char*)compressed_buf,1,comp_buf_size,fp);

                // �����ݰ����н�ѹ����                                     
                int ret = ib_packs_decompress(compressed_buf,comp_buf_size,no_obj_in_pack,optimal_mode,data,data_full_byte_size,len_mode,lens,nulls,_no_nulls);
                if(ret != 0){
                    ThrowWith("Error:ib_packs_decompress error.");
                }

            } 
            else {
                ThrowWith("The process reached to an invalid code path! (LoadData)\n");
            }
        }

        if(lens != NULL){
            free(lens);
            lens = NULL;
        }
        if(data != NULL){
            free(data);
            data = NULL;
        }   
        if(nulls != NULL){
            free(nulls);
            nulls = NULL;
        }
        if(compressed_buf != NULL){
            free(compressed_buf);
            compressed_buf = NULL;
        }
    }
}


std::string GetRiakKey(const int64 packno)
{
    char riak_key_buf[32]; 
    std::string ret; 
    snprintf(riak_key_buf, sizeof(riak_key_buf), "P%lX-%X", packno>>12,(int)(packno&0x00000fff));
    ret = riak_key_buf; 
    return ret;
}

int AttrPackN_Save(const int no_nulls,         /*:    null ����             */
                    const uint *nulls,            /*:    nulls �洢����         */    
                    const void* data_full,        /*:    ��������            */
                    const int no_obj,            /*: ���ݰ��ж�����        */
                    const uchar optimal_mode,        /*: ѹ��״̬ѡ��        */ 
                    const _uint64 minv,            /*: �����е���Сֵ        */
                    const _uint64 maxv,            /*: �����е����ֵ        */
                    const std::string& str_riak_key_node)        /*: riak �д洢��key*/
{
    uint total_size = 1;
    msgRCAttr_packN msg_pack;
    std::string riak_value;
    int ret = 0;

    msg_pack.set_no_obj(no_obj - 1);
    msg_pack.set_no_nulls(no_nulls);
    msg_pack.set_optimal_mode(optimal_mode);
    msg_pack.set_min_val(minv);
    msg_pack.set_max_val(maxv);
    msg_pack.set_total_size(total_size);
    
    ValueType value_type = GetValueType(maxv);

    if(no_nulls)
        msg_pack.set_nulls(nulls, (no_obj + 7) / 8);

    if(data_full)
        msg_pack.set_data(data_full, value_type * no_obj);

    std::string riak_params = "";
    msg_pack.SerializeToString(&riak_value);

#ifdef PRINT_LOG_INFO
	lgprintf("AttrPackN_Save, riakkey = %s , value_size = %d\n", str_riak_key_node.c_str(), (int)(riak_value.size()));
#endif

    if(pAsyncRiakHandle == NULL){
        ret = _riak_cluster->Put(RIAK_CLUSTER_DP_BUCKET_NAME, str_riak_key_node, riak_value, &riak_params);
    }else{
        ret = _riak_cluster->AsyncPut(pAsyncRiakHandle,RIAK_CLUSTER_DP_BUCKET_NAME, str_riak_key_node, riak_value, &riak_params);
    }
    if (ret != 0) {
        std::string errmsg;
        char errmsg_buf[128];
        snprintf(errmsg_buf, sizeof(errmsg_buf), "Error: PackN [%s] save failed, ret [%d], msg [%s]",
            str_riak_key_node.c_str(), ret, strerror(ret));
        errmsg = errmsg_buf;
        ThrowWith("%s\n",errmsg.c_str());
    }

    return ret;
}

// ����packn��
int AttrPackN_Save_2(const int no_nulls,         /*:    null ����             */    
                    const char* compressed_buf,   /*:    ѹ�����ݰ�           */
                    const uint compressed_buf_len,/*:    ѹ�������ݳ��� 	*/
                    const int no_obj,            /*: ���ݰ��ж�����        */
                    const uchar optimal_mode,        /*: ѹ��״̬ѡ��        */ 
                    const _uint64 minv,            /*: �����е���Сֵ        */
                    const _uint64 maxv,            /*: �����е����ֵ        */
                    const std::string& str_riak_key_node,        /*: riak �д洢��key*/
                    const std::string& riak_params) /*: ѹ�����Ͳ��� */
{
    ThrowWith("generate_table::AttrPackN_Save_2 ���ô���.");
    return 0;
}



int AttrPackS_Save(const int no_nulls,         /*:    null ����             */
                    const uint *nulls,            /*:    nulls �洢����         */    
                    const uchar* data,            /*:    ��������            */
                    const uint data_full_byte_size,/*: �������ݳ���*/
                    const short len_mode,
                    const void* lens,  
                    const uint decomposer_id,
                    const uchar no_groups,
                    const int no_obj,            /*: ���ݰ��ж�����        */
                    const uchar optimal_mode,        /*: ѹ��״̬ѡ��        */ 
                    const uchar ver,        /*: �����е����ֵ        */
                    const std::string& str_riak_key_node)        /*: riak �д洢��key*/
{
    msgRCAttr_packS msg_pack;
    std::string riak_value;
    msg_pack.set_no_obj(no_obj - 1);
    msg_pack.set_no_nulls(no_nulls);
    msg_pack.set_optimal_mode(optimal_mode);
    msg_pack.set_decomposer_id(decomposer_id);
    msg_pack.set_no_groups(no_groups);
    msg_pack.set_data_full_byte_size(data_full_byte_size);
    msg_pack.set_ver(ver);
    std::string riak_params = " ";
		
    if(data) {
		std::string tmp_data;    	
    	tmp_data.append((char*)data,(int)data_full_byte_size);
        msg_pack.set_data(tmp_data);
    }

    if(nulls) {
        msg_pack.set_nulls(nulls, (no_obj + 7) / 8);
    }

    if(lens) {
        msg_pack.set_lens(lens, len_mode * no_obj);
    }

    if (!msg_pack.SerializeToString(&riak_value)) {
        printf("Serialize error at: %s\n", str_riak_key_node.c_str());
    }

#ifdef PRINT_LOG_INFO
	lgprintf("AttrPackS_Save, riakkey = %s , value_size = %d\n", str_riak_key_node.c_str(), (int)(riak_value.size()));
#endif

    int ret = 0;
    if(pAsyncRiakHandle == NULL){
        ret = _riak_cluster->Put(RIAK_CLUSTER_DP_BUCKET_NAME, str_riak_key_node, riak_value, &riak_params);
    }else{
        ret = _riak_cluster->AsyncPut(pAsyncRiakHandle,RIAK_CLUSTER_DP_BUCKET_NAME, str_riak_key_node, riak_value, &riak_params);
    }
    if (ret != 0) {
        std::string errmsg;
        char errmsg_buf[128];
        snprintf(errmsg_buf, sizeof(errmsg_buf), "Error: PackS [%s] save failed, ret [%d], msg [%s]",str_riak_key_node.c_str(), ret, strerror(ret));
        errmsg = errmsg_buf;
        ThrowWith("%s\n",errmsg.c_str());
    }

    return 0;
}


int AttrPackS_Save_2(const int no_nulls,         /*:    null ����             */
                    const char* compressed_buf,   /*:    ѹ�����ݰ�           */
                    const uint compressed_buf_len,/*:    ѹ�������ݳ��� 	*/
                    const uint data_full_byte_size,/*: �������ݳ���*/
                    const short len_mode,
                    const uint decomposer_id,
                    const uchar no_groups,
                    const int no_obj,            /*: ���ݰ��ж�����        */
                    const uchar optimal_mode,        /*: ѹ��״̬ѡ��        */ 
                    const uchar ver,        
                    const std::string& str_riak_key_node,        /*: riak �д洢��key*/
                    const std::string& riak_params) /*: ѹ�����Ͳ��� */
{
    ThrowWith("generate_table::AttrPackS_Save_2 ���ô���.");
    return 0;
}
                    


typedef struct stru_table_path
{
    char _pbasepth[300];
    char _pdbname[300];
    char _ptbname[300];
    stru_table_path(const char* pbasepth,const char* pdbname,const char* ptbname){
        strcpy(_pbasepth, pbasepth);      
        strcpy(_pdbname, pdbname);
        strcpy(_ptbname, ptbname);
    }
}stru_table_path,*stru_table_path_ptr;
void DecodePacksTest(const char *datafile,int seek_pos);

int main(int argc,char *argv[]) {
    WOCIInit("generate_table/generate_table_");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(false); 
    if(argc == 3) {
        DecodePacksTest(argv[1],atoi(argv[2]));           
        WOCIQuit(); 
        return 0;        
    }
    else if(argc != 6){
        lgprintf("��DMA�����汾���ƶ�ת�����ֲ�ʽ�汾,�����������!\n");
        lgprintf("����˵��: ");
        lgprintf("        ./generate_table Դ���ݿ�����·��  Դ���ݿ����� Դ���ݿ������ Ŀ�����ݿ�����·�� Ŀ�����ݿ����� ");
        lgprintf("        src_path:Դ���ݿ�����·��,ͨ��Ϊ$DATAMERGER_HOME/var");
        lgprintf("        src_dbname:Դ���ݿ�����");
        lgprintf("        src_tbname:Դ���ݿ������");
        lgprintf("        dst_path:Ŀ�����ݿ�����·��,ͨ��Ϊ$DATAMERGER_HOME/var");
        lgprintf("        dst_dbname:Ŀ�����ݿ�����");
        lgprintf("����:");
        lgprintf("        ./generate_table /home/dma_new/dma/var dbtest dp_datapart /home/dma_new/dma_riak_test/var dbtest");
        WOCIQuit(); 
        return -__LINE__;
    }

    // Դ���Ŀ���ṹ
    stru_table_path st_src_table(argv[1],argv[2],argv[3]);
    stru_table_path st_dst_table(argv[4],argv[5],argv[3]);
    
  
    long ret = 0;
    pthread_once(&_riak_cluster_init_once, _riak_cluster_init);    
    if(_riak_cluster == NULL){
        ThrowWith("Ŀ��������Ӷ���ʧ��,�����˳�.");
    }

    // �첽���߳���Ŀ����,�첽save��ʱ���߳������ܳ���riak�ڵ�����4��
    int cfree = 0;
    getfreecpu(cfree);
    int maxthread= cfree;
    if(_riak_connect_number>0){
        maxthread = (_riak_connect_number > cfree) ? (_riak_connect_number):(cfree);
    }else{ 
        maxthread = (_riak_nodes > cfree) ? (_riak_nodes):(cfree);
    }
    _riak_max_thread = maxthread;
    
    // ��ʼ��dma�����汾�ı�ת���ɷֲ�ʽ�汾�ı�
    ret = generate_riak_table(st_src_table._pbasepth,st_src_table._pdbname,st_src_table._ptbname,
                              st_dst_table._pbasepth,st_dst_table._pdbname,st_dst_table._ptbname);
finish:
    if(_riak_cluster!= NULL){
        delete _riak_cluster;
        _riak_cluster = NULL;
    }

    WOCIQuit(); 
    return 1;
}




