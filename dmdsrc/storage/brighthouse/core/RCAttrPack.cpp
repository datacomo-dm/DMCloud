/* Copyright (C)  2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free  Software Foundation.

This program is distributed in the hope that  it will be useful, but
WITHOUT ANY WARRANTY; without even  the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License version 2.0 for more details.

You should have received a  copy of the GNU General Public License
version 2.0  along with this  program; if not, write to the Free
Software Foundation,  Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA  */

#include "RCAttrPack.h"
#include "RCAttr.h"
#include "core/tools.h"
#include "DataCache.h"
#include "loader/RCAttrLoadBase.h"
#include "edition/core/Transaction.h"

RiakCluster *_riak_cluster = NULL;
pthread_once_t _riak_cluster_init_once = PTHREAD_ONCE_INIT;
int _riak_connect_number = 0;
int _riak_max_thread = 0; 
int _riak_nodes = 0;		// bhloader 装入使用


#ifndef NDEBUG
int trace_tabid=-1,trace_attrn=-1,trace_packn=-1;
std::string trace_nodeaddr;
std::string *checkTraceRiakNode(int tabid,int attrnum,int packno) {
	if(trace_tabid==-1) {
		const char *stabid=getenv("RIAK_TRACE_TABID");
		if(stabid) {
			trace_tabid=atoi(stabid);
			trace_attrn=atoi(getenv("RIAK_TRACE_ATTRID"));
			trace_packn=atoi(getenv("RIAK_TRACE_PACKID"));
			trace_nodeaddr=getenv("RIAK_TRACE_NODE");
		}
		else trace_tabid=-2;
	}
	if(tabid==trace_tabid && attrnum==trace_attrn && packno==trace_packn)
		return &trace_nodeaddr;
	else return NULL;
}
#endif


std::string _get_riak_key(const int tbno,const int colno,const int64 packno)
{
	char riak_key_buf[32]; 
	std::string ret; 
	//snprintf(riak_key_buf, sizeof(riak_key_buf), "P%lX-%X", packno>>12,(int)(packno>>52));
	//format:[63-12][11-0] bit 
	snprintf(riak_key_buf, sizeof(riak_key_buf), "P%lX-%X", packno>>12,(int)(packno&0xfff));
	ret = riak_key_buf; 
	return ret;
}

void _riak_cluster_init()
{
	char *riak_hostname;
	char *riak_port_str;
	int fret;

	riak_hostname = getenv(RIAK_HOSTNAME_ENV_NAME);
	riak_port_str = getenv(RIAK_PORT_ENV_NAME);

	if (!riak_hostname) {
		fprintf(stderr, "Must set $%s\n", RIAK_HOSTNAME_ENV_NAME);
		exit(1);
	}

	// 每一个riak节点的连接线程池数目(riak 驱动部分的连接池数)
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

	// 连接riak数目
	const char* priak_connect_number = getenv(RIAK_CONNECT_THREAD_NUMBER_ENV_NAME);
	if(NULL != priak_connect_number)
		_riak_connect_number = atoi(priak_connect_number);

	// 启动的线程数的设置
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

bool
riak_compress_value(std::string &riak_value)
{
	bool ret = false;

	size_t compressed_msg_len = snappy_max_compressed_length(riak_value.size());
	char *compressed_msg = (char*)malloc(compressed_msg_len);
	if (snappy_compress(riak_value.data(), riak_value.size(), compressed_msg, &compressed_msg_len)
	  == SNAPPY_OK) {
		riak_value.assign(compressed_msg, compressed_msg_len);
		ret = true;
	}
	free(compressed_msg);

	return ret;
}

bool
riak_decompress_value(std::string &riak_value)
{
	bool ret = false;

	size_t uncompressed_length;
	if (snappy_uncompressed_length(riak_value.data(), riak_value.size(), &uncompressed_length)
	  == SNAPPY_OK) {
		char* uncompressed_buf = (char*)malloc(uncompressed_length);
		if (snappy_uncompress(riak_value.data(), riak_value.size(), uncompressed_buf, &uncompressed_length)
		  == SNAPPY_OK) {
			riak_value.assign(uncompressed_buf, uncompressed_length);
			ret = true;
		}
		free(uncompressed_buf);
	}

	return ret;
}


using namespace boost;

//CRITICAL_SECTION rc_save_pack_cs;

AttrPack::AttrPack(PackCoordinate pc, AttributeType attr_type, int inserting_mode, bool no_compression, DataCache* owner)
	:	m_prev_pack(NULL), m_next_pack(NULL), table_number(0),attr_number(0),pack_no(0), is_only_compressed(false),
		inserting_mode(inserting_mode), no_compression(no_compression), attr_type(attr_type), cr_result(CRResultPtr(new CRResult()))
{
    if(_riak_cluster == NULL){
                _riak_cluster_init();
    }
    assert(_riak_cluster != NULL);  
	pAsyncRiakHandle = NULL;
	nulls = 0;
	table_number=pc[0];
	attr_number=pc[1];
	pack_no=pc[2];
	compressed_buf = 0;
	is_empty = true;
	previous_size = 0;
	_logical_coord.ID=bh::COORD_TYPE::PACK;
	_logical_coord.co.pack = pc;
	riak_pack_no = pack_no; // 没有删除过分区的表
	first_obj=0;
	is_cq=false;is_rq=false;
	//SynchronizedDataCache::GlobalDataCache().PutAttrPackRO(table, column, dp, this);
}

AttrPack::AttrPack(const AttrPack &ap) :
	TrackableObject( ap ),
		table_number(ap.table_number),
		attr_number(ap.attr_number),
		pack_no(ap.pack_no),
		no_nulls(ap.no_nulls),
		nulls_bytes(ap.nulls_bytes),
		no_obj(ap.no_obj),
		is_empty(ap.is_empty),
		compressed_up_to_date(ap.compressed_up_to_date),
		saved_up_to_date(ap.saved_up_to_date),
		comp_buf_size(ap.comp_buf_size),
		comp_null_buf_size(ap.comp_null_buf_size),
		comp_len_buf_size(ap.comp_len_buf_size),
		is_only_compressed(ap.is_only_compressed),
		previous_size(ap.previous_size),
		inserting_mode(ap.inserting_mode),
		no_compression(ap.no_compression),
		attr_type(ap.attr_type)
{
	nulls = 0;
	compressed_buf = 0;
	pAsyncRiakHandle = ap.pAsyncRiakHandle;
	
	if(ap.compressed_buf) {
		compressed_buf = (uchar*)alloc(comp_buf_size, BLOCK_COMPRESSED);
		memcpy(compressed_buf, ap.compressed_buf, comp_buf_size);
	}

	if(ap.nulls) {
		nulls_bytes = ap.nulls_bytes;
		nulls = (uint8_t*)alloc(nulls_bytes, BLOCK_UNCOMPRESSED);
		memcpy(nulls, ap.nulls, nulls_bytes);
	}
	m_lock_count = 1;
	first_obj=ap.first_obj;
	riak_pack_no = pack_no; // 没有删除过分区的表
	is_cq=ap.is_cq;is_rq=ap.is_rq;
}

void AttrPack::Release() 
{ 
	if(owner) owner->DropObjectByMM(GetPackCoordinate()); 
}

AttrPack::~AttrPack()
{
	dealloc(nulls);
	dealloc(compressed_buf);
	nulls = 0;
	compressed_buf = 0;
}

bool AttrPack::UpToDate()				// return 1 iff there is no need to save
{
	if(!compressed_up_to_date)
		saved_up_to_date = false;
	return saved_up_to_date;
}

bool AttrPack::ShouldNotCompress()
{
	return (no_compression || (inserting_mode == 1 && no_obj < 0x10000));
}

void
AttrPack::SetNull(int ono)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!is_only_compressed);
	if (!this->nulls) {
		this->nulls = (uint8_t*) alloc(8192, BLOCK_UNCOMPRESSED);
		memset(this->nulls, 0, 8192);
		this->nulls_bytes = 8192;
	}

	if (!CR_Class_NS::BMP_BITTEST(this->nulls, this->nulls_bytes, ono, this->first_obj)) {
		CR_Class_NS::BMP_BITSET(this->nulls, this->nulls_bytes, ono, this->first_obj);
		this->no_nulls++;
	} else {
		// maybe set by last failed load
		//BHASSERT(0, "Expression evaluation failed!");
	}
}
