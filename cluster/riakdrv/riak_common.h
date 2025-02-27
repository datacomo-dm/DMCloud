#ifndef __RIAK_COMMON_H__
#define __RIAK_COMMON_H__

#define RIAK_CONNECT_MEMORY_BUCKET_NAME		"memory"
#define RIAK_CONNECT_TRANSFER_COMPRESS_MIN_SIZE	(16384)

#define RIAK_CONNECT_FLAG_RETURN_HEAD		(0x00000001U)
#define RIAK_CONNECT_FLAG_RETURN_BODY		(0x00000002U)
#define RIAK_CONNECT_FLAG_PUT_IF_NONE_MATCH	(0x00000004U)
#define RIAK_CONNECT_FLAG_PUT_IF_NOT_MODIFIED	(0x00000008U)
#define RIAK_CONNECT_FLAG_PUT_DIRECT		(0x00000010U)

#define RIAK_LOCK_SUFFIX			".lock-5g73p62W5r0jupME"
#define RIAK_KEYINFO_PREFIX			".keyi-3RtWuaQjEDP1DKn9"

#define RIAK_BACKEND_NAME_LEVELDB		"eleveldb_default"
#define RIAK_BACKEND_NAME_MEMORY		"memory_cache"

#define RIAK_META_NAME_PRIVATE_PUT_ID		"CR-Riak-Private-PUT-ID"
#define RIAK_META_NAME_VALUE_VERSION		"CR-Riak-Value-Version"
#define RIAK_META_NAME_STORED_SIZE		"CR-Riak-Stored-Size"
#define RIAK_META_NAME_CRUDE_SIZE		"CR-Riak-Crude-Size"
#define RIAK_META_NAME_VALUE_HASH		"CR-Riak-Value-Hash"
#define RIAK_META_NAME_TRANSFER_COMPRESS	"CR-Riak-Transfer-Compress"
#define RIAK_META_NAME_SAVE_COMPRESS		"CR-Riak-Save-Compress"
#define RIAK_META_NAME_KS_EXPIRE		"CR-Riak-KS-Expire"
#define RIAK_META_NAME_KEY_STATUS		"CR-Riak-Key-Status"

#define RIAK_META_VALUE_TRUE			"TRUE"
#define RIAK_META_VALUE_FALSE			"FALSE"

#define RIAK_META_VALUE_VALID			"VALID"
#define RIAK_META_VALUE_PENDING			"PENDING"
#define RIAK_META_VALUE_DISCARD			"DISCARD"

#define RIAK_CLUSTERINFO_STR_ENVNAME		"RIAK_CLUSTERINFO_STR"
#define RIAKC_DEFAULT_SRV_NAME			"8186"
#define RIAKC_INDEX_NAME_CLUSTER_NODES		"CLUSTER_NODES"
#define RIAKC_META_NAME_HOST_NAME		"HOSTNAME"
#define RIAKC_META_NAME_SRV_NAME		"SRVNAME"

#define RIAK_CLUSTER_KS_USERIAK_ENVNAME		"RIAK_CLUSTER_KS_USE_RIAK"

#define RIAK_CLUSTER_KS_INDEX_DUP_TIMES		(5)
#define RIAK_CLUSTER_KS_LOCK_LEVEL		(9)
#define RIAK_CLUSTER_KS_OP_TIMEOUT_USEC		(3600UL * 1000 * 1000)
#define RIAK_CLUSTER_KS_TIMEOUT_SEC_ENVNAME	"RIAKC_KS_TIMEOUT_SEC"
#define RIAK_CLUSTER_KS_LOCK_TIMEOUT_ENVNAME	"RIAKC_KS_LOCK_TIMEOUT"
#define RIAK_CLUSTER_KS_MINIMUM_ALLOC_ENVNAME	"RIAKC_KS_MINIMUM_ALLOC"
#define RIAK_CLUSTER_KS_BASENAME		"RIAKC_KS."
#define RIAK_CLUSTER_KS_TEMP_SUFFIX		".TEMP"
#define RIAK_CLUSTER_NEXT_KS_ID_BASENAME	"next_ks_id."
#define RIAK_CLUSTER_KS_INDEX_NAME		"riakc_keyspace"
#define RIAK_CLUSTER_KS_INDEX_NAME_TIMEOUT	"TIMEOUT"
#define RIAK_CLUSTER_KS_INDEX_NAME_KEY		"KEY"
#define RIAK_CLUSTER_KS_INDEX_NAME_VALUE	"VALUE"
#define RIAK_CLUSTER_KS_INDEX_NAME_HASH		"HASH"
#define RIAK_CLUSTER_KS_INDEX_NAME_TABLE_ID	"TABLE_ID"
#define RIAK_CLUSTER_KS_INDEX_NAME_COLUMN_ID	"COLUMN_ID"

#endif /* __RIAK_COMMON_H__ */
