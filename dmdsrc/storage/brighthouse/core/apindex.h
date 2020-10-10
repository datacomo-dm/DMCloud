#ifndef AP_INDEX_HEADER
#define AP_INDEX_HEADER

#include <assert.h>
#include <string.h>
#include <leveldb/db.h>
#include <util/hash.h>
#include <leveldb/write_batch.h>
#include <leveldb/slice.h>
#include <leveldb/filter_policy.h>
#include <leveldb/cache.h>
#include <vector>
#include <iostream>
#include <set>
#include <map>
#include <string>
#include <unordered_map>
/* "readdir" etc. are defined here. */
#include <dirent.h>
/* limits.h defines "PATH_MAX". */
#include <limits.h>
#include <sys/sendfile.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <cr_class/cr_class.h>
// very slow for sparse_hash_map<string,string>: 4's--21s
//#include <sparsehash/sparse_hash_map>
//#include <sparsehash/dense_hash_map>

//#include <mm/cache_map.hpp>

#include "DMThreadGroup.h"
//������һ��װ����̲�Ҫ��֣�һ��Write
//521K
//#define MAX_JOBS (2<<20) 
#define MAX_JOBS (1<<19) 
#define MAX_WRITE_BATCH_MEM_SIZE	(1024*1024*1024)	//1G
//#define MAX_JOBS 4096*16 
#define MAX_PART_NAME_LEN 100
//using google::sparse_hash_map;
//using google::dense_hash_map;
// leveldb is thread safe,but not multi-process ready(can not open more than one times paralle)
class apindex;
struct DBEntry {
	leveldb::DB* db;
	leveldb::Options *options;
	// set bfmode before create option
	DBEntry() {db=NULL;options=NULL;}
  void CreateOption() {
  	options=new leveldb::Options();
  	if(bfmode){
		 options->filter_policy = leveldb::NewBloomFilterPolicy(10);
  	}
	options->create_if_missing = true;  
	// 32MB write buffer ( up to two write buffer) 
	options->block_cache=leveldb::NewLRUCache(32 << 20) ;
	options->write_buffer_size = 32<<20;
	options->max_open_files=32768;
  }
  
  void Release() {
		if(db){
			delete db;
		}
		if(options) {
			if(options->filter_policy){
				delete options->filter_policy;
				options->filter_policy = NULL;
			}
			if(options->block_cache){
				delete options->block_cache;
				options->block_cache = NULL;
			}			
		  	delete options;
		}
		db=NULL;options=NULL;
  }
	int refct;
	bool bfmode;
	std::vector<apindex *> _refs;
	~DBEntry() {
  	//if(bfmode)
    // delete options.filter_policy;
		// can not delete here as copy safe not imple.
		//delete db;
	} 
};

class AutoLock{
        DMMutex *plock; 
public:
        AutoLock(DMMutex *p):plock(p){plock->Lock();}
        ~AutoLock(){plock->Unlock();}
};


#define STRING_KEY

#ifndef LEVELDB_SLICE_HASH
#define LEVELDB_SLICE_HASH		
namespace std
{
   template<>
   struct hash<leveldb::Slice>: public std::unary_function<leveldb::Slice, size_t>
   {
      size_t operator()(leveldb::Slice key) const{
      size_t hash = 5381;
      int c;
	  const char *data=key.data();
      while(c = *data++)
        hash = ((hash << 5) + hash) + c; // hash * 33 + c 
      return hash;
     }
  };
};
#endif 

class apindex {
 std::string dbname;
 std::string path;
 int attr_num;
 char part_name[MAX_PART_NAME_LEN];
 leveldb::DB* db;
 static DMMutex  fork_lock;
 
 leveldb::Status status;
 leveldb::WriteBatch *pbatch;
 long pbatch_mem_size;//�ڴ��С

#ifdef 	STRING_KEY
 //typedef mm::cache_map <std::string,std::string> BATCH_MAP;
 //typedef dense_hash_map <std::string,std::string> BATCH_MAP;

 typedef std::unordered_map<std::string,std::string> BATCH_MAP;
#else
// BUG:lost key differ
 typedef std::map<leveldb::Slice,std::string> BATCH_MAP;
 //typedef std::unordered_map<leveldb::Slice,std::string> BATCH_MAP;
#endif
 BATCH_MAP *p_batch_map;
 
 int batch_jobct;
 int dupwt;
 int savedpack;
 class DBPool {
	std::map<std::string,DBEntry > pool;
  DMMutex  pool_lock;
	leveldb::Status status;
public :
	DBPool()
	{
	}
	~DBPool() {
		AutoLock lock(&pool_lock);
		std::map<std::string,DBEntry >::iterator iter=pool.begin();
		while(iter!=pool.end()) {
			iter->second.Release();
			pool.erase(iter++);
		}
	}
	
	leveldb::Options *GetOptions(std::string &dbname) {
        AutoLock lock(&pool_lock);
		std::map<std::string,DBEntry >::iterator iter=pool.find(dbname);
		if(iter==pool.end()) return NULL;
		return iter->second.options;
	}
	
	void Retrieve(std::string &dbname,apindex *mdb,bool bfmode) {
        std::map<std::string,DBEntry >::iterator iter;
		{
			AutoLock lock(&pool_lock);
			iter=pool.find(dbname);
		}
		if(iter==pool.end() || iter->second.db==NULL) {
			DBEntry dbe;
		    
		    dbe.bfmode=bfmode;
		    dbe.CreateOption();
			 // leveldb ���ܴ�2��(��ͬ����Ҳ������)
   			status = leveldb::DB::Open(*dbe.options,dbname, &dbe.db);
   			if(!status.ok()) {
				throw "Open leveldb failed.";
			}
			dbe.refct=1;
			dbe._refs.push_back(mdb);
			AutoLock lock(&pool_lock);
			if(iter==pool.end()) 
			 pool.insert(std::pair<std::string,DBEntry>(dbname,dbe));
			else 
			 iter->second=dbe;
			mdb->SetDB(dbe.db);
		}
		else {
			AutoLock lock(&pool_lock);
			iter->second._refs.push_back(mdb);
			iter->second.refct++;
			mdb->SetDB(iter->second.db);
		}
	}
	
	void Release(std::string &dbname,apindex *mdb) {
        AutoLock lock(&pool_lock);
		std::map<std::string,DBEntry >::iterator iter=pool.find(dbname);
		//assert(iter!=pool.end());
		// RCTableImpl::LoadDirtyData release RCAttr cause this
		if(iter==pool.end()) return;
		// remove reference list element
		for(int i=0;i<iter->second._refs.size();i++) {
			if(iter->second._refs[i]==mdb) {
				mdb->SetDB(NULL);
				iter->second._refs.erase(iter->second._refs.begin()+i);
			}
		}
		iter->second.refct--;
		if(iter->second.refct==0) {
			iter->second.Release();
			pool.erase(iter);
		}
	}
	
	//force remove from pool
	int RemoveDB(std::string &dbname,std::vector<apindex *> &refs) {
    AutoLock lock(&pool_lock);
		std::map<std::string,DBEntry >::iterator iter=pool.find(dbname);
	
		if(iter==pool.end()) {
			throw "leveldb pool find failed.";
		}

		int oldrefct=iter->second.refct;
		refs=iter->second._refs;
		//delete db maybe waiting on write data complete,store to a temporary var ,release after delete db
		iter->second.Release();
		for(int i=0;i<iter->second._refs.size();i++) {
			iter->second._refs[i]->SetDB(NULL);
		}
		pool.erase(iter);
		return oldrefct;
	}
	
	void SetRef(int refct,std::string &dbname,std::vector<apindex *> &refs) {
    AutoLock lock(&pool_lock);
		std::map<std::string,DBEntry >::iterator iter=pool.find(dbname);
		if(iter==pool.end()) {
			throw "leveldb pool find failed.";
		}
		iter->second.refct=refct;
		iter->second._refs=refs;
		//update refs
		for(int i=0;i<iter->second._refs.size();i++) {
			iter->second._refs[i]->SetDB(iter->second.db);
		}
	}
		
};

 static apindex::DBPool ap_pool;
 // adding k/v only store new value of the key!
 //׷��ģʽ  ������¼������packs ,��Add����ʱ������ԭ�е�packs
 // Ҫ��ͬһ��pack�����ظ���add��
 bool appendmode;
 bool writemode;
 bool bfmode; //blooming filter mode
// set containers are generally slower than unordered_set containers to access individual elements by their key, but they allow the direct iteration on subsets based on their order.
//Sets are typically implemented as binary search trees.
 //std::map<int,unsigned int> keyinapack;
 
public :
	
	void SetDB(leveldb::DB *pdb) {db=pdb;}
 
 apindex() {
 	pbatch=NULL;
 	p_batch_map = NULL;
 	pbatch_mem_size = 0;
 	Init();
 	bfmode=true;
 }

 apindex(std::string _dbname,std::string _basepath,bool _bfmode=true) {
 	pbatch=NULL;
 	p_batch_map = NULL;
 	pbatch_mem_size = 0;
   Init();
   bfmode=_bfmode;
   dbname=_dbname;
   if(_basepath.length()==0)
	   path=_dbname;
   else path=_basepath+"/"+_dbname;
   Open();
 }
 
 bool isempty() {
 	leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
	it->SeekToFirst(); 
	bool retval=it->Valid();
	delete it;
	return !retval;
 }

 apindex(const apindex &ap) {
   pbatch=NULL;
   p_batch_map = NULL;
   pbatch_mem_size = 0;
   Init();
   bfmode=ap.bfmode;
   attr_num=ap.attr_num;
   writemode=ap.writemode;
   strcpy(part_name,ap.part_name);
   dbname=ap.dbname;//.assign(fn,strlen(fn));
   path=ap.path;
   Open();
 }
 
 apindex(int attrnum,int partnum,std::string _basepath,bool _writemode,bool _bfmode=true) {
   pbatch=NULL;
   p_batch_map = NULL;
   pbatch_mem_size = 0;
   Init();
   bfmode=_bfmode;
   attr_num=attrnum;
   writemode=_writemode;
   sprintf(part_name,"%05d",partnum);
   char fn[300];
   sprintf(fn,"%d_%s%s",attrnum,part_name,writemode?"_w":"");
   dbname=fn;//.assign(fn,strlen(fn));
   path=_basepath+dbname;
   Open();
 }
 
 static std::string GetDBPath(int attrnum,const char * partname,std::string _basepath,bool _writemode)
 { 
	 std::string dbpath;
   char fn[300];
   sprintf(fn,"%d_%s%s",attrnum,partname,_writemode?"_w":"");
   dbpath=_basepath+fn;
   return dbpath;
 }

 std::string GetPath() {
 	return path;
 }

 apindex(int attrnum,const char * partname,std::string _basepath,bool _writemode,bool _bfmode=true) {
   pbatch=NULL;
   p_batch_map = NULL;
   pbatch_mem_size = 0;
   Init();
   bfmode=_bfmode;
   attr_num=attrnum;
   strcpy(part_name,partname);
   writemode=_writemode;
   char fn[300];
   sprintf(fn,"%d_%s%s",attrnum,part_name,writemode?"_w":"");
   dbname=fn;//.assign(fn,strlen(fn));
   path=_basepath+dbname;
   Open();
 }
 void Init() {
  if(pbatch) delete pbatch;
  pbatch=new leveldb::WriteBatch();
  
  if(p_batch_map) delete p_batch_map;
  p_batch_map = new BATCH_MAP;
  pbatch_mem_size = 0;
  dupwt=0;attr_num=-1;savedpack=-1;
  //default false for append mode
  appendmode=false;
  p_batch_map->clear();
 }
 bool SetSavePack(int n) {
 	if(savedpack>=n) return false;
	savedpack=n;
	return true;
 }

 void SetAppendMode(bool _v) {
 	appendmode=_v;
 }
 
 void RemovePath(const std::string path) {
 	std::string cmd="/bin/sh -c -- 'rm -rf ";
 	cmd+=path;
 	cmd+="'";
 	system(cmd.c_str());
 }

 void ClearDB(bool reopen=true) {
 	std::vector<apindex *> refs;
	int refct=ap_pool.RemoveDB( path,refs);
 	RemovePath(path);
 	if(reopen) {
 		Open();
	  ap_pool.SetRef(refct,  path,refs);
	}
	else db=NULL;
 }
 
 static int BLCopyFile(const char* source, const char* destination)
 {
    //Here we use kernel-space copying for performance reasons

    int input, output;
		struct stat filestat;
		stat(source, &filestat);
    if( (input = open(source, O_RDONLY)) == -1)
        return 0;

    if( (output = open(destination, O_WRONLY | O_CREAT,S_IRUSR|S_IWUSR|S_IRGRP)) == -1)
    {
        close(input);
        return 0;
    }

    off_t bytesCopied=0;

    int result = sendfile(output, input, &bytesCopied, filestat.st_size) == -1;

    close(input);
    close(output);

    return result;
 }
 
 // remove directory
 static void RemoveDir(const char *srcpath) {
 	DIR * d;
	d = opendir (srcpath);
	if(!d) return;
	while(1) {
		struct dirent * entry;
		const char * d_name;
		/* "Readdir" gets subsequent entries from "d". */
    entry = readdir (d);
    if (! entry) {
      /* There are no more entries in this directory, so break
          out of the while loop. */
       break;
    }
    d_name = entry->d_name;
    int path_length;
    char path[PATH_MAX];
    path_length = snprintf (path, PATH_MAX,
                            "%s/%s", srcpath, d_name);
    if (entry->d_type & DT_DIR) {
 /* Check that the directory is not "d" or d's parent. */
            if (strcmp (d_name, "..") != 0 &&
                strcmp (d_name, ".") != 0) {
                /* Recursively call "list_dir" with the new path. */
                RemoveDir (path);
            }
    }
    else 
    	unlink(path);
  }
  rmdir(srcpath);
 }
 
 // link sst && copy all others files
 static void CopyDBFiles(const char *srcpath,const char *dstpath) {
 	DIR * d;
	d = opendir (dstpath);
	if(!d)
   mkdir(dstpath, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  else closedir(d);
	d = opendir (srcpath);
	//srcpath maybe a empty db(no directory)
	if(!d) return;
	while(1) {
		struct dirent * entry;
		const char * d_name;
		/* "Readdir" gets subsequent entries from "d". */
    entry = readdir (d);
    if (! entry) {
      /* There are no more entries in this directory, so break
          out of the while loop. */
       break;
    }
    d_name = entry->d_name;
    int path_length;
    char path[PATH_MAX];
    path_length = snprintf (path, PATH_MAX,
                            "%s/%s", srcpath, d_name);
    char newpath[PATH_MAX];
    snprintf(newpath,PATH_MAX,"%s/%s",dstpath,d_name);
    if (entry->d_type & DT_DIR) {
 /* Check that the directory is not "d" or d's parent. */
            if (strcmp (d_name, "..") != 0 &&
                strcmp (d_name, ".") != 0) {
                /* Recursively call "list_dir" with the new path. */
                CopyDBFiles (path,newpath);
            }
    }
    else {
        // leveldb 1.10 �汾�������ļ�*.sst
        // leveldb 1.15 �汾�������ļ�*.ldb
        if(strlen(d_name)>4 && 
			(strcmp(d_name+strlen(d_name)-4,".sst")==0 ||
			 strcmp(d_name+strlen(d_name)-4,".ldb")==0 )) {
			// create hard link 
            link(path,newpath);
        }
        else {
            BLCopyFile(path,newpath);
        }
    }
  }
  closedir(d);
 }
 
 // Fork a new db with common data (sst file)
 static void ForkDB(const char *db,const char *newdb) {
 	//Reload to merge log file
 	//ReloadDB();
 	// close db to prepare fork
 	//std::vector<apindex *> refs;
	//int refct=ap_pool.RemoveDB( path,refs);
  AutoLock al(&fork_lock);
  RemoveDir(newdb);
  CopyDBFiles(db,newdb);
 	//Open();
	//ap_pool.SetRef(refct,  path,refs);
 }
 
 void ReplaceDB(const char *replacebypath) {
 	std::vector<apindex *> refs;
	int refct=ap_pool.RemoveDB( path,refs);
	RemovePath(path);
	rename(replacebypath,path.c_str());
 	Open();
	ap_pool.SetRef(refct,  path,refs);
 }

 void UpdateBWmemSize(const leveldb::Slice& key,const leveldb::Slice& value){
     pbatch_mem_size += key.size();
     pbatch_mem_size += value.size();      	
 }
 bool ShouldWriteBW(){ 	return pbatch_mem_size>=MAX_WRITE_BATCH_MEM_SIZE;};

  long GetMapSize() {
  	return p_batch_map==NULL?0:p_batch_map->size(); 
  }
 
 void ReloadDB() {
 	std::vector<apindex *> refs;
	int refct=ap_pool.RemoveDB( path,refs);
 	Open();
	ap_pool.SetRef(refct,  path,refs);
 }
 
 void Open()
 {
   ap_pool.Retrieve( path ,this,bfmode);
   BatchBegin();
 }
 
 ~apindex() {
  BatchWrite();
  ap_pool.Release(path,this);
  delete pbatch;
  delete p_batch_map;
 }
 // deprecated
 void PackBegin() {
  //keyinapack.clear();
 }
 
 void ScanDB(int &rownum,int &itemsnum) {
  leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
  rownum=0;
  itemsnum=0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const int *pval=(const int *)((const char *)it->value().data());
    // adjust packs offset to absolute packs offset
    int size=it->value().size()/sizeof(int);
    rownum++;
    itemsnum+=size;
  }
  delete it;
 }
 // Dump db to a text file ,a line for a record(k/v)
 // type: 1,int; 2,long ;3,double ;4,varchar
 void DumpDB(const char *filename,int keytype) {
   FILE *fp=fopen(filename,"w+t");	
   assert(fp);
   leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
   for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const int *pval=(const int *)((const char *)it->value().data());
    // adjust packs offset to absolute packs offset
    int size=it->value().size()/sizeof(int);

    //const char *key=it->key().data();    
    std::string key(it->key().data(),it->key().size());
    const int *pvalend=pval+size;
    char kstring[1000];
    if(keytype==1) {
        _int64 int_key = CR_Class_NS::str2i64(key);
        sprintf(kstring,"%d",int_key);
    }
    else if(keytype==2){
        _int64 long_key = CR_Class_NS::str2i64(key);
        sprintf(kstring,"%ld",long_key);
    }
    else if(keytype==3){
        // TODO
        sprintf(kstring,"%f",*((const double *)key.data()));
    }
    else if(keytype==4) {
        memcpy(kstring,key.data(),it->key().size());
        kstring[it->key().size()]=0;
    }
    
    else assert(0);
    fprintf(fp,"key %s ,value ",kstring);
    while(pval<pvalend) {
     	int pks=*pval;
     	fprintf(fp," %d",pks);
    	pval++;
    }
    fputs("\n",fp);
   }
   fclose(fp);
   delete it;
 }
 
 //merge apindex,keep exists items!
 //  ��ԭ�������Ѿ���KEYʱ:
 // appendmode: ԭ�м�¼��packs��������¼��packs������
 //  ����������¼ֱ��׷�ӵ�ԭ�е�packs
 // ����(appendmode==false):�ٶ���Ҫ�ϲ������ݺ�HashMap����ͬ��key�Ͻ���������һ�����ظ�
 
 void Import(apindex &apidx) {
	leveldb::Iterator* it = apidx.db->NewIterator(leveldb::ReadOptions());
	BatchBegin();
  	for (it->SeekToFirst(); it->Valid(); it->Next()) {
    	if(!appendmode) {
	 		 // for a specified key ,adding new value
	 		 std::string value;
			 // find old packs for this key
		     status = db->Get(leveldb::ReadOptions(), it->key(), &value);
		     if(status.ok()){
		    		const int *pv=(const int *)it->value().data();
		    		int lastkey=*(const int *)(value.data()+value.size()-sizeof(int));
					if(*pv<lastkey) {
						throw "new pack value number less than last loaded pack.";
					}
		    		//first of iter->second equals last of value ?
		    		bool rep=(*pv==lastkey);
		    		value.append((const char *)(rep?(pv+1):pv),it->value().size()-(rep?sizeof(int):0));
		    		UpdateBWmemSize(it->key(),leveldb::Slice(value));
		  	 		pbatch->Put(it->key(),leveldb::Slice(value));
		     }
		     else{ 
		     	UpdateBWmemSize(it->key(),it->value());
				pbatch->Put(it->key(),it->value());     // add to batch writer
			}
	    }
	    else {
	    	// appendmode :ֱ�Ӱ�����׷�ӵ�ԭ�еļ�¼��
	    	UpdateBWmemSize(it->key(),it->value());
	    	pbatch->Put(it->key(),it->value());
	    }
	    batch_jobct++;
	    
	    if(batch_jobct>=MAX_JOBS || ShouldWriteBW()) {
	    	BatchWrite();
	    	batch_jobct=0;
	    }

  }
  assert(it->status().ok());  // Check for any errors found during the scan
  delete it;
  BatchWrite();
 }

 // call MergeSrcDbtoMap to prepare merged hashmap before this one
 //   separator merge and write and reduce lock time
 //  batch_map will be cleared after import to db
 void ImportHash(apindex &apidx) {
 	BatchBegin();
	std::string value;
    BATCH_MAP::iterator iter;
    for(iter = apidx.p_batch_map->begin();iter!=apidx.p_batch_map->end();iter++)
    {
    	leveldb::Slice keyslice(iter->first);
		pbatch->Put(keyslice, leveldb::Slice(iter->second));
		UpdateBWmemSize(keyslice, leveldb::Slice(iter->second));
    	batch_jobct++;
    
    	if(batch_jobct>=MAX_JOBS || ShouldWriteBW()) {
    		BatchWrite();
    	}
    }
 	BatchWrite();
	apidx.p_batch_map->clear();
	delete apidx.p_batch_map;
	apidx.p_batch_map = new BATCH_MAP;
 }
/* deprecated!
 // key type must same in src/new
 // merge srcdb and newdb to this object
 //  overlaped values on duplicated keys will be checked ,but only remove at most one elements
 // TODO: packs duplicate on all element,change to check only edge one
 void Merge(apindex &srcdb,apindex &newdb) {
	ClearDB();
	leveldb::Iterator* it = srcdb.db->NewIterator(leveldb::ReadOptions());
	leveldb::Iterator* itnew = newdb.db->NewIterator(leveldb::ReadOptions());
	BatchBegin();
	std::string newvalue;
	std::string value;
    itnew->SeekToFirst();
  	for (it->SeekToFirst(); it->Valid(); it->Next()) {
		// for a specified key ,adding new value
		// find key in newdb
		while(itnew->Valid() && itnew->key().compare(it->key())<0) {
			pbatch->Put(itnew->key(),itnew->value());
			itnew->Next();
		}
			
		if(itnew->Valid() && itnew->key().compare(it->key())==0) {
			// same key in both db,merge them
			std::set<int> packs;
			value.assign(it->value().data(),it->value().size());
			const int *pval=(const int *)((const char *)value.data());
			int size=value.size()/sizeof(int);
			const int *pvalend=pval+size;
			while(pval<pvalend) {
				packs.insert(*pval++);
			}
			// check exists of new packs and update pack set
			//pval=(const int *)newvalue.data();
			pval=(const int *)itnew->value().data();
			//size=newvalue.size()/sizeof(int);
			size=itnew->value().size()/sizeof(int);
			pvalend=pval+size;
			while(pval<pvalend) {
			   if(packs.count(*pval)==0) {
				int pks=*pval;
				packs.insert(pks);
				value.append((const char *)&pks,sizeof(int));
			   }
			   pval++;
			}
			pbatch->Put(it->key(),leveldb::Slice(value.data(),value.size()));
			itnew->Next();
		}
		else
			pbatch->Put(it->key(),it->value());
    	batch_jobct++;
    	if(batch_jobct>=MAX_JOBS) {
    		BatchWrite();
    		batch_jobct=0;
    	}
	}
  	for (;itnew->Valid(); itnew->Next()) {
        pbatch->Put(itnew->key(),itnew->value());
        batch_jobct++;
        if(batch_jobct>=MAX_JOBS) {
           BatchWrite();
           batch_jobct=0;
        }
  	}
  	assert(it->status().ok());  // Check for any errors found during the scan
  	delete it;
	delete itnew;
  	BatchWrite();
 }
*/
 // key type must same in src/new
 // merge srcdb and newdb to this object
 //  overlaped values on duplicated keys will be checked ,but only remove at most one elements
 // clear newdb after merge.
 void MergeFromHashMap(apindex &srcdb,apindex &newdb) {
	ClearDB();
	leveldb::ReadOptions opts;
	opts.fill_cache=false;
	leveldb::Iterator* it = srcdb.db->NewIterator(opts);
	BATCH_MAP::iterator itnew;
	// newdb is static during this oper
	BatchBegin();
	std::string newkey;
	std::string newvalue;
  	for (it->SeekToFirst(); it->Valid(); it->Next()) {
		// for a specified key ,adding new value
		// find key in newdb
		newkey.assign(it->key().data(),it->key().size());
		itnew=newdb.p_batch_map->find(newkey);
		if(itnew!=newdb.p_batch_map->end()) {
			const int *pv=(const int *)itnew->second.data();
    		int lastkey=*(const int *)(it->value().data()+it->value().size()-sizeof(int));
			if(*pv<lastkey) {
				throw "new pack value number less than last loaded pack.";
			}
    		//first of iter->second equals last of value ?
    		bool rep=*pv==lastkey;
			newvalue.assign(it->value().data(),it->value().size());
    		newvalue.append((const char *)(rep?(pv+1):pv),itnew->second.size()-(rep?sizeof(int):0));
			pbatch->Put(it->key(), leveldb::Slice(newvalue));
			UpdateBWmemSize(it->key(), leveldb::Slice(newvalue));
			//nvalue=leveldb::Slice(itnew->second);
			//pbatch->Put(it->key(),nvalue);
			newdb.p_batch_map->erase(itnew);
		}
		else{
			UpdateBWmemSize(it->key(),it->value());
			pbatch->Put(it->key(),it->value());
		}
		
	    batch_jobct++;
	    if(batch_jobct>=MAX_JOBS || ShouldWriteBW()) {
	    	BatchWrite();
	    	batch_jobct=0;
	    }
	}
  	for (itnew=newdb.p_batch_map->begin();itnew!=newdb.p_batch_map->end();itnew++) {
		pbatch->Put(leveldb::Slice(itnew->first),leveldb::Slice(itnew->second));
		UpdateBWmemSize(leveldb::Slice(itnew->first),leveldb::Slice(itnew->second));
        batch_jobct++;
        if(batch_jobct>=MAX_JOBS || ShouldWriteBW()) {
           BatchWrite();
           batch_jobct=0;
        }
  	}
  	
  	assert(it->status().ok());  // Check for any errors found during the scan
  	delete it;
 	BatchWrite();
	newdb.p_batch_map->clear();
 }

 const char *MergeIndexPath(char *name)
 {
 	return GetMergeIndexPath(path.c_str(),name);
 }

 static const char *GetMergeIndexPath(const char *spath,char *name) {
	 sprintf(name,"%s_mrg",spath);
	 return name;
 }
 
 void RebuildByTruncate(int *packvars,int packvsize,apindex &srcdb)  {
	ClearDB();
	leveldb::ReadOptions opts;
	opts.fill_cache=false;
	leveldb::Iterator* it = srcdb.db->NewIterator(opts);
	BatchBegin();
	std::string _value;
  	for (it->SeekToFirst(); it->Valid(); it->Next()) {
  		_value.assign(it->value().data(),it->value().size());
		//int *pv=(int *)(it->value().data());
		int *pv=(int*)(_value.c_str());
		// int psize=it->value().size()/sizeof(int);
		int psize=_value.size()/sizeof(int);
		int *pvend=pv+psize;
		while(pv<pvend) {
            // try to fix dma-1223,modify by liujs
            // packvars ��˳���������,�����ǰ,С���ں�
            // e.g: {1836, 153, 918, 153, 153, 153}
            // ���pvΪ919����Ҫ��ȥ153,�ڼ�ȥ153,ֻ�ܴӴ���ȼ�ȥ
            int *packvars_ref = packvars;
            int *pvarlast=packvars_ref+packvsize*2-2;
            
            while(packvars_ref<=pvarlast){
                if(*pv>=*packvars_ref) {
                    *pv-=packvars_ref[1];
                }                
                packvars_ref+=2;
            }
            pv++;              
		}
		//pbatch->Put(it->key(),it->value());		
		pbatch->Put(it->key(),leveldb::Slice(_value.data(),_value.size()));
		UpdateBWmemSize(it->key(),leveldb::Slice(_value.data(),_value.size()));
	    batch_jobct++;
	    if(batch_jobct>=MAX_JOBS || ShouldWriteBW()) {
	    	BatchWrite();
	    	batch_jobct=0;
	    }
	}
  	assert(it->status().ok());  // Check for any errors found during the scan
  	delete it;
 	BatchWrite();
 }
// merge srcdb to batch_map ,��ԭ���ݿ��ֵ�ϲ����ڴ�����
// ����:�ٶ���Ҫ�ϲ������ݺ�HashMap����ͬ��key�Ͻ���������һ�����ظ�
 void MergeSrcDbtoMap(apindex &srcdb) {
	std::string value;
    BATCH_MAP::iterator iter;
   leveldb::ReadOptions opts;
   	opts.fill_cache=false;
    for(iter = p_batch_map->begin();iter!=p_batch_map->end();iter++)
    {
#ifdef STRING_KEY
    	leveldb::Slice keyslice(iter->first);
#else
		leveldb::Slice keyslice(iter->frist);
#endif
		// �����ݿ��н��в��ң��ҵ��ͽ��кϲ������������ݿ��кϲ����ڴ�
    	status = srcdb.db->Get(opts, keyslice, &value);
        std::string  &update_value = iter->second;
        	
    	if(status.ok())// ԭ����ֵ�Ļ�ȡkey��Ӧ��pack
    	{ 	
    		const int *pv=(const int *)iter->second.data();
    		int lastkey=*(const int *)(value.data()+value.size()-sizeof(int));
			if(*pv<lastkey) {
				throw "new pack value number less than last loaded pack.";
			}
    		//first of iter->second equals last of value ?
    		bool rep=*pv==lastkey;
    		value.append((const char *)(rep?(pv+1):pv),iter->second.size()-(rep?sizeof(int):0));
    		(*p_batch_map)[iter->first]=value;	    	
    	} // end if(status.ok())
    	
	} // end for(iter = p_batch_map->begin();iter!=p_batch_map->end();iter++)
 }

 
 void BatchBegin() {
 	//pbatch->Clear();
 	delete pbatch;
 	pbatch=new leveldb::WriteBatch();
 	batch_jobct=0;
 	pbatch_mem_size = 0;
 }

 void ClearMap() {
	//p_batch_map->clear();
    delete p_batch_map;
    p_batch_map = new BATCH_MAP;
 }

 leveldb::WriteBatch &GetBatch() {
 	return *pbatch;
 } 
 
 // Dot forget to clear batch_jobct in owner of _batch !
 void BatchWrite(leveldb::WriteBatch &_batch) {
  status = db->Write(leveldb::WriteOptions(), &_batch);
  assert(status.ok());
  _batch.Clear();
  pbatch_mem_size = 0;
 }
 
 void BatchWrite() {
  //>> begin:fix dma-705
  if(batch_jobct<1 ) {
  		BatchBegin();
  		ClearMap();
  		return;
	}
	BATCH_MAP::iterator iter;
	for(iter = p_batch_map->begin();iter != p_batch_map->end();iter++){
			pbatch->Put(iter->first,
				leveldb::Slice(iter->second));
 	}
 	ClearMap();
    //p_batch_map->clear();
  	//<< end: fix dma-705
  
  	//skip write to watch performance
  	status = db->Write(leveldb::WriteOptions(), pbatch);
  	pbatch_mem_size = 0;
  	assert(status.ok());
  	BatchBegin();
 }

 // file name for hash map dump 
 std::string GetHashFile() {
	std::string hashfile=path+"/"+"hashmap.bin";
 	return hashfile;
 }
 
 // write p_batch_map to a binary file.
 // if p_batch_map is empty,remove binary file
 void WriteMap() {
	FILE *fp=fopen(GetHashFile().c_str(),"w+b");
 	assert(fp!=NULL);
	if(batch_jobct>0) {
		int elements=p_batch_map->size();
		fwrite(&elements,sizeof(int),1,fp);
		BATCH_MAP::iterator iter;
		for(iter = p_batch_map->begin();iter != p_batch_map->end();iter++){
			int keylen = iter->first.size();
			int valuelen = iter->second.size();
			fwrite(&keylen,4,1,fp);
			fwrite(iter->first.data(),keylen,1,fp);
			fwrite(&valuelen,4,1,fp);
			fwrite(iter->second.data(),valuelen,1,fp);
 		}
	}
    p_batch_map->clear();
    delete p_batch_map;
    p_batch_map = new BATCH_MAP;
	fclose(fp);
 }
 
// load from map file -> mergesrc ->write db
 int CommitMap(apindex &srcdb) {
 	srcdb.BatchBegin();
	FILE *fp=fopen(GetHashFile().c_str(),"rb");
 	assert(fp!=NULL);
	int elements=0;
	fread(&elements,sizeof(int),1,fp);
	int mxkeylen=300;
	int mxvaluelen=500;
	char *keybuf=(char *)malloc(mxkeylen);
	leveldb::ReadOptions opts;
	opts.fill_cache=false;
	char *valuebuf=(char *)malloc(mxvaluelen);
	std::string value;
	for(int i=0;i<elements;i++) {
		int keylen=0,valuelen=0;
		fread(&keylen,sizeof(int),1,fp);
		if(keylen>mxkeylen) {
			mxkeylen=(keylen*1.1);
			void* tmp_p = realloc(keybuf,mxkeylen);
			if (tmp_p) {
				keybuf=(char*)tmp_p;
			} else {
				fclose(fp);
				throw strerror(errno);
			}
		}
		fread(keybuf,keylen,1,fp);
		fread(&valuelen,sizeof(int),1,fp);
		if(valuelen>mxvaluelen) {
			mxvaluelen=(valuelen*1.1);
			void* tmp_p = realloc(valuebuf,mxvaluelen);
			if (tmp_p) {
				valuebuf=(char*)tmp_p;
			} else {
				fclose(fp);
				throw strerror(errno);
			}
		}
		fread(valuebuf,valuelen,1,fp);
		//std::string value(valuebuf,valuelen);
    	leveldb::Slice keyslice(keybuf,keylen);
		// �����ݿ��н��в��ң��ҵ��ͽ��кϲ������������ݿ��кϲ����ڴ�
    	status = srcdb.db->Get(opts, keyslice, &value);
        //std::string  &update_value = iter->second;
        	
    	if(status.ok())// ԭ����ֵ�Ļ�ȡkey��Ӧ��pack
    	{ 	
    		const int *pv=(const int *)valuebuf;
    		int lastkey=*(const int *)(value.data()+value.size()-sizeof(int));
			if(*pv<lastkey) {
				fclose(fp);
				throw "new pack value number less than last loaded pack.";
			}
    		//first of iter->second equals last of value ?
    		bool rep=*pv==lastkey;
    		value.append((const char *)(rep?(pv+1):pv),valuelen-(rep?sizeof(int):0));
    		srcdb.UpdateBWmemSize(keyslice, leveldb::Slice(value));
			srcdb.pbatch->Put(keyslice, leveldb::Slice(value));
    	} 
		else {
		 srcdb.UpdateBWmemSize(keyslice, leveldb::Slice(valuebuf,valuelen));
    	 srcdb.pbatch->Put(keyslice, leveldb::Slice(valuebuf,valuelen));    	 	
		}  	    	
        srcdb.batch_jobct++;
        if(srcdb.batch_jobct>=MAX_JOBS || srcdb.ShouldWriteBW()) {
    		srcdb.BatchWrite();
    	}
    }	
    srcdb.BatchWrite();
	free(keybuf);
	free(valuebuf);
	fclose(fp);
 }
 
 int LoadMap() {
	FILE *fp=fopen(GetHashFile().c_str(),"rb");
 	assert(fp!=NULL);
	int elements=0;
	fread(&elements,sizeof(int),1,fp);
	int mxkeylen=300;
	int mxvaluelen=500;
	char *keybuf=(char *)malloc(mxkeylen);
	char *valuebuf=(char *)malloc(mxvaluelen);
	p_batch_map->clear();
	for(int i=0;i<elements;i++) {
		int keylen=0,valuelen=0;
		fread(&keylen,sizeof(int),1,fp);
		if(keylen>mxkeylen) {
			mxkeylen=(keylen*1.1); 
			void* tmp_p = realloc(keybuf,mxkeylen);
			if (tmp_p) {
				keybuf=(char*)tmp_p;
			} else {
				fclose(fp);
				throw strerror(errno);
			}
		}
		fread(keybuf,keylen,1,fp);
		fread(&valuelen,sizeof(int),1,fp);
		if(valuelen>mxvaluelen) {
			mxvaluelen=(valuelen*1.1); 
			void* tmp_p = realloc(valuebuf,mxvaluelen);
			if (tmp_p) {
				valuebuf=(char*)tmp_p;
			} else {
				fclose(fp);
				throw strerror(errno);
			}
		}
		fread(valuebuf,valuelen,1,fp);
		std::string key(keybuf,keylen);
		std::string value(valuebuf,valuelen);
		(*p_batch_map)[key]=value;
	}
	free(keybuf);
	free(valuebuf);
	fclose(fp);
 }

 bool Get(int64_t key,int packn) {
  std::string _key = CR_Class_NS::i642str(key);
  leveldb::Slice keyslice(_key);
  return Get(keyslice,packn);
 }

 bool Get(const char *key,int packn) {
 	 leveldb::Slice keyslice(key,strlen(key));
   return Get(keyslice,packn);
 }

 bool Get(leveldb::Slice &key,int packn) {
    std::string value;
    	
    //>> begin: fix dma-704 
	BATCH_MAP::iterator iter;
#ifdef STRING_KEY
	std::string keys(key.data(),key.size());
	iter = p_batch_map->find(keys);
#else
	iter = p_batch_map->find(key);
#endif
	if(iter != p_batch_map->end()){
#ifdef STRING_KEY
		value = (*p_batch_map)[keys];
#else
		value = (*p_batch_map)[key];
#endif
		const int *pval=(const int *)((const char *)value.data());
    	int size=value.size()/sizeof(int);
    	const int *pvalend=pval+size;
    	while(pval<pvalend) {
    		if(*pval++==packn) {
    			return true;
    		}
    	}
	}	
	//<< end: fix dma-704
    	
    status = db->Get(leveldb::ReadOptions(), key, &value);
    if(status.ok()){
    	const int *pval=(const int *)((const char *)value.data());
    	int size=value.size()/sizeof(int);
    	const int *pvalend=pval+size;
    	while(pval<pvalend) {
    		// already exists
    		if(*pval++==packn) {
    			return true;
    		}
    	}
    } 
    return false;
 }

 bool Get(_int64 key,std::vector<int>  &packs) {
   leveldb::Slice keyslice(CR_Class_NS::i642str(key));
   return Get(keyslice,packs);
 }

 void Compact() {
 	db->CompactRange(NULL, NULL);
 }
 
 bool Get(const char *key,std::vector<int>  &packs) {
 	 leveldb::Slice keyslice(key,strlen(key));
   return Get(keyslice,packs);
 }

 bool Get(leveldb::Slice &key,std::vector<int> &packs) {
    std::string value;
#ifdef STRING_KEY
    std::string keys(key.data(),key.size());
#endif
    bool exist = false;
	//>> begin: fix dma-705 
	BATCH_MAP::iterator iter;
#ifdef STRING_KEY
	iter = p_batch_map->find(keys);
#else
	iter = p_batch_map->find(key);
#endif
	if(iter != p_batch_map->end()){
#ifdef STRING_KEY
		value = (*p_batch_map)[keys];
#else
		value = (*p_batch_map)[key];
#endif
    	const int *pval=(const int *)((const char *)value.data());
    	int size=value.size()/sizeof(int);
    	const int *pvalend=pval+size;
    	while(pval<pvalend) {
    		packs.push_back(*pval++);
    	}
		exist = true;
	}	
	//<< end: fix dma-705
    	
    //packs.clear();
    status = db->Get(leveldb::ReadOptions(), key, &value);
    if(status.ok()){
    	const int *pval=(const int *)((const char *)value.data());
    	int size=value.size()/sizeof(int);
    	const int *pvalend=pval+size;
    	while(pval<pvalend) {
    		// already exists
    		packs.push_back(*pval++);
    	}
    	exist = true;
    }
    return exist;
 }
 
 // range query including key & limit
 bool GetRange(_int64 key,_int64 limit,std::vector<int>  &packs) {
   std::string _key = CR_Class_NS::i642str(key);
   std::string _limit =  CR_Class_NS::i642str(limit); 
   leveldb::Slice keyslice(_key);
   leveldb::Slice keylimit(_limit);
   return GetRange(keyslice,keylimit,packs);
 }

 bool GetRange(const char *key,const char *limit,std::vector<int>  &packs) {
   leveldb::Slice keyslice(key,strlen(key));
   leveldb::Slice keylimit(limit,strlen(limit));
   return GetRange(keyslice,keylimit,packs);
 }

 bool GetRange(leveldb::Slice &key,leveldb::Slice &limit,std::vector<int> &packs) {
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(key); it->Valid() && it->key().compare(limit)<=0; it->Next()) {
      leveldb::Slice value=it->value();
      const int * pval=(const int *)value.data();
      int size=value.size()/sizeof(int);
      const int *pvalend=pval+size;
      while(pval<pvalend) {
        // already exists
        packs.push_back(*pval++);
      }
    }
    assert(it->status().ok());  // Check for any errors found during the scan
    delete it;
    return true;
 }

 void Put(_int64 key,int packn) {
   std::string _key = CR_Class_NS::i642str(key);
   leveldb::Slice keyslice(_key);
   Put(keyslice,packn);
 }

 void Put(const char *key,int packn) {
   leveldb::Slice keyslice(key,strlen(key));
   Put(keyslice,packn);
 }

 unsigned int BPHash(const char* str, unsigned int len)
 {
   unsigned int hash = 0;
   unsigned int i    = 0;
   for(i = 0; i < len; str++, i++)
   {
      hash = hash << 7 ^ (*str);
   }

   return hash;
 }

 void Put(leveldb::Slice &key,int packn) {
#ifdef STRING_KEY
    std::string keys(key.data(),key.size());
#endif	
/*
    int hash=	leveldb::Hash(key.data(), key.size(), 0);
	unsigned int v=BPHash(key.data(), key.size());
    if(keyinapack.count(hash)!=0) {
		if(keyinapack[hash]==v){
			return;  // 2��hash���һ�£�������Ϊ��ֵ�Ѿ�����
		}
    }
    else{  
		keyinapack[hash]=v;
	}		
*/	
	//>> fix dma-705 �ȵ�p_batch_map�в���,�ҵ��Ͳ���leveldb�н��в��ң��𵽻��������,����ط�����ʹ��leveldb::writebatch
	std::string newvalue((const char*)&packn,sizeof(int));
	BATCH_MAP::iterator iter;
		
#ifdef STRING_KEY
	iter = p_batch_map->find(keys);
#else
	iter = p_batch_map->find(key);
#endif

	if(iter != p_batch_map->end()){ // �ȵ��ڴ�map�н��в���
		std::string &value = iter->second;
		const int *pval=(const int *)((const char *)value.data());
    	int size=value.size()/sizeof(int);
    	const int *pvalend=pval+size;
    	while(pval<pvalend) {
    		if(*pval++==packn) {// already exists
    			dupwt++;
    			return;
    		}
    	}
    	value.append((const char*)&packn,sizeof(int));
    	return;  //  std::string &value �Ѿ��޸�ֵ�����Է���
	}
	/*
	else{ // �ڴ���û���ҵ��������ݿ�����
		//std::string value;
        status = db->Get(leveldb::ReadOptions(), key, &newvalue);
        if(status.ok()) {
	    	// append value to key item
	    	const int *pval=(const int *)((const char *)newvalue.c_str());
	    	int size=newvalue.size()/sizeof(int);
	    	const int *pvalend=pval+size;
	    	while(pval<pvalend) {	    		
	    		if(*pval++==packn) {
	    			dupwt++;
	    		    return; // already exists
	    		}
	    	}
	    	newvalue.append((const char*)&packn,sizeof(int));
        }
        else
        	assert(newvalue.size()==sizeof(int));
	}
	*/
#ifdef STRING_KEY
	(*p_batch_map)[keys] = newvalue;
#else
	(*p_batch_map)[key] = newvalue;
#endif
	batch_jobct++;
	
	/* // fix dma-707
	if(batch_jobct >= MAX_JOBS){
		BatchWrite();
	}
	*/
  }     	
};

#endif

