#include <cr_class/cr_class.h>
#ifndef ATTRINDEX_HEADER
#define ATTRINDEX_HEADER
#include <system/IBFile.h>
#define ATTRINDEX_VERSION_1 0xa0
#define INDEX_READY  0
#define INDEX_BUILD  1
#define INDEX_DELETE 2
#include "apindex.h"
#include "partinfo.h"
#include "DMThreadGroup.h"


class AttrIndex {
protected:
	DMMutex mutex;
	int attrnum;
	bool ismerged;//query on a list of apindex or a merged apindex
	bool isvalid;
	bool islistvalid;
	//std::vector<apindex> index_list;
	// merged index:
	apindex *index_attr;
	std::string basepath;
	std::string headfile;
  	short max_partseq;
  	unsigned char file_ver;
  	struct apindexinfo {
  	char partname[MAX_PART_NAME_LEN];
  	short action;// 0,none; 1,wait commit 2,wait delete
  	int sessionid;
  	apindex *index;
  	apindexinfo():action(0),sessionid(0),index(NULL){
			partname[0]=0;
		};
  	apindexinfo(short pnum,short _action,int sid):action(_action),sessionid(sid),index(NULL){
			sprintf(partname,"%05d",pnum);
		}
 	  apindexinfo(const char * pname,short _action,int sid):action(_action),sessionid(sid),index(NULL){
			strcpy(partname,pname);
		}
	  apindexinfo(const apindexinfo &ap) {
			action=ap.action;
			strcpy(partname,ap.partname);
			sessionid=ap.sessionid;
			if(ap.index==NULL)
				index=NULL;
			else
				index=new apindex(*ap.index);
		}
		
		apindexinfo& operator = ( const apindexinfo& ap ) {
			action=ap.action;
			strcpy(partname,ap.partname);
			sessionid=ap.sessionid;
			if(ap.index==NULL)
				index=NULL;
			else
				//fix dma-691 temp db (write) not released!
				index=new apindex(*ap.index);
			return *this;
		}			
		
  	~apindexinfo() {
  		if(index != NULL) {
  			delete index;
			index = NULL;
  		}
  	}
	};
  // global index info for temporary index data
  // key: tabid+columnid+sessionid
  static std::map<std::string,apindexinfo *> g_index;// static global index info
  std::vector<apindexinfo *> index_info;
public:
	// mysqld 退出的时候,释放leveldb对象,RCEngine::~RCEngine() 函数中调用
	static void release_leveldb_index(){
		std::map<std::string,apindexinfo *>::iterator iter;
		for(iter = g_index.begin();iter!=g_index.end();iter++){
			apindexinfo* pidx = (apindexinfo*)iter->second;
			if(pidx != NULL){
				if(pidx->index != NULL){
					delete pidx->index;
					pidx->index = NULL;
				}
				delete pidx;
				pidx = NULL;
			}
		}
		g_index.clear();
	}
	//Get global index info key
	std::string GetGKey(int tabid,int sid,const char *partname) {
		char ckey[300]; // tabid_colunum_sid_partname
		sprintf(ckey,"%d_%d_%d_%s",tabid,attrnum,sid,partname);
		return std::string(ckey);
	}
	
	//Move temporary apindex(build data in hashmap) to global map
	void MoveToGlobal(int tabid,int sid,const char *partname) {
		AutoLock lock(&mutex);
		for(int i=0;i<index_info.size();i++) 
		{
			if(strcmp(index_info[i]->partname,partname)==0 && index_info[i]->sessionid==sid) {
				g_index[GetGKey(tabid,sid,partname)] = index_info[i];
				index_info.erase(index_info.begin()+i);
			}
		}
	}
	
	//clear global index info item
	void RemoveGlobal(int tabid,int sid,const char *partname) {
		AutoLock lock(&mutex);
		if(g_index[GetGKey(tabid,sid,partname)]->index != NULL){
			delete g_index[GetGKey(tabid,sid,partname)]->index ;
			g_index[GetGKey(tabid,sid,partname)]->index = NULL;
		}
		delete g_index[GetGKey(tabid,sid,partname)];
		g_index[GetGKey(tabid,sid,partname)] = NULL;
		g_index.erase(GetGKey(tabid,sid,partname));
	}
	
	apindex *GetGlobal(int tabid,int sid,const char *partname) {
		return g_index[GetGKey(tabid,sid,partname)]->index;
	}

	AttrIndex(int _attrnum,const char *_attrpath):index_info() {
		basepath=_attrpath;
		attrnum=_attrnum;
		char fn[300];
    	sprintf(fn,"/attrindex_%d.ctb",attrnum);
		headfile=_attrpath;
		headfile+=fn;
		init();
		index_attr=NULL;
	}

	~AttrIndex() {
		init();
		if(index_attr!=NULL){
			delete index_attr;
			index_attr= NULL;
		}

		for(int i=0;i<index_info.size();i++){
			if(index_info[i]!=NULL){
				if(index_info[i]->index != NULL){
					delete index_info[i]->index;
					index_info[i]->index = NULL;
				}
				delete index_info[i];
				index_info[i] = NULL;
			}
		}
		index_info.clear();
	}
	
	void init () {
		max_partseq=0;ismerged=false;isvalid=false;islistvalid=false;file_ver=ATTRINDEX_VERSION_1;
		for(int i=0;i<index_info.size();i++){
			if(index_info[i]!=NULL){
				if(index_info[i]->index != NULL){
					delete index_info[i]->index;
					index_info[i]->index = NULL;
				}
				delete index_info[i];
				index_info[i] = NULL;
			}
		}
		index_info.clear();
  	}
	
	bool Valid() {
		return isvalid;
	}
	
	bool SetValid(bool v) {
		isvalid=v;
	}
  
  bool LoadHeader()
  {
  	// load header info :
  	init();
  	if(!DoesFileExist(headfile)) 
  		return false;
  	IBFile idxhfile;
  	idxhfile.OpenReadOnly(headfile);
  	idxhfile.ReadExact(&file_ver,1);
  	assert(file_ver==ATTRINDEX_VERSION_1);
  	unsigned char flags;
  	idxhfile.ReadExact(&flags,1);
  	isvalid=flags&0x01;
  	islistvalid=flags&0x02;
  	ismerged=flags&0x04;
  	int datalen=0;
  	max_partseq=0;
  	idxhfile.ReadExact(&datalen,sizeof(int));
  	// at least 2 values
	if(datalen<8) 
			return false;
  	char *tmp=new char[datalen];
  	assert(tmp);
  	idxhfile.ReadExact(tmp,datalen);
  	char *plist=tmp;
  	char *pend=tmp+datalen;
	char partname[300];
	// elements:
	// <short>partnamelen <string>PartName <short>action(status) <int>sessionid
    while(plist<pend) {
    	int pnamelen=*(short *)(plist);
    	max_partseq++;plist+=sizeof(short);
	  	memcpy(partname,plist,pnamelen);
	  	partname[pnamelen]=0;plist+=pnamelen;
  	  	index_info.push_back(new apindexinfo(partname,*(short *)(plist),*(int *)(plist+2)));
  	  	plist+=sizeof(short)+sizeof(int);
    }
    delete []tmp;
    return true;
  }
  // delete a partition index :
  //  1 . load header info
  //  2 . lock table for write
  //  3 . delete index table
  //  4 . save header info
  //  5 . unlock table
  //  6 . reload header
  // add a partition index :
  //  1. load header info
  //  2. create new partition with sessionid(transaction id)-- set has not be commited flag
  //  3. create index table
  //  4. save uncommit header info (waitting for server process to commit)
  //  5. commit by server
  //     5.1 lock table for write
  //     5.2 release index online
  //     5.3 reload header info
  //     5.4 find partnum to be committed
  //     5.5 change flags to commited
  //     5.6 save header info
  //     5.7 unlock table
  //     5.8 reload header
  
  //  make sure this object has correct value to save ,as the header file maybe deleted or rebuild in this method.
  // if truncate_tab ,then save file to xxx.ntrunc
  bool SaveHeader(bool truncate_tab=false,const char *truncpart=NULL) {
  	if(index_info.size()<1) {
  		RemoveFile(headfile);
  		return false;
  	}
  	IBFile idxhfile;
  	idxhfile.OpenCreateEmpty(headfile+(truncate_tab?".ntrunc":""));
  	idxhfile.WriteExact(&file_ver,1);
  	unsigned char flags=0;
  	flags|=isvalid?0x01:0;
  	flags|=islistvalid?0x02:0;
  	flags|=ismerged?0x04:0;
  	idxhfile.WriteExact(&flags,1);
  	int datalen=index_info.size()*(8+MAX_PART_NAME_LEN);
  	char *tmp=new char[datalen];
  	// at least 2 values
  	//assert(datalen>=8 && datalen % 8==0); // 2 short ,2 int
  	
  	char *plist=tmp;
  	for(int i=0;i<index_info.size();i++) {
		if(truncate_tab && strcmp(index_info[i]->partname,truncpart)==0) 
			continue;
  		*(short *)(plist)=strlen(index_info[i]->partname);plist+=sizeof(short);
		memcpy(plist,index_info[i]->partname,strlen(index_info[i]->partname)); 
		plist+=strlen(index_info[i]->partname);
  		*(short *)(plist)=index_info[i]->action;plist+=sizeof(short);
  		*(int *)(plist)=index_info[i]->sessionid;plist+=sizeof(int);
		if(truncate_tab) continue;
  		// db data updated at maximum batch jobs or here :
  		//if(index_info[i]->action==INDEX_BUILD && index_info[i]->index!=NULL)
  		//	index_info[i]->index->BatchWrite();
  	}
	// real length
	datalen=plist-tmp;
  	idxhfile.WriteExact(&datalen,sizeof(int));
	idxhfile.WriteExact(tmp,datalen);
    delete []tmp;
	tmp=NULL;
    return true;
  }
   bool RollbackTruncate(const char *partname) {
		if(!DoesFileExist(headfile+".ntrunc")) return false;
		RemoveFile( headfile+".ntrunc");
		//LoadHeader();
		//LoadForRead();
		return true;
   }

   bool RebuildByTruncate(int *packvars,int packvsize,std::set<std::string> rebuildparts,int sid) {
		std::set<std::string>::iterator iter=rebuildparts.begin();
		while(iter!=rebuildparts.end()){
			apindex *db=GetReadIndex(iter->c_str());
			char mergedbname[300];
			apindex mergeidx(db->MergeIndexPath(mergedbname),"");
			assert(db);
			mergeidx.RebuildByTruncate(packvars,packvsize,*db);
			iter++;
		}
   }

   bool CommitTruncate(const char *partname) {
		if(!DoesFileExist(headfile+".ntrunc")) return false;
		std::string idxpath=GetReadIndex(partname)->GetPath();
		RemovePart(partname);
		DeleteDirectory( idxpath);
		DeleteDirectory( idxpath+"_w");
		RemoveFile( headfile);
		RenameFile(headfile+".ntrunc", headfile);
		LoadHeader();
		LoadForRead();
		for(int i=0;i<index_info.size();i++) 
		{
			if(index_info[i]->action==INDEX_READY) {
				char mergename[300];
				if(DoesFileExist(index_info[i]->index->MergeIndexPath(mergename))){
					index_info[i]->index->ReplaceDB(mergename);
					index_info[i]->index->ReloadDB();
				}
			}
		}
		return true;
   }
  
	//call LoadHeader before this
	void LoadForRead() {
		if(!isvalid || index_info.size()<1) 
			return;
		
		for(int i=0;i<index_info.size();i++) {
			// open committed partition only. skip  crashed none commit partitions
			/* a partition maybe exists both uncommited and commited data */
			if(index_info[i]->action==INDEX_READY && index_info[i]->index==NULL) {
				index_info[i]->index=new apindex(attrnum,index_info[i]->partname,basepath,false);
				if(index_info[i]->index->isempty()) {
					delete index_info[i]->index;
					index_info[i]->index=NULL;
				}
				else isvalid=true;
			}
		}
		
	}
	
	apindex *	GetMergedIndex() {
		assert(index_attr!=NULL);
		return index_attr;
	}
	
	apindex *GetIndex(short partnum,int sessionid) {
    char pname[MAX_PART_NAME_LEN];
		sprintf(pname,"%05d",partnum);
		return GetIndex(pname,sessionid);
	}

	apindex *GetIndex(const char *pname,int sessionid) {
		for(int i=0;i<index_info.size();i++) 
		{
		// GetIndex By both readonly Table RCAttr(PackIndexMerge) and RCAttrLoad(Write temp index table)
			if(strcmp(index_info[i]->partname,pname)==0 && index_info[i]->sessionid==sessionid) {
				return index_info[i]->index;
			}
		}
		throw "GetIndex got null";
	}

  // 如果没有对于分区(会话)的索引，则新建
  //
	apindexinfo *LoadForUpdate(short partnum,int sessionid) {
	       char pname[MAX_PART_NAME_LEN];
		sprintf(pname,"%05d",partnum);
		return LoadForUpdate(pname,sessionid);
	}
  
  apindex *GetReadIndex(const char *pname) {
  	for(int i=0;i<index_info.size();i++) 
		{
			if(strcmp(index_info[i]->partname,pname)==0 && index_info[i]->action==INDEX_READY) {
					if(index_info[i]->index==NULL)
				  		index_info[i]->index=new apindex(attrnum,index_info[i]->partname,basepath,false);
					return index_info[i]->index;
			}
		} 
		index_info.push_back(new apindexinfo(pname,INDEX_READY,-1));
		apindexinfo *newindex=index_info.back();
		newindex->index=new apindex(attrnum,pname,basepath,false);
		return newindex->index;
	}

	void RemovePart(const char *pname) {
  	 for(int i=0;i<index_info.size();i++) 
		{
			if(strcmp(index_info[i]->partname,pname)==0 && index_info[i]->action==INDEX_READY) {
				if(index_info[i]->index!=NULL){
					delete index_info[i]->index;
					index_info[i]->index = NULL;
				}
				delete index_info[i];
				index_info[i]=NULL;
				index_info.erase(index_info.begin()+i);i--;
			}
		} 
	}
	
	// index list中对一个分区可能有两个items,一个是ready for read ,另一个是index_build
	apindexinfo *LoadForUpdate(const char *pname,int sessionid,bool IsMerge = false) {
		for(int i=0;i<index_info.size();i++) 
		{
			if(strcmp(index_info[i]->partname,pname)==0 && index_info[i]->sessionid==sessionid) {
				// increment build is not supported!
				//index_info[i]->sessionid=sessionid;
				assert(index_info[i]->action==INDEX_BUILD);
				if(index_info[i]->index==NULL)
					index_info[i]->index=new apindex(attrnum,index_info[i]->partname,basepath,true);
				if(!IsMerge)
					index_info[i]->index->ClearDB();
				return index_info[i];
			}
			//remove old sessionid load
			else if(strcmp(index_info[i]->partname,pname)==0 && index_info[i]->action==INDEX_BUILD) {
				if(index_info[i]->index != NULL){
					delete index_info[i]->index;
					index_info[i]->index = NULL;
				}
				delete index_info[i];
				index_info[i]=NULL;
				index_info.erase(index_info.begin()+i);i--;
			}
		}
		index_info.push_back(new apindexinfo(pname,INDEX_BUILD,sessionid));
		apindexinfo *newindex=index_info.back();
		newindex->index=new apindex(attrnum,pname,basepath,true);
		if(!IsMerge)
			newindex->index->ClearDB();
		return newindex;
	}

	void Rollback(int tabid,short partnum,int sessionid) {
		char pname[MAX_PART_NAME_LEN];
		sprintf(pname,"%05d",partnum);
		Rollback(tabid,pname,sessionid);
	}

	void Rollback(int tabid,const char *pname,int sessionid) {
		/*std::vector<apindexinfo>::iterator iter=index_info.begin();
		for(;iter!=index_info.end();++iter) {
			if(strcmp(pname,iter->partname)==0 && iter->sessionid==sessionid) {
				// increment build is not supported!
				assert(iter->action==INDEX_BUILD);
				if(iter->index!=NULL)
					delete iter->index;
				iter->index=new apindex(attrnum,iter->partname,basepath,true);
				iter->index->ClearDB();
				index_info.erase(iter);
				break;
			}
		}*/
		RemoveGlobal(tabid,sessionid,pname);		
		ClearTempDB(pname,sessionid);
		SaveHeader();
	}
	
	void ClearTempDB(const char *pname,int sessionid) {
		std::vector<apindexinfo *>::iterator iter=index_info.begin();
		for(;iter!=index_info.end();++iter) {
			// ClearTempDB By a readonly Table (RCAttr instead of RCAttrLoad)
			if(strcmp(pname,(*iter)->partname)==0 && (*iter)->sessionid==sessionid) {
				// increment build is not supported!
				assert((*iter)->action==INDEX_BUILD);
				if((*iter)->index==NULL)
					(*iter)->index=new apindex(attrnum,(*iter)->partname,basepath,true);
				std::string idxpath=(*iter)->index->GetPath();
				(*iter)->index->ClearDB();
				(*iter)->index->ClearMap();
				if((*iter)->index != NULL){
					delete (*iter)->index ;
					(*iter)->index = NULL;
				}
				delete *iter;
				*iter=NULL;
				index_info.erase(iter);
				DeleteDirectory( idxpath);
				break;
			}
		}
	}
	
	void ClearMergeDB() {
		std::vector<apindexinfo *>::iterator iter=index_info.begin();
		for(;iter!=index_info.end();++iter) {
			if((*iter)->action==INDEX_READY) {
				  std::string dbname=apindex::GetDBPath(attrnum,(*iter)->partname,basepath,false);
					char mergepath[300];
					if(DoesFileExist( apindex::GetMergeIndexPath(dbname.c_str(),mergepath)))
						DeleteDirectory(mergepath);
			}
	   }
    }
		
	//lock table for write before commit
	//  and reload table after commit

	void Commit(int tabid,short partnum,int sessionid) {
		char pname[MAX_PART_NAME_LEN];
		sprintf(pname,"%05d",partnum);		
		Commit(tabid,pname,sessionid);
	}

	void Commit(int tabid,const char *pname,int sessionid) {
		for(int i=0;i<index_info.size();i++) 
		{
			if(strcmp(index_info[i]->partname,pname)==0 && index_info[i]->sessionid==sessionid) {
				// increment build is not supported!
				assert(index_info[i]->action==INDEX_BUILD);
				isvalid=true;

				// delete and reopen to close index to commit;
				// Not load index at commit !
				//if(index_info[i]->index!=NULL)
				// delete index_info[i]->index;
				//index_info[i]->index=NULL;
				
				//get apindex or create if not exists.
				apindex *pindex=GetReadIndex(pname);
				char mergename[300];
				if(DoesFileExist(pindex->MergeIndexPath(mergename))){
					pindex->ReplaceDB(mergename);
					pindex->ReloadDB();

					if(index_info[i]->index==NULL)
						index_info[i]->index=new apindex(attrnum,pname,basepath,true);
					std::string idxpath=index_info[i]->index->GetPath();
					if(index_info[i]->index != NULL){
						delete index_info[i]->index;
						index_info[i]->index = NULL;
					}
					delete index_info[i];
					index_info[i]=NULL;
					index_info.erase(index_info.begin()+i);
					DeleteDirectory(idxpath );
				}
				else {
					//reuse temp index
					//use hash map prepared in loading data.
					if(index_info[i]->index==NULL)
						index_info[i]->index=new apindex(attrnum,pname,basepath,true);
					apindex *tomerge=index_info[i]->index;
					
					//apindex *tomerge=GetGlobal(tabid,sessionid,pname);
					//backup for delete later
					std::string idxpath=tomerge->GetPath();
					//pindex->ImportHash(*tomerge);
					//tomerge->ClearDB();// 内部删除*.bin文件
					// release hashmap
					//tomerge->ClearMap();   
					//release write db, must be last instance
					
					//RemoveGlobal(tabid,sessionid,pname);
					if(index_info[i]->index != NULL){
						delete index_info[i]->index;
						index_info[i]->index = NULL;
					}
					delete index_info[i];
					index_info[i]=NULL;
					index_info.erase(index_info.begin()+i);
					DeleteDirectory(idxpath );
					
					// reloaddb to write LOG to SST
					// keep pindex open(no reopen) ?
					pindex->ReloadDB();
					// reloaddb again to release log/sst merge resource
					// pindex->ReloadDB();
				}
				break;
			}
			// clear other sessions in building
			else if(index_info[i]->action==INDEX_BUILD) {
				// clear all others session (hanged session will be remove)
				if(index_info[i]->index==NULL)
					index_info[i]->index=new apindex(attrnum,index_info[i]->partname,basepath,true);
				index_info[i]->index->ClearDB();
				if(index_info[i]->index){
					delete index_info[i]->index;
					index_info[i]->index = NULL;
				}
				delete index_info[i];
				index_info[i]=NULL;
				index_info.erase(index_info.begin()+i);
				// current item(i) became the next one and need be proceed again,minus 1 to cancel out i++ in loop
				--i;
			}
		}
		SaveHeader();
	}

 bool Get(_int64 key,std::vector<int>  &packs) {
   if(!isvalid)
     return false;
   std::string _key = CR_Class_NS::i642str(key);
   leveldb::Slice keyslice(_key);
   return Get(keyslice, packs);
 }

 bool Get(const char *key,std::vector<int>  &packs) {
   if(!isvalid) return false;
   leveldb::Slice keyslice(key,strlen(key));
   return Get(keyslice,packs);
 }

 // return false : can not use index,maybe not a indexed column or index in building
 // return true : return found packs
 bool Get(leveldb::Slice &key,std::vector<int> &packs) {
 	  if(!isvalid) return false;

	if(index_info.size()<3) {
    for(int i=0;i<index_info.size();i++) 
		{
			if(index_info[i]->action==INDEX_READY) {
				if(index_info[i]->index==NULL)
					// TODO : how to decide it's write or read?
					index_info[i]->index=new apindex(attrnum,index_info[i]->partname,basepath,false);
				index_info[i]->index->Get(key,packs);
			}
		}
  }
  else {
	 DMThreadGroup ptlist;
	 std::vector< std::vector<int> > _packs_vector(index_info.size());

	 // get packs
	 for(int i=0;i<index_info.size();i++){
		if(index_info[i]->action==INDEX_READY){
			ptlist.LockOrCreate()->StartInt(boost::bind(&AttrIndex::t_GetPack,this,i,boost::ref(key),boost::ref(_packs_vector[i])));
		}
	 }
	 ptlist.WaitAllEnd();

	 // merge packs
	 for(int i=0;i<_packs_vector.size();i++)	{
	 	packs.insert(packs.end(),_packs_vector[i].begin(),_packs_vector[i].end());
	 }
	}
  return true;
 }

 // 获取单个leveldb 的key对应的pack到vector中
 int t_GetPack(int i,leveldb::Slice &key,std::vector<int> &packs)
 {
 	if(index_info[i]->index==NULL){// TODO : how to decide it's write or read?
				index_info[i]->index=new apindex(attrnum,index_info[i]->partname,basepath,false);
	}
	return index_info[i]->index->Get(key,packs)? 0:-1;
 }
 
 bool GetRange(_int64 key,_int64 limit,std::vector<int>  &packs) {
   if(!isvalid)
     return false;
   std::string _key = CR_Class_NS::i642str(key);
   std::string _limit =  CR_Class_NS::i642str(limit);
   leveldb::Slice keyslice(_key);
   leveldb::Slice keylimit(_limit);
   
   return GetRange(keyslice,keylimit,packs);
 }


 bool GetRange(const char *key,const char *limit,std::vector<int>  &packs) {
 	 if(!isvalid) return false;
 	 leveldb::Slice keyslice(key,strlen(key)),keylimit(limit,strlen(limit));
   return GetRange(keyslice,keylimit,packs);
 }

 // return false : can not use index,maybe not a indexed column or index in building
 // return true : return found packs
 bool GetRange(leveldb::Slice &key,leveldb::Slice &limit,std::vector<int> &packs) {
 	  if(!isvalid) return false;

	if(index_info.size()<3) {
    for(int i=0;i<index_info.size();i++){
			if(index_info[i]->action==INDEX_READY) {
				if(index_info[i]->index==NULL)// TODO : how to decide it's write or read?
					index_info[i]->index=new apindex(attrnum,index_info[i]->partname,basepath,false);
				index_info[i]->index->GetRange(key,limit,packs);
			}
		}
  }
  else {
	 DMThreadGroup ptlist;
	 std::vector< std::vector<int> > _packs_vector(index_info.size());

	 // get packs
	 for(int i=0;i<index_info.size();i++){
		if(index_info[i]->action==INDEX_READY){
			ptlist.LockOrCreate()->StartInt(boost::bind(&AttrIndex::t_GetRangePack,this,i,boost::ref(key),boost::ref(limit),boost::ref(_packs_vector[i])));
		}
	 }
	 ptlist.WaitAllEnd();

	 // merge packs
	 for(int i=0;i<_packs_vector.size();i++)	{
	 	packs.insert(packs.end(),_packs_vector[i].begin(),_packs_vector[i].end());
	 }	
	}
  return true;
 }

  // 获取单个leveldb 的key对应的pack到vector中
 int t_GetRangePack(int i,leveldb::Slice &key,leveldb::Slice &limit,std::vector<int> &packs)
 {
	if(index_info[i]->index==NULL){// TODO : how to decide it's write or read?
				index_info[i]->index=new apindex(attrnum,index_info[i]->partname,basepath,false);
	}		
	return index_info[i]->index->GetRange(key,limit,packs)? 0:-1;
 }

};

#endif

