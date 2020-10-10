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

#include "core/RCEngine.h"
#include "edition/core/Transaction.h"
#include "core/RCTableImpl.h"

using namespace std;

BHEngineReturnValues RCEngine::RunLoader(THD* thd, sql_exchange* ex, TABLE_LIST* table_list, BHError& bherror)
{
	return ExternalLoad(thd, ex, table_list, bherror);
}

BHEngineReturnValues RCEngine::ExternalLoad(THD* thd, sql_exchange* ex,	TABLE_LIST* table_list, BHError& bherror)
{
	BHEngineReturnValues ret = LD_Successed;
	char name[FN_REFLEN];
	TABLE* table;
	int error=0;
	String* field_term = ex->field_term;
	int transactional_table = 0;
	boost::shared_array<char> BHLoader_path(new char[4096]);
	ConnectionInfo* tp = NULL;
	string table_lock_file;

	COPY_INFO info;
	string table_path;
	try {
		// GA
		size_t len = 0;
		dirname_part(BHLoader_path.get(), my_progname, &len);
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(BHLoader_path[len] == 0);

//		dirname_part(BHLoader_path.get(), my_progname);

		strcat(BHLoader_path.get(), BHLoaderAppName);
		if(field_term->length() > 1) {
			my_message(ER_WRONG_FIELD_TERMINATORS,ER(ER_WRONG_FIELD_TERMINATORS), MYF(0));
			bherror = BHError(BHERROR_SYNTAX_ERROR, "Wrong field terminator.");
			return LD_Failed;
		}
//   unlock while lock query on transaction until next commit 
		if(open_and_lock_tables(thd, table_list)) {
			bherror = BHError(BHERROR_UNKNOWN, "Could not open or lock required tables.");
			return LD_Failed;
		}
		
		if(!RCEngine::IsBHTable(table_list->table)) {
			mysql_unlock_tables(thd, thd->lock);
			thd->lock=0;
			return LD_Continue;
		}
		
		auto_ptr<Loader> bhl = LoaderFactory::CreateLoader(LOADER_BH);

		table = table_list->table;
		transactional_table = table->file->has_transactions();

		IOParameters iop;
		BHError bherr;
		if((bherror = RCEngine::GetIOParameters(iop, *thd, *ex, table)) != BHERROR_SUCCESS){
            mysql_unlock_tables(thd, thd->lock); 
			throw LD_Failed;
		}

        // ---------------------------------------------------------
        // 多线程并发insert/bhloader 处理流程
        // 1. 释放锁(让其他线程进行insert提交或者bhloader提交装入数据)
        // 2. 判断loading文件是否存在?
        //    2.1> loading 文件存在,继续等待
        //    2.2> loading 文件不存在,执行流程3
        // 3. 获取表操作的锁
        // 4. 判断loading 文件是否存在?
        //    4.1> loading 文件存在,释放锁继续等待,执行流程2
        //    4.2> loading 文件不存在,执行流程5
        // 5. 创建loading文件
        // 6. 释放锁操作
        // 7. 判断进程是否正常
        // ---------------------------------------------------------
        if(thd->lock) {
            mysql_unlock_tables(thd, thd->lock);
            thd->lock=0;
            //do not commit or rollback transaction,just unlock write table only
            Transaction* trs = GetThreadParams(*thd)->GetTransaction();
            table_lock_manager.ReleaseWriteLocks(*trs);
        }       

        table_path = GetTablePath(thd, table);
        table_lock_file = table_path +".loading";
        bool lastload=false;
        uint lask_packrow =0;
        ulong warnings = 0;

        long wait_start_insert_cnt = 0;
        do{
            if (DoesFileExist(table_lock_file)) {               
                if(wait_start_insert_cnt++ % 10 == 0){
                    rclog << lock << "Warning: RCEngine::ExternalLoad Table ["<< table_lock_file << "] is loading . sleep 5 second , continue..."<< unlock;
                }
                sleep(wait_start_insert_cnt & 5);
                continue;        
            } else {
                Transaction* trs = GetThreadParams(*thd)->GetTransaction();
                if(table_lock_manager.AcquireWriteLock(*trs,table_path,thd->killed, thd->some_tables_deleted).expired()) {
                    continue;
                }

                if (DoesFileExist(table_lock_file)) {   
                    table_lock_manager.ReleaseWriteLocks(*trs);
                    continue;
                }
                struct stat sb;
                int fd = open(table_lock_file.c_str(), O_WRONLY | O_CREAT, DEFFILEMODE);
                if (fd == -1 || fstat(fd, &sb) || close(fd)) {
                    bherror = BHError(BHERROR_UNKNOWN, "Cannot open lock file \"" + table_lock_file + "\" .");
                    table_lock_manager.ReleaseWriteLocks(*trs);
                    throw LD_Failed;
                }
                
                // already checked that this is a BH table
                tp = GetThreadParams(*thd);
                int tab_id = tp->GetTransaction()->GetTable(table_path, table->s)->GetID();
                if(rsi_manager){
                    rsi_manager->UpdateDefForTable(tab_id);
                }

                table->copy_blobs=0;
                thd->cuted_fields=0L;

                bhl->Init(BHLoader_path.get(), &iop);
                bhl->MainHeapSize() = this->m_loader_main_heapsize;
                bhl->CompressedHeapSize() = this->m_loader_compressed_heapsize;

                uint transaction_id = tp->GetTransaction()->GetID();
                bhl->TransactionID() = transaction_id;
                bhl->ConnectID()=tp->GetConnectID();
                bhl->SetTableId(tab_id);

                lask_packrow = JustATable::PackIndex(tp->GetTransaction()->GetTable(table_path, table->s)->NoObj());

                // check if last load of a partition
                lastload=false;
                {
                    char ctl_filename[300];
                    sprintf(ctl_filename,"%s.bht/loadinfo.ctl",table_path.c_str());
                    IBFile loadinfo;
                    loadinfo.OpenReadOnly(ctl_filename);
                    char ctl_buf[200];
                    loadinfo.Read(ctl_buf,14);
                    lastload=*(short *)(ctl_buf+12)==1;
                }
                
                if(thd->killed != 0 ){ // dpshut , exit
                    thd->send_kill_message();
                    table_lock_manager.ReleaseWriteLocks(*trs);
                    break;
                }               
                
                table_lock_manager.ReleaseWriteLocks(*trs);
                
                wait_start_insert_cnt = -1;
            }             
        }while(wait_start_insert_cnt>0);

        rclog << lock << "Info: Begin to start bhloader ..."<< unlock;


#ifdef __WIN__
		if(bhl->Proceed(tp->pi.hProcess) && !tp->killed())
#else
		if(bhl->Proceed(tp->pi) && !tp->killed())
#endif
		{
			
			//deprecated: merge to interal temporary hash_map only,keep memory not been released!!
			// Build to target db from map file directly
			//
			if(!lastload && 0 != tp->GetTransaction()->GetTable(table_path, table->s)->MergePackHash(tp->GetTransaction()->GetID()))
			{	
				bherror = BHError(BHERROR_UNKNOWN, "Merge table pack index error.");
				throw LD_Failed;
			}
			
			// deprecated : packindex 分区合并的时候，不同的进程(bhloader和mysqld)无法打开同一个库(即分开运行的时候不能放入bhloader中)
			//              A+B --> A 和 A+B ---> C 的时候，只能在mysqld中进行
			// new db produced in bhloader
			/*
			if(lastload && 0 != tp->GetTransaction()->GetTable(table_path, table->s)->MergePackIndex(tp->GetTransaction()->GetID()))
			{				
				bherror = BHError(BHERROR_UNKNOWN, "Merge pack index of last file in a partition error.");
				throw LD_Failed;
			}
			*/
			if(open_and_lock_tables(thd, table_list)) {
				bherror = BHError(BHERROR_UNKNOWN, "Could not open or lock required tables.");
				throw LD_Failed;
			}
			
			if((_int64) bhl->NoRecords() > 0) {
				int tid = tp->GetTransaction()->GetTable(table_path, table->s)->GetID();
				tp->GetTransaction()->ReleasePackRow(tid, lask_packrow);
				for(int a = 0; a < tp->GetTransaction()->GetTable(table_path, table->s)->NoAttrs(); a++) {
					tp->GetTransaction()->DropLocalObject(SpliceCoordinate(tid, a, lask_packrow / DEFAULT_SPLICE_SIZE ));
				}
				tp->GetTransaction()->RefreshTable(table_path.c_str()); // thread id used temp to store transaction id ...
			}
			std::ifstream warnings_in( bhl->GetPathToWarnings().c_str() );
			if ( warnings_in.good() ) {
				std::string line;
				while(std::getline(warnings_in, line).good()) {
					push_warning(thd, MYSQL_ERROR::WARN_LEVEL_WARN, ER_UNKNOWN_ERROR, line.c_str());
					warnings++;
				}
				warnings_in.close();
			}
			RemoveFile(bhl->GetPathToWarnings().c_str(), false);
		} else {
			RemoveFile(bhl->GetPathToWarnings().c_str(), false);
			if(tp->killed()) {
				bherror = BHError(BHERROR_KILLED, error_messages[BHERROR_KILLED]);
				thd->send_kill_message();
			}
			else if(*bhl->ErrorMessage() != 0)
				bherror = BHError(bhl->ReturnCode(), bhl->ErrorMessage());
			else
				bherror = BHError(bhl->ReturnCode());
			error = 1; //rollback required
			throw LD_Failed;
		}

		// We must invalidate the table in query cache before binlog writing and
		// ha_autocommit_...
		query_cache_invalidate3(thd, table_list, 0);

		info.records = ha_rows((_int64)bhl->NoRecords());
		info.copied  = ha_rows((_int64)bhl->NoCopied());
		info.deleted = ha_rows((_int64)bhl->NoDeleted());

		snprintf(name, sizeof(name), ER(ER_LOAD_INFO), (ulong) info.records, (ulong) info.deleted,
				(ulong) (info.records - info.copied), (ulong) warnings);
		my_ok(thd, info.copied+info.deleted, 0L, name);
		//send_ok(thd,info.copied+info.deleted,0L,name);
	} catch(BHEngineReturnValues& bherv) {
		ret = bherv;
	}
	catch(...){
		ret = LD_Failed;
	}

	if(thd->transaction.stmt.modified_non_trans_table)
		thd->transaction.all.modified_non_trans_table= TRUE;

	if(transactional_table)
		error = ha_autocommit_or_rollback(thd, error);

	// Load in other process or terminated unnormal(mannual remove file)	
	if(ret==LD_Locked){
		ret=LD_Failed;
	}else{
	    Transaction* trs = GetThreadParams(*thd)->GetTransaction();
		if(table_lock_manager.AcquireWriteLock(*trs,table_path,thd->killed, thd->some_tables_deleted).expired()) {
		    bherror = BHError(BHERROR_UNKNOWN, "Cannot lock table for write before commit .");
            ret= LD_Failed;
		}
	}
	thd->abort_on_warning = 0;

	return ret;
}
