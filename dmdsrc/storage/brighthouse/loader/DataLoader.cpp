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

#include "system/LargeBuffer.h"
#include "system/IOParameters.h"
#include "DataLoader.h"
#include "NewValueSet.h"
#include "edition/loader/RCAttr_load.h"
#include "DataParser.h"
#include "system/LargeBuffer.h"
#include "system/IOParameters.h"
#include "loader/NewValueSetCopy.h"
#include "loader/NewValueSubSet.h"
#include <vector>
#include <algorithm>
#include "dma_mtloader.h"
using namespace std;

DataLoaderImpl::DataLoaderImpl(vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop)
	:	attrs(attrs), buffer(&buffer), iop(iop), packrow_size(MAX_PACK_ROW_SIZE)
{
	no_loaded_rows = 0;
	sort_size = 0;
	enable_sort = false;
}

DataLoaderImpl::~DataLoaderImpl()
{
}



int dma_mtloader::work()
	{
		//Do thread work here
		NewValuesSetCopy *nvse=(NewValuesSetCopy *)objects[1];
		RCAttrLoad *rcl=(RCAttrLoad*)objects[0];
		rcl->LoadData(nvse, false);
		LockStatus();
		isdone=true;
		Unlock();
		return 1;
	}
//load a single row(insert data)
_uint64 DataLoaderImpl::Proceed(NewValuesSetBase **nvses)
{
	//if (process_type == ProcessType::DATAPROCESSOR)
	//	for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::ExpandDPNArrays, _1));
	//for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::SetPackrowSize, _1, iop.GetPackrowSize()));
	try {
					for( uint i=0; i<attrs.size(); i++ ) {
						attrs[i]->LoadData(nvses[i], false,true);
					}
					no_loaded_rows=1;
	} catch(FormatRCException& frce) {
		frce.m_row_no += no_loaded_rows;
		Abort();
		/*
		for(uint i = 0; i < attrs.size(); i++) {
			delete nvses[i];
			nvses[i] = 0;
		}
		*/
		throw;
	} catch(DataTypeConversionRCException&) {

		Abort();
		throw FormatRCException(BHERROR_DATA_ERROR, no_loaded_rows + parser->error_value.first, parser->error_value.second);
	} catch(...) {

		Abort();
		throw InternalRCException(BHERROR_UNKNOWN);
	}

	if(no_loaded_rows == 0)
		throw FormatRCException(BHERROR_DATA_ERROR, no_loaded_rows + parser->error_value.first, parser->error_value.second);

	return no_loaded_rows;
}

_uint64 DataLoaderImpl::ProceedEnd() {
//	for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::Save, _1));
    for( uint i=0; i<attrs.size(); i++ )   attrs[i]->Save(true);
	WritePackIndex();
	return no_loaded_rows;
}

double getfreecpu(int &cpuct);

_uint64 DataLoaderImpl::Proceed()
{
	NewValuesSetCopy **nvses;
	no_loaded_rows = 0;
	int chunk_rows_loaded;
	int to_prepare;
	
	void * _asyn_riak_handle = NULL;	// 异步写包的句柄，给riak调用的

	if( DoSort() )
		to_prepare = GetSortSize();
	else
		// dma-463 : partition support
		//  get current loading partition and it's last pack objects.
		//to_prepare = iop.GetPackrowSize() - (int)(attrs[0]->NoObj() % iop.GetPackrowSize());
		to_prepare=iop.GetPackrowSize() - (int)(attrs[0]->PartLastPackObjs()% iop.GetPackrowSize());

	if (process_type == ProcessType::DATAPROCESSOR)
		for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::ExpandDPNArrays, _1));
	for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::SetPackrowSize, _1, iop.GetPackrowSize()));

	try {
		parser->no_prepared = 0;
		int count=0;
		nvses = new NewValuesSetCopy * [attrs.size()];
		for(uint i = 0; i < attrs.size(); i++)
			nvses[i] = 0;
		
		//Add multi-thread support here
		// detect current cpu resource :
		int cpuct=0;
		uint maxcolmn=attrs.size();
		double cfree=getfreecpu(cpuct);
		if(cfree<2) cfree=2;
		else cfree=cfree*2;
		if(cfree>maxcolmn) cfree=maxcolmn;

		if(_riak_cluster == NULL){
			_riak_cluster_init();
		}
		assert(_riak_cluster != NULL);		

		// 装入线程数修正
		int maxthread= cfree;

		// 异步的线程数目设置,异步save的时候，线程数不能超过riak节点数的4倍
		if(_riak_connect_number>0)
			maxthread = (_riak_connect_number > cfree) ? (_riak_connect_number):(cfree);
		else 
			maxthread = (_riak_nodes > cfree) ? (_riak_nodes):(cfree);
		
		_asyn_riak_handle = _riak_cluster->AsyncInit(maxthread,1024,10);
		
		DMThreadGroup tl(maxthread);
		
		while(true) {
			parser->row_incomplete = false;
			parser->Prepare(to_prepare);
			if (parser->no_prepared == 0) {
				if (LoadData() == 0) {
					if (parser->row_incomplete)
						throw FormatRCException(BHERROR_DATA_ERROR, parser->error_value.first, parser->error_value.second);
					if (parser->no_prepared == 0)
						break;
				}
				else
					continue;
			}
			for(uint i = 0; i < attrs.size(); i++) {
				parser->PrepareNextCol();
				nvses[i] = new NewValuesSetCopy( parser.get(), i, 0, attrs[i]->PackType(), attrs[i]->Type().GetCollation() );
			}
			std::vector<uint> * order( RowOrder( nvses, parser->no_prepared ) );
			OmitAttrs(nvses);
			
			if( order == NULL ) {

				for( uint i=0; i<attrs.size(); i++ ) {
					//Wait idle thread
					DMThreadData *pbc=tl.WaitIdleAndLock();
					attrs[i]->SetAsyncRiakHandle(_asyn_riak_handle);
					pbc->StartInt(boost::bind(&RCAttrLoad::LoadData,attrs[i],nvses[i],false,false,false));
				}
				tl.WaitAllEnd();
				
/* original codes:
				for( uint i=0; i<attrs.size(); i++ ) {
#ifndef __BH_COMMUNITY__
					attrs[i]->LoadData(nvses[i], (process_type != ProcessType::BHLOADER && process_type != ProcessType::DATAPROCESSOR) || true, false);
#else
					attrs[i]->LoadData(nvses[i], false);
#endif
				}
*/
		// dma-463 : partition support
		//  get current loading partition and it's last pack objects.

		//		to_prepare = iop.GetPackrowSize() - (int)(attrs[0]->NoObj() % iop.GetPackrowSize());
		// at here,use last save pack objects to compute append position (including un-commit loading
		//  but use last pack objects(not save position) to append to current valid data pack,skip none commit loading,at begining of loading
				to_prepare = iop.GetPackrowSize() - (int)(attrs[0]->PartLastSavePackObjs() % iop.GetPackrowSize());
			} else {
				int current_pkrow_size=0;
				for( chunk_rows_loaded=0; 
					chunk_rows_loaded<parser->no_prepared; 
					chunk_rows_loaded+=current_pkrow_size ) {
					current_pkrow_size = std::min(iop.GetPackrowSize() - (uint)(attrs[0]->NoObj() % iop.GetPackrowSize()),
												(uint)(parser->no_prepared - chunk_rows_loaded));
					for( uint i=0; i<attrs.size(); i++ ) {
						NewValuesSetBase *sub;
						sub = new NewValuesSubSet(nvses[i],
													*order, 
													chunk_rows_loaded, 
													current_pkrow_size);
#ifndef __BH_COMMUNITY__
						attrs[i]->LoadData(sub, (process_type != ProcessType::BHLOADER && process_type != ProcessType::DATAPROCESSOR) || parser->DoPreparedValuesHasToBeCoppied(), false);
#else
						attrs[i]->LoadData(nvses[i], false);
#endif
						delete sub;
					}
				}
				to_prepare = GetSortSize();
			}
			no_loaded_rows += parser->no_prepared;


			parser->no_prepared = 0;
			count++;
			for(uint i = 0; i < attrs.size(); i++) {
				delete nvses[i];
				nvses[i] = 0;
			}
			delete order;
		}
	} catch(FormatRCException& frce) {
		frce.m_row_no += no_loaded_rows;
		Abort();
		for(uint i = 0; i < attrs.size(); i++) {
			delete nvses[i];
			nvses[i] = 0;
		}
		throw;
	} catch(DataTypeConversionRCException&) {
		for(uint i = 0; i < attrs.size(); i++) {
			delete nvses[i];
			nvses[i] = 0;
		}
		Abort();
		throw FormatRCException(BHERROR_DATA_ERROR, no_loaded_rows + parser->error_value.first, parser->error_value.second);
	} catch(...) {
		for(uint i = 0; i < attrs.size(); i++) {
			delete nvses[i];
			nvses[i] = 0;
		}
		Abort();
		throw InternalRCException(BHERROR_UNKNOWN);
	}

	if(no_loaded_rows == 0)
		throw FormatRCException(BHERROR_DATA_ERROR, no_loaded_rows + parser->error_value.first, parser->error_value.second);

#ifndef __BH_COMMUNITY__
	if (process_type == ProcessType::DATAPROCESSOR) {
		for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::FinalizeStream, _1));
		for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::WaitForSaveThreads, _1));
	} else
#endif
	//for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::Save, _1));
	for( uint i=0; i<attrs.size(); i++ )   attrs[i]->Save();
	WritePackIndex();

	if(_asyn_riak_handle){
		assert(_riak_cluster != NULL);
		std::string err_msg;
		long ret = 0;
		
		#if !PERFORMANCE_SASU_54
		ret = _riak_cluster->AsyncWait(_asyn_riak_handle,err_msg,(RIAK_ASYNC_WAIT_TIMEOUT_US / 1000000.0));
		#else

		// 记录AsyncWait时间
		mytimer _tm_async_wait;
		_tm_async_wait.Restart();
		ret = _riak_cluster->AsyncWait(_asyn_riak_handle,err_msg,(RIAK_ASYNC_WAIT_TIMEOUT_US / 1000000.0));
		_tm_async_wait.Stop();
		DPRINTF("----> AsyncWait table<%d> AsyncWait use time %0.4f（s）.\n",attrs[0]->table_number,_tm_async_wait.GetTime());

		#endif

		if(ret < 0)
		{
			rclog << lock << "Save pack error: " << err_msg << "  ,AsyncWait return :"<<ret<<unlock;
			// todo: 需要添加本次装入所有包号对应的riak中的数据包，多个bhloader装入的时候，需要修改

			Abort();
			throw InternalRCException(BHERROR_UNKNOWN);			
		}	
		else
		{			
			// PackSize_tabid.loading.ctl ,文件格式如下
			// [分区名称1长度:2字节][分区名称1:分区名称1长度][分区1占用字节数:8字节]
			{
				PackSizeInfo psi(attrs[0]->path,attrs[0]->table_number);
				std::string strLoadingFile = psi.GetLoadingPackSizeFileName();
				if(DoesFileExist(strLoadingFile)){
					RemoveFile(strLoadingFile,0);
				}
				std::string strPartName(attrs[0]->partitioninfo->GetSavePartName());
				psi.SaveTmpPackSize(strPartName,ret);	
			}
		}
	}

	// >> begin: sasu-54
	#if PERFORMANCE_SASU_54
	for(int i=0;i<attrs.size();i++){
		attrs[i]->display_save_pack_time();
	}
	#endif 
	//<< end: sasu-54
	
	delete [] nvses;
	return no_loaded_rows;
}

void DataLoaderImpl::WritePackIndex() {
// Note : dma-712,data pack lost in dpn/partinfo perhaps caused by multi-thread process below:
//	 try to fix : move packindex write parallel only.

	rclog << lock << "Note : write packindex begin"<<unlock;
	try
	{
		//mulit-thread merge index
		mytimer _tm_write_packindex;
		_tm_write_packindex.Restart();
       	rclog << lock << "DataLoaderImpl::WritePackIndex()  write packindex begin ..."<<unlock;        

		DMThreadGroup ptlist;
		for(int i = 0; i < attrs.size(); i++)
		if(attrs[i] && attrs[i]->Type().IsPackIndex()) {
			ptlist.LockOrCreate()->Start(boost::bind(&RCAttrLoadBase::SavePartitonPacksIndex,attrs[i],_1));
		}
		ptlist.WaitAllEnd();
        
        _tm_write_packindex.Stop();
        rclog << lock << "DataLoaderImpl::WritePackIndex()  write packindex end ,use ["<<_tm_write_packindex.GetTime()<<"] (S)"<<unlock;        

    }catch(char* e){
        rclog << lock << "ERROR: Write temporary packindex,throw char* msg["<<e<<"]."<<unlock; 

        char err_msg[1024];
        sprintf(err_msg,"ERROR: Write temporary packindex,throw char* msg[%s]",e);
        throw InternalRCException(std::string(err_msg));

    }catch(int e){
        rclog << lock << "ERROR: Write temporary packindex,throw int msg["<<e<<"]."<<unlock; 

        char err_msg[1024];
        sprintf(err_msg,"ERROR: Write temporary packindex,throw char* msg[%d]",e);
        throw InternalRCException(std::string(err_msg));

    }catch(...) {
        rclog << lock << "ERROR: Write temporary packindex "<<unlock;

        char err_msg[1024];
        sprintf(err_msg,"ERROR: Write temporary packindex,throw unknow error");
        throw InternalRCException(std::string(err_msg));
    }

	
	rclog << lock << "Note : write packindex end"<<unlock;

}

int DataLoaderImpl::LoadData()
{
	MEASURE_FET("DataLoader::LoadData()");
	int ret = 0;
	try	{
		ret = buffer->BufFetch(int(parser->buf_end - parser->buf_ptr));
	} catch (FileRCException&) {
		Abort();
		throw;
	}

	if(ret)	{
		parser->buf_ptr = parser->buf_start = buffer->Buf(0);
		parser->buf_end = parser->buf_start + buffer->BufSize();
	}
	return ret;
}


void DataLoaderImpl::Abort()
{
}

auto_ptr<DataLoader>
DataLoaderImpl::CreateDataLoader(vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop)
{
	auto_ptr<DataLoader> data_loader(new DataLoader(attrs, buffer, iop));
	data_loader->parser = DataFormat::GetDataFormat(iop.GetEDF())->CreateDataParser(attrs, buffer, iop);
    //data_loader->parser = DataParserFactory::CreateDataParser(attrs, buffer, iop);
	return data_loader;
}

#ifndef __BH_COMMUNITY__
auto_ptr<DataLoader>
DataLoaderImpl::CreateDataLoader(vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop, 
							SortOrderType &o, int s, std::vector<unsigned int> &om)
{
	auto_ptr<DataLoader> data_loader(new DataLoader(attrs, buffer, iop, o, s, om));
	data_loader->parser = DataFormat::GetDataFormat(iop.GetEDF())->CreateDataParser(attrs, buffer, iop, s);
    //data_loader->parser = DataParserFactory::CreateDataParser(attrs, buffer, iop);
	return data_loader;
}
#endif
