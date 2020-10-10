#include "MapPackInfo.h"
#include "system/RCSystem.h"

MapPackInfo::MapPackInfo(int attr_no)
{
	map_pack_list.clear();
	attr_number = attr_no;
	riak_max_pack_no = 0;	
}
MapPackInfo::~MapPackInfo()
{
	map_pack_list.clear();
	riak_max_pack_no = 0;
	attr_number = 0;
}

// 未提交过的文件: MAP_PACKNO_xxxxx.bin
std::string MapPackInfo::GetMapPackFileName(std::string& tabpth)
{
	char _tabpth[256];
	snprintf(_tabpth, sizeof(_tabpth),"%s/MAP_PACKNO_%05d.bin",tabpth.c_str(),attr_number);
	std::string fn(_tabpth);
	return fn;
}
// 未提交过的文件: MAP_PACKNO_xxxxx.bin.tmp
std::string MapPackInfo::GetTmpMapPackFileName(std::string& tabpth)
{	
	char _tabpth[256];
	snprintf(_tabpth, sizeof(_tabpth),"%s/MAP_PACKNO_%05d.bin.tmp",tabpth.c_str(),attr_number);
	std::string fn(_tabpth);
	return fn;
}
	
int MapPackInfo::LoadPackList(std::string & filename)   // 读取文件内容到 map_pack_list 
{
	assert(map_pack_list.size() == 0);
	FILE *fp=fopen(filename.c_str(),"rb");
 	assert(fp!=NULL);
	int elements=0;
	fseek(fp, -8L, SEEK_END);
	fread(&riak_max_pack_no,sizeof(int),1,fp);
	fread(&elements,sizeof(int),1,fp);
	
	fseek(fp, 0L, SEEK_SET);
	for(int i=0;i<elements;i++) {
		StruMapItem item;
		fread(&item.virtual_pack_no,sizeof(int),1,fp);
		fread(&item.real_pack_no,sizeof(int),1,fp);	
		fread(&item.valid,sizeof(char),1,fp);

		map_pack_list.push_back(item);
	}

	fclose(fp);
	fp = NULL;

	return elements;
	
}
int MapPackInfo::SavePackList(std::string & filename)	  // map_pack_list 保存到文件
{
	if(map_pack_list.empty()){
		return 0;
	}
	FILE *fp=fopen(filename.c_str(),"wb");
	assert(fp!=NULL);
	fseek(fp, 0L, SEEK_SET);
	int _fixed_elemetns = 0;
	if(map_pack_list.size()>0) 
	{

		for(int i = 0;i<map_pack_list.size();i++)
		{
			int virtual_pack_no = map_pack_list[i].virtual_pack_no;
			int real_pack_no = map_pack_list[i].real_pack_no;
			char valid=map_pack_list[i].valid;
			if(INVALID_PACK_NO == virtual_pack_no){
				continue;
			}
			_fixed_elemetns++;
			if(riak_max_pack_no < real_pack_no){
				riak_max_pack_no = real_pack_no;
			}
			fwrite(&virtual_pack_no,4,1,fp);
			fwrite(&real_pack_no,4,1,fp);
			fwrite(&valid,1,1,fp);
		}
		
		fwrite(&riak_max_pack_no,sizeof(int),1,fp);
		fwrite(&_fixed_elemetns,sizeof(int),1,fp);
	}
	fclose(fp);
	fp = NULL;
	
	return _fixed_elemetns;
}

int MapPackInfo::InitPackList(const MapPackList init_pack_list) // 第一次删除分区的时候，将pack列表初始化
{
	assert(map_pack_list.size() == 0);
	if(init_pack_list.empty()){
		return 0;	
	}
	map_pack_list.insert(map_pack_list.end(),init_pack_list.begin(),init_pack_list.end());
	int last_key = map_pack_list.size();
	riak_max_pack_no = map_pack_list[last_key-1].real_pack_no;
	
	return map_pack_list.size();
}

int  MapPackInfo::GetPackListSize()
{
	return map_pack_list.size();
}

int  MapPackInfo::GetRiakMaxPack()
{
	return riak_max_pack_no;
}
void MapPackInfo::AddRiakMaxPack()
{
	riak_max_pack_no += 1;
}

int MapPackInfo::UpdateRangePackList(const int startpackno,const int endpackno) // 删除的时候使用，将一段包的编号进行更新
{
	int update_items = endpackno - startpackno + 1;
	
	// 1. 将[startpackno,startpackno]的包设置为无效
	// 2. 将pack大于endpackno的所有包的v_pack_no - (endpackno - startpackno)
	for(int i=0;i<map_pack_list.size();i++)
	{
		if(map_pack_list[i].virtual_pack_no < startpackno)
			continue;

	    // 将[startpackno,startpackno]的包设置为无效
		if(map_pack_list[i].virtual_pack_no >= startpackno &&
			map_pack_list[i].virtual_pack_no <= endpackno)
		{
			map_pack_list[i].Invalid();  // -1
			continue;
		}

		//  将pack大于endpackno的所有包的v_pack_no - (endpackno - startpackno)
		if(map_pack_list[i].virtual_pack_no > endpackno){
			map_pack_list[i].virtual_pack_no -= update_items;
		}
	}
	
	return update_items;
}

int MapPackInfo::AddSinglePack(const int vpackno,const char valid)    // 编号增加,将之前没有加入的包号都添加进入,valid[0:无效包号,1:有效包号]
{	
	assert(map_pack_list.size() >= 0);

	int _size = map_pack_list.size();
	if(_size > 0){
		// 新添加的包
		assert(map_pack_list[vpackno].virtual_pack_no == vpackno);
		if(map_pack_list[_size -1].virtual_pack_no < vpackno)
		{
			int start_pack = map_pack_list[_size - 1].virtual_pack_no + 1;
			int end_pack = vpackno;
			for(int i=start_pack;i<=end_pack;i++){
				// riak 包号增加
				AddRiakMaxPack();
				StruMapItem st(start_pack,INVALID_PACK_NO,0);
				if(i == end_pack){ // 最后一个包有效
					st.real_pack_no = GetRiakMaxPack();
					st.valid = valid;
				}
				map_pack_list.push_back(st);
			}	
		}	
		else if(map_pack_list[_size -1].virtual_pack_no == vpackno) // 最后一个包,直接保存
		{
			assert(map_pack_list[vpackno].virtual_pack_no == vpackno);
		}
		else{
			assert(0);
		}		
	}
	
	return 0;	
}


int	MapPackInfo::UpdateSinglePackList(const int voldpackno,const int vnewpackno) // load的时候使用
{
	assert(map_pack_list.size() >= voldpackno);
	assert(voldpackno >= vnewpackno);

	for(int i=voldpackno;i<map_pack_list.size();i++)
	{
		if(map_pack_list[i].virtual_pack_no == voldpackno)
		{
			map_pack_list[i].virtual_pack_no = vnewpackno;
			return 0;
		}
	}
	assert(0);
	
	return -1;
	
}

int MapPackInfo::RPack2VPack(const int rpackno)
{
	if(map_pack_list.empty()){
		return rpackno;
	}

	for(int i=0;i<map_pack_list.size();i++){
		if(map_pack_list[i].real_pack_no == rpackno){
			return map_pack_list[i].virtual_pack_no;
		}			
	}

	return INVALID_PACK_NO;

}
int MapPackInfo::VPack2RPack(const int vpackno)
{
	if(map_pack_list.empty()){
		return vpackno;
	}

	for(int i=vpackno;i<map_pack_list.size();i++){
		if(map_pack_list[i].virtual_pack_no == vpackno){
			return map_pack_list[i].real_pack_no;
		}
	}
	
	return INVALID_PACK_NO;
}


bool MapPackInfo::MapPackIsValid2Save()  // 判断内存映射文件是否需要保存
{
	if(map_pack_list.empty()){
		return false;
	}	
	
	for(int i=0;i<map_pack_list.size();i++){
		if(map_pack_list[i].virtual_pack_no != map_pack_list[i].real_pack_no){
			return true;
		}
	}
	
	return false;	
}

void MapPackInfo::ClearList()
{
	map_pack_list.clear();
	riak_max_pack_no = 0;	
}


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
PackSizeInfo::PackSizeInfo(std::string path,int tabid)
{
	this->_strpath = path;
	this->_tabid = tabid;
}

PackSizeInfo::~PackSizeInfo()
{
	this->_strpath = "";
	this->_tabid = 0;
}

// PackSize_tabid.ctl 
std::string PackSizeInfo::GetPackSizeFileName()	
{
	char tmpfn[256];
	snprintf(tmpfn, sizeof(tmpfn),"%s/PackSize_%d.ctl",this->_strpath.c_str(),this->_tabid);
	std::string fn(tmpfn);
	return fn;
}

std::string PackSizeInfo::GetCommitPackSizeFileName() // PackSize_tabid.ctl.tmp
{
	char tmpfn[256];
	snprintf(tmpfn, sizeof(tmpfn),"%s/PackSize_%d.ctl.tmp",this->_strpath.c_str(),this->_tabid);
	std::string fn(tmpfn);
	return fn;
}

 // PackSize_tabid.loading.ctl	
std::string PackSizeInfo::GetLoadingPackSizeFileName()  
{
	char tmpfn[256];
	snprintf(tmpfn, sizeof(tmpfn),"%s/PackSize_%d.loading.ctl",this->_strpath.c_str(),this->_tabid);
	std::string fn(tmpfn);
	return fn;
}

//  PackSize_tabid.loading.ctl	
// [分区名称1长度:2字节][分区名称1:分区名称1长度][分区1占用字节数:8字节]
bool	PackSizeInfo::SaveTmpPackSize(const std::string strPartName,const unsigned long packsize) 
{
	std::string tmpPackFile = GetLoadingPackSizeFileName();
	
	FILE* pFile = NULL;
	if(DoesFileExist(tmpPackFile)){
		RemoveFile(tmpPackFile);
	}
	pFile = fopen(tmpPackFile.c_str(),"wb");
	assert(pFile);

	// [分区名称1长度:2字节][分区名称1:分区名称1长度][分区1占用字节数:8字节]
	// read
	short part_name_len = strPartName.size();
	if(fwrite(&part_name_len,sizeof(part_name_len),1,pFile) != 1){
		rclog << lock << "Error: SaveTmpPackSize file:" << tmpPackFile << " part_name_len error. " << unlock;
		fclose(pFile);
		pFile = NULL;			
		return false;
	}

	const char* p=strPartName.c_str();
	if(fwrite(p,part_name_len,1,pFile) != 1){
		rclog << lock << "Error: SaveTmpPackSize file:" << tmpPackFile << " strPartName error. " << unlock;
		fclose(pFile);
		pFile = NULL;			
		return false;
	}
	
	if(fwrite(&packsize,8,1,pFile) != 1){
		rclog << lock << "Error: SaveTmpPackSize file:" << tmpPackFile << " packsize error. " << unlock;
		fclose(pFile);
		pFile = NULL;			
		return false;
	}
	fclose(pFile);
	pFile = NULL;
	rclog << lock << "Info: SaveTmpPackSize file:" << tmpPackFile << " do not exist, create it. sise = " << packsize <<" bytes."<<unlock;

	return true;
}

//  PackSize_tabid.loading.ctl	
// [分区名称1长度:2字节][分区名称1:分区名称1长度][分区1占用字节数:8字节]
bool PackSizeInfo::GetTmpPackSize(std::string& strPartName,unsigned long& packsize)
{
	std::string tmpPackFile = GetLoadingPackSizeFileName();
	
	FILE* pFile = NULL;
	if(!DoesFileExist(tmpPackFile)){
		rclog << lock << "Warning: GetTmpPackSize file:" << tmpPackFile << " do not exist , pack size is 0 ." << unlock;
		return false;
	}
	else{
		pFile = fopen(tmpPackFile.c_str(),"r+b"); // read ,modify
		assert(pFile);

		// read partname len
		short part_name_len = 0;
		if(fread(&part_name_len,sizeof(part_name_len),1,pFile) != 1){
			rclog << lock << "Error: GetTmpPackSize file:" << tmpPackFile << " read part_name_len error. " << unlock;

			fclose(pFile);
			pFile = NULL;
			
			return false;
		}

		// read part_name
		char part_name[256];
		if(fread(part_name,part_name_len,1,pFile) != 1){
			rclog << lock << "Error: GetTmpPackSize file:" << tmpPackFile << " read part_name error. " << unlock;

			fclose(pFile);
			pFile = NULL;
			
			return false;
		}		
		part_name[part_name_len] = 0;
		strPartName = std::string(part_name);

		// read pack size
		packsize = 0;
		if(fread(&packsize,8,1,pFile) != 1){
			rclog << lock << "Error: GetTmpPackSize file:" << tmpPackFile << " read pack size error. " << unlock;

			fclose(pFile);
			pFile = NULL;
			
			return false;
		}		
	}
	
	fclose(pFile);
	pFile = NULL;

	return true;

}

// 2. 装入完成的文件,PackSize_tabid.ctl 文件,文件格式如下
/*
[分区数:2字节]
[分区名称1长度:2字节][分区名称1:分区名称1长度][分区1占用字节数:8字节]
[分区名称2长度:2字节][分区名称2:分区名称1长度][分区2占用字节数:8字节]
[分区名称3长度:2字节][分区名称3:分区名称1长度][分区3占用字节数:8字节]
[分区名称4长度:2字节][分区名称4:分区名称1长度][分区4占用字节数:8字节]
....
*/
bool 	PackSizeInfo::UpdatePartPackSize(const std::string strPartName,const unsigned long packsize) //  PackSize_tabid.ctl
{
	std::string PackFile = GetPackSizeFileName();
	unsigned long spsize_total = 0;

	rclog << lock << " Info: UpdatePartPackSize to file "<< PackFile << " part name "<<strPartName << " packsize " << packsize <<" ."<< unlock;

	FILE* pFile = NULL;
	if(!DoesFileExist(PackFile)){
		rclog << lock << "Info: SavePackSize file:" << PackFile << " do not exist,create pack file ." << unlock;

		pFile = fopen(PackFile.c_str(),"wb"); 
		assert(pFile != NULL);
		
		int ret = 0;
		
		short part_num = 1;
		ret = fwrite(&part_num,sizeof(short),1,pFile);
		assert(ret == 1);
		
		short partname_len = strPartName.size();
		ret = fwrite(&partname_len,sizeof(short),1,pFile);
		assert(ret == 1);
		
		ret = fwrite(strPartName.c_str(),partname_len,1,pFile);
		assert(ret == 1);

		spsize_total = packsize;
		ret = fwrite(&spsize_total,sizeof(long),1,pFile);
		assert(ret ==1);

		fclose(pFile);
		pFile = NULL;
		
		return true;
	}
	else{
		pFile = fopen(PackFile.c_str(),"rb"); 
		assert(pFile);

		//------------------------------------------------------
		// 1. 读取到内存
		short part_num = 0;
		if(fread(&part_num,sizeof(part_num),1,pFile) != 1){
			rclog << lock << "Error: UpdatePartPackSize file:" << PackFile << " read part_num error. " << unlock;

			fclose(pFile);
			pFile = NULL;
			
			return false;
		}

		std::vector<StruPartPackSize> part_pack_size_vector;
		int _part_index=0;
		while(_part_index<part_num)
		{
			StruPartPackSize _st_item;
			// read partname len
			if(fread(&_st_item.part_name_len,sizeof(_st_item.part_name_len),1,pFile) != 1){
				rclog << lock << "Error: GetTmpPackSize file:" << PackFile << " read part_name_len error. " << unlock;

				fclose(pFile);
				pFile = NULL;
				
				return false;
			}			

			// read part_name
			if(fread(_st_item.part_name,_st_item.part_name_len,1,pFile) != 1){
				rclog << lock << "Error: GetTmpPackSize file:" << PackFile << " read part_name error. " << unlock;

				fclose(pFile);
				pFile = NULL;
				
				return false;
			}		
			_st_item.part_name[_st_item.part_name_len]= 0;
			
			// read pack size
			if(fread(&_st_item.packsize,8,1,pFile) != 1){
				rclog << lock << "Error: GetTmpPackSize file:" << PackFile << " read pack size error. " << unlock;

				fclose(pFile);
				pFile = NULL;
				
				return false;
			}	
			
			part_pack_size_vector.push_back(_st_item);			
			
			_part_index++;
		}

		fclose(pFile);
		pFile = NULL;

		//-------------------------------------------
		//2.更新分区的字节大小到内存
		bool exist_part = false;
		for(int i = 0;i<part_pack_size_vector.size();i++){
			if(strncmp(part_pack_size_vector[i].part_name,strPartName.c_str(),part_pack_size_vector[i].part_name_len) == 0){
				part_pack_size_vector[i].packsize += packsize;
				exist_part = true;
				break;
			}
		}

		if(!exist_part){
			StruPartPackSize _st_item;
			_st_item.part_name_len = strPartName.size();
			dm_strlcpy(_st_item.part_name, strPartName.c_str(), sizeof(_st_item.part_name));
			_st_item.packsize = packsize;

			part_pack_size_vector.push_back(_st_item);
		}

		// -----------------------------------------
		// 3. 更新后的写入临时文件
		std::string CommitPackFile = GetCommitPackSizeFileName();
		
		pFile = fopen(CommitPackFile.c_str(),"wb"); //create file
		assert(pFile);

		part_num = (short)part_pack_size_vector.size();
		if(fwrite(&part_num,sizeof(part_num),1,pFile) != 1){
			rclog << lock << "Error: UpdatePartPackSize file:" << PackFile << " update size "<< packsize <<" error. " << unlock;
			fclose(pFile);
			pFile = NULL;			
			return false;
		}

		for(int i=0;i<part_pack_size_vector.size();i++)
		{
			// write partname len
			if(fwrite(&part_pack_size_vector[i].part_name_len,sizeof(part_pack_size_vector[i].part_name_len),1,pFile) != 1){
				rclog << lock << "Error: GetTmpPackSize file:" << PackFile << " read part_name_len error. " << unlock;

				fclose(pFile);
				pFile = NULL;
				
				RemoveFile(CommitPackFile,0);				
				return false;
			}

			// write part_name
			if(fwrite(part_pack_size_vector[i].part_name,part_pack_size_vector[i].part_name_len,1,pFile) != 1){
				rclog << lock << "Error: GetTmpPackSize file:" << PackFile << " read part_name error. " << unlock;

				fclose(pFile);
				pFile = NULL;
				RemoveFile(CommitPackFile,0);	
				
				return false;
			}		
			
			// write pack size
			if(fwrite(&part_pack_size_vector[i].packsize,8,1,pFile) != 1){
				rclog << lock << "Error: GetTmpPackSize file:" << PackFile << " read pack size error. " << unlock;

				fclose(pFile);
				pFile = NULL;
				RemoveFile(CommitPackFile,0);	
				
				return false;
			}	
		}

		//-------------------------------------------
		//4. 将新写入的临时文件替换成原来的文件
		RenameFile(CommitPackFile,PackFile);
	}
	
	fclose(pFile);
	pFile = NULL;

	return true;

}

bool PackSizeInfo::GetTableSize()// PackSize_tabid.ctl 中所有分区大小
{
	std::string PackFile = GetPackSizeFileName();
	unsigned long spsize_total = 0;

	FILE* pFile = NULL;
	if(!DoesFileExist(PackFile)){
		rclog << lock << "Error: GetTableSize file:" << PackFile << " do not exist, do not get pack size ." << unlock;
		return false;
	}
	else{
		pFile = fopen(PackFile.c_str(),"rb"); // read ,modify
		assert(pFile);

		short part_num = 0;
		if(fread(&part_num,sizeof(part_num),1,pFile) != 1){
			rclog << lock << "Error: UpdatePartPackSize file:" << PackFile << " read part_num error. " << unlock;

			fclose(pFile);
			pFile = NULL;
			
			return false;
		}
	
		int _part_index=0;
		while(_part_index<part_num)
		{
			StruPartPackSize _st_item;
			// read partname len
			if(fread(&_st_item.part_name_len,sizeof(_st_item.part_name_len),1,pFile) != 1){
				rclog << lock << "Error: GetTmpPackSize file:" << PackFile << " read part_name_len error. " << unlock;

				fclose(pFile);
				pFile = NULL;
				
				return false;
			}

			// read part_name
			if(fread(_st_item.part_name,_st_item.part_name_len,1,pFile) != 1){
				rclog << lock << "Error: GetTmpPackSize file:" << PackFile << " read part_name error. " << unlock;

				fclose(pFile);
				pFile = NULL;
				
				return false;
			}		
			
			// read pack size
			if(fread(&_st_item.packsize,8,1,pFile) != 1){
				rclog << lock << "Error: GetTmpPackSize file:" << PackFile << " read pack size error. " << unlock;

				fclose(pFile);
				pFile = NULL;
				
				return false;
			}	
			spsize_total+= _st_item.packsize;	
			
			_part_index++;
		}
	}
}
	

// 2. 装入完成的文件,PackSize_tabid.ctl 文件,文件格式如下
/*
[分区数:2字节]
[分区名称1长度:2字节][分区名称1:分区名称1长度][分区1占用字节数:8字节]
[分区名称2长度:2字节][分区名称2:分区名称1长度][分区2占用字节数:8字节]
[分区名称3长度:2字节][分区名称3:分区名称1长度][分区3占用字节数:8字节]
[分区名称4长度:2字节][分区名称4:分区名称1长度][分区4占用字节数:8字节]
....
*/
bool PackSizeInfo::DeletePartPackSize(const std::string strPartName)//   PackSize_tabid.ctl
{
	std::string PackFile = GetPackSizeFileName();
	unsigned long spsize_total = 0;

	rclog << lock << " Info: DeletePartPackSize to file "<< PackFile << " part name "<<strPartName << unlock;

	FILE* pFile = NULL;
	if(!DoesFileExist(PackFile)){
		rclog << lock << "Error: DeletePartPackSize file:" << PackFile << " do not exist, do not delete pack size ." << unlock;
		return false;
	}
	else{
		pFile = fopen(PackFile.c_str(),"r+b"); // read ,modify
		assert(pFile);

		short part_num = 0;
		if(fread(&part_num,sizeof(part_num),1,pFile) != 1){
			rclog << lock << "Error: DeletePartPackSize file:" << PackFile << " read part_num error. " << unlock;

			fclose(pFile);
			pFile = NULL;
			
			return false;
		}

		std::vector<StruPartPackSize> part_pack_size_vector;
		int _part_index=0;
		while(_part_index<part_num)
		{
			StruPartPackSize _st_item;
			// read partname len
			if(fread(&_st_item.part_name_len,sizeof(_st_item.part_name_len),1,pFile) != 1){
				rclog << lock << "Error: DeletePartPackSize file:" << PackFile << " read part_name_len error. " << unlock;

				fclose(pFile);
				pFile = NULL;
				
				return false;
			}

			// read part_name
			if(fread(_st_item.part_name,_st_item.part_name_len,1,pFile) != 1){
				rclog << lock << "Error: DeletePartPackSize file:" << PackFile << " read part_name error. " << unlock;

				fclose(pFile);
				pFile = NULL;
				
				return false;
			}		
			
			// read pack size
			if(fread(&_st_item.packsize,8,1,pFile) != 1){
				rclog << lock << "Error: DeletePartPackSize file:" << PackFile << " read pack size error. " << unlock;

				fclose(pFile);
				pFile = NULL;
				
				return false;
			}	
			
			part_pack_size_vector.push_back(_st_item);			
			
			_part_index++;
		}

		fclose(pFile);
		pFile=NULL;
		
		// 更新分区 的字节大小
		bool exist_part = false;
		std::vector<StruPartPackSize>::iterator iter;
		for(iter = part_pack_size_vector.begin();iter != part_pack_size_vector.end();iter++)
		{
			if(strcmp(iter->part_name,strPartName.c_str()) == 0)
			{	
				part_pack_size_vector.erase(iter);
				exist_part =true;
				break; 		
			}
		}
		
		if(!exist_part){
			assert(0);
		}

		// 重新写入
		pFile = fopen(PackFile.c_str(),"wb"); 
		assert(pFile);

		part_num = (short)part_pack_size_vector.size();
		if(fwrite(&part_num,sizeof(part_num),1,pFile) != 1){
			rclog << lock << "Error: DeletePartPackSize file:" << PackFile <<" error. " << unlock;

			fclose(pFile);
			pFile = NULL;			
			return false;
		}

		for(int i=0;i<part_pack_size_vector.size();i++)
		{
			// write partname len
			if(fwrite(&part_pack_size_vector[i].part_name_len,sizeof(part_pack_size_vector[i].part_name_len),1,pFile) != 1){
				rclog << lock << "Error: DeletePartPackSize file:" << PackFile << " read part_name_len error. " << unlock;

				fclose(pFile);
				pFile = NULL;
				
				return false;
			}

			// write part_name
			if(fwrite(part_pack_size_vector[i].part_name,part_pack_size_vector[i].part_name_len,1,pFile) != 1){
				rclog << lock << "Error: DeletePartPackSize file:" << PackFile << " read part_name error. " << unlock;

				fclose(pFile);
				pFile = NULL;
				
				return false;
			}		
			
			// write pack size
			if(fwrite(&part_pack_size_vector[i].packsize,8,1,pFile) != 1){
				rclog << lock << "Error: DeletePartPackSize file:" << PackFile << " read pack size error. " << unlock;

				fclose(pFile);
				pFile = NULL;
				
				return false;
			}	
		}

	}
	
	fclose(pFile);
	pFile = NULL;

	return true;

}

bool PackSizeInfo::CommitPackSize()
{
	std::string  psi_loading_file = GetLoadingPackSizeFileName();
	if(DoesFileExist(psi_loading_file))
	{
		std::string _part_name ("");
		unsigned long _part_size = 0;
		if(GetTmpPackSize(_part_name,_part_size)){
			UpdatePartPackSize(_part_name,_part_size);
		}
		else{
			assert(0);
		}
		
		RemoveFile(psi_loading_file);
	}
	// else: create table?
}
bool PackSizeInfo::RollBackPackSize()
{
	std::string  psi_loading_file = GetLoadingPackSizeFileName();
	if(DoesFileExist(psi_loading_file))
	{
		RemoveFile(psi_loading_file);
	}
}

