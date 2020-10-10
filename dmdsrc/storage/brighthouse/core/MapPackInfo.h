#ifndef MAP_PACK_INFO_DEF_H
#define MAP_PACK_INFO_DEF_H

#include "common/CommonDefinitions.h"
#include "system/IBFileSystem.h"
#include "apindex.h"  // 是用AutoLock资源
#include <vector>
#include <string>

#define  INVALID_PACK_NO   -1
typedef struct StruMapItem
{
	int virtual_pack_no;  	// virtual pack no
	int real_pack_no;		// real pack restore to riak
	int valid;			// 判断包是否有效(0:无效，1有效)
	StruMapItem()
	{
		virtual_pack_no = INVALID_PACK_NO;
		real_pack_no = INVALID_PACK_NO;
		valid = 0;
	}
	StruMapItem(int vp,int rp,char _valid)
	{
		virtual_pack_no = vp;
		real_pack_no = rp;
		valid = _valid;
	}
	void Invalid(){
		virtual_pack_no = INVALID_PACK_NO;
		real_pack_no = INVALID_PACK_NO;
		valid = 0;
	}
}StruMapItem,*StruMapItemPtr;

typedef struct StruDelPackItem{
  int  start_pack;		// start delete packno
  int  del_packs;		// delete pack numbers
  StruDelPackItem(){
   	start_pack = 0;
   	del_packs = 0;
  }
  StruDelPackItem(int sp,int dp)
  {
	start_pack = sp;
	del_packs = dp;
  }
 }StruDelPackItem,*StruDelPackItemPtr;

typedef std::vector<StruDelPackItem> DelPackList;		// 删除分区时候使用到的
typedef std::vector<StruMapItem> MapPackList;

// 每一个列保存一个,xxxx表示列号
// 提交过的文件名称: MAP_PACKNO_xxxxx.bin
// 未提交过的文件: MAP_PACKNO_xxxxx.bin.tmp
/*
 MapPackInfo 文件中数据组成形式 
[节点1: v_pack_no<虚包号: 4字节>,r_pack_no<riak包号:4字节>,valid<包是否保存到riak中(0:没有保存到riak中，1:保存到riak中):1字节>]
[节点  1: v_pack_no<虚包号: 4字节>,r_pack_no<riak包号:4字节>,valid<包是否保存到riak中(0:没有保存到riak中，1:保存到riak中):1字节>]
[节点  2: v_pack_no<虚包号: 4字节>,r_pack_no<riak包号:4字节>,valid<包是否保存到riak中(0:没有保存到riak中，1:保存到riak中):1字节>]
.......
[节点N-1: v_pack_no<虚包号: 4字节>,r_pack_no<riak包号:4字节>,valid<包是否保存到riak中(0:没有保存到riak中，1:保存到riak中):1字节>]
[节点  N: v_pack_no<虚包号: 4字节>,r_pack_no<riak包号:4字节>,valid<包是否保存到riak中(0:没有保存到riak中，1:保存到riak中):1字节>]
[raik中最大包号:4字节]
[节点数:4字节]
*/

class MapPackInfo
{
protected:
	MapPackList  map_pack_list;
	int riak_max_pack_no;
	int attr_number;
	
public:
 	MapPackInfo(int attr_no);
 	virtual ~MapPackInfo();

public:
	// 提交过的文件名称: MAP_PACKNO_xxxxx.bin
	std::string GetMapPackFileName(std::string&  tabpth);
	// 未提交过的文件: MAP_PACKNO_xxxxx.bin.tmp
	std::string GetTmpMapPackFileName(std::string&  tabpth);
	
	bool MapPackIsValid2Save();  // 判断内存映射文件是否需要保存
	void ClearList();
	inline MapPackList& GetMapPackList(){return map_pack_list;};
	
public:
	int LoadPackList(std::string&  filename);   // 读取文件内容到 map_pack_list 
	int SavePackList(std::string&  filename);	  // map_pack_list 保存到文件
	
	int InitPackList(const MapPackList init_pack_list); // 第一次删除分区的时候，将pack列表初始化

 	// 编号增加,将之前没有加入的包号都添加进入,valid[0:无效包号(表示将之前所有无效包号的保存进入,bhloader 装入最后一个包调用),1:有效包号],
	int AddSinglePack(const int vpackno,const char valid=1);  
	
	int	UpdateSinglePackList(const int voldpackno,const int vnewpackno);  // load的时候使用
	int UpdateRangePackList(const int startpackno,const int endpackno); // 删除的时候使用，将一段包的编号进行更新

	int RPack2VPack(const int rpackno);
	int VPack2RPack(const int vpackno);

	int  GetRiakMaxPack();
	void AddRiakMaxPack();

	int GetPackListSize();
};


//////////////////////////////////////////////////////////////////////////////////////////////////

// 
// 1. 正在装入的单次装入文件大小的文件
// PackSize_tabid.loading.ctl ,文件格式如下:
/*
[分区名称1长度:2字节][分区名称1:分区名称1长度][分区1占用字节数:8字节]
*/

// 2. 装入完成的文件,PackSize_tabid.ctl 文件,文件格式如下
/*
[分区数:2字节]
[分区名称1长度:2字节][分区名称1:分区名称1长度][分区1占用字节数:8字节]
[分区名称2长度:2字节][分区名称2:分区名称1长度][分区2占用字节数:8字节]
[分区名称3长度:2字节][分区名称3:分区名称1长度][分区3占用字节数:8字节]
[分区名称4长度:2字节][分区名称4:分区名称1长度][分区4占用字节数:8字节]
....
更新文件的时候，将PackSize_tabid.ctl 内容读取出来后，写入
PackSize_tabid.commit.ctl
写入完成后，将PackSize_tabid.commit.ctl --> PackSize_tabid.ctl
*/
class PackSizeInfo
{
public:
	PackSizeInfo(std::string path,int tabid);
	virtual ~ PackSizeInfo();

protected:
	std::string _strpath;
	int _tabid;

	// 分区包字节信息结构
	typedef struct StruPartPackSize
	{
		short part_name_len;
		char part_name[256];
		unsigned long packsize;
		StruPartPackSize(){
			part_name_len = 0;
			part_name[0] = 0;
			packsize = 0;
		}
	}StruPartPackSize;

public:
	std::string GetPackSizeFileName();	// PackSize_tabid.ctl 
	std::string GetCommitPackSizeFileName(); // PackSize_tabid.ctl.tmp 
	std::string GetLoadingPackSizeFileName();// PackSize_tabid.loading.ctl	
	bool SaveTmpPackSize(const std::string strPartName,const unsigned long packsize); //  PackSize_tabid.loading.ctl	
	bool GetTmpPackSize(std::string& strPartName,unsigned long& packsize);
	bool UpdatePartPackSize(const std::string strPartName,const unsigned long packsize); //  PackSize_tabid.ctl
	bool DeletePartPackSize(const std::string strPartName)	;//   PackSize_tabid.ctl
	bool GetTableSize(); // PackSize_tabid.ctl 中所有分区大小
	bool CommitPackSize();
	bool RollBackPackSize();
};

#endif
