#ifndef MAP_PACK_INFO_DEF_H
#define MAP_PACK_INFO_DEF_H

#include "common/CommonDefinitions.h"
#include "system/IBFileSystem.h"
#include "apindex.h"  // ����AutoLock��Դ
#include <vector>
#include <string>

#define  INVALID_PACK_NO   -1
typedef struct StruMapItem
{
	int virtual_pack_no;  	// virtual pack no
	int real_pack_no;		// real pack restore to riak
	int valid;			// �жϰ��Ƿ���Ч(0:��Ч��1��Ч)
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

typedef std::vector<StruDelPackItem> DelPackList;		// ɾ������ʱ��ʹ�õ���
typedef std::vector<StruMapItem> MapPackList;

// ÿһ���б���һ��,xxxx��ʾ�к�
// �ύ�����ļ�����: MAP_PACKNO_xxxxx.bin
// δ�ύ�����ļ�: MAP_PACKNO_xxxxx.bin.tmp
/*
 MapPackInfo �ļ������������ʽ 
[�ڵ�1: v_pack_no<�����: 4�ֽ�>,r_pack_no<riak����:4�ֽ�>,valid<���Ƿ񱣴浽riak��(0:û�б��浽riak�У�1:���浽riak��):1�ֽ�>]
[�ڵ�  1: v_pack_no<�����: 4�ֽ�>,r_pack_no<riak����:4�ֽ�>,valid<���Ƿ񱣴浽riak��(0:û�б��浽riak�У�1:���浽riak��):1�ֽ�>]
[�ڵ�  2: v_pack_no<�����: 4�ֽ�>,r_pack_no<riak����:4�ֽ�>,valid<���Ƿ񱣴浽riak��(0:û�б��浽riak�У�1:���浽riak��):1�ֽ�>]
.......
[�ڵ�N-1: v_pack_no<�����: 4�ֽ�>,r_pack_no<riak����:4�ֽ�>,valid<���Ƿ񱣴浽riak��(0:û�б��浽riak�У�1:���浽riak��):1�ֽ�>]
[�ڵ�  N: v_pack_no<�����: 4�ֽ�>,r_pack_no<riak����:4�ֽ�>,valid<���Ƿ񱣴浽riak��(0:û�б��浽riak�У�1:���浽riak��):1�ֽ�>]
[raik��������:4�ֽ�]
[�ڵ���:4�ֽ�]
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
	// �ύ�����ļ�����: MAP_PACKNO_xxxxx.bin
	std::string GetMapPackFileName(std::string&  tabpth);
	// δ�ύ�����ļ�: MAP_PACKNO_xxxxx.bin.tmp
	std::string GetTmpMapPackFileName(std::string&  tabpth);
	
	bool MapPackIsValid2Save();  // �ж��ڴ�ӳ���ļ��Ƿ���Ҫ����
	void ClearList();
	inline MapPackList& GetMapPackList(){return map_pack_list;};
	
public:
	int LoadPackList(std::string&  filename);   // ��ȡ�ļ����ݵ� map_pack_list 
	int SavePackList(std::string&  filename);	  // map_pack_list ���浽�ļ�
	
	int InitPackList(const MapPackList init_pack_list); // ��һ��ɾ��������ʱ�򣬽�pack�б��ʼ��

 	// �������,��֮ǰû�м���İ��Ŷ���ӽ���,valid[0:��Ч����(��ʾ��֮ǰ������Ч���ŵı������,bhloader װ�����һ��������),1:��Ч����],
	int AddSinglePack(const int vpackno,const char valid=1);  
	
	int	UpdateSinglePackList(const int voldpackno,const int vnewpackno);  // load��ʱ��ʹ��
	int UpdateRangePackList(const int startpackno,const int endpackno); // ɾ����ʱ��ʹ�ã���һ�ΰ��ı�Ž��и���

	int RPack2VPack(const int rpackno);
	int VPack2RPack(const int vpackno);

	int  GetRiakMaxPack();
	void AddRiakMaxPack();

	int GetPackListSize();
};


//////////////////////////////////////////////////////////////////////////////////////////////////

// 
// 1. ����װ��ĵ���װ���ļ���С���ļ�
// PackSize_tabid.loading.ctl ,�ļ���ʽ����:
/*
[��������1����:2�ֽ�][��������1:��������1����][����1ռ���ֽ���:8�ֽ�]
*/

// 2. װ����ɵ��ļ�,PackSize_tabid.ctl �ļ�,�ļ���ʽ����
/*
[������:2�ֽ�]
[��������1����:2�ֽ�][��������1:��������1����][����1ռ���ֽ���:8�ֽ�]
[��������2����:2�ֽ�][��������2:��������1����][����2ռ���ֽ���:8�ֽ�]
[��������3����:2�ֽ�][��������3:��������1����][����3ռ���ֽ���:8�ֽ�]
[��������4����:2�ֽ�][��������4:��������1����][����4ռ���ֽ���:8�ֽ�]
....
�����ļ���ʱ�򣬽�PackSize_tabid.ctl ���ݶ�ȡ������д��
PackSize_tabid.commit.ctl
д����ɺ󣬽�PackSize_tabid.commit.ctl --> PackSize_tabid.ctl
*/
class PackSizeInfo
{
public:
	PackSizeInfo(std::string path,int tabid);
	virtual ~ PackSizeInfo();

protected:
	std::string _strpath;
	int _tabid;

	// �������ֽ���Ϣ�ṹ
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
	bool GetTableSize(); // PackSize_tabid.ctl �����з�����С
	bool CommitPackSize();
	bool RollBackPackSize();
};

#endif
