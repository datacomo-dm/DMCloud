#include "hdfs.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <iostream>

using namespace std;

#define LISTFILE  "./file.lst"

// ��hdfs hdfsListDirectory �������ļ����л�ȡ�ļ�����
// file:/tmp/pth/tmp/data_2013010206_14.txt --> /tmp/pth/tmp/data_2013010206_14.tx
std::string _get_file_name(const char* ls_hdfs_file)
{
	printf("begin:--> %s \n",ls_hdfs_file);
	const char *p=ls_hdfs_file;
	std::string _hdfs_file(p+5);
		
	printf("end:--> %s \n",_hdfs_file.c_str());
	return _hdfs_file;	
}

void listHdfs(const char* nodename,const int port,const char* path,const char* filepatt)
{
	hdfsFS hdfsclient = hdfsConnect(nodename,port);
	if(hdfsclient == NULL){
		printf("����hdfs�ļ�ϵͳ[nodename=%s,port=%s] ʧ��.\n",nodename,port);
	}
	char hdfs_path[200];
	strcpy(hdfs_path,path);  // ���»�ȡ�ļ���Сʹ��
	
	// api : hadoop fs -ls /tmp/*.cpp | ls  -1
	char path_pattern[300] = {0};
	sprintf(path_pattern,"%s/%s",path,filepatt);
	int hdfs_num_entries = 0;
	hdfsFileInfo * phdfsFileInfoAry = hdfsListDirectory(hdfsclient,path_pattern,&hdfs_num_entries);
	if( NULL == phdfsFileInfoAry){
		printf(" file_man::listHdfs  hdfsListDirectory(%p,%s,%p) return NULL\n",hdfsclient,path_pattern,&hdfs_num_entries);
		return ;
	}
	else{
		// ��ȡ�ļ���Ϣ����list�ļ���
		FILE *fp=fopen(LISTFILE,"wt");
		if(fp==NULL) {
			printf("����ļ�%s��ʧ��.",LISTFILE);
		}
		//GLOB_NOSORT  Don��t sort the returned pathnames.  The only reason to do this is to save processing time.  By default, the returned pathnames are sorted.
		for(int i=0;i<hdfs_num_entries;i++){
			if(phdfsFileInfoAry[i].mKind == kObjectKindFile){
				std::string _hdfs_file = _get_file_name(phdfsFileInfoAry[i].mName);
				fprintf(fp,"%s\n",_hdfs_file.c_str());
			}
		}
		fclose(fp);
	}
	
	printf("��ȡ�ļ��б� ...");
	FILE *flist=fopen(LISTFILE,"rt");
	if(flist==NULL) printf("�ļ��б�ʧ��.���ļ��б�%sʧ��.");
	fclose(flist);flist=NULL;
	printf("list hdfs sec.\n");
	
	hdfsFreeFileInfo(phdfsFileInfoAry,hdfs_num_entries);
	
	printf("\n\n");
	
}

int main(int argc, char **argv) {
	listHdfs("default", 0,"/tmp/pth/tmp/","data_2013010206*.txt");
	
	listHdfs("default", 0,"/tmp/pth/tmp/","");

	return 0;
}
