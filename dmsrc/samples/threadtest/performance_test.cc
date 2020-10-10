#ifdef WIN32
#include <process.h>
#define getch getchar
#else
#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include "AutoHandle.h"
#include "dt_common.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "ThreadList.h"
#include <vector>
#include <string>
#include <iostream>
using namespace std;

int	g_threadExit = 0;  //�߳���
pthread_mutex_t         mutex = PTHREAD_MUTEX_INITIALIZER;
int Start(void *ptr);
void* ThreadProc(void* ptr);  // �̺߳���

// ��ѯ��������
int g_column_type = 0;
std::vector<std::string>  g_test_condition_value_vec;
std::vector<long>  g_test_long_condition_value_vec;

/*
���÷�ʽ��
./wdbi_thread_test dsn user pwd dbname,tbname,tsttimes
dsn  ������Դ(data source name)
user : �������ݿ��û���
pwd  : �������ݿ�����
dbname : ���ݿ�����
tbname : ������
tsttimes   ��Ҫ���е����������ļ� 
*/
typedef struct _InitInfo
{
    char dsn[128];
    char user[128];
    char pwd[128];
    char srcdbname[64];
    char srctbname[64];
    char dbname[64];
    char tbname[64];
    char  colname[64]; // ��ѯ������
    int  startitems; // ��ʼ��
    int  items;  // colname�е�ֵ��Ŀ
    int  threads;
}_InitInfo,*_InitInfoPtr;

typedef struct _ThreadParam{
	_InitInfo _st;
	std::vector<std::string>  _test_condition_value_vec;
	std::vector<long>  _test_long_condition_value_vec;	
}_ThreadParam,*_ThreadParamPtr;

int main(int argc,char *argv[])  
{                   
    if(argc != 12){
        printf("performance_test:���ò����������.\n��ο�:./performance_test dsn user pwd srcdbname srctbname dbname tbname colname startitems items threads \n");
        std::cout<<"performance_test:���ò���˵����\n"
        	<<"sdn:		"<<"odbc��������Դ\n"
        	<<"user:	"<<"���ݿ��û���\n"
        	<<"pwd:		"<<"���ݿ�����\n"
        	<<"srcdbname:	"<<"��ѯ�������ݿ�ʵ������\n"
        	<<"srctbname:	"<<"��ѯ�������ݿ������\n"
        	<<"dbname:	"<<"���ܲ�ѯ���ݿ�ʵ������\n"
        	<<"tbname:	"<<"���ܲ�ѯ���ݿ������\n"
        	<<"colname:	"<<"��ѯ����ָ����\n"
        	<<"startitems:	"<<"��ѯ�������Կ�ʼ������\n"
        	<<"items:	"<<"��ѯ�������Լ�¼����\n"
        	<<"threads:	"<<"�����̲߳�����"<<endl;
        	
        return 0;	
    }
    int index = 1;
    _InitInfo stInitInfo;
    strcpy(stInitInfo.dsn,argv[index++]);
    strcpy(stInitInfo.user,argv[index++]);
    strcpy(stInitInfo.pwd,argv[index++]);
    
    strcpy(stInitInfo.srcdbname,argv[index++]);
    strcpy(stInitInfo.srctbname,argv[index++]);
    
    strcpy(stInitInfo.dbname,argv[index++]);
    strcpy(stInitInfo.tbname,argv[index++]);
    
    strcpy(stInitInfo.colname,argv[index++]);
    
    stInitInfo.startitems = atoi(argv[index++]);
    stInitInfo.items = atoi(argv[index++]);
    stInitInfo.threads = atoi(argv[index++]);
    int nRetCode = 0;
    WOCIInit("Performance/");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(true);
    nRetCode=wociMainEntrance(Start,true,(void*)&stInitInfo,2);
    WOCIQuit(); 
    return nRetCode;
}

int Start(void *ptr) 
{ 
	_InitInfoPtr pobj = (_InitInfoPtr)ptr;	


    // ��ȡ��ѯ����
    AutoHandle dtd;
    dtd.SetHandle(wociCreateSession(pobj->user,pobj->pwd,pobj->dsn,DTDBTYPE_ODBC));	
    AutoMt mt_condit(dtd,1000000);
    mt_condit.FetchFirst("select distinct %s from %s.%s where %s is not null limit %d,%d",
    	pobj->colname,pobj->srcdbname,pobj->srctbname,pobj->colname,pobj->startitems,pobj->items);
    	
    int cnt = mt_condit.Wait();
    if(cnt<=0){
       lgprintf("select distinct %s from %s.%s where %s is not null limit %d,%d ---> return <=0",
    	pobj->colname,pobj->srcdbname,pobj->srctbname,pobj->colname,pobj->startitems,pobj->items);
	   return 0;
    }

    // ��ѯ������ȡ
    g_column_type = mt_condit.getColType(0);
    for(int i=0;i<cnt;i++){
    	switch(g_column_type){
    	  case COLUMN_TYPE_CHAR:
    	  {
          	std::string _value_item = std::string(mt_condit.PtrStr(0,i));
          	g_test_condition_value_vec.push_back(_value_item);
          	break;
          }	
    	  case COLUMN_TYPE_INT:
    	  case COLUMN_TYPE_BIGINT:
    	  {
    	  	long _value_item = *mt_condit.PtrInt(0,i);
    	  	g_test_long_condition_value_vec.push_back(_value_item);
    	  	break;
    	  }
          default: 
          	lgprintf("column type err.\n");	
          	return -1;
    	}
    }
  
	
    pthread_t  *thread_array = new pthread_t[pobj->threads];
    
    // �����ݷ�ɢ���̶߳�����
    _ThreadParam *thread_param = new _ThreadParam[pobj->threads];
    int step_item = cnt/pobj->threads;
    for(int j = 0;j<pobj->threads;j++)
    {
    	memcpy(&thread_param[j]._st,pobj,sizeof(_InitInfo));
	    if (g_column_type == COLUMN_TYPE_CHAR){
	    	for(int i=0;i<step_item;i++){
	    		thread_param[j]._test_condition_value_vec.push_back(g_test_condition_value_vec.back());
	    		g_test_condition_value_vec.pop_back();	    		
	    	}    	
	    }
	    else{
	    	for(int i=0;i<step_item;i++){    	
	    		thread_param[j]._test_long_condition_value_vec.push_back(g_test_long_condition_value_vec.back());
	    		g_test_long_condition_value_vec.pop_back();	    		
	    	}
	    }
	}
    
    
    g_threadExit = 0;
    for (int i=0;i<pobj->threads;i++)
    {
        pthread_create(&thread_array[i],NULL,ThreadProc,&thread_param[i]);
    }
    
    while(g_threadExit!=pobj->threads)
    {
        sleep(1);
        continue;	
    }  
        
    delete [] thread_array;
    thread_array = NULL;

    return 1;
}


// �̴߳�����������ѹ����ѹ������
void* ThreadProc(void* ptr)
{
	_ThreadParamPtr pobj_init = (_ThreadParamPtr)ptr;
   	_InitInfoPtr pobj = (_InitInfoPtr)(&pobj_init->_st);	
    AutoHandle dtd;
	
    //--1. ����Ŀ�����ݿ�,ͬһ�������У�����߳̽���������Ҫ��ֹ����
    int   rc;
    rc = pthread_mutex_lock(&mutex);
    dtd.SetHandle(wociCreateSession(pobj->user,pobj->pwd,pobj->dsn,DTDBTYPE_ODBC));
    rc = pthread_mutex_unlock(&mutex);

	
    //--2. ������ѯ���
    AutoMt mt(dtd,1000000);
    int sleep_cnt = 0;

    while(1)
   {
	   	// ��ȡ��ѯ����ֵ
	   	sleep_cnt ++;
		if(sleep_cnt % 173 == 0){
			sleep(1);
		}
		
	     std::string str_value;
	     long int_value;
	   	 if(g_column_type == COLUMN_TYPE_CHAR){
			 if(!pobj_init->_test_condition_value_vec.empty()){
		   	 	str_value = pobj_init->_test_condition_value_vec.back();
				 pobj_init->_test_condition_value_vec.pop_back();
				 
			 }
			 else{			 	 
				 break;
			 }
		 }
		 else{
		     if(!pobj_init->_test_long_condition_value_vec.empty()){
		   	 	int_value = pobj_init->_test_long_condition_value_vec.back();
				 pobj_init->_test_long_condition_value_vec.pop_back();				 
			 }
			 else{			 	 
				 break;
			 }
		 	
		}
	
		// �Բ�ѯ�������в�ѯ
		mytimer tm;
		tm.Start();
		if(g_column_type == COLUMN_TYPE_CHAR){
	    	mt.FetchFirst("select * from %s.%s where %s='%s' ",pobj->dbname,pobj->tbname,pobj->colname,str_value.c_str());
	    }
	    else{
	    	mt.FetchFirst("select * from %s.%s where %s=%d ",pobj->dbname,pobj->tbname,pobj->colname,int_value);
	    }
	    int rn = mt.Wait();
		tm.Stop();
		
		if(g_column_type == COLUMN_TYPE_CHAR){
	    	lgprintf("======threadid[%lu],[%s='%s'] ---> fetch rows [%d], user time %0.2f sec. \n",
		 	 	pthread_self(),pobj->colname,str_value.c_str(),rn,tm.GetTime());
		}
		else{
			lgprintf("======threadid[%lu],[%s='%d'] ---> fetch rows [%d], user time %0.2f sec. \n",
		 	 	pthread_self(),pobj->colname,int_value,rn,tm.GetTime());
		}
    }
    g_threadExit++;  
    return NULL;
}
