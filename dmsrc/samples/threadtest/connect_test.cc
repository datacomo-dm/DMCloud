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
#include "dt_svrlib.h"

typedef struct _InitInfo
{
    char dsn[128];
    char user[128];
    char pwd[128];
    char sql[2048];
    int  tsttimes;	
}_InitInfo,*_InitInfoPtr;

int Start(void *);
int main(int argc,char *argv[])
{
    if(argc != 6){
        printf("connect_test:���ò����������.\n��ο�:./connect_test dsn user pwd \"sql\" times\n");
        return 0;
    }
    int index = 1;
    _InitInfo stInitInfo;
    strcpy(stInitInfo.dsn,argv[index++]);
    strcpy(stInitInfo.user,argv[index++]);
    strcpy(stInitInfo.pwd,argv[index++]);
    strcpy(stInitInfo.sql,argv[index++]);
    stInitInfo.tsttimes = atoi(argv[index++]);
    int nRetCode = 0;
    WOCIInit("connect_test");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(true);
    nRetCode=wociMainEntrance(Start,true,(void*)&stInitInfo,2);
    WOCIQuit();
    return nRetCode;
}

int Start(void *ptr)
{
    _InitInfoPtr pobj = (_InitInfoPtr)ptr;
    AutoHandle dtd;
   
        //--1. ����Ŀ�����ݿ�,ͬһ�������У�����߳̽���������Ҫ��ֹ����
    int   rc;
    mytimer tm;
    tm.Start();
    dtd.SetHandle(wociCreateSession(pobj->user,pobj->pwd,pobj->dsn,DTDBTYPE_ODBC));
    tm.Stop();
    lgprintf("�������ݿ⣬ʹ��(ʱ��%.2f��)��",tm.GetTime());
    
//    tm.Start();
//    MySQLConn  m_pConnector;
//    m_pConnector.Connect("192.168.1.10","root","dbplus03",NULL,3307);
 //   tm.Stop();
  //  lgprintf("�������ݿ�mysql������ʹ��(ʱ��%.2f��)��",tm.GetTime());

    //--2. ������ѯ���
    AutoMt mt(dtd,1000200);

    for(int i=0;i<pobj->tsttimes;i++)
    {
       tm.Clear();
       tm.Start();
       mt.FetchFirst(pobj->sql);
       int rn = mt.Wait();
       tm.Stop();
       lgprintf("fetch times %d,fetch rows [%d] ʹ��(ʱ��%.2f��).\n",i+1,tm.GetTime(),rn);
    }
    
    return 1;
}

