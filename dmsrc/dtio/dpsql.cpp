#include "dtio.h"
#include "dt_common.h"
#include "mysqlconn.h"
#include "dt_svrlib.h"
#include "dt_cmd_base.h"
#include <time.h>
#include <sys/types.h>
#include <unistd.h>

class Mutex
{
#ifdef __unix
    pthread_mutex_t mutexobj;
#else
    CRITICAL_SECTION mutexobj;
#endif
public :
    Mutex() {
#ifdef __unix
        pthread_mutex_init(&mutexobj,NULL);
#else
        InitializeCriticalSection(&mutexobj);
#endif
    }
    int Lock() {
#ifdef __unix
        int rt=pthread_mutex_lock(&mutexobj);
#else
        DWORD rt=WAIT_OBJECT_0;
        EnterCriticalSection(&mutexobj);
#endif
        return rt;
    }
    void Unlock() {
#ifdef __unix
        pthread_mutex_unlock(&mutexobj);
#else
        LeaveCriticalSection(&mutexobj);
#endif
    }
    ~Mutex() {
#ifndef __unix
        DeleteCriticalSection(&mutexobj);
#else
        pthread_mutex_destroy(&mutexobj);
#endif
    }
    int TryLock() {
        return pthread_mutex_trylock(&mutexobj);
    }
};

class mytimer2  // 计时器,sleep也计算在内,精度秒
{
public:
    mytimer2()
    {
        Restart();
    }
    virtual ~mytimer2()
    {
        Restart();
    }

public:
    void Restart()
    {
        tm_mutex.Lock();
        tm_start = time(NULL);
        tm_stop = time(NULL);
        tm_mutex.Unlock();
    }
    double GetTime()
    {
        tm_mutex.Lock();
        tm_stop = time(NULL);
        double diff_sec = difftime(tm_stop,tm_start);
        tm_mutex.Unlock();
        return diff_sec;
    }

private:
    Mutex  tm_mutex;
    time_t tm_start;
    time_t tm_stop;
};

class mytimer3  // 计时器,sleep也计算在内,精度毫秒
{
public:
    mytimer3()
    {
        Restart();
    }
    virtual ~mytimer3()
    {
        Restart();
    }

public:
    void Restart()
    {
        tm_mutex.Lock();
        gettimeofday(&tv, NULL);
        tm_start = 0;
        tm_start = tv.tv_usec / 1000 + tv.tv_sec * 1000;  // ms
        tm_mutex.Unlock();
    }
    double GetTime()
    {
        tm_mutex.Lock();
        long long tm_end = 0;
        gettimeofday(&tv, NULL);
        tm_end = tv.tv_usec / 1000 + tv.tv_sec * 1000;  // ms
        double diff_sec = (double)(tm_end-tm_start)/(double)1000; // second
        tm_mutex.Unlock();
        return diff_sec;
    }

private:
    Mutex  tm_mutex;
    struct timeval tv;
    unsigned long long tm_start;
};




int cmd_base::argc=0;
char **cmd_base::argv=(char **)NULL;
int Start(void *p);
int main(int argc,char **argv) {
    int nRetCode = 0;
    cmd_base::argc=argc;
    cmd_base::argv=(char **)argv;
    if(argc!=2) {
    	return -1;
    } 
    WOCIInit("util/dpsql");
    int corelev=2;
    const char *pcl=getenv("DP_CORELEVEL");
    if(pcl!=NULL) corelev=atoi(pcl);
    if(corelev<0 || corelev>2) corelev=2;
    nRetCode=wociMainEntrance(Start,true,NULL,corelev);
    WOCIQuit(); 
    return nRetCode;
}

int Start(void *p) {
   wociSetOutputToConsole(FALSE);
   wociSetEcho(FALSE);
   cmd_base cp;
   cp.GetEnv();
   if(!cp.checkvalid()) return -1;

   mytimer3 tm_con;
   mytimer3 tm_exe;

   double con_long=0;
   double exe_long=0;

   int __ret_lines=0;
   int __ret_cols=0;


   AutoHandle dts;

   tm_con.Restart();
   dts.SetHandle(wociCreateSession(cp.musrname,cp.mpswd,cp.mhost,DTDBTYPE_ODBC));
   con_long=tm_con.GetTime();

   int connect_id=0;
   {    
       AutoMt mdf(dts,10);
       mdf.FetchFirst("select connection_id() cid");
       mdf.Wait();
       connect_id=(int)mdf.GetLong("cid",0);    
   }

   tm_exe.Restart();
   AutoMt mt(dts,10000);
   mt.FetchFirst(cp.argv[1]);
   if(mt.Wait()<1){ 
       exe_long = tm_exe.GetTime();
       
       printf("[%d]---->sql:%s\n",(int)getpid(),cp.argv[1]);
       printf("[%d]---->\tconn_id[\t%d\t]\trows[\t%d\t]\tcols[\t%d\t]\tconnect_time[%.4f]\tquery_time[\t%.4f\t]\t.\n",(int)getpid(),
            connect_id,__ret_lines,__ret_cols,con_long,exe_long);
       
       return -1;
   }
   exe_long = tm_exe.GetTime();
   
   __ret_lines=mt.GetRows();
   __ret_cols=mt.GetColumnNum();
   printf("[%d]---->sql:%s\n",(int)getpid(),cp.argv[1]);
   printf("[%d]---->\tconn_id[\t%d\t]\trows[\t%d\t]\tcols[\t%d\t]\tconnect_time[%.4f]\tquery_time[\t%.4f\t]\t.\n",(int)getpid(),
        connect_id,__ret_lines,__ret_cols,con_long,exe_long);
    
   char line[30000];
   wociGetLine(mt,0,line,false,NULL);
   printf("[%d]---->[\t%s\t]\n",(int)getpid(),line);

   
   return 0;
}

