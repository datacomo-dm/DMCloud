#include <string.h>
#include <stdio.h>
#include "tbexport_jni.h"
#include "tbexport_helper.h"

#define MAX_EXPORTER_NUM   50
#define EXPORTER_BASE      200

//--------------------------------------------------------------------------------------------------------
pthread_mutex_t     g_exporterlock;         // ��
bool                g_exporter_init_flag = false;
tbexport            g_exporter[MAX_EXPORTER_NUM];  // exporter

/*
    ����: onInit
    ����: ��ʼ���⣬������Դ(�����Ƶ�)
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
    ����ֵ:
         0: �ɹ�
         -1:ʧ��
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_onInit(JNIEnv *env,jobject obj)
{
    printf("NativeCSV_onInit...\n");
    if(g_exporter_init_flag == true){
    	return (jint)0;
    }
    pthread_mutex_init(&g_exporterlock,NULL);
    pthread_mutex_lock(&g_exporterlock);
    if(g_exporter_init_flag == true){
        pthread_mutex_unlock(&g_exporterlock);
    	return (jint)0;
    }
    for(int i=0;i<MAX_EXPORTER_NUM;i++)
    {
       g_exporter[i].setId(-1);
       g_exporter[i].setRunFlag(false);
       g_exporter[i].setStatus(exp_unstart);
    }
    
    // wdbi ��־��ֻ�ܳ�ʼ��1��
    if(!g_logInit){
        g_logInit = true;
        WOCIInit("tbloader_log/tbloader");
        wociSetOutputToConsole(TRUE);
        wociSetEcho(true);
        g_sysPath[0]= 0;
        LoadSysPath(g_sysPath);
    }
    
    pthread_mutex_unlock(&g_exporterlock);
    g_exporter_init_flag = true;    
    lgprintf("NativeCSV_onInit ok.\n");
    return (jint)0;
}

/*
    ����: onUninit
    ����: �ͷ����������Դ(�����Ƶ�)
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
    ����ֵ:
         0: �ɹ�
         -1:ʧ��
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_onUninit(JNIEnv *env,jobject obj)
{
    lgprintf("NativeCSV_onUninit...\n");
	if(g_exporter_init_flag == false){
    	return (jint)0;
    }
    pthread_mutex_lock(&g_exporterlock);
	if(g_exporter_init_flag == false){
        pthread_mutex_unlock(&g_exporterlock);
    	return (jint)0;
    }
    int retcode = 0;
    for(int i=0;i<MAX_EXPORTER_NUM;i++)
    {
       if(g_exporter[i].getRunFlag()){
          g_exporter[i].stop();
          g_exporter[i].setStatus(exp_unstart);
       }
    }       
    pthread_mutex_unlock(&g_exporterlock);
    pthread_mutex_destroy(&g_exporterlock);
    g_exporter_init_flag = false;
    lgprintf("NativeCSV_onUninit ok.\n");
    
    if(g_logInit){
        g_logInit = false;
        WOCIQuit(); 
    }
    return (jint)retcode;   
}

/*
    ����: getCsvID
    ����: ��ȡ���е�CSV����ID
    ����: 
          evn[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
    ����ֵ:
         -1: ʧ��û�п��п��õ�ID
         [0-50]: ���õ�ID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_getCsvID(JNIEnv *env,jobject obj)
{    
    int storerid ;
    pthread_mutex_lock(&g_exporterlock);
    for(int i=0;i<MAX_EXPORTER_NUM;i++)
    {
        if(!g_exporter[i].getRunFlag())  
        {
            storerid = i+EXPORTER_BASE;
            g_exporter[i].setRunFlag(true);
            g_exporter[i].setId(storerid);
            g_exporter[i].setStatus(exp_unstart);
            break;
        }
    }
    pthread_mutex_unlock(&g_exporterlock);
    lgprintf("getCsvID storerid = %d\n",storerid);
    return (jint)storerid;

}

/*
    ����: initCsv
    ����: ��ʼ����csv�ļ����ݽӿ�
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          csvID[in] : csvID
          filepath[in]:csv �ļ�����
          dsn[in]   : odbc �������ݿ�����Դ
          user[in]  : odbc ����Դ�������ݿ���û�����
          pwd[in]   : odbc ����Դ�������ݿ���û�����
          SQL[in]   : sql���
          logpth[in]: ��־���·��
    ����ֵ:
          0: �ɹ�
          -1: ���ݿ�����ʧ�� 
          1: �Ѿ���ʼ�����������ظ�����
          2: ��ЧID
    ǰ������:
          1. odbc ����Դ��������
          2. getCsvID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_initCsv(JNIEnv *env,jobject obj,jint csvID,
                          jstring filepath,jstring dsn ,jstring user,jstring pwd,jstring SQL,jstring logpth)
{
    int ret = 0;
    
    int _csvID = (int)csvID;
    if(_csvID > MAX_EXPORTER_NUM+EXPORTER_BASE || _csvID < EXPORTER_BASE){  	  
        lgprintf("initCsv csvID = %d error.\n",_csvID);
        return 2;
    }
    
    if (g_exporter[_csvID-EXPORTER_BASE].getStatus()!= exp_unstart && g_exporter[_csvID-EXPORTER_BASE].getStatus() != exp_hasstoped)
    {
        lgprintf("initCsv getStatus() = %d\n",g_exporter[_csvID-EXPORTER_BASE].getStatus());
        return 1;
    }
               
    _InitInfo st;
    strcpy(st.dsn,(char *)env->GetStringUTFChars(dsn, 0));
    strcpy(st.user,(char *)env->GetStringUTFChars(user, 0));
    strcpy(st.pwd,(char *)env->GetStringUTFChars(pwd, 0));
    strcpy(st.fn_ext,(char *)env->GetStringUTFChars(filepath, 0));
    const char* logpath = (char *)env->GetStringUTFChars(filepath, 0);
    const char* strsql = (char*)env->GetStringUTFChars(SQL, 0);
    
    lgprintf("initStorerForDB params [storerid = %d,dsn = %s,user = %s pwd = %s,dbname = %s,logpth=%s]\n",
           _csvID,st.dsn,st.user,st.pwd,logpath);
           
    pthread_mutex_lock(&g_exporterlock);
    ret = g_exporter[_csvID-EXPORTER_BASE].start(&st,strsql,logpath);
    pthread_mutex_unlock(&g_exporterlock);
    
    return (jint)ret;
}

/*
    ����: writeCSV
    ����: дcsv�ļ��ӿ�
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          csvID[in] : csvID
    ����ֵ:
          true : �ɹ�
          false: ʧ��
    ǰ������:
          1. initCsv
    ˵��:�ñ�������첽ִ�еģ��ڲ������߳�ִ��
*/
JNIEXPORT jboolean JNICALL Java_com_datamerger_file_NativeCSV_writeCSV(JNIEnv *env,jobject obj,jint csvID)
{
    int ret = 0;
    
    int _csvID = (int)csvID;
    if(_csvID > MAX_EXPORTER_NUM+EXPORTER_BASE || _csvID < EXPORTER_BASE){	  
        lgprintf("initCsv csvID = %d error.\n",_csvID);
        return false;
    }
    lgprintf("writeCSV csvid = %d.",_csvID);  
    ret = g_exporter[_csvID-EXPORTER_BASE].doStart();
    return (jboolean)(ret == 0 ? true:false);  
}

/*
    ����: stateCSV
    ����: дcsv�ļ��ӿ�
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          csvID[in] : csvID
    ����ֵ: ������_��ǰд���¼��_�����ܼ�¼��
        ������ȡֵ:
        0: �ɹ�  <0_��ǰд���¼��_�����ܼ�¼��>
        1: ʧ�ܣ�д������ʧ�� <1_��ǰд���¼��_�����ܼ�¼��>              
    ǰ������:
          1. initCsv
    ˵��:�ñ�������첽ִ�еģ��ڲ������߳�ִ��
*/
JNIEXPORT jstring  JNICALL Java_com_datamerger_file_NativeCSV_stateCSV(JNIEnv *env,jobject obj,jint csvID)
{
    char rtnValue[128] = {0};
    jstring rtn;
    
    int _csvID = (int)csvID;
    
    if(_csvID > MAX_EXPORTER_NUM+EXPORTER_BASE || _csvID < EXPORTER_BASE){	  
        lgprintf("stateCSV csvID = %d error.\n",_csvID);
        strcpy(rtnValue,"1_0_0");        
        rtn = env->NewStringUTF(rtnValue);
        return rtn;
    }    
    sprintf(rtnValue,"0_%d_%d",g_exporter[_csvID-EXPORTER_BASE].getCurrentRows(),g_exporter[_csvID-EXPORTER_BASE].getRowSum());
    lgprintf("id=[%d] stateCSV return : %s.\n",_csvID,rtnValue);
    rtn = env->NewStringUTF(rtnValue);
    return rtn;
}

/*
    ����: closeCsv
    ����: �ͷ�csv�ӿ�
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          csvID[in] : csvID
    ����ֵ:
          0: �ɹ�  
          1: ��Ч��ID
    ǰ������:
          1. getCsvID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_closeCsv(JNIEnv *env,jobject obj,jint csvID)
{
    int ret = 1;
    int _csvID = (int)csvID;
    if(_csvID > MAX_EXPORTER_NUM+EXPORTER_BASE || _csvID < EXPORTER_BASE){
        lgprintf("closeCsv csvID = %d error.\n",_csvID);
        return (jint)ret;
    }
    ret = g_exporter[_csvID-EXPORTER_BASE].stop();
    g_exporter[_csvID-EXPORTER_BASE].setId(-1);
    g_exporter[_csvID-EXPORTER_BASE].setStatus(exp_hasstoped);
    lgprintf("closeCsv csvid = %d.\n",_csvID);
   
    return (jint)0;
}
