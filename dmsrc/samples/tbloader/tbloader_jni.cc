#include <string.h>
#include <stdio.h>
#include "tbloader_jni.h"
#include "tbloader_helper.h"


#define MAX_LOADER_NUM   50

// �������ݶ���
pthread_mutex_t     g_loaderlock;         // ��
bool                g_loader_init_flag = false;
tbloader_helper     g_loader[MAX_LOADER_NUM];  // loader

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
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_onInit(JNIEnv *env,jobject obj)
{
    printf("NativeStorer_onInit...\n");
    if(g_loader_init_flag == true){
    	return (jint)0;
    }
    pthread_mutex_init(&g_loaderlock,NULL);
    if(g_loader_init_flag == true){
        pthread_mutex_unlock(&g_loaderlock);
    	return (jint)0;
    }
    pthread_mutex_lock(&g_loaderlock);
    for(int i=0;i<MAX_LOADER_NUM;i++)
    {
       g_loader[i].setId(-1);
       g_loader[i].setRunFlag(false);
       g_loader[i].setStatus(status_unknown);
    }
    g_loader_init_flag = true;
    
    // wdbi ��־��ֻ�ܳ�ʼ��1��
    if(!g_logInit)
    {        
        g_logInit = true;
        WOCIInit("tbloader_log/tbloader");
        wociSetOutputToConsole(TRUE);
        wociSetEcho(true);
        g_sysPath[0]= 0;
        LoadSysPath(g_sysPath);
    } 
    pthread_mutex_unlock(&g_loaderlock);
    
    lgprintf("NativeStorer_onInit ok.");
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
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_onUninit(JNIEnv *env,jobject obj)
{
    lgprintf("NativeStorer_onUninit...");
    int retcode = 0;
    if(g_loader_init_flag == false){
       return (jint)0;
    }  	
    pthread_mutex_lock(&g_loaderlock);
    if(g_loader_init_flag == false){
       pthread_mutex_unlock(&g_loaderlock);
       return (jint)0;
    } 
    for(int i=0;i<MAX_LOADER_NUM;i++)
    {
       if(g_loader[i].getRunFlag()){
          g_loader[i].stop();
          g_loader[i].setId(-1);
       }
    }
    pthread_mutex_unlock(&g_loaderlock);
    pthread_mutex_destroy(&g_loaderlock);
    g_loader_init_flag = false;
    lgprintf("NativeStorer_onUninit ok.");
    if(g_logInit){
        g_logInit = false;
        WOCIQuit(); 
    }
    return (jint)retcode;   		
}

/*
    ����: getStorerID
    ����: ��ȡ���еĴ洢ID
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
    ����ֵ:
         -1: ʧ��û�п��п��õ�ID
         [0-50]: ���õ�ID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_getStorerID(JNIEnv *env,jobject obj)
{
    int storerid=-1;
    pthread_mutex_lock(&g_loaderlock);
    for(int i=0;i<MAX_LOADER_NUM;i++)
    {
        if(!g_loader[i].getRunFlag())  
        {
            storerid = i;
            g_loader[i].setRunFlag(true);
            g_loader[i].setStatus(unstart);
            g_loader[i].setId(i);
            g_loader[i].setSysPath(g_sysPath);
            break;
        }
    }
    pthread_mutex_unlock(&g_loaderlock);
    
    lgprintf("id=[%d] getStorerID storerid = %d",g_loader[storerid].getId(),storerid);
    return (jint)storerid;
}

/*
    ����: initStorerForDB
    ����: ��ʼ���ݴ洢����
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          storerid[in] : storerID
          dsn[in]   : odbc �������ݿ�����Դ
          user[in]  : odbc ����Դ�������ݿ���û�����
          pwd[in]   : odbc ����Դ�������ݿ���û�����
          dbname[in]: ���ݿ�ʵ������
          tbname[in]: ���ݿ������
          logpth[in]: ��־���·��
          engine[in]: ���ݱ�����<1:MyISAM> <2:brighthouse>
    ����ֵ:
          0: �ɹ�
          -1: ���ݿ�����ʧ�� 
          1: �Ѿ���ʼ�����������ظ�����
          2: ��Ч��storerid
    ǰ������:
          1. odbc ����Դ��������
          2. dbname.tbname Ҫ�����
          3. getStorerID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_initStorerForDB(JNIEnv *env,jobject obj,jint storerid,
                          jstring dsn,jstring user,jstring pwd,jstring dbname,jstring tbname,
                          jint engine,jstring logpth)
{
    int _storerid = storerid;
    if(_storerid > MAX_LOADER_NUM || _storerid < 0){
        lgprintf("initStorerForDB storerid = %d error.",_storerid);
        return 3;
    }
    
    if (g_loader[_storerid].getStatus()!= unstart &&  g_loader[_storerid].getStatus() != hasstoped)
    {
        lgprintf("initStorerForDB id[%d] getStatus() = %d error.",_storerid,g_loader[_storerid].getStatus());
        return 1;
    }
               
    _InitInfo st;
    strcpy(st.dsn,(char *)env->GetStringUTFChars(dsn, 0));
    strcpy(st.user,(char *)env->GetStringUTFChars(user, 0));
    strcpy(st.pwd,(char *)env->GetStringUTFChars(pwd, 0));
    strcpy(st.dbname,(char *)env->GetStringUTFChars(dbname, 0));
    strcpy(st.tbname,(char *)env->GetStringUTFChars(tbname, 0));
    int _engine = (int)engine;
    st.tbEngine = (_EngineType)_engine;
    const char* logpath = (char *)env->GetStringUTFChars(logpth, 0);    
    
    if(st.tbEngine != MyISAM && st.tbEngine != Brighthouse)
    {
       lgprintf("table %s.%s engine %d error,engine<1:MyISAM 2:brighthouse>.",st.dbname,st.tbname,st.tbEngine);
       return 5;
    }
	
    lgprintf("id=[%d] initStorerForDB params [storerid = %d,dsn = %s,user = %s pwd = %s,dbname = %s,tbname = %s,engine = %d,logpth=%s]",
           g_loader[storerid].getId(),storerid,st.dsn,st.user,st.pwd,st.dbname,st.tbname,st.tbEngine,logpath);
    
    int ret = 0;
    pthread_mutex_lock(&g_loaderlock);
    ret = g_loader[_storerid].start(&st,insert_db,logpath);	
    pthread_mutex_unlock(&g_loaderlock);
    return ret;    
}

/*
    ����: initStorerForFile
    ����: ��ʼ���ݴ洢����
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          storerid[in] : storerID
          logpth[in]: ��־���·��
          fname[in]: ����·���ļ�����
    ����ֵ:
          0: �ɹ�
          -1: ���ݿ�����ʧ�� 
          1: �Ѿ���ʼ�����������ظ�����
          2: �ļ���ʧ��
          3: ��Ч��storerid
    ǰ������:
          1. odbc ����Դ��������
          2. dbname.tbname Ҫ�����
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_initStorerForFile(JNIEnv *env,jobject obj,jint storerid,
                           jstring fname,jstring logpth)
{
    int _storerid = storerid;
    if(_storerid > MAX_LOADER_NUM || _storerid < 0){
        lgprintf("initStorerForFile storerid = %d error.",_storerid);
        return 3;
    }
    
    if (g_loader[_storerid].getStatus()!= unstart && g_loader[_storerid].getStatus() != hasstoped)
    {
        lgprintf("initStorerForFile getStatus() = %d error.",g_loader[_storerid].getStatus());
        return 1;
    }
    
    _InitInfo st;
    strcpy(st.fn_ext,(char *)env->GetStringUTFChars(fname, 0));
    const char* logpath = (char *)env->GetStringUTFChars(logpth, 0);
    
    lgprintf("id=[%d] initStorerForFile params [storerid = %d,filename = %s,logpth=%s]",
           g_loader[storerid].getId(),storerid,st.fn_ext,logpath);
           
    return g_loader[_storerid].start(&st,write_files,logpath);	
}

/*
    ����: writeHeadInfo
    ����: д����ͷ����
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          storerid[in] : storerID
          headerlen[in]   : �ļ���ͷ�����ݳ���,4�ֽ�
          header[in]: �ļ���ͷ������
    ����ֵ:
          0: �ɹ�
          -1: �ļ�д��ʧ��
          1: �ļ�ͷ�����ظ�����д��
          2: ���ݳ���У��ʧ��
          3: ��Ч��storerid
          4: δ��ʼ�� initStorerForFile ���� initStorerForDB
		  5: ����ͷ���ݽ�������
		  6: ����ͷ��Ŀ���ṹ��һ��
    ǰ������:
          1. initStorerForFile ���óɹ� ���� initStorerForDB ���óɹ�
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_writeHeadInfo(JNIEnv *env,jobject obj,jint storerid,
                          jint headerlen,jbyteArray header)
{
    int _storerid = storerid;
    if(_storerid > MAX_LOADER_NUM || _storerid < 0){
        lgprintf("writeHeadInfo storerid = %d error.",_storerid);
        return 3;
    }
    
    if (g_loader[_storerid].getStatus()>= parserheader){
        lgprintf("writeHeadInfo getStatus() = %d error.",g_loader[_storerid].getStatus());
        return 1;
    }
    if(g_loader[_storerid].getStatus()< hasstarted){
        lgprintf("writeHeadInfo getStatus() = %d error.",g_loader[_storerid].getStatus());
        return 4;
    }
    lgprintf("id=[%d] writeHeadInfo storerid = %d,headerlen = %d",g_loader[_storerid].getId(),_storerid,headerlen);
    
    jbyte * olddata = (jbyte*)env->GetByteArrayElements(header, 0);
    jint  oldsize = env->GetArrayLength(header);
    unsigned char* bytearr = (unsigned char*)olddata;
    int len = (int)oldsize;
    int _headerlen = (int)headerlen;
    if(len != (int)_headerlen){ 
        env->ReleaseByteArrayElements(header,(jbyte*)olddata,0);
        lgprintf("writeHeadInfo len [ %d != %d] error ",_headerlen,len);
        return 2;
    }
      
    int ret = g_loader[_storerid].parserColumnInfo((const HerderPtr)bytearr,_headerlen);
    env->ReleaseByteArrayElements(header,(jbyte*)olddata,0);    
    return (jint)ret;
}

/*
    ����: writeDataBlock
    ����: д����ͷ����
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          storerid[in] : storerID
          blocklen[in]   : �������ݳ���
          datablock[in]: ��������
    ����ֵ:
          0: �ɹ�
          -1: �ļ�д��ʧ��
          1: �ļ�ͷδд���,tl_writeHeadInfoδ���óɹ���
          2: ���ݳ���У��ʧ��
          3: ��Ч��storerid
          4: �Ѿ��ر�״̬��������д������
		  5: ���ݰ�����
    ǰ������:
          1. initStorerForFile ���óɹ� ���� initStorerForDB ���óɹ�
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_writeDataBlock(JNIEnv *env,jobject obj,jint storerid,
                          jint blocklen,jbyteArray datablock,jint rowNum)
{
    int _storerid = storerid;
    if(_storerid > MAX_LOADER_NUM || _storerid < 0){
        lgprintf("writeDataBlock storerid = %d error.",_storerid);
        return 3;
    }
    
    if (g_loader[_storerid].getStatus()< parserheader){
        lgprintf("writeDataBlock getStatus() = %d error.",g_loader[_storerid].getStatus());
        return 1;
    }
    if(g_loader[_storerid].getStatus() == hasstoped){
        lgprintf("writeDataBlock getStatus() = %d error.",g_loader[_storerid].getStatus());
        return 4;
    }
	if(g_loader[_storerid].getOperType() == write_files && rowNum<=0)
	{
        lgprintf("writeDataBlock getOperType() == write_files && rowNum<=0.");
        return 6;
    }
    Timer t1(_storerid,"NativeStorer_writeDataBlock ��������");
    dbg_lgprintf("id=[%d] writeDataBlock storerid = %d,blocklen = %d,rowNum = %d.",g_loader[_storerid].getId(),_storerid,blocklen,rowNum);
    
    jbyte * olddata = (jbyte*)env->GetByteArrayElements(datablock, 0);
    jint  oldsize = env->GetArrayLength(datablock);
    unsigned char* bytearr = (unsigned char*)olddata;
    int len = (int)oldsize;
    int _blocklen = (int)blocklen;
    if(len != (int)_blocklen){
        lgprintf("id=[%d] closeStorer block len [%d != %d] error = %d",_storerid,_blocklen,blocklen);         
        env->ReleaseByteArrayElements(datablock,(jbyte*)olddata,0);
        return 2;
    }
    
    int ret = g_loader[_storerid].writePackData((const char*)bytearr,_blocklen,rowNum);
    env->ReleaseByteArrayElements(datablock,(jbyte*)olddata,0);
    
    return (jint)ret;
}

/*
    ����: getDataMD5
    ����: ��ȡ���ݵ�md5У��ֵ
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          storerid[in] : storerID
          hasdata[in]   : �Ƿ�д�������
    ����ֵ:
          32���ַ�������md5У��ֵ
    ˵��:
          �ڵ���closeStorer֮ǰ����
*/
JNIEXPORT jstring JNICALL Java_com_datamerger_file_NativeStorer_getDataMD5(JNIEnv *env,jobject obj,jint storerid)
{
    int retcode = 0;
    int _storerid = storerid;
    char md5sum[conMd5StrLen] = {0};
    
    jstring rtn;
    // ��Чid��0000000
    if(_storerid > MAX_LOADER_NUM || _storerid < 0){
        memset(md5sum,0,conMd5StrLen);
        rtn = env->NewStringUTF(md5sum);
        lgprintf("id=[%d] getDataMD5 md5sum = %s",_storerid,md5sum);
        return rtn;
    }

	lgprintf("id=[%d] getDataMD5 begin ...",g_loader[storerid].getId());

	// ��û��ִ����ɣ��ȴ�write������ɺ��ٽ�������dm5ֵ��ȡ
	if(g_loader[_storerid].getStatus() < hasstoped){
		g_loader[_storerid].stop();
		lgprintf("id=[%d] stop finish ...",g_loader[storerid].getId());	
	}
       
    g_loader[_storerid].getMd5Sum(md5sum);
    lgprintf("id=[%d] getDataMD5 md5sum = %s",g_loader[storerid].getId(),md5sum);
    rtn = env->NewStringUTF(md5sum);
    return rtn;
}

/*
    ����: closeStorer
    ����: �ͷ�tbloader�ӿ�
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          storerid[in] : storerID
          hasdata[in]   : �Ƿ�д�������
    ����ֵ:
          code_md5sum<32�ֽڣ�����DataBlock��md5����У��ֵ(writeDataBlock����)>
          codeֵ:
          0: �ɹ�     
          1: ���ݿ��ļ�д��ʧ��
          2: �����ļ�д��ʧ��
          3: δ����initStorerForFile����initStorerForDB
          4: storerid����
    ǰ������:
          1. initStorerForFile ���óɹ� ���� initStorerForDB ���óɹ�
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_closeStorer(JNIEnv *env,jobject obj,jint storerid)
{
    int retcode = 0;
    int _storerid = storerid;
    if(_storerid > MAX_LOADER_NUM || _storerid < 0){
        lgprintf("closeStorer storerid = %d ",_storerid);
        return 4;
    }
   
    lgprintf("id=[%d] closeStorer storerid = %d,runflag = %d ",g_loader[_storerid].getId(),_storerid,g_loader[_storerid].getRunFlag()?1:0);
	// �п���getDataMD5���Ѿ�������
    if(g_loader[_storerid].getRunFlag() == true){
        retcode = g_loader[_storerid].stop();
    }       
    g_loader[_storerid].setId(-1); 

    lgprintf("after close: id=[%d] closeStorer storerid = %d,runflag = %d ",g_loader[_storerid].getId(),_storerid,g_loader[_storerid].getRunFlag()?1:0);

    return (jint)retcode;
}
