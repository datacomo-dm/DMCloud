#ifndef TBLOADER_JIN_DEF_H
#define TBLOADER_JIN_DEF_H

/********************************************************************
  file : tbexport_jni.h
  desc : define the tbexport java call interface
  author: liujianshun,201304
  note: support multiple thread use
  libname:libtbexporter.so
********************************************************************/
#include <jni.h>

#ifdef __cplusplus  
extern "C" {  
#endif 
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
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_onInit(JNIEnv *env,jobject obj);

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
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_onUninit(JNIEnv *env,jobject obj);

/*
    ����: getCsvID
    ����: ��ȡ���е�CSV����ID
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
    ����ֵ:
         -1: ʧ��û�п��п��õ�ID
         �ɹ���[0-50]: ���õ�ID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_getCsvID(JNIEnv *env,jobject obj);

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
                          jstring filepath,jstring dsn ,jstring user,jstring pwd,jstring SQL,jstring logpth); 

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
JNIEXPORT jboolean JNICALL Java_com_datamerger_file_NativeCSV_writeCSV(JNIEnv *env,jobject obj,jint csvID);

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
JNIEXPORT jstring  JNICALL Java_com_datamerger_file_NativeCSV_stateCSV(JNIEnv *env,jobject obj,jint csvID);

/*
    ����: closeCsv
    ����: �ͷ�csv�ӿ�
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          csvID[in] : csvID
    ����ֵ:
          0: �ɹ�  
          -1: ��Ч��ID
    ǰ������:
          1. getCsvID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_closeCsv(JNIEnv *env,jobject obj,jint csvID);


#ifdef __cplusplus  
}
#endif 

#endif
