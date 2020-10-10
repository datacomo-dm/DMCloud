#ifndef __BLOOM_TEST_H__
#define __BLOOM_TEST_H__

// create by liujs
#include <stdlib.h>

typedef unsigned int (*hashfunc_t)(const char *,unsigned int len);


typedef struct StruHashFuncItem
{
	hashfunc_t func;
	char *pFuncName;
}StruHashFuncItem;

class bloom_test 
{
protected:
  	int m_comb_num; // ѡ�������
    int pos_fun;  // �ƶ���λ��
    hashfunc_t* m_res_fun;  // hash ����
    hashfunc_t* m_arr_fun;  // ����11��������
	size_t asize;
	unsigned char *data;
    
    // ��ϲ��Ժ������ݹ�ʵ��
    void fun_Combination(hashfunc_t* arr_fun,int arr_size,int start,int pos_fun,
                      const char *pSourceData,const int sourceClen,const int sourceRows,
                      const char *pFindData,const int findClen,const int findRows,
                      const int comNum);	
public:
  	bloom_test(size_t size);
  	virtual ~bloom_test();
    int write(const char *filename);
  	// ����
    int CombinationTest(const char *pSourceData,const int sourceClen,const int sourceRows,
                      const char *pFindData,const int findClen,const int findRows,
                      const int iSelectNum);
};

#endif

