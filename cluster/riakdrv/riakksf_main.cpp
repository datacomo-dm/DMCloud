#include "riakksf.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <vector>
#include <assert.h>

#ifndef RIAK_PAT_NAME
#define RIAK_PAT_NAME  "RIAK_PACKET_PAT"
#endif

using namespace std;
#define MAX_LINE_SIZE  1024
// 读取一行文件数据
char* my_fgets(char *s, int n,  FILE *stream,int &read_number);

// 读取未SOLID的KeySpace列表
int  ReadUnsolidKeySpaceList(const char* pKeyFileList,std::vector<int64_t>& keyLst);

// 从字符串----未固化列表中导出来的Key
// 71406   <t,a,k,alloc_time>(4780,68,2233785418258092032[0x1f000000b7b89000],20190417173001)
// 中获取 Key
void GetKeyFromStr1(const char * KeyString,int64_t& key);

// 从字符串----固化日志中搜索出来的Key
// 2018-08-05 01:21:08: [SOLID_KEY:<tabid:4375><attrid:0><keyspace:2233785415175766016>]
// 中获取 Key
void GetKeyFromStr2(const char * KeyString,int64_t& key);

int main(int argc,char* argv[])
{
    const char* str1 = "71406   <t,a,k,alloc_time>(4780,68,2233785418258092032[0x1f000000b7b89000],20190417173001)";
    const char* str2 = "2018-08-05 01:21:08: [SOLID_KEY:<tabid:4375><attrid:0><keyspace:2233785415175766016>]";
    int64_t key1,key2;
    GetKeyFromStr1(str1,key1);
    GetKeyFromStr2(str2,key2);
    
    if(argc == 2){
        if(strcmp(argv[1],"alloc_key_file_export") == 0){
            key_space_alloc_file_export();            
        }
        else if (strcmp(argv[1],"unsolid_key_file_export") == 0){
            key_space_unsolid_file_export();            
        }
        else if (strcmp(argv[1],"unsolid_key_recorver") == 0){
            key_space_file_timeout_recover();
        }else {
            goto help_info;
        }
    }
    else if(argc == 4 && (strcmp(argv[1],"check_unsolid_key") == 0)){
        // check_unsolid_key ./unsolid_key_file_export.txt  ./solided_key_file_list.txt
        const char* p_unsolid_key_file_export = argv[2];
        const char* p_solided_key_file_list = argv[3];
        std::vector<int64_t> unsolid_key_vector;

        int   check_unsolid_key_number = 0; 
        int   check_solided_key_number = 0;
        int   check_error_key_number = 0;

        // 将未固化的KeySpace先存储在内存中
       check_unsolid_key_number =  ReadUnsolidKeySpaceList(p_unsolid_key_file_export,unsolid_key_vector);
        if(check_unsolid_key_number >0 )
        {
                // 
                // 打开已经固化的KeySpace文件列表，逐个KeySpace读取出来进行对比，
                // 发现错误就遇到重复的，就说明有错了，并输出
                //                
                FILE *pfile = NULL;
                pfile = fopen(p_solided_key_file_list,"r");
                assert(pfile);
                char line[MAX_LINE_SIZE]={0};
                int read_number = 0;
                while(my_fgets(line,MAX_LINE_SIZE,pfile,read_number))
                {
                    check_solided_key_number ++;
                    int64_t keyspace = 0;
                                        
                    // 从字符串----固化日志中搜索出来的Key
                    // 2018-08-05 01:21:08: [SOLID_KEY:<tabid:4375><attrid:0><keyspace:2233785415175766016>]
                    // 中获取 Key
                    GetKeyFromStr2(line,keyspace);
                    assert(keyspace>100000000);
                    {
                        // 逐个比较
                        for(int i=0;i<check_unsolid_key_number;i++){
                               if(keyspace ==  unsolid_key_vector[i]){
                                    check_error_key_number++;
                                    printf("check error keyspace:%ld<0x%lx>:%s\n",keyspace,keyspace,line);    
                                }
                        }
                    }
                    memset(line,0,MAX_LINE_SIZE);
                }
                fclose(pfile);
                
                unsolid_key_vector.clear();
                printf("\n\n");
                printf("------check summary--------\n");
                printf("check_solided_key_number = %d\n",check_solided_key_number);
                printf("check_unsolid_key_number = %d\n",check_unsolid_key_number);
                printf("check_error_key_number = %d\n",check_error_key_number);
                printf("---------------------------\n");
        }
        else{
            printf("%s has NULL\n",p_unsolid_key_file_export);
        }
    }
    else if(argc == 3 && (strcmp(argv[1],"reuse_cluster_fm_keyspace") == 0)){
            int64_t  reuse_keyspace = atol(argv[2]);
            if(reuse_keyspace<0x1f00000000000000){
                printf("Reuse keyspace [0x%016lx-%ld]\n",reuse_keyspace,reuse_keyspace);
                key_space_file_free(RIAK_PAT_NAME,0,0,reuse_keyspace);
            }else{
                printf("Error,can not reuse keyspace [0x%x-%ld]\n",reuse_keyspace,reuse_keyspace);
            }
    }
    else{
        help_info:
        printf("Input Error,Example \n");
        printf("%s alloc_key_file_export \n",argv[0]);
        printf("\t--Export alloc riak file \n\n",argv[0]);
        
        printf("%s unsolid_key_file_export \n",argv[0]);
        printf("\t--Export unsolid riak file \n\n",argv[0]);
        
        printf("%s reuse_cluster_fm_keyspace <keyspace> \n",argv[0]);
        printf("\t--reuse cluster_fm keyspace \n\n",argv[0]);      

        printf("%s unsolid_key_recorver \n",argv[0]);
        printf("\t--unsolid key recorver \n\n",argv[0]);

        printf("%s check_unsolid_key input_unsolid_key_file_list  input_solided_key_file_list \n",argv[0]);
        printf("\t--check_unsolid_key ./unsolid_key_file_export.txt  ./solided_key_file_list.txt \n\n",argv[0]);
    }
    return 0;
}

// 从字符串----未固化列表中导出来的Key
// 71406   <t,a,k,alloc_time>(4780,68,2233785418258092032[0x1f000000b7b89000],20190417173001)
// 中获取 Key
void GetKeyFromStr1(const char * KeyString,int64_t& key)
{
    char tmp[MAX_LINE_SIZE]={0};
    strcpy(tmp,KeyString);
    char* p = tmp;
    char *q  = p + (strlen(p) -1);
    while(*q != ']') q--;
    *q = 0;
    while(*q != '[') q--;
    q++;
    sscanf(q,"0x%lx",&key);
}

// 从字符串----固化日志中搜索出来的Key
// 2018-08-05 01:21:08: [SOLID_KEY:<tabid:4375><attrid:0><keyspace:2233785415175766016>]
// 中获取 Key
void GetKeyFromStr2(const char * KeyString,int64_t& key)
{
    char tmp[MAX_LINE_SIZE]={0};
    strcpy(tmp,KeyString);
    char* p = tmp;
    char *q  = p + (strlen(p) -1);
    *q=0; q-- ;   // skip ]
    *q=0; q-- ;  //  skip >
    while(*q != ':') q--;
    q++;
    key=atoll(q);
}

char* my_fgets(char *s, int n,  FILE *stream,int &read_number)
{
   int n_ori = n;
   read_number = 0;
   register int c;
   register char *cs;
   cs=s;
   while(--n>0 &&(c = getc(stream))!=EOF)
   if ((*cs++=  c) =='\n'){
         break;
   }
   *cs ='\0';
   
   read_number = (n_ori-n);     
   return (c == EOF && cs == s) ?NULL :s ;
}


// 读取未SOLID的KeySpace列表
int  ReadUnsolidKeySpaceList(const char* pKeyFileList,std::vector<int64_t>& keyLst)
{
    FILE *pfile = NULL;
    pfile = fopen(pKeyFileList,"r");
    assert(pfile);
    char line[MAX_LINE_SIZE]={0};
    int read_number = 0;
    keyLst.clear();
    while(my_fgets(line,MAX_LINE_SIZE,pfile,read_number))
    {
        if(strlen(line)<50) continue;
        int64_t keyspace = 0;
        GetKeyFromStr1(line,keyspace);
        assert(keyspace>100000000);
        keyLst.push_back(keyspace);
        memset(line,0,MAX_LINE_SIZE);
    }
    fclose(pfile);
    
    return keyLst.size();
}


