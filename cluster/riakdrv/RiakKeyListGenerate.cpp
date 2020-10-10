#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <set>
#include <string>
#include <assert.h>
using namespace std;
#define MAX_LINE_SIZE  1024
// 读取一行文件数据
char* my_fgets(char *s, int n,  FILE *stream,int &read_number);

char * get_exe_path( char * buf, int count);
/*
P20000016D24-AF1
P1F00000098F22-10A
P1F0000003F9B6-257
PCBFC-6FB
P1F000000A2776-FE
P1F00000084730-18C
*/
// 已经完成作废
int main0(int argc,char* argv[])
{
    if(argc >=2) {
        char exe_path[MAX_LINE_SIZE]= {0};
        get_exe_path(exe_path,MAX_LINE_SIZE);
        std::set<std::string> keySet;
        for(int i=1; i<argc; i++) {
            FILE *pfile = NULL;
            char filepath[MAX_LINE_SIZE]= {0};
            sprintf(filepath,"%s/%s",exe_path,argv[i]);
            pfile = fopen(filepath,"r");
            assert(pfile);

            printf("\n\n-----------------------------------------------------");
            printf("\n\n filenum %d open file [%s] ....",i,filepath);
            printf("\n\n-----------------------------------------------------");

            char line[MAX_LINE_SIZE]= {0};
            int read_number = 0;
            char head[6]= {0};
            while(my_fgets(line,MAX_LINE_SIZE,pfile,read_number)) {
                memset(head,0x0,6);
                memcpy(head,line,5);
                if(strcmp("P1F00",head) == 0) {
                    printf("new key \n");
                    continue;
                }
                if(keySet.count(std::string(line)) == 0) {
                    keySet.insert(std::string(line));
                } else {
                    printf("same key \n");
                }
                memset(line,0,MAX_LINE_SIZE);
            }
            fclose(pfile);
        }

        std::set<std::string>::iterator iter;
        for(iter = keySet.begin(); iter != keySet.end(); iter ++) {
            printf("/root/dmd-package/riak-rel-20180224140300/riak-toolbox  kvtest-sc DEL IB_DATAPACKS  %s \n",
                   (*iter).c_str());
        }
    } else {
        printf("Input Error,Example \n");
        printf("\t--generate_riakkey  file1 file2 .... \n\n",argv[0]);
    }
    return 0;
}


/*
[root@dma-node1 dmd-riak-operation]# pwd
/root/dmd-riak-operation
[root@dma-node1 dmd-riak-operation]# ll dma-node-valid-riak-key.list.new
-rw-r--r-- 1 root root 244554199 May 22 22:00 dma-node-valid-riak-key.list.new
[root@dma-node1 dmd-riak-operation]# head dma-node-valid-riak-key.list.new
P0-12
P0-2B
P0-31
P1-14
P1-1F
PFFFF-E1
PFFFF-F6
*/

int main(int argc,char* argv[])
{
    if(argc >=2) {
        char exe_path[MAX_LINE_SIZE]= {0};
        get_exe_path(exe_path,MAX_LINE_SIZE);
        std::set<int64_t> keySet;
        for(int i=1; i<argc; i++) {
            FILE *pfile = NULL;
            char filepath[MAX_LINE_SIZE]= {0};
            sprintf(filepath,"%s/%s",exe_path,argv[i]);
            pfile = fopen(filepath,"r");
            char line[MAX_LINE_SIZE]= {0};
            int read_number = 0;
            char head[6]= {0};
            while(my_fgets(line,MAX_LINE_SIZE,pfile,read_number)) {

                int sl = strlen(line);
                if(line[sl-1]=='\n' || line[sl-1]=='\r') line[--sl]=0;
                if(line[sl-1]=='\n' || line[sl-1]=='\r') line[--sl]=0;

                memset(head,0x0,6);
                memcpy(head,line,5);
                if(strcmp("P1F00",head) == 0) {
                    printf("new key \n");
                    continue;
                }

                const char* p=line;
                int64_t keyspace_high = 0;
                int64_t keyspace_low = 0;
                sscanf(p,"P%lX-%X",&keyspace_high,&keyspace_low);
                int64_t keyspace = (keyspace_high<<12);

                if(keySet.count(keyspace) == 0) {
                    keySet.insert(keyspace);
                } else {
                //    printf("same key \n");
                }
                memset(line,0,MAX_LINE_SIZE);
            }
            fclose(pfile);
        }

        std::set<int64_t>::iterator iter;
        for(iter = keySet.begin(); iter != keySet.end(); iter ++) {
            printf("/app/dma/datamerger/var/RiakKeyspaceDataSrc/riakksf_main  reuse_cluster_fm_keyspace  %lld\n",
                   *iter);

            
            printf("-----%lld ---> 0x%016lx ---> 0x%013lx-%03x \n",
                   *iter,*iter,(*iter)>>12,(*iter)&0xfff);
        }
    } else {
        printf("Input Error,Example \n");
        printf("\t--generate_riakkey  file1 file2 .... \n\n",argv[0]);
    }
    return 0;
}


#include <unistd.h>
// 获取进程路径
char * get_exe_path( char * buf, int count)
{
    int i;
    int rslt = readlink("/proc/self/exe", buf, count - 1);
/////注意这里使用的是self
    if (rslt < 0 || (rslt >= count - 1)) {
        return NULL;
    }
    buf[rslt] = '\0';
    for (i = rslt; i >= 0; i--) {
        //printf("buf[%d] %c\n", i, buf[i]);
        if (buf[i] == '/') {
            buf[i + 1] = '\0';
            break;
        }
    }
    return buf;
}


char* my_fgets(char *s, int n,  FILE *stream,int &read_number)
{
    int n_ori = n;
    read_number = 0;
    register int c;
    register char *cs;
    cs=s;
    while(--n>0 &&(c = getc(stream))!=EOF)
        if ((*cs++=  c) =='\n') {
            break;
        }
    *cs ='\0';

    read_number = (n_ori-n);
    return (c == EOF && cs == s) ?NULL :s ;
}




