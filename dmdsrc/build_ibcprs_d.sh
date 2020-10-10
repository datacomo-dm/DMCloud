oldpwd="`pwd`"

# ���԰汾libibcompress_d.a����
cd ${HOME}/dmdsrc/build/community/debug/infobright
echo

if [ "A${?}" != "A0" ]; then
    echo "����Ŀ¼������:${HOME}/dmdsrc/build/community/debug/infobright ��"
    echo "��ǰĿ¼`pwd`" 
    exit
fi

# ����ibcompress/ibdecompress��
echo "rm -f ${oldpwd}/libibcompress_d.a"
rm -f "${oldpwd}/libibcompress_d.a"
echo

# �����RCDataType.o,RCDateTime.o
echo "1. �����RCDataType.o,RCDateTime.o"

echo "rm -f storage/brighthouse/types/RCDataType.o"
rm -f storage/brighthouse/types/RCDataType.o

echo "rm -f storage/brighthouse/types/RCDateTime.o"
rm -f storage/brighthouse/types/RCDateTime.o
echo

# ���±���RCDataType.o
echo "2.���±���RCDataType.o"
g++ -g3 -ggdb3 -O0 -fPIC -fno-inline -std=c++0x -DPURE_LIBRARY  -D_THREAD_SAFE -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS   -m64 -fprefetch-loop-arrays   -fno-strict-aliasing -fno-merge-constants -fno-gcse  -Wno-deprecated -fno-inline -D_THREAD_SAFE -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS -m64 -fprefetch-loop-arrays -fno-strict-aliasing -fno-merge-constants -fno-gcse -Wno-deprecated -pthread -I/usr/local/boost_1_42_0/include -Wmissing-include-dirs -DHAVE_CONFIG_H -D__BH_COMMUNITY__ -DMYSQL_SERVER -I. -I${HOME}/dmdsrc/src/storage/brighthouse -I${HOME}/dmdsrc/src/storage/brighthouse/community -I../.. -I${HOME}/dmdsrc/vendor/mysql/include -I${HOME}/dmdsrc/build/community/debug/vendor/include -I${HOME}/dmdsrc/vendor/mysql/sql -I${HOME}/dmdsrc/vendor/mysql/regex -I${HOME}/dmdsrc/vendor/mysql/extra/yassl/taocrypt/include -I${HOME}/dmdsrc/vendor/mysql/extra/yassl/taocrypt/mySTL  ${HOME}/dmdsrc/src/storage/brighthouse/types/RCDataType.cpp -c -o storage/brighthouse/types/RCDataType.o

# ���±���RCDateTime.o
echo "3.���±���RCDateTime.o"
g++ -g3 -ggdb3 -O0 -fPIC -fno-inline -std=c++0x -DPURE_LIBRARY -D_THREAD_SAFE -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS   -m64 -fprefetch-loop-arrays   -fno-strict-aliasing -fno-merge-constants -fno-gcse  -Wno-deprecated -fno-inline -D_THREAD_SAFE -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS -m64 -fprefetch-loop-arrays -fno-strict-aliasing -fno-merge-constants -fno-gcse -Wno-deprecated -pthread -I/usr/local/boost_1_42_0/include -Wmissing-include-dirs -DHAVE_CONFIG_H -D__BH_COMMUNITY__ -DMYSQL_SERVER -I. -I${HOME}/dmdsrc/src/storage/brighthouse -I${HOME}/dmdsrc/src/storage/brighthouse/community -I../.. -I${HOME}/dmdsrc/vendor/mysql/include -I${HOME}/dmdsrc/build/community/debug/vendor/include -I${HOME}/dmdsrc/vendor/mysql/sql -I${HOME}/dmdsrc/vendor/mysql/regex -I${HOME}/dmdsrc/vendor/mysql/extra/yassl/taocrypt/include -I${HOME}/dmdsrc/vendor/mysql/extra/yassl/taocrypt/mySTL  ${HOME}/dmdsrc/src/storage/brighthouse/types/RCDateTime.cpp -c -o storage/brighthouse/types/RCDateTime.o

# ���±���RCBString.o
echo "4.���±���RCBString.o"
g++ -g3 -ggdb3 -O0 -fPIC -fno-inline -std=c++0x -DPURE_LIBRARY -DlibIBCompress_DEF -D_THREAD_SAFE -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS   -m64 -fprefetch-loop-arrays   -fno-strict-aliasing -fno-merge-constants -fno-gcse  -Wno-deprecated -fno-inline -D_THREAD_SAFE -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS -m64 -fprefetch-loop-arrays -fno-strict-aliasing -fno-merge-constants -fno-gcse -Wno-deprecated -pthread -I/usr/local/boost_1_42_0/include -Wmissing-include-dirs -DHAVE_CONFIG_H -D__BH_COMMUNITY__ -DMYSQL_SERVER -I. -I${HOME}/dmdsrc/src/storage/brighthouse -I${HOME}/dmdsrc/src/storage/brighthouse/community -I../.. -I${HOME}/dmdsrc/vendor/mysql/include -I${HOME}/dmdsrc/build/community/debug/vendor/include -I${HOME}/dmdsrc/vendor/mysql/sql -I${HOME}/dmdsrc/vendor/mysql/regex -I${HOME}/dmdsrc/vendor/mysql/extra/yassl/taocrypt/include -I${HOME}/dmdsrc/vendor/mysql/extra/yassl/taocrypt/mySTL  ${HOME}/dmdsrc/src/storage/brighthouse/types/RCBString.cpp -c -o storage/brighthouse/types/RCBString.o

# �������ɿ�
echo
echo "5.���ɾ�̬��libibcompress_d.a"
echo "ar crv libibcompress_d.a ./storage/brighthouse/compress/*.o ./storage/brighthouse/core/QuickMath.o  ./storage/brighthouse/system/TextUtils.o ./storage/brighthouse/types/*.o"
ar crv libibcompress_d.a ./storage/brighthouse/compress/*.o ./storage/brighthouse/core/QuickMath.o  ./storage/brighthouse/system/TextUtils.o ./storage/brighthouse/types/*.o
echo

echo "cp libibcompress_d.a ${oldpwd}/"
cp libibcompress_d.a "${oldpwd}/"
