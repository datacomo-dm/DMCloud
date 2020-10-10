# ���԰汾libibcompress_r.a����
echo "����Ŀ¼������:/home/dm6/dmdsrc/build/community/release/infobright ��"
echo "��ǰĿ¼`pwd`" 
echo 

# ����ibcompress/ibdecompress��
echo "rm libibcompress_r.a"
rm libibcompress_r.a -rf
echo

# �����RCDataType.o,RCDateTime.o,RCBString.o
echo "1. �����RCDataType.o,RCDateTime.o,RCBString.o"

echo "rm ./ibcprs/RCDataType.o"
rm ./ibcprs/RCDataType.o -rf
echo

echo "rm ./ibcprs/RCDateTime.o"
rm ./ibcprs/RCDateTime.o -rf
echo

echo "rm ./ibcprs/RCBString.o"
rm ./ibcprs/RCBString.o -rf
echo

# ���±���RCDataType.o
echo "2.���±���RCDataType.o"
g++ -O3 -ggdb3 -O0 -fno-inline -std=c++0x -DPURE_LIBRARY  -D_THREAD_SAFE -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS   -m64 -fprefetch-loop-arrays   -fno-strict-aliasing -fno-merge-constants -fno-gcse  -Wno-deprecated   -O3 -ggdb3 -O0 -fno-inline -D_THREAD_SAFE -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS -m64 -fprefetch-loop-arrays -fno-strict-aliasing -fno-merge-constants -fno-gcse -Wno-deprecated -pthread -I/usr/local/boost_1_42_0/include -Wmissing-include-dirs -DHAVE_CONFIG_H -D__BH_COMMUNITY__ -DMYSQL_SERVER -I. -I/home/dm6/dmdsrc/src/storage/brighthouse -I/home/dm6/dmdsrc/src/storage/brighthouse/community -I../.. -I/home/dm6/dmdsrc/vendor/mysql/include -I/home/dm6/dmdsrc/build/community/debug/vendor/include -I/home/dm6/dmdsrc/vendor/mysql/sql -I/home/dm6/dmdsrc/vendor/mysql/regex -I/home/dm6/dmdsrc/vendor/mysql/extra/yassl/taocrypt/include -I/home/dm6/dmdsrc/vendor/mysql/extra/yassl/taocrypt/mySTL  /home/dm6/dmdsrc/src/storage/brighthouse/types/RCDataType.cpp -c -o ./ibcprs/RCDataType.o

# ���±���RCDateTime.o
echo "3.���±���RCDateTime.o"
g++ -O3 -ggdb3 -O0 -fno-inline -std=c++0x -DPURE_LIBRARY -D_THREAD_SAFE -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS   -m64 -fprefetch-loop-arrays   -fno-strict-aliasing -fno-merge-constants -fno-gcse  -Wno-deprecated   -O3 -ggdb3 -O0 -fno-inline -D_THREAD_SAFE -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS -m64 -fprefetch-loop-arrays -fno-strict-aliasing -fno-merge-constants -fno-gcse -Wno-deprecated -pthread -I/usr/local/boost_1_42_0/include -Wmissing-include-dirs -DHAVE_CONFIG_H -D__BH_COMMUNITY__ -DMYSQL_SERVER -I. -I/home/dm6/dmdsrc/src/storage/brighthouse -I/home/dm6/dmdsrc/src/storage/brighthouse/community -I../.. -I/home/dm6/dmdsrc/vendor/mysql/include -I/home/dm6/dmdsrc/build/community/debug/vendor/include -I/home/dm6/dmdsrc/vendor/mysql/sql -I/home/dm6/dmdsrc/vendor/mysql/regex -I/home/dm6/dmdsrc/vendor/mysql/extra/yassl/taocrypt/include -I/home/dm6/dmdsrc/vendor/mysql/extra/yassl/taocrypt/mySTL  /home/dm6/dmdsrc/src/storage/brighthouse/types/RCDateTime.cpp -c -o ./ibcprs/RCDateTime.o

# ���±���RCBString.o
echo "4.���±���RCBString.o"
g++ -O3 -ggdb3 -O0 -fno-inline -std=c++0x -DPURE_LIBRARY -DlibIBCompress_DEF -D_THREAD_SAFE -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS   -m64 -fprefetch-loop-arrays   -fno-strict-aliasing -fno-merge-constants -fno-gcse  -Wno-deprecated   -O3 -ggdb3 -O0 -fno-inline -D_THREAD_SAFE -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS -m64 -fprefetch-loop-arrays -fno-strict-aliasing -fno-merge-constants -fno-gcse -Wno-deprecated -pthread -I/usr/local/boost_1_42_0/include -Wmissing-include-dirs -DHAVE_CONFIG_H -D__BH_COMMUNITY__ -DMYSQL_SERVER -I. -I/home/dm6/dmdsrc/src/storage/brighthouse -I/home/dm6/dmdsrc/src/storage/brighthouse/community -I../.. -I/home/dm6/dmdsrc/vendor/mysql/include -I/home/dm6/dmdsrc/build/community/debug/vendor/include -I/home/dm6/dmdsrc/vendor/mysql/sql -I/home/dm6/dmdsrc/vendor/mysql/regex -I/home/dm6/dmdsrc/vendor/mysql/extra/yassl/taocrypt/include -I/home/dm6/dmdsrc/vendor/mysql/extra/yassl/taocrypt/mySTL  /home/dm6/dmdsrc/src/storage/brighthouse/types/RCBString.cpp -c -o ./ibcprs/RCBString.o

# �������ɿ�
echo
echo "5.���ɾ�̬��libibcompress_r.a"
echo "ar crv libibcompress_r.a ./storage/brighthouse/compress/*.o ./storage/brighthouse/core/QuickMath.o  ./storage/brighthouse/system/TextUtils.o ./storage/brighthouse/types/RCItemTypes.o ./storage/brighthouse/types/RCNum.o ./storage/brighthouse/types/RCValueObject.o ./storage/brighthouse/types/ValueParserForText.o ./ibcprs/*.o"
ar crv libibcompress_r.a ./storage/brighthouse/compress/*.o ./storage/brighthouse/core/QuickMath.o  ./storage/brighthouse/system/TextUtils.o ./storage/brighthouse/types/DataConverter.o  ./storage/brighthouse/types/RCItemTypes.o ./storage/brighthouse/types/RCNum.o ./storage/brighthouse/types/RCValueObject.o ./storage/brighthouse/types/ValueParserForText.o ./ibcprs/*.o
echo

echo "cp libibcompress_r.a /tmp"
cp libibcompress_r.a /tmp

echo "cp libibcompress_r.a /usr/local/lib"
cp libibcompress_r.a /usr/local/lib

echo "cp /home/dm6/dmdsrc/src/storage/brighthouse/compress/IBCompress.h /usr/local/include"
cp /home/dm6/dmdsrc/src/storage/brighthouse/compress/IBCompress.h /usr/local/include