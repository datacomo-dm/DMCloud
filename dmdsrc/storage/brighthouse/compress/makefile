LDFLAGS= -fPIC -fpic -DPIC -lstdc++ -shared  
#INCLUDE= -I./ -I../ -I../common/ -I../system -I/usr/local/boost_1_42_0/include/ -I../
INCLUDE= -I./ -I../common/ -I../system -I/usr/local/boost_1_42_0/include/ 
SOURCE=IBCompress.cc
# *.o (except IBCompress.o) generate by inforbright maker(or makee)
OBJS=PPM.o IncAlloc.o BitstreamCompressor.o  NumCompressor.o TextCompressor.o IBCompress.o PartDict.o IncWGraph.o RangeCoder.o

CXX = gcc 

CFLAGS =  -g -fpic -fPIC -DAVL_TREE_FIX_MEM -DPIC -D_FILE_OFFSET_BITS=64 -DBUILD_LEGACY_64_BIT_MODE -D_LARGEFILE_SOURCE $(INCLUDE) -DWDBI64BIT

TARIBCPRS_S=libibcprs.so

all: $(TARIBCPRS_S)

$(TARIBCPRS_S):
	echo "Build ib compress/decompress library."
	rm -f $(OBJS) $@
	$(CXX) $(CFLAGS)  -c $(SOURCE)
	$(CXX) $(LDFLAGS) $(OBJS) -o $@

clean :
	rm -f IBCompress.o $(TARIBCPRS_S)
