PREFIX?=/usr/local
LIBDIR = $(PREFIX)/lib/
INCDIR = $(PREFIX)/include/

OSNAME = $(shell uname)

CC = g++
AR = ar
RM = rm
INSTALL = install
CPPFLAGS = -Wall -Werror -I $(INCDIR) -DDEBUG -O2 -fPIC -g
#CPPFLAGS = -Wall -Werror -I $(INCDIR) -O2 -fPIC -g
#LDFLAGS = `pkg-config --libs protobuf`
LDFLAGS = -pthread -lz
ifeq ($(OSNAME), Linux)
LDFLAGS += -lrt
endif

SOURCES_TEST0 = test0.cc
OBJECTS_TEST0 = $(SOURCES_TEST0:.cc=.o)

SOURCES_TEST1 = test1.cc
OBJECTS_TEST1 = $(SOURCES_TEST1:.cc=.o)

SOURCES_TEST2 = test2.cc
OBJECTS_TEST2 = $(SOURCES_TEST2:.cc=.o)

SOURCES_TEST3 = test3.cc msgIBRiak.pb.cpp
OBJECTS_TEST3 = $(SOURCES_TEST3:.cc=.o)

test0:	${OBJECTS_TEST0}
	$(CC) -o test0 $(CPPFLAGS) $(OBJECTS_TEST0) $(LDFLAGS) $(LIBDIR)libcr_class.a

test1:	${OBJECTS_TEST1}
	$(CC) -o test1 $(CPPFLAGS) $(OBJECTS_TEST1) $(LDFLAGS) $(LIBDIR)libriakdrv++.a $(LIBDIR)libprotobuf.a

test2:	${OBJECTS_TEST2}
	$(CC) -o test2 $(CPPFLAGS) $(OBJECTS_TEST2) $(LDFLAGS) $(LIBDIR)libcr_class.a

test3:	${OBJECTS_TEST3}
	$(CC) -g -o test3 $(CPPFLAGS) $(OBJECTS_TEST3) $(LDFLAGS) $(LIBDIR)libriakdrv++.a $(LIBDIR)libcr_class.a $(LIBDIR)libprotobuf.a

clean:
	$(RM) -f *.o *.core *~ test?
	$(RM) -f riakproto/*.o
