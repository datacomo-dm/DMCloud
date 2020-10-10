PREFIX?=/usr/local
LIBDIR = $(PREFIX)/lib/
INCDIR = $(PREFIX)/include/

OSNAME = $(shell uname)

TARGET = riak-toolbox

CC = g++
AR = ar
RANLIB = ranlib
RM = rm
MKDIR = mkdir
INSTALL = install
CPPFLAGS = -Wall -Werror -I $(INCDIR) -std=c++0x -g -fPIC -fno-strict-aliasing
LDFLAGS += $(LIBDIR)libriakdrv++.a
LDFLAGS += $(LIBDIR)libcr_class.a
LDFLAGS += $(LIBDIR)libcfmdrv.a
LDFLAGS += -pthread -lprotobuf -lpthread
LDFLAGS += `pkg-config --libs liblzma`
LDFLAGS += -lsnappy
LDFLAGS += -llz4
ifeq ($(OSNAME), Linux)
LDFLAGS += -lrt
endif

SOURCES = $(wildcard *.cc)
OBJECTS = $(SOURCES:.cc=.o)

depend: .depend

.depend: $(SOURCES)
	$(RM) -f ./.depend
	$(CC) $(CPPFLAGS) -M $^ > ./.depend;

include .depend

debug: CPPFLAGS += -DDEBUG -DDIAGNOSTIC -O0
debug: $(TARGET)

release: CPPFLAGS += -O2
release: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $^ -o $@ $(LDFLAGS)

clean:
	$(RM) -f *.o *.core *~ *.a .depend $(TARGET)
