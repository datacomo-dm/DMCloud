PREFIX?=/usr/local
LIBDIR = $(PREFIX)/lib/
INCDIR = $(PREFIX)/include/

OSNAME = $(shell uname)

TARGET = cluster_fm
PAXOS_SO = libevpaxos.so

CC = g++
AR = ar
CD = cd
CMAKE = cmake
RANLIB = ranlib
RM = rm
MKDIR = mkdir
INSTALL = install
CPPFLAGS = -Wall -Werror -I $(INCDIR) -std=c++0x -g -fPIC
EXT_LIBS += -levpaxos
EXT_LIBS += -levent
EXT_LIBS += -lcfmdrv
EXT_LIBS += -lcr_class
EXT_LIBS += -lprotobuf
EXT_LIBS += -lpthread
EXT_LIBS += -ldl
ifeq ($(OSNAME), Linux)
EXT_LIBS += -lrt
endif

LDFLAGS += -pthread $(EXT_LIBS)

SOURCES = $(wildcard *.cc)
OBJECTS = $(SOURCES:.cc=.o)

depend: .depend

.depend: $(SOURCES)
	./makepaxos
	$(RM) -f ./.depend
	$(CC) $(CPPFLAGS) -M $^ > ./.depend;

include .depend

debug: CPPFLAGS += -DDEBUG -DDIAGNOSTIC -O0
debug: $(TARGET)

release: CPPFLAGS += -O2
release: $(TARGET)   

$(TARGET): $(OBJECTS)
	$(CC) $^ -o $@ $(LDFLAGS)

install:
	$(RM) -f $(LIBDIR)$(PAXOS_SO)
	$(MKDIR) -p $(LIBDIR)
	$(INSTALL) $(PAXOS_SO) $(LIBDIR)

uninstall:
	$(RM) -f $(LIBDIR)$(PAXOS_SO)

clean:
	$(RM) -f *.o *.core *~ *.a .depend $(TARGET)
