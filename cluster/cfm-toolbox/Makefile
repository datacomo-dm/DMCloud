PREFIX?=/usr/local
LIBDIR = $(PREFIX)/lib/
INCDIR = $(PREFIX)/include/

OSNAME = $(shell uname)

TARGET = cfm-toolbox

CC = g++
AR = ar
RANLIB = ranlib
RM = rm
MKDIR = mkdir
INSTALL = install
CPPFLAGS = -Wall -Werror -I $(INCDIR) -std=c++0x -g -fPIC
EXT_LIBS += -lcfmdrv
EXT_LIBS += -lcr_class
EXT_LIBS += -lprotobuf
EXT_LIBS += libibcompress/libibcompress.a
LDFLAGS = -pthread -lpthread $(EXT_LIBS) -ldl
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
