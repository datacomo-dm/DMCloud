PREFIX?=/usr/local
LIBDIR = $(PREFIX)/lib/
INCDIR = $(PREFIX)/include/

OSNAME = $(shell uname)

TARGET = libcfm_paxos.so

CC = g++
LD = ld
RM = rm
MKDIR = mkdir
INSTALL = install
CPPFLAGS = -Wall -Werror -I .. -I $(INCDIR) -std=c++0x -g -fPIC
LDFLAGS = -shared -z combreloc -z now
EXTRA_LIBS += -lcfmdrv
EXTRA_LIBS += -lriakdrv++
EXTRA_LIBS += -lcr_class
EXTRA_LIBS += -lpaxos
EXTRA_LIBS += -levpaxos
ifeq ($(OSNAME), Linux)
EXTRA_LIBS += -lrt
endif

SOURCES = $(wildcard *.cc)
OBJECTS = $(SOURCES:.cc=.o)

depend: .depend

.depend: $(SOURCES)
	$(RM) -f ./.depend
	$(CC) $(CPPFLAGS) -MM $^ > ./.depend;

include .depend

debug: CPPFLAGS += -DDEBUG -DDIAGNOSTIC -O0
debug: $(TARGET)

release: CPPFLAGS += -O2
release: $(TARGET)

$(TARGET): $(OBJECTS)
	${CC} $(LDFLAGS) -o $(TARGET) $(OBJECTS) $(EXTRA_LIBS)

clean:
	$(RM) -f *.o *.core *~ *.a .depend $(TARGET)
