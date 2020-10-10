PREFIX?=/usr/local
LIBDIR = $(PREFIX)/lib/
INCDIR = $(PREFIX)/include/

OSNAME = $(shell uname)

TARGET = libcfm_ibload.so

CC = g++
RM = rm
MKDIR = mkdir
INSTALL = install
CPPFLAGS = -Wall -Werror -I .. -I $(INCDIR) -I ./libibcompress -std=c++0x -g -fPIC
EXTRA_LIBS += -L./libibcompress
EXTRA_LIBS += -libcompress
EXTRA_LIBS += -lriakdrv++
EXTRA_LIBS += -lcfmdrv
EXTRA_LIBS += -lcr_class
EXTRA_LIBS += -lleveldb
EXTRA_LIBS += -lprotobuf
ifeq ($(OSNAME), Linux)
EXTRA_LIBS += -lrt
endif

SOURCES = $(wildcard *.cc)
OBJECTS = $(SOURCES:.cc=.o)

depend: .depend

.depend: $(SOURCES)
	protoc --cpp_out=. *.proto
	$(RM) -f ./.depend
	$(CC) $(CPPFLAGS) -M $^ > ./.depend;

include .depend

debug: CPPFLAGS += -DDEBUG -DDIAGNOSTIC -O0
debug: $(TARGET)

release: CPPFLAGS += -O2
release: $(TARGET)

$(TARGET): $(OBJECTS)
	${CC} -shared -o $(TARGET) $(OBJECTS) $(EXTRA_LIBS)

clean:
	$(RM) -f *.o *.core *~ *.a .depend $(TARGET)
