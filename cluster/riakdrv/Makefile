PREFIX?=/usr/local
LIBDIR = $(PREFIX)/lib/
INCDIR = $(PREFIX)/include/

OSNAME = $(shell uname)

TARGET_A = libriakdrv++.a
TARGET_SO = libriakdrv++.so

TARGET = $(TARGET_A) $(TARGET_SO)

CC = gcc
CXX = g++
AR = ar
RANLIB = ranlib
RM = rm
MKDIR = mkdir
INSTALL = install
CPPFLAGS = -Wall -Werror -std=c++0x -g -fPIC -I .. -I $(INCDIR)
LDFLAGS = -fPIC -shared
EXT_LIBS = -lcr_class
EXT_LIBS = -lcfmdrv
ifeq ($(OSNAME), Linux)
LDFLAGS += -lrt
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

install: $(TARGET_A) $(TARGET_SO)
	$(MKDIR) -p $(LIBDIR)
	$(MKDIR) -p $(INCDIR)riakdrv
	$(INSTALL) $(TARGET_A) $(LIBDIR)
	$(INSTALL) $(TARGET_SO) $(LIBDIR)
	$(INSTALL) -m 444 *.h $(INCDIR)riakdrv/

uninstall:
	$(RM) -f $(LIBDIR)$(TARGET_A)
	$(RM) -f $(LIBDIR)$(TARGET_SO)
	$(RM) -rf $(INCDIR)riakdrv

$(TARGET_A): $(OBJECTS)
	${AR} rcs $(TARGET_A) $(OBJECTS)
	$(RANLIB) $(TARGET_A)

$(TARGET_SO): $(OBJECTS)
	${CC} -o $(TARGET_SO) $(LDFLAGS) $(OBJECTS) $(EXT_LIBS)

clean:
	$(RM) -f *.pb.cc *.pb.h *.o *.core *~ *.a *.so .depend
	$(RM) -f riakproto/*.o
